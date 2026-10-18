"""Photo refresh checkpoints and verified, resumable downloads."""

import asyncio
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from aiohttp import ClientConnectionError, ClientPayloadError
from PIL import Image

import main

URL_A = "https://example.test/a.png"
URL_B = "https://example.test/b.png"
CHECKED_AT = datetime(2026, 1, 1, tzinfo=timezone.utc)
EMPTY_PAGE = b'<span id="ContentPlaceHolder1_lbldisplatinname">Example alba - </span><span id="ContentPlaceHolder1_lblFamily">Family</span><table id="ContentPlaceHolder1_tblPlantImges"></table>'


@pytest.fixture
def png():
    output = BytesIO()
    Image.new("RGB", (3, 2), "green").save(output, format="PNG")
    return output.getvalue()


@pytest.fixture
def archive(tmp_path):
    def seed(plants):
        path = tmp_path / "data.sqlite"
        with sqlite3.connect(path) as con:
            main.create_tables(con)
            for name, urls in plants.items():
                row = dict.fromkeys(main.SEARCH_COLUMNS, "")
                row.update(
                    latin_name=name,
                    height=3,
                    edibility_rating=None,
                    medicinal_rating=None,
                    other_uses_rating=None,
                )
                main.record_plant_row(con, row)
                con.execute(
                    "UPDATE plants SET summary='Keep description',family='Keep family',details_scraped_at=? WHERE latin_name=?",
                    (CHECKED_AT.isoformat(), name),
                )
                main.record_plant_images(
                    con, name, dict.fromkeys(urls, "Credit"), CHECKED_AT
                )
        cache = str(tmp_path / "cache.sqlite")
        return path, ["--database", str(path), "--cache", cache]

    return seed


@pytest.fixture
def network(monkeypatch):
    def configure(bodies):
        responses = []
        for body in bodies:
            response = MagicMock(from_cache=False)
            response.read = (
                AsyncMock(side_effect=body)
                if isinstance(body, BaseException)
                else AsyncMock(return_value=body)
            )
            context = AsyncMock()
            context.__aenter__.return_value = response
            responses.append(context)
        session = MagicMock()
        session.request.side_effect = responses
        session.disabled.return_value = AsyncMock()
        outer = AsyncMock()
        outer.__aenter__.return_value = session
        monkeypatch.setattr(main, "CachedSession", MagicMock(return_value=outer))
        return session

    return configure


def test_parser_preserves_source_urls_and_pairs_column_credits():
    raw = EMPTY_PAGE.decode().replace(
        "</table>",
        """<tr><td><img src='../Admin/PlantImages/Plant'One'.jpg'></td><td><img src='../Admin/PlantImages/Plant two é.jpg'></td><td><img src='../Admin/PlantImages/noplantimage2.JPG'></td></tr><tr><td>First credit</td><td>Second credit</td><td>Placeholder</td></tr></table>""",
    )
    assert main.parse_plant_images(main.parse_page_html(raw), "Example alba") == {
        "https://pfaf.org/Admin/PlantImages/Plant%27One%27.jpg": "First credit",
        "https://pfaf.org/Admin/PlantImages/Plant two é.jpg": "Second credit",
    }


@pytest.mark.parametrize(
    "body",
    [
        EMPTY_PAGE.replace(b"Example alba - ", b""),
        EMPTY_PAGE.replace(
            b'<span id="ContentPlaceHolder1_lblFamily">Family</span>', b""
        ),
        EMPTY_PAGE.replace(
            b'<table id="ContentPlaceHolder1_tblPlantImges"></table>', b""
        ),
        EMPTY_PAGE.replace(b"</table>", b""),
    ],
)
def test_parser_rejects_incomplete_photo_pages(body):
    with pytest.raises(ValueError):
        main.parse_plant_images(main.parse_page_html(body), "Example alba")


@pytest.mark.parametrize("invalid", ["html", "png checksum"])
def test_invalid_download_is_isolated_and_not_published(archive, network, png, invalid):
    path, args = archive({"Example alba": [URL_A, URL_B]})
    damaged = bytearray(png)
    offset = damaged.index(b"IDAT")
    length = int.from_bytes(damaged[offset - 4 : offset], "big")
    damaged[offset + 4 + length] ^= 1
    session = network(
        [b"<html>upstream error</html>" if invalid == "html" else bytes(damaged), png]
    )
    with pytest.raises(SystemExit) as failure:
        main.main(args + ["load-images", "--concurrency", "1"])
    assert failure.value.code == 1 and session.request.call_count == 2
    with sqlite3.connect(path) as con:
        rows = con.execute(
            "SELECT path,status,sha256,error FROM image_downloads ORDER BY url"
        ).fetchall()
        assert rows[0][1] == "error" and rows[0][2] is None and rows[0][3]
        assert not (path.parent / rows[0][0]).exists()
        assert rows[1][1:] == ("ok", hashlib.sha256(png).hexdigest(), None)
        assert (path.parent / rows[1][0]).read_bytes() == png
        images = json.loads(con.execute("SELECT images FROM plant_data").fetchone()[0])
        assert images == [{"url": URL_B, "source": "Credit", "path": rows[1][0]}]


@pytest.mark.parametrize(
    "damage", ["different valid bytes", "corrupt with matching hash"]
)
def test_reuse_requires_hash_and_decode_and_force_redownloads(
    archive, network, png, damage
):
    path, args = archive({"Example alba": [URL_A]})
    with sqlite3.connect(path) as con:
        target = (
            path.parent / con.execute("SELECT path FROM image_downloads").fetchone()[0]
        )
    target.parent.mkdir(parents=True)
    target.write_bytes(b"unverified existing file")
    session = network([png])
    main.main(args + ["load-images"])
    assert session.request.call_count == 1
    session = network([])
    main.main(args + ["load-images"])
    assert session.request.call_count == 0
    damaged = png + b"changed" if damage == "different valid bytes" else png[:20]
    target.write_bytes(damaged)
    if damage == "corrupt with matching hash":
        with sqlite3.connect(path) as con:
            con.execute(
                "UPDATE image_downloads SET sha256=?",
                (hashlib.sha256(damaged).hexdigest(),),
            )
    session = network([png])
    main.main(args + ["load-images"])
    assert session.request.call_count == 1 and target.read_bytes() == png
    session = network([png])
    main.main(args + ["load-images", "--force"])
    assert session.request.call_count == 1


@pytest.mark.parametrize(
    "error,message",
    [
        (asyncio.TimeoutError(), "TimeoutError"),
        (ClientConnectionError("connection reset"), "connection reset"),
    ],
)
def test_network_failure_is_isolated_and_payload_error_retries(
    archive, network, png, monkeypatch, capsys, error, message
):
    path, args = archive({"Example alba": [URL_A, URL_B]})
    session = network([error, error, error, ClientPayloadError("truncated"), png])
    sleep = AsyncMock()
    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    with pytest.raises(SystemExit) as failure:
        main.main(args + ["load-images", "--concurrency", "1"])
    assert failure.value.code == 1 and session.request.call_count == 5
    assert sleep.await_args_list == [call(1), call(2), call(1)]
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT status,error FROM image_downloads ORDER BY url"
        ).fetchall() == [("error", message), ("ok", None)]
    assert (
        f"Failed image https://example.test/a.png: {message}" in capsys.readouterr().out
    )


def test_photo_refresh_preserves_details_and_resumes_failures(
    archive, network, monkeypatch
):
    path, args = archive({"Example alba": [URL_A], "Example beta": []})
    with sqlite3.connect(path) as con:
        con.execute("UPDATE image_downloads SET status='ok'")
        details = con.execute(
            "SELECT summary,family,details_scraped_at FROM plants ORDER BY latin_name"
        ).fetchall()
    session = network(
        [asyncio.TimeoutError()] * 3 + [EMPTY_PAGE.replace(b"alba", b"beta")]
    )
    monkeypatch.setattr(main.asyncio, "sleep", AsyncMock())
    with pytest.raises(SystemExit) as failure:
        main.main(args + ["refresh-images", "--force", "--concurrency", "1"])
    assert failure.value.code == 1 and session.request.call_count == 4
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT images_error FROM plants ORDER BY latin_name"
        ).fetchall() == [("TimeoutError",), (None,)]
        assert con.execute("SELECT url FROM plant_images").fetchall() == [(URL_A,)]
        assert con.execute("SELECT images FROM plant_data").fetchall() == [("[]",)] * 2
    session = network([])
    main.main(args + ["load-images", "--force"])
    assert session.request.call_count == 0
    session = network([EMPTY_PAGE])
    main.main(args + ["refresh-images"])
    assert session.request.call_count == 1
    session = network([])
    main.main(args + ["refresh-images"])
    assert session.request.call_count == 0
    with sqlite3.connect(path) as con:
        assert con.execute("SELECT count(*) FROM plant_images").fetchone()[0] == 0
        assert (
            con.execute("SELECT images_error FROM plants").fetchall() == [(None,)] * 2
        )
        assert (
            con.execute(
                "SELECT summary,family,details_scraped_at FROM plants ORDER BY latin_name"
            ).fetchall()
            == details
        )


@pytest.mark.parametrize(
    "error,status,exception",
    [
        (OSError("disk interrupted"), "error", SystemExit),
        (asyncio.CancelledError(), "pending", asyncio.CancelledError),
    ],
)
def test_interrupted_replace_preserves_file_and_resumes(
    archive, network, png, monkeypatch, error, status, exception
):
    path, args = archive({"Example alba": [URL_A]})
    network([png])
    main.main(args + ["load-images"])
    with sqlite3.connect(path) as con:
        target = (
            path.parent / con.execute("SELECT path FROM image_downloads").fetchone()[0]
        )
    network([png + b"new bytes"])
    with monkeypatch.context() as patch:
        patch.setattr(Path, "replace", MagicMock(side_effect=error))
        with pytest.raises(exception) as failure:
            main.main(args + ["load-images", "--force"])
    if exception is SystemExit:
        assert failure.value.code == 1
    assert target.read_bytes() == png and not list(target.parent.glob(".*.tmp"))
    with sqlite3.connect(path) as con:
        state, digest, message = con.execute(
            "SELECT status,sha256,error FROM image_downloads"
        ).fetchone()
        assert state == status and digest == hashlib.sha256(png).hexdigest()
        assert message == ("disk interrupted" if status == "error" else None)
        assert con.execute("SELECT images FROM plant_data").fetchone()[0] == "[]"
    session = network([png])
    main.main(args + ["load-images"])
    assert session.request.call_count == 1


def test_paths_stay_stable_and_shared_urls_download_once(archive, network, png):
    first, second = (
        "https://example.test/first/photo.png",
        "https://example.test/second/photo.png",
    )
    path, args = archive({"Example alba": [first, second], "Example beta": [first]})
    with sqlite3.connect(path) as con:
        con.execute(
            "UPDATE image_downloads SET path='images/current.png' WHERE url=?", (first,)
        )
        con.execute(
            "UPDATE plant_images SET path='images/current.png' WHERE url=?", (first,)
        )
        main.record_plant_images(
            con,
            "Example alba",
            {first: "First credit", second: "Second credit"},
            CHECKED_AT,
        )
    session = network([png, png + b"distinct bytes"])
    main.main(args + ["load-images"])
    assert session.request.call_count == 2
    with sqlite3.connect(path) as con:
        rows = con.execute("SELECT path FROM image_downloads ORDER BY url").fetchall()
        assert rows[0][0] == "images/current.png"
        assert rows[1][0].startswith("images/by-url/") and rows[1][0].endswith(".png")
        assert (path.parent / rows[0][0]).read_bytes() == png
        assert (path.parent / rows[1][0]).read_bytes() == png + b"distinct bytes"
        images = [
            json.loads(row[0])
            for row in con.execute("SELECT images FROM plant_data ORDER BY latin_name")
        ]
        assert len(images[0]) == 2 and len(images[1]) == 1
        assert (
            images[0][0]["source"] == "First credit"
            and images[1][0]["source"] == "Credit"
        )
        con.execute("UPDATE plants SET images_scraped_at=NULL")
        assert con.execute("SELECT images FROM plant_data").fetchall() == [("[]",)] * 2


def test_full_pixel_decode_rejects_jpeg_that_passes_header_verification():
    output = BytesIO()
    Image.new("RGB", (20, 20), "green").save(output, format="JPEG")
    truncated = output.getvalue()[:-5]
    with Image.open(BytesIO(truncated)) as image:
        image.verify()
    with pytest.raises(ValueError, match="Invalid image"):
        main.validate_image(truncated)


def test_worker_pool_is_bounded_and_drains_on_unexpected_failure():
    async def check():
        active, peak, finished = 0, 0, []

        async def handle(item):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.001)
                if item == 3:
                    raise ValueError("Unexpected database failure")
                finished.append(item)
            finally:
                active -= 1

        with pytest.raises(ValueError, match="Unexpected database failure"):
            await main.run_workers(list(range(12)), handle, 3, "Testing workers")
        assert peak == 3 and active == 0 and 0 < len(finished) < 12

    asyncio.run(check())


@pytest.mark.parametrize("command", ["refresh-images", "load-images"])
def test_photo_commands_reject_nonpositive_concurrency_before_writing(
    command, tmp_path
):
    with pytest.raises(SystemExit) as failure:
        main.main(
            ["--database", str(tmp_path / "new.sqlite"), command, "--concurrency", "0"]
        )
    assert failure.value.code == 2 and not (tmp_path / "new.sqlite").exists()
