"""Image-only refresh, URL identity, validated download, and resume regressions."""

import asyncio
import hashlib
import sqlite3
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from aiohttp import ClientConnectionError, ClientPayloadError
from PIL import Image

import main


@pytest.fixture
def png():
    output = BytesIO()
    Image.new("RGB", (3, 2), "green").save(output, format="PNG")
    return output.getvalue()


@pytest.fixture
def archive(tmp_path):
    def seed(urls):
        path = tmp_path / "data.sqlite"
        with sqlite3.connect(path) as con:
            main.create_tables(con)
            row = dict(
                zip(
                    main.SEARCH_COLUMNS,
                    [
                        "Example alba",
                        "Example",
                        "Tree",
                        3,
                        "4-9",
                        "M",
                        "LM",
                        "N",
                        "M",
                        1,
                        1,
                        1,
                    ],
                )
            )
            main.record_plant_row(con, row)
            con.execute(
                "UPDATE plants SET summary='Keep description', family='Keep family', details_scraped_at='2026-01-01T00:00:00+00:00', images_scraped_at='2026-01-01T00:00:00+00:00'"
            )
            con.executemany(
                "INSERT INTO plant_images(plant,url,path) VALUES(?,?,?)",
                [
                    ("Example alba", url, f"images/legacy-{number}.png")
                    for number, url in enumerate(urls)
                ],
            )
        return path

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
        session.request.side_effect = list(responses)
        session.get.side_effect = list(responses)
        session.disabled.return_value = AsyncMock()
        outer = AsyncMock()
        outer.__aenter__.return_value = session
        monkeypatch.setattr(main, "CachedSession", MagicMock(return_value=outer))
        return session

    return configure


def test_html_body_fails_but_later_image_commits(archive, network, png, tmp_path):
    path = archive(["https://example.test/a.png", "https://example.test/b.png"])
    network([b"<html>upstream error</html>", png])
    with pytest.raises(SystemExit) as failure:
        main.main(
            [
                "--database",
                str(path),
                "--cache",
                str(tmp_path / "cache.sqlite"),
                "load-images",
            ]
        )
    assert failure.value.code == 1
    with sqlite3.connect(path) as con:
        rows = con.execute(
            "SELECT url,status,sha256,error FROM image_downloads ORDER BY url"
        ).fetchall()
        assert rows[0][1] == "error" and rows[0][2] is None and rows[0][3]
        assert rows[1][1:3] == ("ok", hashlib.sha256(png).hexdigest())
        target = con.execute(
            "SELECT path FROM image_downloads WHERE status='ok'"
        ).fetchone()[0]
    assert (tmp_path / target).read_bytes() == png


def test_existing_file_without_url_hash_is_downloaded_again(
    archive, network, png, tmp_path
):
    path = archive(["https://example.test/a.png"])
    legacy = tmp_path / "images/legacy-0.png"
    legacy.parent.mkdir()
    legacy.write_bytes(b"wrong existing file")
    session = network([png])
    main.main(
        [
            "--database",
            str(path),
            "--cache",
            str(tmp_path / "cache.sqlite"),
            "load-images",
        ]
    )
    assert session.request.call_count + session.get.call_count == 1
    with sqlite3.connect(path) as con:
        target, sha = con.execute("SELECT path,sha256 FROM image_downloads").fetchone()
    assert sha == hashlib.sha256(png).hexdigest()
    assert (tmp_path / target).read_bytes() == png


def test_photo_refresh_keeps_descriptions_and_checkpoints_empty_page(
    archive, network, tmp_path
):
    path = archive(["https://example.test/old.png"])
    html = b'<h1><span id="ContentPlaceHolder1_lbldisplatinname">Example alba - Author</span></h1><span id="ContentPlaceHolder1_lblFamily">Changed family</span><table id="ContentPlaceHolder1_tblPlantImges"></table>'
    network([html])
    main.main(
        [
            "--database",
            str(path),
            "--cache",
            str(tmp_path / "cache.sqlite"),
            "refresh-images",
            "--force",
        ]
    )
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT summary,family,details_scraped_at,images_error FROM plants"
        ).fetchone() == (
            "Keep description",
            "Keep family",
            "2026-01-01T00:00:00+00:00",
            None,
        )
        assert con.execute("SELECT images_scraped_at FROM plants").fetchone()[0]
        assert con.execute("SELECT count(*) FROM plant_images").fetchone()[0] == 0


def test_raw_apostrophes_and_side_by_side_credits_are_preserved():
    raw = """<span id="ContentPlaceHolder1_lbldisplatinname">Example alba - Author</span>
<span id="ContentPlaceHolder1_lblFamily">Family</span>
<table id="ContentPlaceHolder1_tblPlantImges"><tr>
<td><img src='../Admin/PlantImages/Rhododendron'PJM'.jpg' alt='Example' title='Example'></td>
<td><img src='../Admin/PlantImages/Aloe_perryi_Curtis's_Botanical_Magazine.jpg' /></td>
</tr><tr><td>First photographer</td><td>Second photographer</td></tr>
<tr><td><img src='../Admin/PlantImages/noplantimage2.JPG'></td></tr></table>"""
    images = main.parse_plant_images(main.parse_page_html(raw), "Example alba")
    assert images == {
        "https://pfaf.org/Admin/PlantImages/Rhododendron%27PJM%27.jpg": "First photographer",
        "https://pfaf.org/Admin/PlantImages/Aloe_perryi_Curtis%27s_Botanical_Magazine.jpg": "Second photographer",
    }


def test_missing_or_truncated_photo_table_is_not_an_empty_photo_result():
    raw = '<span id="ContentPlaceHolder1_lbldisplatinname">Example alba</span><span id="ContentPlaceHolder1_lblFamily">Family</span>'
    with pytest.raises(ValueError, match="photo table is missing"):
        main.parse_plant_images(main.parse_page_html(raw), "Example alba")
    with pytest.raises(ValueError, match="photo table is truncated"):
        main.parse_page_html(
            raw
            + '<table id="ContentPlaceHolder1_tblPlantImges"><tr><td><img src="a.jpg">'
        )


def test_photo_refresh_failure_preserves_links_and_can_resume(
    archive, network, tmp_path
):
    path = archive(["https://example.test/old.png"])
    network([b"<html>Malformed page</html>"])
    with pytest.raises(SystemExit) as failure:
        main.main(
            [
                "--database",
                str(path),
                "--cache",
                str(tmp_path / "cache.sqlite"),
                "refresh-images",
                "--force",
            ]
        )
    assert failure.value.code == 1
    with sqlite3.connect(path) as con:
        assert con.execute("SELECT images_error FROM plants").fetchone()[0]
        assert (
            con.execute("SELECT url FROM plant_images").fetchone()[0]
            == "https://example.test/old.png"
        )
        assert (
            con.execute("SELECT images_scraped_at FROM plants").fetchone()[0]
            == "2026-01-01T00:00:00+00:00"
        )
    html = b'<span id="ContentPlaceHolder1_lbldisplatinname">Example alba</span><span id="ContentPlaceHolder1_lblFamily">Family</span><table id="ContentPlaceHolder1_tblPlantImges"></table>'
    network([html])
    main.main(
        [
            "--database",
            str(path),
            "--cache",
            str(tmp_path / "cache.sqlite"),
            "refresh-images",
        ]
    )
    session = network([])
    main.main(
        [
            "--database",
            str(path),
            "--cache",
            str(tmp_path / "cache.sqlite"),
            "refresh-images",
        ]
    )
    assert session.request.call_count == 0
    with sqlite3.connect(path) as con:
        assert con.execute("SELECT images_error FROM plants").fetchone()[0] is None
        assert con.execute("SELECT count(*) FROM plant_images").fetchone()[0] == 0


def test_image_paths_are_portable_and_distinct_even_with_same_basename():
    urls = [
        "https://example.test/a/photo.jpg",
        "https://example.test/b/photo.jpg",
        "https://example.test/a/photo.jpg?size=2",
        "https://example.test/../../bad.EXE",
    ]
    paths = [main.image_path(url) for url in urls]
    assert len(set(paths)) == len(urls)
    for url, path in zip(urls, paths):
        assert path.startswith("images/" + hashlib.sha256(url.encode()).hexdigest())
        assert "\\" not in path and ".." not in path
    assert paths[-1].endswith(".img")


@pytest.mark.parametrize(
    "damage", ["empty", "other_valid_image", "missing", "truncated"]
)
def test_url_hash_and_full_decode_are_required_for_reuse(
    archive, network, png, tmp_path, damage
):
    path = archive(["https://example.test/a.png"])
    network([png])
    command = [
        "--database",
        str(path),
        "--cache",
        str(tmp_path / "cache.sqlite"),
        "load-images",
    ]
    main.main(command)
    session = network([])
    main.main(command)
    assert session.request.call_count == 0
    with sqlite3.connect(path) as con:
        target = (
            tmp_path / con.execute("SELECT path FROM image_downloads").fetchone()[0]
        )
    if damage == "missing":
        target.unlink()
    elif damage == "other_valid_image":
        output = BytesIO()
        Image.new("RGB", (3, 2), "red").save(output, format="PNG")
        target.write_bytes(output.getvalue())
    else:
        target.write_bytes(b"" if damage == "empty" else png[:40])
    session = network([png])
    main.main(command)
    assert session.request.call_count == 1
    assert target.read_bytes() == png
    session = network([png])
    main.main(command + ["--force"])
    assert session.request.call_count == 1


def test_payload_retry_and_exhausted_connection_failure_do_not_stop_other_images(
    archive, network, png, tmp_path, monkeypatch
):
    path = archive(["https://example.test/a.png", "https://example.test/b.png"])
    # First URL exhausts three attempts; second recovers from an incomplete body.
    session = network(
        [ClientConnectionError("reset")] * 3 + [ClientPayloadError("incomplete"), png]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    with pytest.raises(SystemExit) as failure:
        main.main(
            [
                "--database",
                str(path),
                "--cache",
                str(tmp_path / "cache.sqlite"),
                "load-images",
                "--concurrency",
                "1",
            ]
        )
    assert failure.value.code == 1
    assert session.request.call_count == 5
    assert sleep.await_args_list == [call(1), call(2), call(1)]
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT status FROM image_downloads ORDER BY url"
        ).fetchall() == [("error",), ("ok",)]
        assert con.execute(
            "SELECT attempted_at,error FROM image_downloads WHERE status='error'"
        ).fetchone()[0]


def test_atomic_replacement_keeps_previous_valid_file_and_cleans_temp(
    archive, network, png, tmp_path, monkeypatch
):
    path = archive(["https://example.test/a.png"])
    command = [
        "--database",
        str(path),
        "--cache",
        str(tmp_path / "cache.sqlite"),
        "load-images",
    ]
    network([png])
    main.main(command)
    with sqlite3.connect(path) as con:
        target = (
            tmp_path / con.execute("SELECT path FROM image_downloads").fetchone()[0]
        )
    network([png])
    with monkeypatch.context() as patch:
        patch.setattr(
            Path, "replace", MagicMock(side_effect=OSError("disk interrupted"))
        )
        with pytest.raises(SystemExit) as failure:
            main.main(command + ["--force"])
    assert failure.value.code == 1
    assert target.read_bytes() == png
    assert not list(target.parent.glob("*.tmp"))
    with sqlite3.connect(path) as con:
        status, sha, error = con.execute(
            "SELECT status,sha256,error FROM image_downloads"
        ).fetchone()
        assert (
            status == "error"
            and sha == hashlib.sha256(png).hexdigest()
            and "disk interrupted" in error
        )
    session = network([png])
    main.main(command)
    assert session.request.call_count == 1


def test_interrupted_download_is_pending_until_successful_retry(
    archive, network, png, tmp_path
):
    path = archive(["https://example.test/a.png"])
    command = [
        "--database",
        str(path),
        "--cache",
        str(tmp_path / "cache.sqlite"),
        "load-images",
    ]
    network([asyncio.CancelledError()])
    with pytest.raises(asyncio.CancelledError):
        main.main(command)
    with sqlite3.connect(path) as con:
        assert con.execute("SELECT status,sha256 FROM image_downloads").fetchone() == (
            "pending",
            None,
        )
        assert con.execute("SELECT images FROM plant_data").fetchone()[0] == "[]"
    session = network([png])
    main.main(command)
    assert session.request.call_count == 1


def test_public_view_requires_fresh_metadata_and_verified_ledger(
    archive, network, png, tmp_path
):
    path = archive(["https://example.test/a.png"])
    with sqlite3.connect(path) as con:
        assert con.execute("SELECT images FROM plant_data").fetchone()[0] == "[]"
    network([png])
    command = [
        "--database",
        str(path),
        "--cache",
        str(tmp_path / "cache.sqlite"),
        "load-images",
    ]
    main.main(command)
    with sqlite3.connect(path) as con:
        import json

        image = json.loads(con.execute("SELECT images FROM plant_data").fetchone()[0])[
            0
        ]
        assert (
            image["url"] == "https://example.test/a.png"
            and image["path"] == "images/a.png"
        )
        con.execute("UPDATE plants SET images_error='Source unavailable'")
        assert con.execute("SELECT images FROM plant_data").fetchone()[0] == "[]"
    session = network([])
    main.main(command + ["--force"])
    assert session.request.call_count == 0
    with sqlite3.connect(path) as con:
        con.execute("UPDATE plants SET images_error=NULL,images_scraped_at=NULL")
        assert con.execute("SELECT images FROM plant_data").fetchone()[0] == "[]"


def test_worker_pool_is_bounded_and_cancels_on_unexpected_failure():
    async def check():
        active, peak, finished = 0, 0, []

        async def handle(item):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.001)
            finished.append(item)
            active -= 1

        await main.run_workers(list(range(12)), handle, 3, "Testing workers")
        assert peak == 3 and sorted(finished) == list(range(12))
        cancelled = []

        async def fail(item):
            if item == 1:
                raise ValueError("Unexpected database failure")
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cancelled.append(item)
                raise

        with pytest.raises(ValueError, match="Unexpected database failure"):
            await main.run_workers([0, 1, 2], fail, 3, "Testing cancellation")
        assert sorted(cancelled) == [0, 2]

    asyncio.run(check())


def test_photo_commands_reject_nonpositive_concurrency_before_writing(tmp_path):
    for command in ["refresh-images", "load-images"]:
        with pytest.raises(SystemExit) as failure:
            main.main(
                [
                    "--database",
                    str(tmp_path / "new.sqlite"),
                    command,
                    "--concurrency",
                    "0",
                ]
            )
        assert failure.value.code == 2
    assert not (tmp_path / "new.sqlite").exists()


def test_full_pixel_decode_rejects_jpeg_that_passes_header_verification():
    output = BytesIO()
    Image.new("RGB", (20, 20), "green").save(output, format="JPEG")
    truncated = output.getvalue()[:-5]
    with Image.open(BytesIO(truncated)) as image:
        image.verify()
    with pytest.raises(ValueError, match="Invalid image"):
        main.validate_image(truncated)


def test_shared_url_downloads_once_and_filename_collisions_never_overwrite(
    archive, network, png, tmp_path
):
    first = "https://example.test/first/photo.png"
    second = "https://example.test/second/photo.png"
    path = archive([first, second])
    with sqlite3.connect(path) as con:
        row = dict(
            zip(
                main.SEARCH_COLUMNS,
                [
                    "Example beta",
                    "Other",
                    "Tree",
                    2,
                    "4-9",
                    "M",
                    "LM",
                    "N",
                    "M",
                    1,
                    1,
                    1,
                ],
            )
        )
        main.record_plant_row(con, row)
        main.record_plant_images(
            con,
            "Example beta",
            {first: "Second credit"},
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
    output = BytesIO()
    Image.new("RGB", (3, 2), "red").save(output, format="PNG")
    red = output.getvalue()
    session = network([png, red])
    main.main(
        [
            "--database",
            str(path),
            "--cache",
            str(tmp_path / "cache.sqlite"),
            "load-images",
        ]
    )
    assert session.request.call_count == 2
    with sqlite3.connect(path) as con:
        records = con.execute(
            "SELECT url,path,sha256 FROM image_downloads ORDER BY url"
        ).fetchall()
        assert len(records) == 2
        assert records[0][1] != records[1][1]
        assert (tmp_path / records[0][1]).read_bytes() == png
        assert (tmp_path / records[1][1]).read_bytes() == red
        assert con.execute("SELECT count(*) FROM plant_data").fetchone()[0] == 2
        assert con.execute("SELECT count(*) FROM plant_images").fetchone()[0] == 3


def test_photo_association_update_rolls_back_on_storage_failure(archive):
    path = archive(["https://example.test/old.png"])
    with sqlite3.connect(path) as con:
        old = con.execute("SELECT * FROM plant_images").fetchall()

        def deny_image_insert(action, table, *unused):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_INSERT and table == "plant_images"
                else sqlite3.SQLITE_OK
            )

        con.set_authorizer(deny_image_insert)
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            main.record_plant_images(
                con,
                "Example alba",
                {"https://example.test/new.png": "New credit"},
                datetime.now(timezone.utc),
            )
        con.set_authorizer(None)
        assert con.execute("SELECT * FROM plant_images").fetchall() == old
        assert (
            con.execute("SELECT images_scraped_at FROM plants").fetchone()[0]
            == "2026-01-01T00:00:00+00:00"
        )
        assert (
            con.execute(
                "SELECT count(*) FROM image_downloads WHERE url='https://example.test/new.png'"
            ).fetchone()[0]
            == 0
        )


def test_bad_png_checksum_is_reported_without_cancelling_other_downloads(
    archive, network, png, tmp_path
):
    damaged = bytearray(png)
    offset = damaged.index(b"IDAT")
    length = int.from_bytes(damaged[offset - 4 : offset], "big")
    damaged[offset + 4 + length] ^= 1
    path = archive(["https://example.test/a.png", "https://example.test/b.png"])
    session = network([bytes(damaged), png])
    with pytest.raises(SystemExit) as failure:
        main.main(
            [
                "--database",
                str(path),
                "--cache",
                str(tmp_path / "cache.sqlite"),
                "load-images",
            ]
        )
    assert failure.value.code == 1
    assert session.request.call_count == 2
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT status FROM image_downloads ORDER BY url"
        ).fetchall() == [("error",), ("ok",)]


def test_ledger_preserves_safe_names_and_allocates_collision_fallbacks():
    with sqlite3.connect(":memory:") as con:
        main.create_tables(con)
        first = "https://example.test/first/Photo.jpg"
        second = "https://example.test/second/photo.jpg"
        assert main.allocate_image_path(con, first) == "images/Photo.jpg"
        fallback = main.allocate_image_path(con, second)
        assert fallback == main.image_path(second)
        assert main.allocate_image_path(con, first) == "images/Photo.jpg"
        assert main.allocate_image_path(con, second) == fallback
        # NFC normalization protects filesystems that normalize Unicode names.
        composed = "https://example.test/\u00e9.jpg"
        decomposed = "https://example.test/e\u0301.jpg"
        assert main.allocate_image_path(con, composed) == "images/\u00e9.jpg"
        assert main.allocate_image_path(con, decomposed) == main.image_path(decomposed)
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "INSERT INTO image_downloads(url,path,path_key) VALUES(?,?,?)",
                ("other", "images/other.jpg", "images/photo.jpg"),
            )


@pytest.mark.parametrize(
    "name",
    [
        "CON.jpg",
        "aux.png",
        "bad%5Cname.jpg",
        "trailing.",
        "trailing%20",
        "bad%3Aname.png",
        "x" * 201 + ".jpg",
    ],
)
def test_unsafe_source_names_use_safe_url_hash_fallback(name):
    url = "https://example.test/" + name
    with sqlite3.connect(":memory:") as con:
        main.create_tables(con)
        assert main.allocate_image_path(con, url) == main.image_path(url)


def test_existing_ledger_path_and_status_survive_schema_upgrade(archive):
    path = archive(["https://example.test/a.png"])
    with sqlite3.connect(path) as con:
        con.execute("DROP TABLE image_downloads")
        # Reproduce the initial full-URL-hash checkpoint without path_key.
        definitions = ", ".join(
            f"{name} {kind}"
            for name, kind in main.TABLES["image_downloads"].items()
            if name != "path_key"
        )
        con.execute(f"CREATE TABLE image_downloads ({definitions})")
        url = "https://example.test/a.png"
        old_path = main.image_path(url)
        con.execute(
            "INSERT INTO image_downloads(url,path,status,sha256) VALUES(?,?,'ok','known-source-hash')",
            (url, old_path),
        )
        main.create_tables(con)
        assert con.execute(
            "SELECT path,status,sha256,path_key FROM image_downloads"
        ).fetchone() == (old_path, "ok", "known-source-hash", old_path.casefold())
        assert con.execute("SELECT path FROM plant_images").fetchone()[0] == old_path
        assert main.allocate_image_path(con, url) == old_path


def test_photo_parser_preserves_existing_url_identity_for_spaces_and_unicode():
    raw = """<span id="ContentPlaceHolder1_lbldisplatinname">Example alba</span><span id="ContentPlaceHolder1_lblFamily">Family</span><table id="ContentPlaceHolder1_tblPlantImges"><tr><td><img src='../Admin/PlantImages/Example alba \u00e9.jpg'></td></tr></table>"""
    assert list(main.parse_plant_images(main.parse_page_html(raw), "Example alba")) == [
        "https://pfaf.org/Admin/PlantImages/Example alba \u00e9.jpg"
    ]


@pytest.mark.parametrize(
    "name", ["Ericameria nauseosa ssp. consimilis", "Ericameria nauseosa ssp. nauseosa"]
)
def test_photo_page_accepts_empty_author_without_accepting_wrong_or_empty_title(name):
    raw = f'<span id="ContentPlaceHolder1_lbldisplatinname">{name} - </span><span id="ContentPlaceHolder1_lblFamily">Asteraceae</span><table id="ContentPlaceHolder1_tblPlantImges"></table>'
    soup = main.parse_page_html(raw)
    assert main.parse_plant_images(soup, name) == {}
    for title in ["Other plant -", ""]:
        soup.find(id="ContentPlaceHolder1_lbldisplatinname").string = title
        with pytest.raises(ValueError, match="returned title"):
            main.parse_plant_images(soup, name)


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (asyncio.TimeoutError(), "TimeoutError"),
        (ClientConnectionError(), "ClientConnectionError"),
        (ClientConnectionError("connection reset"), "connection reset"),
    ],
)
def test_bare_network_image_failure_has_message_and_later_download_succeeds(
    archive, network, png, tmp_path, monkeypatch, capsys, error, message
):
    path = archive(["https://example.test/a.png", "https://example.test/b.png"])
    session = network([error, error, error, png])
    sleep = AsyncMock()
    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    with pytest.raises(SystemExit) as failure:
        main.main(
            [
                "--database",
                str(path),
                "--cache",
                str(tmp_path / "cache.sqlite"),
                "load-images",
                "--concurrency",
                "1",
            ]
        )
    assert failure.value.code == 1
    assert session.request.call_count == 4
    assert sleep.await_args_list == [call(1), call(2)]
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT status,error FROM image_downloads ORDER BY url"
        ).fetchall() == [("error", message), ("ok", None)]
    assert (
        f"Failed image https://example.test/a.png: {message}" in capsys.readouterr().out
    )


@pytest.mark.parametrize(
    ("error", "message"),
    [
        (asyncio.TimeoutError(), "TimeoutError"),
        (ClientConnectionError(), "ClientConnectionError"),
        (ClientConnectionError("connection reset"), "connection reset"),
    ],
)
def test_bare_network_metadata_failure_has_message_and_later_plant_succeeds(
    archive, network, tmp_path, monkeypatch, capsys, error, message
):
    path = archive(["https://example.test/old.png"])
    with sqlite3.connect(path) as con:
        row = dict(
            zip(
                main.SEARCH_COLUMNS,
                [
                    "Example beta",
                    "Other",
                    "Tree",
                    2,
                    "4-9",
                    "M",
                    "LM",
                    "N",
                    "M",
                    1,
                    1,
                    1,
                ],
            )
        )
        main.record_plant_row(con, row)
    html = b'<span id="ContentPlaceHolder1_lbldisplatinname">Example beta</span><span id="ContentPlaceHolder1_lblFamily">Family</span><table id="ContentPlaceHolder1_tblPlantImges"></table>'
    session = network([error, error, error, html])
    sleep = AsyncMock()
    monkeypatch.setattr(main.asyncio, "sleep", sleep)
    with pytest.raises(SystemExit) as failure:
        main.main(
            [
                "--database",
                str(path),
                "--cache",
                str(tmp_path / "cache.sqlite"),
                "refresh-images",
                "--force",
                "--concurrency",
                "1",
            ]
        )
    assert failure.value.code == 1
    assert session.request.call_count == 4
    assert sleep.await_args_list == [call(1), call(2)]
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT images_error FROM plants ORDER BY latin_name"
        ).fetchall() == [(message,), (None,)]
        assert con.execute(
            "SELECT images_scraped_at FROM plants WHERE latin_name='Example beta'"
        ).fetchone()[0]
        assert (
            con.execute("SELECT url FROM plant_images").fetchone()[0]
            == "https://example.test/old.png"
        )
    assert f"Failed Example alba: {message}" in capsys.readouterr().out
