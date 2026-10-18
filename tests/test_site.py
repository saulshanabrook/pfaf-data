"""Check the committed payload used by the existing main:/ Pages deployment."""

import asyncio
import hashlib
import json
import sqlite3
import unicodedata
from pathlib import Path

from datasette.app import Datasette
from PIL import Image, ImageSequence

ROOT = Path(__file__).resolve().parents[1]


def test_published_database():
    with sqlite3.connect((ROOT / "data.sqlite").as_uri() + "?mode=ro", uri=True) as con:
        if hasattr(con, "setconfig"):
            con.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL, False)
            con.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, False)
        assert con.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert con.execute("PRAGMA foreign_key_check").fetchall() == []
        count = con.execute("SELECT count(*) FROM plants").fetchone()[0]
        assert count >= 8796
        assert con.execute("SELECT count(*) FROM plant_data").fetchone()[0] == count
        cols = {row[1] for row in con.execute("PRAGMA table_info(plant_data)")}
        assert {"native_range", "source_url", "details_scraped_at", "cares"} <= cols
        assert (ROOT / ".nojekyll").is_file()
        assert "metadata=" in (ROOT / "index.html").read_text()


def test_datasette_browse_and_json():
    async def check():
        ds = Datasette(
            immutables=[str(ROOT / "data.sqlite")],
            metadata=json.loads((ROOT / "metadata.json").read_text()),
        )
        for path in ("/", "/data", "/data/plants", "/data/plant_data"):
            response = await ds.client.get(path)
            assert response.status_code == 200, path
        response = await ds.client.get("/data/plant_data.json?_size=1&_shape=objects")
        assert response.status_code == 200
        row = response.json()["rows"][0]
        assert {
            "latin_name",
            "native_range",
            "details_scraped_at",
            "cares",
        } <= row.keys()
        assert isinstance(json.loads(row["cares"]), list)
        response = await ds.client.get("/-/plugins.json")
        plugins = {plugin["name"] for plugin in response.json()}
        assert {"datasette-gzip", "datasette-render-images"} <= plugins

    asyncio.run(check())


def test_published_photo_archive():
    with sqlite3.connect((ROOT / "data.sqlite").as_uri() + "?mode=ro", uri=True) as con:
        assert (
            con.execute(
                "SELECT count(*) FROM plants "
                "WHERE images_scraped_at IS NULL AND images_error IS NULL"
            ).fetchone()[0]
            == 0
        )
        rows = con.execute(
            "SELECT DISTINCT i.url, i.path, d.path, d.status, d.sha256, "
            "d.byte_size, d.fetched_at, d.attempted_at, d.error, d.path_key "
            "FROM plant_images i JOIN plants p ON p.latin_name=i.plant "
            "LEFT JOIN image_downloads d ON d.url=i.url "
            "WHERE p.images_scraped_at IS NOT NULL AND p.images_error IS NULL"
        ).fetchall()
        assert rows
        paths = {}
        for (
            url,
            path,
            asset_path,
            status,
            digest,
            size,
            fetched,
            attempted,
            error,
            path_key,
        ) in rows:
            assert path == asset_path
            assert path_key == unicodedata.normalize("NFC", path).casefold()
            assert paths.setdefault(path_key, url) == url
            photo = ROOT / path
            assert photo.resolve().is_relative_to((ROOT / "images").resolve()), url
            assert status in {"ok", "error"}, url
            assert attempted, url
            if status == "error":
                assert error, url
                continue
            assert error is None and fetched, url
            body = photo.read_bytes()
            assert len(body) == size, url
            assert hashlib.sha256(body).hexdigest() == digest, url
            with Image.open(photo) as image:
                image.verify()
            with Image.open(photo) as image:
                for frame in ImageSequence.Iterator(image):
                    frame.load()
