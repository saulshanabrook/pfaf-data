"""Check the committed payload used by the existing main:/ Pages deployment."""

import asyncio
import json
import sqlite3
from pathlib import Path

from datasette.app import Datasette

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
