"""Offline regressions for website drift, safe CLI behavior, and legacy migration."""

import asyncio
import json
import os
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from aiohttp import ClientConnectionError, ClientResponseError
from bs4 import BeautifulSoup

import main

FIXTURES = Path(__file__).parent / "fixtures"


def search_html(names=("Example alba", "Example beta"), state="first", pager=""):
    header = "".join(f"<th>{name}</th>" for name in main.SEARCH_HEADINGS)
    rows = []
    for name in names:
        values = [
            f'<a href="Plant.aspx?LatinName={name}">{name}</a>',
            "Example plant",
            "Tree",
            "3.5",
            "4-9",
            "M",
            "LMH",
            "SN",
            "DM",
            "4",
            "",
            "2",
        ]
        rows.append("<tr>" + "".join(f"<td>{value}</td>" for value in values) + "</tr>")
    return BeautifulSoup(
        f'<input type="hidden" name="__VIEWSTATE" value="{state}">'
        f'<input type="hidden" name="__EVENTVALIDATION" value="validation-{state}">'
        f'<table id="ContentPlaceHolder1_gvresults"><tr>{header}</tr>{"".join(rows)}{pager}</table>',
        "lxml",
    )


@pytest.fixture
def plant_page():
    return BeautifulSoup((FIXTURES / "plant.html").read_text(), "lxml")


@pytest.fixture
def con():
    database = sqlite3.connect(":memory:")
    database.execute("PRAGMA foreign_keys=ON")
    main.create_tables(database)
    yield database
    database.close()


def test_discovery_updates_all_search_fields_without_losing_details(con):
    plant, _ = main.parse_search_page(search_html())
    assert plant["height"] == 3.5
    assert plant["medicinal_rating"] is None
    main.record_plant_row(con, plant)
    con.execute("UPDATE plants SET family='Preserved family'")
    plant.update(common_name="Changed name", shade="N")
    main.record_plant_row(con, plant)
    assert con.execute("SELECT common_name, shade, family FROM plants").fetchone() == (
        "Changed name",
        "N",
        "Preserved family",
    )


def test_discovery_validates_headers():
    page = search_html()
    page.find("th").string = "Changed field"
    with pytest.raises(ValueError, match="Unexpected PFAF search columns"):
        main.parse_search_page(page)


def test_discovery_handles_no_results_and_ignores_pager_rows():
    assert main.parse_search_page(search_html(names=())) == []
    assert (
        len(
            main.parse_search_page(
                search_html(
                    pager='<tr><td colspan="12"><a href="javascript:next()">2</a></td></tr>'
                )
            )
        )
        == 2
    )


def test_detail_fields_nested_prose_and_current_native_range(plant_page):
    details = main.parse_plant_page(plant_page, "Example alba")
    assert details.values["family"] == "Exampleaceae"
    assert (
        details.values["native_range"]
        == details.values["found_in"]
        == "Example Island."
    )
    assert details.values["summary"] == "A nested summary."
    assert details.values["edibility_rating"] == 4
    assert (
        details.values["edible_uses"]
        == "Fruit with nested prose and an ordinary reference."
    )
    assert details.values["cultivation_details"] == "Grow in moist soil."
    assert details.values["expert_comment"] == "A useful comment."
    assert details.values["conservation_status"] == "Least Concern"
    assert (
        details.values["source_url"]
        == "https://pfaf.org/user/Plant.aspx?LatinName=Example+alba"
    )
    assert details.values["details_scraped_at"].endswith("+00:00")
    assert details.uses["edible parts"] == {"Fruit": "A fruit"}
    assert details.uses["edible uses"] == {"Tea": ""}
    assert details.uses["special uses"] == {"Food Forest": ""}
    assert details.uses["carbon farming"] == {}
    assert details.care == {"Full sun": "https://pfaf.org/images/sun.png"}
    assert details.images == {
        "https://pfaf.org/Admin/PlantImages/Example%20alba.jpg": "Example photographer"
    }


def test_optional_sections_can_be_absent(plant_page):
    for suffix in (
        "txtEdibleUses",
        "txtMediUses",
        "txtOtherUses",
        "txtSpecialUses",
        "txtCultivationDetails",
        "lblCarbFarm",
        "tblIcons",
    ):
        plant_page.find(id=f"ContentPlaceHolder1_{suffix}").decompose()
    plant_page.find(id="ContentPlaceHolder1_tblPlantImges").clear()
    details = main.parse_plant_page(plant_page, "Example alba")
    assert details.values["edible_uses"] == ""
    assert not details.care and not details.images
    assert all(not uses for uses in details.uses.values())


def test_legacy_found_in_is_not_assumed_to_be_native_range(plant_page):
    plant_page.find("h2", string="Native Range").string = "Found In"
    details = main.parse_plant_page(plant_page, "Example alba")
    assert details.values["native_range"] is None
    assert details.values["found_in"] == "Example Island."


def test_carbon_farming_list_when_present(plant_page):
    carbon = plant_page.find(id="ContentPlaceHolder1_lblCarbFarm")
    carbon.string = "Carbon Farming"
    carbon.parent.insert_after(
        BeautifulSoup(
            '<ul><li><a href="carbon.aspx">Food crop</a>: A <b>perennial</b> crop.</li></ul>',
            "lxml",
        ).ul
    )
    details = main.parse_plant_page(plant_page, "Example alba")
    assert details.uses["carbon farming"] == {"Food crop": "A perennial crop."}


def test_wrong_or_empty_plant_title_fails_closed(plant_page):
    with pytest.raises(
        ValueError,
        match="Requested 'Example'",
    ):
        main.parse_plant_page(plant_page, "Example")
    plant_page.find(id="ContentPlaceHolder1_lbldisplatinname").clear()
    with pytest.raises(ValueError, match="returned title ''"):
        main.parse_plant_page(plant_page, "Example alba")


def test_detail_refresh_is_atomic_and_relations_do_not_duplicate(con, plant_page):
    main.record_plant_row(con, main.parse_search_page(search_html())[0])
    details = main.parse_plant_page(plant_page, "Example alba")
    main.record_plant_details(con, "Example alba", details)
    main.record_plant_details(con, "Example alba", details)
    assert con.execute("SELECT count(*) FROM plant_images").fetchone()[0] == 1
    assert con.execute("SELECT path FROM plant_images").fetchone()[
        0
    ] == main.image_path("https://pfaf.org/Admin/PlantImages/Example%20alba.jpg")
    assert con.execute("SELECT count(*) FROM plant_uses").fetchone()[0] == 6
    previous = con.execute("SELECT summary FROM plants").fetchone()[0]
    details.values["summary"] = "Should roll back"
    details.uses["invalid category"] = {"Invalid": ""}
    with pytest.raises(sqlite3.IntegrityError):
        main.record_plant_details(con, "Example alba", details)
    assert con.execute("SELECT summary FROM plants").fetchone()[0] == previous
    assert con.execute("PRAGMA foreign_key_check").fetchall() == []


def test_additive_migration_preserves_legacy_rows_and_returns_empty_arrays():
    with sqlite3.connect(":memory:") as database:
        # Reproduce the committed schema before this migration, including data.
        columns = {
            name: kind
            for name, kind in main.TABLES["plants"].items()
            if name not in {"native_range", "source_url", "details_scraped_at"}
        }
        definitions = ", ".join(f"{name} {kind}" for name, kind in columns.items())
        database.execute(f"CREATE TABLE plants ({definitions})")
        main.record_plant_row(database, main.parse_search_page(search_html())[0])
        for _ in range(2):
            main.create_tables(database)
        row = database.execute(
            "SELECT latin_name, native_range, details_scraped_at, cares, images, uses FROM plant_data"
        ).fetchone()
        assert row[:3] == ("Example alba", None, None)
        assert [json.loads(value) for value in row[3:]] == [[], [], []]


def test_force_recreation_with_populated_foreign_keys(con, plant_page):
    main.record_plant_row(con, main.parse_search_page(search_html())[0])
    main.record_plant_details(
        con, "Example alba", main.parse_plant_page(plant_page, "Example alba")
    )
    main.create_tables(con, force=True)
    assert con.execute("SELECT count(*) FROM plants").fetchone()[0] == 0
    assert con.execute("PRAGMA foreign_key_check").fetchall() == []


def test_view_exposes_cares_without_multiplying_uses_or_images(con, plant_page):
    main.record_plant_row(con, main.parse_search_page(search_html())[0])
    details = main.parse_plant_page(plant_page, "Example alba")
    details.care["Moist soil"] = "https://pfaf.org/moist.png"
    main.record_plant_details(con, "Example alba", details)
    con.execute("UPDATE image_downloads SET status='ok'")
    cares, images, uses = con.execute(
        "SELECT cares, images, uses FROM plant_data"
    ).fetchone()
    assert set(json.loads(cares)) == {"Full sun", "Moist soil"}
    assert len(json.loads(images)) == 1 and len(json.loads(uses)) == 6


def test_no_arguments_only_prints_help(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    main.main([])
    assert "search-plants" in capsys.readouterr().out
    assert list(tmp_path.iterdir()) == []


def test_cli_creates_temporary_database_and_rejects_nonpositive_limit(tmp_path):
    path = tmp_path / "test.sqlite"
    main.main(["--database", str(path), "create-tables"])
    with sqlite3.connect(path) as database:
        assert database.execute("SELECT count(*) FROM plants").fetchone()[0] == 0
    with pytest.raises(SystemExit) as error:
        main.main(["--database", str(path), "search-plants", "--limit", "0"])
    assert error.value.code == 2


def test_pagination_uses_fresh_hidden_fields(monkeypatch):
    pager = "<tr><td><a href=\"javascript:__doPostBack('grid','Page$Next')\">Next</a></td></tr>"
    pages = [
        search_html(("Example alba",), "first", pager),
        search_html(("Example beta",), "second", pager),
        search_html(("Example gamma",), "third"),
    ]
    calls = []

    async def request(session, method, url, **kwargs):
        calls.append((method, kwargs))
        return main.FetchedPage(pages[len(calls) - 1], datetime.now(timezone.utc))

    class Session:
        @asynccontextmanager
        async def disabled(self):
            yield

    monkeypatch.setattr(main, "request_page", request)

    async def collect():
        return [page async for page in main.get_all_search_pages(Session())]

    assert len(asyncio.run(collect())) == 3
    assert calls[1][1]["data"]["__VIEWSTATE"] == "first"
    assert calls[2][1]["data"]["__VIEWSTATE"] == "second"
    assert calls[2][1]["data"]["__EVENTVALIDATION"] == "validation-second"


def test_repeated_pagination_page_is_an_error(monkeypatch):
    page = search_html(
        pager="<tr><td><a href=\"javascript:__doPostBack('grid','Page$2')\">2</a></td></tr>"
    )

    async def request(*args, **kwargs):
        return main.FetchedPage(page, datetime.now(timezone.utc))

    class Session:
        @asynccontextmanager
        async def disabled(self):
            yield

    monkeypatch.setattr(main, "request_page", request)

    async def collect():
        return [page async for page in main.get_all_search_pages(Session())]

    with pytest.raises(ValueError, match="repeated the previous page"):
        asyncio.run(collect())


@pytest.mark.parametrize("from_cache", [True, False])
def test_fetch_timestamp_tracks_source_age_not_cache_read_time(plant_page, from_cache):
    cached_at = datetime(2020, 1, 2, 3, 4, 5)
    response = MagicMock(from_cache=from_cache, created_at=cached_at)
    response.read = AsyncMock(return_value=str(plant_page).encode())
    context = AsyncMock()
    context.__aenter__.return_value = response
    session = MagicMock()
    session.request.return_value = context
    before = datetime.now(timezone.utc)
    page = asyncio.run(main.request_page(session, "GET", main.PLANT_URL))
    if from_cache:
        assert page.fetched_at == cached_at.replace(tzinfo=timezone.utc)
    else:
        assert before <= page.fetched_at <= datetime.now(timezone.utc)
    details = main.parse_plant_page(page.soup, "Example alba", page.fetched_at)
    assert details.values["details_scraped_at"] == page.fetched_at.isoformat(
        timespec="seconds"
    )


@pytest.mark.skipif(
    os.environ.get("PFAF_LIVE") != "1",
    reason="Set PFAF_LIVE=1 for four bounded live requests",
)
def test_live_index_and_representative_plants(tmp_path):
    async def check():
        async with main.CachedSession(
            headers={"User-Agent": main.USER_AGENT},
            timeout=main.ClientTimeout(total=60),
            cache=main.SQLiteBackend(str(tmp_path / "live.sqlite"), disabled=True),
        ) as session:
            index = await main.request_page(
                session, "GET", main.SEARCH_URL, params={"LatinName": "%"}
            )
            plants = main.parse_search_page(index.soup)
            # A lower bound from the old snapshot detects unexpectedly truncated results.
            assert len(plants) >= 8504
            assert len({plant["latin_name"] for plant in plants}) == len(plants)
            known_ids = {
                f"ContentPlaceHolder1_{suffix}"
                for suffix in main.DETAIL_FIELDS.values()
            }
            known_ids.update(
                f"ContentPlaceHolder1_{suffix}" for suffix in main.USE_SECTIONS.values()
            )
            known_ids.update(
                f"ContentPlaceHolder1_{suffix}"
                for suffix in (
                    "lbldisplatinname",
                    "txtEdrating",
                    "txtOtherUseRating",
                    "txtMedRating",
                    "txtEdibleUses",
                    "lblCarbFarm",
                    "lblSubject",
                )
            )
            for name, family in (
                ("Abelia triflora", "Caprifoliaceae"),
                ("Malus domestica", "Rosaceae"),
                ("Moringa oleifera", "Moringaceae"),
            ):
                page = await main.request_page(
                    session, "GET", main.PLANT_URL, params={"LatinName": name}
                )
                details = main.parse_plant_page(page.soup, name, page.fetched_at)
                assert details.values["family"] == family
                assert details.values["native_range"]
                assert details.values["source_url"]
                assert (
                    not {
                        node["id"]
                        for node in page.soup.select('span[id^="ContentPlaceHolder1_"]')
                    }
                    - known_ids
                )

    asyncio.run(check())


def test_failed_schema_migration_rolls_back_ddl_and_preserves_outer_transaction():
    with sqlite3.connect(":memory:") as database:
        columns = {
            name: kind
            for name, kind in main.TABLES["plants"].items()
            if name not in {"native_range", "source_url", "details_scraped_at"}
        }
        database.execute(
            "CREATE TABLE plants ("
            + ", ".join(f"{name} {kind}" for name, kind in columns.items())
            + ")"
        )
        database.execute("CREATE VIEW plant_data AS SELECT latin_name FROM plants")
        main.record_plant_row(database, main.parse_search_page(search_html())[0])
        schema = database.execute(
            "SELECT type, name, sql FROM sqlite_schema ORDER BY name"
        ).fetchall()
        database.execute("BEGIN")
        database.execute("UPDATE plants SET common_name='Outer transaction'")
        alters = 0

        def deny_second_alter(action, *unused):
            nonlocal alters
            if action == sqlite3.SQLITE_ALTER_TABLE:
                alters += 1
                if alters == 2:
                    return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        database.set_authorizer(deny_second_alter)
        with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
            main.create_tables(database)
        database.set_authorizer(None)
        assert alters == 2
        assert (
            database.execute(
                "SELECT type, name, sql FROM sqlite_schema ORDER BY name"
            ).fetchall()
            == schema
        )
        assert database.in_transaction
        assert (
            database.execute("SELECT common_name FROM plants").fetchone()[0]
            == "Outer transaction"
        )
        database.rollback()
        assert (
            database.execute("SELECT common_name FROM plants").fetchone()[0]
            == "Example plant"
        )


@pytest.mark.parametrize(
    "error", [ClientConnectionError("connection reset"), asyncio.TimeoutError()]
)
def test_request_recovers_after_transient_connection_failures(
    plant_page, monkeypatch, error
):
    response = MagicMock(from_cache=False)
    response.read = AsyncMock(return_value=str(plant_page).encode())
    context = AsyncMock()
    context.__aenter__.return_value = response
    session = MagicMock()
    session.request.side_effect = [error, error, context]
    sleep = AsyncMock()
    monkeypatch.setattr(main.asyncio, "sleep", sleep)

    page = asyncio.run(main.request_page(session, "GET", main.PLANT_URL))

    assert (
        main.parse_plant_page(page.soup, "Example alba").values["family"]
        == "Exampleaceae"
    )
    assert session.request.call_count == 3
    assert sleep.await_args_list == [call(1), call(2)]


@pytest.mark.parametrize("status", [400, 403, 404, 429, 500, 502, 503, 504])
def test_request_retries_only_transient_http_statuses_with_three_attempt_limit(
    monkeypatch, status
):
    error = ClientResponseError(
        request_info=SimpleNamespace(real_url=main.PLANT_URL),
        history=(),
        status=status,
        message="Test response",
    )
    response = MagicMock()
    response.raise_for_status.side_effect = error
    context = AsyncMock()
    context.__aenter__.return_value = response
    session = MagicMock()
    session.request.return_value = context
    sleep = AsyncMock()
    monkeypatch.setattr(main.asyncio, "sleep", sleep)

    with pytest.raises(ClientResponseError) as failure:
        asyncio.run(main.request_page(session, "GET", main.PLANT_URL))

    assert failure.value is error
    retryable = status in {429, 500, 502, 503, 504}
    assert session.request.call_count == (3 if retryable else 1)
    assert sleep.await_args_list == ([call(1), call(2)] if retryable else [])


def test_exhausted_connection_failures_exit_cli_cleanly_after_three_attempts(
    tmp_path, monkeypatch, capsys
):
    session = MagicMock()
    session.request.side_effect = ClientConnectionError("connection refused")
    context = AsyncMock()
    context.__aenter__.return_value = session
    monkeypatch.setattr(main, "CachedSession", MagicMock(return_value=context))
    sleep = AsyncMock()
    monkeypatch.setattr(main.asyncio, "sleep", sleep)

    with pytest.raises(SystemExit) as failure:
        main.main(
            [
                "--database",
                str(tmp_path / "test.sqlite"),
                "--cache",
                str(tmp_path / "cache.sqlite"),
                "search-plants",
                "--limit",
                "1",
            ]
        )

    assert failure.value.code == 1
    assert session.request.call_count == 3
    assert sleep.await_args_list == [call(1), call(2)]
    captured = capsys.readouterr()
    assert captured.err == "Error: connection refused\n"
    assert "Traceback" not in captured.out + captured.err


def test_load_batch_reports_bad_plants_continues_and_commits_only_success(
    tmp_path, plant_page, monkeypatch, capsys
):
    path = tmp_path / "test.sqlite"
    names = ("Example alba", "Example beta", "Example gamma")
    with sqlite3.connect(path) as database:
        main.create_tables(database)
        for plant in main.parse_search_page(search_html(names)):
            main.record_plant_row(database, plant)
        database.execute("UPDATE plants SET family='Original family'")
    malformed = BeautifulSoup("<h1>Not a plant page</h1>", "lxml")
    plant_page.find(
        id="ContentPlaceHolder1_lbldisplatinname"
    ).string = "Example gamma - A. Author"
    fetched_at = datetime(2026, 10, 17, tzinfo=timezone.utc)
    missing = ClientResponseError(
        request_info=SimpleNamespace(real_url=main.PLANT_URL),
        history=(),
        status=404,
        message="Not Found",
    )
    request = AsyncMock(
        side_effect=[
            main.FetchedPage(malformed, fetched_at),
            missing,
            main.FetchedPage(plant_page, fetched_at),
        ]
    )
    monkeypatch.setattr(main, "request_page", request)

    with pytest.raises(SystemExit) as failure:
        main.main(
            [
                "--database",
                str(path),
                "--cache",
                str(tmp_path / "cache.sqlite"),
                "load-plants",
            ]
        )

    assert failure.value.code == 1
    assert [
        args.kwargs["params"]["LatinName"] for args in request.await_args_list
    ] == list(names)
    captured = capsys.readouterr()
    assert "Failed Example alba:" in captured.out
    assert "Failed Example beta: 404" in captured.out
    assert "Updated 1 plants" in captured.out
    assert (
        captured.err == "Error: Failed to update 2 plants: Example alba, Example beta\n"
    )
    with sqlite3.connect(path) as database:
        assert database.execute(
            "SELECT latin_name, family, details_scraped_at FROM plants ORDER BY latin_name"
        ).fetchall() == [
            ("Example alba", "Original family", None),
            ("Example beta", "Original family", None),
            ("Example gamma", "Exampleaceae", fetched_at.isoformat(timespec="seconds")),
        ]
        assert database.execute("SELECT DISTINCT plant FROM plant_uses").fetchall() == [
            ("Example gamma",)
        ]
        assert database.execute("PRAGMA foreign_key_check").fetchall() == []


def test_load_batch_aborts_after_exhausted_retryable_http_failure(
    tmp_path, plant_page, monkeypatch, capsys
):
    path = tmp_path / "test.sqlite"
    with sqlite3.connect(path) as database:
        main.create_tables(database)
        for plant in main.parse_search_page(search_html()):
            main.record_plant_row(database, plant)
    error = ClientResponseError(
        request_info=SimpleNamespace(real_url=main.PLANT_URL),
        history=(),
        status=503,
        message="Service Unavailable",
    )
    request = AsyncMock(
        side_effect=[error, main.FetchedPage(plant_page, datetime.now(timezone.utc))]
    )
    monkeypatch.setattr(main, "request_page", request)

    with pytest.raises(SystemExit) as failure:
        main.main(
            [
                "--database",
                str(path),
                "--cache",
                str(tmp_path / "cache.sqlite"),
                "load-plants",
            ]
        )

    assert failure.value.code == 1
    assert request.await_count == 1
    captured = capsys.readouterr()
    assert "Error: 503" in captured.err
    assert "Updated" not in captured.out and "Failed Example" not in captured.out
    with sqlite3.connect(path) as database:
        assert (
            database.execute(
                "SELECT count(*) FROM plants WHERE details_scraped_at IS NOT NULL"
            ).fetchone()[0]
            == 0
        )


def test_plant_data_view_works_with_double_quoted_string_literals_disabled():
    if not hasattr(sqlite3.Connection, "setconfig") or not hasattr(
        sqlite3, "SQLITE_DBCONFIG_DQS_DML"
    ):
        pytest.skip(
            "SQLite DQS configuration requires Python 3.12 and a supporting SQLite build"
        )
    with sqlite3.connect(":memory:") as database:
        database.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DDL, False)
        database.setconfig(sqlite3.SQLITE_DBCONFIG_DQS_DML, False)
        main.create_tables(database)
        for plant, common_name in zip(
            main.parse_search_page(
                search_html(("Example alba", "Example beta", "Example gamma"))
            ),
            ("'Zulu'", "'Alpha'", ""),
        ):
            plant["common_name"] = common_name
            main.record_plant_row(database, plant)
        assert database.execute("SELECT latin_name FROM plant_data").fetchall() == [
            ("Example beta",),
            ("Example alba",),
            ("Example gamma",),
        ]
