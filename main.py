"""Explicit, resumable commands for scraping the Plants For A Future database."""

from __future__ import annotations

import argparse
import asyncio
import re
import sqlite3
from collections.abc import AsyncIterator
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlencode, urljoin, urlparse

from aiohttp import (
    ClientConnectionError,
    ClientResponseError,
    ClientTimeout,
    TCPConnector,
)
from aiohttp_client_cache import CachedSession, SQLiteBackend
from bs4 import BeautifulSoup, NavigableString, Tag
from rich.console import Console
from rich.progress import track

SITE_URL = "https://pfaf.org/user/"
SEARCH_URL = urljoin(SITE_URL, "DatabaseSearhResult.aspx")
PLANT_URL = urljoin(SITE_URL, "Plant.aspx")
USER_AGENT = (
    "Mozilla/5.0 (compatible; pfaf-data; +https://github.com/saulshanabrook/pfaf-data)"
)
RETRYABLE_HTTP_STATUSES = {429, 500, 502, 503, 504}
console = Console()
TABLES = {
    "plants": {
        # From search page
        "latin_name": "TEXT PRIMARY KEY NOT NULL",
        "common_name": "TEXT NOT NULL",
        "habit": "TEXT NOT NULL",
        "height": "REAL NOT NULL",
        "hardiness": "TEXT NOT NULL",
        "growth": "TEXT NOT NULL",
        "soil": "TEXT NOT NULL",
        "shade": "TEXT NOT NULL",
        "moisture": "TEXT NOT NULL",
        "edibility_rating": "INTEGER",
        "medicinal_rating": "INTEGER",
        "other_uses_rating": "INTEGER",
        # From detail page table
        # Detail fields are nullable until fetched. details_scraped_at records
        # source freshness independently of whether optional fields are empty.
        "family": "TEXT",
        "known_hazards": "TEXT",
        "habitats": "TEXT",
        "range": "TEXT",
        "weed_potential": "TEXT",
        # From detail page paragraphs
        "summary": "TEXT",
        "physical_characteristics": "TEXT",
        "synonyms": "TEXT",
        "habitats_section": "TEXT",
        "edible_uses": "TEXT",
        "medicinal_uses": "TEXT",
        "other_uses": "TEXT",
        "cultivation_details": "TEXT",
        "propagation": "TEXT",
        "other_names": "TEXT",
        "found_in": "TEXT",
        "weed_potential_section": "TEXT",
        "conservation_status": "TEXT",
        "expert_comment": "TEXT",
        "author": "TEXT",
        "botanical_references": "TEXT",
        # Current website terminology and refresh provenance.
        "native_range": "TEXT",
        "source_url": "TEXT",
        "details_scraped_at": "TEXT",
    },
    "plant_care": {
        "plant": "TEXT NOT NULL",
        "care": "TEXT NOT NULL",
        "FOREIGN KEY(plant)": "REFERENCES plants",
        "FOREIGN KEY(care)": "REFERENCES care",
        "PRIMARY KEY(plant, care)": "",
    },
    "care": {
        "name": "TEXT PRIMARY KEY NOT NULL",
        "icon_url": "TEXT NOT NULL",
    },
    "plant_images": {
        "plant": "TEXT NOT NULL",
        "url": "TEXT NOT NULL",
        "path": "TEXT NOT NULL",
        "source": "TEXT",
        "FOREIGN KEY(plant)": "REFERENCES plants",
        "PRIMARY KEY(plant, url)": "",
    },
    "plant_uses": {
        "plant": "TEXT NOT NULL",
        "category": "TEXT NOT NULL",
        "name": "TEXT NOT NULL",
        "FOREIGN KEY(plant)": "REFERENCES plants",
        "FOREIGN KEY(category, name)": "REFERENCES uses",
        "PRIMARY KEY(plant, category, name)": "",
    },
    "uses": {
        "category": "TEXT CHECK(category IN ('edible parts','edible uses','medicinal uses','other uses','special uses','cultivation','carbon farming') ) NOT NULL",
        "name": "TEXT NOT NULL",
        "description": "TEXT NOT NULL",
        "PRIMARY KEY(category, name)": "",
    },
}

# Correlated aggregates return [] for missing relations and avoid multiplying rows
# when a plant has several images, care requirements, and uses.
VIEWS = {
    "plant_data": """
SELECT p.*,
    (SELECT json_group_array(care)
       FROM plant_care WHERE plant = p.latin_name) AS cares,
    (SELECT json_group_array(json_object('url', url, 'source', source))
       FROM plant_images WHERE plant = p.latin_name) AS images,
    (SELECT json_group_array(json_object('category', category, 'name', name))
       FROM plant_uses WHERE plant = p.latin_name) AS uses
FROM plants AS p
ORDER BY common_name = '', trim(common_name, '''')
"""
}

SEARCH_COLUMNS = (
    "latin_name",
    "common_name",
    "habit",
    "height",
    "hardiness",
    "growth",
    "soil",
    "shade",
    "moisture",
    "edibility_rating",
    "medicinal_rating",
    "other_uses_rating",
)
SEARCH_HEADINGS = (
    "Latin Name",
    "Common Name",
    "Habit",
    "Height",
    "Hardiness",
    "Growth",
    "Soil",
    "Shade",
    "Moisture",
    "Edible",
    "Medicinal",
    "Other",
)
DETAIL_FIELDS = {
    "common_name": "lblCommanName",
    "hardiness": "lblUSDAhardiness",
    "family": "lblFamily",
    "known_hazards": "lblKnownHazards",
    "habitats": "txtHabitats",
    "range": "lblRange",
    "weed_potential": "lblWeedPotential",
    "summary": "txtSummary",
    "physical_characteristics": "lblPhystatment",
    "synonyms": "lblSynonyms",
    "habitats_section": "lblhabitats",
    "propagation": "txtPropagation",
    "other_names": "lblOtherNameText",
    # Keep the historical column available to existing queries and exports.
    "found_in": "lblFoundInText",
    "weed_potential_section": "lblWeedPotentialText",
    "conservation_status": "lblConservationStatus",
    "author": "txtAuthor",
    "botanical_references": "lblbiorefer",
}
USE_SECTIONS = {
    "medicinal uses": "txtMediUses",
    "other uses": "txtOtherUses",
    "cultivation": "txtCultivationDetails",
    "special uses": "txtSpecialUses",
}


@dataclass
class FetchedPage:
    soup: BeautifulSoup
    fetched_at: datetime


@dataclass
class PlantDetails:
    values: dict[str, str | int | None]
    care: dict[str, str]
    images: dict[str, str | None]
    uses: dict[str, dict[str, str]]


def create_tables(con: sqlite3.Connection, force: bool = False) -> None:
    """Create or additively migrate the schema, preserving existing plant rows."""
    # SQLite's legacy transaction mode does not BEGIN for DDL. A savepoint
    # makes the entire migration atomic and also supports an outer transaction.
    con.execute("SAVEPOINT pfaf_schema")
    try:
        for name in VIEWS:
            con.execute(f"DROP VIEW IF EXISTS {name}")
        if force:
            for name in (
                "plant_care",
                "plant_images",
                "plant_uses",
                "care",
                "uses",
                "plants",
            ):
                con.execute(f"DROP TABLE IF EXISTS {name}")
        for name, columns in TABLES.items():
            definitions = ", ".join(
                f"{column} {kind}" for column, kind in columns.items()
            )
            con.execute(f"CREATE TABLE IF NOT EXISTS {name} ({definitions})")
        columns = {row[1] for row in con.execute("PRAGMA table_info(plants)")}
        for name in ("native_range", "source_url", "details_scraped_at"):
            if name not in columns:
                con.execute(f"ALTER TABLE plants ADD COLUMN {name} TEXT")
        for name, query in VIEWS.items():
            con.execute(f"CREATE VIEW {name} AS {query}")
    except BaseException:
        con.execute("ROLLBACK TO SAVEPOINT pfaf_schema")
        con.execute("RELEASE SAVEPOINT pfaf_schema")
        raise
    else:
        con.execute("RELEASE SAVEPOINT pfaf_schema")


def parse_search_page(soup: BeautifulSoup) -> list[dict[str, str | int | float | None]]:
    """Validate the result layout before interpreting positional search columns."""
    table = soup.find(id="ContentPlaceHolder1_gvresults")
    if table is None:
        raise ValueError(
            "PFAF search results table is missing; the website may have changed"
        )
    rows = table.find_all("tr", recursive=False)
    if not rows:
        raise ValueError("PFAF search results table is empty")
    headings = tuple(
        cell.get_text(" ", strip=True)
        for cell in rows[0].find_all(["th", "td"], recursive=False)
    )
    if headings != SEARCH_HEADINGS:
        raise ValueError(f"Unexpected PFAF search columns: {headings!r}")
    plants = []
    for row in rows[1:]:
        # ASP.NET pager rows also contain td elements, but no plant link.
        if row.find("a", href=re.compile(r"Plant\.aspx\?")) is None:
            continue
        cells = row.find_all("td", recursive=False)
        if len(cells) != len(SEARCH_COLUMNS):
            raise ValueError(
                f"Unexpected PFAF plant row: {row.get_text(' ', strip=True)!r}"
            )
        plant = dict(
            zip(SEARCH_COLUMNS, (cell.get_text(" ", strip=True) for cell in cells))
        )
        plant["height"] = float(plant["height"])
        for name in SEARCH_COLUMNS[-3:]:
            plant[name] = int(plant[name]) if plant[name] else None
        plants.append(plant)
    return plants


def record_plant_row(con: sqlite3.Connection, plant: dict) -> None:
    """Refresh every discovery field without replacing detail data or relations."""
    columns = ", ".join(SEARCH_COLUMNS)
    assignments = ", ".join(f"{name}=excluded.{name}" for name in SEARCH_COLUMNS[1:])
    placeholders = ", ".join("?" for _ in SEARCH_COLUMNS)
    with con:
        con.execute(
            f"INSERT INTO plants ({columns}) VALUES ({placeholders}) "
            f"ON CONFLICT(latin_name) DO UPDATE SET {assignments}",
            [plant[name] for name in SEARCH_COLUMNS],
        )


def prose_without_glossary(section: Tag | None) -> str:
    """Keep nested prose while removing the separate glossary/tag list."""
    if section is None:
        return ""
    copy = BeautifulSoup(str(section), "lxml")
    for link in copy.find_all("a", href=re.compile(r"Search_Use\.aspx", re.IGNORECASE)):
        link.decompose()
    text = " ".join(copy.stripped_strings)
    text = re.sub(r"Edible (?:Parts|Uses):\s*", "", text)
    return re.sub(r"\s+([,.;:!?])", r"\1", text).strip()


def parse_plant_page(
    soup: BeautifulSoup, latin_name: str, fetched_at: datetime | None = None
) -> PlantDetails:
    """Parse optional sections independently; fail closed on the wrong plant."""
    title = soup.find(id="ContentPlaceHolder1_lbldisplatinname")
    title_text = title.get_text(" ", strip=True) if title else ""
    if title_text != latin_name and not title_text.startswith(f"{latin_name} - "):
        raise ValueError(
            f"Requested {latin_name!r}, but PFAF returned title {title_text!r}"
        )
    if soup.find(id="ContentPlaceHolder1_lblFamily") is None:
        raise ValueError(f"PFAF overview is missing for {latin_name!r}")
    values = {}
    for column, suffix in DETAIL_FIELDS.items():
        node = soup.find(id=f"ContentPlaceHolder1_{suffix}")
        values[column] = " ".join(node.stripped_strings) if node else ""
    values["conservation_status"] = (
        values["conservation_status"]
        .removeprefix("IUCN Red List of Threatened Plants Status :")
        .strip()
    )
    native_heading = soup.find(
        ["h2", "h3"], string=re.compile(r"^\s*Native Range\s*$", re.IGNORECASE)
    )
    values["native_range"] = values["found_in"] if native_heading else None
    values["source_url"] = f"{PLANT_URL}?{urlencode({'LatinName': latin_name})}"
    values["details_scraped_at"] = (fetched_at or datetime.now(timezone.utc)).isoformat(
        timespec="seconds"
    )
    for column, suffix in {
        "edibility_rating": "txtEdrating",
        "medicinal_rating": "txtMedRating",
        "other_uses_rating": "txtOtherUseRating",
    }.items():
        node = soup.find(id=f"ContentPlaceHolder1_{suffix}")
        if node and (match := re.search(r"\((\d+) of 5\)", node.get_text())):
            values[column] = int(match[1])
    expert_heading = soup.find(
        ["h2", "h3"], string=re.compile(r"^\s*Expert comment\s*$", re.IGNORECASE)
    )
    expert = expert_heading.find_next_sibling() if expert_heading else None
    values["expert_comment"] = (
        " ".join(expert.stripped_strings) if expert and expert.name == "div" else ""
    )

    care = {}
    icons = soup.find(id="ContentPlaceHolder1_tblIcons")
    if icons:
        for icon in icons.find_all("img", src=True):
            name = (icon.get("title") or icon.get("alt") or "").strip()
            if name:
                care[name] = urljoin(PLANT_URL, icon["src"])

    images = {}
    image_table = soup.find(id="ContentPlaceHolder1_tblPlantImges")
    last_image = None
    if image_table:
        for row in image_table.find_all("tr", recursive=False):
            photos = row.find_all("img", src=True)
            if photos:
                for photo in photos:
                    url = urljoin(PLANT_URL, photo["src"])
                    if (
                        urlparse(url)
                        .path.rsplit("/", 1)[-1]
                        .lower()
                        .startswith("noplantimage")
                    ):
                        last_image = None
                        continue
                    last_image = url
                    images[url] = None
            elif last_image and (source := row.get_text(" ", strip=True)):
                images[last_image] = source

    uses = {
        category: {}
        for category in ("edible parts", "edible uses", *USE_SECTIONS, "carbon farming")
    }
    edible = soup.find(id="ContentPlaceHolder1_txtEdibleUses")
    category = None
    if edible:
        for child in edible.descendants:
            if isinstance(child, NavigableString):
                if "Edible Parts:" in child:
                    category = "edible parts"
                if "Edible Uses:" in child:
                    category = "edible uses"
            elif (
                child.name == "a"
                and category
                and "Search_Use.aspx" in child.get("href", "")
            ):
                name = child.get_text(" ", strip=True)
                if name:
                    uses[category][name] = child.get("title") or ""
    values["edible_uses"] = prose_without_glossary(edible)
    for category, suffix in USE_SECTIONS.items():
        section = soup.find(id=f"ContentPlaceHolder1_{suffix}")
        if section:
            for link in section.find_all(
                "a", href=re.compile(r"Search_Use\.aspx", re.IGNORECASE)
            ):
                if name := link.get_text(" ", strip=True):
                    uses[category][name] = link.get("title") or ""
        if category != "special uses":
            column = (
                "cultivation_details"
                if category == "cultivation"
                else category.replace(" ", "_")
            )
            values[column] = prose_without_glossary(section)
    carbon = soup.find(id="ContentPlaceHolder1_lblCarbFarm")
    # The current site often leaves an empty h3 with no list at all.
    carbon_list = carbon.parent.find_next_sibling() if carbon else None
    if carbon_list and carbon_list.name == "ul":
        for item in carbon_list.find_all("li", recursive=False):
            link = item.find("a")
            if link:
                name = link.get_text(" ", strip=True)
                description = (
                    " ".join(item.stripped_strings).removeprefix(name).strip(" :-")
                )
                uses["carbon farming"][name] = description
    return PlantDetails(values, care, images, uses)


def record_plant_details(
    con: sqlite3.Connection, latin_name: str, details: PlantDetails
) -> None:
    """Replace a plant's detail relations atomically after successful parsing."""
    with con:
        result = con.execute(
            f"UPDATE plants SET {', '.join(f'{name}=?' for name in details.values)} WHERE latin_name=?",
            [*details.values.values(), latin_name],
        )
        if result.rowcount != 1:
            raise ValueError(f"Plant {latin_name!r} is absent; run search-plants first")
        con.executemany(
            "INSERT INTO care(name, icon_url) VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET icon_url=excluded.icon_url",
            details.care.items(),
        )
        con.execute("DELETE FROM plant_care WHERE plant=?", (latin_name,))
        con.executemany(
            "INSERT INTO plant_care VALUES (?, ?)",
            [(latin_name, name) for name in details.care],
        )
        con.execute("DELETE FROM plant_images WHERE plant=?", (latin_name,))
        con.executemany(
            "INSERT INTO plant_images(plant, url, path, source) VALUES (?, ?, ?, ?)",
            [
                (
                    latin_name,
                    url,
                    str(
                        PurePosixPath("images")
                        / PurePosixPath(unquote(urlparse(url).path)).name
                    ),
                    source,
                )
                for url, source in details.images.items()
            ],
        )
        con.executemany(
            "INSERT INTO uses(category, name, description) VALUES (?, ?, ?) "
            "ON CONFLICT(category, name) DO UPDATE SET description=excluded.description",
            [
                (category, name, description)
                for category, names in details.uses.items()
                for name, description in names.items()
            ],
        )
        con.execute("DELETE FROM plant_uses WHERE plant=?", (latin_name,))
        con.executemany(
            "INSERT INTO plant_uses VALUES (?, ?, ?)",
            [
                (latin_name, category, name)
                for category, names in details.uses.items()
                for name in names
            ],
        )


async def request_page(
    session: CachedSession, method: str, url: str, **kwargs
) -> FetchedPage:
    """Use finite timeouts and bounded retries for transient upstream failures."""
    for attempt in range(3):
        try:
            async with session.request(method, url, **kwargs) as response:
                response.raise_for_status()
                soup = BeautifulSoup(await response.text(), "lxml")
                fetched_at = (
                    response.created_at
                    if getattr(response, "from_cache", False)
                    else datetime.now(timezone.utc)
                )
                # aiohttp-client-cache stores created_at as naive UTC.
                if fetched_at.tzinfo is None:
                    fetched_at = fetched_at.replace(tzinfo=timezone.utc)
                return FetchedPage(soup, fetched_at)
        except (
            ClientConnectionError,
            ClientResponseError,
            asyncio.TimeoutError,
        ) as error:
            if (
                isinstance(error, ClientResponseError)
                and error.status not in RETRYABLE_HTTP_STATUSES
            ):
                raise
            if attempt == 2:
                raise
            await asyncio.sleep(attempt + 1)
    raise AssertionError("unreachable")


async def get_all_search_pages(
    session: CachedSession, query: str = "%"
) -> AsyncIterator[BeautifulSoup]:
    """Use the public index and follow any advertised ASP.NET pager dynamically."""
    page = (
        await request_page(session, "GET", SEARCH_URL, params={"LatinName": query})
    ).soup
    page_number = 1
    while True:
        yield page
        targets = []
        for link in page.select("#ContentPlaceHolder1_gvresults a[href]"):
            match = re.search(
                r"__doPostBack\('([^']+)','Page\$(\d+|Next)'\)", link["href"]
            )
            if match and (match[2] == "Next" or int(match[2]) == page_number + 1):
                targets.append((match[1], match[2]))
        if not targets:
            return
        form_state = {
            node["name"]: node.get("value", "")
            for node in page.select('input[type="hidden"][name]')
        }
        if "__VIEWSTATE" not in form_state:
            raise ValueError("PFAF pagination is missing ASP.NET form state")
        target, argument = targets[0]
        form_state.update(__EVENTTARGET=target, __EVENTARGUMENT=f"Page${argument}")
        # Hidden state and cookie context must come from the immediately prior page.
        async with session.disabled():
            next_page = (
                await request_page(
                    session,
                    "POST",
                    SEARCH_URL,
                    params={"LatinName": query},
                    data=form_state,
                )
            ).soup
        if [p["latin_name"] for p in parse_search_page(next_page)] == [
            p["latin_name"] for p in parse_search_page(page)
        ]:
            raise ValueError("PFAF pagination repeated the previous page; stopping")
        page, page_number = next_page, page_number + 1


async def run_network_command(
    args: argparse.Namespace, con: sqlite3.Connection
) -> None:
    async with CachedSession(
        headers={"User-Agent": USER_AGENT},
        connector=TCPConnector(limit_per_host=2),
        timeout=ClientTimeout(total=60),
        cache=SQLiteBackend(
            str(args.cache), expire_after=86400, disabled=args.no_cache
        ),
    ) as session:
        if args.command == "search-plants":
            count = 0
            async for page in get_all_search_pages(session, args.query):
                for plant in parse_search_page(page):
                    record_plant_row(con, plant)
                    count += 1
                    if args.limit and count >= args.limit:
                        console.print(f"Saved {count} search results")
                        return
            console.print(f"Saved {count} search results")
        elif args.command == "load-plants":
            conditions, parameters = [], []
            if args.plant:
                conditions.append(
                    f"latin_name IN ({', '.join('?' for _ in args.plant)})"
                )
                parameters.extend(args.plant)
            if not args.force:
                conditions.append("details_scraped_at IS NULL")
            query = "SELECT latin_name FROM plants"
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            query += " ORDER BY latin_name"
            if args.limit:
                query += " LIMIT ?"
                parameters.append(args.limit)
            plants = [row[0] for row in con.execute(query, parameters)]
            if args.plant:
                existing = {
                    row[0] for row in con.execute("SELECT latin_name FROM plants")
                }
                missing = set(args.plant) - existing
                if missing:
                    raise ValueError(
                        f"Plants absent from database: {sorted(missing)!r}; run search-plants first"
                    )
            updated, failed = 0, []
            for name in track(plants, description="Fetching plants", console=console):
                try:
                    page = await request_page(
                        session, "GET", PLANT_URL, params={"LatinName": name}
                    )
                    record_plant_details(
                        con, name, parse_plant_page(page.soup, name, page.fetched_at)
                    )
                except (ValueError, ClientResponseError) as error:
                    if (
                        isinstance(error, ClientResponseError)
                        and error.status in RETRYABLE_HTTP_STATUSES
                    ):
                        raise
                    console.print(f"Failed {name}: {error}", markup=False)
                    failed.append(name)
                    continue
                updated += 1
            console.print(f"Updated {updated} plants")
            if failed:
                raise ValueError(
                    f"Failed to update {len(failed)} plants: {', '.join(failed)}"
                )
        elif args.command == "load-images":
            images = [
                (url, args.database.parent / path)
                for url, path in con.execute(
                    "SELECT DISTINCT url, path FROM plant_images"
                )
            ]
            images = [(url, path) for url, path in images if not path.exists()]
            if args.limit:
                images = images[: args.limit]
            for url, path in track(
                images, description="Downloading images", console=console
            ):
                # Avoid storing a second copy of every downloaded image in SQLite.
                async with session.disabled(), session.get(url) as response:
                    response.raise_for_status()
                    body = await response.read()
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(body)
            console.print(f"Downloaded {len(images)} images")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("data.sqlite"),
        help="SQLite database (default: data.sqlite)",
    )
    parser.add_argument(
        "--cache", type=Path, default=Path("cache.sqlite"), help="HTTP cache file"
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Fetch fresh pages instead of using the HTTP cache",
    )
    commands = parser.add_subparsers(dest="command")
    create = commands.add_parser(
        "create-tables", help="Create tables or add new columns, preserving data"
    )
    create.add_argument(
        "--force",
        action="store_true",
        help="Delete existing data and recreate all tables",
    )
    search = commands.add_parser(
        "search-plants", help="Discover plant names and search fields"
    )
    search.add_argument(
        "--query",
        default="%",
        help="Public Latin-name search query (default: all names)",
    )
    load = commands.add_parser("load-plants", help="Fetch plant detail pages")
    load.add_argument(
        "--plant", action="append", help="Only this exact Latin name; may be repeated"
    )
    load.add_argument(
        "--force", action="store_true", help="Refresh previously fetched details"
    )
    images = commands.add_parser(
        "load-images", help="Download missing images alongside the database"
    )
    for command in (search, load, images):
        command.add_argument(
            "--limit", type=int, help="Maximum number of records to process"
        )
    diagram = commands.add_parser(
        "draw-tables", help="Render an ER diagram (requires the diagram extra)"
    )
    diagram.add_argument("--output", type=Path, default=Path("diagram.png"))
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        return
    if getattr(args, "limit", None) is not None and args.limit < 1:
        parser.error("--limit must be positive")
    try:
        with closing(sqlite3.connect(args.database)) as con:
            con.execute("PRAGMA foreign_keys = ON")
            create_tables(con, force=args.command == "create-tables" and args.force)
            if args.command == "draw-tables":
                from eralchemy import render_er

                render_er(f"sqlite:///{args.database.resolve()}", str(args.output))
            elif args.command != "create-tables":
                asyncio.run(run_network_command(args, con))
    except (
        ValueError,
        ClientConnectionError,
        ClientResponseError,
        asyncio.TimeoutError,
        sqlite3.Error,
    ) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
