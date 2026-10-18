"""Explicit, resumable commands for scraping the Plants For A Future database."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import re
import sqlite3
from collections.abc import AsyncIterator
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape, unescape
from io import BytesIO
from pathlib import Path, PurePosixPath
from tempfile import NamedTemporaryFile
from urllib.parse import unquote, urlencode, urljoin, urlparse

from aiohttp import (
    ClientConnectionError,
    ClientPayloadError,
    ClientResponseError,
    ClientTimeout,
    DummyCookieJar,
    TCPConnector,
)
from aiohttp_client_cache import CachedSession, SQLiteBackend
from bs4 import BeautifulSoup, NavigableString, Tag, UnicodeDammit
from PIL import Image, ImageSequence, UnidentifiedImageError
from rich.console import Console
from rich.progress import Progress

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
        "images_scraped_at": "TEXT",
        "images_error": "TEXT",
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
    "image_downloads": {
        "url": "TEXT PRIMARY KEY NOT NULL",
        "path": "TEXT UNIQUE NOT NULL",
        "status": "TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','ok','error'))",
        "sha256": "TEXT",
        "fetched_at": "TEXT",
        "error": "TEXT",
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
    (SELECT json_group_array(json_object('url', i.url, 'source', i.source, 'path', d.path))
       FROM plant_images i JOIN image_downloads d ON d.url = i.url
       WHERE i.plant = p.latin_name AND d.status = 'ok'
         AND p.images_scraped_at IS NOT NULL AND p.images_error IS NULL) AS images,
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
class FetchedContent:
    body: bytes
    fetched_at: datetime


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


def image_path(url: str) -> str:
    """Give each exact source URL its own portable filename, without basename collisions."""
    suffix = PurePosixPath(unquote(urlparse(url).path)).suffix.lower()
    if suffix not in {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".tif",
        ".tiff",
        ".bmp",
        ".avif",
    }:
        suffix = ".img"
    return str(
        PurePosixPath("images/by-url")
        / (hashlib.sha256(url.encode()).hexdigest() + suffix)
    )


def parse_page_html(body: bytes | str) -> BeautifulSoup:
    """Repair PFAF's unescaped filename apostrophes using the original attribute text."""
    text = UnicodeDammit(body).unicode_markup
    if text is None:
        raise ValueError("PFAF returned undecodable HTML")
    table_pattern = re.compile(
        r"""(<table\b[^>]*\bid\s*=\s*['"]ContentPlaceHolder1_tblPlantImges['"][^>]*>)(.*?)(</table\s*>)""",
        re.IGNORECASE | re.DOTALL,
    )
    if "ContentPlaceHolder1_tblPlantImges" in text and not table_pattern.search(text):
        raise ValueError("PFAF photo table is truncated")
    # A quote inside a filename is not the closing delimiter unless followed by
    # an actual attribute assignment or the end of the img tag.
    src_pattern = re.compile(
        r"""(<img\b[^>]*?\ssrc\s*=\s*)(['"])([^<>]*?)\2(?=\s+[A-Za-z_:][-A-Za-z0-9_:.]*\s*=|\s*/?>)""",
        re.IGNORECASE | re.DOTALL,
    )

    def repair_table(match):
        def repair_src(source):
            return source[1] + '"' + escape(unescape(source[3]), quote=True) + '"'

        return match[1] + src_pattern.sub(repair_src, match[2]) + match[3]

    return BeautifulSoup(table_pattern.sub(repair_table, text), "lxml")


def parse_plant_images(soup: BeautifulSoup, latin_name: str) -> dict[str, str | None]:
    """Validate plant identity and preserve image-to-credit pairing by table column."""
    title = soup.find(id="ContentPlaceHolder1_lbldisplatinname")
    title_text = title.get_text(" ", strip=True) if title else ""
    if title_text not in {latin_name, f"{latin_name} -"} and not title_text.startswith(
        f"{latin_name} - "
    ):
        raise ValueError(
            f"Requested {latin_name!r}, but PFAF returned title {title_text!r}"
        )
    if soup.find(id="ContentPlaceHolder1_lblFamily") is None:
        raise ValueError(f"PFAF overview is missing for {latin_name!r}")
    table = soup.find(id="ContentPlaceHolder1_tblPlantImges")
    if table is None:
        raise ValueError(f"PFAF photo table is missing for {latin_name!r}")
    images, pending = {}, {}
    for row in table.find_all("tr", recursive=False):
        for column, cell in enumerate(row.find_all(["td", "th"], recursive=False)):
            photos = cell.find_all("img")
            if photos:
                pending[column] = []
                for photo in photos:
                    if not photo.get("src"):
                        raise ValueError(
                            f"PFAF photo has no source URL for {latin_name!r}"
                        )
                    # Keep existing source URL identities, including spaces;
                    # aiohttp encodes them for HTTP. Escape the recovered quotes.
                    url = urljoin(PLANT_URL, photo["src"]).replace("'", "%27")
                    if (
                        PurePosixPath(urlparse(url).path)
                        .name.lower()
                        .startswith("noplantimage")
                    ):
                        continue
                    images[url] = None
                    pending[column].append(url)
            elif source := cell.get_text(" ", strip=True):
                for url in pending.get(column, []):
                    images[url] = source
    return images


def record_plant_images(
    con: sqlite3.Connection,
    latin_name: str,
    images: dict[str, str | None],
    fetched_at: datetime,
) -> None:
    """Atomically replace source-listed photos without changing other plant details."""
    con.execute("SAVEPOINT pfaf_images")
    try:
        result = con.execute(
            "UPDATE plants SET images_scraped_at=?,images_error=NULL WHERE latin_name=?",
            (fetched_at.isoformat(timespec="seconds"), latin_name),
        )
        if result.rowcount != 1:
            raise ValueError(f"Plant {latin_name!r} is absent; run search-plants first")
        con.execute("DELETE FROM plant_images WHERE plant=?", (latin_name,))
        for url, source in images.items():
            existing = con.execute(
                "SELECT path FROM image_downloads WHERE url=?", (url,)
            ).fetchone()
            path = existing[0] if existing else image_path(url)
            if existing is None:
                con.execute(
                    "INSERT INTO image_downloads(url,path) VALUES(?,?)", (url, path)
                )
            con.execute(
                "INSERT INTO plant_images(plant,url,path,source) VALUES(?,?,?,?)",
                (latin_name, url, path, source),
            )
    except BaseException:
        con.execute("ROLLBACK TO SAVEPOINT pfaf_images")
        con.execute("RELEASE SAVEPOINT pfaf_images")
        raise
    else:
        con.execute("RELEASE SAVEPOINT pfaf_images")


def validate_image(body: bytes) -> str:
    """Verify format integrity and decode every frame before trusting or publishing bytes."""
    try:
        with Image.open(BytesIO(body)) as image:
            image.verify()
        with Image.open(BytesIO(body)) as image:
            for frame in ImageSequence.Iterator(image):
                frame.load()
    except (
        OSError,
        ValueError,
        SyntaxError,
        UnidentifiedImageError,
        Image.DecompressionBombError,
    ) as error:
        raise ValueError(f"Invalid image: {error}") from error
    return hashlib.sha256(body).hexdigest()


async def request_page(
    session: CachedSession, method: str, url: str, **kwargs
) -> FetchedPage:
    content = await request_content(session, method, url, **kwargs)
    return FetchedPage(parse_page_html(content.body), content.fetched_at)


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
                "image_downloads",
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
        for name in (
            "native_range",
            "source_url",
            "details_scraped_at",
            "images_scraped_at",
            "images_error",
        ):
            if name not in columns:
                con.execute(f"ALTER TABLE plants ADD COLUMN {name} TEXT")
        con.execute("CREATE INDEX IF NOT EXISTS plant_images_url ON plant_images(url)")
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
    images = parse_plant_images(soup, latin_name)
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
        record_plant_images(
            con,
            latin_name,
            details.images,
            datetime.fromisoformat(str(details.values["details_scraped_at"])),
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


async def request_content(
    session: CachedSession, method: str, url: str, **kwargs
) -> FetchedContent:
    """Use finite timeouts and bounded retries for transient upstream failures."""
    for attempt in range(3):
        try:
            async with session.request(method, url, **kwargs) as response:
                response.raise_for_status()
                body = await response.read()
                fetched_at = (
                    response.created_at
                    if getattr(response, "from_cache", False)
                    else datetime.now(timezone.utc)
                )
                # aiohttp-client-cache stores created_at as naive UTC.
                if fetched_at.tzinfo is None:
                    fetched_at = fetched_at.replace(tzinfo=timezone.utc)
                return FetchedContent(body, fetched_at)
        except (
            ClientConnectionError,
            ClientPayloadError,
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


async def run_workers(items: list, handler, concurrency: int, description: str) -> None:
    """Bound in-flight work and cancel all workers if an unexpected error escapes."""
    iterator = iter(items)
    with Progress(console=console) as progress:
        task = progress.add_task(description, total=len(items))

        async def worker():
            for item in iterator:
                await handler(item)
                progress.advance(task)

        workers = [
            asyncio.create_task(worker()) for _ in range(min(concurrency, len(items)))
        ]
        try:
            await asyncio.gather(*workers)
        except BaseException:
            for worker_task in workers:
                worker_task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise


async def run_network_command(
    args: argparse.Namespace, con: sqlite3.Connection
) -> None:
    async with CachedSession(
        headers={"User-Agent": USER_AGENT},
        cookie_jar=DummyCookieJar()
        if args.command in {"refresh-images", "load-images"}
        else None,
        connector=TCPConnector(limit_per_host=getattr(args, "concurrency", 2)),
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
        elif args.command in {"load-plants", "refresh-images"}:
            conditions, parameters = [], []
            if args.plant:
                conditions.append(
                    f"latin_name IN ({', '.join('?' for _ in args.plant)})"
                )
                parameters.extend(args.plant)
            if not args.force:
                conditions.append(
                    "details_scraped_at IS NULL"
                    if args.command == "load-plants"
                    else "(images_scraped_at IS NULL OR images_error IS NOT NULL)"
                )
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

            async def refresh_plant(name):
                nonlocal updated
                try:
                    page = await request_page(
                        session, "GET", PLANT_URL, params={"LatinName": name}
                    )
                    if args.command == "refresh-images":
                        record_plant_images(
                            con,
                            name,
                            parse_plant_images(page.soup, name),
                            page.fetched_at,
                        )
                    else:
                        record_plant_details(
                            con,
                            name,
                            parse_plant_page(page.soup, name, page.fetched_at),
                        )
                except (
                    ValueError,
                    ClientResponseError,
                    ClientConnectionError,
                    ClientPayloadError,
                    asyncio.TimeoutError,
                ) as error:
                    if args.command == "load-plants" and (
                        not isinstance(error, (ValueError, ClientResponseError))
                        or isinstance(error, ClientResponseError)
                        and error.status in RETRYABLE_HTTP_STATUSES
                    ):
                        raise
                    message = str(error) or type(error).__name__
                    if args.command == "refresh-images":
                        with con:
                            con.execute(
                                "UPDATE plants SET images_error=? WHERE latin_name=?",
                                (message, name),
                            )
                    console.print(f"Failed {name}: {message}", markup=False)
                    failed.append(name)
                    return
                updated += 1

            await run_workers(
                plants,
                refresh_plant,
                getattr(args, "concurrency", 1),
                "Fetching plants",
            )
            console.print(f"Updated {updated} plants")
            if failed:
                raise ValueError(
                    f"Failed to update {len(failed)} plants: {', '.join(failed)}"
                )
        elif args.command == "load-images":
            rows = con.execute("""SELECT d.url,d.path,d.status,d.sha256
                FROM image_downloads d WHERE EXISTS (
                    SELECT 1 FROM plant_images i JOIN plants p ON p.latin_name=i.plant
                    WHERE i.url=d.url AND p.images_scraped_at IS NOT NULL AND p.images_error IS NULL)
                ORDER BY d.url""").fetchall()
            downloaded, reused, failed = 0, 0, []
            pending = []
            for row in rows:
                url, relative, status, expected_hash = row
                if not args.force and status == "ok" and expected_hash:
                    try:
                        actual_hash = validate_image(
                            (args.database.parent / relative).read_bytes()
                        )
                        if actual_hash == expected_hash:
                            reused += 1
                            continue
                    except (OSError, ValueError):
                        pass
                pending.append(row)
                if args.limit and len(pending) >= args.limit:
                    break

            async def download_image(row):
                nonlocal downloaded
                url, relative, status, expected_hash = row
                path = args.database.parent / relative
                with con:
                    con.execute(
                        "UPDATE image_downloads SET status='pending',error=NULL WHERE url=?",
                        (url,),
                    )
                try:
                    async with session.disabled():
                        content = await request_content(session, "GET", url)
                    digest = validate_image(content.body)
                    path.parent.mkdir(parents=True, exist_ok=True)
                    temporary = None
                    try:
                        with NamedTemporaryFile(
                            dir=path.parent,
                            prefix=f".{path.name}.",
                            suffix=".tmp",
                            delete=False,
                        ) as output:
                            temporary = Path(output.name)
                            output.write(content.body)
                            output.flush()
                            os.fsync(output.fileno())
                        temporary.replace(path)
                    finally:
                        if temporary is not None:
                            temporary.unlink(missing_ok=True)
                    with con:
                        con.execute(
                            "UPDATE image_downloads SET status='ok',sha256=?,fetched_at=?,error=NULL WHERE url=?",
                            (
                                digest,
                                content.fetched_at.isoformat(timespec="seconds"),
                                url,
                            ),
                        )
                    downloaded += 1
                except (
                    ValueError,
                    OSError,
                    ClientConnectionError,
                    ClientPayloadError,
                    ClientResponseError,
                    asyncio.TimeoutError,
                ) as error:
                    message = str(error) or type(error).__name__
                    with con:
                        con.execute(
                            "UPDATE image_downloads SET status='error',error=? WHERE url=?",
                            (message, url),
                        )
                    console.print(f"Failed image {url}: {message}", markup=False)
                    failed.append(url)

            await run_workers(
                pending, download_image, args.concurrency, "Downloading images"
            )
            console.print(
                f"Downloaded {downloaded} images; reused {reused} verified images; failed {len(failed)}"
            )
            if failed:
                raise ValueError(f"Failed to download {len(failed)} images")


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
    refresh = commands.add_parser(
        "refresh-images",
        help="Refresh only photo metadata, independent of plant details",
    )
    refresh.add_argument(
        "--plant", action="append", help="Only this exact Latin name; may be repeated"
    )
    refresh.add_argument(
        "--force", action="store_true", help="Refresh already checked photo metadata"
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
    images.add_argument(
        "--force",
        action="store_true",
        help="Download all current URLs again, even verified files",
    )
    for command in (refresh, images):
        command.add_argument(
            "--concurrency",
            type=int,
            default=2,
            help="Maximum simultaneous requests (default: 2)",
        )
    for command in (search, load, refresh, images):
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
    if getattr(args, "concurrency", 1) < 1:
        parser.error("--concurrency must be positive")
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
        ClientPayloadError,
        ClientResponseError,
        asyncio.TimeoutError,
        sqlite3.Error,
    ) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
