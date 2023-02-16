"""
Scrapes PFAF webisite for all plants


We employ two levels of caching:

1. Request level caching, enabled for all requests besides the images and two
   startup requests. This is stored in `cache.sqlite`.
2. Row level caching. If a plant already has details filled in, then skip creating it.

"""
from __future__ import annotations

import logging
from asyncio import create_subprocess_exec, gather, run
from contextlib import closing
from pathlib import Path
from sqlite3 import Connection, connect
from typing import AsyncIterable, Awaitable, Literal, Optional, cast
from urllib.parse import urljoin
from warnings import warn

from aiohttp import ClientTimeout, TCPConnector, TraceConfig
from aiohttp_client_cache import CachedSession, SQLiteBackend
from bs4 import BeautifulSoup
from eralchemy import render_er
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TimeElapsedColumn,
)
from rich.traceback import install
from rich_click.cli import console
from rich_click.typer import Context, Option, Typer

##
# Constants
##

PUBLISH_COMMAND = [
    "datasette",
    "publish",
    "heroku",
    "-m" "metadata.json",
    "--install",
    "datasette-hashed-urls",
    "--install",
    "datasette-gzip",
    "--extra-options",
    "--setting max_returned_rows 100000 --setting sql_time_limit_ms 60000",
    "-n",
    "pfaf-data",
    "data.sqlite",
]

# Total number of search result pages, determined through manual search
N_SEARCH_PAGES = 86

# Database file
DB_FILE = "data.sqlite"

# Mapping of table names to columns, to use for CREATE_TABLE
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
        # We let the rest be null, so that we can fill in just the search
        # details at first. If any of the following are null, we haven't
        # loaded the plant page yet.
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

VIEWS = {
    "plant_data": """
with cares as (
  select
    p.latin_name,
    json_group_array(c.care) cares
  from
    plants p
    left join plant_care c on p.latin_name = c.plant
  group by
    p.latin_name
),
images as (
  select
    p.latin_name,
    json_group_array(json_object('url', i.url, 'source', i.source)) images
  from
    plants p
    left join plant_images i on p.latin_name = i.plant
  group by
    p.latin_name
),
uses as (
  select
    p.latin_name,
    json_group_array(
      json_object('category', u.category, 'name', u.name)
    ) uses
  from
    plants p
    left join plant_uses u on p.latin_name = u.plant
  group by
    p.latin_name
)
select
  *
from
  plants
  join images using (latin_name)
  join uses using (latin_name)
order by
  common_name IS "",
  trim(common_name, "'")
"""
}

app = Typer(chain=True)
console = Console()

# Set async on call
con: Connection
progress: Progress
session: CachedSession

##
# Commands
##


async def main(tasks: list[Awaitable[None]]) -> None:
    global con, progress, session
    with Progress(
        SpinnerColumn(),
        *Progress.get_default_columns(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        # refresh_per_second=1,
        expand=True,
    ) as progress:
        task = progress.add_task("Scraping PFAF...", start=False, total=len(tasks))

        with closing(connect(DB_FILE)) as con:
            con.execute("PRAGMA foreign_keys = ON")
            async with CachedSession(
                # Limit to 10 max requests at once to host, to not overwhelm
                connector=TCPConnector(limit_per_host=10),
                # No timeouts
                timeout=ClientTimeout(total=None),
                trace_configs=[rich_trace_config(progress)],
                cache=SQLiteBackend(
                    "cache",
                    # Cache all requests to individual plants and search pages
                    allowed_methods=("GET", "POST"),
                    # Don't cache based on viewstate
                    ignored_params=["__VIEWSTATE"],
                ),
            ) as session:
                progress.start_task(task)
                for c in tasks:
                    await c
                    progress.advance(task)


def tear_down(tasks, force):
    run(
        main(
            tasks
            or [
                create_tables(force),
                draw_tables(),
                search_plants(),
                load_plants(force),
                load_images(),
                publish(),
            ]
        )
    )


@app.callback(result_callback=tear_down, invoke_without_command=True)
def setup(
    ctx: Context, force: bool = Option(False, help="Force recreating all computation")
):
    """
    Scrape the Plants for a Future website into the data.sqlite file.
    """
    pass


@app.command()
async def create_tables(
    force: bool = Option(False, help="Force recreating the tables, even if they exist"),
):
    """
    Create the tables.
    """
    if force:
        for name in reversed(TABLES.keys()):
            con.execute(f"DROP TABLE IF EXISTS {name}")
    for name, cols in TABLES.items():
        cols_txt = ",".join(f"{name} {tp}" for name, tp in cols.items())
        query = f"CREATE TABLE IF NOT EXISTS {name} ({cols_txt})"
        con.execute(query)
    # Always drop all views and recreate
    for name in VIEWS.keys():
        con.execute(f"DROP VIEW IF EXISTS {name}")
    for name, select in VIEWS.items():
        query = f"CREATE VIEW {name} AS {select}"
        con.execute(query)


@app.command()
async def draw_tables():
    """
    Exports the tables as an image.
    """
    render_er(f"sqlite:///{DB_FILE}", "diagram.png")


@app.command()
async def search_plants():
    """
    Search all the plants and save them to the DB.
    """
    search_pages_task = progress.add_task(
        "Fetching search pages...", total=N_SEARCH_PAGES
    )
    async for page in get_all_search_pages():
        progress.update(search_pages_task, advance=1)
        for row in get_plant_rows_from_search_page(page):
            record_plant_row(row)


@app.command()
async def load_plants(
    force: bool = Option(
        False, help="Force loading plants, even if they have already been loaded"
    )
):
    """
    Load the plant detail pages.
    """
    query = "SELECT latin_name FROM plants"
    # If we are not forcing, only select rows where the family as not been filled in
    if not force:
        query += " WHERE family IS NULL"
    plants = [name for name, in con.execute(query)]
    search_pages_task = progress.add_task("Fetching plants...", total=len(plants))
    tasks = [load_plant(search_pages_task, name) for name in plants]
    await gather(*tasks)


@app.command()
async def load_images():
    """
    Load the image contents.
    """
    urls_and_paths = [
        (url, path)
        for url, path_str in con.execute("SELECT url, path FROM plant_images")
        if not (path := Path(path_str)).exists()
    ]
    task = progress.add_task("Downloading images...", total=len(urls_and_paths))

    tasks = [download_file(task, url, path) for url, path in urls_and_paths]
    await gather(*tasks)


@app.command()
async def publish():
    """
    Push the app to heroku.
    """
    p = await create_subprocess_exec(*PUBLISH_COMMAND)
    await p.wait()


async def download_file(task: TaskID, url: str, path: Path) -> None:
    async with session.get(url) as r:
        if r.status != 200:
            warn(f"Cannot load {url}")
            return
        path.write_bytes(await r.read())
    progress.update(task, advance=1, description=url)


##
# Helper functions
##


async def get_all_search_pages() -> AsyncIterable[BeautifulSoup]:
    """
    Returns all the pages of search results, fetched synchronously.
    Cannot fetch in parallel, otherwise will mess up form logic.
    """
    # Don't cache the first search, so that we have fresh cookies and view state.
    async with session.disabled():
        # First, get the homepage to setup the cookies and get the view_state
        view_state = get_view_state(
            await request_soup(session, "GET", "https://pfaf.org/user/Default.aspx")
        )

        # Then, send a search request for all plants with a " " in their names
        # (should be all plants)
        first_search_page = await request_soup(
            session,
            "POST",
            "https://pfaf.org/user/Default.aspx",
            # Through trial and error, deduced that we need these data to be sent
            # Copied from request in browser
            data={
                "__VIEWSTATE": view_state,
                "__SCROLLPOSITIONX": 0,
                "__SCROLLPOSITIONY": 687,
                "ctl00$ContentPlaceHolder1$txtSearch": " ",
                "ctl00$ContentPlaceHolder1$imgbtnSearch1.x": 47,
                "ctl00$ContentPlaceHolder1$imgbtnSearch1.y": 8,
            },
        )
    yield first_search_page

    # Update the view state to the one from the form
    view_state = get_view_state(first_search_page)

    # Get the remaining search pages
    for i in range(2, N_SEARCH_PAGES + 1):
        yield await request_soup(
            session,
            "POST",
            "https://pfaf.org/user/DatabaseSearhResult.aspx",
            data={
                "__VIEWSTATE": view_state,
                "__VIEWSTATEGENERATOR": "C7892775",
                "ctl00$ContentPlaceHolder1$txtword": "",
                "__EVENTTARGET": "ctl00$ContentPlaceHolder1$gvresults",
                "__EVENTARGUMENT": f"Page${i}",
            },
        )


def get_plant_rows_from_search_page(search_page: BeautifulSoup) -> list[BeautifulSoup]:
    """
    Get all the plant rows from each plant page.
    """
    header, *plants, footer = search_page.find(
        id="ContentPlaceHolder1_gvresults"
    ).find_all("tr", recursive=False)
    return plants


def record_plant_row(plant: BeautifulSoup) -> None:
    """
    Parses a plant row into plant data and a set of common namse
    """
    (
        latin_name,
        common_name,
        habit,
        height,
        hardiness,
        growth,
        soil,
        shade,
        moisture,
        edible,
        medicinal,
        other,
    ) = (
        cast(str, col.get_text()).strip()
        for col in plant.find_all("td", recursive=False)
    )
    with con:
        # Create a plant for this row, if it exists, update it.
        con.execute(
            """INSERT INTO plants(
            latin_name,
            common_name,
            habit,
            height,
            hardiness,
            growth,
            soil,
            shade,
            moisture,
            edibility_rating,
            medicinal_rating,
            other_uses_rating
        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT DO UPDATE SET
            habit=excluded.habit,
            height=excluded.height,
            hardiness=excluded.hardiness,
            growth=excluded.growth,
            soil=excluded.soil,
            moisture=excluded.moisture,
            edibility_rating=excluded.edibility_rating,
            medicinal_rating=excluded.medicinal_rating,
            other_uses_rating=excluded.other_uses_rating""",
            (
                latin_name,
                common_name,
                habit,
                float(height),
                hardiness,
                growth,
                soil,
                shade,
                moisture,
                int(edible) if (edible := edible.strip()) else None,
                int(medicinal) if (medicinal := medicinal.strip()) else None,
                int(other_s) if (other_s := other.strip()) else None,
            ),
        )


async def load_plant(task_id: TaskID, latin_name: str) -> None:
    """
    Parse the plant details from the plant row in a search page, by also visiting
    the plant detail page.
    """
    base_url = "https://pfaf.org/user/Plant.aspx"
    soup = await request_soup(
        session,
        "GET",
        base_url,
        params={"LatinName": latin_name},
    )
    # Verify the latin name is the same
    title = soup.find(id="ContentPlaceHolder1_lbldisplatinname").get_text().strip()
    assert title.startswith(latin_name), latin_name

    care_name_and_url = [
        (c.get("title"), urljoin(base_url, c.get("src")))
        for c in soup.find(id="ContentPlaceHolder1_tblIcons").find_all("img")
    ]
    images_to_sources: dict[str, Optional[str]] = {}
    last_image: str
    for row in soup.find(id="ContentPlaceHolder1_tblPlantImges").find_all(
        "tr", recursive=False
    ):
        if img := row.find("img"):
            url = urljoin(base_url, img.get("src"))
            last_image = url
            images_to_sources[url] = None
        # If there wasn't an image, treat this as a source for the last image
        else:
            source = row.get_text().strip()
            images_to_sources[last_image] = source

    # Remove the no plant image, if that is in the images
    images_to_sources.pop("https://pfaf.org/Admin/PlantImages/noplantimage1.JPG", None)
    images_to_sources.pop("https://pfaf.org/Admin/PlantImages/noplantimage2.JPG", None)

    uses: dict[
        Literal[
            "edible parts",
            "edible uses",
            "medicinal uses",
            "other uses",
            "special uses",
            "cultivation",
            "carbon farming",
        ],
        dict[str, str],
    ] = {
        name: {
            e.get_text().strip(): e.get("title")
            for e in soup.find(id=id_).find_all("a", recursive=False)
        }
        for name, id_ in cast(
            dict[
                Literal["other uses", "cultivation", "medicinal uses", "special uses"],
                str,
            ],
            {
                "other uses": "ContentPlaceHolder1_txtOtherUses",
                "cultivation": "ContentPlaceHolder1_txtCultivationDetails",
                "medicinal uses": "ContentPlaceHolder1_txtMediUses",
                "special uses": "ContentPlaceHolder1_txtSpecialUses",
            },
        ).items()
    }
    uses["carbon farming"] = {
        li.find("a").get_text(): " ".join(
            txt.strip()
            for txt in li.find_all(text=True, recursive=False)
            if txt.strip()
        )
        for li in soup.find(id="ContentPlaceHolder1_lblCarbFarm")
        .parent.find_next_sibling("ul")
        .find_all("li", recursive=False)
    }

    # The uses and parts are all links in the same span, separated by different prefixes
    # Then follows the text description
    edible_uses_paragraph = ""
    current_use_category: dict[str, str]
    for child in soup.find(id="ContentPlaceHolder1_txtEdibleUses"):
        match child:
            case str(m) if m.startswith("Edible Parts:"):
                current_use_category = uses["edible parts"] = {}
            case str(m) if m.startswith("Edible Uses:"):
                current_use_category = uses["edible uses"] = {}
            case str(m) if m.strip():
                edible_uses_paragraph += m.strip().replace("\n", " ")
            case elem if elem.name == "a":
                current_use_category[elem.get_text().strip()] = elem.get("title")
    medicinal_uses_paragraph = " ".join(
        e.strip().replace("\n", " ")
        for e in soup.find(id="ContentPlaceHolder1_txtMediUses").find_all(
            text=True, recursive=False
        )
        if e.strip()
    )
    other_uses_paragraph = " ".join(
        e.strip().replace("\n", " ")
        for e in soup.find(id="ContentPlaceHolder1_txtOtherUses").find_all(
            text=True, recursive=False
        )
        if e.strip()
    )
    cultivation_details_paragraph = " ".join(
        e.strip().replace("\n", " ")
        for e in soup.find(id="ContentPlaceHolder1_txtCultivationDetails").find_all(
            text=True, recursive=False
        )
        if e.strip()
    )
    plant_row = (
        soup.find(id="ContentPlaceHolder1_lblFamily").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblKnownHazards").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_txtHabitats").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblRange").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblWeedPotential").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_txtSummary").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblPhystatment").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblSynonyms").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblhabitats").get_text().strip(),
        edible_uses_paragraph,
        medicinal_uses_paragraph,
        other_uses_paragraph,
        cultivation_details_paragraph,
        soup.find(id="ContentPlaceHolder1_txtPropagation").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblOtherNameText").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblFoundInText").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblWeedPotentialText").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblConservationStatus")
        .get_text()
        .removeprefix("IUCN Red List of Threatened Plants Status :")
        .strip(),
        soup.find(text="Expert comment")
        .parent.find_next_sibling("div")
        .get_text()
        .strip(),
        soup.find(id="ContentPlaceHolder1_txtAuthor").get_text().strip(),
        soup.find(id="ContentPlaceHolder1_lblbiorefer").get_text().strip(),
        latin_name,
    )
    with con:
        # Add plant details
        con.execute(
            """UPDATE plants SET
                family = ?,
                known_hazards = ?,
                habitats = ?,
                range = ?,
                weed_potential = ?,
                summary = ?,
                physical_characteristics = ?,
                synonyms = ?,
                habitats_section = ?,
                edible_uses = ?,
                medicinal_uses = ?,
                other_uses = ?,
                cultivation_details = ?,
                propagation = ?,
                other_names = ?,
                found_in = ?,
                weed_potential_section = ?,
                conservation_status = ?,
                expert_comment = ?,
                author = ?,
                botanical_references = ?
        WHERE latin_name = ?""",
            plant_row,
        )
        # Add cares, if not already exists, and update URLs
        con.executemany(
            "INSERT INTO care(name, icon_url) VALUES(?, ?) ON CONFLICT DO UPDATE SET icon_url=excluded.icon_url",
            care_name_and_url,
        )
        # Remove all existing plant cares for this plant
        con.execute("DELETE FROM plant_care WHERE plant=?", (latin_name,))
        # Add new plant cares
        con.executemany(
            "INSERT INTO plant_care(plant, care) VALUES(?, ?)",
            [(latin_name, care_name) for care_name, url in care_name_and_url],
        )

        # Remove existing plant images
        con.execute("DELETE FROM plant_images WHERE plant=?", (latin_name,))

        # Add plant images
        con.executemany(
            "INSERT INTO plant_images(plant, url, path, source) VALUES(?, ?, ?, ?) ON CONFLICT DO UPDATE SET source=excluded.source",
            [
                (
                    latin_name,
                    image,
                    str(
                        Path("images")
                        / url.removeprefix("https://pfaf.org/Admin/PlantImages/")
                    ),
                    source,
                )
                for image, source in images_to_sources.items()
            ],
        )
        # Add uses
        con.executemany(
            "INSERT INTO uses(category, name, description) VALUES(?, ?, ?) ON CONFLICT DO UPDATE SET description=excluded.description",
            [
                (category, name, description)
                for category, names_and_description in uses.items()
                for name, description in names_and_description.items()
            ],
        )
        # remove existing plant uses
        con.execute("DELETE FROM plant_uses WHERE plant=?", (latin_name,))
        # Add plant uses
        con.executemany(
            "INSERT INTO plant_uses(plant, category, name) VALUES(?, ?, ?)",
            [
                (latin_name, category, name)
                for category, names_and_description in uses.items()
                for name in names_and_description.keys()
            ],
        )
    progress.update(task_id, advance=1, description=latin_name)


def get_view_state(soup: BeautifulSoup) -> str:
    """
    Get the view state from a page.
    """
    return soup.find("input", {"name": "__VIEWSTATE"}).get("value")


async def request_soup(
    session: CachedSession, method: str, url: str, **kwargs
) -> BeautifulSoup:
    """
    Request a URL and parse the body as HTML
    """
    async with session.request(method, url, **kwargs) as r:
        r.raise_for_status()
        html = await r.text()
        return BeautifulSoup(html, "lxml")


def rich_trace_config(progress: Progress) -> TraceConfig:
    """
    Create a trace config which traces all the requests to rich.
    """
    trace_config = TraceConfig()
    n_requests = 0
    all_requests_task = progress.add_task("Visiting pages...", total=n_requests)

    @trace_config.on_request_start.append
    async def on_request_start(session, trace_config_ctx, params):
        nonlocal n_requests
        n_requests += 1
        progress.update(all_requests_task, total=n_requests, advance=0)

    @trace_config.on_request_exception.append
    async def on_request_exception(session, trace_config_ctx, params):
        progress.update(all_requests_task, advance=1)

    @trace_config.on_request_end.append
    async def on_request_end(session, trace_config_ctx, params):
        progress.update(all_requests_task, advance=1)

    return trace_config


if __name__ == "__main__":
    # Install rich logger
    logging.basicConfig(
        level="WARNING", format="%(message)s", datefmt="[%X]", handlers=[RichHandler()]
    )
    # Install rich traceback handler
    install(show_locals=True, console=console, width=None)

    app()
