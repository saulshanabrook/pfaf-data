# Photo archive audit

Photo metadata was checked against the current [PFAF plant pages](https://pfaf.org/user/Default.aspx)
on **2026-10-18 UTC**. Every currently listed unique photo URL was fetched again;
existing filenames were not treated as proof of identity. The plant descriptions,
uses, and care records are unchanged by this photo refresh.

| Check | Result |
| --- | ---: |
| Stored plants checked | 8,796 |
| Successful photo metadata checks | 8,795 |
| Unavailable source pages | 1 |
| Successful pages listing photos | 6,535 |
| Successful pages listing no photos | 2,260 |
| Newly indexed plants with photos | 291 |
| Current plant/photo associations | 12,438 |
| Unique current photo URLs | 12,370 |
| Fresh downloads with matching SHA-256, size, and full image verification | 12,370 |
| Failed current photo downloads | 0 |
| Unchecked plants or pending current downloads | 0 |

Metadata fetches span `2026-10-18T00:24:30+00:00` to `2026-10-18T00:54:32+00:00`. Successful image fetches span
`2026-10-18T00:39:42+00:00` to `2026-10-18T00:55:21+00:00`. The verified current
files total 313,233,218 bytes. Formats: GIF: 1,644, JPEG: 10,707, PNG: 19.

## Repairs and additions

- Corrected **6,206** surviving legacy associations whose
  stored local paths pointed to another filename, including placeholder paths.
- Added **622** current source associations and replaced or removed
  **20** old associations whose URLs were absent from the refreshed pages.
- Added **610 new image files**, preserving safe existing public filenames.
- Refreshed source credit text for **1,633** surviving associations.
- Recovered the exact photo URLs embedded in malformed apostrophe-containing
  image attributes for `Rhododendron 'PJM'` and `Aloe perryi`, preserving each
  photo's paired credit. No filenames were guessed.
- Checked every current allocated path for containment under `images/` and
  Unicode-normalized, case-insensitive uniqueness. Every association has a ledger row.
- Verified that SQLite integrity and foreign-key checks pass, that current
  associations match the cached source-page parsing results, and that all
  non-photo plant columns, uses, and care tables match the prior snapshot.

## Source pages requiring attention

- [Cymopterus purpurescens](https://pfaf.org/user/Plant.aspx?LatinName=Cymopterus+purpurescens): Requested 'Cymopterus purpurescens', but PFAF returned title ''

The retained legacy `Cymopterus purpurescens` URL serves a generic page with empty
plant identity, family, and photo table. It is recorded as a failed source check,
not a confirmed absence of photos. `Cymopterus purpurascens` is a separate existing
record with a valid page; the two names have not been merged or substituted.
Historical associations, if any, remain inspectable but are excluded from the
public photo array while the source check is failing.

## Photo download failures

None.

## Publication and compatibility

`plant_data.images` exposes only successful source associations with verified
`ok` downloads. Each entry contains the original `url`, the PFAF credit text in
`source`, and a relative local `path`. Photos are served directly under the Pages
root at those paths. Safe source filenames were retained for compatibility.

There are **20** unchanged historical files
that are not referenced by current successful photo metadata. They remain for
existing public URLs and are not counted in the current verified archive.
Unreferenced migration-only pending ledger placeholders were removed; actual
attempt evidence is retained. There are no leftover download temporary files.

Verification establishes that the archive matches PFAF's published associations
and the downloaded bytes. It does not independently identify the species pictured.

Final validation: **75 tests passed**, with the optional live test skipped; Ruff
and dependency consistency checks passed. A normal downloader rerun reported
**0 downloaded, 12,370 verified files reused, 0 failures**.

## Reproduce

```sh
uv sync --locked
# --no-cache forces new source HTTP requests; --force selects every plant.
uv run python main.py --no-cache refresh-images --force --concurrency 4
# Normal runs retry failed/pending files and validate reusable files.
uv run python main.py load-images --concurrency 4
# Explicitly fetch every current image URL again:
uv run python main.py load-images --force --concurrency 4
uv run pytest
```

The source refresh intentionally exits non-zero when an upstream plant page fails,
and image downloads do the same for any failed URL, after processing the remaining
items. Inspect `plants.images_error` and `image_downloads.error` for details.
CI checks the committed payload offline on the single pinned Python version.
