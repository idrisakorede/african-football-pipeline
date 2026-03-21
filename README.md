# African Football Data Pipeline

An end-to-end data engineering pipeline scraping football match data across African leagues. Built in three phases to demonstrate how data infrastructure evolves from local scripts to production-grade architecture.

**Phase 1** (current) delivers a fully functional local pipeline: config-driven scraping, season discovery, team name normalisation, and structured file exports — all built with SE best practices including typed Python, comprehensive testing, and conventional commits.

## Table of Contents

- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Getting Started](#getting-started)
- [Data Quality](#data-quality)
- [Retry Logic](#retry-logic)
- [Currently Supported Leagues](#currently-supported-leagues)
- [Project Phases](#project-phases)
- [Tech Stack](#tech-stack)

## Architecture

```
config/
├── leagues.yaml                  ← League definitions (add a league = add a YAML block)
└── canonical_teams/              ← Per-league team name mappings
    └── nigeria_npfl.yaml

src/african_football/
├── models/
│   ├── league_model.py           ← LeagueConfig dataclass with capability flags
│   └── season_model.py           ← SeasonStatus enum + SeasonRecord (two-phase population)
├── config/
│   └── config_loader.py          ← YAML → validated LeagueConfig objects
├── scraping/
│   ├── url_builder.py            ← URL construction + archive URL builder
│   ├── season_discoverer.py      ← Discovers seasons + champions from archive pages
│   └── scraper.py                ← Core Playwright scraper with HT enrichment
└── utils/
    ├── logger.py                 ← PipelineLogger (console + file)
    ├── file_saver.py             ← JSON/TXT export with SHA256 checksums
    └── team_normalizer.py        ← Exact + fuzzy matching with review log

main.py                           ← CLI orchestrator with batch selection + retry logic
tools/                            ← Standalone utilities (extract, merge, format teams)

data/
├── raw/{country}/{league}/       ← Immutable JSON snapshots
├── exports/{country}/{league}/   ← Human-readable TXT exports
└── logs/unmatched_teams/         ← Flagged names for manual curation
```

> **Why this structure for Phase 1?** The project is separated into models, config, scraping, and utils as independent modules because Phase 2 requires them as separate importable components. Airflow DAGs need to import `SeasonDiscoverer`, `FootballScraper`, and `file_saver` as isolated tasks. Building that separation now means Phase 2 is wiring, not rewriting. If this were a Phase 1-only project, a flatter structure would work fine.

### Design Decisions

**Config-driven, not code-driven.** Adding a new African league means adding a block to `leagues.yaml`. No code changes, no redeployment. The pipeline dynamically discovers seasons, builds URLs, and resolves file paths from config alone.

**Two-phase SeasonRecord population.** SeasonDiscoverer populates identity fields (season, URL, champion, status). FootballScraper populates ingestion metadata (scraped_at, records_extracted, checksum, pipeline_version). This separation means discovery and scraping can run independently — critical for Phase 2 where Airflow orchestrates them as separate DAG tasks.

**Capability flags on LeagueConfig.** `fetch_halftime`, `fetch_venue`, `fetch_lineups`, `fetch_scorers`, `fetch_cards` — each defaults to `False` and is toggled per league in YAML. The scraper checks these flags to decide what to extract. New data dimensions are added without modifying existing scraping logic (Open/Closed Principle).

**Graceful normalisation skip.** If no canonical teams file exists for a league, the pipeline logs a warning and continues with raw team names. Raw data is still valuable without normalisation — the pipeline never crashes on missing reference data.

**Year-based ongoing season detection.** Instead of relying on empty winner columns (which breaks for historical data gaps like NPFL 2005), the pipeline combines `end_year >= current_year` with winner column state to classify seasons accurately as COMPLETED, NO_WINNER, or ONGOING.

**Awarded match detection.** Matches decided by walkover are identified from the `event__stage--block` DOM element (same structure as penalty indicators) and tagged with `awarded: true` in the output. These are excluded from halftime score enrichment since no actual play occurred.

## Data Flow

```
leagues.yaml
    │
    ▼
SeasonDiscoverer ──→ archive page ──→ list of SeasonRecords
    │                                   (status, URL, champion)
    ▼
FootballScraper ──→ results page ──→ raw match data
    │                                   (scores, dates, teams)
    │
    ├──→ HT enrichment ──→ individual match pages ──→ halftime scores
    │
    ▼
TeamNormalizer ──→ canonical YAML ──→ resolved team names
    │                                   (exact match → fuzzy → review log)
    ▼
file_saver ──→ data/raw/{country}/{league}/{season}.json    (immutable, checksummed)
           ──→ data/exports/{country}/{league}/{season}.txt  (human-readable)
```

## Getting Started

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (Python package manager)

### Installation

```bash
git clone https://github.com/idrisakorede/african-football-pipeline.git
cd african-football-pipeline

# Install dependencies
uv sync

# Install Playwright browser
uv run playwright install chromium
```

### Running the Pipeline

```bash
uv run python main.py
```

The CLI presents interactive menus:

1. **Select leagues** — pick one, multiple (`1,3,5`), a range (`1-5`), or all (`all`)
2. **For each league**, the pipeline discovers available seasons from Soccerway's archive
3. **Select seasons** — same flexible input format
4. **Pipeline runs**: scrape → normalise → save → report statistics

### Example Output

```
======================================================================
  PIPELINE RUN SUMMARY
======================================================================
  Total seasons:    1
  Succeeded:        1
  Failed:           0
  Total matches:    240
  Elapsed time:     0:24:35
  ──────────────────────────────────────────────────────────────────
  SUCCEEDED:
    Ghana Premier League 2016/2017 — 240 matches
      JSON: data/raw/ghana/gh_pl/2016-17.json
      TXT:  data/exports/ghana/gh_pl/2016-17_gh1.txt
======================================================================
```

### Running Tests

```bash
# All tests
uv run pytest -v

# Unit tests only
uv run pytest tests/unit/ -v

# Integration tests only
uv run pytest tests/integration/ -v
```

### Standalone Tools

```bash
# Extract team names from TXT exports
uv run python tools/extract_teams.py

# Merge per-season team files into master list
uv run python tools/merge_teams.py

# Reformat TXT files with aligned columns
uv run python tools/format_matches.py
```

## Data Quality

### Validation

Every scrape run validates all matches for required fields (home/away teams, scores, date, round, stage). Missing data is logged with match details and URL for manual investigation.

### Change Detection

Raw JSON files include a SHA256 checksum computed from the serialised match data. When the same season is re-scraped, comparing checksums reveals whether any data changed — supporting the CDC (Change Data Capture) strategy planned for Phase 2.

### Team Name Normalisation

The `TeamNormalizer` resolves raw scraped names to canonical forms using a two-stage strategy:

1. **Exact matching** — checks against all known aliases (case-insensitive)
2. **Fuzzy matching** — `difflib.SequenceMatcher` with configurable thresholds:
   - Above 0.85: auto-applied
   - 0.70–0.85: applied but flagged for review
   - Below 0.70: kept as-is, logged to `data/logs/unmatched_teams/`

Canonical team files are YAML with explicit aliases, curated per league as the pipeline discovers new name variations.

### Awarded Matches

Matches decided by walkover are detected, tagged with `awarded: true`, and excluded from halftime score enrichment. In TXT exports they display as `3-0 (awarded)`.

## Retry Logic

Season-level retry with backoff: attempt 1 (immediate) → attempt 2 (10s delay) → attempt 3 (30s delay). If all attempts fail, the pipeline logs the failure and continues to the next season. The run summary shows what succeeded and what failed.

## Currently Supported Leagues

| Country | League | Code | Submission Code |
|---------|--------|------|-----------------|
| Nigeria | Nigeria Professional Football League | npfl | ng1 |
| Nigeria | Nigeria National League | nnl | ng2 |
| Ghana | Ghana Premier League | gh_pl | gh1 |
| Egypt | Egypt Premier League | eg_pl | eg1 |

Adding a new league:

```yaml
# config/leagues.yaml
- code: sa_pl
  name: South Africa Premier Division
  country: south-africa
  slug: premier-division
  fetch_halftime: true
  submission_code: sa1
```

## Project Phases

### Phase 1 — Local Pipeline (current)

Config-driven Python scraper with structured file output, team normalisation, comprehensive testing, and CLI batch processing.

### Phase 2 — Warehouse-Centric Architecture

- **S3** for raw data lake storage
- **Airflow** for orchestration (replaces CLI interaction)
- **Snowflake** for analytical warehouse
- **dbt** for transformation layer
- **Great Expectations** for automated data quality gates
- **CDC logic** with row_hash and natural_key for incremental processing

### Phase 3 — Lakehouse Architecture

- Rebuild pipeline in **Databricks** with Bronze / Silver / Gold medallion layers
- **Delta Lake** with time travel for data versioning
- Architecture comparison: Snowflake Streams vs Delta time travel for CDC

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.12 | Core language |
| uv | Package management |
| Playwright | Browser automation |
| PyYAML | Configuration |
| pytest | Testing framework |
| Ruff | Linting and formatting |
| Pylance | Static type checking |

## License

MIT — see [LICENSE](LICENSE) for details.