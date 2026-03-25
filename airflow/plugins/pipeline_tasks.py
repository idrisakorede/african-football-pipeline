"""
pipeline_tasks.py — Task logic for the African football pipeline DAG.

Contains the heavy lifting: season discovery, match scraping, team
normalisation, and file saving. Imported by the DAG file to keep
DAG parsing lightweight.
"""

import asyncio
from pathlib import Path
from typing import Any

from african_football.config.config_loader import load_leagues
from african_football.models.league_model import LeagueConfig
from african_football.models.season_model import SeasonRecord, SeasonStatus
from african_football.scraping.scraper import FootballScraper
from african_football.scraping.season_discoverer import SeasonDiscoverer
from african_football.utils.file_saver import save_json, save_txt
from african_football.utils.logger import PipelineLogger
from african_football.utils.team_normalizer import TeamNormalizer

CONFIG_PATH = "/opt/airflow/project/config/leagues.yaml"
CANONICAL_TEAMS_DIR = "/opt/airflow/project/config/canonical_teams"
DATA_RAW_DIR = "/opt/airflow/project/data/raw"
DATA_EXPORTS_DIR = "/opt/airflow/project/data/exports"
REVIEW_DIR = "/opt/airflow/project/data/logs/unmatched_teams"


def _run_async(coroutine):
    """Run an async coroutine synchronously for Airflow tasks."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coroutine)
    finally:
        loop.close()


def load_all_league_configs() -> list[dict]:
    """Load all league configs from YAML as serialisable dicts."""

    leagues = load_leagues(CONFIG_PATH)
    return [
        {
            "code": league.code,
            "name": league.name,
            "country": league.country,
            "slug": league.slug,
            "fetch_halftime": league.fetch_halftime,
            "submission_code": league.submission_code,
            "fetch_venue": league.fetch_venue,
            "fetch_lineups": league.fetch_lineups,
            "fetch_scorers": league.fetch_scorers,
            "fetch_cards": league.fetch_cards,
        }
        for league in leagues
    ]


def discover_and_build_jobs(league_configs: list[dict]) -> list[dict]:
    """Discover seasons for all leagues and return flat job list."""
    jobs = []
    for league_dict in league_configs:
        seasons = _run_async(_discover_seasons_async(league_dict))
        for season_dict in seasons:
            jobs.append({"league": league_dict, "season": season_dict})
    return jobs


def scrape_single_job(job: dict) -> dict:
    """Scrape a single league-season pair."""
    return _run_async(_scrape_season_async(job["league"], job["season"]))


def summarise(results: list[dict]) -> None:
    """Log a summary of all scraping results."""
    succeeded = [result for result in results if result.get("status") == "success"]
    failed = [result for result in results if result.get("status") != "success"]
    total_matches = sum(result.get("matches", 0) for result in succeeded)

    summary = [
        "=" * 70,
        "  PIPELINE RUN SUMMARY",
        "=" * 70,
        f"  Succeeded: {len(succeeded)}",
        f"  Failed:    {len(failed)}",
        f"  Total matches: {total_matches}",
    ]

    for result in succeeded:
        summary.append(
            f"    {result['league']} {result['season']} — {result['matches']} matches"
        )

    summary.append("=" * 70)
    print("\n".join(summary))


async def _discover_seasons_async(league_dict: dict) -> list[dict]:
    """Discover all scrapeable seasons for a league."""

    league = LeagueConfig(**league_dict)
    logger = PipelineLogger(log_dir="/opt/airflow/logs/pipeline")

    async with SeasonDiscoverer(league, logger, headless=True) as discoverer:
        seasons = await discoverer.discover()

    return [season.to_dict() for season in seasons if season.is_scrapeable()]


async def _scrape_season_async(league_dict: dict, season_dict: dict) -> dict[str, Any]:
    """Scrape a single season, normalise names, and save outputs."""

    league = LeagueConfig(**league_dict)
    season = SeasonRecord(
        season=season_dict["season"],
        start_year=season_dict["start_year"],
        end_year=season_dict["end_year"],
        status=SeasonStatus(season_dict["status"]),
        url=season_dict["url"],
        champion=season_dict.get("champion"),
        champion_url=season_dict.get("champion_url"),
    )
    logger = PipelineLogger(log_dir="/opt/airflow/logs/pipeline")

    # Scrape
    async with FootballScraper(league, season, logger, headless=True) as scraper:
        data = await scraper.scrape()

    # Normalise team names
    canonical_path = Path(CANONICAL_TEAMS_DIR) / f"{league.country}_{league.code}.yaml"
    season_string = f"{season.start_year}-{str(season.end_year)[-2:]}"

    if canonical_path.exists():
        review_log = (
            Path(REVIEW_DIR) / f"{league.country}_{league.code}_{season_string}.txt"
        )
        normalizer = TeamNormalizer(canonical_path, review_log)
        for stage in data.get("stages", []):
            for match in stage.get("matches", []):
                match["home_team"] = normalizer.resolve(match["home_team"])
                match["away_team"] = normalizer.resolve(match["away_team"])

    # Save
    json_path = save_json(
        data, league, season.start_year, season.end_year, base_dir=DATA_RAW_DIR
    )
    txt_path = save_txt(
        data, league, season.start_year, season.end_year, base_dir=DATA_EXPORTS_DIR
    )

    # Extract raw teams if no canonical file
    if not canonical_path.exists():
        all_teams = set()
        for stage in data.get("stages", []):
            for match in stage.get("matches", []):
                all_teams.add(match["home_team"])
                all_teams.add(match["away_team"])

        teams_dir = Path(DATA_EXPORTS_DIR) / league.country / league.code / "teams"
        teams_dir.mkdir(parents=True, exist_ok=True)
        teams_file = teams_dir / f"{season_string}_teams.txt"

        try:
            with open(teams_file, "w", encoding="utf-8") as file:
                for team in sorted(all_teams):
                    file.write(f"{team}\n")
        except OSError as e:
            print(f"Error writing to {teams_file}: {e}")

    return {
        "league": league.name,
        "season": season.season,
        "status": "success",
        "matches": data.get("total_matches", 0),
        "json_path": str(json_path),
        "txt_path": str(txt_path),
    }
