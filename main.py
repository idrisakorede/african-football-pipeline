"""
main.py — CLI entry point for the African football pipeline.

Orchestrates the full Phase 1 pipeline: load configuration, discover
seasons, scrape match data, normalise team names, and save outputs.
Supports multi-league and multi-season batch runs with retry logic
for failed scrapes.

Typical usage:
    uv run python main.py
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from african_football.config.config_loader import load_leagues
from african_football.models.league_model import LeagueConfig
from african_football.models.season_model import SeasonRecord
from african_football.scraping.scraper import FootballScraper
from african_football.scraping.season_discoverer import SeasonDiscoverer
from african_football.utils.file_saver import save_json, save_txt
from african_football.utils.logger import PipelineLogger
from african_football.utils.team_normalizer import TeamNormalizer

# ---------------------------- Constants ----------------------------------------

CONFIG_PATH = "config/leagues.yaml"
CANONICAL_TEAMS_DIR = "config/canonical_teams"
MAX_RETRIES = 3
RETRY_DELAYS = [0, 10, 30]  # seconds before each attempt
HEADLESS = False  # set True for pipeline use, False for development


# ---------------------------- Selection Parsing ---------------------------------


def parse_selection(user_input: str, max_index: int) -> list[int]:
    """
    Parse a flexible user selection string into a list of indices.

    Supports single values, comma-separated values, ranges, and
    mixed combinations. Returns 1-based indices validated against
    the maximum available index.

    Accepted formats:
        '1'         → [1]
        '1,3,5'     → [1, 3, 5]
        '1-5'       → [1, 2, 3, 4, 5]
        '1-3,7,9-12'→ [1, 2, 3, 7, 9, 10, 11, 12]
        'all' or '0'→ [1, 2, ..., max_index]

    Args:
        user_input: The raw input string from the user.
        max_index:  The highest valid index (1-based).

    Returns:
        A sorted list of unique 1-based indices.

    Raises:
        ValueError: If the input contains invalid characters or
                    indices outside the valid range.
    """
    cleaned = user_input.strip().lower()

    if not cleaned:
        return []

    if cleaned in ("all", "0"):
        return list(range(1, max_index + 1))

    indices: set[int] = set()

    for part in cleaned.split(","):
        part = part.strip()
        if not part:
            continue

        if "-" in part:
            bounds = part.split("-", 1)
            start = int(bounds[0].strip())
            end = int(bounds[1].strip())
            if start < 1 or end > max_index or start > end:
                raise ValueError(
                    f"Range {start}-{end} is out of bounds (1-{max_index})"
                )
            indices.update(range(start, end + 1))
        else:
            num = int(part)
            if num < 1 or num > max_index:
                raise ValueError(f"Index {num} is out of bounds (1-{max_index})")
            indices.add(num)

    return sorted(indices)


# ---------------------- Display Menus -------------------------------------------


def display_league_menu(leagues: list[LeagueConfig]) -> list[LeagueConfig]:
    """
    Display available leagues and return the user's selection.

    Shows a numbered list of all leagues from the configuration file.
    The user can select one, multiple, or all leagues using the
    flexible selection format.

    Args:
        leagues: List of LeagueConfig objects loaded from leagues.yaml.

    Returns:
        A list of selected LeagueConfig objects.
    """
    print("\n" + "=" * 70)
    print("  AVAILABLE LEAGUES")
    print("=" * 70)

    for i, league in enumerate(leagues, 1):
        print(f"  {i}. {league.name} ({league.country})")

    print("\n  0 / all — Select all leagues")
    print("=" * 70)

    while True:
        try:
            user_input = input("\nSelect league(s): ").strip()
            if not user_input:
                continue
            indices = parse_selection(user_input, len(leagues))
            selected = [leagues[i - 1] for i in indices]
            names = ", ".join(n.name for n in selected)
            print(f"\nSelected: {names}")
            return selected
        except (ValueError, IndexError) as e:
            print(f"Invalid selection: {e}. Try again.")


def display_season_menu(
    seasons: list[SeasonRecord], league: LeagueConfig
) -> list[SeasonRecord]:
    """
    Display discovered seasons for a league and return the user's selection.

    Shows only scrapeable (COMPLETED) seasons. Non-scrapeable seasons
    are listed separately for visibility but cannot be selected.

    Args:
        seasons: List of SeasonRecord objects from SeasonDiscoverer.
        league:  The league these seasons belong to.

    Returns:
        A list of selected SeasonRecord objects.
    """
    scrapeable = [s for s in seasons if s.is_scrapeable()]
    skipped = [s for s in seasons if not s.is_scrapeable()]

    print(f"\n{'=' * 70}")
    print(f"  SEASONS — {league.name}")
    print("=" * 70)

    for i, season in enumerate(scrapeable, 1):
        champion_str = f" - Champion: {season.champion}" if season.champion else ""
        print(f"  {i}. {season.season}{champion_str}")

    if skipped:
        print(f"\n Skipped ({len(skipped)}):")
        for s in skipped:
            print(f"    {s.season} [{s.status.value}]")

    print("\n  0 / all - Select all seasons")
    print("=" * 70)

    while True:
        try:
            user_input = input("\nSelect season(s): ").strip()
            if not user_input:
                continue
            indices = parse_selection(user_input, len(scrapeable))
            selected = [scrapeable[i - 1] for i in indices]
            names = ", ".join(s.season for s in selected)
            print(f"\nSelected: {names}")
            return selected
        except (ValueError, IndexError) as e:
            print(f"Invalid selection: {e}. Try again.")


# --------------------------------- Retry Logic -----------------------------------


async def retry_scrape(
    league: LeagueConfig,
    season: SeasonRecord,
    logger: PipelineLogger,
    headless: bool = HEADLESS,
) -> tuple[dict[str, Any] | None, str | None]:
    """
    Attempt to scrape a season with retry logic on failure.

    Tries up to MAX_RETRIES times with increasing delays between
    attempts. Returns the scraped data and statistics report on
    success, or (None, None) if all retries are exhausted.

    Args:
        league:   League configuration for the season.
        season:   SeasonRecord to scrape.
        logger:   Pipeline logger instance.
        headless: Whether to run the browser in headless mode.

    Returns:
        A tuple of (data_dict, statistics_report) on success,
        or (None, None) if all attempts failed.
    """

    for attempt in range(1, MAX_RETRIES + 1):
        delay = RETRY_DELAYS[attempt - 1]
        if delay > 0:
            logger.log(f"Waiting {delay}s before retry attempt...")
            await asyncio.sleep(delay)

        try:
            async with FootballScraper(league, season, logger, headless) as scraper:
                data = await scraper.scrape()
                report = scraper.get_statistics_report()
                return data, report

        except Exception as e:
            logger.log(
                f"Attempt {attempt}/{MAX_RETRIES} failed for "
                f"{league.name} {season.season}: {e}",
                level="ERROR",
            )

    logger.log(
        f"ALL {MAX_RETRIES} attempts exhausted for {league.name} {season.season} ",
        level="ERROR",
    )
    return None, None


# --------------------------- Normalisation ----------------------------------------


def normalise_team_names(
    data: dict[str, Any],
    league: LeagueConfig,
    season: SeasonRecord,
    logger: PipelineLogger,
) -> dict[str, Any]:
    """
    Normalize team names in scraped data using canonical team files.

    Looks for a canonical teams YAML file matching the league code.
    If the file does not exist, logs a warning and returns the data
    unchanged — raw data is still valuable without normalised names.

    Args:
        data:    The scraped match data dictionary.
        league:  League configuration for path resolution.
        season:  SeasonRecord for review log naming.
        logger:  Pipeline logger instance.

    Returns:
        The data dictionary with team names normalised where possible.
    """
    canonical_path = Path(CANONICAL_TEAMS_DIR) / f"{league.country}_{league.code}.yaml"

    if not canonical_path.exists():
        logger.log(
            f"No canonical teams file for {league.code} at {canonical_path}. "
            "skipping normalisation.",
            level="WARNING",
        )
        return data

    season_str = f"{season.start_year}-{str(season.end_year)[-2:]}"
    review_log_path = (
        Path("data/logs/unmatched_teams")
        / f"{league.country}_{league.code}_{season_str}.txt"
    )

    normalizer = TeamNormalizer(canonical_path, review_log_path)

    for stage in data.get("stages", []):
        for match in stage.get("matches", []):
            match["home_team"] = normalizer.resolve(match["home_team"])
            match["away_team"] = normalizer.resolve(match["away_team"])

    logger.log(
        f"Team names normalized for {league.code} {season.season}", level="SUCCESS"
    )
    return data


# ----------------------------------- Pipeline Run -----------------------------------


async def run_single(
    league: LeagueConfig,
    season: SeasonRecord,
    logger: PipelineLogger,
) -> dict[str, Any]:
    """
    Run the full pipeline for a single league season.

    Scrapes data with retry logic, normalises team names, saves
    JSON and TXT outputs, and returns a result summary.

    Args:
        league: League configuration.
        season: SeasonRecord to process.
        logger: Pipeline logger instance.

    Returns:
        A result dictionary with status, paths, and metadata.
    """
    result: dict[str, Any] = {
        "league": league.name,
        "season": season.season,
        "status": "pending",
        "json_path": None,
        "txt_path": None,
        "matches": 0,
        "report": None,
    }

    logger.log_section(f"SCRAPING: {league.name} - {season.season}")

    # Scrape with retry
    data, report = await retry_scrape(league, season, logger)

    if data is None:
        result["status"] = "failed"
        logger.log(
            f"FAILED: {league.name} {season.season} - all retries exhausted",
            level="ERROR",
        )
        return result

    # Normalise team names
    data = normalise_team_names(data, league, season, logger)

    # Save outputs
    json_path = save_json(data, league, season.start_year, season.end_year)
    txt_path = save_txt(data, league, season.start_year, season.end_year)

    result.update(
        {
            "status": "success",
            "json_path": str(json_path),
            "txt_path": str(txt_path),
            "matches": data.get("total_matches", 0),
            "report": report,
        }
    )

    logger.log(f"Saved: {json_path}", level="SUCCESS")
    logger.log(f"Saved: {txt_path}", level="SUCCESS")

    if report:
        logger.log(report)

    return result


# ──------------------------------ Run Summary -----------------------------------


def print_run_summary(results: list[dict[str, Any]], start_time: datetime) -> None:
    """
    Print a summary of the entire pipeline run.

    Shows succeeded and failed seasons with match counts and file
    paths. Displays total elapsed time.

    Args:
        results:    List of result dictionaries from run_single.
        start_time: When the pipeline run started.
    """
    elapsed = datetime.now() - start_time
    succeeded = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]

    print("\n" + "=" * 70)
    print("  PIPELINE RUN SUMMARY")
    print("=" * 70)
    print(f"  Total seasons:    {len(results)}")
    print(f"  Succeeded:        {len(succeeded)}")
    print(f"  Failed:           {len(failed)}")
    print(f"  Total matches:    {sum(r['matches'] for r in succeeded)}")
    print(f"  Elapsed time:     {elapsed}")

    if succeeded:
        print(f"\n  {'─' * 66}")
        print("  SUCCEEDED:")
        for r in succeeded:
            print(f"    {r['league']} {r['season']} — {r['matches']} matches")
            print(f"      JSON: {r['json_path']}")
            print(f"      TXT:  {r['txt_path']}")

    if failed:
        print(f"\n  {'─' * 66}")
        print("  FAILED:")
        for r in failed:
            print(f"    {r['league']} {r['season']}")

    print("=" * 70)


# ──--------------------------------- Entry Point ----------------------------------


async def main() -> None:
    """
    Main entry point for the African football pipeline CLI.

    Loads configuration, presents interactive menus for league and
    season selection, runs the scraping pipeline for each selection,
    and prints a final summary.
    """
    logger = PipelineLogger()
    start_time = datetime.now()

    logger.log_section("AFRICAN FOOTBALL PIPELINE")
    logger.log(f"Started at {start_time.isoformat()}")

    # Load league configuration
    try:
        leagues = load_leagues(CONFIG_PATH)
    except (FileNotFoundError, ValueError) as e:
        logger.log(f"Failed to load config: {e}", level="ERROR")
        sys.exit(1)

    if not leagues:
        logger.log("No leagues found in configuration.", level="ERROR")
        sys.exit(1)

    logger.log(f"Loaded {len(leagues)} league(s) from {CONFIG_PATH}")

    # Select leagues
    selected_leagues = display_league_menu(leagues)

    results: list[dict[str, Any]] = []

    for league in selected_leagues:
        logger.log_section(f"DISCOVERING SEASONS: {league.name}")

        # Discover seasons
        try:
            async with SeasonDiscoverer(
                league, logger, headless=HEADLESS
            ) as discoverer:
                seasons = await discoverer.discover()
        except Exception as e:
            logger.log(
                f"Failed to discover seasons for {league.name}: {e}",
                level="ERROR",
            )
            continue

        if not seasons:
            logger.log(
                f"No seasons found for {league.name}",
                level="WARNING",
            )
            continue

        scrapeable = [s for s in seasons if s.is_scrapeable()]
        if not scrapeable:
            logger.log(
                f"No scrapeable seasons for {league.name}",
                level="WARNING",
            )
            continue

        # Select seasons
        selected_seasons = display_season_menu(seasons, league)

        # Run pipeline for each selected season
        for season in selected_seasons:
            result = await run_single(league, season, logger)
            results.append(result)

    # Final summary
    print_run_summary(results, start_time)
    logger.log(f"Log file: {logger.log_file}")


if __name__ == "__main__":
    asyncio.run(main())
