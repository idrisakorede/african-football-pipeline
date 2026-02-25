"""
file_saver.py — File persistence utilities for the African football pipeline.

Handles saving scraped match data to the structured data lake directory
layout. Supports JSON (raw layer) and TXT (export layer) formats.

Output paths follow the convention:
    data/raw/{country}/{league_code}/{season}.json
    data/exports/{country}/{league_code}/{season}.txt

Typical usage:
    from utils.file_saver import save_json, save_txt

    save_json(data, league_config, start_year, end_year)
    save_txt(data, league_config, start_year, end_year)
"""

import hashlib
import json
from collections import defaultdict
from pathlib import Path

from src.models import LeagueConfig

# Playoff round display order for TXT formatting
PLAYOFF_ROUND_ORDER: dict[str, int] = {
    "quarter-finals": 1,
    "quarter-final": 1,
    "quarterfinals": 1,
    "quarterfinal": 1,
    "semi-finals": 2,
    "semi-final": 2,
    "semifinals": 2,
    "semifinal": 2,
    "final": 3,
    "finals": 3,
}


def _build_season_str(start_year: int, end_year: int) -> str:
    """
    Build a standardised season string for use in file names.

    Args:
        start_year: The year the season starts (e.g. 2024).
        end_year:   The year the season ends (e.g. 2025).

    Returns:
        A season string in the format '2024-25'.
    """
    return f"{start_year}-{str(end_year)[-2:]}"


def _compute_checksum(data: dict) -> str:
    """
    Compute a SHA256 checksum of the serialised match data.

    Used to detect whether scraped content has changed between runs,
    supporting the CDC (Change Data Capture) strategy in later phases.

    Args:
        data: The scraped match data dictionary to checksum.

    Returns:
        A hex-encoded SHA256 digest string.
    """
    serialised = json.dumps(data, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()


def _build_json_path(
    league: LeagueConfig,
    start_year: int,
    end_year: int,
    base_dir: str | Path = "data/raw",
) -> Path:
    """
    Build the output path for a raw JSON file.

    Args:
        league:     The league configuration object.
        start_year: The year the season starts.
        end_year:   The year the season ends.
        base_dir:   Root directory for raw data. Defaults to 'data/raw'.

    Returns:
        A Path object for the JSON output file.
    """
    season_str = _build_season_str(start_year, end_year)
    return Path(base_dir) / league.country / league.code / f"{season_str}.json"


def _build_txt_path(
    league: LeagueConfig,
    start_year: int,
    end_year: int,
    base_dir: str | Path = "data/exports",
) -> Path:
    """
    Build the output path for a TXT export file.

    Args:
        league:     The league configuration object.
        start_year: The year the season starts.
        end_year:   The year the season ends.
        base_dir:   Root directory for exports. Defaults to 'data/exports'.

    Returns:
        A Path object for the TXT output file.
    """
    season_str = _build_season_str(start_year, end_year)
    filename = (
        f"{season_str}_{league.submission_code}.txt"
        if league.submission_code
        else f"{season_str}.txt"
    )
    return Path(base_dir) / league.country / league.code / filename


def save_json(
    data: dict,
    league: LeagueConfig,
    start_year: int,
    end_year: int,
    base_dir: str | Path = "data/raw",
) -> Path:
    """
    Save scraped match data as a raw JSON file with checksum metadata.

    Sorts matches within each stage by round number before saving.
    Attaches a SHA256 checksum to the output for downstream CDC use.
    The raw file is treated as immutable once written — it represents
    exactly what was scraped at that point in time.

    Args:
        data:       The scraped match data dictionary.
        league:     The league configuration object.
        start_year: The year the season starts.
        end_year:   The year the season ends.
        base_dir:   Root directory for raw data. Defaults to 'data/raw'.

    Returns:
        The Path where the JSON file was written.
    """
    output_path = _build_json_path(league, start_year, end_year, base_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    data_copy = data.copy()

    # Sort matches within each stage by round number, None values last
    for stage in data_copy.get("stages", []):
        stage["matches"] = sorted(
            stage["matches"],
            key=lambda m: (
                m.get("round_number") is None,
                m.get("round_number", 999),
                m.get("date", ""),
            ),
        )

    # Attach checksum for CDC and auditability
    data_copy["checksum"] = _compute_checksum(data_copy)

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data_copy, file, indent=2, ensure_ascii=False)

    return output_path


def save_txt(
    data: dict,
    league: LeagueConfig,
    start_year: int,
    end_year: int,
    base_dir: str | Path = "data/exports",
) -> Path:
    """
    Save scraped match data as a human-readable TXT export.

    Formats matches grouped by stage, round, and date. Handles
    penalty shootouts, halftime scores, and playoff round ordering.
    Stages are written in reverse order to present chronologically
    (Soccerway displays latest stage first).

    Args:
        data:       The scraped match data dictionary.
        league:     The league configuration object.
        start_year: The year the season starts.
        end_year:   The year the season ends.
        base_dir:   Root directory for exports. Defaults to 'data/exports'.

    Returns:
        The Path where the TXT file was written.
    """
    output_path = _build_txt_path(league, start_year, end_year, base_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_matches = [m for s in data["stages"] for m in s["matches"]]
    teams = {m["home_team"] for m in all_matches} | {
        m["away_team"] for m in all_matches
    }

    with open(output_path, "w", encoding="utf-8") as file:
        file.write(f"= {data['season']}\n")
        file.write(f"# Teams      {len(teams)}\n")
        file.write(f"# Matches    {len(all_matches)}\n")

        if len(data["stages"]) > 1:
            stage_info = "  ".join(
                f"{s['stage_name']} ({s['total_matches']})"
                for s in reversed(data["stages"])
            )
            file.write(f"# Stages     {stage_info}\n")

        file.write("\n")

        for stage in reversed(data["stages"]):
            if len(data["stages"]) > 1:
                sep = "=" * 70
                file.write(f"\n{sep}\n  {stage['stage_name'].upper()}\n{sep}\n\n")

            rounds = defaultdict(list)
            for m in stage["matches"]:
                round_key = m.get("round") or "Unknown Round"
                rounds[round_key].append(m)

            sorted_rounds = sorted(
                rounds.items(),
                key=lambda item: _round_sort_key(item[0], item[1]),
            )

            for round_name, round_matches in sorted_rounds:
                file.write(f"» {_format_round_header(round_name)}\n")

                dates = defaultdict(list)
                for m in sorted(round_matches, key=lambda x: x.get("date") or ""):
                    date = m["date"].split()[0] if m["date"] else "Unknown"
                    dates[date].append(m)

                for date in sorted(dates.keys()):
                    file.write(f"  {date}\n")
                    for m in dates[date]:
                        time = (
                            m["date"].split()[1]
                            if m["date"] and len(m["date"].split()) > 1
                            else "15.00"
                        )
                        score = _format_score(m)
                        file.write(
                            f"    {time:6} {m['home_team']:24} v "
                            f"{m['away_team']:24} {score}\n"
                        )
                file.write("\n")

    return output_path


def _round_sort_key(round_name: str, matches: list) -> tuple:
    """
    Generate a sort key for ordering rounds in TXT output.

    Playoff rounds (finals, semis, quarters) sort before regular rounds.
    Regular rounds sort by round number. Unknown rounds sort last.

    Args:
        round_name: The display name of the round (e.g. 'Round 1', 'Final').
        matches:    The list of matches in that round (used to read round_number).

    Returns:
        A tuple used for sorting: (priority, order, name).
    """
    round_lower = round_name.lower()
    playoff_order = PLAYOFF_ROUND_ORDER.get(round_lower)
    round_num = matches[0].get("round_number") if matches else None

    if playoff_order is not None:
        return (0, playoff_order, round_name)
    elif round_num is not None:
        return (1, round_num, round_name)
    else:
        return (2, 999, round_name)


def _format_round_header(round_name: str) -> str:
    """
    Format a round name for display in TXT output.

    Converts 'Round N' to 'Matchday N' for regular rounds.
    Playoff and other round names are returned as-is.

    Args:
        round_name: The raw round name from scraped data.

    Returns:
        A formatted string for display in the TXT export.
    """
    if round_name.lower().startswith("round"):
        number = round_name.split()[-1]
        return f"Matchday {number}"
    return round_name


def _format_score(match: dict) -> str:
    """
    Format the score string for a match in TXT output.

    Handles three cases: penalty shootouts (shows pen result and FT score),
    regular matches with halftime scores, and regular matches without.

    Args:
        match: A match dictionary containing score and metadata fields.

    Returns:
        A formatted score string (e.g. '2-1', '2-1 (1-0)', '3-2 pen (2-2)').
    """
    if match.get("penalty_shootout") and match.get("full_time_score"):
        return (
            f"{match['home_score']}-{match['away_score']} pen "
            f"({match['full_time_score']})"
        )

    if match["home_score"] and match["away_score"]:
        score = f"{match['home_score']}-{match['away_score']}"
        if match.get("half_time_score"):
            score += f" ({match['half_time_score']})"
        return score

    return "vs"
