"""
extract_teams.py — Extract unique team names from match data files.

Standalone CLI tool for extracting team names from TXT match exports.
Used for building and maintaining canonical team name files. Not part
of the automated pipeline.

Usage:
    uv run python tools/extract_teams.py

    Then select league and enter:
    - A number (index) of the file
    - The filename
    - "all" to extract from all files
"""

import re
from pathlib import Path

from tools.shared import DATA_DIR, find_available_files, select_file, select_league


def extract_teams_from_file(filepath: str) -> set[str]:
    """
    Extract all unique team names from a match data file.

    Handles both regular seasons and seasons with stages
    (groups/knockout phases).

    Args:
        filepath: Path to the match data file.

    Returns:
        Set of unique team names.
    """
    teams: set[str] = set()

    try:
        with open(filepath, "r", encoding="utf-8") as file:
            for line in file:
                if line.startswith(("=", "#", "»")):
                    continue

                if any(
                    keyword in line.lower()
                    for keyword in ["group", "stage", "playoff", "round", "phase"]
                ):
                    if len(line.strip()) < 50 and " v " not in line:
                        continue

                stripped = line.strip()
                if stripped and len(stripped) < 15 and " v " not in line:
                    continue

                if " v " in line:
                    parts = line.split(" v ")
                    if len(parts) >= 2:
                        home_section = parts[0]
                        home_team = re.sub(
                            r"^\s+\d+[\.:]\d+\s+", "", home_section
                        ).strip()

                        away_section = parts[1]
                        away_team = re.split(
                            r"\s{2,}|\s+\d+-\d+|\s+vs\s+|\s+\[", away_section
                        )[0].strip()

                        if home_team and len(home_team) > 2:
                            teams.add(home_team)
                        if away_team and len(away_team) > 2:
                            teams.add(away_team)
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return teams
    except UnicodeDecodeError as e:
        print(f"Encoding error reading {filepath}: {e}")
        return teams

    return teams


def extract_season_code(filepath: str) -> str:
    """Extract season code from filepath."""
    return Path(filepath).stem


def save_teams_to_file(teams: set[str], output_file: str) -> None:
    """Save team names to file, one per line, alphabetically sorted."""
    sorted_teams = sorted(teams)
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(output_file, "w", encoding="utf-8") as file:
            for team in sorted_teams:
                file.write(f"{team}\n")
        print(f"Saved {len(sorted_teams)} teams to: {output_file}")
    except OSError as e:
        print(f"Error writing to {output_file}: {e}")


def process_single_file(filepath: str, league: dict) -> tuple:
    """Process a single file and return teams and output path."""
    print(f"\nExtracting teams from: {Path(filepath).name}")
    teams = extract_teams_from_file(filepath)

    if not teams:
        print(f"No teams found in {Path(filepath).name}!")
        return set(), None, None

    print(f"Found {len(teams)} unique teams")

    season_code = extract_season_code(filepath)
    output_file = (
        DATA_DIR
        / "exports"
        / league["country"]
        / league["code"]
        / "teams"
        / f"{season_code}_teams.txt"
    )

    return teams, str(output_file), season_code


def process_all_files(available_files: list[str], league: dict) -> None:
    """Process all available files."""
    print("\n" + "=" * 70)
    print("PROCESSING ALL FILES")
    print("=" * 70)

    all_results = []
    total_teams: set[str] = set()

    for filepath in available_files:
        teams, output_file, season_code = process_single_file(filepath, league)

        if teams and output_file:
            save_teams_to_file(teams, output_file)
            all_results.append((season_code, teams, output_file))
            total_teams.update(teams)

            print(f"\n  Teams in {season_code}:")
            for i, team in enumerate(sorted(teams), 1):
                print(f"    {i:2d}. {team}")
            print()

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Processed {len(all_results)} files")
    print(f"Total unique teams across all seasons: {len(total_teams)}")

    for season_code, teams, output_file in all_results:
        print(f"   {season_code}: {len(teams)} teams -> {Path(output_file).name}")

    print("=" * 70 + "\n")


def main() -> None:
    """Main execution."""
    league = select_league(tool_name="AFRICAN FOOTBALL TEAMS EXTRACTOR")

    print(f"\nSelected: {league['name']}")
    print("=" * 70)

    available_files = find_available_files(league)

    if not available_files:
        exports_path = DATA_DIR / "exports" / league["country"] / league["code"]
        print(f"\nNo match data files found in '{exports_path}'")
        print("Please run the pipeline first to generate match data files.")
        return

    print(f"\nFound {len(available_files)} match data file(s):")
    for i, file in enumerate(available_files, 1):
        print(f"  {i}. {Path(file).name}")

    print("\nEnter the file to extract teams from:")
    print("  Enter a number (e.g., 1)")
    print("  Or enter filename")
    print("  Or enter 'all' to extract from all files")
    print("=" * 70 + "\n")

    choice = select_file(available_files)

    if choice is None:
        return

    if choice == "all":
        process_all_files(available_files, league)
        return

    teams, output_file, season_code = process_single_file(choice, league)

    if not teams:
        return

    save_teams_to_file(teams, output_file)

    print("\n" + "=" * 70)
    print(f"TEAMS FROM {season_code.upper()} — {league['name']}")
    print("=" * 70)
    for i, team in enumerate(sorted(teams), 1):
        print(f"  {i:2d}. {team}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
