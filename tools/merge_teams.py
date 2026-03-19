"""
merge_teams.py — Merge season team files into a master list.

Standalone CLI tool that reads all per-season team files and generates
a single file with all unique teams plus appearance statistics.
Not part of the automated pipeline.

Usage:
    uv run python tools/merge_teams.py

Output:
    data/exports/{country}/{league}/teams/all_teams.txt
    data/exports/{country}/{league}/teams/all_teams_with_stats.txt
"""

from collections import defaultdict
from pathlib import Path

from tools.shared import DATA_DIR, find_available_files, select_league


def read_teams_from_file(filepath: str) -> set:
    """
    Read team names from a single teams file

    Args:
        filepath: Path to the teams file

    Returns:
        Set of team names
    """
    teams = set()

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                team = line.strip()
                if team:  # Skip empty lines
                    teams.add(team)
    except Exception as e:
        print(f"⚠️  Error reading {Path(filepath).name}: {e}")

    return teams


def save_all_teams(teams: set, output_file: str, league_name: str) -> None:
    """
    Save all unique teams to file with statistics

    Args:
        teams: Set of all unique team names
        output_file: Path to output file
        team_appearances: Dictionary mapping team names to list of seasons
        league_name: Name of the league (NPFL or NNL)
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Sort teams alphabetically
    sorted_teams = sorted(teams)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"# All Unique Teams in {league_name} History\n")
        f.write(f"# Total: {len(sorted_teams)} teams\n")
        f.write("#" + "=" * 68 + "\n\n")

        for team in sorted_teams:
            f.write(f"{team}\n")

    print(f"✅ Saved {len(sorted_teams)} unique teams to: {output_file}")


def save_teams_with_stats(
    teams: set, output_file: str, team_appearances: dict, league_name: str
) -> None:
    """
    Save all teams with statistics about their appearances

    Args:
        teams: Set of all unique team names
        output_file: Path to output file
        team_appearances: Dictionary mapping team names to list of seasons
        league_name: Name of the league (NPFL or NNL)
    """
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Sort teams by number of appearances (descending), then alphabetically
    sorted_teams = sorted(teams, key=lambda t: (-len(team_appearances[t]), t))

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"# All {league_name} Teams with Appearance Statistics\n")
        f.write(f"# Total: {len(sorted_teams)} unique teams\n")
        f.write("#" + "=" * 68 + "\n\n")

        for team in sorted_teams:
            seasons = team_appearances[team]
            season_count = len(seasons)
            seasons_str = ", ".join(sorted(seasons))

            f.write(f"{team}\n")
            f.write(f"  Seasons: {season_count} ({seasons_str})\n\n")

    print(f"✅ Saved detailed statistics to: {output_file}")


def main():
    """Main execution"""

    # Select league
    league = select_league(tool_name="AFRICAN FOOTBALL TEAMS MERGER")

    print(f"\n📋 Selected: {league['name']}")
    print("=" * 70)

    # Find all teams files
    teams_files = find_available_files(league, subfolder="teams")

    if not teams_files:
        exports_path = (
            DATA_DIR / "exports" / league["country"] / league["code"] / "teams"
        )
        print(f"\nNo teams files found in '{exports_path}'")

    print(f"\n📁 Found {len(teams_files)} teams file(s):")

    # Read teams from all files
    all_teams = set()
    team_appearances = defaultdict(list)  # Track which seasons each team appears in
    season_stats = []

    for filepath in teams_files:
        filename = Path(filepath).name
        season_code = filename.replace("_teams.txt", "")

        teams = read_teams_from_file(filepath)

        if teams:
            print(f"  • {season_code}: {len(teams)} teams")
            all_teams.update(teams)
            season_stats.append((season_code, len(teams)))

            # Track which seasons each team appears in
            for team in teams:
                team_appearances[team].append(season_code)
        else:
            print(f"  • {season_code}: No teams found")

    if not all_teams:
        print("\n❌ No teams found in any file!")
        return

    # Statistics
    print("\n" + "=" * 70)
    print("STATISTICS")
    print("=" * 70)
    print(f"Total unique teams across all seasons: {len(all_teams)}")
    print(f"Total seasons processed: {len(season_stats)}")

    if season_stats:
        avg_teams = sum(count for _, count in season_stats) / len(season_stats)
        print(f"Average teams per season: {avg_teams:.1f}")

        max_season = max(season_stats, key=lambda x: x[1])
        min_season = min(season_stats, key=lambda x: x[1])
        print(f"Most teams in a season: {max_season[1]} ({max_season[0]})")
        print(f"Fewest teams in a season: {min_season[1]} ({min_season[0]})")

    # Find teams that appeared in all seasons
    all_seasons_teams = [
        team for team in all_teams if len(team_appearances[team]) == len(teams_files)
    ]
    if all_seasons_teams:
        print(f"\n🏆 Teams in ALL {len(teams_files)} seasons: {len(all_seasons_teams)}")
        for team in sorted(all_seasons_teams):
            print(f"   • {team}")

    # Find teams that appeared in only one season
    one_season_teams = [team for team in all_teams if len(team_appearances[team]) == 1]
    if one_season_teams:
        print(f"\n📊 Teams in only ONE season: {len(one_season_teams)}")

    print("=" * 70)

    # Save all teams (simple list)
    output_file = (
        DATA_DIR
        / "exports"
        / league["country"]
        / league["code"]
        / "teams"
        / "all_teams.txt"
    )
    save_all_teams(all_teams, str(output_file), league["name"])

    # Save teams with statistics
    stats_output_file = (
        DATA_DIR
        / "exports"
        / league["country"]
        / league["code"]
        / "teams"
        / "all_teams_with_stats.txt"
    )
    save_teams_with_stats(
        all_teams, str(stats_output_file), team_appearances, league["name"]
    )

    # Display all teams
    print("\n" + "=" * 70)
    print(f"ALL UNIQUE {league} TEAMS (Alphabetically Sorted)")
    print("=" * 70)
    for i, team in enumerate(sorted(all_teams), 1):
        appearances = len(team_appearances[team])
        print(
            f"  {i:3d}. {team:<40} ({appearances} season{'s' if appearances != 1 else ''})"
        )
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
