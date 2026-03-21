"""
format_matches.py — Format match data files with aligned columns.

Standalone utility for manually formatting/fixing match data TXT files.
Usually not needed since the pipeline outputs properly formatted files.

Use this only when:
- Manually editing match files
- Fixing corrupted formatting
- Re-formatting old files

Usage:
    uv run python tools/format_matches.py [file_path]

    Or run interactively to select league and files.
"""

import re
import sys
from pathlib import Path
from typing import Optional

from tools.shared import DATA_DIR, find_available_files, select_file, select_league


def is_stage_header(line: str) -> bool:
    """Check if line is a stage header"""
    line_stripped = line.strip().lower()
    stage_keywords = ["stage", "playoff", "round", "phase", "championship", "group"]
    if line.startswith("#"):
        return any(keyword in line_stripped for keyword in stage_keywords)
    return False


def ensure_blank_lines(lines: list) -> list:
    """Ensure proper blank line spacing"""
    result = []
    i = 0

    while i < len(lines):
        line = lines[i]
        result.append(line)

        # One blank line after league header
        if line.startswith("= "):
            result.append("")
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1
            continue

        # Three blank lines after "# Matches" or stage headers
        elif line.strip() == "# Matches" or is_stage_header(line):
            result.append("")
            result.append("")
            result.append("")
            i += 1
            while i < len(lines) and not lines[i].strip():
                i += 1
            continue

        # Two blank lines before matchdays
        elif line.strip().startswith("» Matchday"):
            blank_count = 0
            for j in range(len(result) - 2, -1, -1):
                if not result[j].strip():
                    blank_count += 1
                else:
                    break

            if blank_count < 2:
                result.pop()
                result.append("")
                result.append("")
                result.append(line)

            i += 1
            continue

        i += 1

    return result


def parse_match_line(line: str) -> Optional[dict]:
    """Parse a match line into components"""
    if " v " not in line:
        return None

    parts = line.split(" v ", 1)
    if len(parts) != 2:
        return None

    home_section = parts[0].strip()
    away_section = parts[1].strip()

    # Extract time and home team
    time_match = re.match(r"(\d+:\d+)\s+(.+)$", home_section)
    if not time_match:
        return None

    time = time_match.group(1)
    home_team = time_match.group(2).strip()

    # Extract away team and scores
    away_parts = re.split(r"\s{2,}", away_section, 1)
    if not away_parts:
        return None

    away_team = away_parts[0].strip()

    score = None
    ht_score = None

    if len(away_parts) > 1:
        score_section = away_parts[1].strip()
        score_match = re.search(r"(\d+-\d+)", score_section)
        if score_match:
            score = score_match.group(1)

        ht_match = re.search(r"\((\d+\s*-\s*\d+)\)", score_section)
        if ht_match:
            ht_score = ht_match.group(1).replace(" ", "")

    return {
        "time": time,
        "home_team": home_team,
        "away_team": away_team,
        "score": score,
        "ht_score": ht_score,
    }


def format_match_line(match: dict, home_width: int, away_width: int) -> str:
    """Format a match line with aligned columns"""
    line = f"    {match['time']}  {match['home_team']:<{home_width}} v {match['away_team']:<{away_width}}"

    if match["score"]:
        line += f" {match['score']}"
        if match["ht_score"]:
            line += f" ({match['ht_score']})"

    return line


def find_max_team_name_length(filepath: str) -> tuple:
    """Find maximum length of home and away team names"""
    max_home = 0
    max_away = 0

    try:
        with open(filepath, "r", encoding="utf-8") as file:
            for line in file:
                match = parse_match_line(line)
                if match:
                    max_home = max(max_home, len(match["home_team"]))
                    max_away = max(max_away, len(match["away_team"]))
    except FileNotFoundError:
        print(f"File not found: {filepath}")
    except UnicodeDecodeError as e:
        print(f"Encoding error reading {filepath}: {e}")

    return max_home, max_away


def format_file(input_file: str, output_file: Optional[str] = None):
    """Format a match data file with aligned columns"""
    if output_file is None:
        output_file = input_file

    # Calculate widths
    max_home, max_away = find_max_team_name_length(input_file)
    home_width = max_home + 2
    away_width = max_away + 2

    lines = []

    # Read and format
    try:
        with open(input_file, "r", encoding="utf-8") as file:
            for line in file:
                if " v " not in line:
                    lines.append(line.rstrip("\n"))
                    continue

                match = parse_match_line(line)
                if match:
                    formatted = format_match_line(match, home_width, away_width)
                    lines.append(formatted)
                else:
                    lines.append(line.rstrip("\n"))
    except FileNotFoundError:
        print(f"File not found: {input_file}")
        return
    except UnicodeDecodeError as e:
        print(f"Encoding error reading {input_file}: {e}")
        return

    # Apply spacing
    lines = ensure_blank_lines(lines)

    # Write output
    try:
        with open(output_file, "w", encoding="utf-8") as file:
            for line in lines:
                file.write(line + "\n")
        print(f"✅ Formatted: {output_file}")
    except OSError as e:
        print(f"Error writing to {output_file}: {e}")

    print(f"   Max home team: {max_home} chars")
    print(f"   Max away team: {max_away} chars")


def main():
    """Main execution"""
    print("\n" + "=" * 70)
    print("  MATCH DATA FORMATTER - Manual Utility")
    print("=" * 70)
    print("\n  NOTE: This utility is for manual fixes only.")
    print("   The scraper already outputs properly formatted files.")
    print("=" * 70)

    # Check if file provided as argument
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        if not Path(file_path).exists():
            print(f"\n❌ File not found: {file_path}")
            return

        print(f"\n🔄 Formatting: {file_path}")
        format_file(file_path)
        return

    # Interactive mode
    league = select_league(tool_name="AFRICAN FOOTBALL MATCH FILES FORMATTER")
    available_files = find_available_files(league)

    if not available_files:
        exports_path = DATA_DIR / "exports" / league["country"] / league["code"]
        print(f"No match files found in '{exports_path}'")
        return

    print(f"\n📁 Found {len(available_files)} match file(s):")
    for i, file in enumerate(available_files, 1):
        print(f"  {i}. {Path(file).name}")

    print(
        "\nTip: you can also pass a file path directly: "
        "uv run python tools/format_matches.py path/to/file.txt"
    )

    choice = select_file(available_files)

    if choice is None:
        return

    if choice == "all":
        print(f"\nFormatting {len(available_files)} file(s)...\n")
        for file in available_files:
            print(f"{Path(file).name}")
            format_file(file)
            print()
        print("All files formatted!")
    else:
        print(f"\nFormatting {Path(choice).name}")
        format_file(choice)

    print("\n" + "=" * 70 + "\n")


if __name__ == "__main__":
    main()
