"""
shared.py — Shared utilities for standalone CLI tools.

Contains league selection, path configuration and files finder used across
all tools/ scripts. Not part of the automated pipeline.
"""

import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CONFIG_PATH = PROJECT_ROOT / "config" / "leagues.yaml"


def load_league_choices() -> list[dict]:
    """
    Load available leagues from leagues.yaml for selection.

    Returns:
        A list of dicts with code, name, country, and submission_code.
    """
    try:
        with open(CONFIG_PATH, encoding="utf-8") as file:
            raw = yaml.safe_load(file)

    except FileNotFoundError:
        print(f"Config file not found: {CONFIG_PATH}")
        sys.exit(1)

    except yaml.YAMLError as e:
        print(f"Error parsing {CONFIG_PATH}: {e}")
        sys.exit(1)

    return [
        {
            "code": league["code"],
            "name": league["name"],
            "country": league["country"],
            "submission_code": league.get("submission_code", ""),
        }
        for league in raw.get("leagues", [])
    ]


def select_league(tool_name: str = "TOOL") -> dict:
    """
    Prompt user to select a league from leagues.yaml.

    Args:
        tool_name: Display name for the tool header.

    Returns:
        A dict with code, name, country, and submission_code.
    """

    leagues = load_league_choices()

    print("\n" + "=" * 70)
    print(f"  {tool_name}")
    print("=" * 70)
    print("\nSelect league:")

    for i, league in enumerate(leagues, 1):
        print(f" {i}. {league['name']} ){league['country']}")

    print("=" * 70 + "\n")

    while True:
        choice = input("Enter league number: ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(leagues):
            return leagues[int(choice) - 1]
        print(f"Invalid choice. Please enter 1-{len(leagues)}.\n")


def find_available_files(league: dict, subfolder: str = "") -> list[str]:
    """
    Find all available TXT files for a league in the exports directory.

    Searches data/exports/{country}/{league_code}/{subfolder}/ for
    TXT files. The subfolder parameter allows tools to look in
    subdirectories like 'teams/' without duplicating path logic.

    Args:
        league:    League dict with country and code keys.
        subfolder: Optional subdirectory within the league exports
                   folder. Defaults to empty string (root).

    Returns:
        Sorted list of file paths as strings.
    """

    exports_dir = DATA_DIR / "exports" / league["country"] / league["code"]

    if subfolder:
        exports_dir = exports_dir / subfolder

    if not exports_dir.exists():
        return []

    return sorted(str(file) for file in exports_dir.glob(".*txt"))


def select_file(available_files: list[str]) -> str | None:
    """
    Prompt user to select a file from a list.

    Accepts a number, a filename, or 'all'. Returns the selected
    filepath, 'all' as a string, or None if no valid selection.

    Args:
        available_files: List of file paths to choose from.

    Returns:
        Selected filepath, 'all', or None.
    """
    while True:
        user_input = input("Enter file (or 'all'): ").strip()

        if not user_input:
            continue

        if user_input.lower() == "all":
            return "all"
        try:
            if user_input.isdigit():
                file_number = int(user_input)
                if 1 <= file_number <= len(available_files):
                    return available_files[file_number - 1]
                print(f"Invalid number. Please enter 1-{len(available_files)}\n")
                continue
        except (ValueError, IndexError):
            print("Invalid input. Enter a number or 'all'")
            continue

        matched = [file for file in available_files if Path(file).name == user_input]
        if matched:
            return matched[0]

        print(f"File not found: {user_input}\n")
