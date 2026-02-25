"""
config_loader.py — League configuration loader for the African football pipeline.

Reads league definitions from a YAML configuration file and constructs
validated LeagueConfig objects for use across the pipeline.

Typical usage:
    from src.config_loader import load_leagues

    leagues = load_leagues("config/leagues.yaml")
"""

from pathlib import Path

import yaml

from african_football.models.league_model import LeagueConfig


def load_leagues(config_path: str | Path) -> list[LeagueConfig]:
    """
    Load and validate league configurations from a YAML file.

    Reads league definitions from the specified YAML file and constructs
    a list of LeagueConfig objects. Raises descriptive errors if the file
    is missing, malformed, or contains incomplete league entries.

    Args:
        config_path: Path to the YAML configuration file.

    Returns:
        A list of LeagueConfig objects, one per league defined in the file.

    Raises:
        FileNotFoundError: If the config file does not exist at the given path.
        ValueError: If the YAML structure is invalid or a league entry is
                    missing required fields.
    """
    config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(
            f"League config file not found: {config_path}. "
            "Ensure leagues.yaml exists in the config/ directory."
        )

    with open(config_path, encoding="utf-8") as file:
        raw = yaml.safe_load(file)

    if not isinstance(raw, dict) or "leagues" not in raw:
        raise ValueError(
            "Invalid leagues.yaml structure. "
            "Expected a top-level 'leagues' key containing a list of league definitions."
        )

    leagues = []
    for entry in raw["leagues"]:
        league = _parse_league_entry(entry)
        leagues.append(league)

    return leagues


def _parse_league_entry(entry: dict) -> LeagueConfig:
    """
    Parse and validate a single league entry from the YAML config.

    Validates that all required fields are present before constructing
    a LeagueConfig object. Optional capability flags default to False
    if not specified in the config.

    Args:
        entry: A dictionary representing a single league's configuration,
               as parsed from the YAML file.

    Returns:
        A validated LeagueConfig object.

    Raises:
        ValueError: If any required field is missing from the entry.
    """
    required_fields = ["code", "name", "country", "slug", "fetch_halftime"]
    missing = [field for field in required_fields if field not in entry]

    if missing:
        raise ValueError(
            f"League entry is missing required fields: {missing}Entry received: {entry}"
        )

    return LeagueConfig(
        code=entry["code"],
        name=entry["name"],
        country=entry["country"],
        slug=entry["slug"],
        fetch_halftime=entry["fetch_haltime"],
        submission_code=entry.get("submission_code", ""),
        fetch_venue=entry.get("fetch_venue", False),
        fetch_lineups=entry.get("fetch_lineups", False),
        fetch_scorers=entry.get("fetch_scorers", False),
        fetch_cards=entry.get("fetch_cards", False),
    )
