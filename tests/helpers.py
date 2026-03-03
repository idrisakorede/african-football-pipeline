"""
helpers.py — Pure utility functions shared across the test suite.

These are stateless helper functions used by multiple test modules.
"""

from pathlib import Path

import yaml


def write_yaml(path, data: dict) -> Path:
    """
    Write a dictionary as a YAML file in the given directory.

    Args:
        path: A tmp_path directory from pytest.
        data: Dictionary to serialise as YAML.

    Returns:
        Path to the written YAML file.
    """
    config_file = path / "leagues.yaml"
    with open(config_file, "w") as file:
        yaml.dump(data, file)
    return config_file


def write_canonical_yaml(path: Path, teams: list) -> Path:
    """
    Write a canonical teams list as a YAML file in the given directory.

    Args:
        path:  A directory path, typically pytest's tmp_path.
        teams: List of team dictionaries to serialise as YAML.

    Returns:
        Path to the written YAML file.
    """
    config_file = path / "teams.yaml"
    with open(config_file, "w") as file:
        yaml.dump({"teams": teams}, file)
    return config_file
