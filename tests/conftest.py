"""
conftest.py — Shared pytest fixtures for the African football pipeline test suite.

Fixtures defined here are automatically available to all test modules
without needing to import them explicitly.
"""

import pytest
import yaml

from src.models import LeagueConfig


@pytest.fixture
def nigeria_npfl():
    "A sample LeagueConfig for testing."
    return LeagueConfig(
        code="npfl",
        name="Nigeria Professional Football League",
        country="nigeria",
        slug="npfl",
        fetch_halftime=True,
        submission_code="ng1",
    )


@pytest.fixture
def ghana_pl():
    "A same LeagueConfig for testing."
    return LeagueConfig(
        code="gh_pl",
        name="Ghana Premier League",
        country="ghana",
        slug="premier-league",
        fetch_halftime=False,
        submission_code="gh1",
    )


@pytest.fixture
def sample_league_yaml_data():
    """A minimal valid leagues YAML data structure for config loader tests."""
    return {
        "leagues": [
            {
                "code": "npfl",
                "name": "Nigeria Professional Football League",
                "country": "nigeria",
                "slug": "npfl",
                "fetch_halftime": True,
                "submission_code": "ng1",
            }
        ]
    }


def write_yaml(path, data: dict):
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
