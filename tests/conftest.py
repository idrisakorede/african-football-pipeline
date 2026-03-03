"""
conftest.py — Shared pytest fixtures for the African football pipeline test suite.

Fixtures defined here are automatically available to all test modules
without needing to import them explicitly.
"""

import pytest

from african_football.models.league_model import LeagueConfig
from african_football.models.season_model import SeasonRecord, SeasonStatus


@pytest.fixture
def nigeria_npfl() -> LeagueConfig:
    "A sample LeagueConfig for the Nigerian first division."
    return LeagueConfig(
        code="npfl",
        name="Nigeria Professional Football League",
        country="nigeria",
        slug="npfl",
        fetch_halftime=True,
        submission_code="ng1",
    )


@pytest.fixture
def ghana_pl() -> LeagueConfig:
    "A sample LeagueConfig for the Ghanian first division."
    return LeagueConfig(
        code="gh_pl",
        name="Ghana Premier League",
        country="ghana",
        slug="premier-league",
        fetch_halftime=False,
        submission_code="gh1",
    )


@pytest.fixture
def sample_league_yaml_data() -> dict:
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


@pytest.fixture
def sample_teams() -> list:
    """A minimal valid canonical teams list for normalizer tests."""
    return [
        {
            "canonical": "Enyimba FC",
            "slug": "enyimba",
            "aliases": ["Enyimba", "Enyimba International FC"],
        },
        {"canonical": "ENPPI SC", "slug": "enppi", "aliases": ["Enppi", "Enppi SC"]},
        {
            "canonical": "Orlando Pirates FC",
            "slug": "orlando-pirates",
            "aliases": ["Orlando", "Orlando pirates", "Orlando pirates FC"],
        },
    ]


@pytest.fixture
def completed_season() -> SeasonRecord:
    """A completed SeasonRecord for testing."""
    return SeasonRecord(
        season="2024/2025",
        start_year=2024,
        end_year=2025,
        status=SeasonStatus.COMPLETED,
        url="https://ng.soccerway.com/nigeria/npfl-2024-2025/results/",
        champion="Enyimba FC",
        champion_url="https://ng.soccerway.com/team/enyimba/abc123/",
    )


@pytest.fixture
def no_winner_season() -> SeasonRecord:
    """A no-winner SeasonRecord for testing."""
    return SeasonRecord(
        season="2019/2020",
        start_year=2019,
        end_year=2020,
        status=SeasonStatus.NO_WINNER,
    )


@pytest.fixture
def ongoing_season() -> SeasonRecord:
    """An ongoing SeasonRecord for testing."""
    return SeasonRecord(
        season="2025/2026",
        start_year=2025,
        end_year=2026,
        status=SeasonStatus.ONGOING,
    )
