"""
test_config_loader.py — Unit tests for the league configuration loader.

Uses pytest's tmp_path fixture to avoid touching real config files.
Tests cover valid configs, missing files, malformed YAML structures,
and incomplete league entries.
"""

import pytest

from african_football.config.config_loader import load_leagues
from african_football.models.league_model import LeagueConfig
from tests.conftest import write_yaml


class TestLoadLeagues:
    """Test for the load_leagues function."""

    def test_returns_list_of_league_configs(self, tmp_path, sample_league_yaml_data):
        config_file = write_yaml(tmp_path, sample_league_yaml_data)
        result = load_leagues(config_file)
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], LeagueConfig)

    def test_loads_correct_field_values(self, tmp_path, sample_league_yaml_data):
        config_file = write_yaml(tmp_path, sample_league_yaml_data)
        league = load_leagues(config_file)[0]
        assert league.code == "npfl"
        assert league.name == "Nigeria Professional Football League"
        assert league.country == "nigeria"
        assert league.slug == "npfl"
        assert league.fetch_halftime is True

    def test_optional_flags_default_to_false(self, tmp_path, sample_league_yaml_data):
        config_file = write_yaml(tmp_path, sample_league_yaml_data)
        league = load_leagues(config_file)[0]
        assert league.fetch_venue is False
        assert league.fetch_lineups is False
        assert league.fetch_scorers is False
        assert league.fetch_cards is False

    def test_optional_flags_load_when_present(self, tmp_path, sample_league_yaml_data):
        data = sample_league_yaml_data.copy()
        data["leagues"][0].update(
            {
                "fetch_venue": True,
                "fetch_lineups": True,
                "fetch_scorers": True,
                "fetch_cards": True,
            }
        )
        config_file = write_yaml(tmp_path, data)
        league = load_leagues(config_file)[0]
        assert league.fetch_venue is True
        assert league.fetch_lineups is True
        assert league.fetch_scorers is True
        assert league.fetch_cards is True

    def test_loads_multiple_leagues(self, tmp_path, sample_league_yaml_data):
        data = sample_league_yaml_data.copy()
        data["leagues"].append(
            {
                "code": "gh_pl",
                "name": "Ghana Premier League",
                "country": "ghana",
                "slug": "premier-league",
                "fetch_halftime": False,
                "submission_code": "gh1",
            }
        )
        config_file = write_yaml(tmp_path, data)
        result = load_leagues(config_file)
        assert len(result) == 2
        assert result[0].code == "npfl"
        assert result[1].code == "gh_pl"

    def test_raises_file_not_found_for_missing_config(self, tmp_path):
        missing_path = tmp_path / "zilch.yaml"
        with pytest.raises(FileNotFoundError):
            load_leagues(missing_path)

    def test_raises_value_error_for_missing_leagues_key(self, tmp_path):
        config_file = write_yaml(tmp_path, {"nothing_leagues": []})
        with pytest.raises(ValueError):
            load_leagues(config_file)

    def test_raises_value_error_for_missing_required_field(
        self, tmp_path, sample_league_yaml_data
    ):
        data = sample_league_yaml_data.copy()
        data["leagues"][0] = {"code": "npfl", "name": "NPFL"}
        config_file = write_yaml(tmp_path, data)

        with pytest.raises(ValueError) as error:
            load_leagues(config_file)
        # Error message should tell what is missing
        assert "missing required fields" in str(error.value)

    def test_empty_leagues_list(self, tmp_path):
        config_file = write_yaml(tmp_path, {"leagues": []})
        result = load_leagues(config_file)
        assert result == []
