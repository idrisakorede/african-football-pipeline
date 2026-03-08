"""
test_main.py — Unit tests for the pipeline CLI orchestrator.

Tests focus on pure functions: selection parsing and team name
normalisation. Async orchestration and interactive menus are
covered in integration tests.
"""

import pytest
import yaml

import main
from african_football.utils.logger import PipelineLogger
from main import normalise_team_names, parse_selection

# ----------------------------- parse_selection ----------------------------------


class TestParseSelection:
    """Tests for the flexible selection input parser."""

    # Single value
    def test_single_value(self):
        assert parse_selection("1", 5) == [1]
        assert parse_selection("5", 5) == [5]

    # Comma separated values
    def test_comma_separated(self):
        assert parse_selection("1,3,5", 5) == [1, 3, 5]
        assert parse_selection("1, 3, 5", 5) == [1, 3, 5]
        assert parse_selection("5,1,3", 5) == [1, 3, 5]
        assert parse_selection("1,1,3,3", 5) == [1, 3]

    # Ranges
    def test_range(self):
        assert parse_selection("1-5", 5) == [1, 2, 3, 4, 5]
        assert parse_selection("2-4", 10) == [2, 3, 4]
        assert parse_selection("1-3,7,9-11", 12) == [1, 2, 3, 7, 9, 10, 11]
        assert parse_selection("1-5,3-7", 10) == [1, 2, 3, 4, 5, 6, 7]

    # All
    def test_all_keyword(self):
        assert parse_selection("all", 5) == [1, 2, 3, 4, 5]
        assert parse_selection("ALL", 5) == [1, 2, 3, 4, 5]
        assert parse_selection(" all ", 5) == [1, 2, 3, 4, 5]
        assert parse_selection("0", 5) == [1, 2, 3, 4, 5]

    # Edge cases
    def test_single_item_max_index_one(self):
        assert parse_selection("1", 1) == [1]

    def test_all_with_max_index_one(self):
        assert parse_selection("all", 1) == [1]

    # Error cases
    def test_index_below_one_raises(self):
        with pytest.raises(ValueError):
            parse_selection("-1", 5)

    def test_index_above_max_raises(self):
        with pytest.raises(ValueError):
            parse_selection("1-10", 5)

    def test_range_above_max_raises(self):
        with pytest.raises(ValueError):
            parse_selection("6", 5)

    def test_range_start_above_end_raises(self):
        with pytest.raises(ValueError):
            parse_selection("5-1", 5)

    def test_non_numeric_raises(self):
        with pytest.raises(ValueError):
            parse_selection("abc", 5)

    def test_empty_string_returns_empty(self):
        assert parse_selection("", 5) == []


# ------------------------ normalise_team_names --------------------------------------


class TestNormalizeTeamNames:
    """Tests for the team name normalization wrapper."""

    def test_skips_gracefully_when_no_canonical_file(
        self, nigeria_npfl, completed_season, sample_data, tmp_path
    ):
        """Normalisation should return data unchanged when no file exists."""

        original_dir = main.CANONICAL_TEAMS_DIR
        main.CANONICAL_TEAMS_DIR = str(tmp_path / "nonexistent")
        logger = PipelineLogger(log_dir=tmp_path / "logs")

        result = normalise_team_names(
            sample_data, nigeria_npfl, completed_season, logger
        )

        assert result["stages"][0]["matches"][0]["home_team"] == "Enyimba"
        assert (
            result["stages"][0]["matches"][1]["home_team"] == "Enyimba International FC"
        )

        main.CANONICAL_TEAMS_DIR = original_dir

    def test_normalises_when_canonical_file_exists(
        self, nigeria_npfl, completed_season, sample_data, sample_teams, tmp_path
    ):
        """Team names should resolve to canonical forms."""

        # Create canonical file
        canonical_dir = tmp_path / "canonical"
        canonical_dir.mkdir()
        canonical_file = canonical_dir / "nigeria_npfl.yaml"

        with open(canonical_file, "w") as file:
            yaml.dump({"teams": sample_teams}, file)

        original_dir = main.CANONICAL_TEAMS_DIR
        main.CANONICAL_TEAMS_DIR = str(canonical_dir)
        logger = PipelineLogger(log_dir=tmp_path / "logs")

        result = normalise_team_names(
            sample_data, nigeria_npfl, completed_season, logger
        )

        matches = result["stages"][0]["matches"]
        assert matches[0]["home_team"] == "Enyimba FC"
        assert matches[0]["away_team"] == "Kano Pillars FC"
        assert matches[1]["home_team"] == "Enyimba FC"
        assert matches[1]["away_team"] == "Kano Pillars FC"
        main.CANONICAL_TEAMS_DIR = original_dir

    def test_handles_empty_stages(self, nigeria_npfl, completed_season, tmp_path):
        """Should handle data with no stages gracefully."""

        original_dir = main.CANONICAL_TEAMS_DIR
        main.CANONICAL_TEAMS_DIR = str(tmp_path / "nonexistent")
        logger = PipelineLogger(log_dir=tmp_path / "logs")

        data: dict = {"stages": []}
        result = normalise_team_names(data, nigeria_npfl, completed_season, logger)
        assert result["stages"] == []
        main.CANONICAL_TEAMS_DIR = original_dir
