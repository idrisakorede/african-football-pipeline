"""
test_file_saver.py — Unit tests for file persistence utilities.

Tests focus on pure helper functions: path building, season string
formatting, checksum generation, and score formatting.
save_json and save_txt integration is covered in integration tests.
"""

from african_football.models.league_model import LeagueConfig
from african_football.utils.file_saver import (
    _build_json_path,
    _build_season_str,
    _build_txt_path,
    _compute_checksum,
    _format_round_header,
    _format_score,
)

# ------------------------------- Season String --------------------------------------


class TestBuildSeasonString:
    """Test for the season string formatter."""

    def test_returns_short_end_year(self):
        assert _build_season_str(2024, 2025) == "2024-25"
        assert _build_season_str(2019, 2020) == "2019-20"
        assert _build_season_str(2012, 2013) == "2012-13"


# ------------------------------- Path Builders ---------------------------------------


class TestBuildJsonPath:
    """Tests for the JSON output path builder."""

    def test_includes_neccessary_things_in_path(self, nigeria_npfl, tmp_path):
        path = _build_json_path(nigeria_npfl, 2024, 2025, tmp_path)
        assert "nigeria" in str(path)
        assert "npfl" in str(path)
        assert "2024-25" in path.name
        assert path.suffix == ".json"


class TestBuilTxtPath:
    """Tests for the TXT export path builder."""

    def test_txt_filename_includes_submission_code(self, tmp_path):
        league = LeagueConfig(
            code="npfl",
            name="Ghana Premier League",
            country="ghana",
            slug="premier-league",
            fetch_halftime=True,
            submission_code="gh1",
        )
        path = _build_txt_path(league, 2009, 2010, tmp_path)
        assert "2009-10" in path.name
        assert "gh1" in path.name
        assert path.suffix == ".txt"


# ------------------------------- Checksum ---------------------------------------


class TestComputeChecksum:
    """Tests for the SHA256 checksum utility."""

    def test_returns_string(self):
        result = _compute_checksum({"key": "value"})
        assert isinstance(result, str)

    def test_returns_64_char_hex(self):
        result = _compute_checksum({"key": "value"})
        assert len(result) == 64

    def test_same_data_returns_same_checksum(self):
        data = {"season": "2024-25", "matches": 10}
        assert _compute_checksum(data) == _compute_checksum(data)

    def test_different_data_returns_different_checksum(self):
        assert _compute_checksum({"a": 1}) != _compute_checksum({"a": 2})

    def test_key_order_does_not_affect_checksum(self):
        data1 = {"a": 1, "b": 2}
        data2 = {"b": 2, "a": 1}
        assert _compute_checksum(data1) == _compute_checksum(data2)


# ------------------------------- Score Formatting ---------------------------------------


class TestFormatScore:
    """Tests for the match score formatter."""

    def test_regular_score(self):
        match = {
            "home_score": "2",
            "away_score": "1",
            "penalty_shootout": False,
            "half_time_score": None,
        }
        assert _format_score(match) == "2-1"

    def test_score_with_halftime(self):
        match = {
            "home_score": "2",
            "away_score": "1",
            "penalty_shootout": False,
            "half_time_score": "1-0",
        }
        assert _format_score(match) == "2-1 (1-0)"

    def test_score_with_penalty_shootout(self):
        match = {
            "home_score": "2",
            "away_score": "1",
            "penalty_shootout": True,
            "full_time_score": "1-1",
            "half_time_score": "0-0",
        }
        assert _format_score(match) == "2-1 pen (1-1)"

    def test_missing_scores_returns_vs(self):
        match = {
            "home_score": None,
            "away_score": None,
            "penalty_shoutout": False,
            "half_time_score": None,
        }
        assert _format_score(match) == "vs"


# ------------------------------- Round Header Formatting ---------------------------------------


class TestFormatRoundHeader:
    """Tests for the round header formatter."""

    def test_regular_round_becomes_matchday(self):
        assert _format_round_header("Round 1") == "Matchday 1"

    def test_round_case_insensitivity(self):
        assert _format_round_header("round 5") == "Matchday 5"

    def test_playoff_round_unchanged(self):
        assert _format_round_header("Final") == "Final"

    def test_semi_final_unchanged(self):
        assert _format_round_header("Semi-Final") == "Semi-Final"
