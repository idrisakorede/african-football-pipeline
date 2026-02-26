"""
test_team_normalizer.py — Unit tests for the team name normalizer.

Uses pytest's tmp_path fixture to create isolated canonical team YAML
files and review log paths without touching real config files.
"""

import pytest
import yaml

from african_football.utils.team_normalizer import (
    AUTO_APPLY_THRESHOLD,
    REVIEW_THRESHOLD,
    TeamNormalizer,
)

# ---------------------------- Helpers -------------------------------------------


def write_canonical_yaml(path, teams: list) -> str:
    """Write a canonical teams YAML file for testing."""
    config_file = path / "teams.yaml"
    with open(config_file, "w") as file:
        yaml.dump({"teams": teams}, file)
    return str(config_file)


def make_normalizer(tmp_path, teams: list) -> TeamNormalizer:
    """Create a TeamNormalizer with given teams and a temp review log."""
    canonical_path = write_canonical_yaml(tmp_path, teams)
    review_log = tmp_path / "review.txt"
    return TeamNormalizer(canonical_path, review_log)


SAMPLE_TEAMS = [
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


# ------------------------------ Initialisation -----------------------------------


class TestTeamNormalizerInit:
    """Tests for TeamNormalizer initialisation."""

    def test_raises_file_not_found_for_missing_yaml(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            TeamNormalizer(tmp_path / "zilch.yaml", tmp_path / "review.yaml")

    def test_raises_value_error_for_invalid_yaml_structure(self, tmp_path):
        bad_yaml = tmp_path / "teams.yaml"
        bad_yaml.write_text("not_teams: []")
        with pytest.raises(ValueError):
            TeamNormalizer(bad_yaml, tmp_path / "review.txt")

    def test_raises_value_error_for_missing_canonical_fields(self, tmp_path):
        bad_yaml = tmp_path / "teams.yaml"
        with open(bad_yaml, "w") as file:
            yaml.dump({"teams": [{"slug": "enyimba", "aliases": []}]}, file)
        with pytest.raises(ValueError):
            TeamNormalizer(bad_yaml, tmp_path / "review.txt")

    def test_creates_review_log_directory(self, tmp_path):
        canonical_path = write_canonical_yaml(tmp_path, SAMPLE_TEAMS)
        review_log = tmp_path / "nested" / "dir" / "review.txt"
        TeamNormalizer(canonical_path, review_log)
        assert review_log.parent.exists()


# --------------------------- Exact Matching -------------------------------------


class TestExactMatching:
    """Tests for exact alias matching."""

    def test_resolves_canonical_name_exactly(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        assert normalizer.resolve("Enyimba FC") == "Enyimba FC"

    def test_resolves_known_alias(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        assert normalizer.resolve("Enyimba International FC") == "Enyimba FC"

    def test_resolves_short_alias(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        assert normalizer.resolve("Enppi") == "ENPPI SC"

    def test_resolves_case_sensitive(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        assert normalizer.resolve("orlando pirates") == "Orlando Pirates FC"


# ---------------------------- Fuzzy Matching -------------------------------------


class TestFuzzyMatching:
    """Tests for fuzzy match fallback behaviour."""

    def test_resolves_close_spelling_variation(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        result = normalizer.resolve("Enyimba Fc")
        assert result == "Enyimba FC"

    def test_returns_original_for_completely_unknown_teams(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        result = normalizer.resolve("Blank FC")
        assert result == "Blank FC"


# -------------------------------- Review Log -------------------------------------


class TestReviewLog:
    """Tests for the unmatched team review log."""

    def test_logs_unmatched_team(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        normalizer.resolve("Blank FC")
        log_content = normalizer.review_log_path.read_text()
        assert "Blank FC" in log_content

    def test_log_contains_reason(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        normalizer.resolve("Blank FC")
        log_content = normalizer.review_log_path.read_text()
        assert "NO MATCH" in log_content

    def test_exact_match_does_not_write_to_log(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        normalizer.resolve("Orlando Pirates FC")
        assert not normalizer.review_log_path.exists()

    def test_multiple_unmatches_teams_accumulate_in_logs(self, tmp_path):
        normalizer = make_normalizer(tmp_path, SAMPLE_TEAMS)
        normalizer.resolve("Zilch Team A")
        normalizer.resolve("Zilch Team B")
        log_content = normalizer.review_log_path.read_text()
        assert "Zilch Team A" in log_content
        assert "Zilch Team B" in log_content


# ------------------------- Threshold Constants -----------------------------------


class TestThresholds:
    """Tests that thresholds constants are within expected bounds."""

    def test_auto_apply_threshold_above_review_threshold(self):
        assert AUTO_APPLY_THRESHOLD > REVIEW_THRESHOLD

    def test_thresholds_are_valid_fractions(self):
        assert 0.0 < REVIEW_THRESHOLD < 1.0
        assert 0.0 < AUTO_APPLY_THRESHOLD < 1.0
