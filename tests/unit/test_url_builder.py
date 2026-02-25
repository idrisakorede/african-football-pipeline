"""
test_url_builder.py — Unit tests for URL construction utilities.

Tests cover both URL format builders and the BASE_URL constant.
resolve_url is not unit tested here as it requires network access
and belongs in integration tests.
"""

from src.url_builder import BASE_URL, build_url_single, build_url_split


class TestBuildUrlSplit:
    """Tests for the split-year URL builder."""

    def test_returns_correct_format(self):
        url = build_url_split("nigeria", "npfl", 2024, 2025)
        assert url == f"{BASE_URL}/nigeria/npfl-2024-2025/results"

    def test_include_country_and_slug_and_both_years(self):
        url = build_url_split("ghana", "premier-league", 2024, 2025)
        assert "ghana" in url
        assert "premier-league" in url
        assert "2024" in url
        assert "2025" in url

    def test_starts_with_base_url(self):
        url = build_url_split("nigeria", "npfl", 2024, 2025)
        assert url.startswith(BASE_URL)

    def test_ends_with_results_slash(self):
        url = build_url_split("nigeria", "npfl", 2024, 2025)
        assert url.endswith("/results/")

    def test_hyphenated_country(self):
        url = build_url_split("south-africa", "betway-premiership", 2024, 2025)
        assert "south-africa" in url
        assert "betway-premiership" in url


class TestBuildUrlSingle:
    """Tests for the single-year URL builder."""

    def test_returns_correct_format(self):
        url = build_url_single("nigeria", "npfl", 2019)
        assert url == f"{BASE_URL}/nigeria/npfl-2019/results/"

    def test_includes_country_and_slug_and_years(self):
        url = build_url_single("ghana", "premier-league", 2017)
        assert "ghana" in url
        assert "premier-league" in url
        assert "2017" in url

    def test_ends_with_results_slash(self):
        url = build_url_single("nigeria", "npfl", 2019)
        assert url.endswith("/results/")

    def test_starts_with_base_url(self):
        url = build_url_single("egypt", "premier-league", 2018)
        assert url.startswith(BASE_URL)
