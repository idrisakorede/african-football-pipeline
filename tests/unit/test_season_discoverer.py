"""
test_season_discoverer.py — Unit tests for SeasonDiscoverer static methods.

Tests cover the two pure static methods that contain all the core
logic for year parsing and status determination. Playwright-dependent
methods are covered in integration tests.
"""

from datetime import date

from african_football.models.season_model import SeasonStatus
from african_football.scraping.season_discoverer import SeasonDiscoverer

# --------------------- _parse_years_from_href ------------------------------------


class TestParseYearsFromHref:
    """Tests for split and single year href parsing."""

    # Split year format
    def test_split_year_returns_correct_start_and_end(self):
        start, end = SeasonDiscoverer._parse_years_from_href(
            "/ghana/premier-league-2024-2025"
        )
        assert start == 2024
        assert end == 2025

    # Single year format
    def test_single_year_derives_start_as_year_minus_one(self):
        start, end = SeasonDiscoverer._parse_years_from_href(
            "/south-africa/betway-premiership-2018/"
        )
        assert start == 2017
        assert end == 2018

    # Edge cases
    def test_returns_none_tuple_for_unrecognised_href(self):
        start, end = SeasonDiscoverer._parse_years_from_href("/egypt/premier-league")
        assert start is None
        assert end is None

    def test_returns_none_tuple_for_empty_href(self):
        start, end = SeasonDiscoverer._parse_years_from_href("")
        assert start is None
        assert end is None

    def test_split_year_without_trailing_slash(self):
        start, end = SeasonDiscoverer._parse_years_from_href("/nigeria/npfl-2024-2025")
        assert start == 2024
        assert end == 2025

    def test_single_year_without_trailing_slash(self):
        start, end = SeasonDiscoverer._parse_years_from_href("/nigeria/npfl-2019")
        assert start == 2018
        assert end == 2019


# -------------------------------- _determine_status ------------------------------


class TestDetermineStatus:
    """
    Tests for season status determination logic.

    Uses a fixed reference year to make tests independent of when
    they are run. Current year is mocked via the date in assertions
    where needed.
    """

    PAST_YEAR = date.today().year - 2
    CURRENT_YEAR = date.today().year
    FUTURE_YEAR = date.today().year + 1

    # COMPLETED cases
    def test_past_season_with_champion_is_completed(self):
        status = SeasonDiscoverer._determine_status(
            end_year=self.PAST_YEAR, champion="ENPPI SC", has_no_winner_text=False
        )
        assert status == SeasonStatus.COMPLETED

    def test_current_year_with_champion_is_completed(self):
        status = SeasonDiscoverer._determine_status(
            end_year=self.CURRENT_YEAR,
            champion="Asante Kotoko",
            has_no_winner_text=False,
        )
        assert status == SeasonStatus.COMPLETED

    # NO_WINNER cases
    def test_explicit_no_winner_text_is_no_winner(self):
        status = SeasonDiscoverer._determine_status(
            end_year=self.PAST_YEAR, champion=None, has_no_winner_text=True
        )
        assert status == SeasonStatus.NO_WINNER

    def test_historical_season_no_champion_no_text_is_no_winner(self):
        # e.g. NPFL 2005 - old season, no champion, no explicit text
        status = SeasonDiscoverer._determine_status(
            end_year=self.PAST_YEAR, champion=None, has_no_winner_text=False
        )
        assert status == SeasonStatus.NO_WINNER

    def test_no_winner_text_overrides_champion(self):
        # Defensive — should not happen in real data but logic must be correct
        status = SeasonDiscoverer._determine_status(
            end_year=self.PAST_YEAR,
            champion="Some Team FC",
            has_no_winner_text=True,
        )
        assert status == SeasonStatus.NO_WINNER

    # ONGOING cases
    def test_current_year_no_champion_no_text_is_ongoing(self):
        status = SeasonDiscoverer._determine_status(
            end_year=self.CURRENT_YEAR, champion=None, has_no_winner_text=False
        )
        assert status == SeasonStatus.ONGOING

    def test_future_year_no_champion_no_text_is_ongoing(self):
        status = SeasonDiscoverer._determine_status(
            end_year=self.FUTURE_YEAR, champion=None, has_no_winner_text=False
        )
        assert status == SeasonStatus.ONGOING
