"""
test_season_model.py — Unit tests for SeasonRecord and SeasonStatus.
"""


class TestSeasonRecordIsScrapeable:
    """Tests for the is_scrapeable predicate."""

    def test_completed_with_url_is_scrapeable(self, completed_season):
        assert completed_season.is_scrapeable() is True

    def test_no_winner_is_not_screapeable(self, no_winner_season):
        assert no_winner_season.is_scrapeable() is False

    def test_ongoing_is_not_scrapeable(self, ongoing_season):
        assert ongoing_season.is_scrapeable() is False


class TestSeasonRecordToDict:
    """Tests for SeasonRecord serialization."""

    def test_status_serialised_as_string_value(self, completed_season):
        result = completed_season.to_dict()
        assert result["status"] == "completed"
        assert isinstance(result["status"], str)

    def test_all_fields_present_in_dict(self, completed_season):
        result = completed_season.to_dict()
        expected_keys = {
            "season",
            "start_year",
            "end_year",
            "status",
            "url",
            "champion",
            "champion_url",
            "scraped_at",
            "records_extracted",
            "checksum",
            "pipeline_version",
        }
        assert set(result.keys()) == expected_keys

    def test_optional_fields_default_to_none_in_dict(self, completed_season):
        result = completed_season.to_dict()
        assert result["scraped_at"] is None
        assert result["records_extracted"] is None
        assert result["checksum"] is None
        assert result["pipeline_version"] is None
