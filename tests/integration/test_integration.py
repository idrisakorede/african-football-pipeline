"""
test_integration.py — Integration tests for the African football pipeline.

Tests verify that pipeline components connect correctly: config loading
produces valid objects, those objects generate correct file paths, scraped
data flows through normalisation and saving, and CLI selection logic
works with real model objects.

These tests use real config files and sample scraped data but make no
network calls. They run in seconds.
"""

import json

import pytest

from african_football.models.league_model import LeagueConfig
from african_football.models.season_model import SeasonRecord, SeasonStatus
from african_football.utils.file_saver import (
    save_json,
    save_txt,
)
from main import parse_selection

# ----------------------- Sample scraped data fixture -----------------------


@pytest.fixture
def scraped_data() -> dict:
    """
    Real scraped data from Ghana Premier League 2016/2017.
    Includes regular matches and awarded matches for realistic testing.
    """
    return {
        "league": "gh_pl",
        "league_name": "Ghana Premier League",
        "country": "ghana",
        "season": "2016/2017",
        "start_year": 2016,
        "end_year": 2017,
        "champion": "Aduana Stars",
        "total_matches": 8,
        "has_halftime_scores": True,
        "stages": [
            {
                "stage_name": "Premier League",
                "total_rounds": 25,
                "total_matches": 8,
                "matches": [
                    {
                        "stage": "Premier League",
                        "round": "Round 1",
                        "round_number": 1,
                        "date": "01.03. 16:00",
                        "home_team": "Legon Cities",
                        "away_team": "Elmina Sharks",
                        "home_score": "1",
                        "away_score": "1",
                        "half_time_score": "1 - 0",
                        "match_url": "https://ng.soccerway.com/match/abc123",
                        "awarded": False,
                        "awarded_reason": None,
                        "full_time_score": None,
                        "penalty_shootout": False,
                        "penalty_winner": None,
                    },
                    {
                        "stage": "Premier League",
                        "round": "Round 1",
                        "round_number": 1,
                        "date": "12.02. 16:00",
                        "home_team": "Aduana",
                        "away_team": "Ashanti",
                        "home_score": "1",
                        "away_score": "0",
                        "half_time_score": "0 - 0",
                        "match_url": "https://ng.soccerway.com/match/def456",
                        "awarded": False,
                        "awarded_reason": None,
                        "full_time_score": None,
                        "penalty_shootout": False,
                        "penalty_winner": None,
                    },
                    {
                        "stage": "Premier League",
                        "round": "Round 1",
                        "round_number": 1,
                        "date": "12.02. 16:00",
                        "home_team": "Medeama",
                        "away_team": "WAFA",
                        "home_score": "1",
                        "away_score": "0",
                        "half_time_score": "1 - 0",
                        "match_url": "https://ng.soccerway.com/match/ghi789",
                        "awarded": False,
                        "awarded_reason": None,
                        "full_time_score": None,
                        "penalty_shootout": False,
                        "penalty_winner": None,
                    },
                    {
                        "stage": "Premier League",
                        "round": "Round 4",
                        "round_number": 4,
                        "date": "26.02. 16:00\nAwrd",
                        "home_team": "Bolga AllStars",
                        "away_team": "Great Olympics",
                        "home_score": "0",
                        "away_score": "3",
                        "half_time_score": None,
                        "match_url": "https://ng.soccerway.com/match/jkl012",
                        "awarded": True,
                        "awarded_reason": "walkover",
                        "full_time_score": None,
                        "penalty_shootout": False,
                        "penalty_winner": None,
                    },
                    {
                        "stage": "Premier League",
                        "round": "Round 10",
                        "round_number": 10,
                        "date": "05.04. 16:00",
                        "home_team": "Hearts of Oak",
                        "away_team": "Bechem United",
                        "home_score": "4",
                        "away_score": "2",
                        "half_time_score": "3 - 0",
                        "match_url": "https://ng.soccerway.com/match/mno345",
                        "awarded": False,
                        "awarded_reason": None,
                        "full_time_score": None,
                        "penalty_shootout": False,
                        "penalty_winner": None,
                    },
                    {
                        "stage": "Premier League",
                        "round": "Round 10",
                        "round_number": 10,
                        "date": "05.04. 16:00",
                        "home_team": "Ebusua",
                        "away_team": "Tema Youth",
                        "home_score": "1",
                        "away_score": "1",
                        "half_time_score": "1 - 1",
                        "match_url": "https://ng.soccerway.com/match/pqr678",
                        "awarded": False,
                        "awarded_reason": None,
                        "full_time_score": None,
                        "penalty_shootout": False,
                        "penalty_winner": None,
                    },
                    {
                        "stage": "Premier League",
                        "round": "Round 25",
                        "round_number": 25,
                        "date": "27.08. 16:00\nAwrd",
                        "home_team": "Great Olympics",
                        "away_team": "Hearts of Oak",
                        "home_score": "3",
                        "away_score": "0",
                        "half_time_score": None,
                        "match_url": "https://ng.soccerway.com/match/stu901",
                        "awarded": True,
                        "awarded_reason": "walkover",
                        "full_time_score": None,
                        "penalty_shootout": False,
                        "penalty_winner": None,
                    },
                    {
                        "stage": "Premier League",
                        "round": "Round 30",
                        "round_number": 30,
                        "date": "22.10. 16:00",
                        "home_team": "WAFA",
                        "away_team": "Medeama",
                        "home_score": "1",
                        "away_score": "1",
                        "half_time_score": "1 - 1",
                        "match_url": "https://ng.soccerway.com/match/vwx234",
                        "awarded": False,
                        "awarded_reason": None,
                        "full_time_score": None,
                        "penalty_shootout": False,
                        "penalty_winner": None,
                    },
                ],
            }
        ],
        "scraped_at": "2026-03-19T14:31:40.033026",
        "pipeline_version": "1.0.0",
        "statistics": {
            "total_matches": 8,
            "stages": [{"name": "Premier League", "matches": 8}],
            "ht_scores_found": 6,
            "ht_scores_failed": [],
            "network_errors": 0,
            "missing_data": [],
        },
    }


# ----------------------- Scraped Data → Normalisation → Save -----------------------


class TestDataFlowThroughPipeline:
    def test_save_json_creates_file(self, scraped_data, ghana_pl, tmp_path):
        path = save_json(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        assert path.exists()
        assert path.suffix == ".json"

    def test_saved_json_is_valid(self, scraped_data, ghana_pl, tmp_path):
        path = save_json(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        with open(path) as f:
            loaded = json.load(f)

        assert loaded["league"] == "gh_pl"
        assert loaded["season"] == "2016/2017"
        assert loaded["champion"] == "Aduana Stars"
        assert loaded["total_matches"] == 8
        assert "checksum" in loaded
        assert len(loaded["checksum"]) == 64

    def test_saved_json_has_sorted_matches(self, scraped_data, ghana_pl, tmp_path):
        path = save_json(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        with open(path) as f:
            loaded = json.load(f)

        matches = loaded["stages"][0]["matches"]
        round_numbers = [m["round_number"] for m in matches]
        assert round_numbers == sorted(round_numbers)

    def test_saved_json_contains_awarded_metadata(
        self, scraped_data, ghana_pl, tmp_path
    ):
        path = save_json(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        with open(path) as f:
            loaded = json.load(f)

        awarded = [
            m for s in loaded["stages"] for m in s["matches"] if m.get("awarded")
        ]
        assert len(awarded) == 2
        assert all(m["awarded_reason"] == "walkover" for m in awarded)
        assert all(m["half_time_score"] is None for m in awarded)

    def test_save_txt_creates_file(self, scraped_data, ghana_pl, tmp_path):
        path = save_txt(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        assert path.exists()
        assert path.suffix == ".txt"

    def test_saved_txt_contains_header(self, scraped_data, ghana_pl, tmp_path):
        path = save_txt(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        content = path.read_text()
        assert "2016/2017" in content
        assert "# Teams" in content
        assert "# Matches" in content

    def test_saved_txt_contains_real_team_names(self, scraped_data, ghana_pl, tmp_path):
        path = save_txt(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        content = path.read_text()
        assert "Aduana" in content
        assert "Hearts of Oak" in content
        assert "Medeama" in content
        assert "Bolga AllStars" in content

    def test_saved_txt_contains_awarded_format(self, scraped_data, ghana_pl, tmp_path):
        path = save_txt(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        content = path.read_text()
        assert "(awarded)" in content

    def test_saved_txt_contains_halftime_scores(self, scraped_data, ghana_pl, tmp_path):
        path = save_txt(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        content = path.read_text()
        assert "(1 - 0)" in content or "(0 - 0)" in content

    def test_txt_filename_includes_submission_code(
        self, scraped_data, ghana_pl, tmp_path
    ):
        path = save_txt(scraped_data, ghana_pl, 2016, 2017, base_dir=tmp_path)
        assert "gh1" in path.name


# ----------------------- CLI Selection with Real Models ────────────────────────


class TestSelectionWithModels:
    """
    Tests that parse_selection works correctly when applied to
    real LeagueConfig and SeasonRecord lists.
    """

    def test_select_single_league(self):
        leagues = [
            LeagueConfig(
                code="npfl",
                name="NPFL",
                country="nigeria",
                slug="npfl",
                fetch_halftime=True,
                submission_code="ng1",
            ),
            LeagueConfig(
                code="gh_pl",
                name="Ghana PL",
                country="ghana",
                slug="premier-league",
                fetch_halftime=False,
                submission_code="gh1",
            ),
            LeagueConfig(
                code="eg_pl",
                name="Egypt PL",
                country="egypt",
                slug="premier-league",
                fetch_halftime=False,
                submission_code="eg1",
            ),
        ]
        indices = parse_selection("2", len(leagues))
        selected = [leagues[i - 1] for i in indices]
        assert len(selected) == 1
        assert selected[0].code == "gh_pl"

    def test_select_multiple_leagues(self):
        leagues = [
            LeagueConfig(
                code="npfl",
                name="NPFL",
                country="nigeria",
                slug="npfl",
                fetch_halftime=True,
                submission_code="ng1",
            ),
            LeagueConfig(
                code="gh_pl",
                name="Ghana PL",
                country="ghana",
                slug="premier-league",
                fetch_halftime=False,
                submission_code="gh1",
            ),
            LeagueConfig(
                code="eg_pl",
                name="Egypt PL",
                country="egypt",
                slug="premier-league",
                fetch_halftime=False,
                submission_code="eg1",
            ),
        ]
        indices = parse_selection("1,3", len(leagues))
        selected = [leagues[i - 1] for i in indices]
        assert len(selected) == 2
        assert selected[0].code == "npfl"
        assert selected[1].code == "eg_pl"

    def test_select_all_seasons(self):
        seasons = [
            SeasonRecord(
                season="2022/2023",
                start_year=2022,
                end_year=2023,
                status=SeasonStatus.COMPLETED,
                url="https://example.com/1",
            ),
            SeasonRecord(
                season="2023/2024",
                start_year=2023,
                end_year=2024,
                status=SeasonStatus.COMPLETED,
                url="https://example.com/2",
            ),
            SeasonRecord(
                season="2024/2025",
                start_year=2024,
                end_year=2025,
                status=SeasonStatus.COMPLETED,
                url="https://example.com/3",
            ),
        ]
        indices = parse_selection("all", len(seasons))
        selected = [seasons[i - 1] for i in indices]
        assert len(selected) == 3

    def test_select_season_range(self):
        seasons = [
            SeasonRecord(
                season=f"{y}/{y + 1}",
                start_year=y,
                end_year=y + 1,
                status=SeasonStatus.COMPLETED,
                url=f"https://example.com/{y}",
            )
            for y in range(2015, 2025)
        ]
        indices = parse_selection("3-7", len(seasons))
        selected = [seasons[i - 1] for i in indices]
        assert len(selected) == 5
        assert selected[0].season == "2017/2018"
        assert selected[-1].season == "2021/2022"

    def test_filter_scrapeable_then_select(self):
        """Simulates the real flow: filter scrapeable seasons, then select."""
        seasons = [
            SeasonRecord(
                season="2022/2023",
                start_year=2022,
                end_year=2023,
                status=SeasonStatus.COMPLETED,
                url="https://example.com/1",
            ),
            SeasonRecord(
                season="2019/2020",
                start_year=2019,
                end_year=2020,
                status=SeasonStatus.NO_WINNER,
            ),
            SeasonRecord(
                season="2023/2024",
                start_year=2023,
                end_year=2024,
                status=SeasonStatus.COMPLETED,
                url="https://example.com/2",
            ),
        ]
        scrapeable = [s for s in seasons if s.is_scrapeable()]
        assert len(scrapeable) == 2

        indices = parse_selection("all", len(scrapeable))
        selected = [scrapeable[i - 1] for i in indices]
        assert len(selected) == 2
        assert all(s.is_scrapeable() for s in selected)
