"""
test_scrape.py — Minimal test DAG to verify one league scrapes correctly.
To be deleted after testing.
"""

import pendulum
from airflow.sdk import DAG, task

with DAG(
    dag_id="test_scrape_one_league",
    start_date=pendulum.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    @task()
    def test_discover():
        from plugins.pipeline_tasks import _discover_seasons_async, _run_async

        league = {
            "code": "gh_pl",
            "name": "Ghana Premier League",
            "country": "ghana",
            "slug": "premier-league",
            "fetch_halftime": False,
            "submission_code": "gh1",
            "fetch_venue": False,
            "fetch_lineups": False,
            "fetch_scorers": False,
            "fetch_cards": False,
        }

        seasons = _run_async(_discover_seasons_async(league))
        print(f"Found {len(seasons)} scrapeable seasons")

        for season in seasons[:3]:
            print(f"    {season['season']} - {season['champion']}")
        return seasons[:1]  # return only first season for test

    @task()
    def test_scrape(seasons, **context):
        from plugins.pipeline_tasks import _run_async, _scrape_season_async

        league = {
            "code": "gh_pl",
            "name": "Ghana Premier League",
            "country": "ghana",
            "slug": "premier-league",
            "fetch_halftime": False,
            "submission_code": "gh1",
            "fetch_venue": False,
            "fetch_lineups": False,
            "fetch_scorers": False,
            "fetch_cards": False,
        }

        if not seasons:
            print("No seasons to scrape")
            return

        result = _run_async(_scrape_season_async(league, seasons[0]))
        print(f"Result: {result}")

    seasons = test_discover()
    test_scrape(seasons)
