"""
scrape_leagues.py — Airflow DAG for the African football pipeline.

Thin DAG definition file. All heavy logic lives in plugins/pipeline_tasks.py
to keep DAG parsing lightweight.

Flow:
    load_configs → build_jobs → scrape_job (x N parallel) → summarise
"""

import pendulum
from airflow.sdk import DAG, task

DEFAULT_ARGS = {
    "owner": "dki",
    "retries": 3,
    "retry_delay": pendulum.duration(seconds=30),
    "execution_timeout": pendulum.duration(minutes=45),
}

with DAG(
    dag_id="scrape_african_football",
    description="Scrape football match data across African leagues",
    default_args=DEFAULT_ARGS,
    start_date=pendulum.datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["football", "scraping", "africa"],
) as dag:

    @task()
    def load_configs() -> list[dict]:
        from plugins.pipeline_tasks import load_all_league_configs

        return load_all_league_configs()

    @task()
    def build_jobs(league_configs: list[dict]) -> list[dict]:
        from plugins.pipeline_tasks import discover_and_build_jobs

        return discover_and_build_jobs(league_configs)

    @task()
    def scrape(job: dict) -> dict:
        from plugins.pipeline_tasks import scrape_single_job

        return scrape_single_job(job)

    @task()
    def summarise(results: list[dict]) -> None:
        from plugins.pipeline_tasks import summarise as summarise_results

        summarise_results(results)

    # DAG Flow
    configs = load_configs()
    jobs = build_jobs(configs)  # type: ignore[arg-type]
    results = scrape.expand(job=jobs)
    summarise(results)  # type: ignore[arg-type]
