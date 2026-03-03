"""
season_model.py — Season data models for the African football pipeline.

Defines the SeasonStatus enum and SeasonRecord dataclass used across
the pipeline to represent discovered seasons and their scraping state.

SeasonRecord is populated in two phases:
    1. Discovery phase — SeasonDiscoverer populates core fields
    2. Post-scrape phase — FootballScraper populates ingestion metadata
"""

from dataclasses import dataclass
from enum import Enum


class SeasonStatus(Enum):
    """
    Represents the completion state of a discovered season.

    Attributes:
        COMPLETED: Season has a champion and results to scrape.
        NO_WINNER: Season ran but produced no champion (voided,
                   abandoned, or cancelled). Metadata recorded
                   but no match data scraped.
        ONGOING:   Season is currently in progress. Skipped
                   entirely — no record saved.
    """

    COMPLETED = "completed"
    NO_WINNER = "no_winner"
    ONGOING = "ongoing"


@dataclass
class SeasonRecord:
    """
    Represents a single season discovered from the Soccerway archive page.

    Populated in two phases:
    - Discovery phase: core identity and status fields
    - Post-scrape phase: ingestion metadata fields

    Attributes:
        season:            Display string e.g. '2024/2025'
        start_year:        Integer start year.
        end_year:          Integer end year.
        status:            SeasonStatus indicating completion state.
        url:               Full results URL or None if not scrapeable.
        champion:          Champion team name or None.
        champion_url:      Full URL to champion team page or None.
        scraped_at:        ISO timestamp of when scraping ran.
        records_extracted: Total matches scraped.
        checksum:          SHA256 of raw scraped data.
        pipeline_version:  Version string of the scraper that ran.
    """

    # ------------------------------- Discovery phase ----------------------------
    season: str
    start_year: int
    end_year: int
    status: SeasonStatus

    # Optional at discovery - present for COMPLETED, None otherwise
    url: str | None = None
    champion: str | None = None
    champion_url: str | None = None

    # ------------------------------- Post-scrape phase ----------------------------

    scraped_at: str | None = None
    records_extracted: int | None = None
    checksum: str | None = None
    pipeline_version: str | None = None

    def is_scrapeable(self) -> bool:
        """
        Return True if this season has data worth scraping.

        Only COMPLETED seasons with a valid results URL are scrapeable.

        Returns:
            True if status is COMPLETED and url is not None.
        """
        return self.status == SeasonStatus.COMPLETED and self.url is not None

    def to_dict(self) -> dict:
        """
        Serialise the SeasonRecord to a dictionary for JSON storage.

        Converts SeasonStatus enum to its string value for
        JSON compatibility.

        Returns:
            A dictionary representation of the SeasonRecord.
        """
        return {
            "season": self.season,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "status": self.status.value,
            "url": self.url,
            "champion": self.champion,
            "champion_url": self.champion_url,
            "scraped_at": self.scraped_at,
            "records_extracted": self.records_extracted,
            "checksum": self.checksum,
            "pipeline_version": self.pipeline_version,
        }
