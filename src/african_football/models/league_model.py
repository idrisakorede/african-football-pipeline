"""
models.py - Core data models for the African football pipeline.

Defines structured configuration objects used across the pipeline
to represent league metadata and scraping capabilties.
"""

from dataclasses import dataclass


@dataclass
class LeagueConfig:
    """
    Represents the configuration and scraping capabilities for a single football league.

    Attributes:
        code: Unique identifier for the league (e.g. 'npfl').
        name: Human-readable league name.
        country: Country slug as it appears in Soccerway URLs (e.g. 'nigeria').
        slug: League slug as it appears in Soccerway URLs (e.g. 'npfl').
        fetch_halftime: Whether halftime scores are available for this league.
        fetch_venue:     Whether to scrape match venue (stadium). Defaults to False.
        fetch_lineups:   Whether to scrape match lineups. Defaults to False.
        fetch_scorers:   Whether to scrape goalscorers. Defaults to False.
        fetch_cards:     Whether to scrape bookings (yellow/red cards). Defaults to False.
    """

    code: str
    name: str
    country: str
    slug: str
    fetch_halftime: bool
    submission_code: str = ""
    fetch_venue: bool = False
    fetch_lineups: bool = False
    fetch_scorers: bool = False
    fetch_cards: bool = False
