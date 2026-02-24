from dataclasses import dataclass


@dataclass
class LeagueConfig:
    """
    Represents the configuration for a single football league.

    Attributes:
        code: Unique identifier for the league (e.g. 'npfl').
        name: Human-readable league name.
        country: Country slug as it appears in Soccerway URLs (e.g. 'nigeria').
        slug: League slug as it appears in Soccerway URLs (e.g. 'npfl').
        fetch_halftime: Whether halftime scores are available for this league.
    """

    code: str
    name: str
    country: str
    slug: str
    fetch_halftime: bool
