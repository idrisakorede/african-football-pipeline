"""
season_discoverer.py — Season discovery for the African football pipeline.

Scrapes the Soccerway archive page for a given league and returns a
structured list of all available seasons with their results URLs and
champions.

The archive page URL pattern is:
    https://ng.soccerway.com/{country}/{slug}/archive/

Typical usage:
    from african_football.scraping.season_discoverer import SeasonDiscoverer

    async with SeasonDiscoverer(league_config, logger) as discoverer:
        seasons = await discoverer.discover()
"""

import re
from datetime import date
from typing import Any

from playwright.async_api import Browser, Page, async_playwright

from african_football.models.league_model import LeagueConfig
from african_football.models.season_model import SeasonRecord, SeasonStatus
from african_football.scraping.url_builder import BASE_URL, build_archive_url
from african_football.utils.logger import PipelineLogger


class SeasonDiscoverer:
    """
    Discovers all available seasons for a league from its Soccerway archive page.

    Extracts season names, results URLs, and champions for every completed
    season. Skips ongoing seasons where no results URL is available yet.
    Handles both split-year (2024-2025) and single-year (2024) URL formats
    as they appear in the archive page.

    Attributes:
        league:  The league configuration to discover seasons for.
        logger:  Pipeline logger for progress and error reporting.
    """

    def __init__(
        self,
        league: LeagueConfig,
        logger: PipelineLogger,
        headless: bool = True,
    ) -> None:
        """
        Initialise the SeasonDiscoverer.

        Args:
            league:   The league configuration to discover seasons for.
            logger:   Pipeline logger instance.
            headless: Whether to run the browser in headless mode.
                      Set to False during development for visibility.
                      Defaults to True for pipeline use.
        """
        self.league = league
        self.logger = logger
        self.headless = headless
        self._browser: Browser | None = None

    async def __aenter__(self) -> "SeasonDiscoverer":
        """Start the Playwright browser."""
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Close the Playwright browser."""
        if self._browser:
            await self._browser.close()
        await self._playwright.stop()

    async def discover(self) -> list[SeasonRecord]:
        """
        Discover all available seasons from the league archive page.

        Navigates to the archive page, extracts every season row,
        and returns a structured list of season data. Skips the
        current ongoing season if present.

        Returns:
            A list of season dictionaries, each containing:
                - season:     Display string e.g. '2024/2025'
                - start_year: Integer start year
                - end_year:   Integer end year
                - url:        Full results URL for the season
                - champion:   Champion team name or None
        """
        if self._browser is None:
            raise RuntimeError(
                "Browser not initiated. Use 'async with SeasonDiscoverer(...)' to ensure the browser is started before calling discover()."
            )

        archive_url = build_archive_url(self.league.country, self.league.slug)
        self.logger.log(
            f"Discovering seasons for {self.league.name} from {archive_url}"
        )

        page = await self._browser.new_page()

        try:
            await page.goto(archive_url, timeout=70000)
            await page.wait_for_load_state("networkidle")
            seasons = await self._extract_seasons(page)

        finally:
            await page.close()

        self.logger.log(
            f"Discovered {len(seasons)} season(s) for {self.league.name}",
            level="SUCCESS",
        )
        return seasons

    async def _extract_seasons(self, page: Page) -> list[SeasonRecord]:
        """
        Extract all season rows from the archive page.

        Args:
            page: The Playwright page loaded at the archive URL.

        Returns:
            A list of season dictionaries for all completed seasons.
        """
        rows = await page.locator("div.archiveLatte__row").all()
        seasons = []

        for row in rows:
            season = await self._extract_season_row(row)
            if season is not None:
                seasons.append(season)

        return seasons

    async def _extract_season_row(self, row: Any) -> SeasonRecord | None:
        """
        Extract season data from a single archive row.

        Determines season status using year-based logic combined with
        winner column state. Skips ongoing seasons entirely. Returns
        a SeasonRecord for completed and no-winner seasons.

        Args:
            row: A Playwright locator for a single archiveLatte__row div.

        Returns:
            A SeasonRecord or None if the season is ongoing or malformed.
        """
        season_link = row.locator("div.archiveLatte__season a")

        if await season_link.count() == 0:
            return None

        href = await season_link.get_attribute("href")
        season_text = (await season_link.inner_text()).strip()

        if not href:
            return None

        # Parse years from href
        start_year, end_year = self._parse_years_from_href(href)
        if start_year is None or end_year is None:
            self.logger.log(f"Could not parse years from href: {href}", level="WARNING")
            return None

        # Extract winner state
        champion, champion_url = await self._extract_champion(row)
        has_no_winner_text = await self._is_no_winner(row)

        # Determine status
        status = self._determine_status(end_year, champion, has_no_winner_text)

        # Skip ongoing season
        if status == SeasonStatus.ONGOING:
            self.logger.log(f"Skipping ongoing season: {season_text}", level="INFO")
            return None

        # Log no-winner season
        if status == SeasonStatus.NO_WINNER:
            self.logger.log(f"Recording no-winner season: {season_text}", level="INFO")

        # Build full results URL from archive href
        results_url = (
            f"{BASE_URL}{href}results/" if status == SeasonStatus.COMPLETED else None
        )

        return SeasonRecord(
            season=f"{start_year}/{end_year}",
            start_year=start_year,
            end_year=end_year,
            status=status,
            url=results_url,
            champion=champion,
            champion_url=champion_url,
        )

    async def _extract_champion(self, row: Any) -> tuple[str | None, str | None]:
        """
        Extract the champion team name and URL from an archive row.

        Args:
            row: A Playwright locator for a single archiveLatte__row div.

        Returns:
            A tuple of (champion_name, champion_url), both None if
            no champion is present.
        """

        # Check for clickable winner link
        winner_link = row.locator(
            "div.archiveLatte__winnerBlock a.archiveLatte__text--clickable"
        )
        if await winner_link.count() > 0:
            name = (await winner_link.inner_text()).strip()
            href = await winner_link.get_attribute("href")
            champion_url = f"{BASE_URL}{href}" if href else None
            return name, champion_url

        return None, None

    async def _is_no_winner(self, row: Any) -> bool:
        """
        Check whether a season row explicitly states no winner.

        Args:
            row: A Playwright locator for a single archiveLatte__row div.

        Returns:
            True if the row contains explicit 'No winner' text.
        """
        no_winner_div = row.locator(
            "div.archiveLatte__winnerBlock div.archiveLatte__text"
        )
        if await no_winner_div.count() > 0:
            text = (await no_winner_div.inner_text()).strip().lower()
            return text == "no winner"
        return False

    @staticmethod
    def _is_ongoing(
        end_year: int, champion: str | None, has_no_winner_text: bool
    ) -> bool:
        """
        Determine if a season is currently ongoing.

        A season is considered ongoing if:
        - Its end year is the current year or later, AND
        - It has no champion AND no explicit 'No winner' text
        (which would indicate a historical voided season)

        Args:
            end_year:           The season's end year.
            champion:           Champion name or None.
            has_no_winner_text: Whether the row has explicit 'No winner' text.

        Returns:
            True if the season appears to be currently ongoing.
        """
        if end_year < date.today().year:
            return False
        # Current or future year with no winner and no explicit text -> ongoing
        return champion is None and not has_no_winner_text

    @staticmethod
    def _parse_years_from_href(href: str) -> tuple[int | None, int | None]:
        """
        Parse start and end years from a Soccerway season href.

        Handles both formats:
            Split year: /ghana/premier-league-2024-2025/ → (2024, 2025)
            Single year: /ghana/premier-league-2018/    → (2017, 2018)

        For single year format, start_year is derived as year - 1.

        Args:
            href: The href attribute from the season link.

        Returns:
            A tuple of (start_year, end_year) as integers, or
            (None, None) if parsing fails.
        """

        # Try split year format first: -2024-2025/
        split_match = re.search(r"-(\d{4})-(\d{4})/?$", href)
        if split_match:
            return int(split_match.group(1)), int(split_match.group(2))

        # Try single year format: -2018/
        single_match = re.search(r"-(\d{4})/?$", href)
        if single_match:
            end_year = int(single_match.group(1))
            return end_year - 1, end_year

        return None, None

    @staticmethod
    def _determine_status(
        end_year: int, champion: str | None, has_no_winner_text: bool
    ) -> SeasonStatus:
        """
        Determine the SeasonStatus for a discovered season.

        Args:
            end_year:           The season's end year parsed from href.
            champion:           Champion name or None.
            has_no_winner_text: Whether row has explicit 'No winner' text.

        Returns:
            The appropriate SeasonStatus enum value.
        """
        if has_no_winner_text:
            return SeasonStatus.NO_WINNER

        if champion is not None:
            return SeasonStatus.COMPLETED

        if end_year >= date.today().year:
            return SeasonStatus.ONGOING

        # Historical season with no champion and no text
        return SeasonStatus.NO_WINNER
