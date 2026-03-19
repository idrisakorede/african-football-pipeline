"""
scraper.py — Core match data scraper for the African football pipeline.

Scrapes match data for a single league season from Soccerway using
Playwright. Handles multi-stage seasons, penalty shootouts, and
optional halftime score enrichment based on league configuration.

Typical usage:
    from african_football.scraping.scraper import FootballScraper

    async with FootballScraper(league, season, logger) as scraper:
        data = await scraper.scrape()
"""

import asyncio
import random
from datetime import datetime
from typing import Any, Optional

from playwright.async_api import Browser, Page, async_playwright

from african_football.models.league_model import LeagueConfig
from african_football.models.season_model import SeasonRecord
from african_football.utils.logger import PipelineLogger

PIPELINE_VERSION = "1.0.0"

PLAYOFF_ROUND_ORDER: dict[str, int] = {
    "quarter-finals": 1,
    "quarter-final": 1,
    "quarterfinals": 1,
    "quarterfinal": 1,
    "semi-finals": 2,
    "semi-final": 2,
    "semifinals": 2,
    "semifinal": 2,
    "final": 3,
    "finals": 3,
}


class FootballScraper:
    """
    Scrapes match data for a single league season from Soccerway.

    Handles both single-stage and multi-stage seasons, penalty
    shootouts, and optional halftime score enrichment driven by
    the league configuration. Populates post-scrape metadata fields
    on the SeasonRecord after scraping completes.

    Uses async context manager pattern to guarantee browser cleanup
    even if scraping fails mid-run.

    Attributes:
        league:   League configuration driving scraping behaviour.
        season:   SeasonRecord for the season being scraped.
        logger:   Pipeline logger for progress and error reporting.
        headless: Whether to run browser in headless mode.
        stats:    Scraping statistics accumulated during the run.
    """

    def __init__(
        self,
        league: LeagueConfig,
        season: SeasonRecord,
        logger: PipelineLogger,
        headless: bool = True,
    ) -> None:
        """
        Initialise the FootballScraper.

        Args:
            league:   League configuration for the season being scraped.
            season:   SeasonRecord containing URL and season metadata.
            logger:   Pipeline logger instance.
            headless: Whether to run the browser in headless mode.
                      Set to False during development for visibility.
                      Defaults to True for pipeline use.
        """
        self.league = league
        self.season = season
        self.logger = logger
        self.headless = headless
        self._browser: Browser | None = None
        self.stats: dict[str, Any] = {
            "total_matches": 0,
            "stages": [],
            "ht_scores_found": 0,
            "ht_scores_failed": [],
            "network_errors": 0,
            "missing_data": [],
        }

    async def __aenter__(self) -> "FootballScraper":
        """Start the Playwright browser."""
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=self.headless)
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Close the Playwright browser."""
        if self._browser:
            await self._browser.close()
        await self._playwright.stop()

    async def scrape(self) -> dict[str, Any]:
        """
        Scrape all match data for the season.

        Navigates to the season results page, loads all matches,
        extracts stage and match data, optionally enriches with
        halftime scores, validates the data, and populates
        post-scrape metadata on the SeasonRecord.

        Returns:
            A structured dictionary containing league, season,
            match data, and ingestion metadata ready for file_saver.

        Raises:
            RuntimeError: If called outside of async context manager.
            ValueError: If the SeasonRecord has no valid URL.
        """
        if self._browser is None:
            raise RuntimeError(
                "Browser not started. Use 'async with FootballScraper(...)' "
                "to ensure the browser is started before calling scrape()."
            )

        if not self.season.url:
            raise ValueError(
                f"SeasonRecord for {self.season.season} has no URL. "
                "Only COMPLETED seasons can be scraped."
            )

        self.logger.log(f"Scraping {self.season.season} — {self.league.name}")
        self.logger.log(
            f"Halftime scores: "
            f"{'Enabled' if self.league.fetch_halftime else 'Disabled'}"
        )
        self.logger.log(f"Loading: {self.season.url}")

        page = await self._browser.new_page()

        try:
            await page.goto(self.season.url, timeout=60000)
            await page.wait_for_load_state("networkidle")

            await self._load_all_matches(page)
            stages_data = await self._extract_all_stages(page)

            if self.league.fetch_halftime:
                for stage in stages_data:
                    stage["matches"] = await self._enrich_matches(
                        page, stage["matches"]
                    )
        finally:
            await page.close()

        all_matches = [m for stage in stages_data for m in stage["matches"]]
        self.validate_data(all_matches)

        self.stats["total_matches"] = len(all_matches)
        self.stats["stages"] = [
            {"name": s["stage_name"], "matches": len(s["matches"])} for s in stages_data
        ]

        data = self._build_output(stages_data, all_matches)

        # Populate post-scrape metadata on SeasonRecord
        self.season.scraped_at = data["scraped_at"]
        self.season.records_extracted = len(all_matches)
        self.season.pipeline_version = PIPELINE_VERSION

        return data

    def _build_output(
        self,
        stages_data: list[dict[str, Any]],
        all_matches: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        Build the final output dictionary from scraped data.

        Args:
            stages_data: List of stage dictionaries with match data.
            all_matches: Flat list of all matches across all stages.

        Returns:
            A structured output dictionary ready for file_saver.
        """
        return {
            "league": self.league.code,
            "league_name": self.league.name,
            "country": self.league.country,
            "season": self.season.season,
            "start_year": self.season.start_year,
            "end_year": self.season.end_year,
            "champion": self.season.champion,
            "total_matches": len(all_matches),
            "has_halftime_scores": self.league.fetch_halftime,
            "stages": stages_data,
            "scraped_at": datetime.now().isoformat(),
            "pipeline_version": PIPELINE_VERSION,
            "statistics": self.stats,
        }

    async def _load_all_matches(self, page: Page) -> None:
        """
        Click 'Show more matches' until all matches are loaded.

        Args:
            page: The Playwright page loaded at the season results URL.
        """
        clicks = 0
        while True:
            try:
                btn = page.locator("a.wclButtonLink:has-text('Show more matches')")
                if await btn.count() == 0:
                    break
                await btn.scroll_into_view_if_needed()
                await asyncio.sleep(random.uniform(1, 2))
                await btn.click()
                clicks += 1
                await asyncio.sleep(random.uniform(2, 4))
            except Exception as e:
                self.logger.log(f"Error clicking show more: {e}", level="INFO")
                break
        self.logger.log(f"Loaded all matches ({clicks} clicks)", level="SUCCESS")

    async def _extract_all_stages(self, page: Page) -> list[dict[str, Any]]:
        """
        Extract match data grouped by stage from the results page.

        Finds all stage header elements and extracts matches between
        consecutive headers using JavaScript evaluation to traverse
        sibling DOM elements.

        Args:
            page: The Playwright page with all matches loaded.

        Returns:
            A list of stage dictionaries each containing stage_name,
            total_rounds, total_matches, and matches.
        """
        headers = await page.locator("div.headerLeague__wrapper").all()
        self.logger.log(f"Found {len(headers)} stage(s)")
        stages_data = []

        for idx, header in enumerate(headers):
            try:
                name_elem = header.locator(".headerLeague__title-text")
                stage_name = (
                    await name_elem.inner_text()
                    if await name_elem.count() > 0
                    else f"Stage {idx + 1}"
                ).strip()

                elements = await page.evaluate(
                    """
                    (idx) => {
                        const headers = Array.from(
                            document.querySelectorAll('div.headerLeague__wrapper')
                        );
                        const current = headers[idx];
                        const next = headers[idx + 1];
                        const matches = [];
                        let element = current.nextElementSibling;

                        while (element && element !== next) {
                            if (element.classList.contains('event__round')) {
                                matches.push({
                                    type: 'round',
                                    text: element.textContent.trim()
                                });
                            } else if (element.classList.contains('event__match')) {
                                const matchId = element.id;
                                const stageBlock = element.querySelector(
                                    '.event__stage--block'
                                );
                                const hasStageIndicator = stageBlock !== null;
                                const stageIndicatorText = hasStageIndicator
                                    ? stageBlock.textContent.trim()
                                    : null;
                                const ftScores = element.querySelectorAll(
                                    '.event__part'
                                );
                                const ftHome = ftScores.length >= 2
                                    ? ftScores[0].textContent.trim()
                                    : null;
                                const ftAway = ftScores.length >= 2
                                    ? ftScores[1].textContent.trim()
                                    : null;
                                matches.push({
                                    type: 'match',
                                    id: matchId,
                                    hasStageIndicator: hasStageIndicator,
                                    stageIndicatorText: stageIndicatorText,
                                    ftHome: ftHome,
                                    ftAway: ftAway
                                });
                            }
                            element = element.nextElementSibling;
                        }
                        return matches;
                    }
                    """,
                    idx,
                )

                stage_matches = await self._extract_stage_matches(
                    page, elements, stage_name
                )

                if stage_matches:
                    round_numbers: list[int] = [
                        m["round_number"]
                        for m in stage_matches
                        if m.get("round_number") is not None
                    ]
                    stages_data.append(
                        {
                            "stage_name": stage_name,
                            "total_rounds": max(round_numbers) if round_numbers else 0,
                            "total_matches": len(stage_matches),
                            "matches": stage_matches,
                        }
                    )
                    self.logger.log(
                        f"Stage '{stage_name}': {len(stage_matches)} matches",
                        level="SUCCESS",
                    )
                else:
                    self.logger.log(
                        f"Stage '{stage_name}': no matches found",
                        level="WARNING",
                    )

            except Exception as e:
                self.logger.log(f"Error extracting stage {idx}: {e}", level="ERROR")

        return stages_data

    async def _extract_stage_matches(
        self,
        page: Page,
        elements: list[dict[str, Any]],
        stage_name: str,
    ) -> list[dict[str, Any]]:
        """
        Extract individual matches from a list of stage elements.

        Tracks the current round as elements are processed in order.
        Round elements update the current round context, match elements
        are extracted using that context.

        Args:
            page:       The Playwright page.
            elements:   List of element descriptors from JS evaluation.
            stage_name: Name of the current stage for match metadata.

        Returns:
            A list of match dictionaries for this stage.
        """
        matches = []
        current_round_name: Optional[str] = None
        current_round_number: Optional[int] = None

        for element in elements:
            if element["type"] == "round":
                round_text = element["text"]
                current_round_name = round_text
                try:
                    if "round" in round_text.lower():
                        num_str = round_text.lower().replace("round", "").strip()
                        current_round_number = (
                            int(num_str) if num_str.isdigit() else None
                        )
                    else:
                        current_round_number = None
                except (ValueError, AttributeError):
                    current_round_number = None

            elif element["type"] == "match":
                match = await self._extract_single_match(
                    page.locator(f"#{element['id']}"),
                    current_round_name,
                    current_round_number,
                    stage_name,
                    element.get("stageIndicatorText"),
                    element.get("ftHome"),
                    element.get("ftAway"),
                )
                if match:
                    matches.append(match)

        return matches

    async def _extract_single_match(
        self,
        element: Any,
        round_name: Optional[str],
        round_number: Optional[int],
        stage_name: str,
        stage_indicator: Optional[str] = None,
        ft_home: Optional[str] = None,
        ft_away: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """
        Extract data from a single match row element.

        Args:
            element:         Playwright locator for the match element.
            round_name:      Current round display name.
            round_number:    Current round number for sorting, or None.
            stage_name:      Current stage name.
            stage_indicator: Text from the stage indicator block, e.g.
                             'Awrd' for awarded, 'pen.' for penalties,
                             or None for regular matches.
            ft_home:         Full-time home score before penalties, or None.
            ft_away:         Full-time away score before penalties, or None.

        Returns:
            A match dictionary or None if extraction fails.
        """
        try:
            home = await element.locator(
                ".event__homeParticipant .wcl-name_jjfMf"
            ).inner_text()
            away = await element.locator(
                ".event__awayParticipant .wcl-name_jjfMf"
            ).inner_text()
            home_score = await element.locator(".event__score--home").inner_text()
            away_score = await element.locator(".event__score--away").inner_text()
            raw_date = await element.locator("div.event__time").inner_text()
            date = raw_date.split("\n")[0].strip()
            link = await element.locator("a.eventRowLink").first.get_attribute("href")
            url = (
                f"https://ng.soccerway.com{link}"
                if link and not link.startswith("http")
                else link
            )

            match_data: dict[str, Any] = {
                "stage": stage_name,
                "round": round_name,
                "round_number": round_number,
                "date": date.strip(),
                "home_team": home.strip(),
                "away_team": away.strip(),
                "home_score": home_score.strip(),
                "away_score": away_score.strip(),
                "half_time_score": None,
                "match_url": url,
            }

            indicator = stage_indicator.lower() if stage_indicator else None

            if indicator == "awrd":
                # Awarded match
                match_data["awarded"] = True
                match_data["awarded_reason"] = "walkover"
                match_data["penalty_shootout"] = False
                match_data["penalty_winner"] = None
                match_data["full_time_score"] = None

            elif indicator and ft_home is not None and ft_away is not None:
                # Penalty shootout
                match_data["awarded"] = False
                match_data["awarded_reason"] = None
                match_data["full_time_score"] = f"{ft_home}-{ft_away}"
                match_data["penalty_shootout"] = True
                match_data["penalty_winner"] = (
                    "home" if home_score > away_score else "away"
                )
            else:
                # Regular Match
                match_data["awarded"] = False
                match_data["awarded_reason"] = None
                match_data["full_time_score"] = None
                match_data["penalty_shootout"] = False
                match_data["penalty_winner"] = None

            return match_data

        except Exception as e:
            self.logger.log(f"Error extracting match: {e}", level="WARNING")
            return None

    async def _enrich_matches(
        self,
        page: Page,
        matches: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Enrich match data with halftime scores from individual match pages.

        Attempts to fetch halftime scores for all matches with a URL.
        Retries failed matches up to two additional times before logging
        them as permanently failed.

        Args:
            page:    The Playwright page for navigation.
            matches: List of match dictionaries to enrich.

        Returns:
            The same list with half_time_score populated where available.
        """
        to_enrich = [
            (i, m)
            for i, m in enumerate(matches)
            if m.get("match_url") and not m.get("awarded")
        ]
        self.logger.log(f"Enriching {len(to_enrich)} matches with halftime scores")

        failed = await self._fetch_halftime_scores(page, to_enrich, 1)
        for retry in range(1, 3):
            if not failed:
                break
            await asyncio.sleep(3)
            failed = await self._fetch_halftime_scores(page, failed, retry + 1)

        ht_found = sum(1 for m in matches if m.get("half_time_score"))
        self.stats["ht_scores_found"] += ht_found

        # After all retry passes, find matches that still have no halftime score
        missing_ht = [
            (i, m)
            for i, m in enumerate(matches)
            if m.get("match_url")
            and not m.get("awarded")
            and m.get("half_time_score") is None
        ]

        if missing_ht:
            for idx, m in missing_ht:
                self.stats["ht_scores_failed"].append(
                    {
                        "match_index": idx + 1,
                        "stage": m.get("stage"),
                        "round": m.get("round"),
                        "home_team": m["home_team"],
                        "away_team": m["away_team"],
                        "url": m["match_url"],
                    }
                )

        total = len(matches)
        pct = ht_found / total * 100 if total > 0 else 0
        self.logger.log(
            f"Halftime scores: {ht_found}/{total} ({pct:.1f}%)",
            level="SUCCESS",
        )
        return matches

    async def _fetch_halftime_scores(
        self,
        page: Page,
        matches_list: list[tuple[int, dict[str, Any]]],
        pass_num: int,
    ) -> list[tuple[int, dict[str, Any]]]:
        """
        Fetch halftime scores for a list of matches.

        Args:
            page:         The Playwright page for navigation.
            matches_list: List of (index, match) tuples to process.
            pass_num:     Current pass number for logging context.

        Returns:
            List of (index, match) tuples that failed and need retry.
        """
        failed = []
        for _, (idx, m) in enumerate(matches_list, 1):
            try:
                await asyncio.sleep(random.uniform(1, 2))
                ht = await self._get_halftime_score(page, m["match_url"])
                if ht:
                    m["half_time_score"] = ht
                elif pass_num == 1:
                    failed.append((idx, m))
            except Exception as e:
                if "timeout" in str(e).lower() or "network" in str(e).lower():
                    self.stats["network_errors"] += 1
                failed.append((idx, m))
        return failed

    async def _get_halftime_score(self, page: Page, url: str) -> Optional[str]:
        """
        Fetch the halftime score from an individual match page.

        Args:
            page: The Playwright page for navigation.
            url:  The match page URL to fetch.

        Returns:
            The halftime score string e.g. '1-0', or None if not found.
        """
        await page.goto(url, timeout=30000)
        await page.wait_for_load_state("networkidle")
        loc = (
            page.locator("span:has-text('1st Half')")
            .locator("xpath=following-sibling::span//div")
            .first
        )
        return (await loc.inner_text()).strip() if await loc.count() > 0 else None

    def validate_data(self, matches: list[dict[str, Any]]) -> None:
        """
        Validate all matches for required fields and log any issues.

        Checks each match for the presence of required fields and
        records any missing data in self.stats for reporting.

        Args:
            matches: Flat list of all match dictionaries to validate.
        """
        self.logger.log_section("DATA VALIDATION")
        required_fields = [
            "home_team",
            "away_team",
            "home_score",
            "away_score",
            "date",
            "round",
            "stage",
        ]

        for i, m in enumerate(matches, 1):
            issues = [f for f in required_fields if not m.get(f)]
            if issues:
                info = {
                    "match_index": i,
                    "stage": m.get("stage"),
                    "round": m.get("round"),
                    "teams": (
                        f"{m.get('home_team', '?')} vs {m.get('away_team', '?')}"
                    ),
                    "issues": issues,
                }
                self.stats["missing_data"].append(info)
                self.logger.log(
                    f"Match {i}: {info['teams']} — {', '.join(issues)}",
                    level="WARNING",
                )

        if not self.stats["missing_data"]:
            self.logger.log("All matches have complete data", level="SUCCESS")

    def get_statistics_report(self) -> str:
        """
        Generate a human-readable statistics report for this scrape run.

        Returns:
            A formatted string summarising scraping statistics.
        """
        lines = [
            "\n" + "=" * 70,
            "SCRAPING STATISTICS",
            "=" * 70,
            f"League:  {self.league.name} ({self.league.code.upper()})",
            f"Season:  {self.season.season}",
            f"Matches: {self.stats['total_matches']}",
        ]

        if self.stats["stages"]:
            lines.append("\nStages:")
            lines.extend(
                f"  - {s['name']}: {s['matches']} matches" for s in self.stats["stages"]
            )

        if self.league.fetch_halftime:
            total = self.stats["total_matches"]
            found = self.stats["ht_scores_found"]
            pct = found / total * 100 if total > 0 else 0
            lines.append(f"\nHalftime scores: {found}/{total} ({pct:.1f}%)")

        if self.stats["ht_scores_failed"]:
            lines.append("\nMissing halftime scores:")
            for m in self.stats["ht_scores_failed"]:
                lines.append(
                    f"  - {m['home_team']} vs {m['away_team']} "
                    f"({m['round']}) - {m['url']}"
                )

        lines.extend(
            [
                f"Network errors:  {self.stats['network_errors']}",
                f"Missing data:    {len(self.stats['missing_data'])}",
                "=" * 70,
            ]
        )
        return "\n".join(lines)
