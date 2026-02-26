"""
url_builder.py - Soccerway URL construction and resolution.

Provides utilities for building and validation Soccerway league and league archives URLs.
Handles both spilt-year (2024-2025) and single-year (2024) URL formats,
and resolves which format a given league and season uses by probing the live URLs

Typical usage:
    from src.url_builder import resolve_url, build_archive_url

    url = resolve_url(country='nigeria', slug='npfl', year=2024)
    archive_url = build_archive_url(country='nigeria', slug='npfl')
"""

import requests

BASE_URL = "https://ng.soccerway.com"


def build_url_split(country: str, slug: str, start_year: int, end_year: int) -> str:
    """
    Build a Soccerway results URL using the split-year season format.

    Produces URLs in the format:
        https://ng.soccerway.com/{country}/{slug}-{start_year}-{end_year}/results/

    Args:
        country: Country slug as it appears in Soccerway URLs (e.g. 'nigeria').
        slug: League slug as it appears in Soccerway URLs (e.g. 'npfl').
        start_year: The year the season starts (e.g. 2024).
        end_year: The year the season ends (e.g. 2025).

    Returns:
         A fully formed Soccerway results URL string.
    """
    return f"{BASE_URL}/{country}/{slug}-{start_year}-{end_year}/results/"


def build_url_single(country: str, slug: str, year: int) -> str:
    """
    Build a Soccerway results URL using the single-year season format.

    Produces URLs in the format:
        https://ng.soccerway.com/{country}/{slug}-{year}/results/

    Args:
        country: Country slug as it appears in Soccerway URLs (e.g. 'nigeria').
        slug: League slug as it appears in Soccerway URLs (e.g. 'npfl').
        year: The year the season starts (e.g. 2024).

    Returns:
         A fully formed Soccerway results URL string.
    """
    return f"{BASE_URL}/{country}/{slug}-{year}/results/"


def _is_valid_url(url: str) -> bool:
    """
    Check whether a URL resolves to a valid Soccerway league page.

    Sends a HEAD request and verifies the response is a 200 that did not silently redirects to a different page (e.g. Soccerway homepage), which can happen for invalid league URLs.

    Args:
        url: The fully formed URL to probe.

    Returns:
        True if the URL resolved to a valid league page, False otherwise.
    """
    try:
        reponse = requests.head(url, allow_redirects=True, timeout=10)
        if reponse.status_code != 200:
            return False
        # Guard against silent redirects to homepage
        return reponse.url.rstrip("/") == url.rstrip("/")
    except requests.RequestException:
        return False


def resolve_url(country: str, slug: str, year: int) -> str | None:
    """
    Resolve the correct Soccerway URL for a league and season year.

    Tries the split-year format first (e.g. 2024-2025), then falls
    back to the single-year format (e.g. 2024). This handles the
    inconsistency across Soccerway leagues where some use one format
    and others use the other.

    Args:
        country: Country slug as it appears in Soccerway URLs (e.g. 'nigeria').
        slug: League slug as it appears in Soccerway URLs (e.g. 'npfl').
        year: The start year of the season to resolve (e.g. 2024).

    Returns:
        The first reachable URL as a string, or None if both formats fail.
    """
    candidates = [
        build_url_split(country, slug, year, year + 1),
        build_url_single(country, slug, year),
    ]

    for url in candidates:
        if _is_valid_url(url):
            return url

    return None


def build_archive_url(country: str, slug: str) -> str:
    """
    Build the Soccerway archive URL for a league.

    The archive page lists all available seasons with their URLs
    and champions. Used by SeasonDiscoverer to enumerate seasons
    without hardcoding them.

    Args:
        country: Country slug as it appears in Soccerway URLs.
        slug:    League slug as it appears in Soccerway URLs.

    Returns:
        A fully formed Soccerway archive URL string.
    """
    return f"{BASE_URL}/{country}/{slug}/archive/"
