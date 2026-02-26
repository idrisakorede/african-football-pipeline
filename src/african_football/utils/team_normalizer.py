"""
team_normalizer.py — Team name normalisation for the African football pipeline.

Resolves raw scraped team names to their canonical forms using a
combination of exact alias matching and fuzzy matching. Unmatched
or low-confidence matches are logged to a review file for manual
curation rather than silently applied or raising exceptions.

Typical usage:
    from african_football.utils.team_normalizer import TeamNormalizer

    normalizer = TeamNormalizer(
        canonical_path="config/canonical_teams/nigeria_npfl.yaml",
        review_log_path="data/logs/unmatched/nigeria_npfl_2024-25.txt",
    )
    canonical_name = normalizer.resolve("Enyimba International FC")
"""

from difflib import get_close_matches
from pathlib import Path
from typing import Any

import yaml

# Fuzzy match thresholds
AUTO_APPLY_THRESHOLD = 0.85  # Apply match automatically above this confidence
REVIEW_THRESHOLD = 0.70  # Flag for review between this and AUTO_APPLY_THRESHOLD
# Below REVIEW_THRESHOLD: keep original, log as unmatched


class TeamNormalizer:
    """
    Resolves raw scraped team names to canonical forms.

    Uses a two-stage matching strategy:
    1. Exact match — checks the raw name against all known aliases
       and canonical names. Fast and fully reliable.
    2. Fuzzy match — only triggered on exact match failure. Applies
       matches above AUTO_APPLY_THRESHOLD automatically, flags matches
       between REVIEW_THRESHOLD and AUTO_APPLY_THRESHOLD for human
       review, and keeps the original name for anything below
       REVIEW_THRESHOLD.

    All unmatched and low-confidence names are written to a review
    log file for manual curation.

    Attributes:
        canonical_path:  Path to the canonical teams YAML file.
        review_log_path: Path to the review log file for flagged names.
    """

    def __init__(
        self,
        canonical_path: str | Path,
        review_log_path: str | Path,
    ) -> None:
        """
        Initialise the normalizer and build internal lookup structures.

        Args:
            canonical_path:  Path to the canonical teams YAML file.
            review_log_path: Path to write flagged team names for review.

        Raises:
            FileNotFoundError: If the canonical teams YAML does not exist.
        """
        self.canonical_path = Path(canonical_path)
        self.review_log_path = Path(review_log_path)

        # Maps every known variation  -> canonical name
        # e.g. "Enyimba", "Enyimba FC", "Enyimba International FC" -> "Enyimba FC"
        self._alias_map: dict[str, str] = {}

        # All canonical names - used as the fuzzy match candidate pool
        self._canonical_names: list[str] = []

        self._load(self.canonical_path)
        self._ensure_review_log_dir()

    def resolve(self, raw_name: str) -> str:
        """
        Resolve a raw scraped team name to its canonical form.

        Tries exact matching first, then fuzzy matching. Returns the
        original name unchanged if no confident match is found, and
        logs it to the review file for manual curation.

        Args:
            raw_name: The raw team name as scraped from Soccerway.

        Returns:
            The canonical team name if matched, or the original raw
            name if no confident match was found.
        """

        # Stage 1 - exact match
        exact = self._exact_match(raw_name)
        if exact is not None:
            return exact

        # Stage 2 - fuzzy match
        fuzzy, confidence = self._fuzzy_match(raw_name)

        if fuzzy is not None and confidence >= AUTO_APPLY_THRESHOLD:
            return fuzzy

        if fuzzy is not None and confidence >= REVIEW_THRESHOLD:
            self._log_flagged(raw_name, fuzzy, confidence, reason="low confidence")
            return fuzzy

        # No confident match - keep original, log for curation
        self._log_flagged(raw_name, fuzzy, confidence, reason="no match")
        return raw_name

    def _load(self, path: Path) -> None:
        """
        Load the canonical teams YAML and build alias lookup structures.

        Args:
            path: Path to the canonical teams YAML file.

        Raises:
            FileNotFoundError: If the YAML file does not exist.
            ValueError: If the YAML structure is invalid.
        """

        if not path.exists():
            raise FileNotFoundError(
                f"Canonical teams file not found: {path}. "
                "Create a YAML file with 'teams' list containing "
                "'canonical', 'slug', and optional 'aliases' fields."
            )

        with open(path, encoding="utf-8") as file:
            raw: dict[str, Any] = yaml.safe_load(file)

        if not isinstance(raw, dict) or "teams" not in raw:
            raise ValueError(
                f"Invalid canonical teams YAML structure in {path}. "
                "Expected a top-level 'teams' key containing a list of team definitions."
            )

        for entry in raw["teams"]:
            canonical = entry.get("canonical")
            if not canonical:
                raise ValueError(
                    f"Team entry missing 'canonical' field in {path}: {entry}"
                )
            self._canonical_names.append(canonical)

            # Map canonical name to itself
            self._alias_map[canonical] = canonical
            self._alias_map[canonical.lower()] = canonical

            # Map every alias to the canonical name
            for alias in entry.get("aliases", []):
                self._alias_map[alias] = canonical
                self._alias_map[alias.lower()] = canonical

    def _exact_match(self, raw_name: str) -> str | None:
        """
        Attempt an exact match against known aliases and canonical names.

        Checks both the original casing and lowercase to handle
        capitalisation inconsistencies in scraped data.

        Args:
            raw_name: The raw team name to look up.

        Returns:
            The canonical name if found, or None if no exact match exists.
        """
        return self._alias_map.get(raw_name) or self._alias_map.get(raw_name.lower())

    def _fuzzy_match(self, raw_name: str) -> tuple[str | None, float]:
        """
        Attempt a fuzzy match against the canonical names pool.

        Uses difflib's SequenceMatcher via get_close_matches. Returns
        both the best match and its confidence score so the caller can
        decide how to handle it based on the defined thresholds.

        Args:
            raw_name: The raw team name to match.

        Returns:
            A tuple of (best_match, confidence) where best_match is the
            closest canonical name or None, and confidence is a float
            between 0 and 1. Returns (None, 0.0) if no match found.
        """
        matches = get_close_matches(
            raw_name, self._canonical_names, n=1, cutoff=REVIEW_THRESHOLD
        )

        if not matches:
            return None, 0.0

        best_match = matches[0]

        # Compute actual similarity score for the best match
        from difflib import SequenceMatcher

        confidence = SequenceMatcher(None, raw_name.lower(), best_match.lower()).ratio()

        return best_match, confidence

    def _ensure_review_log_dir(self) -> None:
        """Create the review log directory if it does not exist."""
        self.review_log_path.parent.mkdir(parents=True, exist_ok=True)

    def _log_flagged(
        self, raw_name: str, suggested: str | None, confidence: float, reason: str
    ) -> None:
        """
        Log a flagged team name to the review file for manual curation.

        Appends to the review file so multiple pipeline runs accumulate
        flags without overwriting previous entries.

        Args:
            raw_name:   The raw name that could not be confidently matched.
            suggested:  The best fuzzy match suggestion, or None.
            confidence: The confidence score of the suggestion.
            reason:     Human-readable reason for flagging.
        """
        with open(self.review_log_path, "a", encoding="utf-8") as file:
            suggestion_string = (
                f"{suggested} (confidence: {confidence:.2f})"
                if suggested
                else "no suggestion"
            )
            file.write(f"[{reason.upper()}] '{raw_name}' -> {suggestion_string}\n")

    def load_canonical_teams(self, path: str | Path) -> dict[str, Any]:
        """
        Load raw canonical teams data from a YAML file.

        This is a loader that returns the raw structure.
        For name resolution, use TeamNormalizer directly.

        Args:
            path: Path to the canonical teams YAML file.

        Returns:
            The parsed YAML content as a dictionary.

        Raises:
            FileNotFoundError: If the file does not exist.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Canonical teams file not found: {path}")

        with open(path, encoding="utf-8") as file:
            return yaml.safe_load(file)
