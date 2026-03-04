"""
logger.py — Logging utility for the African football pipeline.

Provides a structured logger that writes timestamped messages to both
the console and a log file. Each pipeline run generates a uniquely
named log file based on the run timestamp.

Typical usage:
    from utils.logger import PipelineLogger

    logger = PipelineLogger()
    logger.log("Scraping started", level="INFO")
    logger.log_section("EXTRACTING STAGES")
"""

from datetime import datetime
from pathlib import Path

# Mapping of log levels to display symbols
_LEVEL_SYMBOLS: dict[str, str] = {
    "INFO": "ℹ️",
    "SUCCESS": "✅",
    "ERROR": "❌",
    "WARNING": "⚠️",
}


class PipelineLogger:
    """
    Structured logger for the African football pipeline.

    Writes timestamped, levelled log messages to both the console
    and a per-run log file stored in the specified log directory.
    Each instantiation creates a new log file named with the current
    timestamp to avoid overwriting previous runs.

    Attributes:
        log_dir:  Path to the directory where log files are written.
        log_file: Path to the log file for the current run.
    """

    def __init__(self, log_dir: str | Path = "logs") -> None:
        """
        Initialise the logger and create the log file for this run.

        Args:
            log_dir: Directory to write log files to. Created if it
                     does not exist. Defaults to 'logs/'.
        """
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"pipeline_{timestamp}.log"

        with open(self.log_file, "w", encoding="utf-8") as file:
            file.write(
                f"African Football Pipeline - {datetime.now().isoformat()}\n"
                f"{'=' * 70}\n\n"
            )

    def log(self, message: str, level: str = "INFO", to_console: bool = True) -> None:
        """
        Write a levelled log message to the console and log file.

        Args:
            message:    The message to log.
            level:      Severity level — one of INFO, SUCCESS, WARNING, ERROR.
                        Defaults to INFO.
            to_console: Whether to print the message to stdout.
                        Defaults to True.
        """
        timestamp = datetime.now().strftime("%H:%M:%S")
        symbol = _LEVEL_SYMBOLS.get(level, "•")

        if to_console:
            print(f"[{timestamp}] {symbol} {message}")

        with open(self.log_file, "a", encoding="utf-8") as file:
            file.write(f"[{timestamp}] [{level}] {message}\n")

    def log_section(self, title: str) -> None:
        """
        Write a section header to the log.

        Useful for separating major pipeline phases such as extraction,
        validation, and saving.

        Args:
            title: The section title to display.
        """
        separator = "=" * 70
        self.log(f"\n{separator}\n{title}\n{separator}")
