"""src/streaming/consumer_teja.py - Local consumer (Teja's version).

Reads messages from a local simulated topic file
and writes consumed records to a local output file one message at a time.

Start with main() at the bottom.
Work up to see how it all fits together.

Author: Venkat Teja Nallamothu
Date: 2026-05

Terminal command to run this file from the root project folder:

    uv run python -m streaming.consumer_teja

OBS:
  Copied from consumer_case.py and renamed.
  Observations: The producer and consumer run independently — the producer
  writes rows to a CSV (simulating a Kafka topic) and the consumer polls
  that file for new rows on its own schedule. They don't call each other
  directly, which mirrors how real Kafka producers and consumers are decoupled.
  As message volume grows, a business could spin up additional consumer
  processes in the same consumer group to share the load — this is the
  "replication" concept that keeps messages from getting lost.
"""

# === DECLARE IMPORTS ===

import os
from pathlib import Path
import time
from typing import Any, Final

from datafun_streaming.io.io_utils import append_csv_row, read_csv_rows
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars

# === CONFIGURE LOGGER ===

LOG = get_logger("C02", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE FALLBACK DEFAULTS ===

# WHY: These defaults intentionally do not match .env.example.
# If they appear in the log, copy .env.example to .env and try again.
DEFAULT_TOPIC_NAME: Final[str] = "missing-env-copy-env-example"
DEFAULT_MAX_MESSAGES: Final[str] = "2"
DEFAULT_POLL_INTERVAL_SECONDS: Final[str] = "0.5"
DEFAULT_TIMEOUT_SECONDS: Final[str] = "3.0"

# === DECLARE GLOBAL CONSTANTS ===

topic_name = os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC_NAME)
msg_count = os.getenv("CONSUMER_MAX_MESSAGES", DEFAULT_MAX_MESSAGES)
poll_interval_seconds = os.getenv(
    "CONSUMER_POLL_INTERVAL_SECONDS",
    DEFAULT_POLL_INTERVAL_SECONDS,
)
timeout_seconds = os.getenv("CONSUMER_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS)

TOPIC_NAME: Final[str] = topic_name
MAX_MESSAGES: Final[int] = int(msg_count)
POLL_INTERVAL_SECONDS: Final[float] = float(poll_interval_seconds)
TIMEOUT_SECONDS: Final[float] = float(timeout_seconds)

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

TOPIC_CSV: Final[Path] = OUTPUT_DIR / f"{TOPIC_NAME}.csv"
OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_sales_teja.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C02")
    LOG.info("========================")
    LOG.info("START consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "TOPIC_CSV", TOPIC_CSV)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)


def load_settings() -> None:
    """Load local consumer settings from .env and log them."""
    LOG.info("Loading settings from .env...")
    LOG.info(f"KAFKA_TOPIC                    = {TOPIC_NAME}")
    LOG.info(f"CONSUMER_MAX_MESSAGES          = {MAX_MESSAGES}")
    LOG.info(f"CONSUMER_POLL_INTERVAL_SECONDS = {POLL_INTERVAL_SECONDS}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS       = {TIMEOUT_SECONDS}")


def verify_topic_file() -> None:
    """Wait for the local simulated topic file to exist.

    Raises:
        SystemExit: If the topic file does not appear before timeout.
    """
    LOG.info("Verifying local simulated topic file...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    start_time = time.monotonic()

    while not TOPIC_CSV.exists():
        elapsed = time.monotonic() - start_time

        if elapsed >= TIMEOUT_SECONDS:
            LOG.error(f"Topic file not found: {TOPIC_CSV}")
            LOG.error(
                "Run the local producer first, or run producer and consumer together."
            )
            raise SystemExit(1)

        LOG.info("Topic file not found yet. Waiting...")
        time.sleep(POLL_INTERVAL_SECONDS)

    LOG.info(f"Topic file found: {TOPIC_CSV.name}")


# ===========================================================================
# DEFINE SECTION C. CONSUME AND PROCESS MESSAGES HELPERS
# ===========================================================================


def initialize_output() -> None:
    """Initialize output directory and clear consumed CSV from prior runs."""
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()

    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")


def process_message(row: dict[str, Any]) -> dict[str, Any]:
    """Process one local message.

    Arguments:
        row: A local message row.

    Returns:
        The same row.
    """
    LOG.info("Processing raw local message.")
    return row


def consume_messages() -> int:
    """Consume and process local messages from the simulated topic file.

    Waits for new rows until MAX_MESSAGES is reached or no new message arrives
    within TIMEOUT_SECONDS.

    Returns:
        The number of consumed messages.
    """
    LOG.info("Consuming local messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info(f"Stopping after {TIMEOUT_SECONDS}s with no new message.\n")

    consumed_count = 0
    last_message_time = time.monotonic()

    while consumed_count < MAX_MESSAGES:
        rows = read_csv_rows(TOPIC_CSV)
        new_rows = rows[consumed_count:]

        if not new_rows:
            elapsed = time.monotonic() - last_message_time

            if elapsed >= TIMEOUT_SECONDS:
                LOG.info(f"No new message received within {TIMEOUT_SECONDS}s timeout.")
                LOG.info("Producer finished or paused. Stopping consumer.")
                break

            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        for row in new_rows:
            LOG.info(row)

            processed = process_message(row)

            append_csv_row(
                path=OUTPUT_CSV,
                row=processed,
                fieldnames=list(processed.keys()),
            )

            consumed_count += 1
            last_message_time = time.monotonic()

            LOG.info("MESSAGE CONSUMED")
            LOG.info(f"consumed={consumed_count}")

            if consumed_count >= MAX_MESSAGES:
                break

    return consumed_count


def save_artifacts() -> None:
    """Log output artifacts."""
    LOG.info("Saving artifacts...")
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(consumed_count: int) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s).")
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    LOG.info("========================")
    LOG.info("Consumer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the local consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    load_settings()
    verify_topic_file()

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    initialize_output()
    consumed_count = consume_messages()
    save_artifacts()

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(consumed_count)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
