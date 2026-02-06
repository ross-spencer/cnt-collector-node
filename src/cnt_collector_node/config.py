"""Configuration variables"""

from os import getenv
from pathlib import Path
from typing import Final

# Version and constants
VERSION: Final[str] = "2.3.0"
UTC_TIME_FORMAT: Final[str] = "%Y-%m-%dT%H:%M:%SZ"
UTXOS_THREAD_TIMEOUT: Final[int] = 300

# Minimum ADA amount for an UTxO, otherwise ignore the UTxO
MIN_ADA_AMOUNT = 5

# Max liquidity pools. We assume one but we want to test for cases where
# this ever deviates in a DEX.
MAX_LPS: Final[int] = 1

# Paths
workdir = Path.cwd()
base_path = workdir.parent if "/src" in str(workdir) else workdir

FILES_PATH: Final[Path] = Path(getenv("FILES_PATH", base_path / "files"))
CNT_DB_NAME: Final[Path] = Path(getenv("CNT_DB_NAME", base_path / "db" / "database.db"))
LOG_FILE_PATH: Final[Path] = Path(
    getenv("LOG_FILE_PATH", base_path / "log" / "cnt-indexer.log")
)
NODE_IDENTITY_LOC: Final[Path] = Path(
    getenv("NODE_IDENTITY_LOC", "/var/tmp/.node-identity.json")
)

# URLs and configuration options
OGMIOS_URL: Final[str] = getenv("OGMIOS_URL")
KUPO_URL: Final[str] = getenv("KUPO_URL")

# Use KUPO or override it.
USE_KUPO: Final[bool] = getenv("USE_KUPO", "False").lower() in ("true", "1", "t")
