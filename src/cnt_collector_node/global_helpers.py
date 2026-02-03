"""Global helpers used throughout the code's modules."""

# pylint: disable=W0603

# Standard library imports
import hashlib
import json
import logging
import logging.handlers
import os
import ssl
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from threading import Event
from typing import Any

# Third-party imports
import certifi

# Local imports
try:
    import config
    import load_pairs
    import utxo_objects
except ModuleNotFoundError:
    try:
        from src.cnt_collector_node import config, load_pairs, utxo_objects
    except ModuleNotFoundError:
        from cnt_collector_node import config, load_pairs, utxo_objects


class NodeError(Exception):
    """Exception to raise if there are any problems with the node."""


class OgmiosError(Exception):
    """Exception to raise when an Ogmios error occurs"""


class IndexerError(Exception):
    """Exception to raise when an Indexer error occurs"""


@dataclass(frozen=True)
class AppContext:
    """Provides an application context object that can be passed
    around the application.

    NB. use of db_name and database needs to be sured up in future
    iterations.
    """

    # pylint: disable=R0902; # too-many instance attributes.

    db_name: str
    database: Any
    ogmios_url: str
    ogmios_ws: Any
    kupo_url: str
    use_kupo: bool
    main_event: Event
    thread_event: Event
    reconnect_event: Event


logger = logging.getLogger(__name__)


def setup_logging(debug: bool) -> None:
    """Set up logging configuration."""
    if not os.path.isdir(os.path.dirname(config.LOG_FILE_PATH)):
        try:
            os.mkdir(os.path.dirname(config.LOG_FILE_PATH))
        except PermissionError:
            print(
                "Cannot create the log folder "
                f"{os.path.dirname(config.LOG_FILE_PATH)}, exiting!"
            )
            sys.exit(1)

    logging.basicConfig(
        format="%(asctime)-15s.%(msecs)03d %(levelname)s :: %(filename)s:%(lineno)s:%(funcName)s() :: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.DEBUG if debug else logging.INFO,
        handlers=[
            logging.handlers.WatchedFileHandler(config.LOG_FILE_PATH),
            logging.StreamHandler(),
        ],
    )
    logging.Formatter.converter = time.gmtime
    global logger
    logger = logging.getLogger(__name__)
    logger.debug("debug logging is configured")


def log_and_raise_error(message: str, exception: Exception, err_src: str) -> None:
    """Log an error message and raise an exception."""
    logger.error("%s in %s exception: %s", exception, err_src, message)
    raise IndexerError(message) from exception


def get_utc_timestamp_now() -> str:
    """Get a formatted UTC timestamp for 'now'."""
    return datetime.now(timezone.utc).strftime(config.UTC_TIME_FORMAT)


async def read_identity(identity_file: str) -> dict:
    """Read the node identity file."""
    try:
        with open(identity_file, "r", encoding="utf-8") as identity_json:
            identity = json.loads(identity_json.read())
    except FileNotFoundError as err:
        log_and_raise_error(
            f"Node identity not found: {err}", NodeError, "read_identity"
        )
    except json.decoder.JSONDecodeError as err:
        log_and_raise_error(f"Problem parsing JSON: {err}", NodeError, "read_identity")
    return {
        "node_id": identity["node_id"],
        "location": identity["location"],
        "validator_web_socket": identity["validator_web_socket"],
    }


async def generate_validator_message(
    feed: str, identity: dict, source_messages: list, now_dt: str
):
    """Create a message compatible with the validator using the
    data collected via the indexer.
    """
    message = {
        "timestamp": "",
        "raw": [],
        "data_points": [[], []],
        "calculated_value": 0,
        "feed": feed,
        "identity": {
            "node_id": identity["node_id"],
            "location": identity["location"],
        },
        "content_signature": "",
        "errors": [],
    }
    for source_message in source_messages:
        message["raw"].append(source_message)
        message["data_points"][0].append(source_message.get("token1_volume"))
        message["data_points"][1].append(source_message.get("token2_volume"))
    # Update the message for the validator node
    calculated_price = calculate_price(message["raw"])
    if calculated_price is None:
        return None
    message["calculated_value"] = str(calculated_price)
    message["timestamp"] = now_dt
    message["content_signature"] = await generate_content_signature(
        now_dt,
        message["data_points"],
        identity["node_id"],
    )
    return message


def calculate_price(raw_data: list) -> float:
    """Calculate the average price from the raw data using a liquidity
    average.
    - Token volume is converted to a floating point according to its
      number of decimals.
    - Dex Price is determined by dividing token[0] by token[1].
    - Dex Price is validated against the already retrieved price.
    - Total volume of tokens (on all DEXes) is generated cumulatively by
      adding all token volumes.
    - Average price is then determined by dividing total volume of
      token[0] by total volume of token[1].
    """
    token1_amount = 0
    token2_amount = 0
    item = {}
    try:
        for item in raw_data:
            token_1_volume = item.get("token1_volume")
            token_2_volume = item.get("token2_volume")
            if not token_1_volume or not token_2_volume:
                # NB. we probably don't need this any more as a divide
                # by zero error should be called on the happy path, the
                # check may be good for logging, so we should keep it
                # for now.
                logger.error("a token amount is 0!")
                logger.error("%s", item)
                return None
            price = token_2_volume / token_1_volume
            raw_item_price = item.get("price")
            if price != raw_item_price:
                logger.error(
                    "price is wrong! calculated: '%s', received: '%s'",
                    price,
                    raw_item_price,
                )
                return None
            # Cumulative volume.
            token1_amount += token_1_volume
            token2_amount += token_2_volume
        avg_price = token2_amount / token1_amount
    except (AttributeError, TypeError, ZeroDivisionError) as err:
        # These should be the only exceptions it is possible to trigger
        # here.
        logger.error("cannot calculate price: %s", err)
        logger.warning("price calculation error: %s", json.dumps(item, indent=2))
        return None
    return avg_price


def _return_ca_ssl_context():
    """Return a ssl context for testing a connection to a validator
    signed by a certificate authority.
    """
    return ssl.create_default_context(cafile=certifi.where())


def get_version(pairs: load_pairs.Pairs = None):
    """Return package version to the calling code.

    Version is set to a default value if it isn't picked up by importlib
    as anticipated, i.e. if the code hasn't been installed or isn't
    being run as a package correctly.
    """
    __version__ = config.VERSION
    try:
        __version__ = version("cnt-collector-node")
    except PackageNotFoundError:
        # package is not installed
        pass
    return f"cnt-collector-node: {__version__}; pairs-file: {pairs.checksum};"


def get_user_agent(pairs: load_pairs.Pairs) -> str:
    """Return a user agent to connect to a validator node with."""
    return f"cnt-collector-node/{get_version(pairs)}"


async def generate_content_signature(
    timestamp: str, data_points: list[list, list], node_id: str
):
    """Generate a content based signature token that enables a consumer
    to verify this data at a later date.
    """
    content_signature_input = [timestamp]
    for data_point in data_points:
        content_signature_input.append(data_point)
    content_signature_input.append(node_id)
    digest = hashlib.sha256()
    for token_input in content_signature_input:
        digest.update(f"{token_input}".encode())
    return f"{digest.hexdigest()}"


def display_block(block: dict) -> int:
    """Display the information about a block. Return the block slot."""
    try:
        logger.info("block hash:         %s", block["id"])
        logger.info("block parent:       %s", block["ancestor"])
        logger.info("block height:       %s", block["height"])
        logger.info("block slot:         %s", block["slot"])
        logger.info("transactions count: %s", len(block["transactions"]))
        block_slot = block["slot"]
    except KeyError:
        logger.error("block data cannot be accessed: %s", block)
        return 0
    return block_slot


def read_pairs_config(source_config: dict) -> dict:
    """Read the addresses and the security tokens from the pairs.py
    source config file and create a new pairs config with them and
    return it to the caller.
    """

    pairs_config = {}
    for pair in source_config:
        for source in pair["sources"]:
            pair_config = utxo_objects.TokensPair(
                pair=pair["name"],
                source=source["source"],
                token_1_policy=pair["token1_policy"],
                token_1_name=pair["token1_name"],
                token_1_decimals=pair["token1_decimals"],
                token_2_policy=pair["token2_policy"],
                token_2_name=pair["token2_name"],
                token_2_decimals=pair["token2_decimals"],
                security_token_policy=source["security_token_policy"],
                security_token_name=source["security_token_name"],
            )
            address = source.get("address", None)
            if address not in pairs_config:
                pairs_config[address] = []
            pairs_config[address].append(pair_config)
    return pairs_config


def cnt_volume_from_tokens(tokens: int, decimals: int):
    """Retrieve a CNT's volume from its number of tokens given
    the number of tokens and the correct number of decimals.
    """
    logger.debug("volume: '%s' decimals; '%s'", tokens, decimals)
    return tokens / pow(10, decimals)
