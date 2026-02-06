"""Keep track of the UTxOs at the DEX addresses"""

# pylint: disable=R0801

# Standard library imports
import argparse
import asyncio
import copy
import logging
import sys
from contextlib import closing
from pathlib import Path
from threading import Event, Thread

# Third-party imports
import websocket

# Local imports
try:
    import config
    import database_initialization
    import global_helpers as helpers
    import helper_functions
    import kupo_helper
    import load_pairs
    import ogmios_helper
except ModuleNotFoundError:
    try:
        from src.cnt_collector_node import config, database_initialization
        from src.cnt_collector_node import global_helpers as helpers
        from src.cnt_collector_node import (
            helper_functions,
            kupo_helper,
            load_pairs,
            ogmios_helper,
        )
    except ModuleNotFoundError:
        from cnt_collector_node import config, database_initialization
        from cnt_collector_node import global_helpers as helpers
        from cnt_collector_node import (
            helper_functions,
            kupo_helper,
            load_pairs,
            ogmios_helper,
        )

logger = logging.getLogger(__name__)


def db_init(db_name: str) -> None:
    """Initialize the database directory if required."""
    db_path = Path(db_name)
    if not db_path.parent.exists():
        logger.info("creating database directory: %s", db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)
    database_initialization.create_database(db_name)


def start_thread(target, args: tuple) -> Thread:
    """Start a new thread with the given target and arguments."""
    thread = Thread(target=target, args=args)
    thread.start()
    return thread


async def indexer_main(
    ogmios_url: str,
    kupo_url: str,
    db_name: str,
    pairs: load_pairs.Pairs,
    unsafe: bool,
) -> None:
    """CNT Collector Node workflow"""

    db_init(db_name=db_name)

    pairs_config_dict = helpers.read_pairs_config(
        source_config=pairs.DEX_PAIRS.copy(),
    )
    watched_addresses = list(pairs_config_dict.keys())

    with closing(websocket.create_connection(ogmios_url)) as ogmios_ws:
        if config.USE_KUPO and kupo_url:
            if not kupo_helper.kupo_health(kupo_url):
                logger.info("kupo is not healthy!")
                sys.exit(1)
            logger.info("kupo is healthy")

        intersection = helper_functions.find_start_block(ogmios_ws)
        logger.info(intersection)

        last_block_slot = ogmios_helper.ogmios_last_block_slot(ogmios_ws)
        helper_functions.update_status(
            db_name=db_name, database={}, block=last_block_slot
        )

        main_event = Event()
        thread_event = Event()
        reconnect_event = Event()
        thread_populate_utxos = start_thread(
            helper_functions.populate_utxos,
            (
                helpers.AppContext(
                    db_name=db_name,
                    database=None,
                    ogmios_url=ogmios_url,
                    ogmios_ws=ogmios_ws,
                    kupo_url=kupo_url,
                    use_kupo=copy.copy(config.USE_KUPO),
                    main_event=main_event,
                    thread_event=thread_event,
                    reconnect_event=reconnect_event,
                ),
                watched_addresses,
                pairs_config_dict,
            ),
        )

        with closing(websocket.create_connection(ogmios_url)) as ogmios_blocks_ws:
            await helper_functions.parse_blocks(
                app_context=helpers.AppContext(
                    db_name=db_name,
                    database=None,
                    ogmios_url=ogmios_url,
                    ogmios_ws=ogmios_blocks_ws,
                    kupo_url=kupo_url,
                    use_kupo=copy.copy(config.USE_KUPO),
                    main_event=main_event,
                    thread_event=thread_event,
                    reconnect_event=reconnect_event,
                ),
                watched_addresses=watched_addresses,
                pairs_config_dict=pairs_config_dict,
                unsafe=unsafe,
            )
        thread_event.set()
        thread_populate_utxos.join()


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="Orcfax CNT Indexer",
        description="A method of indexing CNT prices from Cardano liquidity pools",
        epilog="for more information visit https://orcfax.io/",
    )

    parser.add_argument(
        "--ogmios-url",
        "-o",
        help="mainnet ogmios url",
        required=False,
        default=config.OGMIOS_URL,
        type=str,
    )

    parser.add_argument(
        "--kupo-url",
        "-k",
        help="mainnet kupo url",
        required=False,
        default=config.KUPO_URL,
        type=str,
    )

    parser.add_argument(
        "--database-location",
        "-d",
        help="database location to store the CNT info",
        required=False,
        default=config.CNT_DB_NAME,
        type=str,
    )

    parser.add_argument(
        "--pairs",
        "-p",
        help="supply a pairs file to the script",
        required=True,
    )

    parser.add_argument(
        "--unsafe",
        "-u",
        help="run the indexer without generic exceptions to capture erroneous behavior",
        required=False,
        action="store_true",
    )

    parser.add_argument(
        "--debug",
        help="enable debug logging",
        required=False,
        action="store_true",
    )

    parser.add_argument(
        "--version",
        "-v",
        help="return indexer version",
        required=False,
        action="store_true",
    )

    return parser.parse_args()


def main() -> None:
    """Primary entry point for this script."""
    args = parse_arguments()

    if args.version:
        print(helper_functions.get_version())
        sys.exit(0)

    # Setup global logging.
    helpers.setup_logging(args.debug)

    if not args.ogmios_url:
        logger.error(
            "ogmios URL is not set. Please set the OGMIOS_URL environment variable or provide the Ogmios URL using the -o parameter."
        )
        sys.exit(1)
    else:
        ogmios_url = args.ogmios_url.rstrip("/")

    if not args.kupo_url:
        logger.warning(
            "kupo URL is not set. It is recommended to set the KUPO_URL environment variable or provide the Kupo URL using the -k parameter."
        )
        kupo_url = ""
    else:
        kupo_url = args.kupo_url.rstrip("/")

    pairs = load_pairs.load(path=args.pairs)

    try:
        asyncio.run(
            indexer_main(
                ogmios_url=ogmios_url,
                kupo_url=kupo_url,
                db_name=args.database_location,
                pairs=pairs,
                unsafe=args.unsafe,
            ),
        )
    except KeyboardInterrupt:
        logger.info("keyboard interrupt received in main, exiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()
