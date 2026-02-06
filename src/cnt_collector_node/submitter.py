"""Calculate the Cardano Native Tokens pairs price from DEX listings"""

# pylint: disable=R0913; # too-many arguments.

import argparse
import asyncio
import json
import logging
import sqlite3
import sys
from pathlib import Path

import websocket

try:
    import config
    import database_abstraction as dba
    import database_initialization
    import global_helpers as helpers
    import helper_functions
    import kupo_helper
    import load_pairs
    import ogmios_helper
except ModuleNotFoundError:
    try:
        from src.cnt_collector_node import config
        from src.cnt_collector_node import database_abstraction as dba
        from src.cnt_collector_node import database_initialization
        from src.cnt_collector_node import global_helpers as helpers
        from src.cnt_collector_node import (
            helper_functions,
            kupo_helper,
            load_pairs,
            ogmios_helper,
        )
    except ModuleNotFoundError:
        from cnt_collector_node import config
        from cnt_collector_node import database_abstraction as dba
        from cnt_collector_node import database_initialization
        from cnt_collector_node import global_helpers as helpers
        from cnt_collector_node import (
            helper_functions,
            kupo_helper,
            load_pairs,
            ogmios_helper,
        )

logger = logging.getLogger(__name__)


def database_init(db_name: str, create_db: bool) -> dict:
    """Database initialization.

    Returns database connection object for use throughout the
    submitter.
    """
    # Connect to the database
    db_path = Path(db_name)
    if not db_path.parent.exists():
        # Create the database folder if it does not exist
        logger.info("creating database directory: %s", db_path)
        db_path.parent.mkdir()
    if create_db:
        database_initialization.create_database(db_name=db_name)
    # Connect to the database
    logger.info("connecting to the database")
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    # create the "database" object.
    database = dba.DBObject(connection=conn, cursor=cur)
    return database


async def initialize_context(
    ogmios_url: str, kupo_url: str, db_name: str, create_db: bool
) -> helpers.AppContext:
    """Initialize the context for the CNT workflow."""
    database = database_init(db_name=db_name, create_db=create_db)
    logger.info("connecting to ogmios")
    ogmios_ws = websocket.create_connection(ogmios_url)

    if config.USE_KUPO and not kupo_helper.kupo_health(kupo_url):
        logger.error("kupo is not healthy!")
        sys.exit(1)

    return helpers.AppContext(
        db_name=db_name,
        database=database,
        ogmios_url=ogmios_url,
        ogmios_ws=ogmios_ws,
        kupo_url=kupo_url,
        use_kupo=config.USE_KUPO,
        main_event=None,
        thread_event=None,
        reconnect_event=None,
    )


async def connect_to_validator(identity: dict, nopublish: bool):
    """Connect to the validator WebSocket if available."""
    web_socket_url = identity.get("validator_web_socket")
    if nopublish:
        return None
    if web_socket_url:
        logger.info("connecting to the validator websocket")
        return helper_functions.connect_to_ws(
            validator_uri=web_socket_url,
            node_id=identity.get("node_id"),
        )
    return None


async def process_dex_pairs(
    app_context: helpers.AppContext,
    identity: dict,
    validator_websocket_conn: websocket.WebSocket,
    pairs: load_pairs.Pairs,
):
    """Process DEX pairs and send messages to the validator."""

    helper_functions.logger.info(
        "searching for len: '%s' dex pairs", len(pairs.DEX_PAIRS)
    )
    for idx, tokens_pair in enumerate(pairs.DEX_PAIRS):
        try:
            message, timestamp = await helper_functions.check_tokens_pair(
                app_context=app_context,
                identity=identity,
                tokens_pair=tokens_pair,
            )
        except sqlite3.OperationalError as err:
            logger.error("database query error: %s", err)
            sys.exit(1)

        if not message:
            logger.error("no message returned for: '%s'", tokens_pair["name"])
            continue

        logger.info(
            "checked message (pair '%s'): '%s' datapoints: '%s' datetime: '%s' ",
            idx,
            tokens_pair["name"],
            len(message["raw"]),
            timestamp,
        )

        message = {
            "message": message,
            "node_id": identity["node_id"],
            "validation_timestamp": timestamp,
        }

        if validator_websocket_conn:
            result = helper_functions.send_to_ws(
                websocket_conn=validator_websocket_conn,
                message=json.dumps(message),
            )
            if not result:
                validator_websocket_conn = helper_functions.connect_to_ws(
                    validator_uri=app_context["web_socket_url"],
                    node_id=identity.get("node_id"),
                )

            return

        # Return to the caller.
        print(json.dumps(message, indent=1))


async def cnt_main(
    ogmios_url: str,
    kupo_url: str,
    db_name: str,
    identity_file: str,
    create_db: bool,
    pairs: load_pairs.Pairs,
    nopublish: bool,
) -> None:
    """CNT Collector Node workflow."""
    app_context = await initialize_context(
        ogmios_url=ogmios_url,
        kupo_url=kupo_url,
        db_name=db_name,
        create_db=create_db,
    )
    database = app_context.database
    ogmios_ws = app_context.ogmios_ws

    epoch = ogmios_helper.ogmios_epoch(ogmios_ws).get("result", 0)
    tip = ogmios_helper.ogmios_tip(ogmios_ws)
    last_block_slot = tip["result"]["slot"]
    logger.info("current epoch: %s", epoch)
    logger.info("latest block slot: %s", last_block_slot)

    identity = await helpers.read_identity(identity_file)
    logger.info("node identity: \n%s", identity)

    validator_websocket_conn = await connect_to_validator(
        identity=identity,
        nopublish=nopublish,
    )

    await process_dex_pairs(
        app_context=app_context,
        identity=identity,
        validator_websocket_conn=validator_websocket_conn,
        pairs=pairs,
    )

    if validator_websocket_conn:
        validator_websocket_conn.close()
    database.connection.close()


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
        help="url of ogmios to access (mainnet only)",
        required=False,
        default=config.OGMIOS_URL,
        type=str,
    )

    parser.add_argument(
        "--kupo-url",
        "-k",
        help="url of kupo to access (mainnet only)",
        required=False,
        default=config.KUPO_URL,
        type=str,
    )

    parser.add_argument(
        "--database-location",
        "-d",
        help="database location with the CNT info",
        required=False,
        default=config.CNT_DB_NAME,
        type=str,
    )

    parser.add_argument(
        "--identity-file-location",
        "-i",
        help="node identity file location, default: /var/tmp/.node-identity.json",
        required=False,
        default=config.NODE_IDENTITY_LOC,
        type=str,
    )

    parser.add_argument(
        "--create-db",
        "-c",
        help="create the database if running standalone",
        required=False,
        action="store_true",
    )

    parser.add_argument(
        "--pairs",
        "-p",
        help="supply a pairs file to the script",
        required=True,
    )

    parser.add_argument(
        "--nopublish",
        help="don't publish to websocket",
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
        help="return submitter version",
        required=False,
        action="store_true",
    )

    return parser.parse_args()


def main() -> None:
    """Primary entry point for this script."""
    args = parse_arguments()

    if len(sys.argv) < 1:
        args.print_help()
        sys.exit()

    if args.version:
        print(helpers.get_version())
        sys.exit(0)

    # Setup global logging.
    helpers.setup_logging(args.debug)

    pairs = load_pairs.load(path=args.pairs)

    # Setup global logging.
    helpers.setup_logging(args.debug)

    asyncio.run(
        cnt_main(
            ogmios_url=args.ogmios_url.rstrip("/"),
            kupo_url=args.kupo_url.rstrip("/"),
            db_name=args.database_location,
            identity_file=args.identity_file_location,
            create_db=args.create_db,
            pairs=pairs,
            nopublish=args.nopublish,
        )
    )


if __name__ == "__main__":
    main()
