"""Helper functions"""

# pylint: disable = W0718  # catching too general exception.
# pylint: disable = R0914  # too many local variables.
# pylint: disable = C0302; # too many lines > 1000.

# Standard library imports
import logging
import sqlite3
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from threading import Event
from time import sleep
from typing import Final, Union

# Third-party imports
import websocket

# Local imports
try:
    import config
    import database_abstraction as dba
    import database_initialization
    import global_helpers as helpers
    import kupo_helper
    import ogmios_helper
    import utxo_objects
except ModuleNotFoundError:
    try:
        from src.cnt_collector_node import config
        from src.cnt_collector_node import database_abstraction as dba
        from src.cnt_collector_node import database_initialization
        from src.cnt_collector_node import global_helpers as helpers
        from src.cnt_collector_node import kupo_helper, ogmios_helper, utxo_objects
    except ModuleNotFoundError:
        from cnt_collector_node import config
        from cnt_collector_node import database_abstraction as dba
        from cnt_collector_node import global_helpers as helpers
        from cnt_collector_node import kupo_helper, ogmios_helper, utxo_objects

logger = logging.getLogger(__name__)

# Script constants.
ACTION_SAVE_OUTPUT: Final[str] = "save_output"
ACTION_SAVE_UTXO: Final[str] = "save_utxo"

TOKEN_DISPLAY_ADA: Final[str] = "ADA"
TOKEN_NAME_LOVELACE: Final[str] = "lovelace"
LOVELACE_DECIMALS: Final[int] = 6

# ADA as base- we keep the dash to ensure accuracy.
ADA_AS_BASE: Final[str] = "ADA-"
ADA_AS_QUOTE: Final[str] = "-ADA"

TOKEN_POLICY_NAME_BLANK: Final[str] = ""

CHAIN_DIRECTION_FWD: Final[str] = "forward"


volume_from_tokens = helpers.cnt_volume_from_tokens


@contextmanager
def database_connection(db_name: str):
    """Context manager for database connections."""
    conn = sqlite3.connect(db_name)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()


def search_db_utxo(
    app_context: helpers.AppContext, tx_inputs: dict, output_contents: dict
) -> list:
    """Search for the transactions inputs into the utxos table."""
    with database_connection(app_context.db_name) as conn:
        utxo_ids = _search_db_utxo(
            conn=conn, tx_inputs=tx_inputs, output_contents=output_contents
        )
    return utxo_ids


def _search_db_utxo(
    conn: sqlite3.Connection,
    tx_inputs: list,
    output_contents: dict,
) -> list:
    """Given a database connection search for the transactions inputs
    into the utxos table.
    """
    utxo_ids = []
    cur = conn.cursor()
    for tx_input in tx_inputs:
        for policy_id in list(output_contents["assets"].keys()):
            for token_name in list(output_contents["assets"][policy_id].keys()):
                query_obj = dba.utxo_id_query_obj(
                    tx_id=tx_input["transaction"]["id"],
                    tx_index=tx_input["index"],
                    policy_id=policy_id,
                    token_name=token_name,
                )
                res = dba.select_utxo_id(
                    cursor=cur,
                    query_params=query_obj,
                )
                if not res:
                    continue
                utxo_ids.append(res.utxo_id)
                logger.info("found an UTxO in the database spent by this transaction")
                logger.info(
                    "utxo_id: %s, tx_hash: %s output_index: %s",
                    res.utxo_id,
                    query_obj.tx_id,
                    query_obj.tx_index,
                )
    return utxo_ids


def check_utxo_for_tokens_pair(
    tokens_pair: utxo_objects.TokensPair,
    output_contents: dict,
) -> bool:
    """Check if the UTxO is one we are monitoring"""
    assets = output_contents["assets"]
    if tokens_pair.pair.startswith("ADA-"):
        token_1_volume = volume_from_tokens(
            output_contents["amount"], tokens_pair.token_1_decimals
        )
        if (
            tokens_pair.token_2_policy in assets
            and tokens_pair.token_2_name in assets[tokens_pair.token_2_policy]
            and token_1_volume > config.MIN_ADA_AMOUNT
        ):
            return True
    elif tokens_pair.pair.endswith("-ADA"):
        token_2_volume = volume_from_tokens(
            output_contents["amount"], tokens_pair.token_2_decimals
        )
        if (
            tokens_pair.token_1_policy in assets
            and tokens_pair.token_1_name in assets[tokens_pair.token_1_policy]
            and token_2_volume > config.MIN_ADA_AMOUNT
        ):
            return True
    else:
        if (
            tokens_pair.token_1_policy in assets
            and tokens_pair.token_1_name in assets[tokens_pair.token_1_policy]
            and tokens_pair.token_2_policy in assets
            and tokens_pair.token_2_name in assets[tokens_pair.token_2_policy]
        ):
            return True
    return False


def _parse_block_transactions_single_tx(  # pylint: disable=R0913
    app_context: helpers.AppContext,
    transaction: dict,
    watched_addresses: list,
    slot: int,
    epoch: int,
    pairs_config_dict: dict,
    unsafe: bool,
):
    """Parse a single transaction from a given block."""
    transaction_id = transaction["id"]
    transaction_inputs = transaction["inputs"]
    transaction_outputs = transaction["outputs"]
    logger.info("transaction ID: %s", transaction_id)
    logger.info("inputs count: %s", len(transaction_inputs))
    logger.info("outputs count: %s", len(transaction_outputs))
    output_counter = 0
    for output in transaction_outputs:
        output_counter += 1
        if output["address"] not in watched_addresses:
            continue
        logger.info("new transaction for %s", output.get("address"))
        try:
            output_contents = ogmios_helper.get_output_content(output)
            utxo_ids = search_db_utxo(
                app_context=app_context,
                tx_inputs=transaction_inputs,
                output_contents=output_contents,
            )
            if not utxo_ids:
                utxo_ids = []
            for tokens_pair in pairs_config_dict[output["address"]]:
                # return if the UTxO doesn't contains a known security token
                if (
                    tokens_pair.security_token_policy not in output_contents["assets"]
                    or tokens_pair.security_token_name
                    not in output_contents["assets"][tokens_pair.security_token_policy]
                ):
                    continue
                # return if the minimum amount of ADA is not reached for a ADA pair
                if (
                    tokens_pair.pair.startswith("ADA-")
                    or tokens_pair.pair.endswith("-ADA")
                ) and (output_contents["amount"] < config.MIN_ADA_AMOUNT):
                    continue
                # make sure the UTxO contains both tokens of a tokens pair that we are watching
                # because some security tokens are identical for many tokens pairs (MinSwapV2)
                if not check_utxo_for_tokens_pair(tokens_pair, output_contents):
                    # exit if no watched tokens pair was found
                    continue
                initial_chain_context = utxo_objects.InitialChainContext(
                    block_height=slot,
                    epoch=epoch,
                    address=output["address"],
                    tx_hash=transaction_id,
                    output_index=(output_counter - 1),
                    utxo_ids=utxo_ids,
                )
                save_output(
                    app_context=app_context,
                    initial_chain_context=initial_chain_context,
                    tokens_pair=tokens_pair,
                    output_contents=output_contents,
                )
        except Exception as err:
            if unsafe:
                raise err
            logger.exception("cannot parse block tx: %s", err)
            logger.warning("parse block output: %s", output)


def parse_block_transactions(  # pylint: disable = R0913
    app_context: helpers.AppContext,
    epoch: int,
    block: dict,
    watched_addresses: list,
    pairs_config_dict: dict,
    unsafe: bool,
) -> None:
    """Parse block transactions"""

    transactions = block["transactions"]
    slot = block["slot"]
    counter = 0
    for transaction in transactions:
        counter += 1
        logger.info("--------------- new tx (%s) ---------------", counter)
        _parse_block_transactions_single_tx(
            app_context=app_context,
            transaction=transaction,
            watched_addresses=watched_addresses,
            slot=slot,
            epoch=epoch,
            pairs_config_dict=pairs_config_dict,
            unsafe=unsafe,
        )


def find_start_block(
    ogmios_ws: websocket.WebSocket,
) -> dict:
    """Find the start block after connecting to Ogmios"""
    tip = ogmios_helper.ogmios_tip(ogmios_ws)
    start_block = tip["result"]
    intersection = ogmios_helper.ogmios_intersection(ogmios_ws, start_block)
    return intersection


async def parse_blocks(
    app_context: helpers.AppContext,
    watched_addresses: list,
    pairs_config_dict: dict,
    unsafe: bool,
) -> None:
    """Parse the realtime blocks"""
    db_name = app_context.db_name
    ogmios_ws: websocket.WebSocket = app_context.ogmios_ws
    main_event: Event = app_context.main_event
    thread_event: Event = app_context.thread_event
    # find the tip, to start from it
    intersection = find_start_block(ogmios_ws)
    counter = 0
    counter_fwd = 0
    counter_bck = 0
    while not main_event.is_set():
        try:
            logger.info("requesting next block...")
            next_block = await ogmios_helper.ogmios_next_block(ogmios_ws)
            epoch = ogmios_helper.ogmios_epoch(ogmios_ws).get("result", 0)
            logger.info("next block received")
            try:
                direction = next_block["result"]["direction"]
            except KeyError:
                logger.info("%s", next_block)
                sys.exit(1)
            counter += 1
            if direction == CHAIN_DIRECTION_FWD:
                counter_fwd += 1
                block = next_block["result"]["block"]
                logger.info(
                    "============================= '%s' =============================",
                    counter,
                )
                block_height = helpers.display_block(block)
                update_status(
                    db_name=db_name,
                    database={},
                    block=block_height,
                )
                parse_block_transactions(
                    app_context=app_context,
                    epoch=epoch,
                    block=block,
                    watched_addresses=watched_addresses,
                    pairs_config_dict=pairs_config_dict,
                    unsafe=unsafe,
                )
            else:
                counter_bck += 1
            # statistics
            if counter % 100 == 0:
                logger.info("counter:  %s", counter)
                logger.info("forward:  %s", counter_fwd)
                logger.info("backward: %s", counter_bck)
        except KeyboardInterrupt:
            main_event.set()
            thread_event.set()
        except (
            ConnectionResetError,
            websocket.WebSocketConnectionClosedException,
        ) as err:
            logger.error("%s", err)
            sleep(1)
            logger.info("reconnecting to Ogmios...")
            # reconnect to Ogmios
            ogmios_ws = websocket.create_connection(app_context.ogmios_url)
            app_context.reconnect_event.set()
            # drop the table utxos on reconnect, to update the table records
            database_initialization.create_database(app_context.db_name)
            # start again from the tip
            intersection = find_start_block(ogmios_ws)
            logger.info("%s", intersection)
    # stats before exiting
    logger.info("counter:  %s", counter)
    logger.info("forward:  %s", counter_fwd)
    logger.info("backward: %s", counter_bck)


def _validate_min_ada(token_volume: float, decimals: int, lovelace_amount: int = -1):
    """Validate token and lovelace amounts against min configured
    value.

    lovelace can be set to minus-one (-1) when we're using this code
    to validate CNT volumes only.
    """
    volume_calculation = volume_from_tokens(token_volume, decimals)
    if not volume_calculation > config.MIN_ADA_AMOUNT:
        logger.debug(
            "token volume calculation too low: %s (min: %s)",
            volume_calculation,
            config.MIN_ADA_AMOUNT,
        )
        return False
    if lovelace_amount == -1:
        logger.debug("ignoring lovelace")
        return True
    lovelace_calculation = volume_from_tokens(lovelace_amount, LOVELACE_DECIMALS)
    if not lovelace_calculation > config.MIN_ADA_AMOUNT:
        logger.debug(
            "lovelace calculation too low: %s (min: %s)",
            lovelace_calculation,
            config.MIN_ADA_AMOUNT,
        )
        return False
    return True


def _validate_token_with_ada_as_base(
    pair: str,
    tokens_pair: utxo_objects.TokensPair,
    utxo: dict,
):
    """Validate token two against the pairs configured for the indexer.

    Token is QUOTE in ADA-QUOTE configured pairing, e.g. ADA-SNEK.
    """
    logger.debug("validating token_pair: %s", pair)
    lovelace_amount = utxo["amount"]
    if tokens_pair.token_2_policy not in utxo["assets"]:
        return False
    if tokens_pair.token_2_name not in utxo["assets"][tokens_pair.token_2_policy]:
        return False
    token_2_volume = utxo["assets"][tokens_pair.token_2_policy][
        tokens_pair.token_2_name
    ]
    if not _validate_min_ada(
        token_2_volume, tokens_pair.token_2_decimals, lovelace_amount
    ):
        return False
    return True


def _validate_token_with_ada_as_quote(
    pair: str,
    tokens_pair: utxo_objects.TokensPair,
    utxo: dict,
):
    """Validate token one against the pairs configured for the indexer.

    Token is BASE in BASE-ADA configured pairing, e.g. DJED-ADA.
    """
    logger.debug("validating token_pair: %s", pair)
    lovelace_amount = utxo["amount"]
    if tokens_pair.token_1_policy not in utxo["assets"]:
        return False
    if tokens_pair.token_1_name not in utxo["assets"][tokens_pair.token_1_policy]:
        return False
    token_1_volume = utxo["assets"][tokens_pair.token_1_policy][
        tokens_pair.token_1_name
    ]
    if not _validate_min_ada(
        token_1_volume, tokens_pair.token_1_decimals, lovelace_amount
    ):
        return False
    return True


def _validate_non_ada_cnt_base_and_quote(
    pair: str,
    tokens_pair: utxo_objects.TokensPair,
    utxo: dict,
):
    """Validate a non-ADA BASE-QUOTE, i.e. no pair is ADA, e.g.
    CNT1-CNT2.

    NB. we are yet to configure a pair like this, and so we should
    monitor its usage and determine if we should just return an
    error.
    """
    if pair.startswith("ADA-") or pair.endswith("-ADA"):
        logger.debug("guard rail against ADA BASE-QUOTE failed: %s", pair)
        return False
    token_1_policy_valid = tokens_pair.token_1_policy not in utxo["assets"]
    token_2_policy_valid = tokens_pair.token_2_policy not in utxo["assets"]
    if token_1_policy_valid or token_2_policy_valid:
        logger.error(
            "token for '%s' not in assets: token1: '%s' token2: '%s'",
            pair,
            token_1_policy_valid,
            token_2_policy_valid,
        )
        return False
    token_1_name_valid = (
        tokens_pair.token_1_name not in utxo["assets"][tokens_pair.token_1_policy]
    )
    token_2_name_valid = (
        tokens_pair.token_2_name not in utxo["assets"][tokens_pair.token_2_policy]
    )
    if token_1_name_valid or token_2_name_valid:
        logger.error(
            "token name for '%s' not in assets: token1: '%s' token2: '%s'",
            pair,
            token_1_name_valid,
            token_2_name_valid,
        )
        return False
    token_1_volume = utxo["assets"][tokens_pair.token_1_policy][
        tokens_pair.token_1_name
    ]
    token_1_decimals = tokens_pair.token_1_decimals
    if not _validate_min_ada(token_1_volume, token_1_decimals):
        logger.error("token1 volume too low: '%s'", pair)
        return False
    token_2_volume = utxo["assets"][tokens_pair.token_2_policy][
        tokens_pair.token_2_name
    ]
    if not _validate_min_ada(token_2_volume, tokens_pair.token_2_decimals):
        logger.error("token2 volume too low: '%s'", pair)
        return False
    return True


def check_if_configured_pair(
    initial_chain_context: utxo_objects.InitialChainContext,
    tokens_pair: utxo_objects.TokensPair,
    utxo: dict,
    utxos_dict: dict,
) -> None:
    """Check if a configured tokens pair was found and save it if true.

    NB. IMPLICIT MODIFIER.
    """

    for policy_id in utxo["assets"]:
        if policy_id != tokens_pair.security_token_policy:
            continue
        for asset_name in utxo["assets"][tokens_pair.security_token_policy]:
            if asset_name != tokens_pair.security_token_name:
                continue
            # make sure the UTxO contains both tokens of a tokens pair that we are watching
            # because some security tokens are identical for many tokens pairs (MinSwapV2)
            pair_found = False
            if tokens_pair.pair.startswith(ADA_AS_BASE):
                if _validate_token_with_ada_as_base(
                    tokens_pair.pair,
                    tokens_pair,
                    utxo,
                ):
                    pair_found = True
            elif tokens_pair.pair.endswith(ADA_AS_QUOTE):
                if _validate_token_with_ada_as_quote(
                    tokens_pair.pair,
                    tokens_pair,
                    utxo,
                ):
                    pair_found = True
            else:
                if _validate_non_ada_cnt_base_and_quote(
                    tokens_pair.pair,
                    tokens_pair,
                    utxo,
                ):
                    pair_found = True
            if not pair_found:
                continue
            save_utxo(
                initial_chain_context=initial_chain_context,
                tokens_pair=tokens_pair,
                utxo=utxo,
                utxos_dict=utxos_dict,
            )


def _populate_utxos_make_context(
    app_context: helpers.AppContext,
    address: str,
):
    """Create a context object for populate UTxOs and return it to
    the caller.
    """
    ogmios_ws = app_context.ogmios_ws
    epoch_from_ogmios = ogmios_helper.ogmios_epoch(ogmios_ws)
    block_height = ogmios_helper.ogmios_last_block_slot(ogmios_ws)
    if not block_height:
        helpers.log_and_raise_error(
            "An Ogmios error has occurred", helpers.OgmiosError, "populate_utxos"
        )
    chain_context = utxo_objects.InitialChainContext(
        address=address,
        epoch=epoch_from_ogmios.get("result", 0),
        block_height=block_height,
        tx_hash=None,
        output_index=None,
    )
    return chain_context


def _populate_utxos_from_on_chain(app_context: helpers.AppContext, address: str):
    """Retrieve utxos and their content from Kupo or Ogmios."""
    utxos = []
    if app_context.kupo_url:
        # Use Kupo.
        kupo_utxos = kupo_helper.get_kupo_matches(app_context.kupo_url, address)
        for item in kupo_utxos:
            content = kupo_helper.get_kupo_utxo_content(item)
            utxos.append(content)
        return utxos
    # Use Ogmios.
    result = ogmios_helper.ogmios_addresses_utxos(app_context.ogmios_ws, [address])
    ogmios_utxos = result.get("result")
    for item in ogmios_utxos:
        content = ogmios_helper.get_ogmios_utxo_content(item)
        utxos.append(content)
    return utxos


def _populate_utxos_collect_runner(
    app_context: helpers.AppContext,
    utxos_dict: dict,
    watched_addresses: list,
    pairs_config_dict: dict,
    thread_event: Event,
):
    """Loops through each watched address and populates a UTxO
    dictionary with updated information.
    """

    # Loop all watched addresses.
    for address in watched_addresses:
        # Make sure that the context is configured correctly.
        chain_context = _populate_utxos_make_context(
            app_context=app_context,
            address=address,
        )
        logger.info("reading the UTxOs from the address %s...", address)
        # Read UTxOs from onchain.
        utxos = _populate_utxos_from_on_chain(
            app_context=app_context,
            address=address,
        )
        # Log how many UTxOs were discovered. If we haven't
        # usable data log the exception.
        try:
            logger.info("%s UTxO(s) found", len(utxos))
        except TypeError as err:
            logger.error("%s", err)
            logger.warning("%s", utxos)
            continue
        # For each UTxO perform a check to see if they were
        # configured and if so, save the UTxO.
        for utxo in utxos:
            if thread_event.is_set():
                break
            for tokens_pair in pairs_config_dict[address]:
                # Saves to database if configured.
                check_if_configured_pair(
                    initial_chain_context=chain_context,
                    tokens_pair=tokens_pair,
                    utxo=utxo,
                    utxos_dict=utxos_dict,
                )
        if thread_event.is_set():
            break


def populate_utxos(
    app_context: helpers.AppContext,
    watched_addresses: list,
    pairs_config_dict: dict,
) -> None:
    """Initial read of the UTxOs content from Ogmios"""
    thread_event: Event = app_context.thread_event
    main_event: Event = app_context.main_event
    reconnect_event: Event = app_context.reconnect_event
    utxos_dict = {}
    while not thread_event.is_set():
        reconnect_event.clear()
        try:
            # NB. needs to return a utxos_dict object not modify it
            # implicitly.
            _populate_utxos_collect_runner(
                app_context=app_context,
                utxos_dict=utxos_dict,
                watched_addresses=watched_addresses,
                pairs_config_dict=pairs_config_dict,
                thread_event=thread_event,
            )
            if thread_event.is_set():
                main_event.set()
                break
            # Inserts a datapoint into the database if the parameters
            # are correct.
            save_utxos_dict(app_context.db_name, utxos_dict)
            # Clear the UTxOs dict so as not to maintain state, and
            # then sleep.
            utxos_dict = {}
            logger.info("sleeping for '%s' seconds...", config.UTXOS_THREAD_TIMEOUT)
            for _ in range(config.UTXOS_THREAD_TIMEOUT):
                if reconnect_event.is_set():
                    logger.info("exiting loop to repopulate the utxos table...")
                    break
                if thread_event.is_set():
                    main_event.set()
                    break
                sleep(1)
        except ConnectionRefusedError:
            # Cannot connect to Kupo
            logger.error("connection refused")
            sleep(1)
        except (
            ConnectionResetError,
            websocket.WebSocketConnectionClosedException,
            helpers.OgmiosError,
        ) as err:
            logger.error("%s", err)
            sleep(1)
            logger.info("reconnecting to Ogmios...")
            # reconnect to Ogmios
            ogmios_ws = websocket.create_connection(app_context.ogmios_url)
            # drop the table utxos on reconnect, to update the table records
            # start again from the tip
            intersection = find_start_block(ogmios_ws)
            logger.info("%s", intersection)
        except KeyboardInterrupt:
            main_event.set()
            thread_event.set()


def save_utxos_dict(db_name: str, utxos_dict: dict) -> None:
    """Wrap _save_utxos_dict to make it testable.

    NB. IMPLICIT MODIFIER.
    """
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cur,
    )
    _save_utxos_dict(
        database=db,
        utxos_dict=utxos_dict,
    )
    # Bookend from the _save_utxos_dict.
    # Double check if we need to close the connection here at all.
    conn.commit()
    conn.close()


def _save_utxos_dict(database: dba.DBObject, utxos_dict: dict) -> None:
    """Save the liquidity pools UTxOs from the polulate_utxos thread into the database"""
    for pair in utxos_dict:
        for source in utxos_dict[pair]:
            context = utxos_dict[pair][source]["context"]
            tokens_pair_dict = utxos_dict[pair][source]["tokens_pair"]
            tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
            # Map the UTxO context to a read only context object.
            utxo_update_context = utxo_objects.UTxOUpdateContext(
                block_height=context["block_height"],
                epoch=context["epoch"],
                address=context["address"],
                tx_hash=context["tx_hash"],
                output_index=context["output_index"],
                caller=context["caller"],
                token_1_amount=context["token1_amount"],
                token_2_amount=context["token2_amount"],
                price=context["price"],
            )
            saved_utxo_record = save_utxo_record(
                database=database,
                utxo_update_context=utxo_update_context,
                tokens_pair=tokens_pair,
            )
            if not saved_utxo_record:
                continue
            price_record = dba.price_record_obj_from_dicts(
                tokens_pair=tokens_pair,
                utxo_update_context=utxo_update_context,
            )
            dba.insert_price_record(db=database, price_record=price_record)


def update_status(db_name: str, database: dict, block: int) -> None:
    """Update the status table on the script startup"""
    db = database
    if not database:
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()
        db = dba.DBObject(
            connection=conn,
            cursor=cur,
        )
    logger.info("latest block slot: %s", block)
    previous_block = dba.get_status(db)
    if previous_block is None:
        dba.insert_status(db=db, block=block)
        if not database:
            conn.commit()
            conn.close()
        return True
    if block <= previous_block:
        return False
    dba.update_status(db=db, block=block)
    if not database:
        conn.commit()
        conn.close()
    return True


def get_status_block(cur: sqlite3.Cursor) -> int:
    """Get the latest block height from the status table"""
    db = dba.DBObject(
        connection=None,
        cursor=cur,
    )
    block = dba.get_status(db=db)
    if block is None:
        return 0
    return block


def connect_to_ws(validator_uri: str, node_id: str) -> websocket.WebSocket:
    """Connect to the configured web-socket.

    Websocket URL, e.g. locally; `ws://localhost:8000/ws` with no
    trailing slash.
    """
    validator_connection = f"{validator_uri}/{node_id}/"
    try:
        websocket_conn: websocket.WebSocket = websocket.create_connection(
            validator_connection
        )
        logger.info("connected to the validator web-socket")
    except ConnectionRefusedError:
        logger.error(
            "error connecting to the validator web-socket (%s): ConnectionRefusedError",
            validator_connection,
        )
        websocket_conn = None
    return websocket_conn


def send_to_ws(websocket_conn: websocket.WebSocket, message: str) -> bool:
    """Send the data to the validator web-socket, return the delivery status as bool"""
    try:
        websocket_conn.send(message)
        msg = websocket_conn.recv()
        if "ERROR" in msg:
            logger.error("error sending message to validator web-socket: %s", msg)
            return False
        logger.info("%s", msg)
        return True
    except Exception as err:  # pylint: disable=W0718
        logger.error("error sending message to validator web-socket: %s", err)
        return False


def parse_utxo(
    database: dba.DBObject,
    utxo: dict,
    epoch: int,
    block_height: int,
    tokens_details: dict,
) -> dict:
    """Process the matched UTxO in order to save a new data point into the database"""
    try:
        if tokens_details.get("token1_name") == TOKEN_NAME_LOVELACE:
            token1_amount = utxo["amount"]
            token1 = TOKEN_NAME_LOVELACE
            token1_display_name = TOKEN_DISPLAY_ADA
        else:
            token1_amount = (
                utxo["assets"]
                .get(tokens_details.get("token1_policy"), {})
                .get(tokens_details.get("token1_name"), 0)
            )
            token1 = f"{tokens_details.get('token1_policy')}.{tokens_details.get('token1_name')}"
            token1_display_name = str(tokens_details.get("token1_name"))

        if tokens_details.get("token2_name") == TOKEN_NAME_LOVELACE:
            token2_amount = utxo["amount"]
            token2 = TOKEN_NAME_LOVELACE
            token2_display_name = TOKEN_DISPLAY_ADA
        else:
            token2_amount = (
                utxo["assets"]
                .get(tokens_details.get("token2_policy"), {})
                .get(tokens_details.get("token2_name"), 0)
            )
            token2 = f"{tokens_details.get('token2_policy')}.{tokens_details.get('token2_name')}"
            token2_display_name = str(tokens_details.get("token2_name"))
        if token1_amount > 0 and token2_amount > 0:
            token_1_decimals = tokens_details.get("token1_decimals")
            token_2_decimals = tokens_details.get("token2_decimals")
            token1_real_amount = volume_from_tokens(token1_amount, token_1_decimals)
            token2_real_amount = volume_from_tokens(token2_amount, token_2_decimals)
            if token1_real_amount <= config.MIN_ADA_AMOUNT:
                return {}
            if token2_real_amount <= config.MIN_ADA_AMOUNT:
                return {}
            price = token2_real_amount / token1_real_amount
            logger.info(
                "%s pair price on %s: %s",
                tokens_details.get("feed"),
                tokens_details.get("source"),
                price,
            )
            logger.info(
                "{%s} {%s} / {%s} {%s}",
                token1_real_amount,
                token1_display_name,
                token2_real_amount,
                token2_display_name,
            )
            price_record = dba.price_record_obj(
                pair=tokens_details.get("feed"),
                epoch=epoch,
                block_height=block_height,
                price=price,
                token_1_amount=token1_amount,
                token_2_amount=token2_amount,
                source=tokens_details.get("source"),
            )
            dba.insert_price_record(db=database, price_record=price_record)
            info = {
                "utxo": f"{utxo['tx_hash']}#{str(utxo['tx_index'])}",
                "token1_volume": token1_real_amount,
                "token2_volume": token2_real_amount,
                "price": price,
                "amounts": {
                    token1: token1_amount,
                    token2: token2_amount,
                },
            }
        else:
            logger.debug("one of the tokens amounts is 0!")
            logger.debug("%s", tokens_details)
            logger.debug("%s", utxo)
            info = {}
    except KeyError as err:
        info = {}
        logger.exception("cannot parse utxo: %s", err)
    return info


def check_dex_tokens_pair(
    database: dba.DBObject,
    epoch: int,
    block_height: int,
    tokens_pair: utxo_objects.TokensPair,
    utxos: list,
) -> dict:
    """Check tokes pairs from a smart contract address to find a specific pair"""

    # pylint: disable=R0912

    if tokens_pair.token_1_name == TOKEN_NAME_LOVELACE:
        token1_policy = TOKEN_POLICY_NAME_BLANK
        token1_name = TOKEN_NAME_LOVELACE
    else:
        token1_policy = tokens_pair.token_1_policy
        token1_name = tokens_pair.token_1_name
    if tokens_pair.token_2_name == TOKEN_NAME_LOVELACE:
        token2_policy = TOKEN_POLICY_NAME_BLANK
        token2_name = TOKEN_NAME_LOVELACE
    else:
        token2_policy = tokens_pair.token_2_policy
        token2_name = tokens_pair.token_2_name
    logger.info(
        "pair: '%s' ( '%s' - '%s' )", tokens_pair.pair, token1_name, token2_name
    )
    all_infos = []
    for utxo in utxos:
        security_token_policy = tokens_pair.security_token_policy
        security_token_name = tokens_pair.security_token_name
        # Search for the security token in the UTxO.
        security_token_found = False
        for policy_id in utxo["assets"]:
            if policy_id != security_token_policy:
                continue
            for asset_name in utxo["assets"][security_token_policy]:
                if str(asset_name) != str(security_token_name):
                    continue
                security_token_found = True
        if not security_token_found:
            continue
        info = parse_utxo(
            database=database,
            utxo=utxo,
            epoch=epoch,
            block_height=block_height,
            tokens_details={
                "feed": tokens_pair.pair,
                "source": tokens_pair.source,
                "token1_policy": token1_policy,
                "token1_name": token1_name,
                "token1_decimals": tokens_pair.token_1_decimals,
                "token2_policy": token2_policy,
                "token2_name": token2_name,
                "token2_decimals": tokens_pair.token_2_decimals,
                "security_token_policy": security_token_policy,
                "security_token_name": security_token_name,
            },
        )
        if not info:
            # If we don't have an info object, e.g. because it's real
            # amount is too low, we continue.
            continue
        all_infos.append(info)
    if len(all_infos) > config.MAX_LPS:
        # Provide guardrail logging to help us get more information
        # in future if LP handling changes.
        #
        # NB. provides a further guardrail if we receive too many info
        # objects, e.g. if MIN ADA isn't configured correctly.
        logger.error(
            "multiple valid liquidity pools found for token pair: %s", tokens_pair.pair
        )
        return {}
    try:
        return all_infos[0]
    except IndexError:
        logger.debug("no liquidity pools found for: %s", tokens_pair.pair)
    return {}


def _get_utxos_content(app_context: helpers.AppContext, utxos: list) -> list:
    """Read UTxO content from on-chain given a UTxO pointer."""
    utxos_content = []
    for utxo in utxos:
        # Use Kupo.
        if app_context.use_kupo:
            utxo_content = kupo_helper.get_kupo_utxo_content(utxo)
            utxos_content.append(utxo_content)
            continue
        # Use Ogmios.
        utxo_content = ogmios_helper.get_ogmios_utxo_content(utxo)
        utxos_content.append(utxo_content)
    return utxos_content


def _get_token_labels(row):
    """Determine token labels given the results returned from
    the database.
    """
    if (
        row.token_1_policy == TOKEN_POLICY_NAME_BLANK
        and row.token_1_name == TOKEN_NAME_LOVELACE
    ):
        token1 = TOKEN_NAME_LOVELACE
    else:
        token1 = f"{row.token_1_policy}.{row.token_1_name}"
    if (
        row.token_2_policy == TOKEN_POLICY_NAME_BLANK
        and row.token_2_name == TOKEN_NAME_LOVELACE
    ):
        token2 = TOKEN_NAME_LOVELACE
    else:
        token2 = f"{row.token_2_policy}.{row.token_2_name}"
    return token1, token2


def retrieve_utxo_token_info_from_db(
    database: dba.DBObject,
    tokens_pair: utxo_objects.TokensPair,
    last_block_slot: int,
):
    """Retrieve utxo and token information from the database."""
    # Check if the information is in the indexer table "utxos"
    # Get the current block height from the status table to check if the database data is current or old
    current_status_block = get_status_block(database.cursor)
    query_obj = dba.utxo_source_policy_query_obj(
        pair=tokens_pair.pair,
        source=tokens_pair.source,
        address=None,
        security_token_policy=tokens_pair.security_token_policy,
        security_token_name=tokens_pair.security_token_name,
    )
    row = dba.select_utxo_record_by_pair_source_and_policy(
        db=database, query_obj=query_obj
    )
    logger.info(
        "last_block_slot: %s, current_status_block: %s",
        last_block_slot,
        current_status_block,
    )
    info = {}
    if not row or last_block_slot > current_status_block:
        logger.debug("current data doesn't exist or is out of date, querying ogmios")
        return info
    # if the data collected by the indexer is current, use it
    logger.info("using index: '%s' - '%s'", tokens_pair.pair, tokens_pair.source)
    token1, token2 = _get_token_labels(row)
    info = {
        "utxo": f"{row.tx_hash}#{row.output_index}",
        "token1_volume": volume_from_tokens(row.token_1_volume, row.token_1_decimals),
        "token2_volume": volume_from_tokens(row.token_2_volume, row.token_2_decimals),
        "price": volume_from_tokens(row.token_2_volume, row.token_2_decimals)
        / volume_from_tokens(row.token_1_volume, row.token_1_decimals),
        "amounts": {
            token1: row.token_1_volume,
            token2: row.token_2_volume,
        },
    }
    if not info:
        logger.error(
            "information object for '%s' couldn't be created", tokens_pair.pair
        )
        return info
    return info


def retrieve_utxo_token_info_from_chain_index(
    app_context: helpers.AppContext,
    tokens_pair: utxo_objects.TokensPair,
    last_block_slot: int,
):
    """Retrieve utxo and token information from ogmios."""
    logger.info(
        "not using index: '%s' - '%s' - '%s'",
        tokens_pair.pair,
        tokens_pair.source,
        tokens_pair.address,
    )
    ogmios_ws: websocket.WebSocket = app_context.ogmios_ws
    kupo_url: str = app_context.kupo_url
    # 1. Connect to Ogmios.
    epoch = ogmios_helper.ogmios_epoch(ogmios_ws).get("result", 0)
    if app_context.use_kupo:
        utxos = kupo_helper.get_kupo_matches(kupo_url, tokens_pair.address)
    else:
        # 2. Connect to Ogmios.
        result = ogmios_helper.ogmios_addresses_utxos(ogmios_ws, [tokens_pair.address])
        utxos = result.get("result")
    logger.info("found '%s' UTxO(s)", len(utxos))
    utxos_content = _get_utxos_content(
        app_context=app_context,
        utxos=utxos,
    )
    info = check_dex_tokens_pair(
        database=app_context.database,
        epoch=epoch,
        block_height=last_block_slot,
        tokens_pair=tokens_pair,
        utxos=utxos_content,
    )
    if not info:
        logger.error(
            "information object for '%s' couldn't be created", tokens_pair.pair
        )
        return info
    return info


def check_address_pair(
    app_context: helpers.AppContext,
    tokens_pair: utxo_objects.TokensPair,
    last_block_slot: int,
) -> dict:
    """Check the token pair at the specified address

    Create and return a message with the following format:
    {
        "block_height": 111087863,
        "source": "WingRiders",
        "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnayg6pp9snyy9v7uwarxd7dqc5k52egtc49y5w5h3nqqdy6qs2nzs8y",
        "feed": "ADA-FACT",
        "utxo": "0237ccb53f4a69589bc5e138057cc98fe70cbb852a51c2db8b0d1ef009563916#0",
        "token1_volume": 1165552.299359,
        "token2_volume": 35849891.509507,
        "price": 30.75785748,
        "amounts":
        {
            "lovelace": 1165552299359,
            "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e51.6f7263666178746f6b656e": 35849891509507
        }
    }
    """
    # Create the message about the current tokens pairs
    feed_info = {
        "token1_name": tokens_pair.token_1_name,
        "token1_decimals": tokens_pair.token_1_decimals,
        "token2_name": tokens_pair.token_2_name,
        "token2_decimals": tokens_pair.token_2_decimals,
        "block_height": last_block_slot,
        "source": tokens_pair.source,
        "collector": tokens_pair.collector,
        "address": tokens_pair.address,
        "feed": tokens_pair.pair,
    }
    info = retrieve_utxo_token_info_from_db(
        database=app_context.database,
        tokens_pair=tokens_pair,
        last_block_slot=last_block_slot,
    )
    if info:
        for key, value in info.items():
            feed_info[key] = value
        return feed_info
    # otherwise collect the data from Ogmios / Kupo
    info = retrieve_utxo_token_info_from_chain_index(
        app_context=app_context,
        tokens_pair=tokens_pair,
        last_block_slot=last_block_slot,
    )
    if not info:
        return {}
    for key, value in info.items():
        feed_info[key] = value
    return feed_info


async def _get_source_messages(
    app_context: helpers.AppContext,
    tokens_pair: utxo_objects.TokensPair,
    feed: str,
) -> list:
    """Query on-chain for the data we require for each pair at each
    given source.
    """
    ogmios_ws: websocket.WebSocket = app_context.ogmios_ws
    last_block_slot = ogmios_helper.ogmios_last_block_slot(ogmios_ws)
    source_messages = []
    for source in tokens_pair.get("sources", []):
        source_message = check_address_pair(
            app_context=app_context,
            tokens_pair=utxo_objects.TokensPair(
                pair=tokens_pair.get("name"),
                source=source.get("source"),
                token_1_policy=tokens_pair.get("token1_policy"),
                token_1_name=tokens_pair.get("token1_name"),
                token_1_decimals=tokens_pair.get("token1_decimals"),
                token_2_policy=tokens_pair.get("token2_policy"),
                token_2_name=tokens_pair.get("token2_name"),
                token_2_decimals=tokens_pair.get("token2_decimals"),
                security_token_policy=source.get("security_token_policy"),
                security_token_name=source.get("security_token_name"),
                # Submitter specific. Possibly belongs in InitialContext.
                address=source.get("address"),
                collector=helpers.get_user_agent(),
            ),
            last_block_slot=last_block_slot,
        )
        if not source_message:
            logger.error(
                "could not generate source message: '%s' check liquidity pool", feed
            )
            continue
        source_messages.append(source_message)
    return source_messages


async def check_tokens_pair(
    app_context: helpers.AppContext,
    identity: dict,
    tokens_pair: utxo_objects.TokensPair,
) -> Union[tuple[dict | str] | tuple[None | str]]:
    """Check a tokens pair"""
    feed = tokens_pair.get("name")
    source_messages = await _get_source_messages(
        app_context=app_context,
        tokens_pair=tokens_pair,
        feed=feed,
    )
    if not source_messages:
        logger.warning("no source messages for: %s", feed)
        return {}, ""
    now_dt = helpers.get_utc_timestamp_now()
    message = await helpers.generate_validator_message(
        feed=feed,
        identity=identity,
        source_messages=source_messages,
        now_dt=now_dt,
    )
    return message, now_dt


def validate_and_save_utxo_update(
    database: dba.DBObject,
    res: dba.UTxORecordResults,
    pair_name: str,
    utxo_update_context: utxo_objects.UTxOUpdateContext,
):
    """Given a request to save the UTxO check that it can be saved
    and then perform the database update or insert.

    NB. NEEDS RETURN.
    """
    # first, check if the block is more recent than the block when the record was updated previously
    # then, make sure at least one of the tokens amounts is bigger than the previous ones
    # this resolves 2 issues:
    #
    #   1. make sure this is the biggest liquidity pool on this DEX for this tokens pair
    #   2. do not update (the populate_utxos could do this) if nothing has changed (no new transactions occurred)
    #
    updated = False
    context_block_height = utxo_update_context.block_height
    action = utxo_update_context.caller
    if not context_block_height >= res.block_height:
        return updated
    context_tx_hash = utxo_update_context.tx_hash
    context_output_index = utxo_update_context.output_index
    if (
        f"{res.tx_hash}#{res.output_index}"
        == f"{context_tx_hash}#{context_output_index}"
    ):
        return updated
    token_1_volume = volume_from_tokens(res.token_1_amount, res.token_1_decimals)
    token_2_volume = volume_from_tokens(res.token_2_amount, res.token_2_decimals)
    if not (
        token_1_volume > config.MIN_ADA_AMOUNT
        and token_2_volume > config.MIN_ADA_AMOUNT
    ):
        return updated
    context_utxo_ids = utxo_update_context.utxo_ids
    if not context_utxo_ids:
        context_utxo_ids = []
    if not (
        (action != ACTION_SAVE_UTXO and len(context_utxo_ids) > 0)
        or action == ACTION_SAVE_UTXO
    ):
        return updated
    logger.info("updating '%s' in the utxos table...", pair_name)
    context_price = utxo_update_context.price
    context_token_1_amount = utxo_update_context.token_1_amount
    context_token_2_amount = utxo_update_context.token_2_amount
    context_tx_hash = utxo_update_context.tx_hash
    context_tx_index = utxo_update_context.output_index
    utxo_obj = dba.partial_utxo_obj(
        block_height=context_block_height,
        price=context_price,
        token_1_amount=context_token_1_amount,
        token_2_amount=context_token_2_amount,
        tx_hash=context_tx_hash,
        tx_index=context_tx_index,
    )
    dba.update_utxo_partial(db=database, utxo_record=utxo_obj, row_id=res.row_id)
    updated = update_status(
        db_name="",
        database=database,
        block=context_block_height,
    )
    return updated


def save_utxo_record(
    database: dba.DBObject,
    utxo_update_context: utxo_objects.UTxOUpdateContext,
    tokens_pair: utxo_objects.TokensPair,
) -> bool:
    """Insert or update a record into the utxos table.

    NB. IMPLICIT MODIFIER.
    """

    updated = False
    pair_name = f"{tokens_pair.pair} on {tokens_pair.source}"
    if utxo_update_context.caller == ACTION_SAVE_OUTPUT:
        # if this is called from save_output (a live transaction)
        utxo_count = dba.select_utxo_count_by_tx_info(
            db=database,
            tx_hash=utxo_update_context.tx_hash,
            output_index=utxo_update_context.output_index,
        )
        if utxo_count == 1:
            # a rollback happened, transaction already processed -- no update.
            return updated
    res = None
    utxo_ids = utxo_update_context.utxo_ids
    zeroth_utxo_id = None
    try:
        zeroth_utxo_id = utxo_ids[0]
    except IndexError:
        pass
    if utxo_update_context.caller == ACTION_SAVE_OUTPUT and len(utxo_ids) == 1:
        # if this is called from save_output (a live transaction)
        res = dba.select_utxo_by_id(db=database, id_=zeroth_utxo_id)
    elif utxo_update_context.caller == ACTION_SAVE_UTXO:
        # if this is called from the save_utxo (populate_utxos thread)
        address = utxo_update_context.address
        query_obj = dba.utxo_source_policy_query_obj(
            pair=tokens_pair.pair,
            source=tokens_pair.source,
            address=address,
            security_token_policy=tokens_pair.security_token_policy,
            security_token_name=tokens_pair.security_token_name,
        )
        res = dba.select_utxo_record_by_source_address_and_policy(
            db=database, query_obj=query_obj
        )
    if not res and utxo_update_context.caller == ACTION_SAVE_UTXO:
        logger.info("inserting '%s' into the utxos table...", pair_name)
        complete_utxo_obj = dba.complete_utxo_obj_from_dicts(
            tokens_pair=tokens_pair,
            utxo_update_context=utxo_update_context,
        )
        dba.insert_utxo_complete(db=database, utxo_record=complete_utxo_obj)
        block_height = utxo_update_context.block_height
        updated = update_status(
            db_name="",
            database=database,
            block=block_height,
        )
        return updated
    updated = validate_and_save_utxo_update(
        database=database,
        res=res,
        pair_name=pair_name,
        utxo_update_context=utxo_update_context,
    )
    return updated


def save_output(
    app_context: helpers.AppContext,
    initial_chain_context: utxo_objects.InitialChainContext,
    tokens_pair: utxo_objects.TokensPair,
    output_contents: dict,
):
    """Wrapper for _save_output to separate database connection
    calls from function logic.

    NB. IMPLICIT MODIFIER.
    """
    conn = sqlite3.connect(app_context.db_name)
    cur = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cur,
    )
    _save_output(
        database=db,
        initial_chain_context=initial_chain_context,
        tokens_pair=tokens_pair,
        output_contents=output_contents,
    )
    # Bookend save_output database functions.
    conn.commit()
    conn.close()


def _save_output(
    database: dba.DBObject,
    initial_chain_context: utxo_objects.InitialChainContext,
    tokens_pair: utxo_objects.TokensPair,
    output_contents: dict,
) -> None:
    """Process the matched UTxO in order to save a new data point into the database
    This is called when a new matching block transaction is found.

    NB. IMPLICIT MODIFIER.
    """

    # pylint: disable=R0902
    @dataclass
    class TempContext:
        """Mutable object allowing us to build state before writing
        to a utxo_objects.UTxOUpdateContext.
        """

        block_height: int
        epoch: int
        address: str
        tx_hash: str
        output_index: int
        caller: Union[str | None] = None
        token_1_amount: Union[int | None] = None
        token_2_amount: Union[int | None] = None
        price: Union[float | None] = None

    temp_context = TempContext(
        block_height=initial_chain_context.block_height,
        epoch=initial_chain_context.epoch,
        address=initial_chain_context.address,
        tx_hash=initial_chain_context.tx_hash,
        output_index=initial_chain_context.output_index,
        caller=ACTION_SAVE_OUTPUT,
    )

    # Set token 1 volume.
    if tokens_pair.token_1_name == TOKEN_NAME_LOVELACE:
        temp_context.token_1_amount = output_contents["amount"]
    else:
        temp_context.token_1_amount = (
            output_contents["assets"]
            .get(tokens_pair.token_1_policy, {})
            .get(tokens_pair.token_1_name, 0)
        )

    # Set token 2 volune.
    if tokens_pair.token_2_name == TOKEN_NAME_LOVELACE:
        temp_context.token_2_amount = output_contents["amount"]
    else:
        temp_context.token_2_amount = (
            output_contents["assets"]
            .get(tokens_pair.token_2_policy, {})
            .get(tokens_pair.token_2_name, 0)
        )

    # Assign values.
    context_token_1_volume = temp_context.token_1_amount
    context_token_1_decimals = tokens_pair.token_1_decimals
    context_token_2_volume = temp_context.token_2_amount
    context_token_2_decimals = tokens_pair.token_2_decimals

    pair_name = f"{tokens_pair.pair} on {tokens_pair.source}"

    # Token volumes are zero or less and so this data needs to be
    # looked at in more detail.
    if context_token_1_volume <= 0 or context_token_2_volume <= 0:
        logger.warning(
            "an amount of tokens for the '%s' pair is 0, this needs to be investigated!",
            pair_name,
        )
        logger.warning("%s", output_contents)
        return

    # Otherwise, determine price and add to context.
    token1_real_amount = volume_from_tokens(
        context_token_1_volume, context_token_1_decimals
    )
    token2_real_amount = volume_from_tokens(
        context_token_2_volume, context_token_2_decimals
    )

    # Assign price.
    price = token2_real_amount / token1_real_amount
    temp_context.price = price

    update_utxo_chain_context = utxo_objects.UTxOUpdateContext(
        caller=temp_context.caller,
        epoch=temp_context.epoch,
        block_height=temp_context.block_height,
        tx_hash=initial_chain_context.tx_hash,
        output_index=initial_chain_context.output_index,
        address=temp_context.address,
        price=temp_context.price,
        token_1_amount=temp_context.token_1_amount,
        token_2_amount=temp_context.token_2_amount,
        utxo_ids=initial_chain_context.utxo_ids,
    )

    # Attempt to save record.
    utxo_saved = save_utxo_record(
        database=database,
        utxo_update_context=update_utxo_chain_context,
        tokens_pair=tokens_pair,
    )
    if not utxo_saved:
        return

    # If successfully saved, create a price record object and update
    # the database.
    price_record_obj = dba.price_record_obj_from_dicts(
        tokens_pair=tokens_pair,
        utxo_update_context=update_utxo_chain_context,
    )
    dba.insert_price_record(db=database, price_record=price_record_obj)
    return


def utxos_dict_update(
    utxo_update_context: utxo_objects.UTxOUpdateContext,
    utxos_dict: dict,
    tokens_pair: utxo_objects.TokensPair,
) -> None:
    """Add a new tokens pair to the utxos_dict. Keep only the biggest
    lquidity pool from each DEX. This is called from the populate_utxos
    thread.

    NB. IMPLICIT MODIFIER.
    """

    # Convert to dictionary to be compatible with current UTxO handling.
    tokens_pair_dict = utxo_objects.tokens_pair_dict_from_obj(tokens_pair)

    # Value to update utxos_dict with below.
    context_tokens_pair = {
        "context": utxo_objects.chain_context_adapter(utxo_update_context),
        "tokens_pair": tokens_pair_dict,
    }

    if tokens_pair.pair not in utxos_dict:
        # The pair, e.g. SNEK-ADA, FACT-DJED does not exist in the
        # utxos dictionary. First it needs to be created and then
        # it can be added to the structure.
        utxos_dict[tokens_pair.pair] = {}
        utxos_dict[tokens_pair.pair][tokens_pair.source] = context_tokens_pair
        return

    utxo_dict_pair = utxos_dict[tokens_pair.pair]

    if tokens_pair.source not in utxo_dict_pair:
        # The pair exists in the dictionary but the source information
        # doesn't.
        utxos_dict[tokens_pair.pair][tokens_pair.source] = context_tokens_pair
        # utxos_dict changed, return.
        return

    utxos_dict_tokens_source = utxos_dict[tokens_pair.pair][tokens_pair.source]

    utxo_token_1_volume = utxos_dict_tokens_source["context"]["token1_amount"]
    utxo_token_2_volume = utxos_dict_tokens_source["context"]["token2_amount"]

    context_token_1_volume = (
        utxo_update_context.token_1_amount
    )  # chain_context["token1_amount"]
    context_token_2_volume = (
        utxo_update_context.token_2_amount
    )  # chain_context["token2_amount"]

    if (
        utxo_token_1_volume < context_token_1_volume
        and utxo_token_2_volume < context_token_2_volume
    ):
        # The current volume is less than the new volume, update the
        # values.
        utxos_dict[tokens_pair.pair][tokens_pair.source] = context_tokens_pair
        # utxos_dict changed, return.
        return

    # utxos_dict remains unchanged.
    return


def save_utxo(
    initial_chain_context: utxo_objects.InitialChainContext,
    tokens_pair: utxo_objects.TokensPair,
    utxo: dict,
    utxos_dict: dict,
) -> None:
    """Process the matched UTxO in order to save a new data point into the database
    This is called from the populate_utxos thread.

    NB. IMPLICIT MODIFIER.
    """

    utxo_tx_hash = utxo["tx_hash"]
    utxo_tx_index = utxo["tx_index"]
    utxo_amount = utxo["amount"]
    action = ACTION_SAVE_UTXO

    # pylint: disable=R0902
    @dataclass
    class TempContext:
        """Mutable object allowing us to build state before writing
        to a utxo_objects.UTxOUpdateContext.
        """

        block_height: int
        epoch: int
        address: str
        tx_hash: str
        output_index: int
        caller: Union[str | None] = None
        token_1_amount: Union[int | None] = None
        token_2_amount: Union[int | None] = None
        price: Union[float | None] = None

    temp_context = TempContext(
        block_height=initial_chain_context.block_height,
        epoch=initial_chain_context.epoch,
        address=initial_chain_context.address,
        tx_hash=utxo_tx_hash,
        output_index=utxo_tx_index,
        caller=action,
    )

    # Set token 1 name.
    if tokens_pair.token_1_name == TOKEN_NAME_LOVELACE:
        temp_context.token_1_amount = utxo_amount
    else:
        temp_context.token_1_amount = (
            utxo["assets"]
            .get(tokens_pair.token_1_policy, {})
            .get(tokens_pair.token_1_name, 0)
        )

    # Set token 2 name.
    if tokens_pair.token_2_name == TOKEN_NAME_LOVELACE:
        temp_context.token_2_amount = utxo_amount
    else:
        temp_context.token_2_amount = (
            utxo["assets"]
            .get(tokens_pair.token_2_policy, {})
            .get(tokens_pair.token_2_name, 0)
        )

    # Assign values.
    context_token_1_volume = temp_context.token_1_amount
    context_token_1_decimals = tokens_pair.token_1_decimals
    context_token_2_volume = temp_context.token_2_amount
    context_token_2_decimals = tokens_pair.token_2_decimals

    pair_name = f"{tokens_pair.pair} on {tokens_pair.source}"

    # Token volumes are zero or less and so this data needs to be
    # looked at in more detail.
    if context_token_1_volume <= 0 or context_token_2_volume <= 0:
        logger.warning(
            "an amount of tokens for the '%s' pair is 0, this needs to be investigated!",
            pair_name,
        )
        logger.warning("%s", utxo)
        return

    # Otherwise, determine price and add to context.
    token1_real_amount = volume_from_tokens(
        context_token_1_volume, context_token_1_decimals
    )
    token2_real_amount = volume_from_tokens(
        context_token_2_volume, context_token_2_decimals
    )

    # Assign price.
    price = token2_real_amount / token1_real_amount
    temp_context.price = price

    utxo_update_context = utxo_objects.UTxOUpdateContext(
        block_height=temp_context.block_height,
        epoch=temp_context.epoch,
        address=temp_context.address,
        tx_hash=temp_context.tx_hash,
        output_index=temp_context.output_index,
        caller=temp_context.caller,
        token_1_amount=temp_context.token_1_amount,
        token_2_amount=temp_context.token_2_amount,
        price=temp_context.price,
    )

    utxos_dict_update(
        utxo_update_context=utxo_update_context,
        utxos_dict=utxos_dict,
        tokens_pair=tokens_pair,
    )
