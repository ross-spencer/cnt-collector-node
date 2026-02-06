"""Abstraction of the database functions of the collector."""

# pylint: disable=R0913,R0902,R0914

import sqlite3
from dataclasses import dataclass
from typing import Optional

try:
    import global_helpers as helpers
    import utxo_objects
except ModuleNotFoundError:
    try:
        from src.cnt_collector_node import global_helpers as helpers
        from src.cnt_collector_node import utxo_objects
    except ModuleNotFoundError:
        from cnt_collector_node import global_helpers as helpers
        from cnt_collector_node import utxo_objects


@dataclass
class DBObject:
    """Context object for the database."""

    connection: sqlite3.Connection
    cursor: sqlite3.Cursor


def get_status(db: DBObject):
    """Retrieve status information from the database."""
    db.cursor.execute("SELECT current_block_slot FROM status")
    try:
        return db.cursor.fetchone()[0]
    except TypeError:
        return None


def insert_status(db: DBObject, block: int):
    """Insert status information into the database."""
    db.cursor.execute(
        "INSERT INTO status(current_block_slot, date_time) VALUES(?, ?)",
        (block, helpers.get_utc_timestamp_now()),
    )


def update_status(db: DBObject, block: int):
    """Update status information in the database."""
    db.cursor.execute(
        "UPDATE status SET current_block_slot = ?, date_time = ?",
        (block, helpers.get_utc_timestamp_now()),
    )


@dataclass
class UTxOIDQueryParams:
    """Query params to return a single UTxO ID from the database."""

    tx_id: str
    tx_index: int
    policy_id: str
    token_name: str


@dataclass
class UTxOID:
    """UTxO ID object."""

    utxo_id: int


def utxo_id_query_obj(
    tx_id: str, tx_index: int, policy_id: str, token_name: str
) -> UTxOID:
    """Create and potentially validate a query object used to
    return a UTxO ID from the database.
    """
    return UTxOIDQueryParams(
        tx_id=tx_id,
        tx_index=tx_index,
        policy_id=policy_id,
        token_name=token_name,
    )


def select_utxo_id(
    cursor: sqlite3.Cursor, query_params: UTxOIDQueryParams
) -> Optional[UTxOID]:
    """Select and return a UTxO ID from the database."""
    cursor.execute(
        "SELECT id "
        "FROM utxos "
        "WHERE tx_hash = ? AND output_index = ? "
        "AND (token1_policy = ? OR token2_policy = ?) "
        "AND (token1_name = ? OR token2_name = ?)",
        (
            query_params.tx_id,
            query_params.tx_index,
            query_params.policy_id,
            query_params.policy_id,
            query_params.token_name,
            query_params.token_name,
        ),
    )
    row = cursor.fetchone()
    if not row:
        return None
    return UTxOID(utxo_id=row[0])


@dataclass
class PriceRecord:
    """Price record object."""

    pair: str
    epoch: int
    block_height: int
    price: float
    token_1_amount: int
    token_2_amount: int
    source: str


def price_record_obj_from_dicts(
    tokens_pair: utxo_objects.TokensPair,
    utxo_update_context: utxo_objects.UTxOUpdateContext,
):
    """Wrap the return of a price object so that is can
    accept an argument based on the CNT-indexers dictionaries.

    This helps reduce the number of lines in our helper functions
    library and may help further refactoring of these functions down
    the line.
    """
    return price_record_obj(
        pair=tokens_pair.pair,
        epoch=utxo_update_context.epoch,
        block_height=utxo_update_context.block_height,
        price=utxo_update_context.price,
        token_1_amount=utxo_update_context.token_1_amount,
        token_2_amount=utxo_update_context.token_2_amount,
        source=tokens_pair.source,
    )


def price_record_obj(
    pair: str,
    epoch: int,
    block_height: int,
    price: float,
    token_1_amount: int,
    token_2_amount: int,
    source: str,
):
    """Create a price record object from the given parameters."""
    return PriceRecord(
        pair=pair,
        epoch=epoch,
        block_height=block_height,
        price=price,
        token_1_amount=token_1_amount,
        token_2_amount=token_2_amount,
        source=source,
    )


def insert_price_record(db: DBObject, price_record: PriceRecord):
    """Insert a new price record into the database."""
    db.cursor.execute(
        "INSERT INTO price(pair, epoch, block_height, price, "
        "token1_amount, token2_amount, source, date_time) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            price_record.pair,
            price_record.epoch,
            price_record.block_height,
            price_record.price,
            price_record.token_1_amount,
            price_record.token_2_amount,
            price_record.source,
            helpers.get_utc_timestamp_now(),
        ),
    )
    db.connection.commit()


@dataclass
class UTxOSourcePolicyQueryParams:
    """Query parameters to retrieve UTxO by source and security
    policy.
    """

    pair: str
    source: str
    address: str
    security_token_policy: str
    security_token_name: str


@dataclass
class UTxOSourcePolicyResults:
    """Results object for when we query with source and policy."""

    tx_hash: str  # [0]
    output_index: int  # [1]
    token_1_volume: int  # [2]
    token_1_decimals: int  # [3]
    token_2_volume: int  # [4]
    token_2_decimals: int  # [5]
    token_1_policy: str  # [6]
    token_1_name: str  # [7]
    token_2_policy: str  # [8]
    token_2_name: str  # [9]


def utxo_source_policy_query_obj(
    pair: str,
    source: str,
    address: str,
    security_token_policy: str,
    security_token_name: str,
):
    """Return a query object to retrieve UTxO by source and security
    policy.
    """
    return UTxOSourcePolicyQueryParams(
        pair=pair,
        source=source,
        address=address,
        security_token_policy=security_token_policy,
        security_token_name=security_token_name,
    )


def select_utxo_record_by_pair_source_and_policy(
    db: DBObject, query_obj: UTxOSourcePolicyQueryParams
):
    """Select a UTxO record by its pair, source and security policy.

    NB. doesn't use address, but can use the query_object we created to
    make this effort easier above.
    """
    db.cursor.execute(
        "SELECT tx_hash, output_index, "
        "token1_amount, token1_decimals, "
        "token2_amount, token2_decimals, "
        "token1_policy, token1_name, "
        "token2_policy, token2_name "
        "FROM utxos "
        "WHERE pair = ? AND source = ? AND security_token_policy = ? "
        "AND security_token_name = ? ORDER BY block_height DESC LIMIT 1",
        (
            query_obj.pair,
            query_obj.source,
            query_obj.security_token_policy,
            query_obj.security_token_name,
        ),
    )
    row = db.cursor.fetchone()
    try:
        res = UTxOSourcePolicyResults(
            tx_hash=row[0],
            output_index=row[1],
            token_1_volume=row[2],
            token_1_decimals=row[3],
            token_2_volume=row[4],
            token_2_decimals=row[5],
            token_1_policy=row[6],
            token_1_name=row[7],
            token_2_policy=row[8],
            token_2_name=row[9],
        )
    except TypeError:
        return None
    return res


@dataclass
class UTxORecordResults:
    """Results object for UTxO results retrieve from the database."""

    row_id: int  # [0]
    block_height: int  # [1]
    token_1_amount: int  # [2]
    token_1_decimals: int  # [3]
    token_2_amount: int  # [4]
    token_2_decimals: int  # [5]
    tx_hash: str  # [6]
    output_index: int  # [7]


def select_utxo_record_by_source_address_and_policy(
    db: DBObject, query_obj: UTxOSourcePolicyQueryParams
):
    """Select a UTxO record by its source and security policy."""
    db.cursor.execute(
        "SELECT id, block_height, token1_amount, token1_decimals, "
        "token2_amount, token2_decimals, tx_hash, output_index "
        "FROM utxos "
        "WHERE pair = ? AND source = ? AND address = ? "
        "AND security_token_policy = ? AND security_token_name = ?",
        (
            query_obj.pair,
            query_obj.source,
            query_obj.address,
            query_obj.security_token_policy,
            query_obj.security_token_name,
        ),
    )
    row = db.cursor.fetchone()
    try:
        res = UTxORecordResults(
            row_id=row[0],
            block_height=row[1],
            token_1_amount=row[2],
            token_1_decimals=row[3],
            token_2_amount=row[4],
            token_2_decimals=row[5],
            tx_hash=row[6],
            output_index=row[7],
        )
    except TypeError:
        return None
    return res


def select_utxo_by_id(db: DBObject, id_: int):
    """Given a UTxO ID attempt to find a match in the database."""
    db.cursor.execute(
        "SELECT id, block_height, token1_amount, token1_decimals, "
        "token2_amount, token2_decimals, tx_hash, output_index "
        "FROM utxos "
        "WHERE id = ?",
        (id_,),
    )
    row = db.cursor.fetchone()
    try:
        res = UTxORecordResults(
            row_id=row[0],
            block_height=row[1],
            token_1_amount=row[2],
            token_1_decimals=row[3],
            token_2_amount=row[4],
            token_2_decimals=row[5],
            tx_hash=row[6],
            output_index=row[7],
        )
    except TypeError:
        return None
    return res


@dataclass
class CompleteUTxO:
    """Complete UTxO object."""

    pair: str
    source: str
    price: float
    block_height: int
    address: str
    token_1_policy: str
    token_1_name: str
    token_1_decimals: int
    token_2_policy: str
    token_2_name: str
    token_2_decimals: int
    security_token_policy: str
    security_token_name: str
    token_1_amount: int
    token_2_amount: int
    tx_hash: str
    tx_index: int


def complete_utxo_obj_from_dicts(
    tokens_pair: utxo_objects.TokensPair,
    utxo_update_context: utxo_objects.UTxOUpdateContext,
):
    """Wrap the return of a complete utxo object so that is can
    accept an argument based on the CNT-indexers dictionaries.

    This helps reduce the number of lines in our helper functions
    library and may help further refactoring of these functions down
    the line.
    """
    return complete_utxo_obj(
        pair=tokens_pair.pair,
        source=tokens_pair.source,
        price=utxo_update_context.price,
        block_height=utxo_update_context.block_height,
        address=utxo_update_context.address,
        token_1_policy=tokens_pair.token_1_policy,
        token_1_name=tokens_pair.token_1_name,
        token_1_decimals=tokens_pair.token_1_decimals,
        token_2_policy=tokens_pair.token_2_policy,
        token_2_name=tokens_pair.token_2_name,
        token_2_decimals=tokens_pair.token_2_decimals,
        security_token_policy=tokens_pair.security_token_policy,
        security_token_name=tokens_pair.security_token_name,
        token_1_amount=utxo_update_context.token_1_amount,
        token_2_amount=utxo_update_context.token_2_amount,
        tx_hash=utxo_update_context.tx_hash,
        tx_index=utxo_update_context.output_index,
    )


def complete_utxo_obj(
    pair: str,
    source: str,
    price: float,
    block_height: int,
    address: str,
    token_1_policy: str,
    token_1_name: str,
    token_1_decimals: int,
    token_2_policy: str,
    token_2_name: str,
    token_2_decimals: int,
    security_token_policy: str,
    security_token_name: str,
    token_1_amount: int,
    token_2_amount: int,
    tx_hash: str,
    tx_index: int,
):
    """Return a complete UTxO object."""
    return CompleteUTxO(
        pair=pair,
        source=source,
        price=price,
        block_height=block_height,
        address=address,
        token_1_policy=token_1_policy,
        token_1_name=token_1_name,
        token_1_decimals=token_1_decimals,
        token_2_policy=token_2_policy,
        token_2_name=token_2_name,
        token_2_decimals=token_2_decimals,
        security_token_policy=security_token_policy,
        security_token_name=security_token_name,
        token_1_amount=token_1_amount,
        token_2_amount=token_2_amount,
        tx_hash=tx_hash,
        tx_index=tx_index,
    )


def insert_utxo_complete(db: DBObject, utxo_record: CompleteUTxO):
    """Insert an entirely new record for a UTxO in the database."""
    db.cursor.execute(
        "INSERT INTO utxos(pair, source, price, block_height, address, "
        "token1_policy, token1_name, token1_decimals, "
        "token2_policy, token2_name, token2_decimals, "
        "security_token_policy, security_token_name, "
        "token1_amount, token2_amount, tx_hash, output_index, date_time) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            utxo_record.pair,
            utxo_record.source,
            utxo_record.price,
            utxo_record.block_height,
            utxo_record.address,
            utxo_record.token_1_policy,
            utxo_record.token_1_name,
            utxo_record.token_1_decimals,
            utxo_record.token_2_policy,
            utxo_record.token_2_name,
            utxo_record.token_2_decimals,
            utxo_record.security_token_policy,
            utxo_record.security_token_name,
            utxo_record.token_1_amount,
            utxo_record.token_2_amount,
            utxo_record.tx_hash,
            utxo_record.tx_index,
            helpers.get_utc_timestamp_now(),
        ),
    )


@dataclass
class PartialUTxO:
    block_height: int
    price: float
    token_1_amount: int
    token_2_amount: int
    tx_hash: str
    tx_index: int


def partial_utxo_obj(
    block_height: int,
    price: float,
    token_1_amount: int,
    token_2_amount: int,
    tx_hash: str,
    tx_index: int,
) -> PartialUTxO:
    """Return a UTxO object."""
    return PartialUTxO(
        block_height=block_height,
        price=price,
        token_1_amount=token_1_amount,
        token_2_amount=token_2_amount,
        tx_hash=tx_hash,
        tx_index=tx_index,
    )


def update_utxo_partial(db: DBObject, utxo_record: PartialUTxO, row_id: int):
    """Update partial UTxO records in the database."""
    db.cursor.execute(
        "UPDATE utxos SET block_height = ?, price = ?, "
        "token1_amount = ?, token2_amount = ?, "
        "tx_hash = ?, output_index = ?, date_time = ? "
        "WHERE id = ?",
        (
            utxo_record.block_height,
            utxo_record.price,
            utxo_record.token_1_amount,
            utxo_record.token_2_amount,
            utxo_record.tx_hash,
            utxo_record.tx_index,
            helpers.get_utc_timestamp_now(),
            row_id,
        ),
    )


def select_utxo_count_by_tx_info(db: DBObject, tx_hash: str, output_index: int):
    """Select a count of UTxOs from the database given a transaction
    hash and output index.
    """
    db.cursor.execute(
        "SELECT count(*) FROM utxos WHERE tx_hash = ? AND output_index = ?",
        (tx_hash, output_index),
    )
    return db.cursor.fetchone()[0]
