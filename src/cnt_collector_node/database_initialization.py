"""Database initialization"""

# pylint: disable=R0914

import logging
import sqlite3

logger = logging.getLogger(__name__)


def create_database(db_name: str) -> None:
    """Create the sqlite3 database and tables if they don't exist"""
    conn = sqlite3.connect(db_name)
    _create_database(conn)
    conn.close()


def _create_database(conn: sqlite3.Connection) -> None:
    """Create the underlying database structure given a database
    connection."""

    drop_utxos = "DROP TABLE IF EXISTS utxos"

    create_price_table = """CREATE TABLE IF NOT EXISTS price (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        pair TEXT NOT NULL,
        source TEXT NOT NULL,
        price FLOAT NOT NULL,
        token1_amount INTEGER NOT NULL,
        token2_amount INTEGER NOT NULL,
        epoch INTEGER NOT NULL,
        block_height INTEGER NOT NULL,
        date_time timestamp
    )
    """

    create_status_table = """CREATE TABLE IF NOT EXISTS status (
        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        current_block_slot INTEGER NOT NULL,
        date_time timestamp
    )
    """

    create_utxos_table = """CREATE TABLE IF NOT EXISTS utxos (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                pair TEXT NOT NULL,
                source TEXT NOT NULL,
                price FLOAT NOT NULL,
                block_height INTEGER NOT NULL,
                address TEXT NOT NULL,
                token1_policy TEXT NOT NULL,
                token1_name TEXT NOT NULL,
                token1_decimals INTEGER NOT NULL,
                token2_policy TEXT NOT NULL,
                token2_name TEXT NOT NULL,
                token2_decimals INTEGER NOT NULL,
                security_token_policy TEXT NOT NULL,
                security_token_name TEXT NOT NULL,
                token1_amount INTEGER NOT NULL,
                token2_amount INTEGER NOT NULL,
                tx_hash TEXT NOT NULL,
                output_index INTEGER NOT NULL,
                date_time timestamp
                )"""

    index_price_pair = "CREATE INDEX IF NOT EXISTS price_pair ON price(pair)"
    index_price_epoch = "CREATE INDEX IF NOT EXISTS price_epoch ON price(epoch)"

    index_utxos_name = "CREATE INDEX IF NOT EXISTS utxos_name ON utxos(pair, source)"
    index_utxos_token1_policy = (
        "CREATE INDEX IF NOT EXISTS utxos_token1_policy ON utxos(token1_policy)"
    )
    index_utxos_token2_policy = (
        "CREATE INDEX IF NOT EXISTS utxos_token2_policy ON utxos(token2_policy)"
    )
    index_utxos_security_policy = "CREATE INDEX IF NOT EXISTS utxos_security_token_policy ON utxos(security_token_policy)"
    index_utxos_tx_hash = "CREATE INDEX IF NOT EXISTS utxos_tx_hash ON utxos(tx_hash)"
    index_utxos_data_time = (
        "CREATE INDEX IF NOT EXISTS utxos_date_time ON utxos(date_time)"
    )

    schema = [
        drop_utxos,
        create_price_table,
        create_status_table,
        create_utxos_table,
        index_price_pair,
        index_price_epoch,
        index_utxos_name,
        index_utxos_token1_policy,
        index_utxos_token2_policy,
        index_utxos_security_policy,
        index_utxos_tx_hash,
        index_utxos_data_time,
    ]

    cur = conn.cursor()
    for item in schema:
        cur.execute(item.strip().replace("  ", " ").replace("\n", " "))

    logger.info("database initialization complete")
