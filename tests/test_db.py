"""Test database functions."""

# pylint: disable=W0212

import sqlite3

import pytest

from src.cnt_collector_node import database_initialization


def test_db_init():
    """Test DB initialization.

    Characterization tests for DB creation.

    The tests aren't extensive but just provide a sample to ensure
    init functions don't error and tables are created. Some spot
    checks are done to test column creation.
    """
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    # No tables should exist here.
    with pytest.raises(sqlite3.OperationalError):
        cursor.execute("SELECT * from price;")
        cursor.execute("SELECT * from utxos;")
        cursor.execute("SELECT * from status;")
    # Create a UTxO table to later check it is dropped and created.
    cursor.execute(
        "CREATE TABLE utxos (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL);"
    )
    # Make sure the UTxO table exists but hasn't been fully initialized
    # in aid of these tests.
    cursor.execute("SELECT id from utxos;")
    with pytest.raises(sqlite3.OperationalError):
        cursor.execute("SELECT pair from utxos;")
    # Create the database.
    database_initialization._create_database(conn=conn)
    # Ensure our three primary tables are created.
    cursor.execute("SELECT * from price;")
    cursor.execute("SELECT * from utxos;")
    cursor.execute("SELECT * from status;")
    # Ensure that at least one column is created correctly after UTxO
    # table is dropped during init.
    cursor.execute("SELECT pair from utxos;")
    conn.close()


def test_db_indexes():
    """Ensure that indexes are created on the database.

    Characterization tests for database index creation.
    """
    indexes = [
        "CREATE INDEX price_pair ON price(pair)",
        "CREATE INDEX price_epoch ON price(epoch)",
        "CREATE INDEX utxos_name ON utxos(pair, source)",
        "CREATE INDEX utxos_token1_policy ON utxos(token1_policy)",
        "CREATE INDEX utxos_token2_policy ON utxos(token2_policy)",
        "CREATE INDEX utxos_security_token_policy ON utxos(security_token_policy)",
        "CREATE INDEX utxos_tx_hash ON utxos(tx_hash)",
        "CREATE INDEX utxos_date_time ON utxos(date_time)",
    ]
    conn = sqlite3.connect(":memory:")
    database_initialization._create_database(conn=conn)
    cursor = conn.cursor()
    schemata = cursor.execute("SELECT sql FROM sqlite_master;")
    created_indexes = []
    for schema in schemata.fetchall():
        ins = schema[0]
        if "CREATE INDEX" not in ins.upper():
            continue
        created_indexes.append(ins.upper())
    assert len(created_indexes) == len(indexes)
    for idx in indexes:
        assert idx.upper() in created_indexes
    conn.close()
