"""Enable testing of database abstraction.

One function of these tests is to provide some tests of original
functionality via mocking that we can then confirm continue to work
after refactoring.
"""

# pylint: disable=R0913

import sqlite3

import pytest

from src.cnt_collector_node.database_abstraction import (
    DBObject,
    UTxORecordResults,
    complete_utxo_obj,
    insert_utxo_complete,
    select_utxo_record_by_source_address_and_policy,
    utxo_source_policy_query_obj,
)
from src.cnt_collector_node.database_initialization import _create_database


def insert_utxo_complete_(
    db: DBObject,
    pair: str,
    source: str,
    address: str,
    token_1_amount: int,
    decimals_1: int,
    token_2_amount: int,
    decimals_2: int,
    security_token_policy: str,
    security_token_name: str,
    tx_hash: str,
):
    """Create some data to test against."""

    utxo_obj = complete_utxo_obj(
        pair=pair,
        source=source,
        price=3.142,
        block_height=10,
        address=address,
        token_1_policy="policy1",
        token_1_name="token1name",
        token_1_decimals=decimals_1,
        token_2_policy="policy2",
        token_2_name="token2name",
        token_2_decimals=decimals_2,
        security_token_policy=security_token_policy,
        security_token_name=security_token_name,
        token_1_amount=token_1_amount,
        token_2_amount=token_2_amount,
        tx_hash=tx_hash,
        tx_index=3,
    )

    insert_utxo_complete(db, utxo_obj)


def orig_utxo_by_source_query(db: DBObject, tokens_pair: dict, context: dict):
    """Copy of the original function from the original indexer code."""
    db.cursor.execute(
        "SELECT id, block_height, token1_amount, token1_decimals, "
        "token2_amount, token2_decimals, tx_hash, output_index "
        "FROM utxos "
        "WHERE pair = ? AND source = ? AND address = ? "
        "AND security_token_policy = ? AND security_token_name = ?",
        (
            tokens_pair["pair"],
            tokens_pair["source"],
            context["address"],
            tokens_pair["security_token_policy"],
            tokens_pair["security_token_name"],
        ),
    )


utxo_by_source_and_policy_tests = [
    (
        # Tokens pairs.
        {
            "pair": "BASE-QUOTE",
            "source": "SUPERDEX",
            "security_token_policy": "policyABC",
            "security_token_name": "nameABC",
        },
        # Context.
        {
            "address": "addr123",
        },
        # DB inserts.
        {
            "pair": "BASE-QUOTE",
            "source": "SUPERDEX",
            "address": "addr123",
            "token_1_amount": 200,
            "decimals_1": 8,
            "token_2_amount": 500,
            "decimals_2": 6,
            "security_token_policy": "policyABC",
            "security_token_name": "nameABC",
            "tx_hash": "123",
        },
        # Expected result.
        (
            # id,
            1,
            # block-height,
            10,
            # token_1_amount,
            200,
            # token_1_decimals,
            8,
            # token_2_amount,
            500,
            # token_2_decimals,
            6,
            # tx_hash
            "123",
            # output_index.
            3,
        ),
    ),
    (
        # Tokens pairs.
        {
            "pair": "ADA-USD",
            "source": "SUPERDEX2",
            "security_token_policy": "policy123",
            "security_token_name": "name123",
        },
        # Context.
        {
            "address": "addr123",
        },
        # DB inserts.
        {
            "pair": "ADA-USD",
            "source": "SUPERDEX2",
            "address": "addr123",
            "token_1_amount": 2000,
            "decimals_1": 2,
            "token_2_amount": 5,
            "decimals_2": 10,
            "security_token_policy": "policy123",
            "security_token_name": "name123",
            "tx_hash": "abc",
        },
        # Expected result.
        (
            # id,
            1,
            # block-height,
            10,
            # token_1_amount,
            2000,
            # token_1_decimals,
            2,
            # token_2_amount,
            5,
            # token_2_decimals,
            10,
            # tx_hash
            "abc",
            # output_index.
            3,
        ),
    ),
]


@pytest.mark.parametrize(
    "tokens_pair, context, db_insert, expected", utxo_by_source_and_policy_tests
)
def test_select_utxo_record_by_source_address_and_policy(
    tokens_pair: dict, context: dict, db_insert: dict, expected: tuple
):
    """Provide a characterization/continuity test for this function
    which is otherwise quite hard to test in the current form.
    """

    # Create database and prepare the cursor and connection.
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = DBObject(
        connection=conn,
        cursor=cursor,
    )

    # Attempt to get a result from the database using the original
    # query. There should be no values.
    orig_utxo_by_source_query(db=db, tokens_pair=tokens_pair, context=context)
    res = db.cursor.fetchone()
    assert res is None

    # Add data to the database.
    insert_utxo_complete_(
        db=db,
        pair=db_insert["pair"],
        source=db_insert["source"],
        address=db_insert["address"],
        token_1_amount=db_insert["token_1_amount"],
        decimals_1=db_insert["decimals_1"],
        token_2_amount=db_insert["token_2_amount"],
        decimals_2=db_insert["decimals_2"],
        security_token_policy=db_insert["security_token_policy"],
        security_token_name=db_insert["security_token_name"],
        tx_hash=db_insert["tx_hash"],
    )

    # Attempt to get a result from the database using the original
    # query.
    orig_utxo_by_source_query(db=db, tokens_pair=tokens_pair, context=context)
    res1 = db.cursor.fetchone()
    assert res1 == expected

    # Create a query using our new functionality and ensure the
    # result matched.
    query_obj = utxo_source_policy_query_obj(
        pair=tokens_pair["pair"],
        source=tokens_pair["source"],
        address=context["address"],
        security_token_policy=tokens_pair["security_token_policy"],
        security_token_name=tokens_pair["security_token_name"],
    )
    res2 = select_utxo_record_by_source_address_and_policy(
        db=db,
        query_obj=query_obj,
    )

    # Refactor: translate expected results to reduce code changes.
    # Remove once complete.
    res1_obj = UTxORecordResults(
        row_id=res1[0],
        block_height=res1[1],
        token_1_amount=res1[2],
        token_1_decimals=res1[3],
        token_2_amount=res1[4],
        token_2_decimals=res1[5],
        tx_hash=res1[6],
        output_index=res1[7],
    )

    # Refactor: translate expected results to reduce code changes.
    # Remove once complete.
    expected_obj = UTxORecordResults(
        row_id=expected[0],
        block_height=expected[1],
        token_1_amount=expected[2],
        token_1_decimals=expected[3],
        token_2_amount=expected[4],
        token_2_decimals=expected[5],
        tx_hash=expected[6],
        output_index=expected[7],
    )

    assert res1_obj == res2
    assert res2 == expected_obj

    # Prevent false positives by making sure we get no results for
    # bad data.
    query_obj = utxo_source_policy_query_obj(
        pair="NOT-A-REAL-VALUE",
        source=tokens_pair["source"],
        address=context["address"],
        security_token_policy=tokens_pair["security_token_policy"],
        security_token_name=tokens_pair["security_token_name"],
    )
    res3 = select_utxo_record_by_source_address_and_policy(
        db=db,
        query_obj=query_obj,
    )
    assert res3 is None
