"""Tests for pupulate_utxos.

Primarily tests a subset of these functions with a number of smaller
functions covered in other unit tests.

Where required populate utxos has been split up into some smaller
functions with fewer responsibilities. The bigger function is not
tested wholesale.

There is a point of diminishing returns with these tests. populate
utxos from onchain for example will fail early enough in code if
it isn't working and just merges two dictionaries.

_populate_utxos_collect_runner remains too complicated to test as of
yet.
"""

import datetime
import sqlite3
from datetime import timezone

import pytest
import time_machine

from src.cnt_collector_node import global_helpers as helpers
from src.cnt_collector_node import utxo_objects
from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.helper_functions import (
    _populate_utxos_from_on_chain,
    _populate_utxos_make_context,
)

make_context_tests = [
    (
        {
            "jsonrpc": "2.0",
            "method": "queryLedgerState/epoch",
            "result": 591,
            "id": None,
        },
        {
            "db_name": "NOT USED",
            "ogmios_url": "OGMIOS_URL",
            "ogmios_ws": "OGMIOS_WS",
            "kupo_url": "KUPO_URL",
            "main_event": "MAINEVENT",
            "thread_event": "THREADEVENT",
            "reconnect_event": "RECONNECTEVENT",
            "address": "addr123",
            "epoch": 591,
            "last_block_slot": 9999,
        },
    ),
    (
        {
            "jsonrpc": "2.0",
            "method": "queryLedgerState/epoch",
            "result": 591,
            "id": None,
        },
        {
            "db_name": "NOT USED",
            "ogmios_url": "OGMIOS_URL",
            "ogmios_ws": "OGMIOS_WS",
            "kupo_url": "KUPO_URL",
            "main_event": "MAINEVENT",
            "thread_event": "THREADEVENT",
            "reconnect_event": "RECONNECTEVENT",
            "address": "addr123",
            "epoch": 591,
            "last_block_slot": 9999,
        },
    ),
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize("ogmios_result, expected", make_context_tests)
def test_populate_utxos_make_context(mocker, ogmios_result, expected):
    """Test parse utxo and ensure it summarizes the CNT data it
    finds.
    """

    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    conn.cursor()

    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.ogmios_epoch",
        return_value=ogmios_result,
    )
    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.ogmios_last_block_slot",
        return_value=9999,
    )

    app_context = helpers.AppContext(
        db_name=None,
        database=None,
        ogmios_url="",
        ogmios_ws=None,
        kupo_url=None,
        use_kupo=True,
        main_event=None,
        thread_event=None,
        reconnect_event=None,
    )

    res = _populate_utxos_make_context(
        app_context=app_context,
        address="addr123",
    )

    # Map the expected object into an InitialChainContext object
    # so that the test's changes are clear.
    expected_mapped = utxo_objects.InitialChainContext(
        block_height=expected["last_block_slot"],
        epoch=expected["epoch"],
        address=expected["address"],
        tx_hash=None,
        output_index=None,
        utxo_ids=[],
    )

    assert res == expected_mapped


on_chain_tests = [
    # Kupo.
    (
        True,
        [
            {
                "transaction_index": 11,
                "transaction_id": "413ee5a83cb03f4b0ae35365975600f048f11627c2fde187b7b2b28ffa16682b",
                "output_index": 0,
                "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                "value": {
                    "coins": 135774403864,
                    "assets": {
                        "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 35625466,
                        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f.4d494e53574150": 1,
                        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1.63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896": 1,
                    },
                },
                "datum_hash": "1a49474316a0ed8df67ee02f192edfb1f10bdfd0cf4ea87edc2406fcbceb157d",
                "datum_type": "hash",
                "script_hash": None,
                "created_at": {
                    "slot_no": 170272922,
                    "header_hash": "093eaeab6392ee70c117515a0437933f6f7a8763dc328c27af66bdd2c34e1eb0",
                },
                "spent_at": None,
            }
        ],
        {
            "tx_hash": "413ee5a83cb03f4b0ae35365975600f048f11627c2fde187b7b2b28ffa16682b",
            "tx_index": 0,
            "amount": 135774403864,
            "assets": {
                "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f": {
                    "534e454b": 35625466
                },
                "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                    "4d494e53574150": 1
                },
                "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                    "63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896": 1
                },
            },
        },
        [
            {
                "tx_hash": "413ee5a83cb03f4b0ae35365975600f048f11627c2fde187b7b2b28ffa16682b",
                "tx_index": 0,
                "amount": 135774403864,
                "assets": {
                    "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f": {
                        "534e454b": 35625466
                    },
                    "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                        "4d494e53574150": 1
                    },
                    "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                        "63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896": 1
                    },
                },
            }
        ],
    ),
    # Ogmios. Yes to be completed due to time and testing issues with
    (
        False,
        {},
        [],
        {},
    ),
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize("kupo, tx, contents, expected", on_chain_tests)
def test_populate_utxos_from_on_chain(mocker, kupo, tx, contents, expected):
    """Rudimentary test for populate_utxos_from_on_chain."""
    if kupo:
        mocker.patch(
            "src.cnt_collector_node.kupo_helper.get_kupo_matches",
            return_value=tx,
        )
        mocker.patch(
            "src.cnt_collector_node.kupo_helper.get_kupo_utxo_content",
            return_value=contents,
        )
    else:
        # Ogmios is not yet yested.
        return

    app_context = helpers.AppContext(
        db_name=None,
        database=None,
        ogmios_url="",
        ogmios_ws="UNUSED",
        kupo_url="UNUSED",
        use_kupo=False,
        main_event=None,
        thread_event=None,
        reconnect_event=None,
    )

    res = _populate_utxos_from_on_chain(
        app_context=app_context,
        address="",
    )
    assert res == expected
