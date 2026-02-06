"""Tests for pupulate_utxos.

We test a helper function _parse_block_transactions_single_tx as
parse_block_transaction is a little too complex to test standalone and
the amount of data required is too high.

A number of functions under parse_block_transactions are covered in
other unit tests.
"""

# pylint: disable=R0913

import datetime
import sqlite3
from datetime import timezone

import pytest
import time_machine

from src.cnt_collector_node import global_helpers as helpers
from src.cnt_collector_node import utxo_objects
from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.helper_functions import _parse_block_transactions_single_tx

from . import parse_block_data


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "tx, watched, pairs_config, output_contents, utxo_ids, tokens_pair_dict, output, tx_id, output_index",
    parse_block_data.parse_blocks_tx_tests,
)
def test_parse_block_transactions_single_tx(
    mocker,
    tx,
    watched,
    pairs_config,
    output_contents,
    utxo_ids,
    tokens_pair_dict,
    output,
    tx_id,
    output_index,
):
    """Characterization test for a small section of
    parse_block_transactions and ensure it remains consistent across
    refactor.
    """

    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    conn.cursor()
    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.get_output_content",
        return_value=output_contents,
    )
    mocker.patch(
        "src.cnt_collector_node.helper_functions.search_db_utxo", return_value=utxo_ids
    )
    save_output = mocker.patch("src.cnt_collector_node.helper_functions.save_output")

    app_context = helpers.AppContext(
        db_name="NOT_USED",
        database=None,
        ogmios_url="",
        ogmios_ws=None,
        kupo_url=None,
        use_kupo=True,
        main_event=None,
        thread_event=None,
        reconnect_event=None,
    )

    _parse_block_transactions_single_tx(
        app_context=app_context,
        transaction=tx,
        watched_addresses=watched,
        slot=9999,
        epoch=9999,
        pairs_config_dict=pairs_config,
        unsafe=False,
    )

    chain_context = {
        "block_height": 9999,
        "epoch": 9999,
        "address": output["address"],
        "tx_hash": tx_id,
        "output_index": output_index,
        "utxo_ids": utxo_ids,
    }

    chain_context = utxo_objects.InitialChainContext(
        block_height=9999,
        epoch=9999,
        address=output["address"],
        tx_hash=tx_id,
        output_index=output_index,
        utxo_ids=utxo_ids,
    )

    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    save_output.assert_called()
    save_output.assert_called_with(
        app_context=app_context,
        initial_chain_context=chain_context,
        tokens_pair=tokens_pair,
        output_contents=output_contents,
    )
