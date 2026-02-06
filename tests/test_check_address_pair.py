"""Tests for check address pair.

NB. because of the sheer amount of data these functions process it
isn't really economical to just keep throwing more data at it. As such
we test one good example for Ogmios and Kupo each and then we will
ensure consistency with lower level functions used by the code.

Other functions that should gain coverage from this test:

    * check_dex_tokens_pair,
    * parse_utxo,
    * insert_data_point.

Although their status should still be observed when refactoring.
"""

# pylint: disable=R0913

import datetime
import sqlite3
from datetime import timezone

import pytest
import time_machine

import src.cnt_collector_node.database_abstraction as dba
import src.cnt_collector_node.global_helpers as helpers
from src.cnt_collector_node import utxo_objects
from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.helper_functions import check_address_pair

from . import address_utxos_example

check_address_tests = [
    (
        {
            "pair": "ADA-iUSD",
            "source": "SundaeSwapV3",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
            "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
            "security_token_name": "000de140c7ef237f227542a0c8930d37911491c56a341fdef8437e0f21d024f8",
            "token1_policy": "",
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
            "token2_name": "69555344",
            "token2_decimals": 6,
        },
        (
            30,
            "ADA-iUSD",
            "SundaeSwapV3",
            0.6671725902748644,
            170087072,
            "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
            "",
            "lovelace",
            6,
            "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
            "69555344",
            6,
            "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
            "000de140c7ef237f227542a0c8930d37911491c56a341fdef8437e0f21d024f8",
            23813086544,
            15887438632,
            "6785087dbca3f5a1c6665e9531a2838a65d009d74bcd60e176f8844465c0e953",
            0,
        ),
        170087072,
        170087072,
        {
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_name": "69555344",
            "token2_decimals": 6,
            "block_height": 170087072,
            "source": "SundaeSwapV3",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
            "feed": "ADA-iUSD",
            "utxo": "6785087dbca3f5a1c6665e9531a2838a65d009d74bcd60e176f8844465c0e953#0",
            "token1_volume": 23813.086544,
            "token2_volume": 15887.438632,
            "price": 0.6671725902748644,
            "amounts": {
                "lovelace": 23813086544,
                "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 15887438632,
            },
        },
    ),
    (
        {
            "pair": "CBLP-ADA",
            "source": "SundaeSwapV3",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
            "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
            "security_token_name": "000de140549bd196264a186d09e00bcfd41727622e515154612f76dc6b8120b9",
            "token1_policy": "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd",
            "token1_name": "43424c50",
            "token1_decimals": 6,
            "token2_policy": "",
            "token2_name": "lovelace",
            "token2_decimals": 6,
        },
        (
            48,
            "CBLP-ADA",
            "SundaeSwapV3",
            0.15470180398201155,
            170087072,
            "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
            "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd",
            "43424c50",
            6,
            "",
            "lovelace",
            6,
            "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
            "000de140549bd196264a186d09e00bcfd41727622e515154612f76dc6b8120b9",
            289107650,
            44725475,
            "4ac5c4db431e76c7085625dcd89283bf571f35ad95a28bd1a4b0aa77e14c917d",
            0,
        ),
        170087072,
        170087072,
        {
            "token1_name": "43424c50",
            "token1_decimals": 6,
            "token2_name": "lovelace",
            "token2_decimals": 6,
            "block_height": 170087072,
            "source": "SundaeSwapV3",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
            "feed": "CBLP-ADA",
            "utxo": "4ac5c4db431e76c7085625dcd89283bf571f35ad95a28bd1a4b0aa77e14c917d#0",
            "token1_volume": 289.10765,
            "token2_volume": 44.725475,
            "price": 0.15470180398201155,
            "amounts": {
                "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd.43424c50": 289107650,
                "lovelace": 44725475,
            },
        },
    ),
    (
        {
            "pair": "IAG-ADA",
            "source": "WingRidersV2",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhq7lz3zuxz0l95ne0pwxdy0r7uvyqmx39l0nv4jyc9g59ngsj543je",
            "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
            "security_token_name": "4c",
            "token1_policy": "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
            "token1_name": "494147",
            "token1_decimals": 6,
            "token2_policy": "",
            "token2_name": "lovelace",
            "token2_decimals": 6,
        },
        (
            26,
            "IAG-ADA",
            "WingRidersV2",
            0.16724767145451247,
            170087072,
            "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhq7lz3zuxz0l95ne0pwxdy0r7uvyqmx39l0nv4jyc9g59ngsj543je",
            "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
            "494147",
            6,
            "",
            "lovelace",
            6,
            "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
            "4c",
            1814185535501,
            303418306399,
            "7979c7e61f993e26ed8544519d152191cc3fdc41557ac314e6a73ea0bfe5aaa0",
            0,
        ),
        170087072,
        170087080,  # current status block less than latest block slot.
        {
            "token1_name": "494147",
            "token1_decimals": 6,
            "token2_name": "lovelace",
            "token2_decimals": 6,
            "block_height": 170087072,
            "source": "WingRidersV2",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhq7lz3zuxz0l95ne0pwxdy0r7uvyqmx39l0nv4jyc9g59ngsj543je",
            "feed": "IAG-ADA",
            "utxo": "7979c7e61f993e26ed8544519d152191cc3fdc41557ac314e6a73ea0bfe5aaa0#0",
            "token1_volume": 1814185.535501,
            "token2_volume": 303418.306399,
            "price": 0.16724767145451247,
            "amounts": {
                "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114.494147": 1814185535501,
                "lovelace": 303418306399,
            },
        },
    ),
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "tokens_pair_dict, ins_mock, last_block_slot, current_status_block, expected",
    check_address_tests,
)
def test_check_address_pairs(
    mocker, tokens_pair_dict, ins_mock, last_block_slot, current_status_block, expected
):
    """Test check address pair.

    Doesn't use ogmios or kupo, just the database. This function
    isn't triggered often and so we have to mock most of the data.
    This just ensures we can refactor it more safely than currently.
    """

    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )

    app_context = helpers.AppContext(
        db_name=None,
        database=db,
        ogmios_url="",
        ogmios_ws=None,
        kupo_url=None,
        use_kupo=True,
        main_event=None,
        thread_event=None,
        reconnect_event=None,
    )

    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.ogmios_epoch",
        return_value={"result": "UNUSED"},
    )

    cursor.execute(
        "INSERT INTO utxos(id, pair, source, price, block_height, "
        "address, token1_policy, token1_name, token1_decimals, "
        "token2_policy, token2_name, token2_decimals, "
        "security_token_policy, security_token_name, token1_amount, "
        "token2_amount, tx_hash, output_index)"
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ins_mock,
    )

    mocker.patch(
        "src.cnt_collector_node.helper_functions.get_status_block",
        return_value=current_status_block,
    )

    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    fs_info = check_address_pair(
        app_context=app_context,
        tokens_pair=tokens_pair,
        last_block_slot=last_block_slot,
    )

    assert fs_info == expected

    # Make sure no database update is performed.
    cursor.execute("select * from price;")
    price_table_before_update = cursor.fetchall()
    assert len(price_table_before_update) == 0


check_address_tests_kupo = [
    (
        {
            "pair": "ADA-USDM",
            "source": "Spectrum",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
            "security_token_policy": "fd0b614f52f2286df3b4db4fc70656bcd3df4877e909fd5a44e956f0",
            "security_token_name": "0014efbfbd105553444d5f4144415f4e4654",
            "token1_policy": "",
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_policy": "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad",
            "token2_name": "0014df105553444d",
            "token2_decimals": 6,
        },
        170079368,
        address_utxos_example.address_utxos_example_kupo,
        address_utxos_example.utxos_content_kupo,
        {
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_name": "0014df105553444d",
            "token2_decimals": 6,
            "block_height": 170079368,
            "source": "Spectrum",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
            "feed": "ADA-USDM",
            "utxo": "00f89b6ab12ec840f0883564e24a11d221a49bc7fe2f4b86e78ba03cd8395693#1",
            "token1_volume": 15.180126,
            "token2_volume": 10.127849,
            "price": 0.6671781907475604,
            "amounts": {
                "lovelace": 15180126,
                "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 10127849,
            },
        },
        [
            (
                1,
                "ADA-USDM",
                "Spectrum",
                0.6671781907475604,
                15180126,
                10127849,
                "UNUSED",
                170079368,
                "2018-02-19T12:55:00Z",
            )
        ],
    ),
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "tokens_pair_dict, last_block_slot, utxo_result, utxos_content, expected, db_expected",
    check_address_tests_kupo,
)
def test_check_address_pair_kupo(
    mocker,
    tokens_pair_dict,
    last_block_slot,
    utxo_result,
    utxos_content,
    expected,
    db_expected,
):
    """Test parse utxo and ensure it summarizes the CNT data it
    finds.
    """

    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )

    app_context = helpers.AppContext(
        db_name=None,
        database=db,
        ogmios_url="",
        ogmios_ws="UNUSED",
        kupo_url="UNUSED",
        use_kupo=True,
        main_event=None,
        thread_event=None,
        reconnect_event=None,
    )

    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.ogmios_epoch",
        return_value={"result": "UNUSED"},
    )
    mocker.patch(
        "src.cnt_collector_node.kupo_helper.get_kupo_matches",
        return_value=utxo_result,
    )
    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.get_ogmios_utxo_content",
        return_value={},
    )
    mocker.patch(
        "src.cnt_collector_node.helper_functions._get_utxos_content",
        return_value=utxos_content,
    )

    # At this point there is no information in the database.
    cursor.execute("select * from price;")
    price_table_before_update = cursor.fetchall()
    assert len(price_table_before_update) == 0

    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    feed_info = check_address_pair(
        app_context=app_context,
        tokens_pair=tokens_pair,
        last_block_slot=last_block_slot,
    )
    assert feed_info == expected

    # Here we expect the database to be correctly updated.
    cursor.execute("select * from price;")
    price_table_after_update = cursor.fetchall()
    assert len(price_table_after_update) == 1
    assert price_table_after_update == db_expected


check_address_tests_ogmios = [
    (
        {
            "pair": "ADA-DJED",
            "source": "MinSwap",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
            "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
            "security_token_name": "d944eda9d4fd8c26171a4362539bfd4ccf35f5a4d0cc7525b22327b997a4f4b9",
            "token1_policy": "",
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_policy": "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61",
            "token2_name": "446a65644d6963726f555344",
            "token2_decimals": 6,
        },
        170017979,
        address_utxos_example.address_utxos_example_ogmios,
        address_utxos_example.utxos_content_ogmios,
        {
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_name": "446a65644d6963726f555344",
            "token2_decimals": 6,
            "block_height": 170017979,
            "source": "MinSwap",
            "collector": "cnt-collector-node/2.3.0",
            "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
            "feed": "ADA-DJED",
            "utxo": "d55855aae8341e5e2da089f885cc91057ece648832a71b427111033ee5cf0fe9#0",
            "token1_volume": 17286.946198,
            "token2_volume": 11610.806014,
            "price": 0.6716516544341014,
            "amounts": {
                "lovelace": 17286946198,
                "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61.446a65644d6963726f555344": 11610806014,
            },
        },
        [
            (
                1,
                "ADA-DJED",
                "MinSwap",
                0.6716516544341014,
                17286946198,
                11610806014,
                "UNUSED",
                170017979,
                "2018-02-19T12:55:00Z",
            )
        ],
    )
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "tokens_pair_dict, last_block_slot, utxo_result, utxos_content, expected, db_expected",
    check_address_tests_ogmios,
)
def test_check_address_pair_ogmios(
    mocker,
    tokens_pair_dict,
    last_block_slot,
    utxo_result,
    utxos_content,
    expected,
    db_expected,
):
    """Test parse utxo and ensure it summarizes the CNT data it
    finds.
    """

    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )

    app_context = helpers.AppContext(
        db_name=None,
        database=db,
        ogmios_url="",
        ogmios_ws="UNUSED",
        kupo_url="UNUSED",
        use_kupo=False,
        main_event=None,
        thread_event=None,
        reconnect_event=None,
    )

    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.ogmios_epoch",
        return_value={"result": "UNUSED"},
    )
    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.ogmios_addresses_utxos",
        return_value=utxo_result,
    )
    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.get_ogmios_utxo_content",
        return_value={},
    )
    mocker.patch(
        "src.cnt_collector_node.helper_functions._get_utxos_content",
        return_value=utxos_content,
    )

    # At this point there is no information in the database.
    cursor.execute("select * from price;")
    price_table_before_update = cursor.fetchall()
    assert len(price_table_before_update) == 0

    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    feed_info = check_address_pair(
        app_context=app_context,
        tokens_pair=tokens_pair,
        last_block_slot=last_block_slot,
    )
    assert feed_info == expected

    # Here we expect the database to be correctly updated.
    cursor.execute("select * from price;")
    price_table_after_update = cursor.fetchall()
    assert len(price_table_after_update) == 1
    assert price_table_after_update == db_expected
