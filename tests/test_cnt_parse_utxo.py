"""Placeholder tests."""

# pylint: disable = E0401, R0801

import datetime
import sqlite3
from datetime import timezone

import pytest
import time_machine

import src.cnt_collector_node.database_abstraction as dba
from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.global_helpers import display_block
from src.cnt_collector_node.helper_functions import parse_utxo

from . import block_example

block_tests = [
    # Basic test with all keys.
    (
        {
            "id": 1,
            "ancestor": 2,
            "height": 3,
            "slot": 4,
            "transactions": [],
        },
        4,
    ),
    # No block keys available.
    ({}, 0),
    # Real-life example.
    (block_example.example_one, 167589340),
]


@pytest.mark.parametrize("input_block, result", block_tests)
def test_display_block(input_block: dict, result: int):
    """A ruimentary test to ensure block functions don't error."""
    assert display_block(input_block) == result


djed_minswap = (
    {"epoch": 585, "block_height": 167676238},
    {
        "tx_hash": "9af1534482e82fa941a5e9b8c9df0e780ee92d6f6ddccc9d7283b7f801732219",
        "tx_index": 0,
        "amount": 15851844464,
        "assets": {
            "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
                "d944eda9d4fd8c26171a4362539bfd4ccf35f5a4d0cc7525b22327b997a4f4b9": 16902957
            },
            "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61": {
                "446a65644d6963726f555344": 12563856040
            },
            "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                "4d494e53574150": 1
            },
            "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                "d944eda9d4fd8c26171a4362539bfd4ccf35f5a4d0cc7525b22327b997a4f4b9": 1
            },
        },
    },
    {
        "feed": "ADA-DJED",
        "source": "MinSwap",
        "token1_policy": "",
        "token1_name": "lovelace",
        "token1_decimals": 6,
        "token2_policy": "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61",
        "token2_name": "446a65644d6963726f555344",
        "token2_decimals": 6,
        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
        "security_token_name": "d944eda9d4fd8c26171a4362539bfd4ccf35f5a4d0cc7525b22327b997a4f4b9",
    },
    # Expected results.
    {
        "utxo": "9af1534482e82fa941a5e9b8c9df0e780ee92d6f6ddccc9d7283b7f801732219#0",
        "token1_volume": 15851.844464,
        "token2_volume": 12563.85604,
        "price": 0.7925800728447017,
        "amounts": {
            "lovelace": 15851844464,
            "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61.446a65644d6963726f555344": 12563856040,
        },
    },
    [
        (
            1,
            "ADA-DJED",
            "MinSwap",
            0.7925800728447017,
            15851844464,
            12563856040,
            585,
            167676238,
            "2018-02-19T12:55:00Z",
        )
    ],
)

fact_sundae = (
    {"epoch": 585, "block_height": 167678074},
    {
        "tx_hash": "e36bc756f4a94b40abda2a0a85ed8a1eb1f19c2e760a0ead6d4bfc51878945b4",
        "tx_index": 0,
        "amount": 3235058618,
        "assets": {
            "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b": {
                "000de140a5b624b96af21138b6dff057e0499e7f767fcfe7ac8adb549f3818d7": 1
            },
            "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e51": {
                "6f7263666178746f6b656e": 660985870918
            },
        },
    },
    {
        "feed": "FACT-ADA",
        "source": "SundaeSwapV3",
        "token1_policy": "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e51",
        "token1_name": "6f7263666178746f6b656e",
        "token1_decimals": 6,
        "token2_policy": "",
        "token2_name": "lovelace",
        "token2_decimals": 6,
        "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
        "security_token_name": "000de140a5b624b96af21138b6dff057e0499e7f767fcfe7ac8adb549f3818d7",
    },
    # Expected results.
    {
        "utxo": "e36bc756f4a94b40abda2a0a85ed8a1eb1f19c2e760a0ead6d4bfc51878945b4#0",
        "token1_volume": 660985.870918,
        "token2_volume": 3235.058618,
        "price": 0.00489429314654947,
        "amounts": {
            "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e51.6f7263666178746f6b656e": 660985870918,
            "lovelace": 3235058618,
        },
    },
    [
        (
            1,
            "FACT-ADA",
            "SundaeSwapV3",
            0.00489429314654947,
            660985870918,
            3235058618,
            585,
            167678074,
            "2018-02-19T12:55:00Z",
        )
    ],
)

iusd_wingriders = (
    {"epoch": 585, "block_height": 167678057},
    {
        "tx_hash": "4187d9731293502ff1c0b4ef67e07893f57a08f4859c4c909e952cd1980b2e4c",
        "tx_index": 0,
        "amount": 3251272289,
        "assets": {
            "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880": {
                "69555344": 2584583824
            },
            "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570": {
                "452089abb5bf8cc59b678a2cd7b9ee952346c6c0aa1cf27df324310a70d02fc3": 9223372034407162828,
                "4c": 1,
            },
        },
    },
    {
        "feed": "ADA-iUSD",
        "source": "WingRiders",
        "token1_policy": "",
        "token1_name": "lovelace",
        "token1_decimals": 6,
        "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
        "token2_name": "69555344",
        "token2_decimals": 6,
        "security_token_policy": "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
        "security_token_name": "4c",
    },
    # Expected results.
    {
        "utxo": "4187d9731293502ff1c0b4ef67e07893f57a08f4859c4c909e952cd1980b2e4c#0",
        "token1_volume": 3251.272289,
        "token2_volume": 2584.583824,
        "price": 0.7949453611573534,
        "amounts": {
            "lovelace": 3251272289,
            "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 2584583824,
        },
    },
    [
        (
            1,
            "ADA-iUSD",
            "WingRiders",
            0.7949453611573534,
            3251272289,
            2584583824,
            585,
            167678057,
            "2018-02-19T12:55:00Z",
        )
    ],
)

iag_wingridesv2 = (
    {"epoch": 585, "block_height": 167678074},
    {
        "tx_hash": "c230d1867357bd292ac8a97440a62504cfa6bd0b4643251db061d44c43233eb4",
        "tx_index": 0,
        "amount": 288052828506,
        "assets": {
            "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737": {
                "0c5eb41c9d6525aae8a8d6f04045a6acc7e0eacae69da96987cf1d9124f22421": 9223371351054998394,
                "4c": 1,
            },
            "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114": {
                "494147": 1953183438367
            },
        },
    },
    {
        "feed": "IAG-ADA",
        "source": "WingRidersV2",
        "token1_policy": "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
        "token1_name": "494147",
        "token1_decimals": 6,
        "token2_policy": "",
        "token2_name": "lovelace",
        "token2_decimals": 6,
        "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
        "security_token_name": "4c",
    },
    # Expected results.
    {
        "utxo": "c230d1867357bd292ac8a97440a62504cfa6bd0b4643251db061d44c43233eb4#0",
        "token1_volume": 1953183.438367,
        "token2_volume": 288052.828506,
        "price": 0.14747863556882942,
        "amounts": {
            "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114.494147": 1953183438367,
            "lovelace": 288052828506,
        },
    },
    [
        (
            1,
            "IAG-ADA",
            "WingRidersV2",
            0.14747863556882942,
            1953183438367,
            288052828506,
            585,
            167678074,
            "2018-02-19T12:55:00Z",
        )
    ],
)

snek_spectrum = (
    {"epoch": 586, "block_height": 168189641},
    {
        "tx_hash": "9b0667342374d69e282874d8b494dbaef8922fa057dc6943462be2809912e1a5",
        "tx_index": 3,
        "amount": 1977157659,
        "assets": {
            "f8fd67ee46f66da669f68dc941090eb753687636b47fc6fd7f5e6254": {
                "534e454b5f4144415f4e4654": 1
            },
            "5909011713c342c40a08f5b8ab6c2f1417e86ba3abeca8e932a09c1c": {
                "534e454b5f4144415f4c51": 9223372036832714987
            },
            "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f": {
                "534e454b": 397918
            },
        },
    },
    {
        "feed": "SNEK-ADA",
        "source": "Spectrum",
        "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
        "token1_name": "534e454b",
        "token1_decimals": 0,
        "token2_policy": "",
        "token2_name": "lovelace",
        "token2_decimals": 6,
        "security_token_policy": "f8fd67ee46f66da669f68dc941090eb753687636b47fc6fd7f5e6254",
        "security_token_name": "534e454b5f4144415f4e4654",
    },
    # Expected results.
    {
        "utxo": "9b0667342374d69e282874d8b494dbaef8922fa057dc6943462be2809912e1a5#3",
        "token1_volume": 397918.0,
        "token2_volume": 1977.157659,
        "price": 0.0049687565252137375,
        "amounts": {
            "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 397918,
            "lovelace": 1977157659,
        },
    },
    [
        (
            1,
            "SNEK-ADA",
            "Spectrum",
            0.0049687565252137375,
            397918,
            1977157659,
            586,
            168189641,
            "2018-02-19T12:55:00Z",
        )
    ],
)

axo_minswap = (
    {"epoch": 586, "block_height": 168194226},
    {
        "tx_hash": "45c84655e7c4100839d5227f11298d184763ec7eac9d514dcfa9967041efe04c",
        "tx_index": 0,
        "amount": 2799878,
        "assets": {
            "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
                "7808399fc27d730b799f1c12f152e1062ac2ffbe687b101cc0fa30a3014742ef": 1401459492
            },
            "420000029ad9527271b1b1e3c27ee065c18df70a4a4cfc3093a41a44": {
                "41584f": 1610487896264
            },
            "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                "4d494e53574150": 1
            },
            "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                "7808399fc27d730b799f1c12f152e1062ac2ffbe687b101cc0fa30a3014742ef": 1
            },
        },
    },
    {
        "feed": "AXO-ADA",
        "source": "MinSwap",
        "token1_policy": "420000029ad9527271b1b1e3c27ee065c18df70a4a4cfc3093a41a44",
        "token1_name": "41584f",
        "token1_decimals": 9,
        "token2_policy": "",
        "token2_name": "lovelace",
        "token2_decimals": 6,
        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
        "security_token_name": "7808399fc27d730b799f1c12f152e1062ac2ffbe687b101cc0fa30a3014742ef",
    },
    # Expected results should be nil.
    {},
    [],
)

rserg_minswap = (
    {"epoch": 586, "block_height": 168194363},
    {
        "tx_hash": "a54e4e4a2b7655e7c0dcdf71059231710cc3395c9d754c5e6f4cd109261fe69c",
        "tx_index": 0,
        "amount": 6230107,
        "assets": {
            "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
                "dc06f64060dfa4119c56f8f5b8c69f897a335e3e9802456f4ae26ff7f6f1b570": 1098951379
            },
            "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                "4d494e53574150": 1
            },
            "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                "dc06f64060dfa4119c56f8f5b8c69f897a335e3e9802456f4ae26ff7f6f1b570": 1
            },
            "04b95368393c821f180deee8229fbd941baaf9bd748ebcdbf7adbb14": {
                "7273455247": 7199617831
            },
        },
    },
    {
        "feed": "rsERG-ADA",
        "source": "MinSwap",
        "token1_policy": "04b95368393c821f180deee8229fbd941baaf9bd748ebcdbf7adbb14",
        "token1_name": "7273455247",
        "token1_decimals": 9,
        "token2_policy": "",
        "token2_name": "lovelace",
        "token2_decimals": 6,
        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
        "security_token_name": "dc06f64060dfa4119c56f8f5b8c69f897a335e3e9802456f4ae26ff7f6f1b570",
    },
    # Expected results.
    {
        "utxo": "a54e4e4a2b7655e7c0dcdf71059231710cc3395c9d754c5e6f4cd109261fe69c#0",
        "token1_volume": 7.199617831,
        "token2_volume": 6.230107,
        "price": 0.8653385702188947,
        "amounts": {
            "04b95368393c821f180deee8229fbd941baaf9bd748ebcdbf7adbb14.7273455247": 7199617831,
            "lovelace": 6230107,
        },
    },
    [
        (
            1,
            "rsERG-ADA",
            "MinSwap",
            0.8653385702188947,
            7199617831,
            6230107,
            586,
            168194363,
            "2018-02-19T12:55:00Z",
        )
    ],
)


parse_tests = [
    djed_minswap,
    fact_sundae,
    iusd_wingriders,
    iag_wingridesv2,
    snek_spectrum,
    axo_minswap,
    rserg_minswap,
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize("context, utxo, token_details, res, db_row", parse_tests)
def test_parse_utxo(
    context: dict, utxo: dict, token_details: dict, res: dict, db_row: list
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
    parsed_res = parse_utxo(
        database=db,
        utxo=utxo,
        epoch=context["epoch"],
        block_height=context["block_height"],
        tokens_details=token_details,
    )
    assert parsed_res == res
    # Ensure the database row was updated correctly.
    price_data_row = cursor.execute("select * from price")
    price_data = price_data_row.fetchall()
    try:
        # Ensure the database row was updated correctly.
        price_data_row = cursor.execute("select * from price")
        price_data = price_data_row.fetchall()
        assert len(price_data) == 1
        assert price_data == db_row
        conn.close()
    except AssertionError as err:
        if not db_row:
            return
        raise AssertionError from err


mock_source1 = (
    {"epoch": 1, "block_height": 1},
    {
        "tx_hash": "TX_HASH_",
        "tx_index": 0,
        "amount": 20000000,
        "assets": {
            "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
                "MOCK_ASSET_NAME_UNUSED_1": 9223371351054998394,
                "4c": 1,
            },
            "BADF00D1": {"CAFED00D": 10000000},
        },
    },
    {
        "feed": "BASE-ADA",
        "source": "SuperMockDEX1",
        "token1_policy": "BADF00D1",
        "token1_name": "CAFED00D",
        "token1_decimals": 6,
        "token2_policy": "TOKEN_2_POLICY_UNUSED",
        "token2_name": "lovelace",
        "token2_decimals": 6,
        "security_token_policy": "MOCK_SECURITY_POLICY_ID_UNUSED_1",
        "security_token_name": "SECURITY_TOKEN_NAME_UNUSED",
    },
    # Expected results.
    {
        "utxo": "TX_HASH_#0",
        "token1_volume": 10.0,
        "token2_volume": 20.0,
        "price": 2.0,
        "amounts": {"BADF00D1.CAFED00D": 10000000, "lovelace": 20000000},
    },
    # Database result.
    [
        (
            # ID (Auto Increment)
            1,
            # Pair.
            "BASE-ADA",
            # Source.
            "SuperMockDEX1",
            # Price.
            2.0,
            # Token one volume.
            10000000,
            # Token two volume.
            20000000,
            # Epoch.
            1,
            # Block height.
            1,
            # Datetime.
            "2020-12-25T13:01:00Z",
        )
    ],
)

mock_source2 = (
    {"epoch": 1, "block_height": 1},
    {
        "tx_hash": "TX_HASH_",
        "tx_index": 1,
        "amount": 20000000,
        "assets": {
            "BA5EBA11": {"CAFEF00D": 10000000},
            "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
                "MOCK_ASSET_NAME_UNUSED_1": 9223372034407162828,
                "4c": 1,
            },
        },
    },
    {
        # Generic pair names.
        "feed": "ADA-QUOTE",
        "source": "SuperMockDEX1",
        "token1_policy": "TOKEN_1_POLICY_UNUSED",
        "token1_name": "lovelace",
        "token1_decimals": 6,
        "token2_policy": "BA5EBA11",
        "token2_name": "CAFEF00D",
        "token2_decimals": 6,
        "security_token_policy": "MOCK_SECURITY_POLICY_ID_UNUSED_1",
        "security_token_name": "SECURITY_TOKEN_NAME_UNUSED",
    },
    # Expected results.
    {
        "utxo": "TX_HASH_#1",
        "token1_volume": 20.0,
        "token2_volume": 10.0,
        "price": 0.5,
        "amounts": {
            "lovelace": 20000000,
            "BA5EBA11.CAFEF00D": 10000000,
        },
    },
    # Database result.
    [
        (
            # ID (Auto Increment)
            1,
            # Pair.
            "ADA-QUOTE",
            # Source.
            "SuperMockDEX1",
            # Price.
            0.5,
            # Token one volume.
            20000000,
            # Token two volume.
            10000000,
            # Epoch.
            1,
            # Block height.
            1,
            # Datetime.
            "2020-12-25T13:01:00Z",
        )
    ],
)

mock_source3 = (
    {"epoch": 1, "block_height": 1},
    {
        "tx_hash": "TX_HASH_",
        "tx_index": 0,
        "amount": 20000000,
        "assets": {
            "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
                "MOCK_ASSET_NAME_UNUSED_1": 9223371351054998394,
                "4c": 1,
            },
            "BADF00D1": {"CAFED00D": 10000000},
        },
    },
    {
        # Generic pair names.
        "feed": "BASE-QUOTE",
        "source": "SuperMockDEX1",
        "token1_policy": "BADF00D1",
        "token1_name": "CAFED00D",
        "token1_decimals": 6,
        "token2_policy": "TOKEN_2_POLICY_UNUSED",
        # Generic pairs enabled by name "lovelace".
        "token2_name": "lovelace",
        "token2_decimals": 6,
        "security_token_policy": "MOCK_SECURITY_POLICY_ID_UNUSED_1",
        "security_token_name": "SECURITY_TOKEN_NAME_UNUSED",
    },
    # Expected results.
    {
        "utxo": "TX_HASH_#0",
        "token1_volume": 10.0,
        "token2_volume": 20.0,
        "price": 2.0,
        "amounts": {"BADF00D1.CAFED00D": 10000000, "lovelace": 20000000},
    },
    # Database result.
    [
        (
            # ID (Auto Increment)
            1,
            # Pair.
            "BASE-QUOTE",
            # Source.
            "SuperMockDEX1",
            # Price.
            2.0,
            # Token one volume.
            10000000,
            # Token two volume.
            20000000,
            # Epoch.
            1,
            # Block height.
            1,
            # Datetime.
            "2020-12-25T13:01:00Z",
        )
    ],
)

mock_parse_tests = [
    mock_source1,
    mock_source2,
    mock_source3,
]


@time_machine.travel(datetime.datetime(2020, 12, 25, 13, 1, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize("context, utxo, token_details, res, db_row", mock_parse_tests)
def test_parse_utxo_with_mocks(
    context: dict, utxo: dict, token_details: dict, res: dict, db_row: list
):
    """Use mock data to test parse utxo and ensure it summarizes the
    data correctly.
    """
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )
    parsed_res = parse_utxo(
        database=db,
        utxo=utxo,
        epoch=context["epoch"],
        block_height=context["block_height"],
        tokens_details=token_details,
    )
    assert parsed_res == res
    # Ensure the database row was updated correctly.
    price_data_row = cursor.execute("select * from price")
    price_data = price_data_row.fetchall()
    assert len(price_data) == 1
    assert price_data == db_row
    conn.close()


mock_source_negative_res_1 = (
    # Lovelace isn't one of the token names.
    {"epoch": 1, "block_height": 1},
    {
        "tx_hash": "TX_HASH_",
        "tx_index": 0,
        "amount": 20000000,
        "assets": {
            "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
                "MOCK_ASSET_NAME_UNUSED_1": 9223371351054998394,
                "4c": 1,
            },
            "BADF00D1": {"CAFED00D": 10000000},
        },
    },
    {
        # Generic pair names.
        "feed": "BASE-QUOTE",
        "source": "SuperMockDEX1",
        "token1_policy": "BADF00D1",
        "token1_name": "CAFED00D",
        "token1_decimals": 6,
        "token2_policy": "TOKEN_2_POLICY_UNUSED",
        # Use a badd token name".
        "token2_name": "NOTLOVELACE",
        "token2_decimals": 6,
        "security_token_policy": "MOCK_SECURITY_POLICY_ID_UNUSED_1",
        "security_token_name": "SECURITY_TOKEN_NAME_UNUSED",
    },
    # Expected results.
    {},
    # Database result.
    [],
)

mock_source_negative_res_2 = (
    # CNT value is zero.
    {"epoch": 1, "block_height": 1},
    {
        "tx_hash": "TX_HASH_",
        "tx_index": 0,
        "amount": 20000000,
        "assets": {
            "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
                "MOCK_ASSET_NAME_UNUSED_1": 9223371351054998394,
                "4c": 1,
            },
            "BADF00D1": {"CAFED00D": 0},
        },
    },
    {
        # Generic pair names.
        "feed": "BASE-QUOTE",
        "source": "SuperMockDEX1",
        "token1_policy": "BADF00D1",
        "token1_name": "CAFED00D",
        "token1_decimals": 6,
        "token2_policy": "TOKEN_2_POLICY_UNUSED",
        # Use a badd token name".
        "token2_name": "lovelace",
        "token2_decimals": 6,
        "security_token_policy": "MOCK_SECURITY_POLICY_ID_UNUSED_1",
        "security_token_name": "SECURITY_TOKEN_NAME_UNUSED",
    },
    # Expected results.
    {},
    # Database result.
    [],
)

mock_source_negative_res_3 = (
    # Lovelace (token2) value is zero.
    {"epoch": 1, "block_height": 1},
    {
        "tx_hash": "TX_HASH_",
        "tx_index": 0,
        "amount": 0,
        "assets": {
            "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
                "MOCK_ASSET_NAME_UNUSED_1": 9223371351054998394,
                "4c": 1,
            },
            "BADF00D1": {"CAFED00D": 10000000},
        },
    },
    {
        # Generic pair names.
        "feed": "BASE-QUOTE",
        "source": "SuperMockDEX1",
        "token1_policy": "BADF00D1",
        "token1_name": "CAFED00D",
        "token1_decimals": 6,
        "token2_policy": "TOKEN_2_POLICY_UNUSED",
        "token2_name": "lovelace",
        "token2_decimals": 6,
        "security_token_policy": "MOCK_SECURITY_POLICY_ID_UNUSED_1",
        "security_token_name": "SECURITY_TOKEN_NAME_UNUSED",
    },
    # Expected results.
    {},
    # Database result.
    [],
)

mock_source_negative_res_4 = (
    # Lovelace (token1) value is zero.
    {"epoch": 1, "block_height": 1},
    {
        "tx_hash": "TX_HASH_",
        "tx_index": 0,
        "amount": 0,
        "assets": {
            "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
                "MOCK_ASSET_NAME_UNUSED_1": 9223371351054998394,
                "4c": 1,
            },
            "BADF00D1": {"CAFED00D": 10000000},
        },
    },
    {
        # Generic pair names.
        "feed": "BASE-QUOTE",
        "source": "SuperMockDEX1",
        "token1_policy": "TOKEN_2_POLICY_UNUSED",
        "token1_name": "lovelace",
        "token1_decimals": 6,
        "token2_policy": "BADF00D1",
        "token2_name": "CAFED00D",
        "token2_decimals": 6,
        "security_token_policy": "MOCK_SECURITY_POLICY_ID_UNUSED_1",
        "security_token_name": "SECURITY_TOKEN_NAME_UNUSED",
    },
    # Expected results.
    {},
    # Database result.
    [],
)

mock_parse_negative_tests = [
    mock_source_negative_res_1,
    mock_source_negative_res_2,
    mock_source_negative_res_3,
    mock_source_negative_res_4,
]


@time_machine.travel(datetime.datetime(2020, 12, 25, 13, 1, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "context, utxo, token_details, res, db_row", mock_parse_negative_tests
)
def test_parse_utxo_with_mocks_negative_results(
    context: dict, utxo: dict, token_details: dict, res: dict, db_row: list
):
    """Ensure that incorrect data doesn't return results."""
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )
    parsed_res = parse_utxo(
        database=db,
        utxo=utxo,
        epoch=context["epoch"],
        block_height=context["block_height"],
        tokens_details=token_details,
    )
    assert parsed_res == res
    # Ensure the database row was updated correctly.
    price_data_row = cursor.execute("select * from price")
    price_data = price_data_row.fetchall()
    assert len(price_data) == 0
    assert price_data == db_row
    conn.close()
