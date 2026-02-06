"""Test the save_output, and save_utxo functions as they are both
quite involved.
"""

# pylint: disable=E0401,E1111,R0913,C0302,E1128

import copy
import datetime
import sqlite3
from datetime import timezone

import pytest
import time_machine
from deepdiff import DeepDiff

import src.cnt_collector_node.database_abstraction as dba
from src.cnt_collector_node import utxo_objects
from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.helper_functions import (
    _save_output,
    save_utxo,
    utxos_dict_update,
)

t1_context = utxo_objects.InitialChainContext(
    block_height="MOCK_BLOCK_HEIGHT",
    epoch="MOCK_EPOCH",
    address="MOCK_ADDRESS",
    tx_hash="MOCK_TX_HASH",
    output_index="MOCK_OUTPUT_INDEX",
    utxo_ids=[1],
)


t1_tokens_pair = {
    "pair": "BASE-QUOTE",
    "source": "SuperMockDex",
    "token1_name": "mock_token1",
    "token1_policy": "mock_token1_policy",
    "token2_name": "lovelace",
    "token2_policy": "mock_token2_policy",
    "token1_decimals": 6,
    "token2_decimals": 6,
    "security_token_policy": "unused",
    "security_token_name": "unused",
}

t1_output_expected = {
    "amount": 10000000,
    "assets": {
        "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
            "MOCK_ASSET_NAME_UNUSED_1": 9223371351054998394,
            "4c": 1,
        },
        "mock_token1_policy": {"mock_token1": 20000000},
    },
}

t1_row_expected = [
    (
        1,
        "BASE-QUOTE",
        "SuperMockDex",
        0.5,
        20000000,
        10000000,
        "MOCK_EPOCH",
        "MOCK_BLOCK_HEIGHT",
        "2018-02-19T12:55:00Z",
    )
]

t2_context = utxo_objects.InitialChainContext(
    block_height="MOCK_BLOCK_HEIGHT2",
    epoch="MOCK_EPOCH2",
    address="MOCK_ADDRESS2",
    tx_hash="MOCK_TX_HASH2",
    output_index="MOCK_OUTPUT_INDEX2",
    utxo_ids=[1],
)


t2_tokens_pair = {
    "pair": "BASE-QUOTE2",
    "source": "SuperMockDex2",
    "token1_name": "lovelace",
    "token1_policy": "mock_token1_policy",
    "token2_name": "mock_token2",
    "token2_policy": "mock_token2_policy",
    "token1_decimals": 6,
    "token2_decimals": 6,
    "security_token_policy": "unused",
    "security_token_name": "unused",
}

t2_output_expected = {
    "amount": 10000000,
    "assets": {
        "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
            "MOCK_ASSET_NAME_UNUSED_1": 92233713510549983941111,
            "4c": 1,
        },
        "mock_token2_policy": {"mock_token2": 20000000},
    },
}

t2_row_expected = [
    (
        1,
        "BASE-QUOTE2",
        "SuperMockDex2",
        2.0,
        10000000,
        20000000,
        "MOCK_EPOCH2",
        "MOCK_BLOCK_HEIGHT2",
        "2018-02-19T12:55:00Z",
    )
]

save_output_tests = [
    (t1_context, t1_tokens_pair, t1_output_expected, t1_row_expected),
    (t2_context, t2_tokens_pair, t2_output_expected, t2_row_expected),
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "context, tokens_pair_dict, output_contents, expected", save_output_tests
)
def test_save_output(
    mocker,
    context: utxo_objects.InitialChainContext,
    tokens_pair_dict: dict,
    output_contents: dict,
    expected: dict,
):
    """Ensure that save_output writes to the database correctly values
    based on what is provided.

    We don't have any sample data for this function so it is currently
    relying entirely on mocks.
    """
    output_copy = copy.deepcopy(output_contents)
    context_copy = copy.deepcopy(context)
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )
    # It's probably fine to mock this as it should be modelled in
    # other unit tests.
    mocker.patch(
        "src.cnt_collector_node.helper_functions.save_utxo_record", return_value=False
    )
    # Test one shouldn't write any data. This provides some level of
    # integration testing we can make use of in this characterization
    # test.
    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    ret = _save_output(
        database=db,
        initial_chain_context=context,
        tokens_pair=tokens_pair,
        output_contents=output_contents,
    )
    assert ret is None  # Function returns None by default.
    cursor.execute("select * from price;")
    res = cursor.fetchall()
    # Make sure nothing was written to the database.
    assert not res
    # Test two changes the parameter to `True` so that the data is
    # written as anticipated.`
    mocker.patch(
        "src.cnt_collector_node.helper_functions.save_utxo_record", return_value=True
    )
    ret = _save_output(
        database=db,
        initial_chain_context=context,
        tokens_pair=tokens_pair,
        output_contents=output_contents,
    )
    assert ret is None  # Function returns None by default.
    cursor.execute("select * from price;")
    res = cursor.fetchall()
    assert res == expected
    # The other dicts are prone to change. Ensure they remain
    # unchanged.
    assert output_contents == output_copy
    # Context is unchanged.
    assert context_copy == context


t1_context_negative = utxo_objects.InitialChainContext(
    block_height="MOCK_BLOCK_HEIGHT",
    epoch="MOCK_EPOCH",
    address="MOCK_ADDRESS",
    tx_hash="MOCK_TX_HASH",
    output_index="MOCK_OUTPUT_INDEX",
    utxo_ids=[2],
)

t1_tokens_pair_negative = {
    "pair": "BASE-QUOTE",
    "source": "SuperMockDex",
    "token1_name": "mock_token1",
    "token1_policy": "mock_token1_policy",
    "token2_name": "lovelace",
    "token2_policy": "mock_token2_policy",
    "token1_decimals": 6,
    "token2_decimals": 6,
    "security_token_policy": "unused",
    "security_token_name": "unused",
}

t1_output_expected_negative = {
    "amount": 0,
    "assets": {
        "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
            "MOCK_ASSET_NAME_UNUSED_1": 9223371351054998394,
            "4c": 1,
        },
        "mock_token1_policy": {"mock_token1": 20000000},
    },
}

t1_row_expected_negative = [
    (
        1,
        "BASE-QUOTE",
        "SuperMockDex",
        0.5,
        20000000,
        10000000,
        "MOCK_EPOCH",
        "MOCK_BLOCK_HEIGHT",
        "2018-02-19T12:55:00Z",
    )
]


t2_context_negative = utxo_objects.InitialChainContext(
    block_height="MOCK_BLOCK_HEIGHT2",
    epoch="MOCK_EPOCH2",
    address="MOCK_ADDRESS2",
    tx_hash="MOCK_TX_HASH2",
    output_index="MOCK_OUTPUT_INDEX2",
    utxo_ids=[2],
)

t2_tokens_pair_negative = {
    "pair": "BASE-QUOTE2",
    "source": "SuperMockDex2",
    "token1_name": "lovelace",
    "token1_policy": "mock_token1_policy",
    "token2_name": "mock_token2",
    "token2_policy": "mock_token2_policy",
    "token1_decimals": 6,
    "token2_decimals": 6,
    "security_token_policy": "unused",
    "security_token_name": "unused",
}

t2_output_expected_negative = {
    "amount": 10000000,
    "assets": {
        "MOCK_SECURITY_POLICY_ID_UNUSED_1": {
            "MOCK_ASSET_NAME_UNUSED_1": 922337135105499839411111,
            "4c": 1,
        },
        "mock_token2_policy": {"mock_token2": 0},
    },
}

t2_row_expected_negative = [
    (
        1,
        "BASE-QUOTE2",
        "SuperMockDex2",
        2.0,
        10000000,
        20000000,
        "MOCK_EPOCH2",
        "MOCK_BLOCK_HEIGHT2",
        "2018-02-19T12:55:00Z",
    )
]

save_output_tests = [
    # Amount is zero for token 1.
    (
        t1_context_negative,
        t1_tokens_pair_negative,
        t1_output_expected_negative,
        [],
    ),
    # Amount if zero for token 2.
    (
        t2_context_negative,
        t2_tokens_pair_negative,
        t2_output_expected_negative,
        [],
    ),
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "context, tokens_pair_dict, output_contents, expected", save_output_tests
)
def test_save_output_dont_write(
    mocker,
    context: dict,
    tokens_pair_dict: dict,
    output_contents: dict,
    expected: dict,
):
    """Ensure that save_output does not write to the database when
    certain cases are not met.
    """
    output_contents_copy = copy.deepcopy(output_contents)
    context_copy = copy.deepcopy(context)
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )
    # It's probably fine to mock this as it should be modelled in
    # other unit tests.
    mocker.patch(
        "src.cnt_collector_node.helper_functions.save_utxo_record", return_value=True
    )
    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    ret = _save_output(
        database=db,
        initial_chain_context=context,
        tokens_pair=tokens_pair,
        output_contents=output_contents,
    )
    assert ret is None  # Function returns None by default.
    cursor.execute("select * from price;")
    res = cursor.fetchall()
    assert res == expected
    assert output_contents_copy == output_contents
    # Context is unchanged.
    assert context == context_copy


save_utxo_tests = [
    (
        utxo_objects.InitialChainContext(
            block_height=169573509,
            epoch=590,
            address="addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
            tx_hash=None,
            output_index=None,
            utxo_ids=None,
        ),
        {
            "pair": "ADA-iUSD",
            "source": "MinSwap",
            "token1_policy": "",
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
            "token2_name": "69555344",
            "token2_decimals": 6,
            "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
            "security_token_name": "8fde43a3f0b9f0e6f63bec7335e0b855c6b62a4dc51f1b762ccb6dfbbafcfe47",
        },
        {
            "tx_hash": "bcb17da7458e1872160ab58c5d36d9d250785d4c991296c17d723852b990bcf2",
            "tx_index": 0,
            "amount": 9531723661,
            "assets": {
                "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880": {
                    "69555344": 6238259240
                },
                "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
                    "8fde43a3f0b9f0e6f63bec7335e0b855c6b62a4dc51f1b762ccb6dfbbafcfe47": 1200102
                },
                "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                    "4d494e53574150": 1
                },
                "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                    "8fde43a3f0b9f0e6f63bec7335e0b855c6b62a4dc51f1b762ccb6dfbbafcfe47": 1
                },
            },
        },
        {
            "ADA-DJED": {
                "MinSwap": {
                    "context": {
                        "block_height": 169573509,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "0bcabcd9235c2d94806584ecb215384dafbebf3a2b95eeff0eb43b3674f4a4b0",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 17636729902,
                        "token2_amount": 11372763013,
                        "price": 0.6448339956553019,
                    },
                    "tokens_pair": {
                        "pair": "ADA-DJED",
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
                }
            },
            "IAG-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169573509,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "833df6170e96e86c24c2eb4441ef8d90c675d4337f446c777e0f16ea7f66eded",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 49877926483,
                        "token2_amount": 8155858520,
                        "price": 0.16351639081828667,
                    },
                    "tokens_pair": {
                        "pair": "IAG-ADA",
                        "source": "MinSwap",
                        "token1_policy": "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
                        "token1_name": "494147",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "bdfd144032f09ad980b8d205fef0737c2232b4e90a5d34cc814d0ef687052400",
                    },
                }
            },
        },
        {
            "ADA-DJED": {
                "MinSwap": {
                    "context": {
                        "block_height": 169573509,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "0bcabcd9235c2d94806584ecb215384dafbebf3a2b95eeff0eb43b3674f4a4b0",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 17636729902,
                        "token2_amount": 11372763013,
                        "price": 0.6448339956553019,
                    },
                    "tokens_pair": {
                        "pair": "ADA-DJED",
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
                }
            },
            "IAG-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169573509,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "833df6170e96e86c24c2eb4441ef8d90c675d4337f446c777e0f16ea7f66eded",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 49877926483,
                        "token2_amount": 8155858520,
                        "price": 0.16351639081828667,
                    },
                    "tokens_pair": {
                        "pair": "IAG-ADA",
                        "source": "MinSwap",
                        "token1_policy": "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
                        "token1_name": "494147",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "bdfd144032f09ad980b8d205fef0737c2232b4e90a5d34cc814d0ef687052400",
                    },
                }
            },
            "ADA-iUSD": {
                "MinSwap": {
                    "context": {
                        "block_height": 169573509,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "bcb17da7458e1872160ab58c5d36d9d250785d4c991296c17d723852b990bcf2",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 9531723661,
                        "token2_amount": 6238259240,
                        "price": 0.6544733630418244,
                    },
                    "tokens_pair": {
                        "pair": "ADA-iUSD",
                        "source": "MinSwap",
                        "token1_policy": "",
                        "token1_name": "lovelace",
                        "token1_decimals": 6,
                        "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
                        "token2_name": "69555344",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "8fde43a3f0b9f0e6f63bec7335e0b855c6b62a4dc51f1b762ccb6dfbbafcfe47",
                    },
                }
            },
        },
    ),
    (
        utxo_objects.InitialChainContext(
            block_height=169657474,
            epoch=590,
            address="addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
            tx_hash=None,
            output_index=None,
            utxo_ids=None,
        ),
        {
            "pair": "IAG-ADA",
            "source": "MinSwap",
            "token1_policy": "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
            "token1_name": "494147",
            "token1_decimals": 6,
            "token2_policy": "",
            "token2_name": "lovelace",
            "token2_decimals": 6,
            "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
            "security_token_name": "bdfd144032f09ad980b8d205fef0737c2232b4e90a5d34cc814d0ef687052400",
        },
        {
            "tx_hash": "4d4def07031b979f0609697d8bc6898fedfa8dbe449fbd589f95d8506ac33e0d",
            "tx_index": 0,
            "amount": 7975352667,
            "assets": {
                "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
                    "bdfd144032f09ad980b8d205fef0737c2232b4e90a5d34cc814d0ef687052400": 24118864
                },
                "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114": {
                    "494147": 51010801496
                },
                "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                    "4d494e53574150": 1
                },
                "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                    "bdfd144032f09ad980b8d205fef0737c2232b4e90a5d34cc814d0ef687052400": 1
                },
            },
        },
        {
            "SNEK-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169657474,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "078306644dd40ea58e8f201d754698f1daf8ac73ac2d0290969ce0273dd5d7f2",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 34722671,
                        "token2_amount": 138164922159,
                        "price": 0.003979098329129116,
                    },
                    "tokens_pair": {
                        "pair": "SNEK-ADA",
                        "source": "MinSwap",
                        "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
                        "token1_name": "534e454b",
                        "token1_decimals": 0,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896",
                    },
                }
            },
            "MIN-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169657474,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "575a6d84f6eadc83171a67a2e3470ce63cfb0f6e0fd066d7b41796ca53d666e8",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 26505519689124,
                        "token2_amount": 610495933215,
                        "price": 0.02303278488312397,
                    },
                    "tokens_pair": {
                        "pair": "MIN-ADA",
                        "source": "MinSwap",
                        "token1_policy": "29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6",
                        "token1_name": "4d494e",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "6aa2153e1ae896a95539c9d62f76cedcdabdcdf144e564b8955f609d660cf6a2",
                    },
                }
            },
            "INDY-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169657474,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "0c7dafc9a5cfebae58d55ca28ce9b86fc6a4ee00a248a75bfa7566702a224e65",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 237511262982,
                        "token2_amount": 326876797257,
                        "price": 1.3762580904711565,
                    },
                    "tokens_pair": {
                        "pair": "INDY-ADA",
                        "source": "MinSwap",
                        "token1_policy": "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0",
                        "token1_name": "494e4459",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "571cdbdfae07f098049b917007366cca8f2e0770a7b2bae5f7726f36849fbcb9",
                    },
                }
            },
            "ADA-DJED": {
                "MinSwap": {
                    "context": {
                        "block_height": 169657474,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "221d5448e52d47fa0e4ece3c5785e4533757560f5a13f3fecc66aed77a086b95",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 17671726404,
                        "token2_amount": 11352572651,
                        "price": 0.6424144642953696,
                    },
                    "tokens_pair": {
                        "pair": "ADA-DJED",
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
                }
            },
        },
        {
            "SNEK-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169657474,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "078306644dd40ea58e8f201d754698f1daf8ac73ac2d0290969ce0273dd5d7f2",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 34722671,
                        "token2_amount": 138164922159,
                        "price": 0.003979098329129116,
                    },
                    "tokens_pair": {
                        "pair": "SNEK-ADA",
                        "source": "MinSwap",
                        "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
                        "token1_name": "534e454b",
                        "token1_decimals": 0,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896",
                    },
                }
            },
            "MIN-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169657474,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "575a6d84f6eadc83171a67a2e3470ce63cfb0f6e0fd066d7b41796ca53d666e8",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 26505519689124,
                        "token2_amount": 610495933215,
                        "price": 0.02303278488312397,
                    },
                    "tokens_pair": {
                        "pair": "MIN-ADA",
                        "source": "MinSwap",
                        "token1_policy": "29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6",
                        "token1_name": "4d494e",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "6aa2153e1ae896a95539c9d62f76cedcdabdcdf144e564b8955f609d660cf6a2",
                    },
                }
            },
            "INDY-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169657474,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "0c7dafc9a5cfebae58d55ca28ce9b86fc6a4ee00a248a75bfa7566702a224e65",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 237511262982,
                        "token2_amount": 326876797257,
                        "price": 1.3762580904711565,
                    },
                    "tokens_pair": {
                        "pair": "INDY-ADA",
                        "source": "MinSwap",
                        "token1_policy": "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0",
                        "token1_name": "494e4459",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "571cdbdfae07f098049b917007366cca8f2e0770a7b2bae5f7726f36849fbcb9",
                    },
                }
            },
            "ADA-DJED": {
                "MinSwap": {
                    "context": {
                        "block_height": 169657474,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "221d5448e52d47fa0e4ece3c5785e4533757560f5a13f3fecc66aed77a086b95",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 17671726404,
                        "token2_amount": 11352572651,
                        "price": 0.6424144642953696,
                    },
                    "tokens_pair": {
                        "pair": "ADA-DJED",
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
                }
            },
            "IAG-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169657474,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "4d4def07031b979f0609697d8bc6898fedfa8dbe449fbd589f95d8506ac33e0d",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 51010801496,
                        "token2_amount": 7975352667,
                        "price": 0.15634635083366383,
                    },
                    "tokens_pair": {
                        "pair": "IAG-ADA",
                        "source": "MinSwap",
                        "token1_policy": "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
                        "token1_name": "494147",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "bdfd144032f09ad980b8d205fef0737c2232b4e90a5d34cc814d0ef687052400",
                    },
                }
            },
        },
    ),
]


@pytest.mark.parametrize(
    "chain_context, tokens_pair_dict, utxo, utxos_dict, expected_utxos_dict",
    save_utxo_tests,
)
def test_save_utxo_updated(
    chain_context: utxo_objects.InitialChainContext,
    tokens_pair_dict: dict,
    utxo: dict,
    utxos_dict: dict,
    expected_utxos_dict: dict,
):
    """Test save_utxo when the utxo record is updated.

    It looks like in most cases save_utxo will always change the context
    dictionary or utxos_dict and so we need to test for changes in
    these structures.
    """

    utxo_copy = copy.deepcopy(utxo)
    context_copy = copy.deepcopy(chain_context)
    utxos_dict_copy = copy.deepcopy(utxos_dict)

    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)

    save_utxo(
        initial_chain_context=chain_context,
        tokens_pair=tokens_pair,
        utxo=utxo,
        utxos_dict=utxos_dict,
    )

    # Remain unchanged by function.
    assert utxo == utxo_copy
    assert chain_context == context_copy

    # Changed by the function.
    assert utxos_dict == expected_utxos_dict
    assert utxos_dict != utxos_dict_copy


save_utxo_tests_not_updated = [
    (
        utxo_objects.InitialChainContext(
            block_height=169658202,
            epoch=590,
            address="addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
            tx_hash="",
            output_index="",
            utxo_ids=[],
        ),
        {
            "pair": "SNEK-ADA",
            "source": "SundaeSwap",
            "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
            "token1_name": "534e454b",
            "token1_decimals": 0,
            "token2_policy": "",
            "token2_name": "lovelace",
            "token2_decimals": 6,
            "security_token_policy": "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913",
            "security_token_name": "70201f04",
        },
        {
            "tx_hash": "c479882c1c4f032325a0ec37ac862b59f47e1b139922d4af2f633d0d9a4e054a",
            "tx_index": 0,
            "amount": 37950882598,
            "assets": {
                "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f": {
                    "534e454b": 9508480
                },
                "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913": {
                    "70201f04": 1
                },
            },
        },
        {
            "SNEK-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "e3af879d2313fef1a2bf31e59443bd1e6c8f2eaef4b021d3ee62fb54d44c155f",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 34519908,
                        "token2_amount": 138978922159,
                        "price": 0.004026051348659446,
                    },
                    "tokens_pair": {
                        "pair": "SNEK-ADA",
                        "source": "MinSwap",
                        "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
                        "token1_name": "534e454b",
                        "token1_decimals": 0,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896",
                    },
                },
                "MinSwapV2": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                        "tx_hash": "8060e62bb25de9b6acd26bb567d6119a21d0d45eb14e62437ab97afeb01e8b13",
                        "output_index": 1,
                        "caller": "save_utxo",
                        "token1_amount": 1100020488,
                        "token2_amount": 4378207569584,
                        "price": 0.003980114568178842,
                    },
                    "tokens_pair": {
                        "pair": "SNEK-ADA",
                        "source": "MinSwapV2",
                        "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
                        "token1_name": "534e454b",
                        "token1_decimals": 0,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                        "security_token_name": "4d5350",
                    },
                },
                "SundaeSwap": {
                    "context": {
                        "block_height": 169658202,
                        "epoch": 590,
                        "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
                        "tx_hash": "c479882c1c4f032325a0ec37ac862b59f47e1b139922d4af2f633d0d9a4e054a",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 9508480,
                        "token2_amount": 37950882598,
                        "price": 0.003991267016179242,
                    },
                    "tokens_pair": {
                        "pair": "SNEK-ADA",
                        "source": "SundaeSwap",
                        "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
                        "token1_name": "534e454b",
                        "token1_decimals": 0,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913",
                        "security_token_name": "70201f04",
                    },
                },
            },
            "MIN-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "575a6d84f6eadc83171a67a2e3470ce63cfb0f6e0fd066d7b41796ca53d666e8",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 26505519689124,
                        "token2_amount": 610495933215,
                        "price": 0.02303278488312397,
                    },
                    "tokens_pair": {
                        "pair": "MIN-ADA",
                        "source": "MinSwap",
                        "token1_policy": "29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6",
                        "token1_name": "4d494e",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "6aa2153e1ae896a95539c9d62f76cedcdabdcdf144e564b8955f609d660cf6a2",
                    },
                },
                "MinSwapV2": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                        "tx_hash": "7f2557d27e73e3afe72a200e9e22718a9b984d34a04092742c3bf6800e17d650",
                        "output_index": 1,
                        "caller": "save_utxo",
                        "token1_amount": 229153664231882,
                        "token2_amount": 5300844568367,
                        "price": 0.02313227059290242,
                    },
                    "tokens_pair": {
                        "pair": "MIN-ADA",
                        "source": "MinSwapV2",
                        "token1_policy": "29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6",
                        "token1_name": "4d494e",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                        "security_token_name": "4d5350",
                    },
                },
            },
        },
    ),
    (
        utxo_objects.InitialChainContext(
            block_height=169658162,
            epoch=590,
            address="addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
            tx_hash="",
            output_index="",
            utxo_ids=[],
        ),
        {
            "pair": "ADA-iUSD",
            "source": "MinSwapV2",
            "token1_policy": "",
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
            "token2_name": "69555344",
            "token2_decimals": 6,
            "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
            "security_token_name": "4d5350",
        },
        {
            "tx_hash": "1c843962fbdac30c8c73762414d2bcf8f028c302dd558b1df29fc7e97b8a17dc",
            "tx_index": 1,
            "amount": 898425580328,
            "assets": {
                "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880": {
                    "69555344": 572591445480
                },
                "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                    "452089abb5bf8cc59b678a2cd7b9ee952346c6c0aa1cf27df324310a70d02fc3": 9223371372842902037,
                    "4d5350": 1,
                },
            },
        },
        {
            "SNEK-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "e3af879d2313fef1a2bf31e59443bd1e6c8f2eaef4b021d3ee62fb54d44c155f",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 34519908,
                        "token2_amount": 138978922159,
                        "price": 0.004026051348659446,
                    },
                    "tokens_pair": {
                        "pair": "SNEK-ADA",
                        "source": "MinSwap",
                        "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
                        "token1_name": "534e454b",
                        "token1_decimals": 0,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896",
                    },
                }
            },
            "MIN-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "575a6d84f6eadc83171a67a2e3470ce63cfb0f6e0fd066d7b41796ca53d666e8",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 26505519689124,
                        "token2_amount": 610495933215,
                        "price": 0.02303278488312397,
                    },
                    "tokens_pair": {
                        "pair": "MIN-ADA",
                        "source": "MinSwap",
                        "token1_policy": "29d222ce763455e3d7a09a665ce554f00ac89d2e99a1a83d267170c6",
                        "token1_name": "4d494e",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "6aa2153e1ae896a95539c9d62f76cedcdabdcdf144e564b8955f609d660cf6a2",
                    },
                }
            },
            "INDY-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "0c7dafc9a5cfebae58d55ca28ce9b86fc6a4ee00a248a75bfa7566702a224e65",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 237511262982,
                        "token2_amount": 326876797257,
                        "price": 1.3762580904711565,
                    },
                    "tokens_pair": {
                        "pair": "INDY-ADA",
                        "source": "MinSwap",
                        "token1_policy": "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0",
                        "token1_name": "494e4459",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "571cdbdfae07f098049b917007366cca8f2e0770a7b2bae5f7726f36849fbcb9",
                    },
                }
            },
            "ADA-DJED": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "221d5448e52d47fa0e4ece3c5785e4533757560f5a13f3fecc66aed77a086b95",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 17671726404,
                        "token2_amount": 11352572651,
                        "price": 0.6424144642953696,
                    },
                    "tokens_pair": {
                        "pair": "ADA-DJED",
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
                }
            },
            "IAG-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "4d4def07031b979f0609697d8bc6898fedfa8dbe449fbd589f95d8506ac33e0d",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 51010801496,
                        "token2_amount": 7975352667,
                        "price": 0.15634635083366383,
                    },
                    "tokens_pair": {
                        "pair": "IAG-ADA",
                        "source": "MinSwap",
                        "token1_policy": "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
                        "token1_name": "494147",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "bdfd144032f09ad980b8d205fef0737c2232b4e90a5d34cc814d0ef687052400",
                    },
                }
            },
            "ADA-iUSD": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "e85b32ac1cd3260636ac7d658f36d17aafd9710e91e9b06e1f0687de02069d95",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 9603365867,
                        "token2_amount": 6194885063,
                        "price": 0.64507435713633,
                    },
                    "tokens_pair": {
                        "pair": "ADA-iUSD",
                        "source": "MinSwap",
                        "token1_policy": "",
                        "token1_name": "lovelace",
                        "token1_decimals": 6,
                        "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
                        "token2_name": "69555344",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "8fde43a3f0b9f0e6f63bec7335e0b855c6b62a4dc51f1b762ccb6dfbbafcfe47",
                    },
                },
                "MinSwapV2": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                        "tx_hash": "1c843962fbdac30c8c73762414d2bcf8f028c302dd558b1df29fc7e97b8a17dc",
                        "output_index": 1,
                        "caller": "save_utxo",
                        "token1_amount": 898425580328,
                        "token2_amount": 572591445480,
                        "price": 0.6373276295972745,
                    },
                    "tokens_pair": {
                        "pair": "ADA-iUSD",
                        "source": "MinSwapV2",
                        "token1_policy": "",
                        "token1_name": "lovelace",
                        "token1_decimals": 6,
                        "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
                        "token2_name": "69555344",
                        "token2_decimals": 6,
                        "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                        "security_token_name": "4d5350",
                    },
                },
            },
            "SHEN-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "3c29f83ca72661b042c7fb679c9eb20ec487d01ec80270f715234f1b0c0e4010",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 15069514818,
                        "token2_amount": 16415703946,
                        "price": 1.0893319489219406,
                    },
                    "tokens_pair": {
                        "pair": "SHEN-ADA",
                        "source": "MinSwap",
                        "token1_policy": "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61",
                        "token1_name": "5368656e4d6963726f555344",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "53225313968e796f2c1e0b57540a13c3b81e06e2ed2637ac1ea9b9f4e27e3dc4",
                    },
                }
            },
            "CBLP-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "b71d407310d115ea8f2d5d52cb1b0a51c9f62d20541ffa64e4aefac9cfd93b5e",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 1334923690496,
                        "token2_amount": 441157602,
                        "price": 0.000330474022703189,
                    },
                    "tokens_pair": {
                        "pair": "CBLP-ADA",
                        "source": "MinSwap",
                        "token1_policy": "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd",
                        "token1_name": "43424c50",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "dfe1be4e42a1cf6a8f5648e904bef0b4b11ee8ca4131521b5256856ef34e3486",
                    },
                }
            },
            "ADA-USDM": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "be82097a4113fa59a0183476d65f9df22cb9640bbe01a372d610c312c397d252",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 113452869,
                        "token2_amount": 78396090,
                        "price": 0.691001388426766,
                    },
                    "tokens_pair": {
                        "pair": "ADA-USDM",
                        "source": "MinSwap",
                        "token1_policy": "",
                        "token1_name": "lovelace",
                        "token1_decimals": 6,
                        "token2_policy": "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad",
                        "token2_name": "0014df105553444d",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "116df62938bc100b55c0e72b57a48dced2f928635ad66660bc165a8f40f8e735",
                    },
                }
            },
            "FACT-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 169658162,
                        "epoch": 590,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "897c3c4ea64542238aa85c79ed5844a5339f7156f2b8ac01bccf52a69dd22e07",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 91061757796,
                        "token2_amount": 404595751,
                        "price": 0.0044430918180427695,
                    },
                    "tokens_pair": {
                        "pair": "FACT-ADA",
                        "source": "MinSwap",
                        "token1_policy": "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e51",
                        "token1_name": "6f7263666178746f6b656e",
                        "token1_decimals": 6,
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token2_decimals": 6,
                        "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                        "security_token_name": "b4ba2b47edce71234f328fa20efdb25c3f96e348ca19a683193880489bb368db",
                    },
                }
            },
        },
    ),
]


@pytest.mark.parametrize(
    "chain_context, tokens_pair_dict, utxo, utxos_dict",
    save_utxo_tests_not_updated,
)
def test_save_utxo_dict_not_updated(
    chain_context: utxo_objects.InitialChainContext,
    tokens_pair_dict: dict,
    utxo: dict,
    utxos_dict: dict,
):
    """Test save_utxo when the context is updated but the utxo dict
    isn't.
    """
    utxo_copy = copy.deepcopy(utxo)
    context_copy = copy.deepcopy(chain_context)
    utxos_dict_copy = copy.deepcopy(utxos_dict)
    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    save_utxo(
        initial_chain_context=chain_context,
        tokens_pair=tokens_pair,
        utxo=utxo,
        utxos_dict=utxos_dict,
    )
    # Remain unchanged by function.
    assert utxo == utxo_copy
    assert utxos_dict == utxos_dict_copy
    assert chain_context == context_copy


save_utxo_tests_mock = [
    (
        # Chain context.
        utxo_objects.InitialChainContext(
            block_height=None,
            epoch=None,
            address=None,
            tx_hash=None,
            output_index=None,
            utxo_ids=None,
        ),
        # Tokens pair.
        {
            "pair": "BASE1-QUOTE1",
            "source": "SuperDex1",
            "token1_name": "lovelace",
            "token2_name": "cnt_token_1",
            "token1_policy": "",
            "token2_policy": "CNT_POLICY_1",
            "token1_decimals": 6,
            "token2_decimals": 6,
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        # UTxO.
        {
            "tx_hash": "MOCK_HASH_1",
            "tx_index": "MOCK_INDEX_1",
            "amount": 10000000,
            "assets": {
                "CNT_POLICY_1": {"cnt_token_1": 20000000},
            },
        },
        # UTxOs dict..
        {},
        # Expected UTxOs dict.
        {
            "BASE1-QUOTE1": {
                "SuperDex1": {
                    "context": {
                        "address": None,
                        "block_height": None,
                        "epoch": None,
                        "tx_hash": "MOCK_HASH_1",
                        "output_index": "MOCK_INDEX_1",
                        "caller": "save_utxo",
                        "token1_amount": 10000000,
                        "token2_amount": 20000000,
                        "price": 2.0,
                    },
                    "tokens_pair": {
                        "pair": "BASE1-QUOTE1",
                        "source": "SuperDex1",
                        "token1_name": "lovelace",
                        "token2_name": "cnt_token_1",
                        "token1_policy": "",
                        "token2_policy": "CNT_POLICY_1",
                        "token1_decimals": 6,
                        "token2_decimals": 6,
                        "security_token_policy": "unused",
                        "security_token_name": "unused",
                    },
                }
            }
        },
    ),
    (
        # Context.
        utxo_objects.InitialChainContext(
            block_height=None,
            epoch=None,
            address=None,
            tx_hash=None,
            output_index=None,
            utxo_ids=None,
        ),
        # Tokens pair.
        {
            "pair": "BASE1-QUOTE1",
            "source": "SuperDex2",
            "token1_name": "cnt_token_2",
            "token1_policy": "CNT_POLICY_2",
            "token2_policy": "",
            "token2_name": "lovelace",
            "token1_decimals": 6,
            "token2_decimals": 6,
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        # UTxO.
        {
            "tx_hash": "MOCK_HASH_1",
            "tx_index": "MOCK_INDEX_1",
            "amount": 10000000,
            "assets": {
                "CNT_POLICY_2": {"cnt_token_2": 20000000},
            },
        },
        # UTxOs dict.
        {},
        # Expected UTxOs dict.
        {
            "BASE1-QUOTE1": {
                "SuperDex2": {
                    "context": {
                        "address": None,
                        "block_height": None,
                        "epoch": None,
                        "tx_hash": "MOCK_HASH_1",
                        "output_index": "MOCK_INDEX_1",
                        "caller": "save_utxo",
                        "token1_amount": 20000000,
                        "token2_amount": 10000000,
                        "price": 0.5,
                    },
                    "tokens_pair": {
                        "pair": "BASE1-QUOTE1",
                        "source": "SuperDex2",
                        "token1_name": "cnt_token_2",
                        "token1_policy": "CNT_POLICY_2",
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token1_decimals": 6,
                        "token2_decimals": 6,
                        "security_token_policy": "unused",
                        "security_token_name": "unused",
                    },
                }
            }
        },
    ),
    # Cumulative addition to UTXOs Dict.
    (
        # Context.
        utxo_objects.InitialChainContext(
            block_height=None,
            epoch=None,
            address=None,
            tx_hash=None,
            output_index=None,
            utxo_ids=None,
        ),
        # Tokens pair.
        {
            "pair": "BASE1-QUOTE1",
            "source": "SuperDex3",
            "token1_name": "cnt_token_2",
            "token1_policy": "CNT_POLICY_2",
            "token2_policy": "",
            "token2_name": "lovelace",
            "token1_decimals": 6,
            "token2_decimals": 6,
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        # UTxO.
        {
            "tx_hash": "MCOK_HASH_1",
            "tx_index": "MOCK_INDEX_1",
            "amount": 10000000,
            "assets": {
                "CNT_POLICY_2": {"cnt_token_2": 20000000},
            },
        },
        # Expected UTxOs dict.
        {
            "BASE1-QUOTE_UNUSED": {
                "SuperDex_UNUSED": {
                    "context": {
                        "address": None,
                        "block_height": None,
                        "epoch": None,
                        "tx_hash": "MOCK_HASH_1",
                        "output_index": "MOCK_INDEX_1",
                        "caller": "save_utxo",
                        "token1_amount": 20000000,
                        "token2_amount": 10000000,
                        "price": 0.5,
                    },
                    "tokens_pair": {
                        "pair": "BASE1-QUOTE1",
                        "source": "SuperDex3",
                        "token1_name": "cnt_token_2",
                        "token1_policy": "CNT_POLICY_2",
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token1_decimals": 6,
                        "token2_decimals": 6,
                        "security_token_policy": "unused",
                        "security_token_name": "unused",
                    },
                }
            }
        },
        # Expected UTxOs dict.
        {
            "BASE1-QUOTE1": {
                "SuperDex3": {
                    "context": {
                        "address": None,
                        "block_height": None,
                        "epoch": None,
                        "tx_hash": "MCOK_HASH_1",
                        "output_index": "MOCK_INDEX_1",
                        "caller": "save_utxo",
                        "token1_amount": 20000000,
                        "token2_amount": 10000000,
                        "price": 0.5,
                    },
                    "tokens_pair": {
                        "pair": "BASE1-QUOTE1",
                        "source": "SuperDex3",
                        "token1_name": "cnt_token_2",
                        "token1_policy": "CNT_POLICY_2",
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token1_decimals": 6,
                        "token2_decimals": 6,
                        "security_token_policy": "unused",
                        "security_token_name": "unused",
                    },
                }
            },
            "BASE1-QUOTE_UNUSED": {
                "SuperDex_UNUSED": {
                    "context": {
                        "address": None,
                        "block_height": None,
                        "epoch": None,
                        "tx_hash": "MOCK_HASH_1",
                        "output_index": "MOCK_INDEX_1",
                        "caller": "save_utxo",
                        "token1_amount": 20000000,
                        "token2_amount": 10000000,
                        "price": 0.5,
                    },
                    "tokens_pair": {
                        "pair": "BASE1-QUOTE1",
                        "source": "SuperDex3",
                        "token1_name": "cnt_token_2",
                        "token1_policy": "CNT_POLICY_2",
                        "token2_policy": "",
                        "token2_name": "lovelace",
                        "token1_decimals": 6,
                        "token2_decimals": 6,
                        "security_token_policy": "unused",
                        "security_token_name": "unused",
                    },
                }
            },
        },
    ),
]


@pytest.mark.parametrize(
    "chain_context, tokens_pair_dict, utxo, utxos_dict, expected_utxos_dict",
    save_utxo_tests_mock,
)
def test_save_utxo_mock(
    chain_context: utxo_objects.InitialChainContext,
    tokens_pair_dict: dict,
    utxo: dict,
    utxos_dict: dict,
    expected_utxos_dict: dict,
):
    """Test save_utxo with mock data. Both the context and utxos dict
    are updated.
    """

    utxo_copy = copy.deepcopy(utxo)
    context_copy = copy.deepcopy(chain_context)
    utxos_dict_copy = copy.deepcopy(utxos_dict)
    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    res = save_utxo(
        initial_chain_context=chain_context,
        tokens_pair=tokens_pair,
        utxo=utxo,
        utxos_dict=utxos_dict,
    )

    # Function currently returns None.
    assert res is None

    # Remain unchanged by function.
    assert utxo == utxo_copy
    assert chain_context == context_copy

    # Changed by the function.
    assert utxos_dict != utxos_dict_copy
    assert utxos_dict == expected_utxos_dict


save_utxo_tests_mock_trigger_warning = [
    (
        # tokens_pair.
        {
            "pair": "BASE1-QUOTE1",
            "source": "SuperDex1",
            "token1_name": "lovelace",
            "token2_name": "cnt_token_1",
            "token1_policy": "",
            "token2_policy": "CNT_POLICY_1",
            "token1_decimals": 6,
            "token2_decimals": 6,
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        # utxo.
        {
            "tx_hash": "MOCK_HASH_1",
            "tx_index": "MOCK_INDEX_1",
            "amount": 0,
            "assets": {
                "CNT_POLICY_1": {"cnt_token_1": 20000000},
            },
        },
        # utxos_dict.
        {
            "ARBITRARY_KEY": "NOT EDITED 1",
        },
    ),
    (
        # tokens_pair.
        {
            "pair": "BASE1-QUOTE1",
            "source": "SuperDex1",
            "token1_name": "cnt_token_2",
            "token1_policy": "CNT_POLICY_2",
            "token2_policy": "",
            "token2_name": "lovelace",
            "token1_decimals": 6,
            "token2_decimals": 6,
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        # utxo.
        {
            "tx_hash": "MCOK_HASH_1",
            "tx_index": "MOCK_INDEX_1",
            "amount": 10000000,
            "assets": {
                "CNT_POLICY_2": {"cnt_token_2": 0},
            },
        },
        # utxos_dict.
        {
            "ARBITRARY_KEY": "NOT EDITED 2",
        },
    ),
]


@pytest.mark.parametrize(
    "tokens_pair_dict, utxo, utxos_dict",
    save_utxo_tests_mock_trigger_warning,
)
def test_save_utxo_mock_trigger_warning(
    tokens_pair_dict: dict,
    utxo: dict,
    utxos_dict: dict,
):
    """Test save_utxo when one of the token amounts is zero. The context
    is updated but the utxos dict is left untouched.

    NB. This test should be preserved to ensure the integrity of the
    dicts while we pass them into the function. We can delete this
    when we're satisfied we can.
    """

    utxo_copy = copy.deepcopy(utxo)

    # context_copy = copy.deepcopy(chain_context)
    utxos_dict_copy = copy.deepcopy(utxos_dict)

    # Context mapped into a UTxO object so that the intentions
    # of the test are clearer during the refactor.
    unused_context = utxo_objects.InitialChainContext(
        block_height=None,
        epoch=None,
        address=None,
        tx_hash=None,
        output_index=None,
        utxo_ids=None,
    )

    unused_context_copy = copy.deepcopy(unused_context)

    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)

    res = save_utxo(
        initial_chain_context=unused_context,
        tokens_pair=tokens_pair,
        utxo=utxo,
        utxos_dict=utxos_dict,
    )

    # Remain unchanged by function and function does not return a
    # value..
    assert res is None
    assert utxo == utxo_copy
    assert utxos_dict == utxos_dict_copy
    assert unused_context == unused_context_copy


update_utxo_dict_tests = [
    # Data is not in UTXOs Dict.
    (
        # arbitrary context. it is only included because it is added
        # to the utxo information via this test but its properties
        # are only tested for consistency. This follows for all
        # tests elow.
        utxo_objects.UTxOUpdateContext(
            address="123",
            epoch=123,
            block_height=234,
            tx_hash="abc123arbitraryContextInfo",
            output_index="1",
            caller="save_utxo",
            token_1_amount=1000000,
            token_2_amount=20000000,
            price=20,
            utxo_ids=None,
        ),
        # tokens_pair.
        {
            "pair": "base-quote",
            "source": "SuperDex1",
            "token1_policy": "unused",
            "token1_name": "unused",
            "token1_decimals": "unused",
            "token2_policy": "unused",
            "token2_name": "unused",
            "token2_decimals": "unused",
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        # utxos_dict.
        {},
        # utxos_dict expected.
        {
            "base-quote": {
                "SuperDex1": {
                    "context": {
                        "address": "123",
                        "block_height": 234,
                        "caller": "save_utxo",
                        "epoch": 123,
                        "output_index": "1",
                        "price": 20,
                        "token1_amount": 1000000,
                        "token2_amount": 20000000,
                        "tx_hash": "abc123arbitraryContextInfo",
                    },
                    "tokens_pair": {
                        "pair": "base-quote",
                        "source": "SuperDex1",
                        "security_token_name": "unused",
                        "security_token_policy": "unused",
                        "token1_decimals": "unused",
                        "token1_name": "unused",
                        "token1_policy": "unused",
                        "token2_decimals": "unused",
                        "token2_name": "unused",
                        "token2_policy": "unused",
                    },
                }
            }
        },
        # deepdiff expected.
        {"dictionary_item_added": ["root['base-quote']"]},
    ),
    # Data is in UTXOs dict.
    (
        # context.
        utxo_objects.UTxOUpdateContext(
            address="123456",
            epoch=123456,
            block_height=23456,
            tx_hash="abc123arbitraryContextInfo",
            output_index="1",
            caller="save_utxo",
            token_1_amount=1000000,
            token_2_amount=20000000,
            price=20,
            utxo_ids=None,
        ),
        # tokens_pair.
        {
            "pair": "base-quote",
            "source": "SuperDex2",
            "token1_policy": "unused",
            "token1_name": "unused",
            "token1_decimals": "unused",
            "token2_policy": "unused",
            "token2_name": "unused",
            "token2_decimals": "unused",
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        # utxos_dict.
        {
            "base-quote": {
                "SuperDex1": {
                    "context": {"arbitrary_context": "12345"},
                    "tokens_pair": {"pair": "base-quote", "source": "SuperDex1"},
                }
            }
        },
        # utxos_dict_expected.
        {
            "base-quote": {
                "SuperDex1": {
                    "context": {"arbitrary_context": "12345"},
                    "tokens_pair": {"pair": "base-quote", "source": "SuperDex1"},
                },
                "SuperDex2": {
                    "context": {
                        "address": "123456",
                        "block_height": 23456,
                        "caller": "save_utxo",
                        "epoch": 123456,
                        "output_index": "1",
                        "price": 20,
                        "token1_amount": 1000000,
                        "token2_amount": 20000000,
                        "tx_hash": "abc123arbitraryContextInfo",
                    },
                    "tokens_pair": {
                        "pair": "base-quote",
                        "source": "SuperDex2",
                        "token1_policy": "unused",
                        "token1_name": "unused",
                        "token1_decimals": "unused",
                        "token2_policy": "unused",
                        "token2_name": "unused",
                        "token2_decimals": "unused",
                        "security_token_policy": "unused",
                        "security_token_name": "unused",
                    },
                },
            }
        },
        # deepdiff expected.
        {"dictionary_item_added": ["root['base-quote']['SuperDex2']"]},
    ),
    # Source not in utxos dict.
    (
        # context.
        utxo_objects.UTxOUpdateContext(
            address="addr3121",
            epoch=3121,
            block_height=3121,
            tx_hash="abc123arbitraryContextInfo",
            output_index="1",
            caller="save_utxo",
            token_1_amount=20000000,
            token_2_amount=30000000,
            price=0.2,
            utxo_ids=None,
        ),
        # tokens_pair.
        {
            "pair": "base-quote",
            "source": "SuperDex2",
            "token1_policy": "unused",
            "token1_name": "unused",
            "token1_decimals": "unused",
            "token2_policy": "unused",
            "token2_name": "unused",
            "token2_decimals": "unused",
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        # utxos_dict.
        {
            "base-quote": {
                "SuperDex1": {
                    "context": {
                        "arbitrary_context": "12345",
                    },
                    "tokens_pair": {"pair": "base-quote", "source": "SuperDex1"},
                },
                "SuperDex2": {
                    "context": {
                        "arbitrary_context": "12345",
                        "more_context": 12345,
                        "token1_amount": 10000000,
                        "token2_amount": 20000000,
                    },
                    "tokens_pair": {
                        "pair": "base-quote",
                        "token1_policy": "unused",
                        "token1_name": "unused",
                        "token1_decimals": "unused",
                        "token2_policy": "unused",
                        "token2_name": "unused",
                        "token2_decimals": "unused",
                        "security_token_policy": "unused",
                        "security_token_name": "unused",
                    },
                },
            }
        },
        # utxos_dict_expected.
        {
            "base-quote": {
                "SuperDex1": {
                    "context": {"arbitrary_context": "12345"},
                    "tokens_pair": {"pair": "base-quote", "source": "SuperDex1"},
                },
                "SuperDex2": {
                    "context": {
                        "address": "addr3121",
                        "block_height": 3121,
                        "caller": "save_utxo",
                        "epoch": 3121,
                        "output_index": "1",
                        "price": 0.2,
                        "token1_amount": 20000000,
                        "token2_amount": 30000000,
                        "tx_hash": "abc123arbitraryContextInfo",
                    },
                    "tokens_pair": {
                        "pair": "base-quote",
                        "source": "SuperDex2",
                        "token1_policy": "unused",
                        "token1_name": "unused",
                        "token1_decimals": "unused",
                        "token2_policy": "unused",
                        "token2_name": "unused",
                        "token2_decimals": "unused",
                        "security_token_policy": "unused",
                        "security_token_name": "unused",
                    },
                },
            }
        },
        # deepdiff expected.
        {
            "dictionary_item_added": [
                "root['base-quote']['SuperDex2']['tokens_pair']['source']"
            ],
            "values_changed": {
                "root['base-quote']['SuperDex2']['context']": {
                    "new_value": {
                        "block_height": 3121,
                        "epoch": 3121,
                        "address": "addr3121",
                        "tx_hash": "abc123arbitraryContextInfo",
                        "output_index": "1",
                        "caller": "save_utxo",
                        "token1_amount": 20000000,
                        "token2_amount": 30000000,
                        "price": 0.2,
                    },
                    "old_value": {
                        "arbitrary_context": "12345",
                        "more_context": 12345,
                        "token1_amount": 10000000,
                        "token2_amount": 20000000,
                    },
                }
            },
        },
    ),
]


@pytest.mark.parametrize(
    "context, tokens_pair_dict, utxos_dict, utxos_dict_expected, deepdiff_expected",
    update_utxo_dict_tests,
)
def test_utxos_dict_updated(
    context: utxo_objects.UTxOUpdateContext,
    tokens_pair_dict: dict,
    utxos_dict: dict,
    utxos_dict_expected: dict,
    deepdiff_expected: dict,
):
    """Use mock data to test update of the utxos dict via
    utxos_dict_update.

    Tests are designed to always update the utxos_dictionary and
    make sure the others remain unchanged. Tests are separated from
    non-updates to provide as much confidence as possible all
    pathways are affected.
    """

    utxos_dict_copy = copy.deepcopy(utxos_dict)
    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    res = utxos_dict_update(
        utxo_update_context=context,
        utxos_dict=utxos_dict,
        tokens_pair=tokens_pair,
    )
    assert res is None
    assert utxos_dict_copy != utxos_dict
    assert utxos_dict == utxos_dict_expected
    # Very defensive tests to make sure data we expect to change is
    # correctly changed.
    comp = DeepDiff(utxos_dict_copy, utxos_dict)
    assert comp == deepdiff_expected
    comp = DeepDiff(utxos_dict_copy, utxos_dict_expected)
    assert comp == deepdiff_expected


non_update_utxo_dict_tests = [
    # Dict is not updated.
    (
        utxo_objects.UTxOUpdateContext(
            address="",
            epoch="",
            block_height="",
            tx_hash="",
            output_index="",
            caller="save_utxo",
            token_1_amount=10000000,
            token_2_amount=20000000,
            utxo_ids=[],
        ),
        {
            "pair": "base-quote",
            "source": "SuperDex2",
            "token1_name": "unused",
            "token2_name": "unused",
            "token1_policy": "unused",
            "token2_policy": "unused",
            "token1_decimals": 0,
            "token2_decimals": 0,
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        {
            "base-quote": {
                "SuperDex1": {
                    "context": {
                        "arbitrary_context": "12345",
                    },
                    "tokens_pair": {"pair": "base-quote", "source": "SuperDex1"},
                },
                "SuperDex2": {
                    "context": {
                        "arbitrary_context": "12345",
                        "more_context": 12345,
                        "token1_amount": 10000000,
                        "token2_amount": 20000000,
                    },
                    "tokens_pair": {
                        "pair": "base-quote",
                        "source": "SuperDex2",
                        "additional": "data",
                    },
                },
            }
        },
    ),
    # Dict is not updated 2.
    (
        utxo_objects.UTxOUpdateContext(
            address="",
            epoch="",
            block_height="",
            tx_hash="",
            output_index="",
            caller="save_utxo",
            token_1_amount=10000000,
            token_2_amount=20000000,
            utxo_ids=[],
        ),
        {
            "pair": "base-quote",
            "source": "SuperDex2",
            "token1_name": "unused",
            "token2_name": "unused",
            "token1_policy": "unused",
            "token2_policy": "unused",
            "token1_decimals": 0,
            "token2_decimals": 0,
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        {
            "base-quote": {
                "SuperDex1": {
                    "context": {
                        "arbitrary_context": "12345",
                    },
                    "tokens_pair": {"pair": "base-quote", "source": "SuperDex1"},
                },
                "SuperDex2": {
                    "context": {
                        "arbitrary_context": "12345",
                        "more_context": 12345,
                        "token1_amount": 10000000,
                        "token2_amount": 20000000,
                    },
                    "tokens_pair": {
                        "pair": "base-quote",
                        "source": "SuperDex2",
                        "additional": "data",
                    },
                },
            }
        },
    ),
]


@pytest.mark.parametrize(
    "context, tokens_pair_dict, utxos_dict", non_update_utxo_dict_tests
)
def test_utxos_dict_not_updated(
    context: utxo_objects.UTxOUpdateContext,
    tokens_pair_dict: dict,
    utxos_dict: dict,
):
    """Use mock data to test utxos_dict_update.

    Tests are designed so that they do not trigger an update. This
    is to provide confidence that the function really isn't changing
    anything.
    """
    context_dict_copy = copy.deepcopy(context)
    utxos_dict_copy = copy.deepcopy(utxos_dict)
    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    utxos_dict_update(
        utxo_update_context=context,
        utxos_dict=utxos_dict,
        tokens_pair=tokens_pair,
    )
    assert context == context_dict_copy
    assert utxos_dict == utxos_dict_copy
