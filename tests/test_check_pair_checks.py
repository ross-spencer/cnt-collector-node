"""Various smaller tests for check pair functions."""

# pylint: disable=C0302,R0913

import copy
import datetime
import sqlite3
from datetime import timezone
from typing import Any

import pytest
import pytest_mock
import time_machine

from src.cnt_collector_node import config
from src.cnt_collector_node import global_helpers as helpers
from src.cnt_collector_node import load_pairs, utxo_objects
from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.helper_functions import (
    _validate_min_ada,
    _validate_non_ada_cnt_base_and_quote,
    check_if_configured_pair,
    check_tokens_pair,
    check_utxo_for_tokens_pair,
    save_utxo,
)

check_configured_pair_tests = [
    (
        utxo_objects.InitialChainContext(
            block_height=170096500,
            epoch=591,
            address="addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
            tx_hash=None,
            output_index=None,
            utxo_ids=None,
        ),
        {
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
        {
            "tx_hash": "be82097a4113fa59a0183476d65f9df22cb9640bbe01a372d610c312c397d252",
            "tx_index": 0,
            "amount": 113452869,
            "assets": {
                "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
                    "116df62938bc100b55c0e72b57a48dced2f928635ad66660bc165a8f40f8e735": 2897537
                },
                "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad": {
                    "0014df105553444d": 78396090
                },
                "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                    "4d494e53574150": 1
                },
                "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                    "116df62938bc100b55c0e72b57a48dced2f928635ad66660bc165a8f40f8e735": 1
                },
            },
        },
        {
            "CBLP-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 170096500,
                        "epoch": 591,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "58466346f8778f756dda25f5056c2b52e67e2732f0b4857c6e5b2a9287ee5fc5",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 1319373299185,
                        "token2_amount": 447421434,
                        "price": 0.0003391166353573928,
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
        },
        {
            "CBLP-ADA": {
                "MinSwap": {
                    "context": {
                        "block_height": 170096500,
                        "epoch": 591,
                        "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                        "tx_hash": "58466346f8778f756dda25f5056c2b52e67e2732f0b4857c6e5b2a9287ee5fc5",
                        "output_index": 0,
                        "caller": "save_utxo",
                        "token1_amount": 1319373299185,
                        "token2_amount": 447421434,
                        "price": 0.0003391166353573928,
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
                        "block_height": 170096500,
                        "epoch": 591,
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
        },
    ),
    (
        utxo_objects.InitialChainContext(
            block_height=170096500,
            epoch=591,
            address="addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
            tx_hash=None,
            output_index=None,
            utxo_ids=None,
        ),
        {
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
        {
            "tx_hash": "897c3c4ea64542238aa85c79ed5844a5339f7156f2b8ac01bccf52a69dd22e07",
            "tx_index": 0,
            "amount": 404595751,
            "assets": {
                "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
                    "b4ba2b47edce71234f328fa20efdb25c3f96e348ca19a683193880489bb368db": 181409857
                },
                "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e51": {
                    "6f7263666178746f6b656e": 91061757796
                },
                "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                    "4d494e53574150": 1
                },
                "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                    "b4ba2b47edce71234f328fa20efdb25c3f96e348ca19a683193880489bb368db": 1
                },
            },
        },
        {
            "ADA-USDM": {
                "MinSwap": {
                    "context": {
                        "block_height": 170096500,
                        "epoch": 591,
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
        },
        {
            "ADA-USDM": {
                "MinSwap": {
                    "context": {
                        "block_height": 170096500,
                        "epoch": 591,
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
                        "block_height": 170096500,
                        "epoch": 591,
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


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "chain_context, tokens_pair_dict, utxo, utxos_dict, expected_utxos_dict",
    check_configured_pair_tests,
)
def test_if_configured_pair(
    mocker: pytest_mock.MockerFixture,
    chain_context: utxo_objects.InitialChainContext,
    tokens_pair_dict: dict,
    utxo: dict,
    utxos_dict: dict,
    expected_utxos_dict: dict,
):
    """Test if configured pair.

    A database connection is fed through these functions but it isn't
    required as the calls currently update the dictionaries provided.
    """

    chain_context_copy = copy.deepcopy(chain_context)
    utxo_copy = copy.deepcopy(utxo)
    utxos_dict_copy = copy.deepcopy(utxos_dict)

    save_utxo_mock = mocker.patch(
        "src.cnt_collector_node.helper_functions.save_utxo", wraps=save_utxo
    )

    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    check_if_configured_pair(
        initial_chain_context=chain_context,
        tokens_pair=tokens_pair,
        utxo=utxo,
        utxos_dict=utxos_dict,
    )

    save_utxo_mock.assert_called()

    save_utxo_mock.assert_called_with(
        initial_chain_context=chain_context,
        tokens_pair=tokens_pair,
        utxo=utxo_copy,
        utxos_dict=expected_utxos_dict,
    )

    assert chain_context == chain_context_copy
    assert utxo == utxo_copy

    # Changed by the function.
    assert utxos_dict != utxos_dict_copy
    assert utxos_dict == expected_utxos_dict


check_configured_pair_tests_no_match = [
    # Triggers no match at: `if asset_name == security_token_name:`
    (
        # tokens_pairs.
        {
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
        # utxo.
        {
            "tx_hash": "897c3c4ea64542238aa85c79ed5844a5339f7156f2b8ac01bccf52a69dd22e07",
            "tx_index": 0,
            "amount": 404595751,
            "assets": {
                "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
                    "b4ba2b47edce71234f328fa20efdb25c3f96e348ca19a683193880489bb368db": 181409857
                },
                "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e51": {
                    "NOMATCH": 91061757796
                },
                "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                    "4d494e53574150": 1
                },
                "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                    "b4ba2b47edce71234f328fa20efdb25c3f96e348ca19a683193880489bb368db": 1
                },
            },
        },
        # utxos_dict.
        {
            "ADA-USDM": {
                "MinSwap": {
                    "context": {
                        "db_name": "",
                        "block_height": 170096500,
                        "epoch": 591,
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
        },
    ),
    # Triggers no match at; `if policy_id == security_token_policy:`
    (
        # tokens_pair.
        {
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
        # utxo.
        {
            "tx_hash": "897c3c4ea64542238aa85c79ed5844a5339f7156f2b8ac01bccf52a69dd22e07",
            "tx_index": 0,
            "amount": 404595751,
            "assets": {
                "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
                    "b4ba2b47edce71234f328fa20efdb25c3f96e348ca19a683193880489bb368db": 181409857
                },
                "a3931691f5c4e65d01c429e473d0dd24c51afdb6daf88e632a6c1e51": {
                    "6f7263666178746f6b656e": 91061757796
                },
                "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                    "4d494e53574150": 1
                },
                "NOMATCH": {
                    "b4ba2b47edce71234f328fa20efdb25c3f96e348ca19a683193880489bb368db": 1
                },
            },
        },
        # utxos_dict.
        {
            "ADA-USDM": {
                "MinSwap": {
                    "context": {
                        "db_name": "",
                        "block_height": 170096500,
                        "epoch": 591,
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
        },
    ),
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "tokens_pair_dict, utxo, utxos_dict",
    check_configured_pair_tests_no_match,
)
def test_if_configured_pair_no_match(
    mocker: pytest_mock.plugin.MockerFixture,
    tokens_pair_dict: dict,
    utxo: dict,
    utxos_dict: dict,
):
    """Test if configured pair.

    A database connection is fed through these functions but it isn't
    required as the calls currently update the dictionaries provided.
    """

    utxo_copy = copy.deepcopy(utxo)
    utxos_dict_copy = copy.deepcopy(utxos_dict)

    save_utxo_mock = mocker.patch("src.cnt_collector_node.helper_functions.save_utxo")

    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    check_if_configured_pair(
        initial_chain_context=None,
        tokens_pair=tokens_pair,
        utxo=utxo,
        utxos_dict=utxos_dict,
    )

    save_utxo_mock.assert_not_called()

    assert utxo == utxo_copy

    # Changed by the function.
    assert utxos_dict == utxos_dict_copy


test_node_id = {
    "node_id": "UUIDV4",
    "location": {
        "ip": "IP_ADDR",
        "city": "STADT",
        "region": "WORLD",
        "country": "LAND",
        "loc": "LOC",
        "org": "PROVIDER",
        "postal": "80210",
        "timezone": "TheGlobe",
        "readme": "https://ipinfo.io/",
    },
}


check_tokens_pair_tests = [
    (
        {
            "name": "ADA-USDM",
            "token1_policy": "",
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_policy": "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad",
            "token2_name": "0014df105553444d",
            "token2_decimals": 6,
            "sources": [
                {
                    "source": "MinSwap",
                    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                    "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                    "security_token_name": "116df62938bc100b55c0e72b57a48dced2f928635ad66660bc165a8f40f8e735",
                },
                {
                    "source": "MinSwapV2",
                    "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                    "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                    "security_token_name": "4d5350",
                },
                {
                    "source": "SundaeSwapV3",
                    "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
                    "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
                    "security_token_name": "000de14064f35d26b237ad58e099041bc14c687ea7fdc58969d7d5b66e2540ef",
                },
                {
                    "source": "WingRiders",
                    "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaytmskjm3nhaazcv20yhs07hel96yv29zuf0gzlk5dt9ugzs2y5pq5",
                    "security_token_policy": "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
                    "security_token_name": "4c",
                },
                {
                    "source": "WingRidersV2",
                    "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqlm3e807762pklheldndtjhrk0qxzzfh9vhc9kkc706xglsv8s5nq",
                    "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
                    "security_token_name": "4c",
                },
                {
                    "source": "SundaeSwap",
                    "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
                    "security_token_policy": "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913",
                    "security_token_name": "70204f05",
                },
                {
                    "source": "Spectrum",
                    "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
                    "security_token_policy": "fd0b614f52f2286df3b4db4fc70656bcd3df4877e909fd5a44e956f0",
                    "security_token_name": "0014efbfbd105553444d5f4144415f4e4654",
                },
            ],
        },
        [
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "0014df105553444d",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "MinSwap",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                "feed": "ADA-USDM",
                "utxo": "be82097a4113fa59a0183476d65f9df22cb9640bbe01a372d610c312c397d252#0",
                "token1_volume": 113.452869,
                "token2_volume": 78.39609,
                "price": 0.691001388426766,
                "amounts": {
                    "lovelace": 113452869,
                    "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 78396090,
                },
            },
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "0014df105553444d",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "SundaeSwapV3",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
                "feed": "ADA-USDM",
                "utxo": "2a495dd777df9661cffb510477eedbf3aa74fb74b447b023fba6c94c6b9ce7b0#0",
                "token1_volume": 1129920.892954,
                "token2_volume": 758489.624983,
                "price": 0.6712767501803143,
                "amounts": {
                    "lovelace": 1129920892954,
                    "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 758489624983,
                },
            },
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "0014df105553444d",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "WingRiders",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaytmskjm3nhaazcv20yhs07hel96yv29zuf0gzlk5dt9ugzs2y5pq5",
                "feed": "ADA-USDM",
                "utxo": "4e202b4dba2665ff5e1f022819c8c8de83c42cfee3ab4bf259ed2f122446420e#0",
                "token1_volume": 500.316158,
                "token2_volume": 338.308714,
                "price": 0.6761898623310103,
                "amounts": {
                    "lovelace": 500316158,
                    "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 338308714,
                },
            },
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "0014df105553444d",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "WingRidersV2",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqlm3e807762pklheldndtjhrk0qxzzfh9vhc9kkc706xglsv8s5nq",
                "feed": "ADA-USDM",
                "utxo": "f4770419a3e0f5de2569cb7245219b58bbf40b2aaaa2cfbfd156261dafbe3e96#0",
                "token1_volume": 334366.001622,
                "token2_volume": 224367.012482,
                "price": 0.6710222073823354,
                "amounts": {
                    "lovelace": 334366001622,
                    "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 224367012482,
                },
            },
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "0014df105553444d",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "SundaeSwap",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
                "feed": "ADA-USDM",
                "utxo": "7fbf9ba38cead768c5b544bdd19f81ca23c871b746c171d89927c71377845e42#0",
                "token1_volume": 2.107464,
                "token2_volume": 0.001129,
                "price": 0.0005357149635770766,
                "amounts": {
                    "lovelace": 2107464,
                    "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 1129,
                },
            },
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "0014df105553444d",
                "token2_decimals": 6,
                "block_height": 170112113,
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
        ],
        {
            "timestamp": "2018-02-19T12:55:00Z",
            "raw": [
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "0014df105553444d",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "MinSwap",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                    "feed": "ADA-USDM",
                    "utxo": "be82097a4113fa59a0183476d65f9df22cb9640bbe01a372d610c312c397d252#0",
                    "token1_volume": 113.452869,
                    "token2_volume": 78.39609,
                    "price": 0.691001388426766,
                    "amounts": {
                        "lovelace": 113452869,
                        "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 78396090,
                    },
                },
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "0014df105553444d",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "SundaeSwapV3",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
                    "feed": "ADA-USDM",
                    "utxo": "2a495dd777df9661cffb510477eedbf3aa74fb74b447b023fba6c94c6b9ce7b0#0",
                    "token1_volume": 1129920.892954,
                    "token2_volume": 758489.624983,
                    "price": 0.6712767501803143,
                    "amounts": {
                        "lovelace": 1129920892954,
                        "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 758489624983,
                    },
                },
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "0014df105553444d",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "WingRiders",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaytmskjm3nhaazcv20yhs07hel96yv29zuf0gzlk5dt9ugzs2y5pq5",
                    "feed": "ADA-USDM",
                    "utxo": "4e202b4dba2665ff5e1f022819c8c8de83c42cfee3ab4bf259ed2f122446420e#0",
                    "token1_volume": 500.316158,
                    "token2_volume": 338.308714,
                    "price": 0.6761898623310103,
                    "amounts": {
                        "lovelace": 500316158,
                        "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 338308714,
                    },
                },
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "0014df105553444d",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "WingRidersV2",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqlm3e807762pklheldndtjhrk0qxzzfh9vhc9kkc706xglsv8s5nq",
                    "feed": "ADA-USDM",
                    "utxo": "f4770419a3e0f5de2569cb7245219b58bbf40b2aaaa2cfbfd156261dafbe3e96#0",
                    "token1_volume": 334366.001622,
                    "token2_volume": 224367.012482,
                    "price": 0.6710222073823354,
                    "amounts": {
                        "lovelace": 334366001622,
                        "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 224367012482,
                    },
                },
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "0014df105553444d",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "SundaeSwap",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
                    "feed": "ADA-USDM",
                    "utxo": "7fbf9ba38cead768c5b544bdd19f81ca23c871b746c171d89927c71377845e42#0",
                    "token1_volume": 2.107464,
                    "token2_volume": 0.001129,
                    "price": 0.0005357149635770766,
                    "amounts": {
                        "lovelace": 2107464,
                        "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad.0014df105553444d": 1129,
                    },
                },
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "0014df105553444d",
                    "token2_decimals": 6,
                    "block_height": 170112113,
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
            ],
            "data_points": [
                [
                    113.452869,
                    1129920.892954,
                    500.316158,
                    334366.001622,
                    2.107464,
                    15.180126,
                ],
                [
                    78.39609,
                    758489.624983,
                    338.308714,
                    224367.012482,
                    0.001129,
                    10.127849,
                ],
            ],
            "calculated_value": "0.6712208492265617",
            "feed": "ADA-USDM",
            "identity": copy.deepcopy(test_node_id),
            "content_signature": "b93cb75a7e448bf5ccfbe3278fbe2a0e670e31bb12a85f1eabc3efa0d317dca8",
            "errors": [],
        },
        "2018-02-19T12:55:00Z",
    ),
    (
        {
            "name": "ADA-iUSD",
            "token1_policy": "",
            "token1_name": "lovelace",
            "token1_decimals": 6,
            "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
            "token2_name": "69555344",
            "token2_decimals": 6,
            "sources": [
                {
                    "source": "MinSwap",
                    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                    "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                    "security_token_name": "8fde43a3f0b9f0e6f63bec7335e0b855c6b62a4dc51f1b762ccb6dfbbafcfe47",
                },
                {
                    "source": "MinSwapV2",
                    "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                    "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                    "security_token_name": "4d5350",
                },
                {
                    "source": "SundaeSwapV3",
                    "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
                    "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
                    "security_token_name": "000de140c7ef237f227542a0c8930d37911491c56a341fdef8437e0f21d024f8",
                },
                {
                    "source": "WingRiders",
                    "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaytpq3ryg76qvca5eu9c6py33ncg8zf09nh7gy2cvdps2yeqlvvkfh",
                    "security_token_policy": "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
                    "security_token_name": "4c",
                },
                {
                    "source": "WingRidersV2",
                    "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhq7nzm5jzjeevh40p5h3f682mv3r6fnnsldx749n52asr6vsnevx3j",
                    "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
                    "security_token_name": "4c",
                },
                {
                    "source": "Spectrum",
                    "address": "addr1x8nz307k3sr60gu0e47cmajssy4fmld7u493a4xztjrll0aj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrswgxsta",
                    "security_token_policy": "e36480a99003832c2a4dd7b9919915e5c9b5b00244117e5f5ece009d",
                    "security_token_name": "697573645f4144415f4e4654",
                },
            ],
        },
        [
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "69555344",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "MinSwap",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                "feed": "ADA-iUSD",
                "utxo": "be53cdb9c2a12c4d82e65e9044e3628854212d700cd928718b08d1a643d7b029#0",
                "token1_volume": 9374.981421,
                "token2_volume": 6350.396891,
                "price": 0.6773770107719982,
                "amounts": {
                    "lovelace": 9374981421,
                    "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 6350396891,
                },
            },
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "69555344",
                "token2_decimals": 6,
                "block_height": 170112113,
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
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "69555344",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "WingRiders",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaytpq3ryg76qvca5eu9c6py33ncg8zf09nh7gy2cvdps2yeqlvvkfh",
                "feed": "ADA-iUSD",
                "utxo": "a19d1d3c09cb1f5a2e30c3b8bfb9d64d86d6dff9714f54bfc33b6a82f1ddf748#0",
                "token1_volume": 3532.63402,
                "token2_volume": 2387.64466,
                "price": 0.6758822585307039,
                "amounts": {
                    "lovelace": 3532634020,
                    "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 2387644660,
                },
            },
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "69555344",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "WingRidersV2",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhq7nzm5jzjeevh40p5h3f682mv3r6fnnsldx749n52asr6vsnevx3j",
                "feed": "ADA-iUSD",
                "utxo": "beaba8251508bab635704f11e3ee3843939642bbed899ee1f7aab24f9224e6ac#0",
                "token1_volume": 2501.148298,
                "token2_volume": 1687.243035,
                "price": 0.6745873630720636,
                "amounts": {
                    "lovelace": 2501148298,
                    "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 1687243035,
                },
            },
            {
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_name": "69555344",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "Spectrum",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1x8nz307k3sr60gu0e47cmajssy4fmld7u493a4xztjrll0aj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrswgxsta",
                "feed": "ADA-iUSD",
                "utxo": "dcc931860b23dffc2f2e25e0533cfe1f756792eb84462ffc3c2fe4072b614129#0",
                "token1_volume": 150.000001,
                "token2_volume": 46.0828,
                "price": 0.30721866461854225,
                "amounts": {
                    "lovelace": 150000001,
                    "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 46082800,
                },
            },
        ],
        {
            "timestamp": "2018-02-19T12:55:00Z",
            "raw": [
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "69555344",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "MinSwap",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                    "feed": "ADA-iUSD",
                    "utxo": "be53cdb9c2a12c4d82e65e9044e3628854212d700cd928718b08d1a643d7b029#0",
                    "token1_volume": 9374.981421,
                    "token2_volume": 6350.396891,
                    "price": 0.6773770107719982,
                    "amounts": {
                        "lovelace": 9374981421,
                        "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 6350396891,
                    },
                },
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "69555344",
                    "token2_decimals": 6,
                    "block_height": 170112113,
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
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "69555344",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "WingRiders",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaytpq3ryg76qvca5eu9c6py33ncg8zf09nh7gy2cvdps2yeqlvvkfh",
                    "feed": "ADA-iUSD",
                    "utxo": "a19d1d3c09cb1f5a2e30c3b8bfb9d64d86d6dff9714f54bfc33b6a82f1ddf748#0",
                    "token1_volume": 3532.63402,
                    "token2_volume": 2387.64466,
                    "price": 0.6758822585307039,
                    "amounts": {
                        "lovelace": 3532634020,
                        "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 2387644660,
                    },
                },
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "69555344",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "WingRidersV2",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhq7nzm5jzjeevh40p5h3f682mv3r6fnnsldx749n52asr6vsnevx3j",
                    "feed": "ADA-iUSD",
                    "utxo": "beaba8251508bab635704f11e3ee3843939642bbed899ee1f7aab24f9224e6ac#0",
                    "token1_volume": 2501.148298,
                    "token2_volume": 1687.243035,
                    "price": 0.6745873630720636,
                    "amounts": {
                        "lovelace": 2501148298,
                        "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 1687243035,
                    },
                },
                {
                    "token1_name": "lovelace",
                    "token1_decimals": 6,
                    "token2_name": "69555344",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "Spectrum",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1x8nz307k3sr60gu0e47cmajssy4fmld7u493a4xztjrll0aj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrswgxsta",
                    "feed": "ADA-iUSD",
                    "utxo": "dcc931860b23dffc2f2e25e0533cfe1f756792eb84462ffc3c2fe4072b614129#0",
                    "token1_volume": 150.000001,
                    "token2_volume": 46.0828,
                    "price": 0.30721866461854225,
                    "amounts": {
                        "lovelace": 150000001,
                        "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69555344": 46082800,
                    },
                },
            ],
            "data_points": [
                [9374.981421, 23813.086544, 3532.63402, 2501.148298, 150.000001],
                [6350.396891, 15887.438632, 2387.64466, 1687.243035, 46.0828],
            ],
            "calculated_value": "0.6694835479629906",
            "feed": "ADA-iUSD",
            "identity": copy.deepcopy(test_node_id),
            "content_signature": "98d52385b381a44eec418a8e8a633f09574fc3ff1a8b1f580931e46c22368a4f",
            "errors": [],
        },
        "2018-02-19T12:55:00Z",
    ),
    (
        {
            "name": "CBLP-ADA",
            "token1_policy": "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd",
            "token1_name": "43424c50",
            "token1_decimals": 6,
            "token2_policy": "",
            "token2_name": "lovelace",
            "token2_decimals": 6,
            "sources": [
                {
                    "source": "MinSwap",
                    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                    "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                    "security_token_name": "dfe1be4e42a1cf6a8f5648e904bef0b4b11ee8ca4131521b5256856ef34e3486",
                },
                {
                    "source": "MinSwapV2",
                    "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                    "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                    "security_token_name": "4d5350",
                },
                {
                    "source": "SundaeSwapV3",
                    "address": "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
                    "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
                    "security_token_name": "000de140549bd196264a186d09e00bcfd41727622e515154612f76dc6b8120b9",
                },
                {
                    "source": "Spectrum",
                    "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
                    "security_token_policy": "1e707f0a237ccf47bc9eb02184442459761ab0a0096eed2b27a2f2d9",
                    "security_token_name": "43424c505f4144415f4e4654",
                },
            ],
        },
        [
            {
                "token1_name": "43424c50",
                "token1_decimals": 6,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "MinSwap",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                "feed": "CBLP-ADA",
                "utxo": "58466346f8778f756dda25f5056c2b52e67e2732f0b4857c6e5b2a9287ee5fc5#0",
                "token1_volume": 1319373.299185,
                "token2_volume": 447.421434,
                "price": 0.0003391166353573928,
                "amounts": {
                    "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd.43424c50": 1319373299185,
                    "lovelace": 447421434,
                },
            },
            {
                "token1_name": "43424c50",
                "token1_decimals": 6,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112113,
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
            {
                "token1_name": "43424c50",
                "token1_decimals": 6,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112113,
                "source": "Spectrum",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
                "feed": "CBLP-ADA",
                "utxo": "1c4fce9ef701f5841e8734cd6ccb282c87fa7af1add3be271b867aed451981e4#0",
                "token1_volume": 1126.795042,
                "token2_volume": 7.531517,
                "price": 0.006684016808089576,
                "amounts": {
                    "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd.43424c50": 1126795042,
                    "lovelace": 7531517,
                },
            },
        ],
        {
            "timestamp": "2018-02-19T12:55:00Z",
            "raw": [
                {
                    "token1_name": "43424c50",
                    "token1_decimals": 6,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "MinSwap",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                    "feed": "CBLP-ADA",
                    "utxo": "58466346f8778f756dda25f5056c2b52e67e2732f0b4857c6e5b2a9287ee5fc5#0",
                    "token1_volume": 1319373.299185,
                    "token2_volume": 447.421434,
                    "price": 0.0003391166353573928,
                    "amounts": {
                        "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd.43424c50": 1319373299185,
                        "lovelace": 447421434,
                    },
                },
                {
                    "token1_name": "43424c50",
                    "token1_decimals": 6,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112113,
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
                {
                    "token1_name": "43424c50",
                    "token1_decimals": 6,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112113,
                    "source": "Spectrum",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
                    "feed": "CBLP-ADA",
                    "utxo": "1c4fce9ef701f5841e8734cd6ccb282c87fa7af1add3be271b867aed451981e4#0",
                    "token1_volume": 1126.795042,
                    "token2_volume": 7.531517,
                    "price": 0.006684016808089576,
                    "amounts": {
                        "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd.43424c50": 1126795042,
                        "lovelace": 7531517,
                    },
                },
            ],
            "data_points": [
                [1319373.299185, 289.10765, 1126.795042],
                [447.421434, 44.725475, 7.531517],
            ],
            "calculated_value": "0.0003783180732321986",
            "feed": "CBLP-ADA",
            "identity": copy.deepcopy(test_node_id),
            "content_signature": "e05c36b97499ced8f8501d5c968b6f9e8d9777e808e9006bc7720d699f3508e5",
            "errors": [],
        },
        "2018-02-19T12:55:00Z",
    ),
    (
        {
            "name": "SNEK-ADA",
            "token1_policy": "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
            "token1_name": "534e454b",
            "token1_decimals": 0,
            "token2_policy": "",
            "token2_name": "lovelace",
            "token2_decimals": 6,
            "sources": [
                {
                    "source": "MinSwap",
                    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                    "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                    "security_token_name": "63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896",
                },
                {
                    "source": "MinSwapV2",
                    "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                    "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                    "security_token_name": "4d5350",
                },
                {
                    "source": "SundaeSwapV3",
                    "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
                    "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
                    "security_token_name": "000de140cacb7fd5f5b84bf876d40dc60d4991c72112d78d76132b1fb769e6ad",
                },
                {
                    "source": "WingRiders",
                    "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnay2lz4g5wy95jwh2l6ca2jyq5xu8aga0fh3jyplef6m0npeslcq0pj",
                    "security_token_policy": "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
                    "security_token_name": "4c",
                },
                {
                    "source": "WingRidersV2",
                    "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqlhhdq34c6wgm2u5xkg84nqkql6vq6fzm5grzcequr2rmwqwgf0zz",
                    "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
                    "security_token_name": "4c",
                },
                {
                    "source": "SundaeSwap",
                    "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
                    "security_token_policy": "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913",
                    "security_token_name": "70201f04",
                },
                {
                    "source": "Spectrum",
                    "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
                    "security_token_policy": "f8fd67ee46f66da669f68dc941090eb753687636b47fc6fd7f5e6254",
                    "security_token_name": "534e454b5f4144415f4e4654",
                },
            ],
        },
        [
            {
                "token1_name": "534e454b",
                "token1_decimals": 0,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112169,
                "source": "MinSwap",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                "feed": "SNEK-ADA",
                "utxo": "99bb717918f4c5b157d46333dd178fce221b69141cf4a5f7e2cdfa9885fcf8b8#0",
                "token1_volume": 34505969.0,
                "token2_volume": 139776.931699,
                "price": 0.004050804418765925,
                "amounts": {
                    "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 34505969,
                    "lovelace": 139776931699,
                },
            },
            {
                "token1_name": "534e454b",
                "token1_decimals": 0,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112169,
                "source": "MinSwapV2",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                "feed": "SNEK-ADA",
                "utxo": "6df9479a2014a858fc447ea4f8ad28f90a6e2c313e17490635333609130d5307#1",
                "token1_volume": 1091386593.0,
                "token2_volume": 4422911.081768,
                "price": 0.004052561310662902,
                "amounts": {
                    "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 1091386593,
                    "lovelace": 4422911081768,
                },
            },
            {
                "token1_name": "534e454b",
                "token1_decimals": 0,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112169,
                "source": "SundaeSwapV3",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
                "feed": "SNEK-ADA",
                "utxo": "d824b00949c71a06c475fc6ba4459d942db668a3c8082e40e6f81122c2c37f39#0",
                "token1_volume": 15285198.0,
                "token2_volume": 61945.13656,
                "price": 0.00405262244950965,
                "amounts": {
                    "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 15285198,
                    "lovelace": 61945136560,
                },
            },
            {
                "token1_name": "534e454b",
                "token1_decimals": 0,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112169,
                "source": "WingRiders",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnay2lz4g5wy95jwh2l6ca2jyq5xu8aga0fh3jyplef6m0npeslcq0pj",
                "feed": "SNEK-ADA",
                "utxo": "cf381595cc59eac457ceae2994609fb31199a1c383a74eb7dd04df8ba2b59a00#0",
                "token1_volume": 45214801.0,
                "token2_volume": 183711.67064,
                "price": 0.004063087010821965,
                "amounts": {
                    "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 45214801,
                    "lovelace": 183711670640,
                },
            },
            {
                "token1_name": "534e454b",
                "token1_decimals": 0,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112169,
                "source": "WingRidersV2",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqlhhdq34c6wgm2u5xkg84nqkql6vq6fzm5grzcequr2rmwqwgf0zz",
                "feed": "SNEK-ADA",
                "utxo": "9d49910400019657bf0fd41796d6f3016a1c8508b50e8ba07078214ee16f7d68#0",
                "token1_volume": 128135762.0,
                "token2_volume": 520990.229316,
                "price": 0.004065923682695234,
                "amounts": {
                    "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 128135762,
                    "lovelace": 520990229316,
                },
            },
            {
                "token1_name": "534e454b",
                "token1_decimals": 0,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112169,
                "source": "SundaeSwap",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
                "feed": "SNEK-ADA",
                "utxo": "98dac52291418e658e656ac61e2e66c2189939ac651ba05684f9c0a75d1000ec#0",
                "token1_volume": 9465167.0,
                "token2_volume": 38166.78577,
                "price": 0.004032341507550791,
                "amounts": {
                    "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 9465167,
                    "lovelace": 38166785770,
                },
            },
            {
                "token1_name": "534e454b",
                "token1_decimals": 0,
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "block_height": 170112169,
                "source": "Spectrum",
                "collector": "cnt-collector-node/2.3.0",
                "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
                "feed": "SNEK-ADA",
                "utxo": "3356dd8b5dbc15e581412bf49a6927bf7e6b01430cf089a033f5dd1b4aab061f#0",
                "token1_volume": 449253.0,
                "token2_volume": 1761.304693,
                "price": 0.0039205184895815945,
                "amounts": {
                    "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 449253,
                    "lovelace": 1761304693,
                },
            },
        ],
        {
            "timestamp": "2018-02-19T12:55:00Z",
            "raw": [
                {
                    "token1_name": "534e454b",
                    "token1_decimals": 0,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112169,
                    "source": "MinSwap",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                    "feed": "SNEK-ADA",
                    "utxo": "99bb717918f4c5b157d46333dd178fce221b69141cf4a5f7e2cdfa9885fcf8b8#0",
                    "token1_volume": 34505969.0,
                    "token2_volume": 139776.931699,
                    "price": 0.004050804418765925,
                    "amounts": {
                        "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 34505969,
                        "lovelace": 139776931699,
                    },
                },
                {
                    "token1_name": "534e454b",
                    "token1_decimals": 0,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112169,
                    "source": "MinSwapV2",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                    "feed": "SNEK-ADA",
                    "utxo": "6df9479a2014a858fc447ea4f8ad28f90a6e2c313e17490635333609130d5307#1",
                    "token1_volume": 1091386593.0,
                    "token2_volume": 4422911.081768,
                    "price": 0.004052561310662902,
                    "amounts": {
                        "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 1091386593,
                        "lovelace": 4422911081768,
                    },
                },
                {
                    "token1_name": "534e454b",
                    "token1_decimals": 0,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112169,
                    "source": "SundaeSwapV3",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
                    "feed": "SNEK-ADA",
                    "utxo": "d824b00949c71a06c475fc6ba4459d942db668a3c8082e40e6f81122c2c37f39#0",
                    "token1_volume": 15285198.0,
                    "token2_volume": 61945.13656,
                    "price": 0.00405262244950965,
                    "amounts": {
                        "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 15285198,
                        "lovelace": 61945136560,
                    },
                },
                {
                    "token1_name": "534e454b",
                    "token1_decimals": 0,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112169,
                    "source": "WingRiders",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnay2lz4g5wy95jwh2l6ca2jyq5xu8aga0fh3jyplef6m0npeslcq0pj",
                    "feed": "SNEK-ADA",
                    "utxo": "cf381595cc59eac457ceae2994609fb31199a1c383a74eb7dd04df8ba2b59a00#0",
                    "token1_volume": 45214801.0,
                    "token2_volume": 183711.67064,
                    "price": 0.004063087010821965,
                    "amounts": {
                        "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 45214801,
                        "lovelace": 183711670640,
                    },
                },
                {
                    "token1_name": "534e454b",
                    "token1_decimals": 0,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112169,
                    "source": "WingRidersV2",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqlhhdq34c6wgm2u5xkg84nqkql6vq6fzm5grzcequr2rmwqwgf0zz",
                    "feed": "SNEK-ADA",
                    "utxo": "9d49910400019657bf0fd41796d6f3016a1c8508b50e8ba07078214ee16f7d68#0",
                    "token1_volume": 128135762.0,
                    "token2_volume": 520990.229316,
                    "price": 0.004065923682695234,
                    "amounts": {
                        "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 128135762,
                        "lovelace": 520990229316,
                    },
                },
                {
                    "token1_name": "534e454b",
                    "token1_decimals": 0,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112169,
                    "source": "SundaeSwap",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
                    "feed": "SNEK-ADA",
                    "utxo": "98dac52291418e658e656ac61e2e66c2189939ac651ba05684f9c0a75d1000ec#0",
                    "token1_volume": 9465167.0,
                    "token2_volume": 38166.78577,
                    "price": 0.004032341507550791,
                    "amounts": {
                        "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 9465167,
                        "lovelace": 38166785770,
                    },
                },
                {
                    "token1_name": "534e454b",
                    "token1_decimals": 0,
                    "token2_name": "lovelace",
                    "token2_decimals": 6,
                    "block_height": 170112169,
                    "source": "Spectrum",
                    "collector": "cnt-collector-node/2.3.0",
                    "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
                    "feed": "SNEK-ADA",
                    "utxo": "3356dd8b5dbc15e581412bf49a6927bf7e6b01430cf089a033f5dd1b4aab061f#0",
                    "token1_volume": 449253.0,
                    "token2_volume": 1761.304693,
                    "price": 0.0039205184895815945,
                    "amounts": {
                        "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f.534e454b": 449253,
                        "lovelace": 1761304693,
                    },
                },
            ],
            "data_points": [
                [
                    34505969.0,
                    1091386593.0,
                    15285198.0,
                    45214801.0,
                    128135762.0,
                    9465167.0,
                    449253.0,
                ],
                [
                    139776.931699,
                    4422911.081768,
                    61945.13656,
                    183711.67064,
                    520990.229316,
                    38166.78577,
                    1761.304693,
                ],
            ],
            "calculated_value": "0.004053979055586852",
            "feed": "SNEK-ADA",
            "identity": copy.deepcopy(test_node_id),
            "content_signature": "6b2e03be4ddc5b2c9713fee5e42b8bb73c55a1276e6509f35de49bf0a1d4c47c",
            "errors": [],
        },
        "2018-02-19T12:55:00Z",
    ),
]


@pytest.mark.asyncio
@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "tokens_pair, source_messages, expected_message, expected_dt",
    check_tokens_pair_tests,
)
async def test_check_tokens_pair(
    mocker,
    tokens_pair,
    source_messages,
    expected_message,
    expected_dt,
):
    """Test check tokens pair."""
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    database = {"conn": conn, "cur": cursor}
    app_context = helpers.AppContext(
        db_name=None,
        database=database,
        ogmios_url="",
        ogmios_ws="ws_unused",
        kupo_url="kupo_unused",
        use_kupo=True,
        main_event=None,
        thread_event=None,
        reconnect_event=None,
    )
    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.ogmios_last_block_slot",
    )
    mocker.patch(
        "src.cnt_collector_node.helper_functions._get_source_messages",
        return_value=source_messages,
    )
    message, dt = await check_tokens_pair(
        app_context=app_context,
        identity=test_node_id,
        tokens_pair=tokens_pair,
        pairs=load_pairs.Pairs({}, "12345"),
    )
    assert message == expected_message
    assert dt == expected_dt


check_utxo_for_tokens_pair_tests = [
    (
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
            "amount": 1130496749688,
            "assets": {
                "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114": {
                    "494147": 6727695525020
                },
                "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                    "4d5350": 1,
                    "7b12f25ce8d6f424e1edbc8b61f0742fb13252605f31dc40373d6a245e8ec1d1": 9223369779119105809,
                },
            },
        },
        False,
    ),
    (
        {
            "pair": "CBLP-ADA",
            "source": "MinSwapV2",
            "token1_policy": "ee0633e757fdd1423220f43688c74678abde1cead7ce265ba8a24fcd",
            "token1_name": "43424c50",
            "token1_decimals": 6,
            "token2_policy": "",
            "token2_name": "lovelace",
            "token2_decimals": 6,
            "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
            "security_token_name": "4d5350",
        },
        {
            "amount": 1130496749688,
            "assets": {
                "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114": {
                    "494147": 6727695525020
                },
                "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                    "4d5350": 1,
                    "7b12f25ce8d6f424e1edbc8b61f0742fb13252605f31dc40373d6a245e8ec1d1": 9223369779119105809,
                },
            },
        },
        False,
    ),
    (
        {
            "pair": "IAG-ADA",
            "source": "MinSwapV2",
            "token1_policy": "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
            "token1_name": "494147",
            "token1_decimals": 6,
            "token2_policy": "",
            "token2_name": "lovelace",
            "token2_decimals": 6,
            "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
            "security_token_name": "4d5350",
        },
        {
            "amount": 1131519483409,
            "assets": {
                "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114": {
                    "494147": 6721660272401
                },
                "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                    "4d5350": 1,
                    "7b12f25ce8d6f424e1edbc8b61f0742fb13252605f31dc40373d6a245e8ec1d1": 9223369779119105809,
                },
            },
        },
        True,
    ),
    (
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
            "amount": 866355244478,
            "assets": {
                "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                    "452089abb5bf8cc59b678a2cd7b9ee952346c6c0aa1cf27df324310a70d02fc3": 9223371378343225146,
                    "4d5350": 1,
                },
                "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880": {
                    "69555344": 584912501346
                },
            },
        },
        True,
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "tokens_pair_dict, output_contents, expected", check_utxo_for_tokens_pair_tests
)
async def test_check_utxo_for_tokens_pair(tokens_pair_dict, output_contents, expected):
    """Test check utxo for a given tokens pair."""
    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    res = check_utxo_for_tokens_pair(
        tokens_pair=tokens_pair,
        output_contents=output_contents,
    )
    assert res == expected


non_ada_tests = [
    (
        "BASE-QUOTE",
        {
            "token1_policy": "abc123token1",
            "token1_name": "name123token1",
            "token1_decimals": 6,
            "token2_policy": "abc123token2",
            "token2_name": "name123token2",
            "token2_decimals": 6,
            "pair": "unused",
            "source": "unused",
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        {
            "assets": {
                "abc123token1": {
                    "name123token1": 9223371378343225146,
                    "4d5350": 1,
                },
                "abc123token2": {"name123token2": 584912501346},
            },
        },
        True,
    ),
    # Fails ADA-ADA test.
    (
        "BASE-ADA",
        {
            "token1_policy": "abc123token1",
            "token1_name": "name123token1",
            "token1_decimals": 6,
            "token2_policy": "abc123token2",
            "token2_name": "name123token2",
            "token2_decimals": 6,
            "pair": "unused",
            "source": "unused",
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        {
            "assets": {
                "abc123token1": {
                    "name123token1": 9223371378343225146,
                    "4d5350": 1,
                },
                "abc123token2": {"name123token2": 584912501346},
            },
        },
        False,
    ),
    # Fails ADA-ADA test.
    (
        "ADA-QUOTE",
        {
            "token1_policy": "abc123token1",
            "token1_name": "name123token1",
            "token1_decimals": 6,
            "token2_policy": "abc123token2",
            "token2_name": "name123token2",
            "token2_decimals": 6,
            "pair": "unused",
            "source": "unused",
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        {
            "assets": {
                "abc123token1": {
                    "name123token1": 9223371378343225146,
                    "4d5350": 1,
                },
                "abc123token2": {"name123token2": 584912501346},
            },
        },
        False,
    ),
    # Token1 MIN-ADA test, token 1 is only 5.
    (
        "BASE-QUOTE",
        {
            "token1_policy": "abc123token1",
            "token1_name": "name123token1",
            "token1_decimals": 6,
            "token2_policy": "abc123token2",
            "token2_name": "name123token2",
            "token2_decimals": 6,
            "pair": "unused",
            "source": "unused",
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        {
            "assets": {
                "abc123token1": {
                    "name123token1": 5000000,
                },
                "abc123token2": {"name123token2": 6000000},
            },
        },
        False,
    ),
    # Token2 MIN-ADA test, token 2 is only 5.
    (
        "BASE-QUOTE",
        {
            "token1_policy": "abc123token1",
            "token1_name": "name123token1",
            "token1_decimals": 6,
            "token2_policy": "abc123token2",
            "token2_name": "name123token2",
            "token2_decimals": 6,
            "pair": "unused",
            "source": "unused",
            "security_token_policy": "unused",
            "security_token_name": "unused",
        },
        {
            "assets": {
                "abc123token1": {
                    "name123token1": 6000000,
                },
                "abc123token2": {"name123token2": 5000000},
            },
        },
        False,
    ),
]


@pytest.fixture(name="_modify_min_ada_config")
def fixture_modify_min_ada_config():
    """Ensure the MIN ADA value is constant even if changed globally."""
    config.MIN_ADA_AMOUNT = 5


@pytest.mark.parametrize(
    "pair, tokens_pair_dict, utxo, expected",
    non_ada_tests,
)
def test_validate_non_ada(
    _modify_min_ada_config: Any,
    pair: str,
    tokens_pair_dict: dict,
    utxo: dict,
    expected: bool,
):
    """Provide some basic tests for our validate function when dealing
    with non ADA pairs, e.g. CNT1-CNT2.
    """
    tokens_pair = utxo_objects.tokens_pair_from_dict(tokens_pair_dict)
    res = _validate_non_ada_cnt_base_and_quote(
        pair=pair,
        tokens_pair=tokens_pair,
        utxo=utxo,
    )
    assert res is expected


min_ada_tests = [
    # Nonsense values won't pass.
    (0, 6, 0, False),
    # All pass.
    (6000000, 6, 6000000, True),
    # Lovelace fails.
    (6000000, 6, 5000000, False),
    # Token 1 fails.
    (5000000, 6, 6000000, False),
    # Lovelace ignored and passes.
    (6000000, 6, -1, True),
    # Lovelace ignored and fails.
    (5000000, 6, -1, False),
    # Check against min val 5, fails.
    (4999999, 6, -1, False),
    # Check against min val 5, passes.
    (5000001, 6, -1, True),
]


@pytest.mark.parametrize(
    "token_volume, decimals, lovelace_amount, expected",
    min_ada_tests,
)
def test_validate_min_ada(
    _modify_min_ada_config: Any,
    token_volume: int,
    decimals: int,
    lovelace_amount: int,
    expected: bool,
):
    """A basic test allowing us to test our validate min ada
    function.
    """
    res = _validate_min_ada(
        token_volume=token_volume,
        decimals=decimals,
        lovelace_amount=lovelace_amount,
    )
    assert res == expected
