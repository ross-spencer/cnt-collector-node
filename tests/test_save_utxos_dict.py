"""Test save utxos dict."""

# pylint: disable=E0401

import datetime
import sqlite3
from datetime import timezone
from pathlib import PosixPath

import pytest
import time_machine

import src.cnt_collector_node.database_abstraction as dba
from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.helper_functions import _save_utxos_dict

ada_iusd_utxos = {
    "ADA-iUSD": {
        "MinSwap": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283625,
                "epoch": 587,
                "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                "tx_hash": "772c6e0a90f2260d43198c3a0dd5f6eddc59a3e3c0cb0df01fa073bbb966f0c6",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 8372813921,
                "token2_amount": 7074598561,
                "price": 0.8449487385902695,
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
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283625,
                "epoch": 587,
                "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                "tx_hash": "2bd3a3a97bfde13825d2e7f2ff8cf266b4e3b26cf29577f6797eaaec14356773",
                "output_index": 1,
                "caller": "save_utxo",
                "token1_amount": 1515045619476,
                "token2_amount": 1290594702279,
                "price": 0.851852040419331,
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
        "Spectrum": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283633,
                "epoch": 587,
                "address": "addr1x8nz307k3sr60gu0e47cmajssy4fmld7u493a4xztjrll0aj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrswgxsta",
                "tx_hash": "dcc931860b23dffc2f2e25e0533cfe1f756792eb84462ffc3c2fe4072b614129",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 150000001,
                "token2_amount": 46082800,
                "price": 0.30721866461854225,
            },
            "tokens_pair": {
                "pair": "ADA-iUSD",
                "source": "Spectrum",
                "token1_policy": "",
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
                "token2_name": "69555344",
                "token2_decimals": 6,
                "security_token_policy": "e36480a99003832c2a4dd7b9919915e5c9b5b00244117e5f5ece009d",
                "security_token_name": "697573645f4144415f4e4654",
            },
        },
        "SundaeSwapV3": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283633,
                "epoch": 587,
                "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
                "tx_hash": "a6b1d9a4c29c1966007cb2f1902903a6d761c90d739894d5c54cf955fd1c9151",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 22037116620,
                "token2_amount": 18394300846,
                "price": 0.8346963517589262,
            },
            "tokens_pair": {
                "pair": "ADA-iUSD",
                "source": "SundaeSwapV3",
                "token1_policy": "",
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
                "token2_name": "69555344",
                "token2_decimals": 6,
                "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
                "security_token_name": "000de140c7ef237f227542a0c8930d37911491c56a341fdef8437e0f21d024f8",
            },
        },
        "WingRiders": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283633,
                "epoch": 587,
                "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaytpq3ryg76qvca5eu9c6py33ncg8zf09nh7gy2cvdps2yeqlvvkfh",
                "tx_hash": "1645ad4dd9572f7e604fcb8e3e29ac1dd7991a6a46eb83d936f5fa31c4f40593",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 3130723111,
                "token2_amount": 2684548592,
                "price": 0.8574851549687239,
            },
            "tokens_pair": {
                "pair": "ADA-iUSD",
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
        },
        "WingRidersV2": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283633,
                "epoch": 587,
                "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhq7nzm5jzjeevh40p5h3f682mv3r6fnnsldx749n52asr6vsnevx3j",
                "tx_hash": "94ffcad959039235337f331ab8d8a3cb0d36399f8666a1df16d9d74854356d30",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 2210143906,
                "token2_amount": 1910799119,
                "price": 0.8645586895100578,
            },
            "tokens_pair": {
                "pair": "ADA-iUSD",
                "source": "WingRidersV2",
                "token1_policy": "",
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
                "token2_name": "69555344",
                "token2_decimals": 6,
                "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
                "security_token_name": "4c",
            },
        },
    },
}

copi_ada_utxos = {
    "COPI-ADA": {
        "MinSwap": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283625,
                "epoch": 587,
                "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
                "tx_hash": "fef6adbfa0ec7abd5db575d80b24bcc615c76ba84bcce465608b40fe4a43df04",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 1040201399627,
                "token2_amount": 18977985510,
                "price": 0.018244529873546803,
            },
            "tokens_pair": {
                "pair": "COPI-ADA",
                "source": "MinSwap",
                "token1_policy": "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0",
                "token1_name": "436f726e75636f70696173205b76696120436861696e506f72742e696f5d",
                "token1_decimals": 6,
                "token2_policy": "",
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                "security_token_name": "7925263b1aff069a191db67d5ac185c029f7f43e084a4ef6e5fa2848a56e2aa6",
            },
        },
        "MinSwapV2": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283625,
                "epoch": 587,
                "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                "tx_hash": "65ca836885cc486d4bf7e6c18dde6ecb0c746c5da1d542f31cdc959e1404173f",
                "output_index": 1,
                "caller": "save_utxo",
                "token1_amount": 30311707658055,
                "token2_amount": 557098708830,
                "price": 0.018378994516396283,
            },
            "tokens_pair": {
                "pair": "COPI-ADA",
                "source": "MinSwapV2",
                "token1_policy": "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0",
                "token1_name": "436f726e75636f70696173205b76696120436861696e506f72742e696f5d",
                "token1_decimals": 6,
                "token2_policy": "",
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                "security_token_name": "4d5350",
            },
        },
        "SundaeSwap": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283625,
                "epoch": 587,
                "address": "addr1w9qzpelu9hn45pefc0xr4ac4kdxeswq7pndul2vuj59u8tqaxdznu",
                "tx_hash": "1f64395163d5e99a26d7b997411ac41800fda24c5d840ff9aec5799f379d97e4",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 8713103037,
                "token2_amount": 175489566,
                "price": 0.020140880379215926,
            },
            "tokens_pair": {
                "pair": "COPI-ADA",
                "source": "SundaeSwap",
                "token1_policy": "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0",
                "token1_name": "436f726e75636f70696173205b76696120436861696e506f72742e696f5d",
                "token1_decimals": 6,
                "token2_policy": "",
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "security_token_policy": "0029cb7c88c7567b63d1a512c0ed626aa169688ec980730c0473b913",
                "security_token_name": "70207c02",
            },
        },
        "SundaeSwapV3": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283633,
                "epoch": 587,
                "address": "addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
                "tx_hash": "441cbc74f49ee012e2c1fbd530a8e34d8d9a460e7fdcef9703e7e8fa3dfb464e",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 189981457576,
                "token2_amount": 3486503735,
                "price": 0.018351810642389995,
            },
            "tokens_pair": {
                "pair": "COPI-ADA",
                "source": "SundaeSwapV3",
                "token1_policy": "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0",
                "token1_name": "436f726e75636f70696173205b76696120436861696e506f72742e696f5d",
                "token1_decimals": 6,
                "token2_policy": "",
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "security_token_policy": "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
                "security_token_name": "000de140d251b55cb84789c15327ef6af1c02ad8044da88ae7dd6cfbbd5504cd",
            },
        },
        "Spectrum": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283633,
                "epoch": 587,
                "address": "addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
                "tx_hash": "4638fae3533c7495270944ce2f84dfb49973ce883b4bb04bad8c53fc899db612",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 1340230664,
                "token2_amount": 65254262,
                "price": 0.04868882928349414,
            },
            "tokens_pair": {
                "pair": "COPI-ADA",
                "source": "Spectrum",
                "token1_policy": "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0",
                "token1_name": "436f726e75636f70696173205b76696120436861696e506f72742e696f5d",
                "token1_decimals": 6,
                "token2_policy": "",
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "security_token_policy": "97ee81694da57202968cfb49d5f890f443f9598fa6fd798317b1c7bb",
                "security_token_name": "436f726e75636f706961735f4144415f4e4654",
            },
        },
        "WingRiders": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283633,
                "epoch": 587,
                "address": "addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnay0ehqvwna79w5um659cjdpxfgw0c4ds9wy7y0a2x644uc6qp8mufz",
                "tx_hash": "cfc67d6eecf974f5668bafb2b175f16c6f7e322155fa817bea0aa39415565175",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 253792330735,
                "token2_amount": 4642469804,
                "price": 0.018292395954421038,
            },
            "tokens_pair": {
                "pair": "COPI-ADA",
                "source": "WingRiders",
                "token1_policy": "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0",
                "token1_name": "436f726e75636f70696173205b76696120436861696e506f72742e696f5d",
                "token1_decimals": 6,
                "token2_policy": "",
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "security_token_policy": "026a18d04a0c642759bb3d83b12e3344894e5c1c7b2aeb1a2113a570",
                "security_token_name": "4c",
            },
        },
        "WingRidersV2": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283633,
                "epoch": 587,
                "address": "addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqcawt5de839vdfek5d2pyt0jxj2mdswrrylreasjtztvfjsp3k4lq",
                "tx_hash": "77584d6381c5b71e5bc15392acf77d03bea0e1399243f3e973a5410d4a73fbc6",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 7602057809922,
                "token2_amount": 140243150889,
                "price": 0.01844805109294992,
            },
            "tokens_pair": {
                "pair": "COPI-ADA",
                "source": "WingRidersV2",
                "token1_policy": "b6a7467ea1deb012808ef4e87b5ff371e85f7142d7b356a40d9b42a0",
                "token1_name": "436f726e75636f70696173205b76696120436861696e506f72742e696f5d",
                "token1_decimals": 6,
                "token2_policy": "",
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
                "security_token_name": "4c",
            },
        },
    },
}

ada_ieth_utxos = {
    "ADA-iETH": {
        "MinSwapV2": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283625,
                "epoch": 587,
                "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                "tx_hash": "f1239003de32f70f6d4bff7f601e19441d1aa4b87f041c55fb86c9c48808b473",
                "output_index": 1,
                "caller": "save_utxo",
                "token1_amount": 217036637438,
                "token2_amount": 39833292,
                "price": 0.0001835325706765938,
            },
            "tokens_pair": {
                "pair": "ADA-iETH",
                "source": "MinSwapV2",
                "token1_policy": "",
                "token1_name": "lovelace",
                "token1_decimals": 6,
                "token2_policy": "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
                "token2_name": "69455448",
                "token2_decimals": 6,
                "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                "security_token_name": "4d5350",
            },
        }
    },
}

# Should write results for two different pairs.
xer_milk_ada_utxos = {
    "XER-ADA": {
        "MinSwapV2": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283625,
                "epoch": 587,
                "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
                "tx_hash": "d3b2da513e11c5e935e7bdc3f5708403c0c037ab1f33a92c9c3b42bfa8c4ec79",
                "output_index": 1,
                "caller": "save_utxo",
                "token1_amount": 21709070303607,
                "token2_amount": 351879199520,
                "price": 0.016208856233771315,
            },
            "tokens_pair": {
                "pair": "XER-ADA",
                "source": "MinSwapV2",
                "token1_policy": "6d06570ddd778ec7c0cca09d381eca194e90c8cffa7582879735dbde",
                "token1_name": "584552",
                "token1_decimals": 6,
                "token2_policy": "",
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
                "security_token_name": "4d5350",
            },
        }
    },
    "MILK-ADA": {
        "MinSwap": {
            "context": {
                "db_name": PosixPath("/tmp/cnt-indexer.db"),
                "block_height": 168283633,
                "epoch": 587,
                "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxzwzgvrk3c2sgy7n0nuaq0e23eqcy2qp2wefkae9cvjxamlslfpwh5",
                "tx_hash": "da47bc292127482a688037e78a8c2209d59db2307f85ae34ce479de9c5e8814f",
                "output_index": 0,
                "caller": "save_utxo",
                "token1_amount": 21937557831,
                "token2_amount": 7246107731,
                "price": 0.33030603437364,
            },
            "tokens_pair": {
                "pair": "MILK-ADA",
                "source": "MinSwap",
                "token1_policy": "afbe91c0b44b3040e360057bf8354ead8c49c4979ae6ab7c4fbdc9eb",
                "token1_name": "4d494c4b7632",
                "token1_decimals": 6,
                "token2_policy": "",
                "token2_name": "lovelace",
                "token2_decimals": 6,
                "security_token_policy": "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
                "security_token_name": "dcf06101dba894ca6cf60469ea2cc0153ff7e725361d652d1ebba7d6fdce615a",
            },
        }
    },
}

ieth_res = [
    ("ADA-iETH", 168283625, 0.0001835325706765938, "MinSwapV2", "2018-02-19T12:55:00Z")
]


iusd_res = [
    ("ADA-iUSD", 168283625, 0.8449487385902695, "MinSwap", "2018-02-19T12:55:00Z"),
    ("ADA-iUSD", 168283625, 0.851852040419331, "MinSwapV2", "2018-02-19T12:55:00Z"),
    ("ADA-iUSD", 168283633, 0.30721866461854225, "Spectrum", "2018-02-19T12:55:00Z"),
    ("ADA-iUSD", 168283633, 0.8346963517589262, "SundaeSwapV3", "2018-02-19T12:55:00Z"),
    ("ADA-iUSD", 168283633, 0.8574851549687239, "WingRiders", "2018-02-19T12:55:00Z"),
    ("ADA-iUSD", 168283633, 0.8645586895100578, "WingRidersV2", "2018-02-19T12:55:00Z"),
]

xer_milk_res = [
    ("XER-ADA", 168283625, 0.016208856233771315, "MinSwapV2", "2018-02-19T12:55:00Z"),
    ("MILK-ADA", 168283633, 0.33030603437364, "MinSwap", "2018-02-19T12:55:00Z"),
]

copi_res = [
    ("COPI-ADA", 168283625, 0.018244529873546803, "MinSwap", "2018-02-19T12:55:00Z"),
    ("COPI-ADA", 168283625, 0.018378994516396283, "MinSwapV2", "2018-02-19T12:55:00Z"),
    ("COPI-ADA", 168283625, 0.020140880379215926, "SundaeSwap", "2018-02-19T12:55:00Z"),
    (
        "COPI-ADA",
        168283633,
        0.018351810642389995,
        "SundaeSwapV3",
        "2018-02-19T12:55:00Z",
    ),
    ("COPI-ADA", 168283633, 0.04868882928349414, "Spectrum", "2018-02-19T12:55:00Z"),
    ("COPI-ADA", 168283633, 0.018292395954421038, "WingRiders", "2018-02-19T12:55:00Z"),
    (
        "COPI-ADA",
        168283633,
        0.01844805109294992,
        "WingRidersV2",
        "2018-02-19T12:55:00Z",
    ),
]


save_utxos_tests = [
    (xer_milk_ada_utxos, xer_milk_res),
    (ada_ieth_utxos, ieth_res),
    (ada_iusd_utxos, iusd_res),
    (copi_ada_utxos, copi_res),
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize("values, expected", save_utxos_tests)
def test_save_utxos_dict(mocker, values: dict, expected: list):
    """Make sure save UTxOs performs consistently.

    Res provides a sample check to ensure some of the values are written
    without testing every single input.
    """
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )
    # Mock save_utxo record. We manage to test most of its routing in
    # other unit tests.
    #
    # First mock ensures we don't write data when the expected
    # conditions are not met -- return_value == False.
    mocker.patch(
        "src.cnt_collector_node.helper_functions.save_utxo_record", return_value=False
    )
    _save_utxos_dict(db, values)
    cursor.execute("select * from price;")
    res = cursor.fetchall()
    assert not res
    # Make sure we write when the expected conditions are met
    # -- return_value == True.
    mocker.patch(
        "src.cnt_collector_node.helper_functions.save_utxo_record", return_value=True
    )
    _save_utxos_dict(db, values)
    cursor.execute("select * from price;")
    res = cursor.fetchall()
    assert len(res) > 0
    no_columns = 9
    for row in res:
        # Ensure we insert: pair, epoch, block_height, price,
        # token1_amount, token2_amount, source, date_time + IDX.
        assert len(row) == no_columns
    cursor.execute("select pair, block_height, price, source, date_time from price;")
    res = cursor.fetchall()
    assert res == expected
