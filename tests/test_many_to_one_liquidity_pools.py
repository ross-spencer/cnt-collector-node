"""Provide some tests for results expected for dexes with liquidity
pools across multiple UTxOs.

* Indexer functions should store a single UTxO and report on a single
  UTxO.
* Submitter functions should generate a price based on a single UTxO.

We have determined that most liquidity will exist in just a single
UTxO. In some cases like MinswapV2 there will be more UTxOs with
value, but they won't pass the MIN_ADA test.

These tests are designed to potentially allow more than one liquidity
pool in future, but are used to help us ensure the behavior of
1 liquidity pool per source right now.
"""

# pylint: disable=C0302; too-many lines in module.

import datetime
import sqlite3
from datetime import timezone

import pytest
import time_machine

import src.cnt_collector_node.database_abstraction as dba
import src.cnt_collector_node.global_helpers as helpers
from src.cnt_collector_node import helper_functions
from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.utxo_objects import TokensPair

ibtc_minswapv2_utxos = [
    {
        "tx_hash": "b8a498a087544272899b1142069920d19507348a9bfc63dd2cd20874bc573d47",
        "tx_index": 1,
        "amount": 6627650,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4041938753729fb08787e37a015dd351c46028db88ce704b098c0159c5c1f888": 9223372036854774817,
                "4d5350": 1,
            },
            "b4f3c361a27f7e1394b33ae3dbd8f5799c955003f30ccf66728fd01c": {
                "44656572616e64446f67": 55125209
            },
        },
    },
    {
        "tx_hash": "7503d728fb4ae11554dea04a35a311ba20280ed22c3cf1d604feed91827011aa",
        "tx_index": 1,
        "amount": 4504035,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "6db92aa6848e0763f75746cfd9db9e4b1a55307486e0700183047e97829b753e": 9223372036854775807,
            },
            "1efc907b46c9a3f91e48157160a7460046c9b5a1e3681ce79ee4e8dc": {
                "526163636f6f6e50617472696f74": 1
            },
        },
    },
    {
        "tx_hash": "ec2ba7c948fd4fa803471a4886bc01c16f6e9bd41fa093e9b41089782715f043",
        "tx_index": 1,
        "amount": 4622289,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "a21bbd162bf57727ed1e4c311ad6423f08487acc224eaf0a1db2b99c8ed3d517": 9223372036854775807,
            },
            "db29198e2485b7b2487395be0e974c49fad6e4674255c4e9757594d8": {
                "42594544454e": 32171366
            },
        },
    },
    {
        "tx_hash": "0039a20b944b330667372c1c9fed1a18d7c0c1d163fc051c94fe72d478ed6bf9",
        "tx_index": 1,
        "amount": 4549982,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "ff997e2195eef4f2d59ff30130562cb4786dc8c2ae6979d900289d387b4db128": 9223372036854775807,
            },
            "c3d63b40da90b312d833a3336a6038d518be5ef4ef1bc46741f00fff": {
                "4845444745": 2090225
            },
        },
    },
    {
        "tx_hash": "8716c4368c44f5a961c1da6354acc379fe4e2194735454f2319aa565864fa4cb",
        "tx_index": 1,
        "amount": 8685932,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "d91281aa43f67cfa668c9ded85076f71e51fae9ba9cc0d74c69281b4d7487dfa": 9223372036758807314,
            },
            "436649cee1a57ab2ab38a8be97003870b7fe7f1489706090e5be4b64": {
                "636c616d73": 2392815024
            },
        },
    },
    {
        "tx_hash": "891922bfe8fdb790ea6b2896cd9ecf7601622339a8f927529044fafda662d775",
        "tx_index": 1,
        "amount": 1981054849,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "158029c22023017557a269187389cfcfaa43e3023ba45242372d94f978893ba6": 9223372033065199745,
                "4d5350": 1,
            },
            "a8f54e6eb47a7ef6d96aceb892f4e9b6211f8b736a1f6d3109648c20": {
                "4f6666696369616c436861726c65736d656d65": 13290988172
            },
        },
    },
    {
        "tx_hash": "385f8cbe608a9cc296512ade41df03218d436456c6b666f3cb212088a89dbeab",
        "tx_index": 1,
        "amount": 4502500,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "d0054e5c83c7b9d23f24879e12ddadcd1905b57b532ec0ab4e48d90a9baacf57": 9223372036854775807,
            },
            "03aa9ef9fb3bb0bd9a9b21469838615b6c8005c8b98c0c173bf32d8a": {
                "5069675061727479": 3965
            },
        },
    },
    {
        "tx_hash": "60f4b7aa1e658661f6fa66a1b93288830b3d3e8a79838ba44db0d7a86490848c",
        "tx_index": 1,
        "amount": 4727427,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "9191e0f19aa3f0d72a2551fe7b5b6dd9d6dce66dd9a509c8fcf68f9de6b63fe5": 9223372036854775807,
            },
            "7bb3515fdb50add0d6541dc9bbf475daf1f277061b3370bfcc635ec0": {
                "426162794b414d41": 6
            },
        },
    },
    {
        "tx_hash": "522cc6ea1e4ae7f5c6e0a9ce4a491e2af8df7e4a29d2a05e50e99361fc32a9bf",
        "tx_index": 1,
        "amount": 548207461785,
        "assets": {
            "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880": {
                "69425443": 2271231
            },
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "63a3b8ee322ea31a931fd1902528809dc681bc650af21895533c9e98fa4bef2e": 9223372035830097073,
            },
        },
    },
    {
        "tx_hash": "0fe480ef92ef4bbcfb3eb34739df2f9cf9c36714f212b82472675b6a41b6870b",
        "tx_index": 1,
        "amount": 4599962,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "9552b796f73a220355d6f6b74998139806bf42324eb39341369aa48d15356ac0": 9223372036854775807,
            },
            "b109f67689977d61896c3030596e1dec53d752f324cb0c1b0d8f2b0e": {
                "534852494d50": 454310
            },
        },
    },
    {
        "tx_hash": "a10525ec14286c52df437c608a3b9b500e61abe197543bc4f3ea270ece56b24d",
        "tx_index": 1,
        "amount": 4500025,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "54b8fe927e699d12f2ae7bffe3173426b9ef9dc6caf2393ae827d724450704c4": 9223372036854775807,
            },
            "90ab6485d58e1f3cf6128a61f567a92ca9c2081d430e506ca5c67cff": {
                "24454c464f4e": 5
            },
        },
    },
    {
        "tx_hash": "2af74faa288207f079083c31534ad2cd7d9c0b1bbccb430de5282762c81d4f1d",
        "tx_index": 2,
        "amount": 4500000,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "566b4f6b08287d57ba21c131fcfda0f6fce10f5acc6d56e1df62ea4d9414a792": 9223372036854775807,
            },
            "65bdf33f8f7fd4debeb2ad659473749eb4eac177e06650bb75a8fe50": {
                "4d69746872546f6b656e": 571
            },
            "44e241540aa68b7c7019183c7676742bdabac878653540cf22548aba": {
                "53414e4d415254494e": 1
            },
        },
    },
    {
        "tx_hash": "246d9dd45e4bcbba692685b91d0a4a50988554ba155430f013611c549a62cc99",
        "tx_index": 1,
        "amount": 28066968,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "ab44a915b3cb40b8d4657dab9394f9b9179ddd26782ca34f81cba632f1c352ba": 9223372036854775807,
            },
            "1a0dd392a13a966f6957a15a6831d725fbcd756be0511e925fb1e645": {
                "5350414345434154": 871511
            },
        },
    },
    {
        "tx_hash": "0637a20cb12a418d981b1f99697829db3931cfa2118b32eb4684863953665c9a",
        "tx_index": 1,
        "amount": 5858740,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4005a925287b087934e38528b92e1186721b77a707dc8c3da2b1bf95409662c7": 9223372036854775807,
                "4d5350": 1,
            },
            "9e040e9380646292bed09e2434ddc7835569d054602311a082831100": {
                "505650": 43044427
            },
        },
    },
    {
        "tx_hash": "cef6b105a0b939c248a6b283ff159739c58f4926872609271bd09d2a83e17c81",
        "tx_index": 2,
        "amount": 4500000,
        "assets": {
            "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880": {
                "69425443": 63578,
                "69555344": 5425991847,
            },
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4c20beaf8f5fca3cab69ac0e77002225ab68a18db599b46e69887720ce0e59db": 9223372036836744659,
                "4d5350": 1,
            },
        },
    },
    {
        "tx_hash": "872aa8b024400e45b855de5836ed600e0ba6914db386b43ffe0b66bb32cb580b",
        "tx_index": 1,
        "amount": 15500000,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "060fac93bdecdc588260b1099e0b2e32017b831900a766b87b37b5517c1c3f3c": 9223372036823153040,
                "4d5350": 1,
            },
            "bc8cf381653448b3fb3bf2be4b49082033a078743ebe7ba2daee8a45": {
                "49534b59": 90913224
            },
        },
    },
    {
        "tx_hash": "074370b9b37a3394553b95608a2f7040b7e5c053f3c1f5ace4dd51113dd7ebe0",
        "tx_index": 1,
        "amount": 4568104,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "fbab4f681d4c623104d91182245f44fd8338a6fcfb9b9ccfaf40077ca66ef70a": 9223372036854775706,
            },
            "052320c20e4a9fdd09a2980b04807ff9e5e51e4d11886e9c83914cde": {
                "536e656b456e65726779": 3016717457
            },
        },
    },
]

xer_minswapv2_utxos = [
    {
        "tx_hash": "8850419943066456774a3edecd7f5e22487a19b5a9ae019dbd07a1fd45763f90",
        "tx_index": 1,
        "amount": 6500000,
        "assets": {
            "fdcd53f2bb0509479d2e9b085d0037ea58ae1941872ac691c5c1eead": {
                "542d52756d7020": 388870
            },
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "1328615d8a6fe2d88f29296e9f77ed15f089b3f39e4fd8be01fac8d9d043eeb9": 9223372036853894583,
                "4d5350": 1,
            },
        },
    },
    {
        "tx_hash": "385d23a0d61e32e1d14abde9bebc5d0c5ce5fb7eaa96e4096ee95633e0b53c14",
        "tx_index": 1,
        "amount": 191178354786,
        "assets": {
            "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880": {
                "69455448": 22150805
            },
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "562b9ff903fe8d9e1c980120a233051e7b1518cfc75eb9b4227f7710b670b6e9": 9223372034965540159,
            },
        },
    },
    {
        "tx_hash": "7493b1bbfba5311132ebdcda3ecc88216fb7cf0cd4e885d60389e5ac68571560",
        "tx_index": 0,
        "amount": 5000000,
        "assets": {},
    },
    {
        "tx_hash": "eccc9a98f08e9bad07b7b4891348922a060c5fd3342f9a471dc2e4c3ece59c30",
        "tx_index": 1,
        "amount": 7550431,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "de3564cc0b5819719bd3ba491e76450d7335d58683d0a594a3c0b57889fa9b78": 9223372036854775807,
            },
            "e7acc0e385ccebfea3c0f0f7798627d38dbeb225310d4d7a5d7361f2": {
                "4e4549524f": 1
            },
        },
    },
    {
        "tx_hash": "e03feb60c3bca09618f18c8586e1aed71f62d7155b4dd00a08c2f16734eadd23",
        "tx_index": 1,
        "amount": 115340532739,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "f310d21783a3ae8d2b519e20e2379800390e54c895132f7162a7542ccf834ca5": 9223371760743709990,
            },
            "5deab590a137066fef0e56f06ef1b830f21bc5d544661ba570bdd2ae": {
                "424f44454741": 1295345700912
            },
        },
    },
    {
        "tx_hash": "69eb87087593cce1ac0a428b7f6848347032e1e140882f93f21da1bbe7a71b15",
        "tx_index": 1,
        "amount": 5969157,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "79d878415c78a27c60acad6363010ce6a50ab76230f43a9019cfa726617731ed": 9223372036854775807,
            },
            "78c2d629e35b7dcab28e92ea48c15cd9769a965f284fbf72d76ae41b": {
                "47484f5354": 130065
            },
        },
    },
    {
        "tx_hash": "2eeab4d0a1d3ef61302f327a8b1f1f15436cc7a0edcc4fbf8b56333ed9749249",
        "tx_index": 1,
        "amount": 4599981,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "bf352f3f7cf643fef554f1572d5f82939306a938fcb34135425b077039a7a8b8": 9223372036854775807,
            },
            "cb0e67641a7b98a24742741c3d4a0ccdc51d7441beea367e9bfd9171": {"50444f47": 5},
        },
    },
    {
        "tx_hash": "ac4688f8dac1d709acdc9d835ee5470aaa2126b0bf0517631bfd3700bebbfe1d",
        "tx_index": 1,
        "amount": 37943472517,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "d1dce95696a3a5a62b81d03e21f9a1af6952a8114d141b2fbc4968de1260fd95": 9223209461095808787,
            },
            "a967738feca0f92afa0f781f2db4ec318ee6f06cb515fecf988fabfb": {
                "0014df105355504552494f52": 742043064756801885
            },
        },
    },
    {
        "tx_hash": "8bba48bcb2793488f2e52a7ec762c69844dcd7ffddcb95005cf15b1e50f2f5ed",
        "tx_index": 0,
        "amount": 5500000,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "4d5350": 1,
                "4ebcd9bd181cebd5417373dd80c901214d83638eec97ef026502b0480c2a3fa5": 9223372036847322257,
            },
            "1d20fc2c0f308854fcf9fafc466c563757e3849c8a6855631b497cc5": {
                "726174617461": 55555555
            },
        },
    },
    {
        "tx_hash": "6d39e231e1144a17e762acb0d352ea33d9aa7e040e420e1fa69f4df39c5ca5c1",
        "tx_index": 1,
        "amount": 260368000949,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                "372468543304a13de4f75942b2a22d5a3df59395f2f65955de5eb3d0a4ebd08b": 9223369962494334350,
                "4d5350": 1,
            },
            "6d06570ddd778ec7c0cca09d381eca194e90c8cffa7582879735dbde": {
                "584552": 20491328187024
            },
        },
    },
]

ibtc_minswapv2_tokens_pair = TokensPair(
    pair="ADA-iBTC",
    source="MinSwapV2",
    token_1_policy="",
    token_1_name="lovelace",
    token_1_decimals=6,
    token_2_policy="f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
    token_2_name="69425443",
    token_2_decimals=6,
    security_token_policy="f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
    security_token_name="4d5350",
    address="addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
    collector="cnt-collector-node/2.3.0",
)

xer_db_expected = [
    (
        1,
        "XER-ADA",
        "MinSwapV2",
        0.012706253034094507,
        20491328187024,
        260368000949,
        123,
        123,
        "2025-12-24T14:50:00Z",
    )
]

xer_minswapv2_expected = {
    "utxo": "6d39e231e1144a17e762acb0d352ea33d9aa7e040e420e1fa69f4df39c5ca5c1#1",
    "token1_volume": 20491328.187024,
    "token2_volume": 260368.000949,
    "price": 0.012706253034094507,
    "amounts": {
        "6d06570ddd778ec7c0cca09d381eca194e90c8cffa7582879735dbde.584552": 20491328187024,
        "lovelace": 260368000949,
    },
}

xer_minswapv2_tokens_pair = TokensPair(
    pair="XER-ADA",
    source="MinSwapV2",
    token_1_policy="6d06570ddd778ec7c0cca09d381eca194e90c8cffa7582879735dbde",
    token_1_name="584552",
    token_1_decimals=6,
    token_2_policy="",
    token_2_name="lovelace",
    token_2_decimals=6,
    security_token_policy="f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
    security_token_name="4d5350",
    address="addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
    collector=None,
)

multiple_utxos_tests = [
    # XER has one liquidity pool that should be returned.
    (
        xer_minswapv2_utxos,
        xer_minswapv2_tokens_pair,
        xer_minswapv2_expected,
        xer_db_expected,
    ),
    # iBTC has two liquidity pools that are too small to return.
    (
        ibtc_minswapv2_utxos,
        ibtc_minswapv2_tokens_pair,
        # Ensure responses are empty, i.e. not None.
        {},
        [],
    ),
]


@time_machine.travel(datetime.datetime(2025, 12, 24, 14, 50, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "utxos, tokens_pair, expected, db_expected",
    multiple_utxos_tests,
)
def test_many_to_one_utxos(utxos, tokens_pair, expected, db_expected):
    """Ensure check_dex_tokens_pair can handle multiple liquidity
    pools from a single source.

    Without proper guardrails, the iBTC response is:

    ```
        ibtc_minswapv2_expected = [
            {
                "utxo": "522cc6ea1e4ae7f5c6e0a9ce4a491e2af8df7e4a29d2a05e50e99361fc32a9bf#1",
                "token1_volume": 548207.461785,
                "token2_volume": 2.271231,
                "price": 4.143013655094589e-06,
                "amounts": {
                    "lovelace": 548207461785,
                    "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69425443": 2271231,
                },
            },
            {
                "utxo": "cef6b105a0b939c248a6b283ff159739c58f4926872609271bd09d2a83e17c81#2",
                "token1_volume": 4.5,
                "token2_volume": 0.063578,
                "price": 0.014128444444444443,
                "amounts": {
                    "lovelace": 4500000,
                    "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880.69425443": 63578,
                },
            },
        ]
    ```


    """

    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()

    # NB. we could mock `parse_utxo` here, but we might end up
    # with false positives. We can improve on this the more we
    # get closer to single purpose functions.
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )

    res = helper_functions.check_dex_tokens_pair(
        epoch=123,
        block_height=123,
        database=db,
        tokens_pair=tokens_pair,
        utxos=utxos,
    )
    assert res == expected

    # Check the database results are written as expected.
    cursor.execute("select * from price;")
    db_res = cursor.fetchall()
    try:
        assert db_res == db_expected
    except AssertionError as err:
        if not db_expected:
            return
        raise AssertionError from err


address_pair_tests = [
    (
        ibtc_minswapv2_tokens_pair,
        ibtc_minswapv2_utxos,
        {},
        [],
    ),
    (
        xer_minswapv2_tokens_pair,
        xer_minswapv2_utxos,
        {
            "token1_name": "584552",
            "token1_decimals": 6,
            "token2_name": "lovelace",
            "token2_decimals": 6,
            "block_height": "12345",
            "source": "MinSwapV2",
            "collector": None,
            "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
            "feed": "XER-ADA",
            "utxo": "6d39e231e1144a17e762acb0d352ea33d9aa7e040e420e1fa69f4df39c5ca5c1#1",
            "token1_volume": 20491328.187024,
            "token2_volume": 260368.000949,
            "price": 0.012706253034094507,
            "amounts": {
                "6d06570ddd778ec7c0cca09d381eca194e90c8cffa7582879735dbde.584552": 20491328187024,
                "lovelace": 260368000949,
            },
        },
        [
            (
                1,
                "XER-ADA",
                "MinSwapV2",
                0.012706253034094507,
                20491328187024,
                260368000949,
                123,
                12345,
                "2025-12-24T14:50:00Z",
            )
        ],
    ),
]


@time_machine.travel(datetime.datetime(2025, 12, 24, 14, 50, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "tokens_pair, utxos, address_res, db_expected",
    address_pair_tests,
)
def test_check_one_to_many_check_address_pair(
    mocker, tokens_pair, utxos, address_res, db_expected
):
    """Ensure that check address pair works as anticipated when not
    using the index."""

    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )

    mocker.patch(
        "src.cnt_collector_node.ogmios_helper.ogmios_epoch",
        return_value={
            "jsonrpc": "2.0",
            "method": "queryLedgerState/epoch",
            "result": 123,
            "id": None,
        },
    )

    mocker.patch(
        "src.cnt_collector_node.helper_functions._get_utxos_content",
        return_value=utxos,
    )

    mocker.patch(
        "src.cnt_collector_node.kupo_helper.get_kupo_matches",
        return_value=utxos,
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

    res = helper_functions.check_address_pair(
        app_context=app_context,
        tokens_pair=tokens_pair,
        last_block_slot="12345",
    )
    if res and not address_res:
        assert False, f"result returned when not expected: {res}"
    elif not address_res:
        return
    assert res == address_res

    cursor.execute("select * from price;")
    db_res = cursor.fetchall()
    try:
        assert db_res == db_expected
    except AssertionError as err:
        if not db_expected:
            return
        raise AssertionError from err
