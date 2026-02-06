"""Placeholder tests."""

import pytest

from src.cnt_collector_node.kupo_helper import get_kupo_utxo_content
from src.cnt_collector_node.ogmios_helper import (
    get_ogmios_utxo_content,
    get_output_content,
)

output_one_asset = {
    "transaction": {
        "id": "fff2c860cab76a65f9a253af9a3b79da9bcf652b68dbfe60cf7ff56495296013"
    },
    "index": 0,
    "address": "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
    "value": {
        "ada": {"lovelace": 8508000},
        "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b": {
            "000de1406f06ff23c10168fed731aaa3a485db994d744284d2c89eb1d41299d9": 1
        },
    },
    "datum": "d8799f581c6f06ff23c10168fed731aaa3a485db994d744284d2c89eb1d41299d99f9f4040ff9f581c37e8ea14957affc28b19ec1962af2db1f8fdd899edc8c6542b4d7ea547554e4841505059ffff00181e181ed87a80001a0081d260ff",
}

output_one_asset_res = {
    "amount": 8508000,
    "assets": {
        "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b": {
            "000de1406f06ff23c10168fed731aaa3a485db994d744284d2c89eb1d41299d9": 1
        }
    },
}


output_three_assets = {
    "transaction": {
        "id": "57546fc455e71bd67797f1061fad217947b5ebd539d65764ba7c62b731ce73f5"
    },
    "index": 0,
    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
    "value": {
        "ada": {"lovelace": 5000000},
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "8c9f57eb4a354c57096cebad10a28a92c9beca04e064867ae402b965c254ff49": 1
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "7893fb6ff8536bdd19cd552ca01937f9e834bfdc788cb22ec87a263b": {
            "647465737433": 10000000
        },
    },
    "datumHash": "58b6d1cd0de192f7bd42fb72d491dac62e1a16a4b4f5796297749bbac5205afd",
}

output_three_assets_res = {
    "amount": 5000000,
    "assets": {
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "8c9f57eb4a354c57096cebad10a28a92c9beca04e064867ae402b965c254ff49": 1
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "7893fb6ff8536bdd19cd552ca01937f9e834bfdc788cb22ec87a263b": {
            "647465737433": 10000000
        },
    },
}


output_five_assets = {
    "transaction": {
        "id": "5764fbcc5001934412f819d6a19b7eac3372321e08387f60541e11cec54766e8"
    },
    "index": 0,
    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
    "value": {
        "ada": {"lovelace": 2223960},
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "7489a627641ff1db0131445090653db82999c70b18512332469e30785f92ebc0": 1
        },
        "133fac9e153194428eb0919be39837b42b9e977fc7298f3ff1b76ef9": {
            "5055444759": 373978042
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
            "7489a627641ff1db0131445090653db82999c70b18512332469e30785f92ebc0": 1907
        },
        "f01ec1cb021922a491ea300fb4791dbaca720372b2a3142579c52e7d": {"4b616e69": 6095},
    },
    "datumHash": "8c255d61ea9d6bd5a65bb42c461a5ff810e8fe9eb9fbd9ba20c67bf6c8ab0c62",
}

output_five_assets_res = {
    "amount": 2223960,
    "assets": {
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "7489a627641ff1db0131445090653db82999c70b18512332469e30785f92ebc0": 1
        },
        "133fac9e153194428eb0919be39837b42b9e977fc7298f3ff1b76ef9": {
            "5055444759": 373978042
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
            "7489a627641ff1db0131445090653db82999c70b18512332469e30785f92ebc0": 1907
        },
        "f01ec1cb021922a491ea300fb4791dbaca720372b2a3142579c52e7d": {"4b616e69": 6095},
    },
}


output_no_assets = {
    "transaction": {
        "id": "861d00e2ae4ad1c4f615b3d503e7417725548d773d9ab04047f96fbc652cd1b1"
    },
    "index": 0,
    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
    "value": {"ada": {"lovelace": 1000000}},
}

output_no_assets_res = {"amount": 1000000, "assets": {}}

output_tests = [
    (output_one_asset, output_one_asset_res),
    (output_three_assets, output_three_assets_res),
    (output_five_assets, output_five_assets_res),
    (output_no_assets, output_no_assets_res),
]


@pytest.mark.parametrize("content, result", output_tests)
def test_output_content(content: dict, result: dict):
    """Make sure we parse ogmios content correctly."""
    res = get_output_content(content)
    assert res == result


output_one_asset_mock = {
    "transaction": {"id": "MOCK_TX_ID"},
    "index": 0,
    "address": "MOCK_ADDR",
    "value": {
        "ada": {"lovelace": 10000000},
        "MOCK_POLICY": {"MOCK_POLICY_NAME": 100},
    },
    "datum": "MOCK_DATUM_ID",
}

output_one_asset_res_mock = {
    "amount": 10000000,
    "assets": {"MOCK_POLICY": {"MOCK_POLICY_NAME": 100}},
}

output_three_asset_mock = {
    "transaction": {"id": "MOCK_TX_ID"},
    "index": 0,
    "address": "MOCK_ADDR",
    "value": {
        "ada": {"lovelace": 20000000},
        "MOCK_POLICY_1": {"MOCK_POLICY_NAME": 200},
        "MOCK_POLICY_2": {"MOCK_POLICY_NAME": 200},
        "MOCK_POLICY_3": {"MOCK_POLICY_NAME": 200},
    },
    "datum": "MOCK_DATUM_ID",
}

output_three_asset_res_mock = {
    "amount": 20000000,
    "assets": {
        "MOCK_POLICY_1": {"MOCK_POLICY_NAME": 200},
        "MOCK_POLICY_2": {"MOCK_POLICY_NAME": 200},
        "MOCK_POLICY_3": {"MOCK_POLICY_NAME": 200},
    },
}


mock_output_tests = [
    (output_one_asset_mock, output_one_asset_res_mock),
    (output_three_asset_mock, output_three_asset_res_mock),
]


@pytest.mark.parametrize("content, result", mock_output_tests)
def test_output_content_mock(content: dict, result: dict):
    """Make sure we parse ogmios content correctly."""
    res = get_output_content(content)
    assert res == result


djed_utxo_ogmios = {
    "transaction": {
        "id": "5c6de452e76a4be2bf33483261db309e589fa2b4d2fb3e6c766b1caec3b70aef"
    },
    "index": 0,
    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
    "value": {
        "ada": {"lovelace": 3139346},
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "557b28208f00b0889bfb327d31c8264a2a32e7588d069f6716861a387106d507": 1
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
            "557b28208f00b0889bfb327d31c8264a2a32e7588d069f6716861a387106d507": 8664439
        },
        "f59bf53502caff7cf596113b524120fa2920c5e1076f422a3d27b2cf": {
            "444a4544": 26370346102
        },
    },
    "datumHash": "e8569755918879fa557aaa9db9aa232f3b3edf3435114c7ee33e1e50eabc376e",
}

djed_processed_content_ogmios = {
    "tx_hash": "5c6de452e76a4be2bf33483261db309e589fa2b4d2fb3e6c766b1caec3b70aef",
    "tx_index": 0,
    "amount": 3139346,
    "assets": {
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "557b28208f00b0889bfb327d31c8264a2a32e7588d069f6716861a387106d507": 1
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
            "557b28208f00b0889bfb327d31c8264a2a32e7588d069f6716861a387106d507": 8664439
        },
        "f59bf53502caff7cf596113b524120fa2920c5e1076f422a3d27b2cf": {
            "444a4544": 26370346102
        },
    },
}

usda_utxo_ogmios = {
    "transaction": {
        "id": "a093f8c53a1b284c0eeacde2e4c031976e19f27fc6f1cb66ba092e2adf3a3606"
    },
    "index": 0,
    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
    "value": {
        "ada": {"lovelace": 4353222},
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "bba34b54505f0d0f1cf2fe34fd53d427790993c1103c9216c74d9fa1f8b02f85": 1
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "203b808c61bdb0cc925ec1845ba105d9eb23a7e39de1d7864e0e542b": {"55534441": 120},
    },
    "datumHash": "40c5ff9c05ad77410fa52631f4080b4745054aaf062fc907d1b4f90fa719a1d0",
}

usda_processed_content_ogmios = {
    "tx_hash": "a093f8c53a1b284c0eeacde2e4c031976e19f27fc6f1cb66ba092e2adf3a3606",
    "tx_index": 0,
    "amount": 4353222,
    "assets": {
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "bba34b54505f0d0f1cf2fe34fd53d427790993c1103c9216c74d9fa1f8b02f85": 1
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "203b808c61bdb0cc925ec1845ba105d9eb23a7e39de1d7864e0e542b": {"55534441": 120},
    },
}

goblin_utxo_ogmios = {
    "transaction": {
        "id": "0004d98a9866522dbad164c567386c581830f02fc98859d77ac8485f91581b7c"
    },
    "index": 0,
    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
    "value": {
        "ada": {"lovelace": 2034322},
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "949bbe2d9d64248a9583475b5e2a990fed5c356751a0b08c2d86032b25038bbc": 1
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "eacbc20c2a1e1644cee500505aecd29381abdc5bf86955371297f435": {
            "476f626c696e": 55743
        },
    },
    "datumHash": "651e2292ae35ac140f832d0bb0c807a616525bf595eef2d8e8c747a4e9795195",
}

goblin_processed_content_ogmios = {
    "tx_hash": "0004d98a9866522dbad164c567386c581830f02fc98859d77ac8485f91581b7c",
    "tx_index": 0,
    "amount": 2034322,
    "assets": {
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "949bbe2d9d64248a9583475b5e2a990fed5c356751a0b08c2d86032b25038bbc": 1
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "eacbc20c2a1e1644cee500505aecd29381abdc5bf86955371297f435": {
            "476f626c696e": 55743
        },
    },
}

hosky_utxo_ogmios = {
    "transaction": {
        "id": "cf24a8e108e458f74d8b9c82e8dee8bf78ffc9ab826ff219808480af9a35ea29"
    },
    "index": 0,
    "address": "addr1x8nz307k3sr60gu0e47cmajssy4fmld7u493a4xztjrll0aj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrswgxsta",
    "value": {
        "ada": {"lovelace": 119180738},
        "8e89bd4d89f905e2fb18bd8aa29498444fa75046e84934155a820eef": {
            "484f534b595f4144415f4c51": 9223372036224324030
        },
        "99f53c6447b7e07f20852bc38e7040efe83deff794d42183a9eba093": {
            "484f534b595f4144415f4e4654": 1
        },
        "a0028f350aaabe0545fdcb56b039bfb08e4bb4d8c4d7c3c7d481c235": {
            "484f534b59": 3343607890
        },
    },
    "datum": "d8799fd8799f581c99f53c6447b7e07f20852bc38e7040efe83deff794d42183a9eba0934d484f534b595f4144415f4e4654ffd8799f4040ffd8799f581ca0028f350aaabe0545fdcb56b039bfb08e4bb4d8c4d7c3c7d481c23545484f534b59ffd8799f581c8e89bd4d89f905e2fb18bd8aa29498444fa75046e84934155a820eef4c484f534b595f4144415f4c51ff1903e59f581c156dd93e32916bb253f3df4ccb983134a6791299ef494bed29b8f29bff1b00000004a817c800ff",
}

hosky_processed_content_ogmios = {
    "tx_hash": "cf24a8e108e458f74d8b9c82e8dee8bf78ffc9ab826ff219808480af9a35ea29",
    "tx_index": 0,
    "amount": 119180738,
    "assets": {
        "8e89bd4d89f905e2fb18bd8aa29498444fa75046e84934155a820eef": {
            "484f534b595f4144415f4c51": 9223372036224324030
        },
        "99f53c6447b7e07f20852bc38e7040efe83deff794d42183a9eba093": {
            "484f534b595f4144415f4e4654": 1
        },
        "a0028f350aaabe0545fdcb56b039bfb08e4bb4d8c4d7c3c7d481c235": {
            "484f534b59": 3343607890
        },
    },
}

ogmios_tests = [
    (djed_utxo_ogmios, djed_processed_content_ogmios),
    (goblin_utxo_ogmios, goblin_processed_content_ogmios),
    (usda_utxo_ogmios, usda_processed_content_ogmios),
    (hosky_utxo_ogmios, hosky_processed_content_ogmios),
]


@pytest.mark.parametrize("content, result", ogmios_tests)
def test_get_ogmios_content(content: dict, result: dict):
    """Make sure we parse ogmios content correctly."""
    res = get_ogmios_utxo_content(content)
    assert res == result


output_one_asset_mock_ogmios = {
    "transaction": {"id": "MOCK_TX_ID"},
    "index": 0,
    "address": "MOCK_ADDR",
    "value": {
        "ada": {"lovelace": 10000000},
        "MOCK_POLICY": {"MOCK_POLICY_NAME": 100},
    },
    "datum": "MOCK_DATUM_ID",
}

output_one_asset_res_mock_ogmios = {
    "tx_hash": "MOCK_TX_ID",
    "tx_index": 0,
    "amount": 10000000,
    "assets": {"MOCK_POLICY": {"MOCK_POLICY_NAME": 100}},
}

output_three_asset_mock_ogmios = {
    "transaction": {"id": "MOCK_TX_ID"},
    "index": 4,
    "address": "MOCK_ADDR",
    "value": {
        "ada": {"lovelace": 20000000},
        "MOCK_POLICY_1": {"MOCK_POLICY_NAME": 200},
        "MOCK_POLICY_2": {"MOCK_POLICY_NAME": 200},
        "MOCK_POLICY_3": {"MOCK_POLICY_NAME": 200},
    },
    "datum": "MOCK_DATUM_ID",
}

output_three_asset_res_mock_ogmios = {
    "tx_hash": "MOCK_TX_ID",
    "tx_index": 4,
    "amount": 20000000,
    "assets": {
        "MOCK_POLICY_1": {"MOCK_POLICY_NAME": 200},
        "MOCK_POLICY_2": {"MOCK_POLICY_NAME": 200},
        "MOCK_POLICY_3": {"MOCK_POLICY_NAME": 200},
    },
}


ogmios_mock_tests = [
    (output_one_asset_mock_ogmios, output_one_asset_res_mock_ogmios),
    (output_three_asset_mock_ogmios, output_three_asset_res_mock_ogmios),
]


@pytest.mark.parametrize("content, result", ogmios_mock_tests)
def test_get_ogmios_content_mock(content: dict, result: dict):
    """Make sure we parse ogmios content correctly."""
    res = get_ogmios_utxo_content(content)
    assert res == result


kupo_one_asset = {
    "transaction_index": 6,
    "transaction_id": "4e65e205c9d9370b3138db4b76aa8bf222850fbd032278df9791d3a00738a205",
    "output_index": 0,
    "address": "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
    "value": {
        "coins": 8688003,
        "assets": {
            "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b.000de1408a26dfa60a90fd8b019deff41aa643c5979f67cfa59de8bcab9cf4a3": 1
        },
    },
    "datum_hash": "1b5b38a9e649681e6f132843c36fa68a82e9d59cd92e70d92ad2d242f26e1a3b",
    "datum_type": "inline",
    "script_hash": None,
    "created_at": {
        "slot_no": 129352061,
        "header_hash": "71cd9fd6fa27e6fd83ad653282e1977113b8063060a54b1fbbf649b8dd92a1e4",
    },
    "spent_at": None,
}

kupo_one_asset_result = {
    "tx_hash": "4e65e205c9d9370b3138db4b76aa8bf222850fbd032278df9791d3a00738a205",
    "tx_index": 0,
    "amount": 8688003,
    "assets": {
        "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b": {
            "000de1408a26dfa60a90fd8b019deff41aa643c5979f67cfa59de8bcab9cf4a3": 1
        }
    },
}

kupo_two_assets = {
    "transaction_index": 7,
    "transaction_id": "d309289a9787af2bf0748d9bc987ea697bef7f6756eb84f8baca29e3faf3b077",
    "output_index": 0,
    "address": "addr1x8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz26rnxqnmtv3hdu2t6chcfhl2zzjh36a87nmd6dwsu3jenqsslnz7e",
    "value": {
        "coins": 19976001,
        "assets": {
            "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b.000de140b141bfe9def67e636a12ad1a7ad7580a5377149f9460c8e6bcaccf57": 1,
            "4b4d39d5d98c871c2a0b0c043fdc486f0c233f9906efdd74f0051228.5343414d414c41": 31,
        },
    },
    "datum_hash": "4bee51da88f602bc8c1637f58039a8fdfa7fcdd339f51bcec6f8fd8340fb91ad",
    "datum_type": "inline",
    "script_hash": None,
    "created_at": {
        "slot_no": 138584007,
        "header_hash": "c5fbe3ff12d43212a78f006deaab24c67e25914f0748dd4135abf91fbc2890de",
    },
    "spent_at": None,
}

kupo_two_assets_result = {
    "tx_hash": "d309289a9787af2bf0748d9bc987ea697bef7f6756eb84f8baca29e3faf3b077",
    "tx_index": 0,
    "amount": 19976001,
    "assets": {
        "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b": {
            "000de140b141bfe9def67e636a12ad1a7ad7580a5377149f9460c8e6bcaccf57": 1
        },
        "4b4d39d5d98c871c2a0b0c043fdc486f0c233f9906efdd74f0051228": {
            "5343414d414c41": 31
        },
    },
}

kupo_four_assets = {
    "transaction_index": 21,
    "transaction_id": "2bb07c715d06c48f4ceaa485b2c93ee60253c2fee1e57add2af6f9bd63c07800",
    "output_index": 0,
    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
    "value": {
        "coins": 19384916,
        "assets": {
            "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86.5f1430dbc62d75ecdd444cae212ad6afeea31ba889399163d040e7193284022f": 58698,
            "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f.4d494e53574150": 1,
            "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1.5f1430dbc62d75ecdd444cae212ad6afeea31ba889399163d040e7193284022f": 1,
            "0166507c4d3c15ba9ecda98c0a9b3f95c49d6a2d35eb2e66399de352.4348455353": 1037459890,
        },
    },
    "datum_hash": "1fda7f68ffcd8f91e74e1ee240116f8f35f0c38c1f8f19e7edd98c32399e8ea9",
    "datum_type": "hash",
    "script_hash": None,
    "created_at": {
        "slot_no": 141124888,
        "header_hash": "563bc696074f5007465cd72313c54bc3beb16dafbe59c5ac2b570048fc594067",
    },
    "spent_at": None,
}

kupo_four_assets_result = {
    "tx_hash": "2bb07c715d06c48f4ceaa485b2c93ee60253c2fee1e57add2af6f9bd63c07800",
    "tx_index": 0,
    "amount": 19384916,
    "assets": {
        "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
            "5f1430dbc62d75ecdd444cae212ad6afeea31ba889399163d040e7193284022f": 58698
        },
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "5f1430dbc62d75ecdd444cae212ad6afeea31ba889399163d040e7193284022f": 1
        },
        "0166507c4d3c15ba9ecda98c0a9b3f95c49d6a2d35eb2e66399de352": {
            "4348455353": 1037459890
        },
    },
}

kupo_three_assets_positive_tx_idx = {
    "transaction_index": 4,
    "transaction_id": "8df312f3a79175380273a4bb946f4d723a78d49dfc674e6706be5d13b26d99f5",
    "output_index": 2,
    "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
    "value": {
        "coins": 6069884,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c.4d5350": 1,
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c.b9756dd3c710c36a73315e1eb7a2a6feba63aac86e7e29096db2b408597ae894": 9223371720627009789,
            "dd3bf2d07121a6263fd1ad074e204e44c3e2064c4cc999d66f781e9d.0014df104c4f414e": 66781155770321163,
        },
    },
    "datum_hash": "aaff85831d7b1bdfc6c6530e933b5897a695f3a1ab795a3d60a329d58a8ff63a",
    "datum_type": "inline",
    "script_hash": None,
    "created_at": {
        "slot_no": 151254169,
        "header_hash": "1a414e983f0088fae27feca85dc9e2e563dbe4200e4aa04d412853132f722a1c",
    },
    "spent_at": None,
}

kupo_three_assets_positive_tx_idx_res = {
    "tx_hash": "8df312f3a79175380273a4bb946f4d723a78d49dfc674e6706be5d13b26d99f5",
    "tx_index": 2,
    "amount": 6069884,
    "assets": {
        "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
            "4d5350": 1,
            "b9756dd3c710c36a73315e1eb7a2a6feba63aac86e7e29096db2b408597ae894": 9223371720627009789,
        },
        "dd3bf2d07121a6263fd1ad074e204e44c3e2064c4cc999d66f781e9d": {
            "0014df104c4f414e": 66781155770321163
        },
    },
}

kupo_utxo_trigger_index_error_qada = {
    "transaction_index": 4,
    "transaction_id": "beb8eb054e8fcc693669894d6ed95692c9fe79618f2421b470929be9e7c68bd0",
    "output_index": 0,
    "address": "addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
    "value": {
        "coins": 1059941085,
        "assets": {
            "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86.6d3a372ede61dd629c2c707bf54bb8174838d5f23764129004863c994c9915ce": 49886413,
            # Asset has no name.(it is apparently qADA).
            "a04ce7a52545e5e33c2867e148898d9e667a69602285f6a1298f9d68": 56211803302,
            "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f.4d494e53574150": 1,
            "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1.6d3a372ede61dd629c2c707bf54bb8174838d5f23764129004863c994c9915ce": 1,
        },
    },
    "datum_hash": "76a0502c4b70b76ad4601de50367a7a75e95382e6c66811bac58311d0da4ee74",
    "datum_type": "hash",
    "script_hash": None,
    "created_at": {
        "slot_no": 168107374,
        "header_hash": "5c83bc902d4437404520e61e7bb850c039892f8771d60091b533b534091d6a4d",
    },
    "spent_at": None,
}

kupo_res_trigger_index_error_qada = {
    "tx_hash": "beb8eb054e8fcc693669894d6ed95692c9fe79618f2421b470929be9e7c68bd0",
    "tx_index": 0,
    "amount": 1059941085,
    "assets": {
        "e4214b7cce62ac6fbba385d164df48e157eae5863521b4b67ca71d86": {
            "6d3a372ede61dd629c2c707bf54bb8174838d5f23764129004863c994c9915ce": 49886413
        },
        "a04ce7a52545e5e33c2867e148898d9e667a69602285f6a1298f9d68": {"": 56211803302},
        "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
            "4d494e53574150": 1
        },
        "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
            "6d3a372ede61dd629c2c707bf54bb8174838d5f23764129004863c994c9915ce": 1
        },
    },
}

kupo_utxo_trigger_index_error_qdjed = {
    "transaction_index": 0,
    "transaction_id": "01523811036890d66e2216d01ca5ddf4345dc944429e1fdd60ce46112bc713f2",
    "output_index": 1,
    "address": "addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
    "value": {
        "coins": 4500000,
        "assets": {
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c.4d5350": 1,
            "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c.e3c73badfb9403660eff44eeb622f977c2c556998e3e1420ae32044f7c18e1a2": 9223372027959847727,
            "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61.446a65644d6963726f555344": 1445989530,
            "6df63e2fdde8b2c3b3396265b0cc824aa4fb999396b1c154280f6b0c": 54745208948,
        },
    },
    "datum_hash": "81500dad9524ecdd9ad6d90f704d958c1bba99418a2e56f2d9d47fd0ac59aa52",
    "datum_type": "inline",
    "script_hash": None,
    "created_at": {
        "slot_no": 167763122,
        "header_hash": "27fb364ea7e33234c88adde9c17d56f2ae649d0cbad797309e899f966760e781",
    },
    "spent_at": None,
}

kupo_res_trigger_index_error_qdjed = {
    "tx_hash": "01523811036890d66e2216d01ca5ddf4345dc944429e1fdd60ce46112bc713f2",
    "tx_index": 1,
    "amount": 4500000,
    "assets": {
        "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
            "4d5350": 1,
            "e3c73badfb9403660eff44eeb622f977c2c556998e3e1420ae32044f7c18e1a2": 9223372027959847727,
        },
        "8db269c3ec630e06ae29f74bc39edd1f87c819f1056206e879a1cd61": {
            "446a65644d6963726f555344": 1445989530
        },
        "6df63e2fdde8b2c3b3396265b0cc824aa4fb999396b1c154280f6b0c": {"": 54745208948},
    },
}


kupo_tests = [
    (kupo_one_asset, kupo_one_asset_result),
    (kupo_two_assets, kupo_two_assets_result),
    (kupo_four_assets, kupo_four_assets_result),
    (kupo_three_assets_positive_tx_idx, kupo_three_assets_positive_tx_idx_res),
    # Checks whether asset name split fails in function call and
    # ensure the output is predictable.
    (kupo_utxo_trigger_index_error_qada, kupo_res_trigger_index_error_qada),
    (kupo_utxo_trigger_index_error_qdjed, kupo_res_trigger_index_error_qdjed),
]


@pytest.mark.parametrize("content, result", kupo_tests)
def test_get_kupo_content(content: dict, result: dict):
    """Make sure we parse kupo content correctly."""
    res = get_kupo_utxo_content(content)
    assert res == result


output_one_asset_mock_kupo = {
    "transaction_index": 0,
    "transaction_id": "MOCK_TX_ID",
    "output_index": 99,
    "address": "MOCK_ADDR",
    "value": {
        "coins": 10000000,
        "assets": {
            "MOCK_POLICY.MOCK_ASSET_NAME": 100,
        },
    },
    "datum_hash": "MOCK_DATUM_ID",
    "datum_type": "hash",
    "script_hash": None,
    "created_at": {
        "slot_no": 0,
        "header_hash": "MOCK_HEADER_HASH",
    },
    "spent_at": None,
}

output_one_asset_res_mock_kupo = {
    "tx_hash": "MOCK_TX_ID",
    "tx_index": 99,
    "amount": 10000000,
    "assets": {"MOCK_POLICY": {"MOCK_ASSET_NAME": 100}},
}

output_three_asset_mock_kupo = {
    "transaction_index": 0,
    "transaction_id": "MOCK_TX_ID",
    "output_index": 9,
    "address": "MOCK_ADDR",
    "value": {
        "coins": 20000000,
        "assets": {
            "MOCK_POLICY_1.MOCK_ASSET_NAME": 200,
            "MOCK_POLICY_2.MOCK_ASSET_NAME": 200,
            "MOCK_POLICY_3.MOCK_ASSET_NAME": 200,
        },
    },
    "datum_hash": "MOCK_DATUM_ID",
    "datum_type": "hash",
    "script_hash": None,
    "created_at": {
        "slot_no": 0,
        "header_hash": "MOCK_HEADER_HASH",
    },
    "spent_at": None,
}

output_three_asset_res_mock_kupo = {
    "tx_hash": "MOCK_TX_ID",
    "tx_index": 9,
    "amount": 20000000,
    "assets": {
        "MOCK_POLICY_1": {"MOCK_ASSET_NAME": 200},
        "MOCK_POLICY_2": {"MOCK_ASSET_NAME": 200},
        "MOCK_POLICY_3": {"MOCK_ASSET_NAME": 200},
    },
}


kupo_mock_tests = [
    (output_one_asset_mock_kupo, output_one_asset_res_mock_kupo),
    (output_three_asset_mock_kupo, output_three_asset_res_mock_kupo),
]


@pytest.mark.parametrize("content, result", kupo_mock_tests)
def test_get_kupo_content_mock(content: dict, result: dict):
    """Make sure we parse kupo content correctly."""
    res = get_kupo_utxo_content(content)
    assert res == result
