"""Test the save_utxo_record function.

There's a lot going on in this function so it has expanded over a
number of lines. As we refactor it should get simpler.

NB. there's also just a lot of data so it takes space.
"""

# pylint: disable=R0914

import copy
import sqlite3

import pytest

import src.cnt_collector_node.database_abstraction as dba
from src.cnt_collector_node import config, utxo_objects
from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.helper_functions import save_utxo_record, update_status

context_mock_1 = utxo_objects.UTxOUpdateContext(
    address=None,
    epoch=None,
    block_height=None,
    tx_hash="mock_hash",
    output_index="mock_index",
    caller="save_output",
    token_1_amount=None,
    token_2_amount=None,
    price=None,
    utxo_ids=None,
)

record_mock_1 = {
    "pair": "1",
    "source": "2",
}

ins_mock_1 = (
    "mock_hash",
    "mock_index" "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
)

rollback_mocks = [(context_mock_1, record_mock_1, ins_mock_1)]


@pytest.mark.parametrize("context, record, ins_mock", rollback_mocks)
def test_rollback_mock(
    context: utxo_objects.UTxOUpdateContext, record: dict, ins_mock: tuple
):
    """Make sure save UTxO record performs consistently.

    Return update if there has been a rollback but only uses mock
    data to ensure behavior works as anticipated. Ideally we would have
    some real data to use.

        cur.execute(
            "SELECT count(*) FROM utxos WHERE tx_hash = ? AND output_index = ?",
            (context["tx_hash"], context["output_index"]),
        )

    """
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )
    cursor.execute(
        "INSERT INTO utxos(tx_hash, output_index, "
        "pair, source, price, block_height, address, "
        "token1_policy, token1_name, token1_decimals, "
        "token2_policy, token2_name, token2_decimals, "
        "security_token_policy, security_token_name, "
        "token1_amount, token2_amount, date_time) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ins_mock,
    )
    context_copy = copy.deepcopy(context)
    record_copy = copy.deepcopy(record)
    updated = save_utxo_record(
        database=db,
        utxo_update_context=context,
        tokens_pair=utxo_objects.TokensPair(
            pair=record["pair"],
            source=record["source"],
            token_1_policy=record.get("token1_policy"),
            token_1_name=record.get("token1_name"),
            token_1_decimals=record.get("token1_decimals"),
            token_2_policy=record.get("token2_policy"),
            token_2_name=record.get("token2_name"),
            token_2_decimals=record.get("token2_decimals"),
            security_token_policy=record.get("security_token_policy"),
            security_token_name=record.get("security_token_name"),
        ),
    )
    assert not updated
    # Repeat select from rollback code to help protect against a
    # false positives.
    cursor.execute(
        "SELECT count(*) FROM utxos WHERE tx_hash = ? AND output_index = ?",
        (ins_mock[0], ins_mock[1]),
    )
    res = cursor.fetchone()
    assert res[0] == 1
    cursor.execute(
        "SELECT count(*) FROM utxos WHERE tx_hash = ? AND output_index = ?",
        ("no_value", ins_mock[1]),
    )
    res = cursor.fetchone()
    assert res[0] == 0
    cursor.execute(
        "SELECT count(*) FROM utxos WHERE tx_hash = ? AND output_index = ?",
        (ins_mock[0], "no value"),
    )
    res = cursor.fetchone()
    assert res[0] == 0
    # Make sure we haven't any existing state in the status table.
    status_select = "select current_block_slot from status"
    cursor.execute(status_select)
    res = cursor.fetchone()
    assert res is None
    # Make sure out functions preserve the immutability of our
    # objects.
    assert context == context_copy
    assert record == record_copy


# LQ-ADA on WingRidersV2
lq_utxo_ids_is_1_context = utxo_objects.UTxOUpdateContext(
    address="addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhq6pr0ayfupfkpyjs0lxpyulnd9wq4ct2zmaz0rg0e8zpjyq7wxle2",
    epoch=587,
    block_height=168354553,
    tx_hash="1d5051d8cff29474c4279c964f431b235dd3d105566780f29242e9f76b49b2a3",
    output_index=0,
    caller="save_output",
    token_1_amount=10429526267,
    token_2_amount=40791731240,
    price=3.9111777654819155,
    utxo_ids=[154],
)


lq_utxo_ids_is_1_record = {
    "pair": "LQ-ADA",
    "source": "WingRidersV2",
    "token1_policy": "da8c30857834c6ae7203935b89278c532b3995245295456f993e1d24",
    "token1_name": "4c51",
    "token1_decimals": 6,
    "token2_policy": "",
    "token2_name": "lovelace",
    "token2_decimals": 6,
    "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
    "security_token_name": "4c",
}

lq_utxo_ids_is_1_row = (
    154,
    168354353,
    10381526266,
    6,
    40979662965,
    6,
    "eefaeeffd6e3ef7856ab50d24a610922594b1963678e2ca163528513067f0a68",
    0,
)

lq_utxo_ids_is_1_row_res = (
    154,
    168354553,
    10429526267,
    6,
    40791731240,
    6,
    "1d5051d8cff29474c4279c964f431b235dd3d105566780f29242e9f76b49b2a3",
    0,
)

# IAG-ADA on MinSwapV2
iag_utxo_ids_is_1_context = utxo_objects.UTxOUpdateContext(
    address="addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
    epoch=587,
    block_height=168354696,
    tx_hash="59b0ab4aaea03a92314bf235b135e41bff8389e2058ce3dfff862d532dfd021c",
    output_index=1,
    caller="save_output",
    token_1_amount=8228648656559,
    token_2_amount=1296910079101,
    price=0.15760912067466165,
    utxo_ids=[29],
)


iag_utxo_ids_is_1_record = {
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
}

iag_utxo_ids_is_1_row = (
    29,
    168354305,
    8245433791687,
    6,
    1294250079101,
    6,
    "b9dbe03e874311e5899ac398fc654c8050a125ff9e271ea9eb9894295e0a75df",
    1,
)

iag_utxo_ids_is_1_row_res = (
    29,
    168354696,
    8228648656559,
    6,
    1296910079101,
    6,
    "59b0ab4aaea03a92314bf235b135e41bff8389e2058ce3dfff862d532dfd021c",
    1,
)

# WMTX-ADA on MinSwapV2
wmtx_utxo_ids_is_1_context = utxo_objects.UTxOUpdateContext(
    address="addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
    epoch=587,
    block_height=168354696,
    tx_hash="06692a011feb30268c9bc0b66bdfb5dd346fd7a90a00aa6ce2b2f615b8d672ad",
    output_index=1,
    caller="save_output",
    token_1_amount=3902046167401,
    token_2_amount=926058271061,
    price=0.23732632350626726,
    utxo_ids=[144],
)

wmtx_utxo_ids_is_1_record = {
    "pair": "WMTX-ADA",
    "source": "MinSwapV2",
    "token1_policy": "e5a42a1a1d3d1da71b0449663c32798725888d2eb0843c4dabeca05a",
    "token1_name": "576f726c644d6f62696c65546f6b656e58",
    "token1_decimals": 6,
    "token2_policy": "",
    "token2_name": "lovelace",
    "token2_decimals": 6,
    "security_token_policy": "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
    "security_token_name": "4d5350",
}

wmtx_utxo_ids_is_1_row = (
    144,
    168354305,
    3899942200177,
    6,
    926556367930,
    6,
    "85648952c8ec1ecbdefad67e7594d029acccc355491e623563e764742fedc12f",
    1,
)

wmtx_utxo_ids_is_1_row_res = (
    144,
    168354696,
    3902046167401,
    6,
    926058271061,
    6,
    "06692a011feb30268c9bc0b66bdfb5dd346fd7a90a00aa6ce2b2f615b8d672ad",
    1,
)

# ADA-iUSD on WingRiders.
iusd_utxo_ids_is_1_context = utxo_objects.UTxOUpdateContext(
    address="addr1z8nvjzjeydcn4atcd93aac8allvrpjn7pjr2qsweukpnaytpq3ryg76qvca5eu9c6py33ncg8zf09nh7gy2cvdps2yeqlvvkfh",
    epoch=587,
    block_height=168361147,
    tx_hash="2f742ae5f60690f3b6166306dcfd3d7901526770ce3f3ed7a8251cdd1c297a23",
    output_index=0,
    caller="save_output",
    token_1_amount=3192723111,
    token_2_amount=2632549164,
    price=0.824546655777943,
    utxo_ids=[70],
)

iusd_utxo_ids_is_1_record = {
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
}

iusd_utxo_ids_is_1_row = (
    70,
    168361030,
    3130723111,
    6,
    2684548592,
    6,
    "1645ad4dd9572f7e604fcb8e3e29ac1dd7991a6a46eb83d936f5fa31c4f40593",
    0,
)

iusd_utxo_ids_is_1_row_res = (
    70,
    168361147,
    3192723111,
    6,
    2632549164,
    6,
    "2f742ae5f60690f3b6166306dcfd3d7901526770ce3f3ed7a8251cdd1c297a23",
    0,
)

save_output_records_tests_1 = [
    # save_output (is set) and utxo_ids == 1
    (
        lq_utxo_ids_is_1_context,
        lq_utxo_ids_is_1_record,
        lq_utxo_ids_is_1_row,
        lq_utxo_ids_is_1_row_res,
    ),
    (
        iag_utxo_ids_is_1_context,
        iag_utxo_ids_is_1_record,
        iag_utxo_ids_is_1_row,
        iag_utxo_ids_is_1_row_res,
    ),
    (
        wmtx_utxo_ids_is_1_context,
        wmtx_utxo_ids_is_1_record,
        wmtx_utxo_ids_is_1_row,
        wmtx_utxo_ids_is_1_row_res,
    ),
    (
        iusd_utxo_ids_is_1_context,
        iusd_utxo_ids_is_1_record,
        iusd_utxo_ids_is_1_row,
        iusd_utxo_ids_is_1_row_res,
    ),
]


@pytest.mark.parametrize("context, record, row, row_res", save_output_records_tests_1)
def test_save_utxo_record_1(
    context: utxo_objects.UTxOUpdateContext, record: dict, row: tuple, row_res: tuple
):
    """Make sure save UTxO record performs consistently.

    Tests for saving the UTxO if `save_output` is set and utxo length
    is one and the result is positive, i.e. exists.
    """
    assert len(context.utxo_ids) == 1
    assert context.caller == "save_output"
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )
    utxo_id = context.utxo_ids[0]
    ins_mock = row + ("", "", "", "", "", "", "", "", "", "")
    cursor.execute(
        "INSERT INTO utxos(id, block_height, token1_amount, "
        "token1_decimals, token2_amount, token2_decimals, tx_hash, "
        "output_index, pair, source, price, address, token1_policy, "
        "token1_name, token2_policy, token2_name, security_token_policy, "
        "security_token_name)"
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ins_mock,
    )
    save_output_select = f"""
        SELECT id, block_height, token1_amount, token1_decimals,
        token2_amount, token2_decimals, tx_hash, output_index
        FROM utxos WHERE id = {utxo_id}
        """.strip().replace(
        "  ", " "
    )
    cursor.execute(save_output_select)
    res = cursor.fetchone()
    assert res
    # There's a lot going on in the original function, but while we're
    # in it, make sure our guard rail works. Copy the tmp_min amount
    # and make sure we don't write to the DB when its value is super
    # high then set it back and ensure it does write.
    tmp_min_amount = config.MIN_ADA_AMOUNT
    config.MIN_ADA_AMOUNT = 999999999
    # Make sure we haven't any existing state in the status table.
    status_select = "select current_block_slot from status"
    cursor.execute(status_select)
    res = cursor.fetchone()
    assert res is None
    # Save the record.
    context_copy = copy.deepcopy(context)
    record_copy = copy.deepcopy(record)
    updated = save_utxo_record(
        database=db,
        utxo_update_context=context,
        tokens_pair=utxo_objects.TokensPair(
            pair=record["pair"],
            source=record["source"],
            token_1_policy=record["token1_policy"],
            token_1_name=record["token1_name"],
            token_1_decimals=record["token1_decimals"],
            token_2_policy=record["token2_policy"],
            token_2_name=record["token2_name"],
            token_2_decimals=record["token2_decimals"],
            security_token_policy=record["security_token_policy"],
            security_token_name=record["security_token_name"],
        ),
    )
    assert updated is False
    config.MIN_ADA_AMOUNT = tmp_min_amount
    updated = save_utxo_record(
        database=db,
        utxo_update_context=context,
        tokens_pair=utxo_objects.TokensPair(
            pair=record["pair"],
            source=record["source"],
            token_1_policy=record["token1_policy"],
            token_1_name=record["token1_name"],
            token_1_decimals=record["token1_decimals"],
            token_2_policy=record["token2_policy"],
            token_2_name=record["token2_name"],
            token_2_decimals=record["token2_decimals"],
            security_token_policy=record["security_token_policy"],
            security_token_name=record["security_token_name"],
        ),
    )
    assert updated
    cursor.execute(save_output_select)
    res = cursor.fetchone()
    assert res == row_res
    assert res[1] > row[1]
    cursor.execute(status_select)
    res = cursor.fetchone()
    assert res[0] == context.block_height
    # Make sure out functions preserve the immutability of our
    # objects.
    assert context == context_copy
    assert record == record_copy


# ADA-USDA on WingRidersV2.
usda_save_utxo_context = utxo_objects.UTxOUpdateContext(
    address="addr1zxhew7fmsup08qvhdnkg8ccra88pw7q5trrncja3dlszhqc5jq5npz5xdnmdzjh7ez6e4j5xst29eqgcnmzyf60zmadsq3q9h0",
    epoch=587,
    block_height=168366523,
    tx_hash="8c587b3587eed1f21534cad43fd0edab9c06d53731c7bfba91ad1b69a464c6ef",
    output_index=0,
    caller="save_utxo",
    token_1_amount=339155613541,
    token_2_amount=278920823192,
    price=0.8223977786476522,
)


usda_save_utxo_record = {
    "pair": "ADA-USDA",
    "source": "WingRidersV2",
    "token1_policy": "",
    "token1_name": "lovelace",
    "token1_decimals": 6,
    "token2_policy": "fe7c786ab321f41c654ef6c1af7b3250a613c24e4213e0425a7ae456",
    "token2_name": "55534441",
    "token2_decimals": 6,
    "security_token_policy": "6fdc63a1d71dc2c65502b79baae7fb543185702b12c3c5fb639ed737",
    "security_token_name": "4c",
}

usda_save_utxo_row_res = (
    1,
    168366523,
    339155613541,
    6,
    278920823192,
    6,
    "8c587b3587eed1f21534cad43fd0edab9c06d53731c7bfba91ad1b69a464c6ef",
    0,
)


# ADA-USDM on SpectrumFACT-ADA on MinSwap.
usdm_save_utxo_context = utxo_objects.UTxOUpdateContext(
    address="addr1x94ec3t25egvhqy2n265xfhq882jxhkknurfe9ny4rl9k6dj764lvrxdayh2ux30fl0ktuh27csgmpevdu89jlxppvrst84slu",
    epoch=587,
    block_height=168366523,
    tx_hash="92997dc8d184b22ee78868318fde8dab4f2de94da36c22c9db09f68f6eb148a2",
    output_index=0,
    caller="save_utxo",
    token_1_amount=13459399,
    token_2_amount=11333515,
    price=0.8420520856837664,
    utxo_ids=[],
)

usdm_save_utxo_record = {
    "pair": "ADA-USDM",
    "source": "Spectrum",
    "token1_policy": "",
    "token1_name": "lovelace",
    "token1_decimals": 6,
    "token2_policy": "c48cbb3d5e57ed56e276bc45f99ab39abe94e6cd7ac39fb402da47ad",
    "token2_name": "0014df105553444d",
    "token2_decimals": 6,
    "security_token_policy": "fd0b614f52f2286df3b4db4fc70656bcd3df4877e909fd5a44e956f0",
    "security_token_name": "0014efbfbd105553444d5f4144415f4e4654",
}

usdm_save_utxo_row_res = (
    1,
    168366523,
    13459399,
    6,
    11333515,
    6,
    "92997dc8d184b22ee78868318fde8dab4f2de94da36c22c9db09f68f6eb148a2",
    0,
)

# FACT-ADA on MinSwap.
fact_save_utxo_context = utxo_objects.UTxOUpdateContext(
    address="addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
    epoch=587,
    block_height=168366523,
    tx_hash="897c3c4ea64542238aa85c79ed5844a5339f7156f2b8ac01bccf52a69dd22e07",
    output_index=0,
    caller="save_utxo",
    token_1_amount=91061757796,
    token_2_amount=404595751,
    price=0.0044430918180427695,
)


fact_save_utxo_record = {
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
}

fact_save_utxo_row_res = (
    1,
    168366523,
    91061757796,
    6,
    404595751,
    6,
    "897c3c4ea64542238aa85c79ed5844a5339f7156f2b8ac01bccf52a69dd22e07",
    0,
)


save_utxo_records_tests_1 = [
    (usda_save_utxo_context, usda_save_utxo_record, usda_save_utxo_row_res),
    (usdm_save_utxo_context, usdm_save_utxo_record, usdm_save_utxo_row_res),
    (fact_save_utxo_context, fact_save_utxo_record, fact_save_utxo_row_res),
]


@pytest.mark.parametrize("context, record, row_res", save_utxo_records_tests_1)
def test_update_utxo_record(
    context: utxo_objects.UTxOUpdateContext, record: dict, row_res: tuple
):
    """Make sure save UTxO record performs consistently.

    Tests for updating the UTxO if it does exist.
    """
    assert (
        context.utxo_ids == []
    ), "utxo_ids should be [] by default factory or user setting"
    assert context.caller != "save_output"
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )
    save_utxo_select = """
        SELECT id, block_height, token1_amount, token1_decimals,
        token2_amount, token2_decimals, tx_hash, output_index
        FROM utxos WHERE pair = ? AND source = ?
        AND address = ? AND security_token_policy = ? AND security_token_name = ?
    """.strip().replace(
        "  ", " "
    )
    cursor.execute(
        save_utxo_select,
        (
            record["pair"],
            record["source"],
            context.address,
            record["security_token_policy"],
            record["security_token_name"],
        ),
    )
    res = cursor.fetchone()
    assert res is None
    # Make sure we haven't any existing state in the status table.
    status_select = "select current_block_slot from status"
    cursor.execute(status_select)
    res = cursor.fetchone()
    assert res is None
    # Save the record.
    context_copy = copy.deepcopy(context)
    record_copy = copy.deepcopy(record)
    updated = save_utxo_record(
        database=db,
        utxo_update_context=context,
        tokens_pair=utxo_objects.TokensPair(
            pair=record["pair"],
            source=record["source"],
            token_1_policy=record["token1_policy"],
            token_1_name=record["token1_name"],
            token_1_decimals=record["token1_decimals"],
            token_2_policy=record["token2_policy"],
            token_2_name=record["token2_name"],
            token_2_decimals=record["token2_decimals"],
            security_token_policy=record["security_token_policy"],
            security_token_name=record["security_token_name"],
        ),
    )
    assert updated
    # Call the select_utxo query again and it should be populated.
    cursor.execute(
        save_utxo_select,
        (
            record["pair"],
            record["source"],
            context.address,
            record["security_token_policy"],
            record["security_token_name"],
        ),
    )
    res = cursor.fetchone()
    assert res == row_res
    # Make sure the block state record has been added or updated.
    cursor.execute(status_select)
    res = cursor.fetchone()
    assert res[0] == context.block_height
    # Make sure out functions preserve the immutability of our
    # objects.
    assert context == context_copy
    assert record == record_copy


update_status_tests = [
    (-1, 1, True),
    (0, 1, True),
    (10, 9, False),
]


@pytest.mark.parametrize("old, new, updated", update_status_tests)
def test_update_status(old: int, new: int, updated: bool):
    """Rudimentary test to ensure update_status works as anticipated."""
    conn = sqlite3.connect(":memory:")
    _create_database(conn)
    cursor = conn.cursor()
    db = dba.DBObject(
        connection=conn,
        cursor=cursor,
    )
    if old >= 0:
        # Write a mock value to act as a previous value.
        cursor.execute(
            "INSERT INTO status(current_block_slot, date_time) VALUES(?, ?)",
            (old, ""),
        )
    update_status(db_name="", database=db, block=new)
    cursor.execute("select current_block_slot from status")
    res = cursor.fetchall()
    assert len(res) == 1
    assert (res[0][0] == new) is updated
