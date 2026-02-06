"""Provide characterization tests for search_db_utxo."""

# pylint: disable=E0401,R0902,R0913,R0914

import datetime
import sqlite3
from dataclasses import dataclass
from datetime import timezone

import pytest
import time_machine

from src.cnt_collector_node.database_initialization import _create_database
from src.cnt_collector_node.global_helpers import get_utc_timestamp_now
from src.cnt_collector_node.helper_functions import _search_db_utxo


@dataclass
class UTxO:
    """Object to hold UTxO information used in testing."""

    id: int
    pair: str
    source: str
    price: float
    block_height: int
    address: str
    token1_policy: str
    token1_name: str
    token1_decimals: str
    token2_policy: str
    token2_name: str
    token2_decimals: int
    security_token_policy: str
    security_token_name: str
    token1_amount: int
    token2_amount: int
    tx_hash: str
    output_index: int
    date_time: str


def get_utxo_obj(
    id_: int,
    pair: str,
    source: str,
    price: float,
    block_height: int,
    address: str,
    token1_policy: str,
    token1_name: str,
    token1_decimals: str,
    token2_policy: str,
    token2_name: str,
    token2_decimals: int,
    security_token_policy: str,
    security_token_name: str,
    token1_amount: int,
    token2_amount: int,
    tx_hash: str,
    output_index: int,
) -> UTxO:
    """Helper to return a UTxO object."""

    dt = get_utc_timestamp_now()

    return UTxO(
        id=id_,
        pair=pair,
        source=source,
        price=price,
        block_height=block_height,
        address=address,
        token1_policy=token1_policy,
        token1_name=token1_name,
        token1_decimals=token1_decimals,
        token2_policy=token2_policy,
        token2_name=token2_name,
        token2_decimals=token2_decimals,
        security_token_policy=security_token_policy,
        security_token_name=security_token_name,
        token1_amount=token1_amount,
        token2_amount=token2_amount,
        tx_hash=tx_hash,
        output_index=output_index,
        date_time=dt,
    )


def update_utxos(cursor: sqlite3.Cursor, utxo_data: UTxO):
    """Provide a way to write UTxO information to the database."""
    cursor.execute(
        "INSERT INTO utxos(id, pair, source, price, block_height, address, "
        "token1_policy, token1_name, token1_decimals, "
        "token2_policy, token2_name, token2_decimals, "
        "security_token_policy, security_token_name, "
        "token1_amount, token2_amount, tx_hash, output_index, date_time) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            utxo_data.id,
            utxo_data.pair,
            utxo_data.source,
            utxo_data.price,
            utxo_data.block_height,
            utxo_data.address,
            utxo_data.token1_policy,
            utxo_data.token1_name,
            utxo_data.token1_decimals,
            utxo_data.token2_policy,
            utxo_data.token2_name,
            utxo_data.token2_decimals,
            utxo_data.security_token_policy,
            utxo_data.security_token_name,
            utxo_data.token1_amount,
            utxo_data.token2_amount,
            utxo_data.tx_hash,
            utxo_data.output_index,
            utxo_data.date_time,
        ),
    )


search_db_tests = [
    (
        get_utxo_obj(
            id_=34,
            pair="IAG-ADA",
            source="SundaeSwapV3",
            price=0.15631478676906585,
            block_height=169718986,
            address="addr1z8srqftqemf0mjlukfszd97ljuxdp44r372txfcr75wrz2auzrlrz2kdd83wzt9u9n9qt2swgvhrmmn96k55nq6yuj4qw992w9",
            token1_policy="5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
            token1_name="494147",
            token1_decimals=6,
            token2_policy="",
            token2_name="lovelace",
            token2_decimals=6,
            security_token_policy="e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b",
            security_token_name="000de1406f79e3e55eef82b9d03cf62cc3d4a6d0d03b00bf7b1b43330f829779",
            token1_amount=2755734048970,
            token2_amount=430761980257,
            tx_hash="70d27c82fd12e79c26df7df8a68cd0b7069dbfd2790f4190c90746a3946e7805",
            output_index=0,
        ),
        [
            {
                "transaction": {
                    "id": "6791175991230f5b9e50fefb9271aac9a44c5682209d9323953e08d3d46814af"
                },
                "index": 2,
            },
            {
                "transaction": {
                    "id": "6cd038d5dd600ce25c4389d6715b06eb64ac34fb75bbf7f927b0abefaa4529fb"
                },
                "index": 0,
            },
            {
                "transaction": {
                    "id": "70d27c82fd12e79c26df7df8a68cd0b7069dbfd2790f4190c90746a3946e7805"
                },
                "index": 0,
            },
        ],
        {
            "amount": 431693260257,
            "assets": {
                "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114": {
                    "494147": 2749853340170
                },
                "e0302560ced2fdcbfcb2602697df970cd0d6a38f94b32703f51c312b": {
                    "000de1406f79e3e55eef82b9d03cf62cc3d4a6d0d03b00bf7b1b43330f829779": 1
                },
            },
        },
        [34],
    ),
    (
        get_utxo_obj(
            id_=32,
            pair="IAG-ADA",
            source="MinSwapV2",
            price=0.15676051778723948,
            block_height=169718986,
            address="addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
            token1_policy="5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114",
            token1_name="494147",
            token1_decimals=6,
            token2_policy="",
            token2_name="lovelace",
            token2_decimals=6,
            security_token_policy="f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
            security_token_name="4d5350",
            token1_amount=7693068113661,
            token2_amount=41205969340870,
            tx_hash="550ecf65691e37a8474ebc1e122b34ad6bb5abd8cc11426a1e1e230018b4163e",
            output_index=1,
        ),
        [
            {
                "transaction": {
                    "id": "3ba317b4bfa182474143e107f3461ffcb37d57a82271688fbc7d4648a7d5eb26"
                },
                "index": 0,
            },
            {
                "transaction": {
                    "id": "550ecf65691e37a8474ebc1e122b34ad6bb5abd8cc11426a1e1e230018b4163e"
                },
                "index": 1,
            },
            {
                "transaction": {
                    "id": "6cd038d5dd600ce25c4389d6715b06eb64ac34fb75bbf7f927b0abefaa4529fb"
                },
                "index": 1,
            },
            {
                "transaction": {
                    "id": "c6f021a621571ada01d031e8d87a03fb8b094a07cfd632f31ce094ebaa690ffe"
                },
                "index": 2,
            },
        ],
        {
            "amount": 1208639340870,
            "assets": {
                "5d16cc1a177b5d9ba9cfa9793b07e60f1fb70fea1f8aef064415d114": {
                    "494147": 7676200531425
                },
                "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                    "4d5350": 1,
                    "7b12f25ce8d6f424e1edbc8b61f0742fb13252605f31dc40373d6a245e8ec1d1": 9223369540414810468,
                },
            },
        },
        [32],
    ),
    (
        get_utxo_obj(
            id_=1,
            pair="SNEK-ADA",
            source="MinSwap",
            price=0.004039478916100507,
            block_height=169719625,
            address="addr1z8snz7c4974vzdpxu65ruphl3zjdvtxw8strf2c2tmqnxz2j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq0xmsha",
            token1_policy="279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f",
            token1_name="534e454b",
            token1_decimals=0,
            token2_policy="",
            token2_name="lovelace",
            token2_decimals=6,
            security_token_policy="0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1",
            security_token_name="63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896",
            token1_amount=34477920,
            token2_amount=4139272830911,
            tx_hash="0168966cd2a4130f9092147277b626fa6f8fbe8904f33d9465fe313be58b24e3",
            output_index=0,
        ),
        [
            {
                "transaction": {
                    "id": "0168966cd2a4130f9092147277b626fa6f8fbe8904f33d9465fe313be58b24e3"
                },
                "index": 0,
            },
            {
                "transaction": {
                    "id": "744efcf69c09c060409d6a05df013a68d539736d1c63d54d2a6a168d3d33b2d5"
                },
                "index": 2,
            },
            {
                "transaction": {
                    "id": "847ff1c5d4c59e9ba3a2dd23d60170105c25f221e21eb02075ddbc26e46baf81"
                },
                "index": 1,
            },
        ],
        {
            "amount": 139764967615,
            "assets": {
                "0be55d262b29f564998ff81efe21bdc0022621c12f15af08d0f2ddb1": {
                    "63f2cbfa5bf8b68828839a2575c8c70f14a32f50ebbfa7c654043269793be896": 1
                },
                "13aa2accf2e1561723aa26871e071fdf32c867cff7e7d50ad470d62f": {
                    "4d494e53574150": 1
                },
                "279c909f348e533da5808898f87f9a14bb2c3dfbbacccd631d927a3f": {
                    "534e454b": 34356881
                },
            },
        },
        [1],
    ),
    (
        get_utxo_obj(
            id_=20,
            pair="ADA-iUSD",
            source="MinSwapV2",
            price=0.6499717981109374,
            block_height=169720195,
            address="addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
            token1_policy="",
            token1_name="lovelace",
            token1_decimals=6,
            token2_policy="f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880",
            token2_name="69555344",
            token2_decimals=6,
            security_token_policy="f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
            security_token_name="4d5350",
            token1_amount=890165897906,
            token2_amount=4578582729279,
            tx_hash="31962b4bd42b91fbf9a7fb88cccc18b8423f46ed436099aba3d34c29af6e1fae",
            output_index=1,
        ),
        [
            {
                "transaction": {
                    "id": "1d2b0cde3004b6a14823b6942393c398c2a28516c666a3a851bc91d73f028021"
                },
                "index": 2,
            },
            {
                "transaction": {
                    "id": "31962b4bd42b91fbf9a7fb88cccc18b8423f46ed436099aba3d34c29af6e1fae"
                },
                "index": 1,
            },
            {
                "transaction": {
                    "id": "7c36362df253e07b4ebbb7ecabf107b555a7ca6d4026d66249e313d4a9f70b90"
                },
                "index": 2,
            },
        ],
        {
            "amount": 890263947906,
            "assets": {
                "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                    "452089abb5bf8cc59b678a2cd7b9ee952346c6c0aa1cf27df324310a70d02fc3": 9223371372683754595,
                    "4d5350": 1,
                },
                "f66d78b4a3cb3d37afa0ec36461e51ecbde00f26c8f0a68f94b69880": {
                    "69555344": 578519198287
                },
            },
        },
        [20],
    ),
    (
        get_utxo_obj(
            id_=9,
            pair="INDY-ADA",
            source="MinSwapV2",
            price=1.3721944178236085,
            block_height=169732417,
            address="addr1z84q0denmyep98ph3tmzwsmw0j7zau9ljmsqx6a4rvaau66j2c79gy9l76sdg0xwhd7r0c0kna0tycz4y5s6mlenh8pq777e2a",
            token1_policy="533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0",
            token1_name="494e4459",
            token1_decimals=6,
            token2_policy="",
            token2_name="lovelace",
            token2_decimals=6,
            security_token_policy="f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c",
            security_token_name="4d5350",
            token1_amount=323211311009,
            token2_amount=443508756744,
            tx_hash="287437780f9129e223569e042d2bdebe437a2c23d74aacf80a37b9a110b92b1e",
            output_index=1,
        ),
        [
            {
                "transaction": {
                    "id": "287437780f9129e223569e042d2bdebe437a2c23d74aacf80a37b9a110b92b1e"
                },
                "index": 1,
            },
            {
                "transaction": {
                    "id": "2b3e25402a6cc80a0bd15972a492937f60862c1c69a9312212aedd24974d5e3d"
                },
                "index": 2,
            },
            {
                "transaction": {
                    "id": "bd47b34e53a2e5f6321c645a919f1c3ee44d27b2b53d07e6d4dddb5455a201ca"
                },
                "index": 1,
            },
        ],
        {
            "amount": 442979537165,
            "assets": {
                "533bb94a8850ee3ccbe483106489399112b74c905342cb1792a797a0": {
                    "494e4459": 323601334595
                },
                "f5808c2c990d86da54bfc97d89cee6efa20cd8461616359478d96b4c": {
                    "4d5350": 1,
                    "9b65707373c4cec488b16151a64d7102dbae16857c500652b5c513650b8d604e": 9223371681410224192,
                },
            },
        },
        [9],
    ),
]


@time_machine.travel(datetime.datetime(2018, 2, 19, 12, 55, 00, tzinfo=timezone.utc))
@pytest.mark.parametrize(
    "db_entry, tx_inputs, output_contents, expected", search_db_tests
)
def test_search_db_utxo(
    db_entry: UTxO, tx_inputs: list, output_contents: dict, expected: list
):
    """Ensure the search db utxo function works as expected."""

    conn = sqlite3.connect(":memory:")

    cursor = conn.cursor()

    _create_database(conn)

    # Write an initial data state to the database.
    update_utxos(cursor, db_entry)

    # Retrieve a result from the database and check it matches.
    res = _search_db_utxo(conn, tx_inputs, output_contents)
    assert res == expected
