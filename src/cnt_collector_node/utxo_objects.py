"""Library containing objects used throughout the CNT script."""

# pylint: disable=R0902

from dataclasses import dataclass, field
from typing import Union


@dataclass(frozen=True)
class InitialChainContext:
    """Chain context object to provide functions with current chain
    state information.
    """

    block_height: int
    epoch: int
    address: str
    tx_hash: str
    output_index: int
    utxo_ids: list[int] = field(default_factory=list)


@dataclass(frozen=True)
class UTxOUpdateContext:
    """Chain context object to provide functions with current chain
    state information.
    """

    # Most common attributes used by this object.
    address: str
    epoch: int
    block_height: int
    tx_hash: str
    output_index: int

    # UTxOs is from the InitialContext. It might belong in another
    # semantic unit, not this context.
    utxo_ids: list[int] = field(default_factory=list)

    # Attributes that aren't always passed around by an object.
    caller: Union[str | None] = None
    token_1_amount: Union[int | None] = None
    token_2_amount: Union[int | None] = None
    price: Union[float | None] = None


def chain_context_adapter(utxo_update_context: UTxOUpdateContext):
    """Provide a temporarry method to convert the UTxO update object
    back into a dictionary to support legacy functions that take time
    to overwrite and update.
    """

    return {
        "block_height": utxo_update_context.block_height,
        "epoch": utxo_update_context.epoch,
        "address": utxo_update_context.address,
        "tx_hash": utxo_update_context.tx_hash,
        "output_index": utxo_update_context.output_index,
        "caller": utxo_update_context.caller,
        "token1_amount": utxo_update_context.token_1_amount,
        "token2_amount": utxo_update_context.token_2_amount,
        "price": utxo_update_context.price,
    }


@dataclass(frozen=True)
class TokensPair:
    """Token pair object to allow token data to be passed around the
    application.
    """

    pair: str
    source: str
    token_1_policy: str
    token_1_name: str
    token_1_decimals: int
    token_2_policy: str
    token_2_name: str
    token_2_decimals: int
    security_token_policy: str
    security_token_name: str

    # submitter specific.
    address: str = None
    collector: str = None


def tokens_pair_from_dict(tokens_pair_dict: dict) -> TokensPair:
    """Helper function to return a tokens pair object from a
    dictionary.
    """
    try:
        return TokensPair(
            pair=tokens_pair_dict["pair"],
            source=tokens_pair_dict["source"],
            token_1_policy=tokens_pair_dict["token1_policy"],
            token_1_name=tokens_pair_dict["token1_name"],
            token_1_decimals=tokens_pair_dict["token1_decimals"],
            token_2_policy=tokens_pair_dict["token2_policy"],
            token_2_name=tokens_pair_dict["token2_name"],
            token_2_decimals=tokens_pair_dict["token2_decimals"],
            security_token_policy=tokens_pair_dict["security_token_policy"],
            security_token_name=tokens_pair_dict["security_token_name"],
            address=tokens_pair_dict["address"],
            collector=tokens_pair_dict["collector"],
        )
    except KeyError:
        return TokensPair(
            pair=tokens_pair_dict["pair"],
            source=tokens_pair_dict["source"],
            token_1_policy=tokens_pair_dict["token1_policy"],
            token_1_name=tokens_pair_dict["token1_name"],
            token_1_decimals=tokens_pair_dict["token1_decimals"],
            token_2_policy=tokens_pair_dict["token2_policy"],
            token_2_name=tokens_pair_dict["token2_name"],
            token_2_decimals=tokens_pair_dict["token2_decimals"],
            security_token_policy=tokens_pair_dict["security_token_policy"],
            security_token_name=tokens_pair_dict["security_token_name"],
        )


def tokens_pair_dict_from_obj(tokens_pair: TokensPair):
    """Helper function to return a tokens pair dictionary from a
    TokensPair object.
    """
    return {
        "pair": tokens_pair.pair,
        "source": tokens_pair.source,
        "token1_policy": tokens_pair.token_1_policy,
        "token1_name": tokens_pair.token_1_name,
        "token1_decimals": tokens_pair.token_1_decimals,
        "token2_policy": tokens_pair.token_2_policy,
        "token2_name": tokens_pair.token_2_name,
        "token2_decimals": tokens_pair.token_2_decimals,
        "security_token_policy": tokens_pair.security_token_policy,
        "security_token_name": tokens_pair.security_token_name,
    }
