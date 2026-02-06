"""Ogmios helper functions"""

# pylint: disable=C0103, W1203

import json
import logging
from typing import Dict, Final, List, Union

import requests
import requests.exceptions
import websocket

JSONRPC_VERSION = "2.0"

logger = logging.getLogger(__name__)

POLICY_ADA: Final[str] = "ada"


def ogmios_version(ogmios_url: str) -> str:
    """Find out the ogmios version"""
    try:
        url = f"{ogmios_url}/health".replace("wss://", "https://").replace(
            "ws://", "http://"
        )
        logger.info("fetching Ogmios version from: %s", url)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        version = json.loads(resp.text).get("version")
        logger.info("ogmios version: %s", version)
        return version
    except (requests.exceptions.RequestException, json.JSONDecodeError) as err:
        logger.error("failed to fetch Ogmios version: %s", err)
        raise


def send_ws_request(ws: websocket.WebSocket, msg: dict) -> dict:
    """Send a WebSocket request and return the response."""
    try:
        ws.send(json.dumps(msg))
        resp = json.loads(ws.recv())
        return resp
    except (websocket.WebSocketException, json.JSONDecodeError, BrokenPipeError) as err:
        logger.error("websocket communication failed: %s", err)
        return {}


def ogmios_tip(ws: websocket.WebSocket) -> dict:
    """Ogmios tip"""
    msg = {"jsonrpc": JSONRPC_VERSION, "method": "queryNetwork/tip"}
    return send_ws_request(ws, msg)


def ogmios_last_block_slot(ws: websocket.WebSocket) -> int | None:
    """Find out the latest block slot."""
    tip = ogmios_tip(ws).get("result")
    if not tip:
        logger.warning("%s", tip)
    return tip.get("slot") if tip else None


def ogmios_epoch(ws: websocket.WebSocket) -> dict:
    """Ogmios epoch"""
    msg = {"jsonrpc": JSONRPC_VERSION, "method": "queryLedgerState/epoch"}
    return send_ws_request(ws, msg)


def ogmios_intersection(ws: websocket.WebSocket, point: dict) -> dict:
    """Ogmios intersection"""
    msg = {
        "jsonrpc": JSONRPC_VERSION,
        "method": "findIntersection",
        "params": {"points": [point]},
    }
    return send_ws_request(ws, msg)


async def ogmios_next_block(ws: websocket.WebSocket) -> dict:
    """Ogmios next block"""
    msg = {"jsonrpc": JSONRPC_VERSION, "method": "nextBlock"}
    return send_ws_request(ws, msg)


def ogmios_addresses_utxos(ws: websocket.WebSocket, addresses: List[str]) -> Dict:
    """Ogmios intersection"""
    msg = {
        "jsonrpc": JSONRPC_VERSION,
        "method": "queryLedgerState/utxo",
        "params": {"addresses": addresses},
    }
    return send_ws_request(ws, msg)


def ogmios_acquire_ledger_state(ws: websocket.WebSocket, point) -> dict:
    """Ogmios acquire ledger state"""
    msg = {
        "jsonrpc": JSONRPC_VERSION,
        "method": "acquireLedgerState",
        "params": {"point": point},
    }
    return send_ws_request(ws, msg)


def ogmios_mempool_size(ws: websocket.WebSocket) -> dict:
    """Ogmios mempool size"""
    msg = {"jsonrpc": JSONRPC_VERSION, "method": "sizeOfMempool"}
    return send_ws_request(ws, msg)


def ogmios_mempool_acquire(ws: websocket.WebSocket) -> dict:
    """Ogmios mempool snapshot"""
    msg = {"jsonrpc": JSONRPC_VERSION, "method": "acquireMempool"}
    return send_ws_request(ws, msg)


def ogmios_mempool_release(ws: websocket.WebSocket) -> dict:
    """Ogmios mempool release"""
    msg = {"jsonrpc": JSONRPC_VERSION, "method": "releaseMempool"}
    return send_ws_request(ws, msg)


def ogmios_mempool_transactions(ws: websocket.WebSocket) -> dict:
    """Ogmios mempool transactions"""
    msg = {
        "jsonrpc": JSONRPC_VERSION,
        "method": "nextTransaction",
        "params": {"fields": "all"},
    }
    return send_ws_request(ws, msg)


def get_output_content(output: dict) -> dict:
    """Parse the contents of an output
    Return a dictionary with the amounts of lovelace and tokens in an UTxO
    """
    content = {
        "amount": output["value"]["ada"]["lovelace"],
        "assets": {},
    }
    for policy, assets in output["value"].items():
        if policy.lower() == POLICY_ADA:
            continue
        content["assets"][policy] = {}
        for name, amount in assets.items():
            content["assets"][policy][name] = amount
    return content


def get_ogmios_utxo_content(utxo: Union[list, dict]) -> dict:
    """Parse the contents of an Ogmios UTxO
    Return a dictionary with the amounts of lovelace and tokens in an Ogmios UTxO
    and also with the input transaction hash and output index
    """
    content = {
        "tx_hash": utxo["transaction"]["id"],
        "tx_index": utxo["index"],
        "amount": utxo["value"]["ada"]["lovelace"],
        "assets": {},
    }
    output = utxo
    content["assets"] = get_output_content(output)["assets"]
    return content
