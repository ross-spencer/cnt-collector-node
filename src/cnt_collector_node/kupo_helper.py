"""Functions related to Kupo functionality."""

# pylint: disable = W0718  # catching too general exception.

# Standard library imports
import json
import logging
from typing import Final

# Third-party imports
import requests

logger = logging.getLogger(__name__)

ASSET_NAME_BLANK: Final[str] = ""


def kupo_health(kupo_url: str) -> bool:
    """Check if Kupo is healthy."""
    try:
        resp = requests.get(f"{kupo_url}/health", timeout=30)
        resp.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        kupo_healthy = False
        kupo_node_tip = None
        latest_kupo_checkpoint = None

        for line in resp.text.splitlines():
            if line.startswith("kupo_most_recent_checkpoint"):
                latest_kupo_checkpoint = line.split(" ")[1]
            if line.startswith("kupo_most_recent_node_tip"):
                kupo_node_tip = line.split(" ")[1]

        if (
            latest_kupo_checkpoint is not None
            and kupo_node_tip is not None
            and latest_kupo_checkpoint == kupo_node_tip
        ):
            kupo_healthy = True

        return kupo_healthy

    except requests.exceptions.RequestException as err:
        logger.exception("error during Kupo health check: %s", err)
        return False
    except Exception as err:
        logger.exception("unexpected error during Kupo health check: %s", err)
        return False


def get_kupo_matches(kupo_url: str, pattern: str) -> list:
    """Get all the matches from Kupo"""
    try:
        # Searching for all the matches (UTxOs)
        url = f"{kupo_url}/matches/{pattern}?unspent"
        resp = requests.get(url, timeout=120)
        matches = json.loads(resp.text)
        return matches
    except requests.exceptions.RequestException as err:
        logger.exception("error during get_kupo_matches: %s", err)
        return None
    except Exception as err:
        logger.exception("unexpected error during get_kupo_matches: %s", err)
        return None


def get_kupo_utxo_content(utxo: dict) -> dict:
    """Parse the contents of a Kupo UTxO
    Return a dictionary with the amounts of lovelace and tokens in an UTxO
    """
    content = {
        "tx_hash": utxo["transaction_id"],
        "tx_index": utxo["output_index"],
        "amount": utxo["value"]["coins"],
        "assets": {},
    }
    # for policy, assets in utxo.output.amount.multi_asset.data.items():
    for asset, amount in utxo["value"]["assets"].items():
        asset_split = asset.split(".")
        policy_id = asset_split[0]
        try:
            asset_name = asset_split[1]
        except IndexError:
            asset_name = ASSET_NAME_BLANK
        if policy_id not in content["assets"]:
            content["assets"][policy_id] = {}
        if asset_name not in content["assets"][policy_id]:
            content["assets"][policy_id][asset_name] = amount
        else:
            logger.info("kupo += amount called: %s", asset)
            content["assets"][policy_id][asset_name] += amount
    return content
