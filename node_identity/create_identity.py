"""Create a node identity object."""

import argparse
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from typing import Final

import requests

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s :: %(filename)s:%(lineno)s:%(funcName)s() :: %(message)s",  # noqa: E501
    datefmt="%Y-%m-%d %H:%M:%S",
    level="INFO",
)

logger = logging.getLogger(__name__)


NODE_IDENTITY_LOC: Final[str] = os.path.join("/var/tmp", ".node-identity.json")


def get_version():
    """Return package version to the calling code.

    Version is set to None if it isn't picked up by importlib correctly
    we return `None` or `null` for now.
    """
    __version__ = None
    try:
        __version__ = version("orcfax-collector-node")
    except PackageNotFoundError:
        # package is not installed
        pass
    logging.info(__version__)
    return __version__


def validate_ws_string(validator_ws: str) -> bool:
    """Perform some rudimentary validation on the init string for the
    validator websocket.

    ws:// is the only supported protocol until we implement an
    encrypted tls connection and can provide wss://.
    """
    if not validator_ws.startswith("ws://") and not validator_ws.startswith("wss://"):
        return False
    return True


def where_am_i():
    """Tell me where I am."""

    resp = requests.get("https://ipinfo.io/ip", timeout=30)
    ip_addr = resp.text
    resp = requests.get(f"https://ipinfo.io/{ip_addr}", timeout=30)
    return json.loads(resp.text)


def keygen():
    """Public/private keypair generation.

    TMP: returns a uuid4 in v1.
    """
    return f"{uuid.uuid4()}"


def who_am_i():
    """Declare me who I am.

    NB. replace with keygen procedure and return public key.
    """
    return keygen()


def when_am_i():
    """Declare when I was brought online."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def create_id(validator_ws: str, cert: str):
    """Create an identity for me."""
    who = who_am_i()
    where = where_am_i()
    ws_endpoint = f"{validator_ws}/{who}/"
    logger.info("Validator ws is: %s", ws_endpoint)
    id_obj = {}
    id_obj["node_id"] = who
    id_obj["location"] = where
    id_obj["initialization"] = when_am_i()
    id_obj["init_version"] = get_version()
    id_obj["validator_web_socket"] = validator_ws
    id_obj["validator_certificate"] = cert
    return id_obj


def save_id(id_: dict) -> None:
    """Save the ID to a location on disk."""
    with open(NODE_IDENTITY_LOC, "w", encoding="utf-8") as identity:
        identity.write(json.dumps(id_))


def main():
    """Primary entry point for this script."""

    parser = argparse.ArgumentParser(
        prog="coop-node-init",
        description="Initializes a COOP node by creating an identity and declaring its validator",
        epilog="for more information visit https://orcfax.io",
    )

    parser.add_argument(
        "--websocket",
        help="provide the websocket location of the coop-validator web-socket",
        required=False,
    )

    parser.add_argument(
        "--cert",
        help="provide the websocket certificate to enable wss://",
        required=False,
    )

    parser.add_argument(
        "--version",
        help="return script version",
        required=False,
        action="store_true",
    )

    args = parser.parse_args()

    if args.version:
        print(f"coop-node-init: {get_version()}")
        sys.exit()

    validator_ws = None
    if args.websocket:
        validator_ws = args.websocket
        if not validate_ws_string(validator_ws):
            logger.error(
                "validator string '%s' is invalid, exiting (do not pass init arg if testing without validator)",
                validator_ws,
            )
            sys.exit(1)

    if args.cert:
        logger.info("self-signed certificate testing is not yet implemented")

    logger.info("websocket set to: %s", validator_ws)
    id_obj = create_id(validator_ws, args.cert)
    save_id(id_obj)
    logger.info("identity output to: %s", NODE_IDENTITY_LOC)


if __name__ == "__main__":
    main()
