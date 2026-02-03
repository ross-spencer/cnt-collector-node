"""Helper module for loading DEX pairs."""

import hashlib
import importlib
import importlib.util
import logging
import sys
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Pairs:
    """Wrapper for the DEX_PAIRS object to make sure that it is
    passed around code in an immutable form.
    """

    pairs: dict
    checksum: str

    @property
    def DEX_PAIRS(self):  # pylint:disable=C0103
        """Wrap the pairs instance in a function that retrieves the
        peoperty for us. This helps us to protect the data.
        """
        return self.pairs


def load(path: str) -> Pairs:
    """Load DEX_PAIRS from a given path.

    Module load: https://stackoverflow.com/a/67692/23789970
    """

    pairs = None
    try:
        spec = importlib.util.spec_from_file_location("pairs", path)
        pairs = importlib.util.module_from_spec(spec)
        sys.dont_write_bytecode = True
        sys.modules["module.name"] = pairs
        spec.loader.exec_module(pairs)
    except (ModuleNotFoundError, AttributeError) as err:
        logger.error("problem loading module: %s", err)
        raise SystemExit from err
    try:
        checksum = get_file_checksum(path=path)
        return Pairs(pairs=pairs.DEX_PAIRS, checksum=checksum)
    except AttributeError as err:
        logger.error("pairs module doesn't contain DEX_PAIRS dict")
        raise SystemExit from err


def get_file_checksum(path: str):
    """Return a checksum for a file."""
    checksum = None
    # Open,close, read file and calculate MD5 on its contents
    with open(path, "rb") as file_to_check:
        # read contents of the file
        data = file_to_check.read()
        # pipe contents of the file through
        checksum = hashlib.md5(data).hexdigest()
    return checksum
