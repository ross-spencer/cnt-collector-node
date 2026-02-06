"""Helper module for loading DEX pairs."""

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
        return Pairs(pairs=pairs.DEX_PAIRS)
    except AttributeError as err:
        logger.error("pairs module doesn't contain DEX_PAIRS dict")
        raise SystemExit from err
