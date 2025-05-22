"""ihme_cc_paf_aggregator."""

from importlib.metadata import version  # type: ignore

try:
    __version__ = version(__name__)
except Exception:  # pragma: no cover
    __version__ = "unknown"
