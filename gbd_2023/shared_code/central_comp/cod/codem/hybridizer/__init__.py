"""CODEm Hybridizer."""

from importlib.metadata import version

try:
    __version__ = version(
        "codem_hybridizer"
    )  # Use name of distribution package rather than module
except Exception:  # pragma: no cover
    __version__ = "unknown"
