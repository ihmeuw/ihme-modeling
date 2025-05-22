"""CODEm."""

from importlib.metadata import version

try:
    __version__ = version(__name__)
except Exception:  # pragma: no cover
    __version__ = "unknown"


del version
