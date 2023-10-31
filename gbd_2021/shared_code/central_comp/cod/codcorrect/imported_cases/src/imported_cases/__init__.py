"""Imported Cases."""
try:
    from importlib.metadata import version  # type: ignore
except ImportError:
    from importlib_metadata import version  # type: ignore


try:
    __version__ = version(__name__)
except Exception:  # pragma: no cover
    __version__ = "unknown"
