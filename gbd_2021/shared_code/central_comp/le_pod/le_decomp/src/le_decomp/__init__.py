"""Life Expectancy Decomposition."""
try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version


try:
    __version__ = version(__name__)
except Exception:
    __version__ = "unknown"
