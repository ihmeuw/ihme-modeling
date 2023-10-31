"""
Load configuration into convenience objects and let the user interact.
"""
from winnower.config.ubcov import (
    UbcovConfigLoader,
    UrlPaths,
)


def interactive_query():
    config_root = UrlPaths.for_google_docs()
    cl = config_loader = UbcovConfigLoader.from_root(config_root)  # noqa
    breakpoint(header=("'cl' and 'config_loader' are your config."
                       "\nYour first command is probably 'interact'"))
