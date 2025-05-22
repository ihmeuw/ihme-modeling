"""ST-GPR."""

from importlib.metadata import version

try:
    __version__ = version(__name__)
except Exception:  # pragma: no cover
    __version__ = "unknown"


from stgpr.api.public import (
    get_custom_covariates,
    get_estimates,
    get_input_data,
    get_model_status,
    get_parameters,
    get_stgpr_versions,
    register_stgpr_model,
    stgpr_sendoff,
)

__all__ = [
    "__version__",
    "get_custom_covariates",
    "get_estimates",
    "get_input_data",
    "get_model_status",
    "get_parameters",
    "get_stgpr_versions",
    "register_stgpr_model",
    "stgpr_sendoff",
]

del version
