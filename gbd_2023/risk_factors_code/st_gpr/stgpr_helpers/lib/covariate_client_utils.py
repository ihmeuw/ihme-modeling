"""Covariate service client-side functionality."""

import contextlib
import warnings
from typing import Iterator

import pandas as pd
import requests
from urllib3 import exceptions as urllib_exceptions

from stgpr_helpers.lib.constants import columns
from stgpr_helpers.lib.constants import exceptions as exc

BASE_URL: str = "PATH"


@contextlib.contextmanager
def covariate_client() -> Iterator[requests.Session]:
    """Context manager for querying the covariate service.

    When verify is false, urllib3 throws a TLS warning that we filter out:
    https://urllib3.readthedocs.io/en/latest/advanced-usage.html#tls-warnings

    Yields:
        requests.Session client with the covariate service
    """
    with requests.Session() as client:
        client.headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )
        client.verify = False
        warnings.filterwarnings(
            "ignore", category=urllib_exceptions.InsecureRequestWarning, module="urllib3"
        )
        yield client


def get_covariates() -> pd.DataFrame:
    """Get covariates info.

    Returns:
        dataframe with two columns: "covariate_id", "covariate_name_short"
    """
    with covariate_client() as client:
        response = client.get(f"{BASE_URL}/covariates")

    if response.status_code != 200:
        _handle_error(response)

    return (
        pd.DataFrame(response.json())
        .drop(columns="covariateName")
        .rename(
            columns={
                "covariateId": columns.COVARIATE_ID,
                "covariateNameShort": columns.COVARIATE_NAME_SHORT,
            }
        )
    )


def _handle_error(response: requests.Response) -> None:
    """Generic error handling."""
    raise exc.FailedToQueryCovariateService(
        f"Failed to query covariate service with status code {response.status_code} and "
        f"reason '{response.reason}'. Try resuming this model, and if it fails again, then "
        "file a help ticket with Central Computation."
    )
