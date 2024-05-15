"""Dataclasses for capturing the configuration of the pipeline."""

from typing import Dict, Optional

from fhs_lib_cli_tools.lib.fhs_dataclasses import BaseModel
from fhs_lib_file_interface.lib.version_metadata import VersionMetadata
from fhs_lib_file_interface.lib.versioning import Versions
from fhs_lib_year_range_manager.lib.year_range import YearRange


class CauseSpecificModelingArguments(BaseModel):
    """Common modeling arguments used in mortality cause-specific.

    These attributes represent the "dials" you could turn in this repo which would
    influence the modeled aspects of cause-specific mortality. These include the boolean
    flags which turn on & off certain features, as well as the attributes which determine the
    contents of what's being modeled (like sex and years).
    """

    acause: str
    draws: int
    drivers_at_reference: bool
    fit_on_subnational: bool
    gbd_round_id: int
    sex_id: int
    spline: bool
    subnational: bool
    version: str
    years: YearRange
    seed: Optional[int]


class CauseSpecificVersionArguments(BaseModel):
    """Common version arguments used in mortality cause-specific.

    These attributes generally represent Versions objects containing references to input data.
    The ``logspace_conversion_flags`` is the snowflake in this dataclass which is a dictionary
    mapping the input data name to whether it should be calculated in log space (e.x.
    {"death": True, "sdi": False}).
    """

    versions: Versions
    logspace_conversion_flags: Dict[str, bool]
    output_scenario: int | None


class SumToAllCauseModelingArguments(BaseModel):
    """Common modeling arguments used in mortality sum-to-all-cause."""

    acause: str
    agg_version: VersionMetadata
    approximation: bool
    draws: int
    gbd_round_id: int
    input_version: VersionMetadata
    period: str
    output_scenario: int | None
