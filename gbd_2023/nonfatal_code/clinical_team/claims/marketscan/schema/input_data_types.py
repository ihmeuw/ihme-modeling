"""
Classes that manage the assignment of certain values used in create_schema processing methods
based on input data type. Input data type being inpatient, outpatient and enrollment detail.
This does not include source (ccae, mdcr) because the schema of both sources should be
identical while the schema in each data type is different.

Note: This contains values to define how the data should be processed but any actual data
processing occurs in create_yearly_ms_schema.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List

from crosscutting_functions.clinical_constants.pipeline import marketscan as constants


@dataclass
class InpatientData:
    name: str = "inpatient"
    assign_cols: Dict[str, Any] = field(
        default_factory=lambda: {"facility_id": None, "is_otp": 0}
    )
    read_cols: List[str] = field(default_factory=lambda: constants.INP_READ_COLS)
    file_suffix: str = "i"
    swap_live_births: bool = True


@dataclass
class OutpatientData:
    name: str = "outpatient"
    assign_cols: Dict[str, Any] = field(default_factory=lambda: {"is_otp": 1, "dstatus": -1})
    read_cols: List[str] = field(default_factory=lambda: constants.OTP_READ_COLS)
    file_suffix: str = "o"
    swap_live_births: bool = False


@dataclass
class EnrollmentData:
    name: str = "enrollment_detail"
    assign_cols: Dict[str, Any] = field(default_factory=lambda: {})
    read_cols: List[str] = field(default_factory=lambda: constants.ENROL_READ_COLS)
    file_suffix: str = "t"
    swap_live_births: bool = False
