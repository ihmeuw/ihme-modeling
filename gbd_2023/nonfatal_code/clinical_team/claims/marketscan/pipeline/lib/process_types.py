from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class Estimates:
    INPATIENT_PRIMARY_ADMISSIONS = 14
    INPATIENT_PRIMARY_INDIVIDUALS = 15
    INPATIENT_ANY_CLAIMS = 16
    INPATIENT_ANY_INDIVIDUALS = 17
    OUTPATIENT_ANY_CLAIMS = 18
    OUTPATIENT_ANY_INDIVIDUAL = 19
    UNION_ANY_INDIVIDUALS = 21


@dataclass
class Deliverable:
    name: str
    groupby_cols: List[str]
    apply_maternal_adjustment: bool
    rm_locs: List[int]
    estimate_ids: List[int]


GbdDeliverable = Deliverable(
    name="gbd",
    groupby_cols=[],
    apply_maternal_adjustment=True,
    rm_locs=[102],
    estimate_ids=[Estimates.INPATIENT_ANY_INDIVIDUALS, Estimates.UNION_ANY_INDIVIDUALS],
)

CfDeliverable = Deliverable(
    name="correction_factors",
    groupby_cols=["facility_id"],
    apply_maternal_adjustment=False,
    rm_locs=[],
    estimate_ids=[
        Estimates.INPATIENT_PRIMARY_ADMISSIONS,
        Estimates.INPATIENT_PRIMARY_INDIVIDUALS,
        Estimates.INPATIENT_ANY_INDIVIDUALS,
        Estimates.OUTPATIENT_ANY_CLAIMS,
        Estimates.OUTPATIENT_ANY_INDIVIDUAL,
        Estimates.UNION_ANY_INDIVIDUALS,
    ],
)


def create_deliverable(deliverable_name: str) -> Deliverable:
    """Method to convert a deliverable_name str into the deliverable object"""
    if deliverable_name == "gbd":
        deliverable = GbdDeliverable
    elif deliverable_name == "correction_factors":
        deliverable = CfDeliverable
    else:
        raise ValueError(f"{deliverable_name} is not a supported deliverable")
    return deliverable