"""dataclass holding some important attributes related to estimate_ids
Estimate IDs are closely tied to deduplication rules

Currently this only supports Claims estimate_ids
"""

from dataclasses import dataclass


@dataclass
class ClaimsEstimate:
    name: str
    estimate_id: int
    apply_deduplication: bool
    is_otp_vals: tuple
    primary_dx_only: bool


ESTIMATE_14 = ClaimsEstimate(
    name="claims-primary-inp_claims",
    estimate_id=14,
    apply_deduplication=False,
    is_otp_vals=(0,),
    primary_dx_only=True,
)


ESTIMATE_15 = ClaimsEstimate(
    name="claims-primary-inp_indv",
    estimate_id=15,
    apply_deduplication=True,
    is_otp_vals=(0,),
    primary_dx_only=True,
)


ESTIMATE_16 = ClaimsEstimate(
    name="claims-any-inp_claims",
    estimate_id=16,
    apply_deduplication=False,
    is_otp_vals=(0,),
    primary_dx_only=False,
)


ESTIMATE_17 = ClaimsEstimate(
    name="claims-any-inp_indv",
    estimate_id=17,
    apply_deduplication=True,
    is_otp_vals=(0,),
    primary_dx_only=False,
)


ESTIMATE_18 = ClaimsEstimate(
    name="claims-any-otp_claims",
    estimate_id=18,
    apply_deduplication=False,
    is_otp_vals=(1,),
    primary_dx_only=False,
)


ESTIMATE_19 = ClaimsEstimate(
    name="claims-any-otp_indv",
    estimate_id=19,
    apply_deduplication=True,
    is_otp_vals=(1,),
    primary_dx_only=False,
)


ESTIMATE_21 = ClaimsEstimate(
    name="claims-any-inp_union_require2_otp_indv",
    estimate_id=21,
    apply_deduplication=True,
    is_otp_vals=(0, 1),
    primary_dx_only=False,
)


ESTIMATE_26 = ClaimsEstimate(
    name="claims-primary-inp_5_percent_claims",
    estimate_id=26,
    apply_deduplication=False,
    is_otp_vals=(0,),
    primary_dx_only=True,
)


ESTIMATE_28 = ClaimsEstimate(
    name="claims-primary-inp_indv-5_percent_claims",
    estimate_id=28,
    apply_deduplication=True,
    is_otp_vals=(0,),
    primary_dx_only=True,
)


ESTIMATE_29 = ClaimsEstimate(
    name="claims-any-inp_indv-5_percent_claims",
    estimate_id=29,
    apply_deduplication=True,
    is_otp_vals=(0,),
    primary_dx_only=False,
)
