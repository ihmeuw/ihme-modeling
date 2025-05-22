CLINICAL_TEAM = ["USERNAME"]

QUERY_BY_TYPES = ["icd", "bundle", "icg"]

FACILITY_TYPE_ID = {"inpatient": 0, "outpatient": 1}

# compression algorithm for clinical data
COMP: str = "zstd"


class Estimates:
    inp_primary_unadj = 1
    inp_primary_cf1_modeled = 2
    inp_primary_cf2_modeled = 3
    inp_primary_cf3_modeled = 4
    inp_primary_live_births_unadj = 6
    inp_primary_live_births_cf1_modeled = 7
    inp_primary_live_births_cf2_modeled = 8
    inp_primary_live_births_cf3_modeled = 9
    otp_any_unadj = 11
    claims_primary_inp_claims = 14
    claims_primary_inp_indv = 15
    claims_any_inp_claims = 16
    claims_any_inp_indv = 17
    claims_any_otp_claims = 18
    claims_any_otp_indv = 19
    claims_any_inp_union_require2_otp_indv = 21
    inp_primary_inj_cf_en_prop = 22
    otp_any_otp_cf = 23
    otp_any_otp_inj_cf = 24
    otp_any_icpc = 25
    inp_pri_claims_5_percent = 26
    claims_any_inp_union_otp_adm = 27
    claims_primary_inp_indv_5_percent_claims = 28
    claims_any_inp_indv_5_percent_claims = 29


class ClinicalDataTypes:
    inpatient = 1
    outpatient = 2
    claims = 3
    claims_inpatient_only = 4
    claims_flagged = 5


# TODO Create a better way to look this up,
CLAIMS_DTYPE = {
    "sgp": ClinicalDataTypes.claims,
    "twn_nhi": ClinicalDataTypes.claims,
    "rus_moh": ClinicalDataTypes.claims_flagged,
    "mng_h_info": ClinicalDataTypes.claims_flagged,
    "kor_hira": ClinicalDataTypes.claims_flagged,
    "pol_nhf": ClinicalDataTypes.claims,
    "cms_mdcr": ClinicalDataTypes.claims,
    "cms_max": ClinicalDataTypes.claims,
    "ms": ClinicalDataTypes.claims,
}

legacy_cf_estimate_types = {
    Estimates.claims_primary_inp_claims: "inp_pri_claims_cases",
    Estimates.claims_primary_inp_indv: "inp_pri_indv_cases",
    Estimates.claims_any_inp_indv: "inp_any_indv_cases",
    Estimates.claims_any_otp_claims: "otp_any_claims_cases",
    Estimates.claims_any_otp_indv: "otp_any_indv_cases",
    Estimates.claims_any_inp_union_require2_otp_indv: "inp_otp_any_adjusted_otp_only_indv_cases",  # noqa
    Estimates.inp_pri_claims_5_percent: "inp_pri_claims_5_percent",
    Estimates.claims_primary_inp_indv_5_percent_claims: "inp_pri_indv_5_percent_cases",
    Estimates.claims_any_inp_indv_5_percent_claims: "inp_any_indv_5_percent_cases",
}