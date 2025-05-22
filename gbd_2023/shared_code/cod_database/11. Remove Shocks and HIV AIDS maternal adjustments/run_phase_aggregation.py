import sys

import numpy as np
import pandas as pd

from cod_prep.claude.aggregators import CauseAggregator, LocationAggregator
from cod_prep.claude.cf_adjustments import (
    AnemiaAdjuster,
    RTIAdjuster,
    SampleSizeCauseRemover,
)
from cod_prep.claude.claude_io import (
    get_input_launch_set_id,
    get_input_launch_set_ids,
    get_phase_output,
    write_phase_output,
)
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.count_adjustments import EnvelopeLocationSplitter
from cod_prep.claude.hiv_maternal_pafs import HIVMatPAFs
from cod_prep.claude.metric_functions import calc_sample_size, convert_to_cause_fractions
from cod_prep.claude.parent_garbage import ParentMappedAggregatedGarbageAdder
from cod_prep.claude.redistribution_variance import (
    RedistributionVarianceEstimator,
    dataset_has_redistribution_variance,
)
from cod_prep.claude.squaring import Squarer
from cod_prep.downloaders import (
    add_location_metadata,
    add_survey_type,
    get_ages,
    get_cause_map,
    get_cause_package_hierarchy,
    get_current_cause_hierarchy,
    get_current_location_hierarchy,
    get_env,
    get_map_version,
    get_package_map,
    get_remove_decimal,
    get_values_from_nid,
)
from cod_prep.utils import report_duplicates, report_if_merge_fail


VA_DATA_TYPE = 8
POLICE_SURVEY_DATA_TYPE = [4, 5, 6, 7]
POLICE_DATA_TYPE = 4
CC_CODE = 919
CONF = Configurator("standard")
MATERNAL_SQUARED = ["Mexico_BIRMM", "SUSENAS", "China_MMS", "China_Child", "Other_Maternal", "DHS_maternal"]


def drop_cc_code(df):
    return df.loc[lambda d: d["cause_id"] != CC_CODE, :]


def conform_one_like_cf_to_one(df, cf_cols):
    for cf_col in cf_cols:
        df.loc[(df[cf_col] > 1) & (np.isclose(df[cf_col], 1)), cf_col] = 1
    return df


def assert_valid_cause_fractions(df, cf_cols):
    assert df[cf_cols].notnull().values.all()
    assert (df[cf_cols] >= 0).values.all()
    assert (df[cf_cols] <= 1).values.all()


def log_statistic(df):
    if "cf" in df.columns:
        vcols = [x for x in df.columns if x.startswith("cf")]
    elif "deaths" in df.columns:
        vcols = [x for x in df.columns if x.startswith("deaths")]
    else:
        return "rows={}".format(len(df))
    value_dict = {}
    for vcol in vcols:
        value_dict[vcol] = "min={}, max={}".format(
            round(df[vcol].min(), 3), round(df[vcol].max(), 3)
        )
    value_dict["rows"] = len(df)
    return value_dict

def square_maternal_sources(df, nid, extract_type_id, cause_meta_df, age_meta_df):
    squarer = Squarer(cause_meta_df, age_meta_df, data_type="VR")
    df = add_survey_type(df)
    dhs = df["survey_type"] == "DHS"
    if len(df[dhs]) > 0:
        non_dhs = df[~dhs]
        dhs = df[dhs]
        dhs = squarer.get_computed_dataframe(dhs)
        dhs["nid"].fillna(nid, inplace=True)
        dhs["extract_type_id"].fillna(extract_type_id, inplace=True)
        df = pd.concat([dhs, non_dhs], ignore_index=True)
    else:
        df = squarer.get_computed_dataframe(df)
        df = df.query("sample_size > 0")

    df.drop("survey_type", axis=1, inplace=True)
    df = drop_cc_code(df)

    assert df.notnull().values.any()
    report_duplicates(
        df,
        [
            "year_id",
            "sex_id",
            "location_id",
            "cause_id",
            "age_group_id",
            "nid",
            "extract_type_id",
            "site_id",
        ],
    )
    return df


def generate_cf_agg(df):
    df["cf_agg"] = df["cf"]
    return df


def run_phase(df: pd.DataFrame, nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    cache_kwargs = {
        "force_rerun": False,
        "block_rerun": True,
        "cache_results": False,
        "cache_dir": CONF.get_directory("db_cache"),
        "verbose": False,
    }
    project_id = CONF.get_id("project")
    location_set_version_id = CONF.get_id("location_set_version")
    cause_set_version_id = CONF.get_id("cause_set_version")
    env_run_id = CONF.get_id("env_run")
    code_system_id, iso3, data_type_id, source = get_values_from_nid(
        nid,
        extract_type_id,
        values=["code_system_id", "iso3", "data_type_id", "source"],
        **cache_kwargs,
    )
    remove_decimal = get_remove_decimal(code_system_id, **cache_kwargs)
    code_map_version_id = get_map_version(code_system_id, **cache_kwargs)
    location_meta_df = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id, **cache_kwargs
    )
    age_meta_df = get_ages(**cache_kwargs)
    cause_meta_df = get_current_cause_hierarchy(
        cause_set_version_id=cause_set_version_id,
        **cache_kwargs,
    )
    cause_map = get_cause_map(code_map_version_id=code_map_version_id, **cache_kwargs)
    package_map = get_package_map(code_system_id=code_system_id, **cache_kwargs)
    cause_package_hierarchy = get_cause_package_hierarchy(code_system_id, **cache_kwargs)
    env_meta_df = get_env(env_run_id=env_run_id, **cache_kwargs)
    
    disagg_lsid, msdc_lsid = get_input_launch_set_ids(
        nid, extract_type_id, launch_set_id,
        phases=["disaggregation", "misdiagnosiscorrection"]
    )
    disagg_df = get_phase_output("disaggregation", nid, extract_type_id, disagg_lsid)
    msdc_df = get_phase_output("misdiagnosiscorrection", nid, extract_type_id, msdc_lsid)

    location_aggregator = LocationAggregator(df, location_meta_df)
    if (data_type_id == 7) & (iso3 == "IND"):
        df = location_aggregator.get_computed_dataframe("full")
    else:
        df = location_aggregator.get_computed_dataframe()

    df = calc_sample_size(df)

    df = df.loc[df["sample_size"] > 0]
    df = convert_to_cause_fractions(df, ["deaths", "deaths_cov", "deaths_rd", "deaths_corr", "deaths_raw"])

    if data_type_id == VA_DATA_TYPE:
        va_anemia_adjuster = AnemiaAdjuster()
        df = va_anemia_adjuster.get_computed_dataframe(df)

    if data_type_id == POLICE_DATA_TYPE and source == "Various_RTI":
        rti_adjuster = RTIAdjuster(df, cause_meta_df, age_meta_df, location_meta_df)
        df = rti_adjuster.get_computed_dataframe()

    if data_type_id in POLICE_SURVEY_DATA_TYPE:
        cause_list = df.cause_id.unique()
        square_me = (len(cause_list) == 2) & (CC_CODE in cause_list)
        if (source in MATERNAL_SQUARED) or square_me:
            df = square_maternal_sources(df, nid, extract_type_id, cause_meta_df, age_meta_df)

    df = drop_cc_code(df)

    env_loc_splitter = EnvelopeLocationSplitter(df, env_meta_df, source)
    df = env_loc_splitter.get_computed_dataframe()

    cause_aggregator = CauseAggregator(df, cause_meta_df, source, data_type_id)
    df = cause_aggregator.get_computed_dataframe()

    parent_gbg_adder = ParentMappedAggregatedGarbageAdder(
        nid,
        extract_type_id,
        source,
        data_type_id,
        cause_package_hierarchy,
        cause_meta_df,
        package_map,
        cause_map,
        remove_decimal,
        disagg_df,
        msdc_df,
    )
    df = parent_gbg_adder.get_computed_dataframe(df)

    hiv_shock_remover = SampleSizeCauseRemover(cause_meta_df)
    df = hiv_shock_remover.get_computed_dataframe(df)

    hmp = HIVMatPAFs(project_id)
    df = hmp.get_computed_dataframe(df, cause_meta_df, location_meta_df)

    df = conform_one_like_cf_to_one(df, cf_cols=["cf", "cf_raw", "cf_corr", "cf_rd", "cf_cov"])

    assert_valid_cause_fractions(df, cf_cols=["cf"])

    if dataset_has_redistribution_variance(data_type_id, source):
        rdvar = RedistributionVarianceEstimator(
            nid,
            extract_type_id,
            launch_set_id,
            cause_meta_df,
            remove_decimal,
            code_system_id,
            package_map,
            code_map_version_id=code_map_version_id,
        )
        df = rdvar.get_computed_dataframe(df, **cache_kwargs)

    df = generate_cf_agg(df)

    return df


def main(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    df = get_phase_output(
        "corrections",
        nid,
        extract_type_id,
        get_input_launch_set_id(nid, extract_type_id, launch_set_id, "corrections"),
    )
    df = run_phase(df, nid, extract_type_id, launch_set_id)
    write_phase_output(df, "aggregation", nid, extract_type_id, launch_set_id)
    return df


if __name__ == "__main__":
    main(
        nid=int(sys.argv[1]),
        extract_type_id=int(sys.argv[2]),
        launch_set_id=int(sys.argv[3]),
    )
