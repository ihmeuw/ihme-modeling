import sys

import numpy as np
import pandas as pd

from cod_prep.claude.adjust_nzl_deaths import correct_maori_non_maori_deaths
from cod_prep.claude.age_sex_split import AgeSexSplitter
from cod_prep.claude.cause_reallocation import adjust_leukemia_subtypes, adjust_liver_cancer
from cod_prep.claude.claude_io import get_input_launch_set_id, get_phase_output, write_phase_output
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.mapping import GBDCauseMapper
from cod_prep.claude.unmodeled_demographics_remap import AgeRemap, AgeSexRemap
from cod_prep.downloaders import (
    add_code_metadata,
    add_envelope,
    add_site_metadata,
    get_cause_map,
    get_current_location_hierarchy,
    get_env,
    get_map_version,
    get_values_from_nid,
)
from cod_prep.utils import CodSchema, print_log_message, report_if_merge_fail

CONF = Configurator()


def needs_liver_cancer_correction(iso3, code_system_id, year_id, data_type_id):
    is_old_china_dsp = (iso3 == "CHN") and (code_system_id in [40, 6])
    is_thailand = (iso3 == "THA") and (code_system_id == 1)
    is_new_china_dsp = (iso3 == "CHN") and (year_id >= 2018) and (data_type_id == 10)
    return is_old_china_dsp or is_new_china_dsp or is_thailand


def assert_no_six_minor_territories(df):
    """Ensure six minor territories locations not present."""
    urban_terr = [43871, 43876, 43878, 43879, 43889, 43897]
    rural_terr = [43907, 43912, 43914, 43915, 43925, 43933]
    assert not (
        df["location_id"].isin(urban_terr) | df["location_id"].isin(rural_terr)
    ).values.any()
    return df


def drop_data_out_of_scope(df, location_meta_df, source):
    """Drop data that is outside of the scope of analysis.

    Drop data before 1980, unless:
        it is ICD7A data
        it is historical Australia data
        it is ICD8A data - TODO not sure we should keep this
    """
    exceptions = ["ICD7A", "ITA_IRCCS_ICD8_detail"]
    if source not in exceptions:
        df = df.loc[(df["year_id"] >= 1980)]
    locations_in_hierarchy = location_meta_df["location_id"].unique()
    df = df[df["location_id"].isin(locations_in_hierarchy)]
    return df


def calculate_cc_code(df, env_meta_df, code_map):
    """Calculate total deaths denominator.

    Note: This step is usually done in formatting. Moving this calculation
    after age/sex splitting should return more accurate results for data that
    has a mix of known, detailed age groups and unknown ages.
    """
    df_cc = df.copy()

    group_cols = CodSchema.infer_from_data(df).demo_cols
    df_cc = df_cc.groupby(group_cols, as_index=False).deaths.sum()

    df_cc = add_envelope(df_cc, env_df=env_meta_df)
    df_cc["value"] = "cc_code"
    df_cc = add_code_metadata(df_cc, ["code_id"], merge_col="value", code_map=code_map)
    report_if_merge_fail(df_cc, ["code_id"], ["value"])
    df_cc["cause_id"] = 919
    df_cc["deaths"] = df_cc["mean_env"] - df_cc["deaths"]
    assert df_cc.notnull().values.any()

    df = pd.concat([df, df_cc], ignore_index=True)

    assert np.isclose(df["deaths"].sum(), df.mean_env.sum())
    df = df.drop(["mean_env", "value"], axis=1)

    return df


def collapse_sites(df, **cache_options):
    """
    Collapse sites together and reassign site_id.

    This function exists to collapse sites in historical VA data. When new VA is added to our
    database, sites should be collapsed in formatting, not here.
    """
    start_cols = df.columns
    start_deaths = df.deaths.sum()

    df = add_site_metadata(df, "site_name", **cache_options)
    report_if_merge_fail(df, check_col="site_name", merge_cols="site_id")
    old_sites = set(df.site_name.unique())

    site_dem_cols = ["nid", "extract_type_id", "location_id", "year_id"]
    df["agg_site"] = df.groupby(site_dem_cols)["site_name"].transform(
        lambda x: ", ".join(x.drop_duplicates().sort_values())
    )

    df = df.groupby(df.columns.drop(["site_id", "site_name", "deaths"]).tolist(), as_index=False)[
        "deaths"
    ].sum()
    df.rename({"agg_site": "site_name"}, axis="columns", inplace=True)

    too_long = df.site_name.str.len() > 250
    Bangladesh_study = (df.nid == 243436) & (df.extract_type_id == 1)
    Mozambique_study = (df.nid == 93710) & (df.extract_type_id == 1)
    df.loc[Bangladesh_study & too_long, "site_name"] = "51 upazilas in Bangladesh"
    df.loc[Mozambique_study & too_long, "site_name"] = "8 areas in Sofala province"

    df = add_site_metadata(df, add_cols="site_id", merge_col="site_name", **cache_options)
    report_if_merge_fail(df, check_col="site_id", merge_cols="site_name")

    new_sites = set(df.site_name.unique())
    if new_sites != old_sites:
        print_log_message("Collapsed sites: \n{} \ninto sites: \n{}".format(old_sites, new_sites))
    else:
        print_log_message("Did not collapse any sites")

    df.drop("site_name", axis="columns", inplace=True)
    assert set(df.columns) == set(start_cols)
    assert np.isclose(df.deaths.sum(), start_deaths)

    return df


def run_pipeline(
    df: pd.DataFrame,
    nid: int,
    extract_type_id: int,
    launch_set_id: int,
    diagnostic: bool = False,
) -> pd.DataFrame:
    """Run the full pipeline, chaining together CodProcesses."""
    cache_kwargs = {
        "force_rerun": False,
        "block_rerun": True,
        "cache_results": False,
        "cache_dir": CONF.get_directory("db_cache"),
        "verbose": False,
    }
    project_id = CONF.get_id("project")
    cause_set_version_id = CONF.get_id("cause_set_version")
    location_set_version_id = CONF.get_id("location_set_version")
    pop_run_id = CONF.get_id("pop_run")
    env_run_id = CONF.get_id("env_run")
    code_system_id, data_type_id, iso3, source, year_id = get_values_from_nid(
        nid,
        extract_type_id,
        values=["code_system_id", "data_type_id", "iso3", "source", "year_id"],
        **cache_kwargs,
    )
    distribution_set_version_id = CONF.get_id(
        "NOR_distribution_set_version" if iso3 == "NOR" else "distribution_set_version"
    )
    location_meta_df = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id, **cache_kwargs
    )
    code_map_version_id = get_map_version(code_system_id, **cache_kwargs)
    code_map = get_cause_map(
        code_map_version_id=code_map_version_id,
        **cache_kwargs,
    )

    if data_type_id == 8:
        print_log_message("Collapsing sites in VA")
        df = collapse_sites(df, **cache_kwargs)

    print_log_message("Overriding causes when necessary")
    df = overrides(df, location_meta_df)

    print_log_message("Dropping data out of scope")
    df = drop_data_out_of_scope(df, location_meta_df, source)
    if len(df) > 0:
        assert_no_six_minor_territories(df)

        print_log_message("\nDeaths before MAPPING: {}".format(df.deaths.sum()))
        Mapper = GBDCauseMapper(project_id, cause_set_version_id, code_map)
        df = Mapper.get_computed_dataframe(df, code_system_id)
        if diagnostic:
            write_phase_output(
                df, "mapping", nid, extract_type_id, launch_set_id, sub_dirs="diagnostic"
            )

        print_log_message("\nDeaths before AGESEXSPLIT: {}".format(df.deaths.sum()))
        MySplitter = AgeSexSplitter(
            cause_set_version_id,
            pop_run_id,
            distribution_set_version_id,
            verbose=True,
            collect_diagnostics=diagnostic,
        )

        df = MySplitter.get_computed_dataframe(df, location_meta_df)
        if diagnostic:
            diag_df = MySplitter.get_diagnostic_dataframe()
            write_phase_output(
                diag_df,
                "agesexsplit",
                nid,
                extract_type_id,
                launch_set_id,
                sub_dirs="diagnostic",
            )

        print_log_message("\nDeaths before CORRECTIONS: {}".format(df.deaths.sum()))
        age_remapper = AgeRemap(code_system_id, cause_set_version_id)
        df = age_remapper.get_computed_dataframe(df)

        age_sex_remapper = AgeSexRemap(
            code_system_id, cause_set_version_id, collect_diagnostics=False, verbose=True
        )
        df = age_sex_remapper.get_computed_dataframe(df)

        if source in [
            "Iran_maternal_surveillance",
            "Iran_NAME_forensic",
            "US_police_conflict",
        ]:
            env_meta_df = get_env(env_run_id=env_run_id, **cache_kwargs)
            df = calculate_cc_code(df, env_meta_df, code_map)
            print_log_message("\nDeaths after adding cc_code: {}".format(df.deaths.sum()))

        if source in ["NZL_MOH_ICD9", "NZL_MOH_ICD10"]:
            df = correct_maori_non_maori_deaths(df)
            print_log_message(
                "\nDeaths after Maori/non-Maori adjustment: {}".format(df.deaths.sum())
            )

        if code_system_id in [1, 6]:
            print_log_message(
                "Special correction step for leukemia subtypes by age in ICD9 and ICD10"
            )
            df = adjust_leukemia_subtypes(df, code_system_id, code_map_version_id)

        if needs_liver_cancer_correction(iso3, code_system_id, year_id, data_type_id):
            print_log_message("Applying Liver cancer adjustment (C22.9 remapping)")
            try:
                year_id = int(year_id)
            except TypeError:
                raise ValueError(f"Need a single year, but got: {year_id}")
            df = adjust_liver_cancer(df, iso3, year_id, data_type_id, code_system_id, code_map_version_id)

        if code_system_id == 1:

            g31_code_ids = [6477, 6478, 6479, 6480, 6481, 6483, 6484, 6485, 6486, 6487, 6488, 6489, 6490]
            df.loc[df.code_id.isin(g31_code_ids), 'cause_id'] = 1149
            df.loc[df.code_id.isin(g31_code_ids), 'code_id'] = 5200

        print_log_message("\nDeaths at END: {}".format(df.deaths.sum()))

    return df


def cause_overrides(df, location_ids, year, old_causes, new_cause):
    """
    Takes in a list of location ids and the earliest year that you want to change
    as well as old and new cause ids
    """
    for location in location_ids:
        for old_cause in old_causes:
            df.loc[
                (df["location_id"] == location)
                & (df["code_id"] == old_cause)
                & (df["year_id"] >= year),
                "code_id",
            ] = new_cause
    return df


def overrides(df, locs):
    original_deaths = df["deaths"].sum()
    original_shape = df.shape
    original_diarrheal_deaths = df[df["code_id"].isin([95, 96, 13851])]["deaths"].sum()

    df = cause_overrides(df, [89], 2013, [95, 96], 13851)
    df = cause_overrides(df, [93], 2010, [95, 96], 13851)
    df = cause_overrides(df, [81], 2011, [95, 96], 13851)
    df = cause_overrides(df, [80], 2011, [95, 96], 13851)
    df = cause_overrides(df, [71], 2013, [95, 96], 13851)
    UK = locs.query("iso3 == 'GBR'").location_id.unique().tolist()
    df = cause_overrides(df, UK, 2011, [95, 96], 13851)
    USA = locs.query("iso3 == 'USA'").location_id.unique().tolist()
    df = cause_overrides(df, USA, 2011, [95, 96], 13851)
    ITA = locs.query("iso3 == 'ITA'").location_id.unique().tolist()
    df = cause_overrides(df, ITA, 2016, [95, 96], 13851)

    assert original_deaths == df["deaths"].sum()
    assert original_shape == df.shape
    assert original_diarrheal_deaths == df[df["code_id"].isin([95, 96, 13851])]["deaths"].sum()

    return df


def main(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    df = get_phase_output(
        "formatted",
        nid,
        extract_type_id,
        get_input_launch_set_id(nid, extract_type_id, launch_set_id, "formatted"),
    )
    assert not df.empty, "Dataframe is empty. Are you sure this source is in the input database?"
    df = run_pipeline(df, nid, extract_type_id, launch_set_id, diagnostic=False)
    write_phase_output(df, "disaggregation", nid, extract_type_id, launch_set_id)
    return df


if __name__ == "__main__":
    main(
        nid=int(sys.argv[1]),
        extract_type_id=int(sys.argv[2]),
        launch_set_id=int(sys.argv[3]),
    )
