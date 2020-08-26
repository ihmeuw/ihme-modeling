import sys
import os
import pandas as pd
import numpy as np

from mapping import GBDCauseMapper
from age_sex_split import AgeSexSplitter
from configurator import Configurator
from correct_restrictions import RestrictionsCorrector
from correct_age_restrictions import AgeRestrictionsCorrector
from adjust_nzl_deaths import correct_maori_non_maori_deaths
from cod_prep.downloaders import (
    get_current_location_hierarchy, get_datasets,
    get_value_from_nid, get_env, add_envelope,
    get_cause_map, add_code_metadata, add_site_metadata
)
from cod_prep.claude.cause_reallocation import adjust_leukemia_subtypes
from claude_io import (
    get_claude_data,
    write_phase_output,
    delete_claude_output,
    makedirs_safely
)
from cod_prep.utils import report_if_merge_fail, print_log_message


"""
take in nid as an argument

download data from the input database (probably cod.input_data)

run mapping
    mapping is instantiated with a dataframe to map
    it returns a mapped dataframe

run age sex splitting
    age sex splitting instantiated with dataframe to split
    it returns a split dataframe

run restriction correction

upload to database
"""

CONF = Configurator()


def assert_no_six_minor_territories(df):
    """Ensure six minor territories locations not present."""
    urban_terr = [43871, 43876, 43878, 43879, 43889, 43897]
    rural_terr = [43907, 43912, 43914, 43915, 43925, 43933]
    assert not (df['location_id'].isin(urban_terr) |
                df['location_id'].isin(rural_terr)).values.any()
    return df


def drop_data_out_of_scope(df, location_meta_df, source):
    """Drop data that is outside of the scope of analysis.

    Drop data before 1970, unless:
        it is ICD7A data
        it is historical Australia data
        it is ICD8A data - TODO not sure we should keep this

    Directly modify the passed dataframe

    Args:
        df (DataFrame)

    Returns:
        DataFrame
    """
    # hist_aus = source.startswith('_Australia')
    # exceptions = ['ICD7A', 'ICD8A'] + hist_aus
    exceptions = ['ICD7A', 'ITA_IRCCS_ICD8_detail']
    # if source not in exceptions:
    if source not in exceptions:
        df = df.loc[(df['year_id'] >= 1980)]
    locations_in_hierarchy = location_meta_df['location_id'].unique()
    df = df[df['location_id'].isin(locations_in_hierarchy)]
    return df


def calculate_cc_code(df, env_meta_df, code_map):
    """Calculate total deaths denominator.

    Note: This step is usually done in formatting. Moving this calculation
    after age/sex splitting should return more accurate results for data that
    has a mix of known, detailed age groups and unknown ages.
    """
    df_cc = df.copy()

    # groupby everything except cause + code_id
    group_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id',
                  'nid', 'extract_type_id', 'site_id']
    df_cc = df_cc.groupby(group_cols, as_index=False).deaths.sum()

    # merge on envelope
    df_cc = add_envelope(df_cc, env_df=env_meta_df)
    df_cc['value'] = 'cc_code'
    df_cc = add_code_metadata(df_cc, ['code_id'], merge_col='value',
                              code_map=code_map)
    report_if_merge_fail(df_cc, ['code_id'], ['value'])
    df_cc['cause_id'] = 919
    df_cc['deaths'] = df_cc['mean_env'] - df_cc['deaths']
    assert df_cc.notnull().values.any()

    # append together
    df = pd.concat([df, df_cc], ignore_index=True)

    assert np.isclose(df['deaths'].sum(), df.mean_env.sum())
    df = df.drop(['mean_env', 'value'], axis=1)

    return df


def collapse_sites(df, **cache_options):
    """
    Collapse sites together and reassign site_id.

    This function exists to collapse sites in historical VA data. When new VA is added to our
    database, sites should be collapsed in formatting, not here.
    """
    # Get some properties so that we don't change things
    start_cols = df.columns
    start_deaths = df.deaths.sum()

    # Get the existing site names
    df = add_site_metadata(df, 'site_name', **cache_options)
    report_if_merge_fail(df, check_col='site_name', merge_cols='site_id')
    old_sites = set(df.site_name.unique())

    # Assign new aggregated sites
    # We assume that within the same study, location, and year, each site was surveyed
    # for the same ages, sexes, and causes, and that any missingness across age, sex,
    # or cause comes from a lack of deaths rather than a lack of observation
    site_dem_cols = ['nid', 'extract_type_id', 'location_id', 'year_id']
    df['agg_site'] = df.groupby(site_dem_cols)['site_name'].transform(
        lambda x: ', '.join(x.drop_duplicates().sort_values())
    )

    # Collapse the old sites and replace with the aggregate sites
    df = df.groupby(
        df.columns.drop(['site_id', 'site_name', 'deaths']).tolist(), as_index=False
    )['deaths'].sum()
    df.rename({'agg_site': 'site_name'}, axis='columns', inplace=True)

    # This is pretty bad, should think of something better
    # Handle aggregated sites that are too long for the db (>250 characters)
    too_long = (df.site_name.str.len() > 250)
    Bangladesh_study = ((df.nid == 243436) & (df.extract_type_id == 1))
    Mozambique_study = ((df.nid == 93710) & (df.extract_type_id == 1))
    df.loc[Bangladesh_study & too_long, 'site_name'] = '51 upazilas in Bangladesh'
    df.loc[Mozambique_study & too_long, 'site_name'] = '8 areas in Sofala province'

    # Add new site_ids
    df = add_site_metadata(df, add_cols='site_id', merge_col='site_name', **cache_options)
    report_if_merge_fail(df, check_col='site_id', merge_cols='site_name')

    # Report what changed (if anything)
    new_sites = set(df.site_name.unique())
    if new_sites != old_sites:
        print("Collapsed sites: \n{} \ninto sites: \n{}".format(old_sites, new_sites))
    else:
        print("Did not collapse any sites")

    df.drop("site_name", axis='columns', inplace=True)
    assert set(df.columns) == set(start_cols)
    assert np.isclose(df.deaths.sum(), start_deaths)

    return df


def run_pipeline(nid, extract_type_id, launch_set_id,
                 df, code_system_id, code_map_version_id, cause_set_version_id,
                 location_set_version_id, pop_run_id, env_run_id,
                 distribution_set_version_id, diagnostic=False):
    """Run the full pipeline, chaining together CodProcesses."""

    cache_options = {
        'force_rerun': False,
        'block_rerun': True,
        'cache_results': False,
        'cache_dir': CONF.get_directory('db_cache'),
        'verbose': False
    }

    location_meta_df = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id,
        **cache_options
    )

    code_map = get_cause_map(code_system_id=code_system_id,
            code_map_version_id=code_map_version_id, **cache_options)

    source = get_value_from_nid(nid, "source", extract_type_id)
    data_type_id = get_value_from_nid(nid, "data_type_id", extract_type_id)

    if data_type_id == 8:
        print("Collapsing sites in VA")
        df = collapse_sites(df, **cache_options)

    print("Overriding causes when necessary")
    df = overrides(df, location_meta_df)

    print("Dropping data out of scope")
    df = drop_data_out_of_scope(df, location_meta_df, source)
    if len(df) > 0:
        # make sure six minor territories are grouped correctly
        assert_no_six_minor_territories(df)

        # run mapping
        print("\nDeaths before MAPPING: {}".format(df.deaths.sum()))
        Mapper = GBDCauseMapper(cause_set_version_id, code_map)
        df = Mapper.get_computed_dataframe(df, code_system_id)
        if diagnostic:
            write_phase_output(df, 'mapping', nid, extract_type_id,
                               launch_set_id, sub_dirs='diagnostic')

        print("\nDeaths before AGESEXSPLIT: {}".format(df.deaths.sum()))
        # run age sex splitting
        MySplitter = AgeSexSplitter(
            cause_set_version_id,
            pop_run_id,
            distribution_set_version_id,
            verbose=True,
            collect_diagnostics=False
        )

        df = MySplitter.get_computed_dataframe(df, location_meta_df)
        if diagnostic:
            diag_df = MySplitter.get_diagnostic_dataframe()
            write_phase_output(diag_df, 'agesexsplit', nid, extract_type_id,
                               launch_set_id, sub_dirs='diagnostic')

        print("\nDeaths before CORRECTIONS: {}".format(df.deaths.sum()))
        # run restrictions corrections
        AgeCorrector = AgeRestrictionsCorrector(
            code_system_id, cause_set_version_id
        )
        df = AgeCorrector.get_computed_dataframe(df)

        Corrector = RestrictionsCorrector(
            code_system_id, cause_set_version_id,
            collect_diagnostics=False, verbose=True
        )
        df = Corrector.get_computed_dataframe(df)

        # calculate cc_code for some sources
        if source in ['Iran_maternal_surveillance']:
            env_meta_df = get_env(env_run_id=env_run_id, **cache_options)
            df = calculate_cc_code(df, env_meta_df, code_map)
            print("\nDeaths after adding cc_code: {}".format(
                df.deaths.sum()))

        # adjust deaths for New Zealand by maori/non-maori ethnicities
        if source in ["NZL_MOH_ICD9", "NZL_MOH_ICD10"]:
            df = correct_maori_non_maori_deaths(df)
            print("\nDeaths after Maori/non-Maori adjustment: {}".format(
                df.deaths.sum()))

        if code_system_id in [1, 6]:
            print_log_message("Special correction step for leukemia subtypes by age in ICD9 and ICD10")
            df = adjust_leukemia_subtypes(df, code_system_id, code_map_version_id)

        print("\nDeaths at END: {}".format(df.deaths.sum()))

    return df

# takes in a list of location ids and the earliest year that you want to change
# as well as old and new cause ids


def cause_overrides(df, location_ids, year, old_causes, new_cause):
    for location in location_ids:
        for old_cause in old_causes:
            df.loc[(df['location_id'] == location) & (df['code_id'] == old_cause) &
                   (df['year_id'] >= year), 'code_id'] = new_cause
    return df


def create_iso3(row):
    string = row['ihme_loc_id'].split("_")  # gets iso3 from ihme_loc_id
    return string[0]


def overrides(df, locs):
    locs['iso3'] = locs.apply(create_iso3, axis=1)

    original_deaths = df['deaths'].sum()
    original_shape = df.shape
    original_diarrheal_deaths = df[df['code_id'].isin([95, 96, 13851])]['deaths'].sum()

    # netherlands after 2013, 95-A090, 96-A099, 13851-K529 diarrhea remap to IBD
    df = cause_overrides(df, [89], 2013, [95, 96], 13851)
    # Sweden after 2010, 95-A090, 96-A099, 13851-K529 diarrhea remap to IBD
    df = cause_overrides(df, [93], 2010, [95, 96], 13851)
    # Germany after 2011, 95-A090, 96-A099, 13851-K529 diarrhea remap to IBD
    df = cause_overrides(df, [81], 2011, [95, 96], 13851)
    # France after 2011, 95-A090, 96-A099, 13851-K529 diarrhea remap to IBD
    df = cause_overrides(df, [80], 2011, [95, 96], 13851)
    # Australia after 2013, 95-A090, 96-A099, 13851-K529 diarrhea remap to IBD
    df = cause_overrides(df, [71], 2013, [95, 96], 13851)
    UK = list(locs.query("iso3 == 'GBR'")['location_id'])
    # UK after 2011, 95-A090, 96-A099, 13851-K529 diarrhea remap to IBD
    df = cause_overrides(df, UK, 2011, [95, 96], 13851)
    USA = list(locs.query("iso3 == 'USA'")['location_id'])
    # US after 2011, 95-A090, 96-A099, 13851-K529 diarrhea remap to IBD
    df = cause_overrides(df, USA, 2011, [95, 96], 13851)

    assert original_deaths == df['deaths'].sum()
    assert original_shape == df.shape
    assert original_diarrheal_deaths == df[df['code_id'].isin([95, 96, 13851])]['deaths'].sum()

    return df


def main(nid, extract_type_id, code_system_id, launch_set_id):

    cause_set_version_id = CONF.get_id('cause_set_version')
    location_set_version_id = CONF.get_id('location_set_version')
    pop_run_id = CONF.get_id('pop_run')
    env_run_id = CONF.get_id('env_run')

    # need to use special age/sex distribution for Norway based on National data
    if get_value_from_nid(nid, 'iso3', extract_type_id=extract_type_id) == 'NOR':
        distribution_set_version_id = CONF.get_id('NOR_distribution_set_version')
    else:
        distribution_set_version_id = CONF.get_id('distribution_set_version')

    # download data from input database
    df = get_claude_data('formatted',
                         nid=nid,
                         extract_type_id=extract_type_id,
                         location_set_version_id=location_set_version_id)

    assert len(df) != 0, ("Dataframe is empty."
                          " Are you sure this source is in"
                          "the input database?")
    # run the pipeline
    df = run_pipeline(nid, extract_type_id, launch_set_id,
                      df, code_system_id, code_map_version_id, cause_set_version_id,
                      location_set_version_id, pop_run_id, env_run_id,
                      distribution_set_version_id, diagnostic=False)
    # upload to database
    write_phase_output(df, 'disaggregation', nid, extract_type_id,
                       launch_set_id)


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    code_system_id = int(sys.argv[3])
    code_map_version_id = int(sys.argv[4])
    launch_set_id = int(sys.argv[5])
    main(nid, extract_type_id, code_system_id, launch_set_id)
