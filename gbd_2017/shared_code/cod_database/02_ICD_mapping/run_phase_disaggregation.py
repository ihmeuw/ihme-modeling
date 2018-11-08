import sys
import os
import pandas as pd
import numpy as np
this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, 'FILEPATH'))
sys.path.append(repo_dir)
from mapping import GBDCauseMapper
from age_sex_split import AgeSexSplitter
from configurator import Configurator
from correct_restrictions import RestrictionsCorrector
from adjust_nzl_deaths import correct_maori_non_maori_deaths
from cod_prep.downloaders import (
    get_current_location_hierarchy, get_datasets,
    get_value_from_nid, get_env, add_envelope,
    get_cause_map, add_code_metadata
)
from claude_io import (
    get_claude_data,
    write_phase_output,
    delete_claude_output,
    makedirs_safely
)
from cod_prep.utils import report_if_merge_fail


"""
take in nid as an argument

download data from the input database

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


def make_clean_dirs(nid, extract_type_id):
    """Make and clean up any directories as needed.
    """
    data_dir = ("FILEPATH").format(nid, extract_type_id)
    makedirs_safely(data_dir)
    delete_claude_output('disaggregation', nid,
                         extract_type_id)


def drop_data_out_of_scope(df, location_meta_df, source):

    if source != 'ICD7A':
        df = df.loc[(df['year_id'] >= 1980)]
    locations_in_hierarchy = location_meta_df['location_id'].unique()
    df = df[df['location_id'].isin(locations_in_hierarchy)]
    return df


def calculate_cc_code(df, env_meta_df, code_map):

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


def run_pipeline(nid, extract_type_id, launch_set_id,
                 df, code_system_id, cause_set_version_id,
                 location_set_version_id, pop_run_id, env_run_id,
                 distribution_set_version_id, diagnostic=False):
    """Run the full pipeline"""

    cache_options = {
        'force_rerun': False,
        'block_rerun': True,
        'cache_results': False,
        'cache_dir': CONF.get_directory('FILEPATH'),
        'verbose': False
    }

    location_meta_df = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id,
        **cache_options
    )

    code_map = get_cause_map(code_system_id=code_system_id, **cache_options)

    source = get_value_from_nid(nid, "source", extract_type_id)

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
        Corrector = RestrictionsCorrector(
            code_system_id, cause_set_version_id,
            collect_diagnostics=False, verbose=True
        )
        df = Corrector.get_computed_dataframe(df)

        # calculate cc_code for some sources
        if source in ['Iran_maternal_surveillance', 'Iran_forensic']:
            env_meta_df = get_env(env_run_id=env_run_id, **cache_options)
            df = calculate_cc_code(df, env_meta_df, code_map)
            print("\nDeaths after adding cc_code: {}".format(
                df.deaths.sum()))

        # adjust deaths for New Zealand by maori/non-maori ethnicities
        if source in ["NZL_MOH_ICD9", "NZL_MOH_ICD10"]:
            df = correct_maori_non_maori_deaths(df)
            print("\nDeaths after Maori/non-Maori adjustment: {}".format(
                df.deaths.sum()))

        print("\nDeaths at END: {}".format(df.deaths.sum()))

    return df


def cause_overrides(df, location_ids, year, old_causes, new_cause):
    for location in location_ids:
        for old_cause in old_causes:
            df.loc[(df['location_id'] == location) & (df['code_id'] == old_cause) &
                   (df['year_id'] >= year), 'code_id'] = new_cause
    return df


def create_iso3(row):
    string = row['ihme_loc_id'].split("_") 
    return string[0]


def overrides(df, locs):
    locs['iso3'] = locs.apply(create_iso3, axis=1)

    original_deaths = df['deaths'].sum()
    original_shape = df.shape
    original_diarrheal_deaths = df[df['code_id'].isin([95, 96, 13851])]['deaths'].sum()

   
    df = cause_overrides(df, [89], 2013, [95, 96], 13851)
    
    df = cause_overrides(df, [93], 2010, [95, 96], 13851)
    
    df = cause_overrides(df, [81], 2011, [95, 96], 13851)
    
    df = cause_overrides(df, [80], 2011, [95, 96], 13851)
    
    df = cause_overrides(df, [71], 2013, [95, 96], 13851)
    UK = list(locs.query("iso3 == 'GBR'")['location_id'])
    
    df = cause_overrides(df, UK, 2011, [95, 96], 13851)
    USA = list(locs.query("iso3 == 'USA'")['location_id'])
    
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
                      df, code_system_id, cause_set_version_id,
                      location_set_version_id, pop_run_id, env_run_id,
                      distribution_set_version_id, diagnostic=False)
    # upload to database
    write_phase_output(df, 'disaggregation', nid, extract_type_id,
                       launch_set_id)


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    code_system_id = int(sys.argv[3])
    launch_set_id = int(sys.argv[4])
    main(nid, extract_type_id, code_system_id, launch_set_id)
