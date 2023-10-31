"""Noise reduction phase."""
import sys
import os
import getpass
import subprocess
import re
import pandas as pd

from cod_prep.downloaders import (
    get_current_location_hierarchy,
    add_location_metadata,
    get_current_cause_hierarchy,
    get_ages,
    add_nid_metadata,
    add_survey_type,
    add_cause_metadata,
    get_all_related_causes
)
from cod_prep.claude.configurator import Configurator
from cod_prep.utils import (
    print_log_message, report_if_merge_fail, submit_cod, wait,
    just_keep_trying, log_statistic, report_duplicates
)
from cod_prep.claude.claude_io import get_claude_data, makedirs_safely
from cod_prep.claude.squaring import Squarer
from cod_prep.claude.redistribution_variance import (
    modelgroup_has_redistribution_variance
)
from cod_prep.utils.nr_helpers import is_country_vr_non_subnat, is_region_vr

pd.options.mode.chained_assignment = None

CONF = Configurator('standard')
RUN_BY_CAUSE_ROW_THRESHOLD = 500000
IND_SRS_SOURCES = ["India_SRS_states_report", "India_SRS_Maternal_states"]
IDN_SRS_SOURCES = ["Indonesia_SRS_2014", "Indonesia_SRS_province"]
MATLAB_SOURCES = ['Matlab_1963_1981', 'Matlab_1982_1986', 'Matlab_1987_2002',
                  'Matlab_2003_2006', 'Matlab_2007_2012', 'Matlab_2011_2014']
MATERNAL_NR_SOURCES = [
    "Other_Maternal", "Mexico_BIRMM", "Maternal_report", "SUSENAS",
    "China_MMS", "China_Child",
]


def model_group_is_run_by_cause(model_group):
    return (
        (model_group.startswith("VR")) or
        (model_group.startswith("MATERNAL")) or
        (model_group.startswith("Cancer")) or
        (model_group == "VA-G")
    )


def get_model_data(model_group, project_id, location_hierarchy,
                   location_set_version_id, cause_meta_df):
    """Get data to run in NR model with incoming data."""
    iso3s = location_hierarchy.query('level == 3')['ihme_loc_id'].unique()
    regions = location_hierarchy.query('level == 2')['ihme_loc_id'].unique()
    super_region_ids = location_hierarchy.query(
        'level == 1')['location_id'].unique()

    super_region_ids = [str(s) for s in super_region_ids]
    super_region_to_region_ids = location_hierarchy.query('level == 2')

    super_region_to_region_ids = (
        super_region_to_region_ids[['location_id', 'parent_id']].groupby(
            'parent_id'
        ).apply(lambda df: list(set(df['location_id']))).to_dict()
    )

    regions_to_ids = location_hierarchy.query(
        'level == 2').set_index('ihme_loc_id')['region_id']

    level_three_location_ids = location_hierarchy.query(
        'level == 3')['location_id'].unique()

    model_group_filters = {}

    bad_model_group = False
    if model_group.startswith("VR-"):
        model_group_filters['data_type_id'] = [9, 10]
        loc_code = model_group.replace("VR-", "")
        if loc_code in iso3s:
            model_group_filters['iso3'] = loc_code
        elif loc_code in regions:
            region_id = regions_to_ids[loc_code]
            model_group_filters['region_id'] = region_id
            model_group_filters['exec_function'] = restrict_to_location_ids
            model_group_filters['exec_function_args'] = [
                level_three_location_ids
            ]
        elif loc_code == "GRL-AK":
            AK_LOC_ID = 524
            GRL_LOC_ID = 349
            model_group_filters['location_id'] = [AK_LOC_ID, GRL_LOC_ID]
        else:
            bad_model_group = True
    elif model_group.startswith("VA-"):
        model_group_filters['data_type_id'] = [8, 12]
        if model_group == "VA-SRS-IND":
            model_group_filters['source'] = IND_SRS_SOURCES
        elif model_group == "VA-SRS-IDN":
            model_group_filters['source'] = IDN_SRS_SOURCES
        elif model_group == "VA-Matlab":
            model_group_filters['source'] = MATLAB_SOURCES
        elif model_group == "VA-Nepal-Burden":
            model_group_filters['iso3'] = "NPL"
        elif model_group == "VA-IND":
            model_group_filters['iso3'] = "IND"
        elif model_group == "VA-158":
            model_group_filters['iso3'] = ['PAK', 'NPL', 'BGD', 'BTN']
        elif model_group == 'VA-G':
            pass
        else:
            loc_code = model_group.replace("VA-", "")
            if loc_code in super_region_ids:
                super_region_id = int(loc_code)
                model_group_filters['region_id'] = \
                    super_region_to_region_ids[super_region_id]
            else:
                bad_model_group = True

    elif model_group == "Cancer_Registry":
        model_group_filters['source'] = "Cancer_Registry"

    elif model_group.startswith("MATERNAL"):
        for source in MATERNAL_NR_SOURCES:
            if source in model_group:
                model_group_filters['source'] = source
        if "HH_SURVEYS" in model_group:
            model_group_filters['survey_type'] = ["DHS", "RHS", "AHS",
                                                  "DLHS", "NFHS"]
        model_group_filters['iso3'] = model_group[-3:]

    # special malaria model groups for VA data
    elif model_group.startswith('malaria'):
        model_group_filters['data_type_id'] = [8, 12]
        model_group_filters['malaria_model_group'] = model_group
        if "IND_SRS" in model_group:
            model_group_filters['source'] = IND_SRS_SOURCES
    elif model_group == "CHAMPS":
        model_group_filters['data_type_id'] = [12]
    # USA Police Violence surveillance studies
    elif model_group == 'POLICE_VIOLENCE-SURVEILLANCE-USA-FE':
        model_group_filters['nid'] = 448801
    elif model_group == 'POLICE_VIOLENCE-SURVEILLANCE-USA-MPV':
        model_group_filters['nid'] = 448802
    else:
        bad_model_group = True
    if bad_model_group:
        raise AssertionError(
            "Unrecognized model group: {}".format(bad_model_group)
        )

    model_df = get_claude_data(
        phase="aggregation",
        project_id=project_id,
        is_active=True,
        is_dropped=False,
        location_set_id=35,
        year_id=list(range(1980, 2050)),
        assert_all_available=True,
        location_set_version_id=location_set_version_id,
        **model_group_filters
    )

    add_cols = ['code_system_id']

    if model_group.startswith(("VA", "MATERNAL", "malaria", "CHAMPS")) or \
            model_group in ["VR-RUS", "VR-R9"]:
        add_cols.append('source')

    if model_group.startswith('MATERNAL-HH_SURVEYS'):
        model_df = add_survey_type(model_df)

    # add on code_system_id
    model_df = add_nid_metadata(
        model_df, add_cols, project_id=project_id, force_rerun=False, block_rerun=True,
        cache_dir='standard', cache_results=False
    )
    if model_group == "VR-RUS" or model_group == "VR-R9":
        replace_source = "Russia_FMD_ICD9"
        replace_csid = 213
        fmd_conv_10 = model_df['source'] == replace_source
        num_replace = len(model_df[fmd_conv_10])
        assert num_replace > 0, \
            "No rows found with source {} in " \
            "model group {}".format(replace_source, model_group)
        print_log_message(
            "Setting code system to {cs} for {s} "
            "source: {n} rows changed".format(
                cs=replace_csid, s=replace_source, n=num_replace)
        )
        model_df.loc[fmd_conv_10, 'code_system_id'] = replace_csid

    report_if_merge_fail(
        model_df, 'code_system_id', ['nid', 'extract_type_id']
    )

    # special source drops for certain groups
    model_df = drop_source_data(model_df, model_group, location_hierarchy,
                                cause_meta_df)

    return model_df


def drop_source_data(df, model_group, location_hierarchy, cause_meta_df):
    """Drop source specific data from model dataframe.
    """
    if model_group == "VA-IND":
        srs = df['source'].str.startswith("India_SRS")
        scd = df['source'].str.startswith("India_SCD")
        df = df[~(srs | scd)]


    if model_group == 'VA-158':
        df = df.loc[df.source != "Nepal_Burden_VA"]
        df = df.loc[~df.source.str.startswith("Matlab")]

    # special conditions for maternal sources
    if model_group.startswith("MATERNAL"):

        # grab countries for which we produce subnational estimates
        ihme_loc_dict = location_hierarchy.set_index(
            'location_id')['ihme_loc_id'].to_dict()
        df['iso3'] = df['location_id'].map(ihme_loc_dict)
        subnational_modeled_iso3s = CONF.get_id('subnational_modeled_iso3s')
        agg_locs = df['iso3'].isin(subnational_modeled_iso3s)

        # sources allowed to have location aggregates in model_df
        agg_sources = ["Other_Maternal", "Mexico_BIRMM"]
        no_loc_agg_source = ~(df['source'].isin(agg_sources))

        df = df[~(no_loc_agg_source & agg_locs)]

        # cleanup extra columns
        df.drop('iso3', axis=1, inplace=True)

    # these causes were likely introduced when adding in rd variance
    df = add_cause_metadata(df, 'yld_only', cause_meta_df=cause_meta_df)
    df = df.loc[df['yld_only'] != 1]
    df = df.drop('yld_only', axis=1)

    if model_group == 'VR-BOL':
        chagas = df['cause_id'] == 346
        df = df.loc[chagas]

    malaria = df['cause_id'] == 345
    if model_group.startswith('malaria'):
        df = df.loc[malaria]
    if model_group in ["malaria_IND_hypoendem", "malaria_IND_SRS_hypoendem"]:
        df = df.query('location_id != 163')
    if model_group in ["malaria_IND_mesoendem", "malaria_IND_SRS_mesoendem"]:
        df = df.loc[~(df['location_id'].isin([43902, 43938]))]

    # VA model groups use CHAMPS, but CHAMPS has a much longer
    # cause list - drop any cause/sex that is only present in CHAMPS
    if model_group.startswith("VA-"):
        assert df.source.notnull().all()
        df['only_champs'] = df.groupby(['cause_id', 'sex_id'])[
            'source'].transform(lambda x: set(x) == {'CHAMPS'})
        assert (df.loc[df.only_champs, 'source'] == 'CHAMPS').all()
        df = df.loc[~df.only_champs]
        df = df.drop('only_champs', axis=1)

    return df


def restrict_to_location_ids(df, location_ids):
    """Sub set location ids."""
    df = df.loc[
        df['location_id'].isin(location_ids)
    ]
    return df


def square_dhs_data(model_df, cause_meta_df, age_meta_df, location_hierarchy):
    dhs = model_df['survey_type'] == "DHS"
    non_dhs = model_df[~dhs]
    dhs = model_df[dhs]

    if len(dhs) > 0:
        # get df with id_cols to merge on apres square
        nid_loc_df = model_df[
            ['nid', 'location_id', 'site_id', 'extract_type_id']
        ].drop_duplicates()
        print_log_message(
            "Bringing back zeros (squaring) so noise reduction "
            "knows to depress time series"
        )
        squarer = Squarer(cause_meta_df, age_meta_df,
                          location_meta_df=location_hierarchy, data_type='DHS')
        dhs = squarer.get_computed_dataframe(dhs)

        # fill in some metadata
        dhs['code_system_id'].fillna(177, inplace=True)
        dhs['source'].fillna("Other_Maternal", inplace=True)
        dhs['survey_type'].fillna("DHS", inplace=True)

        dhs = nid_loc_df.merge(dhs, on=['location_id', 'site_id'], how='right')

        tls = dhs.query('location_id == 19')
        dhs = dhs.query('location_id != 19')
        tls = tls.drop_duplicates(
            subset=['location_id', 'year_id', 'cause_id', 'age_group_id',
                    'sex_id', 'nid_y', 'extract_type_id_y'], keep='first'
        )
        dhs = pd.concat([tls, dhs], ignore_index=True)

        # replace null values with merged
        dhs.loc[dhs['nid_y'].isnull(), 'nid_y'] = dhs['nid_x']
        dhs.loc[
            dhs['extract_type_id_y'].isnull(), 'extract_type_id_y'
        ] = dhs['extract_type_id_x']

        # clean up
        dhs.drop(['extract_type_id_x', 'nid_x', 'iso3'], axis=1, inplace=True)
        dhs.rename(columns={
            'nid_y': 'nid', 'extract_type_id_y': 'extract_type_id'
        }, inplace=True)

        # fill in sample size for rows that were newly created
        # want sample size to be > 0 for noise reduction
        dhs.loc[dhs['sample_size'] == 0, 'sample_size'] = 0.5

        # append all household survey types back together
        model_df = pd.concat([dhs, non_dhs], ignore_index=True)

    assert model_df.notnull().values.any()
    report_duplicates(
        model_df, ['year_id', 'sex_id', 'location_id', 'cause_id',
                   'age_group_id', 'nid', 'extract_type_id', 'site_id'])

    return model_df


def get_code_system_cause_ids(df):
    df = df.copy()
    df = df[['cause_id', 'code_system_id']].drop_duplicates()
    cs_cause_dict = df.groupby(
        'code_system_id'
    ).apply(lambda x: x['cause_id'].values.tolist()).to_dict()
    return cs_cause_dict


def restrict_to_cause_ids(code_system_cause_dict, df):
    """Restrict to cause_ids present in aggregation phase output.
    """
    df = df.copy()
    df_list = []
    for code_system_id, cause_ids in code_system_cause_dict.items():
        cs_df = df.loc[
            (
                (df['code_system_id'] == code_system_id) &
                (df['cause_id'].isin(code_system_cause_dict[code_system_id]))
            )
        ]
        df_list.append(cs_df)
    df = pd.concat(df_list)

    return df


def format_for_nr(df, location_hierarchy):
    """Merge on needed location metadata."""
    locs = df[['location_id']].drop_duplicates()
    locs = add_location_metadata(
        locs,
        add_cols=["ihme_loc_id", "path_to_top_parent"],
        merge_col="location_id",
        location_meta_df=location_hierarchy
    )
    report_if_merge_fail(locs, 'path_to_top_parent', 'location_id')
    locs['country_id'] = locs['path_to_top_parent'].str.split(",").apply(
        lambda x: int(x[3]))
    locs['subnat_id'] = locs['ihme_loc_id'].apply(
        lambda x: int(x.split("_")[1]) if "_" in x else 0)
    locs['iso3'] = locs['ihme_loc_id'].str.slice(0, 3)
    different_locations = locs['country_id'] != locs['location_id']

    locs.loc[different_locations, 'iso3'] = \
        locs['iso3'].apply(lambda x: x + "_subnat")
    locs = locs[['location_id', 'country_id', 'subnat_id', 'iso3']]
    df = df.merge(locs, on='location_id', how='left')
    report_if_merge_fail(locs, 'country_id', 'location_id')

    df['is_loc_agg'] = (
        df.eval("country_id == location_id")
        & (df["iso3"] != "GRL")
    ).astype(int)

    # remove 0 sample size rows
    df = df.loc[df['sample_size'] > 0]

    return df


def create_year_bins(df):
    """
    We have found that regional VR models produce poor priors
    when there is only country in a given year. Bin years
    to combat this
    """
    # Get number of locations per year in the data
    year_to_num_locs = df.astype({'year_id': int})\
        .groupby('year_id')['location_id'].apply(pd.Series.nunique)\
        .sort_index(ascending=False).to_dict()

    # Starting with the most recent year
    year_to_bin = {}
    for year, num_locs in year_to_num_locs.items():
        if year not in year_to_bin:
            if num_locs == 1:
                if year != 1980:
                    year_bin = f"{year - 1}_{year}"
                    pair_year = year - 1
                else:
                    bin_1981 = [
                        x for x in year_to_bin.values()
                        if '1981' in x
                    ]
                    if len(bin_1981) > 0:
                        year_bin = bin_1981[0]
                    else:
                        year_bin = "1980_1981"
                    pair_year = 1981
                year_to_bin[year] = year_bin
                year_to_bin[pair_year] = year_bin
            else:
                year_bin = str(year)
                year_to_bin[year] = year_bin

    # Create year bin column
    df['year_bin'] = df['year_id'].map(year_to_bin)
    report_if_merge_fail(df, 'year_bin', 'year_id')
    return df


def create_sparsity_flag(df, cause_meta_df):
    """
    Create a sparsity flag.
    """
    exclude_causes = pd.read_csv(
        CONF.get_resource("transmission_sensitive_causes"))
    exclude_causes = set(exclude_causes.acause.apply(
        get_all_related_causes, args=(cause_meta_df,)).explode())

    df['sparsity_flag'] = (
        (df.assign(deaths=df['cf'] * df['sample_size'])
           .groupby('cause_id')['deaths'].transform(sum) < 50) &
        df.assign(cas_sum=df.groupby(['cause_id', 'sex_id', 'age_group_id'])['cf'].transform(sum))
          .groupby('cause_id')['cas_sum'].transform(lambda x: (x == 0).any()) &
        ~df.cause_id.isin(exclude_causes)
    ).astype(int)
    assert df.sparsity_flag.notnull().all()
    return df


def write_nrmodel_data(df, model_group, launch_set_id, cause_id=None):
    """Write input data for the noisereduction model."""
    nr_dir = CONF.get_directory('nr_process_data')
    if cause_id is not None:
        outdir = "FILEPATH"
    else:
        outdir = "FILEPATH"
    makedirs_safely(outdir)

    if launch_set_id == 0:
        del_path = "FILEPATH"
        if os.path.exists(del_path):
            os.unlink(del_path)
        del_path = "FILEPATH"
        if os.path.exists(del_path):
            os.unlink(del_path)

    if cause_id is not None:
        write_df = df.loc[df['cause_id'] == cause_id]
    else:
        write_df = df

    write_df.to_csv("FILEPATH")


def determine_worker(model_group):
    claude_dir = CONF.get_directory('claude_code')
    if model_group.startswith(("VA", "malaria", "CHAMPS")):
        worker = "FILEPATH"
        shell_script = None
        language = "stata"
    else:
        worker = "FILEPATH"
        shell_script = None
        language = "r"
    return worker, shell_script, language


def run_phase_by_model_group(model_df, model_group, launch_set_id, queue='all.q'):
    """Run the model, parallelizing by country."""
    print_log_message("Writing NR input file")
    write_nrmodel_data(model_df, model_group, launch_set_id)
    # determine if the model_df has draws
    if pd.Series(['draw_' in x for x in model_df.columns]).any():
        num_draws = CONF.get_resource('uncertainty_draws')
    else:
        num_draws = 0
    params = [model_group, str(launch_set_id), CONF.get_directory("nr_process_data"), str(num_draws)]

    worker, shell_script, language = determine_worker(model_group)
    jobname = "claude_nrmodelworker_{model_group}".format(
        model_group=model_group)
    if model_group == 'VA-G':
        slots = 20
    else:
        slots = 1
    log_base_dir = "FILEPATH"
    submit_cod(
        jobname,
        slots,
        language,
        worker,
        params=params,
        verbose=(launch_set_id == 0),
        logging=True,
        log_base_dir=log_base_dir,
        shell_script=shell_script,
        queue=queue
    )
    wait("claude_nrmodelworker_{model_group}".format(
        model_group=model_group), 30)


def run_phase_by_cause(model_df, model_group, launch_set_id, queue='all.q'):
    """Run the model, parallelizing by country and cause."""
    nocause = model_df[model_df['cause_id'].isnull()]
    if len(nocause) > 0:
        raise AssertionError("Have {} rows with missing cause: {}".format(
            len(nocause),
            nocause
        ))
    causes = list(set(model_df['cause_id']))
    causes = [int(cause) for cause in causes]

    print_log_message(
        "Writing NR input file and submitting jobs for "
        "{} causes".format(len(causes)))

    log_base_dir = "FILEPATH"
    worker, shell_script, language = determine_worker(model_group)
    slots = 5
    if model_group == 'VR-GBR':
        cores = 25
    elif model_group == 'VA-G':
        cores = 1
    else:
        cores = 15
    subnat_iso3s = CONF.get_id('subnational_modeled_iso3s')
    for subnat_iso3 in subnat_iso3s:
        if model_group == "VR-{}".format(subnat_iso3):
            slots = 18
    if model_group == 'VR-GBR':
        slots = 100
    if model_group.startswith("VA-"):
        slots = 20

    num_draws = CONF.get_resource('uncertainty_draws')
    if not modelgroup_has_redistribution_variance(model_group):
        num_draws = 0

    for cause_id in causes:
        write_nrmodel_data(
            model_df, model_group, launch_set_id, cause_id=cause_id)
        params = [
            model_group, str(launch_set_id), CONF.get_directory("nr_process_data"),
            str(num_draws), str(int(cause_id))
        ]
        jobname = "claude_nrmodelworker_{model_group}_{cause_id}".format(
            model_group=model_group, cause_id=cause_id)

        submit_cod(
            jobname,
            slots,
            language,
            worker,
            cores=cores,
            params=params,
            verbose=(launch_set_id == 0),
            logging=True,
            log_base_dir=log_base_dir,
            shell_script=shell_script,
            queue=queue
        )

    wait("claude_nrmodelworker_{model_group}".format(
        model_group=model_group), 30)

    nr_dir = CONF.get_directory('nr_process_data')
    iso_dir = "FILEPATH"
    causes_outpath = "FILEPATH"
    cause_path = "FILEPATH"

    for cause_id in causes:
        outpath = cause_path.format(iso_dir=iso_dir, cause_id=cause_id,
                                    lsid=launch_set_id)

        just_keep_trying(
            os.path.exists,
            args=[outpath],
            max_tries=250,
            seconds_between_tries=6,
            verbose=True
        )

    print_log_message("Writing causes file to {}".format(causes_outpath))
    causes_out = model_df[
        list(set(model_df).intersection({'cause_id', 'sparsity_flag'}))
    ].drop_duplicates().astype(int)
    report_duplicates(causes_out, 'cause_id')
    causes_out.to_csv(causes_outpath, index=False)


def main(model_group, project_id, location_set_version_id, cause_set_version_id,
         launch_set_id, queue='all.q'):
    print_log_message(
        "Beginning NR modeling for model_group {}".format(model_group)
    )

    cache_dir = CONF.get_directory('db_cache')
    read_file_cache_options = {
        'block_rerun': True,
        'cache_dir': cache_dir,
        'force_rerun': False,
        'cache_results': False
    }

    print_log_message("Preparing location hierarchy")
    location_hierarchy = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id,
        **read_file_cache_options
    )

    cause_meta_df = get_current_cause_hierarchy(
        cause_set_version_id=cause_set_version_id,
        **read_file_cache_options
    )

    age_meta_df = get_ages(
        **read_file_cache_options
    )

    print_log_message("Preparing model data")
    model_df = get_model_data(
        model_group, project_id, location_hierarchy, location_set_version_id, cause_meta_df
    )

    print_log_message("Got {} rows of model data".format(len(model_df)))
    if len(model_df) == 0:
        print_log_message("Exiting...")
        return

    if 'deaths' in model_df.columns:
        model_df = model_df.drop('deaths', axis=1)

    # get the unique cause_ids by code_system
    code_system_cause_dict = get_code_system_cause_ids(model_df)

    # square data for certain data types
    if (model_group.startswith("VR")) or (model_group.startswith("Cancer")):
        data_type = "VR"
    elif model_group.startswith("VA-"):
        data_type = "VA"
    elif model_group == 'CHAMPS':
        data_type = "CHAMPS"
    else:
        data_type = None
    if data_type:
        print_log_message(
            "Bringing back zeros (squaring) so noise reduction "
            "knows to depress time series"
        )
        squarer = Squarer(cause_meta_df, age_meta_df, data_type=data_type)
        model_df = squarer.get_computed_dataframe(model_df)
    if "HH_SURVEYS" in model_group:
        model_df = square_dhs_data(model_df, cause_meta_df,
                                   age_meta_df, location_hierarchy)

    print_log_message(log_statistic(model_df))

    print_log_message("Restricting model data to only existing cause_ids")
    model_df = restrict_to_cause_ids(code_system_cause_dict, model_df)

    print_log_message("Adding NR location info")
    model_df = format_for_nr(model_df, location_hierarchy)

    if is_region_vr(model_group):
        print_log_message("Creating year bins")
        model_df = model_df.groupby('is_loc_agg').apply(
            create_year_bins).reset_index(drop=True)

    if is_country_vr_non_subnat(model_group):
        print_log_message("Creating sparsity flag")
        model_df = create_sparsity_flag(model_df, cause_meta_df)

    if model_group_is_run_by_cause(model_group):
        run_phase_by_cause(
            model_df, model_group, launch_set_id, queue=queue)
    else:
        run_phase_by_model_group(
            model_df, model_group, launch_set_id, queue=queue)

    print_log_message("Job complete. Exiting...")


if __name__ == "__main__":
    model_group = str(sys.argv[1])
    project_id = int(sys.argv[2])
    launch_set_id = int(sys.argv[3])
    queue = str(sys.argv[4])
    location_set_version_id = CONF.get_id('location_set_version')
    cause_set_version_id = CONF.get_id('cause_set_version')
    main(model_group, project_id, location_set_version_id, cause_set_version_id,
         launch_set_id, queue=queue)
