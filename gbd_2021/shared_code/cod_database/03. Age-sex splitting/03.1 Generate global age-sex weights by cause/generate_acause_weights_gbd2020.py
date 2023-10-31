import pandas as pd
import numpy as np
import os
import sys
import getpass

USER = getpass.getuser()
COD_REPO = "FILEPATH".format(user=USER)
sys.path.append(COD_REPO)

from cod_prep.claude.claude_io import get_claude_data
from cod_prep.downloaders.ages import get_cod_ages, get_ages, add_age_metadata
from cod_prep.downloaders.locations import get_current_location_hierarchy, add_location_metadata
from cod_prep.downloaders.causes import get_current_cause_hierarchy, add_cause_metadata, get_all_related_causes
from cod_prep.downloaders.nids import get_datasets
from cod_prep.downloaders.population import get_pop, get_env
from cod_prep.downloaders.engine_room import get_cause_map
from cod_prep.utils import report_if_merge_fail, report_duplicates
from cod_prep.claude.configurator import Configurator

CONF = Configurator('standard')

ID_COLS = ['ihme_loc_id', 'year_id', 'age_group_id', 'sex_id', 'cause_id']
AGE_WEIGHT_COLS = ['age_group_id', 'sex_id', 'cause_id']
SHOCKS_DIR = "FILEPATH"
CACHED_INPUT_VR = "FILEPATH"
WEIGHTS_OUT_DIR = "FILEPATH"

def get_good_ages():
    cod_age_df = get_cod_ages()
    good_ages = cod_age_df.age_group_id.unique().tolist()
    return good_ages

def add_shock_data(df):
    df['shock_data'] = 0
    files = ["FILEPATHS"]
    for f in files:
        shock_temp = pd.read_csv("{}/{}".format(SHOCKS_DIR, f))
        shock_temp['shock_data'] = 1
        df = df.append(shock_temp, ignore_index=True)
    return df

def map_gbd_cause(df, csid, cause_meta):
    cm = get_cause_map(csid)
    cm = cm[['code_id', 'cause_id']]
    df = df.merge(cm, on='code_id', how='left')
    report_if_merge_fail(df, 'cause_id', 'code_id')

    # if it isn't ICD9 or ICD10, don't include war, execution, or disasters
    if csid not in [1, 6]:
        war_causes = get_all_related_causes('inj_war_warterror', cause_meta_df=cause_meta)
        disaster_causes = get_all_related_causes('inj_disaster', cause_meta_df=cause_meta)
        execution_causes = get_all_related_causes('inj_war_execution', cause_meta_df=cause_meta)
        drop_causes = war_causes + disaster_causes + execution_causes
        df = df.loc[~df.cause_id.isin(drop_causes)]
    if csid == 8:
        df = df.loc[df.cause_id != 321]
    return df

def load_data(cause_meta):
    """
    Load the data, and map GBD cause. Pulling this data by source/csid in order to
    be able to easily map causes.
    """
    dfs = []
    # first pull all the active VR from 1980 onwards by source/code_system (no MCCD)
    ds = get_datasets(is_active=True, data_type_id=[9,10], year_id=list(range(1980, 2020)))
    ds = ds.loc[~(ds.source.str.contains('MCCD'))]
    src_csids = ds[['source', 'code_system_id']].drop_duplicates().to_records(index=False).tolist()
    for src, csid in src_csids:
        df = get_claude_data(
            'formatted',
            data_type_id=[9, 10],
            source=src,
            code_system_id=csid,
            is_active=True,
            year_id=list(range(1980, 2020)),
            location_set_id=CONF.get_id('location_set'),
            location_set_version_id=CONF.get_id('location_set_version')
        )
        df = map_gbd_cause(df, csid, cause_meta)
        dfs.append(df)

    old_ds = get_datasets(data_type_id=[9, 10], source=['ICD8A', 'ICD8_detail', 'ICD7A'], year_id=list(range(1970, 1980)))
    src_csids_old = old_ds[['source', 'code_system_id']].drop_duplicates().to_records(index=False).tolist()
    for src, csid in src_csids_old:
        df = get_claude_data(
            'formatted',
            data_type_id=[9, 10],
            source=src,
            code_system_id=csid,
            year_id=list(range(1970, 1980)),
            location_set_id=CONF.get_id('location_set'),
            location_set_version_id=CONF.get_id('location_set_version')
        )
        df = map_gbd_cause(df, csid, cause_meta)
        dfs.append(df)

    # get SRS data
    srs_ds = get_datasets(
        data_type_id=8,
        source=['Indonesia_SRS_province', 'Indonesia_SRS_2014', 'India_SRS_states_report'],
        is_active=True
    )
    src_csids_srs = srs_ds[['source', 'code_system_id']].drop_duplicates().to_records(index=False).tolist()
    for src, csid in src_csids_srs:
        df = get_claude_data(
            'formatted',
            data_type_id=[8],
            source=src,
            code_system_id=csid,
            is_active=True,
            year_id=list(range(1980, 2020)),
            location_set_id=CONF.get_id('location_set'),
            location_set_version_id=CONF.get_id('location_set_version')
        )
        df = map_gbd_cause(df, csid, cause_meta)
        dfs.append(df)

    # concat it all together
    df = pd.concat(dfs, ignore_index=True)

    df = add_shock_data(df)
    return df

def aggregate_to_national(df, loc_meta):
    keep_locs = loc_meta.location_id.unique().tolist()
    df = df.loc[df.location_id.isin(keep_locs)]

    df = add_location_metadata(df, 'ihme_loc_id', location_meta_df=loc_meta)
    report_if_merge_fail(df, 'ihme_loc_id', 'location_id')
    df['ihme_loc_id'] = df['ihme_loc_id'].str.slice(0,3)
    df = df.groupby(ID_COLS + ['shock_data'], as_index=False).deaths.sum()
    return df

def limit_pre_1980(df, cause_meta, age_meta):
    pre_1980_cause_names = ['tb', 'diarrhea', 'lri', 'meningitis', 'encephalitis', 'diptheria',
        'whooping', 'tetanus', 'measles', 'varicella', 'malaria', 'ntd_leish_visc', 'neonatal',
        'neonatal_preterm', 'neonatal_enceph', 'neonatal_sepsis', 'std_syphilis', 'hepatitis',
        'neonatal_sepsis_gbs'
    ]
    pre_1980_causes = cause_meta.loc[cause_meta.acause.isin(pre_1980_cause_names)].cause_id.unique().tolist()
    pre_1980_df = df.loc[
        (df.year_id < 1980) &
        (df.year_id >= 1970) &
        (df.cause_id.isin(pre_1980_causes))
    ]
    pre_1980_df = add_age_metadata(pre_1980_df, 'simple_age', age_meta_df=age_meta)
    pre_1980_df = pre_1980_df.loc[pre_1980_df.simple_age < 15]
    df = df.loc[(df.year_id >= 1980) | (df.shock_data == 1)]
    df = df.append(pre_1980_df, ignore_index=True)
    return df

def limit_malaria(df, cause_meta):
    malaria_countries = [
        'BRA', 'COL', 'CPV', 'DOM', 'ECU', 'GHA', 'GTM', 'GUY', 'HND',
        'HTI', 'IDN', 'IND', 'LKA', 'MLI', 'MYS', 'NIC', 'PER', 'PHL', 'PNG', 'SLV',
        'SUR', 'THA', 'TJK', 'VEN', 'ZWE'
    ]
    malaria_causes = get_all_related_causes('malaria', cause_meta_df=cause_meta)
    df = df.loc[~(
        (df.cause_id.isin(malaria_causes)) &
        (~df.ihme_loc_id.isin(malaria_countries))
    )]
    return df

def restrict_shock_causes(df, cause_meta):
    war_causes = get_all_related_causes('inj_war_warterror', cause_meta_df=cause_meta)
    disaster_causes = get_all_related_causes('inj_disaster', cause_meta_df=cause_meta)
    execution_causes = get_all_related_causes('inj_war_execution', cause_meta_df=cause_meta)
    drop_causes = war_causes + disaster_causes + execution_causes
    df = df.loc[~((df.ihme_loc_id == 'IRN') & (df.cause_id.isin(war_causes)) & (df.shock_data == 0))]
    df = df.loc[~((df.cause_id.isin(drop_causes)) & (df.deaths < 10))]
    return df

def clean_data(df, loc_meta, cause_meta, age_meta):
    """
    Limit to detailed GBD ages and sexes, and aggregate to the national.

    Limit the pre-1980 data to a select set of causes and ages.

    Limiting malaria to select countries, small counts of malaria deaths in several
    developed countries are throwing off the age pattern in young and old ages

    A handful of miscelaneous data drops/fixes
    """
    # Limit to just good ages, and good sexes (no unknowns)
    good_ages = get_good_ages()
    df = df.loc[df.age_group_id.isin(good_ages)]
    df = df.loc[df.sex_id.isin([1, 2])]

    # aggregate data to national level
    df = aggregate_to_national(df, loc_meta)

    df = limit_pre_1980(df, cause_meta, age_meta)

    df = limit_malaria(df, cause_meta)

    df = restrict_shock_causes(df, cause_meta)

    df = df.loc[df.ihme_loc_id != 'ZAF']
    df = df.loc[~((df.ihme_loc_id == 'CHN') & (df.year_id < 2004))]
    df = df.loc[~((df.ihme_loc_id == 'GBR') & (df.year_id == 1980))]
    df = df.loc[~((df.ihme_loc_id == 'GUM') & (df.year_id == 1991))]
    df = df.loc[~((df.ihme_loc_id == 'IRN') & (df.year_id.isin(list(range(1996, 2001)))))]
    df = df.loc[~((df.ihme_loc_id == 'IDN') & (df.cause_id == 357) &
        (df.sex_id == 2) & (df.age_group_id == 389))
    ]

    df = add_cause_metadata(df, ['yld_only'], cause_meta_df=cause_meta)
    df = df.loc[df.yld_only != 1]
    df = df.groupby(ID_COLS, as_index=False).deaths.sum()
    return df

def aggregate_cause_detail(df, cause_meta):
    """
    When generating age weights, we want to create a weight for all causes either
    level 3 and below, OR most detailed cause. Doing cause aggregation here so that
    aggregated causes will have accurate weights.
    """
    start_deaths = df.deaths.sum()
    df = add_cause_metadata(df, ['level'], cause_meta_df=cause_meta)
    max_level = int(df.level.max())
    levels = list(range(1, max_level+1))
    levels.reverse()

    # loop through each cause level and create parent aggregated levels
    for lvl in levels:
        agg_df = df.loc[df.level == lvl]
        if len(agg_df) > 0:
            agg_df = add_cause_metadata(agg_df, 'parent_id', cause_meta_df=cause_meta)
            report_if_merge_fail(agg_df, 'parent_id', 'cause_id')
            agg_df['cause_id'] = agg_df['parent_id']
            agg_df = agg_df.groupby(ID_COLS, as_index=False).deaths.sum()
            agg_df['level'] = (lvl-1)
            df = df.append(agg_df, ignore_index=True)

    # Keep level 3 and below OR most detailed. Drop secret causes, with a couple exceptions
    df = add_cause_metadata(df, ['most_detailed', 'secret_cause', 'acause'], cause_meta_df=cause_meta)
    df = df.loc[
        ((df.level <= 3) | (df.most_detailed == 1) | (df.acause == 'neonatal_sepsis')) &
        ((df.secret_cause == 0) | (df.acause == '_gc'))
    ]

    # collapse to id cols after aggregation
    df = df.groupby(ID_COLS, as_index=False).deaths.sum()

    # assert all cause (294) is identical to the original deaths
    assert np.allclose(start_deaths, df.loc[df.cause_id == 294].deaths.sum()), \
        "Deaths were added or dropped in cause aggregation."

    # make a cc_code weight, this is just identical to the _all pattern
    cc_code_df = df.loc[df.cause_id == 294]
    cc_code_df['cause_id'] = 919
    df = df.append(cc_code_df, ignore_index=True)
    return df

def limit_shock_data(df, loc_meta):
    exclusion_file = pd.read_excel("FILEPATH".format(SHOCKS_DIR))
    exclusion_file = add_location_metadata(exclusion_file, 'ihme_loc_id', location_meta_df=loc_meta)
    exclusion_file = exclusion_file[['ihme_loc_id', 'year_id', 'cause_id']]
    df = df.merge(exclusion_file, how='left', on=['ihme_loc_id', 'year_id', 'cause_id'], indicator=True)
    # drop any observation where merge succeeded
    df = df.loc[df._merge != 'both']
    df.drop('_merge', axis=1, inplace=True)
    df = df.loc[~((df.ihme_loc_id == 'IRQ') & (df.cause_id == 854) & (df.age_group_id == 19) & (df.year_id.isin([2013, 2014])))]
    df = df.loc[~((df.ihme_loc_id == 'IRQ') & (df.cause_id == 945) & (df.age_group_id.isin([20, 30, 31, 32, 235])) & (df.year_id == 2008))]
    df = df.loc[~((df.ihme_loc_id == 'EGY') & (df.cause_id == 854) & (df.age_group_id == 238) &
        (df.year_id.isin([2000, 2004, 2006, 2007, 2008, 2010, 2012, 2013])))
    ]
    df = df.loc[~((df.cause_id == 854) & (df.sex_id == 2) & (df.ihme_loc_id == 'IRQ'))]
    df = df.loc[~((df.ihme_loc_id == 'IRQ') & (df.cause_id == 854) & (df.age_group_id == 18) & (df.year_id.isin([2012, 2013, 2014])))]
    df = df.loc[~((df.ihme_loc_id == 'EGY') & (df.cause_id == 854) & (df.age_group_id == 18) & (df.year_id.isin([2005, 2012, 2015])))]
    df = df.loc[~((df.ihme_loc_id.isin(['PRT', 'HRV', 'LKA'])) & (df.cause_id == 945) & (df.age_group_id == 30) & (df.sex_id == 2))]
    df = df.loc[~((df.ihme_loc_id.isin(['PRT', 'HRV', 'LKA'])) & (df.cause_id == 945) & (df.age_group_id.isin([20, 30, 31, 32, 235])) & (df.sex_id == 1))]

    return df

def get_completeness_dfs(loc_meta):
    # get the mort comp df
    mort_comp_df = pd.read_csv("FILEPATH")
    mort_comp_df.rename(columns={'iso3': 'ihme_loc_id', 'year': 'year_id'}, inplace=True)
    report_duplicates(mort_comp_df, ['ihme_loc_id', 'year_id'])
    # get the cod comp df
    cod_comp_df = pd.read_csv(CONF.get_resource('completeness'))
    cod_comp_df = cod_comp_df.loc[cod_comp_df.denominator == 'envelope']
    cod_comp_df = add_location_metadata(
        cod_comp_df,
        ['level', 'ihme_loc_id'],
        location_meta_df=loc_meta
    )
    cod_comp_df = cod_comp_df.loc[cod_comp_df.level == 3][['ihme_loc_id', 'year_id', 'comp']]
    cod_comp_df.loc[cod_comp_df.comp > 1, 'comp'] = 1
    report_duplicates(cod_comp_df, ['ihme_loc_id', 'year_id'])
    return mort_comp_df, cod_comp_df

def get_pop_df(loc_meta):
    pop_df = get_pop(
        pop_run_id=CONF.get_id('pop_run'),
        force_rerun=False,
        block_rerun=True
    )
    # add ihme_loc_id, limit to national locations
    pop_df = add_location_metadata(pop_df,
        ['ihme_loc_id', 'level'],
        location_meta_df = loc_meta
    )
    pop_df = pop_df.loc[pop_df.level == 3]
    pop_df = pop_df[['age_group_id', 'ihme_loc_id', 'year_id', 'sex_id', 'population']]
    return pop_df

def apply_ddm_completeness(pre_1980, mort_comp_file, age_meta):
    # merge completeness on for pre 1980 years
    pre_1980 = add_age_metadata(pre_1980, 'simple_age', age_meta_df=age_meta)
    pre_1980 = pre_1980.merge(mort_comp_file, on=['ihme_loc_id', 'year_id'], how='left')
    pre_1980.loc[pre_1980.simple_age < 5,
        'deaths'] = pre_1980['deaths'] / pre_1980['u5_comp_pred']
    pre_1980.loc[pre_1980.simple_age.between(5, 14),
        'deaths'] = pre_1980['deaths'] / pre_1980['kid_comp']
    pre_1980.loc[pre_1980.simple_age >= 15,
        'deaths'] = pre_1980['deaths'] / pre_1980['trunc_pred']
    pre_1980 = pre_1980[ID_COLS + ['deaths']]
    return pre_1980

def apply_srs_scaling(srs):
    # get the envelope to scale up to
    env_df = get_env(
        env_run_id=CONF.get_id('env_run'),
        force_rerun=False,
        block_rerun=True
    )
    env_df = add_location_metadata(env_df, 'ihme_loc_id')
    env_df = env_df.query("ihme_loc_id in ['IND', 'IDN'] & sex_id == 3 & age_group_id == 22")
    comp_df = srs.groupby(['ihme_loc_id', 'year_id'], as_index=False).deaths.sum()
    comp_df = comp_df.merge(
        env_df[['ihme_loc_id', 'year_id', 'mean_env']],
        on=['ihme_loc_id', 'year_id'],
        how='left'
    )
    comp_df['comp'] = comp_df['deaths'] / comp_df['mean_env']

    # merge it on and scale up
    srs = srs.merge(
        comp_df[['ihme_loc_id', 'year_id', 'comp']],
        on=['ihme_loc_id', 'year_id'],
        how='left'
    )
    srs['deaths'] = srs['deaths'] / srs['comp']
    srs = srs[ID_COLS + ['deaths']]
    return srs

def apply_completeness_adjustment(df, mort_comp_file, cod_comp_file, age_meta):
    """
    Use the completeness files -
    The strategy will be to divide death counts by these completeness estimates to 
    ensure death counts match the envelope as closely as possible.

    This primarily has effect on countries with poor VR, and special cases like China

    For pre 1980, use the mortality completeness file, for post 1980, use CoD completeness
    determined from sourcemetadata. For SRS (India and Indonesia), we need to directly
    scale up to the envelope as we have no completeness estimates.
    """
    irq_shock = (df.ihme_loc_id == 'IRQ') & (df.year_id.isin(list(range(1980, 2006))))
    irn_shock = (df.ihme_loc_id == 'IRN') & (df.year_id.isin(list(range(1980, 1996))))
    pre_1980_data = (df.year_id < 1980)
    srs_data = (df.ihme_loc_id.isin(['IDN', 'IND']))
    pre_1980 = df.loc[pre_1980_data | irq_shock | irn_shock]
    pre_1980 = apply_ddm_completeness(pre_1980, mort_comp_file, age_meta)
    
    srs = df.loc[srs_data]
    srs = apply_srs_scaling(srs)

    post_1980 = df.loc[(df.year_id >= 1980) & (~srs_data) & (~irn_shock) & (~irq_shock)]
    post_1980 = post_1980.merge(cod_comp_file, on=['ihme_loc_id', 'year_id'], how='left')
    post_1980['deaths'] = post_1980['deaths'] / post_1980['comp']
    post_1980 = post_1980[ID_COLS + ['deaths']]

    df = pd.concat([pre_1980, post_1980, srs], ignore_index=True)
    assert df.deaths.notnull().all()
    return df

def make_weights(df, pop_df):
    # merge pop on by loc/year/age/sex
    df = df.merge(
        pop_df,
        on=['ihme_loc_id', 'year_id', 'age_group_id', 'sex_id'],
        how='left'
    )
    assert df.population.notnull().all()

    # collapse the data to all years and global
    df = df.groupby(AGE_WEIGHT_COLS, as_index=False)['deaths', 'population'].sum()

    # make weights
    df['weight'] = df['deaths'] / df['population']
    df = df[AGE_WEIGHT_COLS + ['weight']]
    return df

def square_weights(df):
    """
    Every cause/age/sex needs a weight, even if it is zero. Squaring sex
    and age here to ensure every possible observation has a weight.
    """
    # square ages
    df = df.pivot_table(index=['cause_id', 'sex_id'], columns='age_group_id', values='weight', fill_value=0)
    df = df.reset_index()
    df = df.melt(id_vars=['cause_id', 'sex_id'], var_name='age_group_id', value_name='weight')
    # square sexes
    df = df.pivot_table(index=['cause_id', 'age_group_id'], columns='sex_id', values='weight', fill_value=0)
    df = df.reset_index()
    df = df.melt(id_vars=['cause_id', 'age_group_id'], var_name='sex_id', value_name='weight')
    return df

def apply_malaria_adjustment(df):
    # get fill values
    fix_df = df.loc[
        (df.cause_id == 345) &
        (df.age_group_id.isin([3, 238]))
    ].groupby(['sex_id'], as_index=False).weight.mean()
    male_fill_val = fix_df.loc[fix_df.sex_id == 1].weight.tolist()[0]
    female_fill_val = fix_df.loc[fix_df.sex_id == 2].weight.tolist()[0]

    df.loc[
        (df.cause_id == 345) &
        (df.age_group_id.isin([388, 389])) &
        (df.sex_id == 1),
        'weight'
    ] = male_fill_val
    df.loc[
        (df.cause_id == 345) &
        (df.age_group_id.isin([388, 389])) &
        (df.sex_id == 2),
        'weight'
    ] = female_fill_val
    return df

def apply_restrictions(df, cause_meta, age_meta):
    df = add_age_metadata(df, 'simple_age', age_meta_df=age_meta)

    df = apply_malaria_adjustment(df)

    malaria_causes = get_all_related_causes('malaria', cause_meta_df=cause_meta)
    df.loc[
    	(df.cause_id.isin(malaria_causes)) &
    	((df.simple_age >= 15) | (df.simple_age == 0)),
    	'weight'
    ] = 0
    mental_drug_causes = get_all_related_causes('mental_drug', cause_meta_df=cause_meta)
    df.loc[
    	(df.cause_id.isin(mental_drug_causes)) &
    	((df.simple_age < 15) | (df.simple_age == 95)),
    	'weight'
    ] = 0
    homicide_causes = get_all_related_causes('inj_homicide', cause_meta_df=cause_meta)
    df.loc[
        (df.cause_id.isin(homicide_causes)) &
        (df.simple_age == 0),
        'weight'
    ] = 0
    measles_causes = get_all_related_causes('measles', cause_meta_df=cause_meta)
    df.loc[
        (df.cause_id.isin(measles_causes)) &
        (df.simple_age == 0.1),
        'weight'
    ] = 0
    war_causes = get_all_related_causes('inj_war_warterror', cause_meta_df=cause_meta)
    execution_causes = get_all_related_causes('inj_war_execution', cause_meta_df=cause_meta)
    war_and_exec_causes = war_causes + execution_causes
    df.loc[
        (df.cause_id.isin(war_and_exec_causes)) &
        (df.simple_age <= 0.01),
        'weight'
    ] = 0
    ms_causes = get_all_related_causes('neuro_ms', cause_meta_df=cause_meta)
    df.loc[
        (df.cause_id.isin(ms_causes)) &
        (df.simple_age == 95),
        'weight'
    ] = 0

    f_12_to_23 = df.loc[(df.cause_id == 716) & (df.sex_id == 2) & (df.simple_age == 1)].weight.iloc[0]
    df.loc[
        (df.cause_id == 716) &
        (df.sex_id == 2) &
        (df.simple_age == 0.5),
        'weight'
    ] = f_12_to_23

    m_85_to_89 = df.loc[(df.cause_id == 967) & (df.sex_id == 1) & (df.simple_age == 85)].weight.iloc[0]
    df.loc[
        (df.cause_id == 967) &
        (df.sex_id == 1) &
        (df.simple_age == 90),
        'weight'
    ] = m_85_to_89

    drop_acauses = ['hepatitis_e', 'neo_liver_hepb', 'neo_liver_hepc', 'neo_liver_alcohol',
        'neo_liver_other', 'skin_scabies', 'ntd_ebola', 'neo_leukemia_ll_acute',
        'neo_leukemia_ll_chronic', 'neo_leukemia_ml_acute', 'neo_leukemia_ml_chronic',
        'sub_total', 'ntd_zika', 'neo_leukemia_other', 'neo_liver_nash', 'rubella',
        'neo_liver_hbl', 'neo_eye_rb', 'neo_eye_other', 'neonatal_sepsis_gbs'
    ]
    drop_causes = cause_meta.loc[
        cause_meta.acause.isin(drop_acauses)
    ].cause_id.unique().tolist()
    df = df.loc[~(df.cause_id.isin(drop_causes))]

    # clean the cause hierarchy for merging
    cause_meta = cause_meta[['cause_id', 'yll_age_start', 'yll_age_end', 'male', 'female']]
    cause_meta['yll_age_start'] = cause_meta['yll_age_start'].fillna(0)
    cause_meta['yll_age_end'] = cause_meta['yll_age_end'].fillna(95)
    cause_meta[['male', 'female']] = cause_meta[['male', 'female']].fillna(1)
    assert cause_meta.notnull().values.all()
    df = df.merge(cause_meta, on='cause_id', how='left')
    assert df.notnull().values.all()

    df['is_restricted'] = 0
    df.loc[(df.sex_id == 1) & (df.male == 0), 'is_restricted'] = 1
    df.loc[(df.sex_id == 2) & (df.female == 0), 'is_restricted'] = 1
    df.loc[df.simple_age < df.yll_age_start, 'is_restricted'] = 1
    df.loc[df.simple_age > df.yll_age_end, 'is_restricted'] = 1
    df.loc[df.is_restricted == 1, 'weight'] = 0
    min_df = df.loc[df.weight > 0].groupby('cause_id', as_index=False).weight.min()
    min_df.rename(columns={'weight': 'min_weight'}, inplace=True)
    df = df.merge(min_df, on='cause_id', how='left')
    df.loc[(df.is_restricted == 0) & (df.weight == 0), 'weight'] = df['min_weight']

    assert df.notnull().values.all()
    df = df[AGE_WEIGHT_COLS + ['weight']]
    return df

def main(LOAD):
    # load some hierarchies
    loc_meta = get_current_location_hierarchy(
        location_set_id=CONF.get_id('location_set'),
        location_set_version_id=CONF.get_id('location_set_version'),
        force_rerun=False,
        block_rerun=True
    )
    cause_meta = get_current_cause_hierarchy(
        cause_set_id=CONF.get_id('cause_set'),
        cause_set_version_id=CONF.get_id('cause_set_version'),
        force_rerun=False,
        block_rerun=True
    )
    age_meta = get_ages(
    	force_rerun=True,
    	block_rerun=False
    )
    ############################################
    ############################################
    # load the data
    if LOAD:
        print("Loading the data... \n")
        df = load_data(cause_meta)
        # clean the data for weight making
        df = clean_data(df, loc_meta, cause_meta, age_meta)
        df.to_csv(CACHED_INPUT_VR, index=False)
    else:
        print("Using pre loaded, cleaned data - skipping data load... \n")
        df = pd.read_csv(CACHED_INPUT_VR)
    print("Finished Loading, now cleaning and aggregating causes... \n")
    # perform cause aggregation
    df = aggregate_cause_detail(df, cause_meta)
    # limit the shock data
    df = limit_shock_data(df, loc_meta)
    # load demographic based inputs (completeness and population)
    mort_comp_df, cod_comp_df = get_completeness_dfs(loc_meta)
    pop_df = get_pop_df(loc_meta)
    # apply adjustment for completeness
    print("Applying completeness adjustment and making weights... \n")
    df = apply_completeness_adjustment(df, mort_comp_df, cod_comp_df, age_meta)
    # make the weights
    df = make_weights(df, pop_df)
    # square the df
    df = square_weights(df)
    df = apply_restrictions(df, cause_meta, age_meta)
    df.to_csv(WEIGHTS_OUT_DIR, index=False)
    print("Done!")

if __name__ == "__main__":
    LOAD = int(sys.argv[1])
    assert LOAD in [0,1], "Please pass 1 or 0 as an input argument."
    main(LOAD)