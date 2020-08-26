import getpass
import sys
import warnings
import pandas as pd
from db_queries import get_population


user = getpass.getuser()
prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)
repo = r"FILEPATH".format(user)
sys.path.append(repo + "FILENAME")
import hosp_prep
import gbd_hosp_prep
import clinical_mapping


def test_weight_sources(df):
    
    no_min = []
    no_max = []
    for source in df.source.unique():
        age_min = df[df.source == source].age_start.min()
        age_max = df[df.source == source].age_start.max()
        if age_min != 0:
            no_min.append(source)
        if age_max != 95:
            no_max.append(source)

    bad_sources = no_min + no_max
    if bad_sources:
        assert False, "source(s) {} don't have min age 0 and source(s)"\
            " {} don't have max age start 95".format(no_min, no_max)
    return


def prep_weights(df, level, gbd_round_id, decomp_step, squaring_method, inp_pipeline=True):
    """
    Function that computes weights for use in age sex splitting.

    Parameters:
        df: pandas DataFrame
            input data to inform weights. Should already have the appropriate
            age groups.  Should have column "product" which is the product
            of cause_fraction and the hospital utilization envelope.
            age_group_id should be present.  df is used only as an input to
            make weights.
        level: string
            Must be "bundle_id" or "icg_id". indicates if we're making
            bundle level weights or cause level weights. Note bundle ID weights will fail
        squaring_method: str
            Must be 'broad' or 'bundle_source_specific'. Broad means bundles that are never
            coded for a data source will be added as zeros, while the other does not
    Returns:
        DataFrame that has weights computed at level of the Parameter 'level'
    """

    
    assert level in df.columns, "{} is not present in the data".format(level)
    assert 'product' in df.columns, "Product column must be present"
    assert 'source' in df.columns, "Source column must be present"
    
    if level not in ("bundle_id", "icg_id"):
        raise ValueError("level must either be 'bundle_id' or 'icg_id'")

    if squaring_method not in ('broad', 'bundle_source_specific'):
        raise ValueError("squaring method must be either 'broad' or 'bundle_source_specific'")

    
    
    

    print("Getting {} weights...".format(level))

    
    df = df[df.sex_id != 3].copy()

    
    df = gbd_hosp_prep.all_group_id_start_end_switcher(df, remove_cols=False)

    test_weight_sources(df[['source', 'age_start', 'age_end']].drop_duplicates())

    if inp_pipeline:
        
        
        
        keep_cols = ['age_group_id', 'age_start', 'age_end', 'location_id',
                     'sex_id', 'year_end', 'year_start', 'product', 'icg_id']
        if squaring_method == 'bundle_source_specific':
            new_square_cols = ['source', 'nid', 'facility_id', 'representative_id']
            keep_cols = keep_cols + new_square_cols

        df = df[keep_cols].copy()

    
    
    
    
    
    if squaring_method == 'broad':
        template = hosp_prep.make_square(df)
        df = template.merge(df, how='left',
                            on=['age_group_id', 'sex_id', 'location_id',
                                'year_start', 'year_end', 'age_start', 'age_end',
                            level])
        
        df.update(df['product'].fillna(0))

    elif squaring_method == 'bundle_source_specific':
        df = hosp_prep.make_zeros(df, cols_to_square='product', etiology=level)
        
        
        if inp_pipeline:
            df.drop(new_square_cols, axis=1, inplace=True)

    else:
        assert False, f"{squaring_method} isn't a recognized squaring method"
    print("Finished making the data square")

    
    

    
    
    age_list = list(df.age_group_id.unique())
    loc_list = list(df.location_id.unique())
    year_list = list(df.year_start.unique())

    
    pop = get_population(age_group_id=age_list, location_id=loc_list,
                         sex_id=[1, 2], year_id=year_list,
                         gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    
    pop.drop("run_id", axis=1, inplace=True)  
    pop['year_start'] = pop['year_id']
    pop['year_end'] = pop['year_id']
    pop.drop('year_id', axis=1, inplace=True)

    
    pre_shape = df.shape[0]
    df = df.merge(pop, how='left',
                  on=['location_id', 'year_start', 'year_end',
                      'age_group_id', 'sex_id'])
    assert pre_shape == df.shape[0], "Merging on pop changed the 
    print("Population has been merged onto the data")

    
    
    df['counts'] = df['product'] * df['population']

    
    
    
    group_cols = ['age_end', 'age_start', 'age_group_id', 'sex_id', level]
    df = df.groupby(by=group_cols).agg({'counts': 'sum',
                                        'population': 'sum'}).reset_index()
    print("Groupby is complete, calculating first pass of weights")

    
    
    df['weight'] = df['counts'] / df['population']

    return(df)


def compute_weights(df, round_id, run_id, gbd_round_id, decomp_step, squaring_method,
                    level='icg_id', fill_gaps=True, overwrite_weights=False,
                    inp_pipeline=True):
    """
    Args:
        df: pandas DataFrame that contains the data that you want to use to
            inform weights. Should already have the appropriate
            age groups.  Should have column "product" which is the product
            of cause_fraction and the hospital utilization envelope.
            age_group_id should be present. df is used only as an input to
            the other function, in order to make weights.
        round_id: int
            indicates what round of weights we're on, which affects file names
            for output. Should either be 1 or 2
        gbd_round_id: int
            indicates which gbd round to use when pulling in central population
            estimates
        decomp_step: str
            indicates which decomp step to use within the gbd round above
        fill_gaps: Boolean
            switch. If true, age sex restrictions will be used to find gaps in
            icg_id age sex pattern.  if a gap is found, that entire
            age sex pattern will be replaced with the baby sequelae's level 1
            bundle's age sex pattern.  This is all if a baby sequelae has a
            bundle id
        overwrite_weights: Boolean
            weights are written as a csv within a run_id for use in the pipeline
            if True, write weights regardless of whether they already exist or not
        inp_pipeline: Boolean
            Modifying this function for use outside of the inp_pipeline in July 2019
            but would like to retain behavior within the pipeline
    Returns:
        string "Done"
    """

    assert round_id == 1 or round_id == 2,\
        "round_id isn't 1 or 2, it's {}".format(round_id)

    
    df = prep_weights(df, level=level,
                      squaring_method=squaring_method,
                      gbd_round_id=gbd_round_id,
                      decomp_step=decomp_step,
                      inp_pipeline=inp_pipeline)

    
    print("done getting {} weights".format(level))

    
    
    clinical_mapping.apply_restrictions(df, age_set='binned', cause_type=level[:-3], prod=False)

    if inp_pipeline:  
        
        
        
        if fill_gaps:
            assert False, "There's a bug in the bundle level weights that needs to be fixed first"
            print("filling gaps")
            df_list = []
            print("Replacing gaps in bundle weights with cause weights")
            counter = 0.0  
            is_null = 0
            for bundle in df.bundle_id.unique():
                counter += 1
                for sex in df.sex_id.unique():
                    df_s = df[(df.sex_id == sex)&(df.bundle_id == bundle)].copy()
                    if df_s['weight'].isnull().any():
                        
                        is_null += 1
                        df_s['weight'] = df_s['bundle_weight']
                    df_list.append(df_s)
            print(r"{}% of the age patterns were replaced with cause weights".format(is_null / counter))
            df = pd.concat(df_list, sort=False).reset_index(drop=True)

    
    
    groups = df.groupby(['age_group_id', 'sex_id'])

    bad = []  
    
    for group in groups:
        
        if (group[1].weight == 0).all():
            
            bad.append(group[0])  

    warning_message = """
    There are age-sex groups in the weights where every weight is zero

    Here's some tuples (age_group_id, sex_id) that identify these groups:
    {g}
    """.format(g=", ".join(str(g) for g in bad))

    if len(bad) > 0:
        warnings.warn(warning_message)

    
    if inp_pipeline:

        if overwrite_weights:
            df.drop(['age_start', 'age_end', 'counts', 'population'],
                    axis=1).to_csv(r"FILENAME"
                                   r"FILEPATH"
                                   r"FILEPATH".format(run=run_id, lev=level),
                                   index=False)

        
        if round_id == 1:
            round_name = "one"
        if round_id == 2:
            round_name = "two"
        
        df.drop(['age_start', 'age_end', 'counts', 'population'],
                axis=1).to_csv(r"FILENAME"
                               r"FILEPATH"
                               r"FILEPATH".\
                               format(run=run_id, round=round_name, lev=level), index=False)

        
        df['sex_id'].replace([1, 2], ["Male", "Female"], inplace=True)
        df.to_csv(r"FILEPATH"
                  r"FILENAME"
                  r"FILEPATH".\
                  format(run=run_id, round=round_name, lev=level), index=False)
    
    else:
        return df

    return("done")
