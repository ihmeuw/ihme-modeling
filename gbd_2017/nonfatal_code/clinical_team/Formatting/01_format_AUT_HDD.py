# -*- coding: utf-8 -*-
"""

Format the raw data for Austria. This replaces the script at:

"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import time
import glob
import os
import getpass

user = getpass.getuser()
# load our functions
prep_path = r"FILEPATH/Functions".format(user)
sys.path.append(prep_path)

import hosp_prep

# Environment:
if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    warnings.warn()
    exit
    

# switch to write dta files to HDF in /raw or read in the existing HDFs
write_hdf_helper = False
if write_hdf_helper:
    start = time.time()
    #####################################################
    # READ DATA AND KEEP RELEVANT COLUMNS
    # ASSIGN FEATURE NAMES TO OUR STRUCTURE
    #####################################################
    # AUT data isn't stored in a uniform manner so hardcode paths
    drivelesspaths = [r"FILEPATH/AUT_HOSPITAL_"
                      r"INPATIENT_DISCHARGES_1989_2000_Y2011M11D28.DTA",
                      "FILEPATH/AUT_HOSPITAL_"
                      r"INPATIENT_DISCHARGES_2011_Y2016M02D18.DTA",
                      "FILEPATH/AUT_HOSPITAL_"
                      r"INPATIENT_DISCHARGES_2012_Y2013M01D14.DTA",
                      "FILEPATH/AUT_HOSPITAL_"
                      r"INPATIENT_DISCHARGES_2013_Y2015M04D19.DTA",
                      "FILEPATH/AUT_HOSPITAL_"
                      r"INPATIENT_DISCHARGES_2014_Y2016M02D12.DTA",
                      "FILEPATH/AUT_HOSPITAL_"
                      r"INPATIENT_DISCHARGES_2001_2010_Y2011M11D28.DTA"]
    
    # read data into a list then concat it together
    df_list = []
    for f in drivelesspaths:
        print("Reading in file {}".format(f))
        adf = pd.read_stata(root + f)
    
        to_drop = adf.filter(regex="^(MEL)").columns
        adf.drop(to_drop, axis=1, inplace=True)
    
        # rename all the 2ndary dx cols
        if "AD1" in adf.columns:
            for i in np.arange(1, 44, 1):
                dxnum = str(i + 1)
                adf.rename(columns={"AD" + str(i): "dx_" + dxnum}, inplace=True)
        if "NEBEN1" in adf.columns:
            for i in np.arange(1, 13, 1):
                dxnum = str(i + 1)
                adf.rename(columns={"NEBEN" + str(i): "dx_" + dxnum}, inplace=True)
        adf.to_hdf("/FILEPATH/{}.H5".format(os.path.basename(f)[0:39]), key='df',
                   format='table', complib='blosc', complevel=6, mode='w')
        print((time.time()-start)/60)
else:
    start = time.time()
    globby = "FILEPATH/*.H5"

    # this is for the production run
    df_list = [pd.read_hdf(f, 'df') for f in glob.glob(globby)]
    print((time.time()-start)/60)

final_list = []  # append all the processed dfs to this list
for df in df_list:
    
    assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
        "the index has a length of " + str(len(df.index.unique())) +
        " while the DataFrame has " + str(df.shape[0]) + " rows" +
        "try this: df = df.reset_index(drop=True)")
    
    cols = df.columns
    drops = ['bundeslpat', 'verlurs', 'NEBEN_ANZAHL', 'anzlst', 'anzvlst', 'aufnzl']
    to_drop = [c for c in cols if c in drops]
    df.drop(to_drop, axis=1, inplace=True)
            
    hosp_wide_feat = {
        'nid': 'nid',
        'location_id': 'location_id',
        'representative_id': 'representative_id',
        # 'year': 'year',
        'berj': 'year_start',
        'year_end': 'year_end',
        'geschlnum': 'sex_id',
        'alterentle': 'age',
        'age_group_unit': 'age_group_unit',
        'code_system_id': 'code_system_id',
    
        # measure_id variables
        'entlartnum': 'outcome_id',
        'facility_id': 'facility_id',
        # diagnosis varibles
        'viersteller': 'dx_1'}
    
    # Rename features using dictionary created above
    df.rename(columns=hosp_wide_feat, inplace=True)
        
    new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                           set(df.columns)))
    df = df.join(new_col_df)
        
    df['representative_id'] = 1
    df['location_id'] = 75
    
    # group_unit 1 signifies age data is in years
    df['age_group_unit'] = 1
    df['source'] = 'AUT_HDD'
    
    # code 1 for ICD-9, code 2 for ICD-10
    df['code_system_id'] = 2 
    df.loc[df.year_start <= 2000, 'code_system_id'] = 1
    
    df['facility_id'] = 'inpatient unknown'
    df['year_end'] = df['year_start']

    # convert outcome ID to str
    df['outcome_id'] = df['outcome_id'].astype(str)
    # convert sex_id to str
    df['sex_id'] = df['sex_id'].astype(str)

    missing_out = df[df.outcome_id == 'nan'].shape[0]
    print("There are {} null values in the outcome ID column. This should be"
          r" looked into. Setting to discharge for now".
          format(missing_out))
    if missing_out > 0:
        df.loc[df.outcome_id == 'nan', 'outcome_id'] = 'discharge'
        
    # case is the sum of live discharges and deaths
    pre_outcomes = df.outcome_id.notnull().sum()
    df['outcome_id'] = df['outcome_id'].replace(['discharged alive',
                                                'Discharged (alive)', 'dead',
                                                'Dead'],
                                                ['discharge', 'discharge', 'death',
                                                'death'])
    assert pre_outcomes == df.outcome_id.notnull().sum(),\
        "Some null outcomes were introduced"
    assert set(df.outcome_id) == set(['discharge', 'death']),\
        "There are outcomes that aren't death or discharge"

    missing_sex = df[df.sex_id == 'nan'].shape[0]
    print("There are {} null values in the sex_id column. Setting to 3".
          format(missing_sex))
    if missing_sex > 0:
        df.loc[df.sex_id == 'nan', 'sex_id'] = 3

    pre_sexes = df.sex_id.notnull().sum()
    df['sex_id'] = df['sex_id'].replace(['Male', 'male', 'Female', 'female'],
                                        [1, 1, 2, 2])
    assert pre_sexes == df.sex_id.notnull().sum()
    for s in df.sex_id.unique():
        assert s in [1, 2, 3], "data includes an unusable sex id"

    print("There are {} null values and {} blank values in the dx_1 column."
          r" Setting to 'cc_code'".
          format(df.dx_1.isnull().sum(), df[df.dx_1 == ""].shape[0]))
    df.loc[df.dx_1.isnull(), 'dx_1'] = 'cc_code'
    df.loc[df.dx_1 == "", 'dx_1'] = 'cc_code'

    # metric_id == 1 signifies that the 'val' column consists of counts
    df['metric_id'] = 1

    # Create a dictionary with year-nid as key-value pairs
    nid_dictionary = {1989: 121917, 1990: 121841, 1991: 121842, 1992: 121843,
                      1993: 121844, 1994: 121845, 1995: 121846, 1996: 121847,
                      1997: 121848, 1998: 121849, 1999: 121850, 2000: 121851,
                      2001: 121831, 2002: 121854, 2003: 121855, 2004: 121856,
                      2005: 121857, 2006: 121858, 2007: 121859, 2008: 121860,
                      2009: 121862, 2010: 121863, 2011: 121832, 2012: 128781,
                      2013: 205019, 2014: 239353}
    df = hosp_prep.fill_nid(df, nid_dictionary)

    #####################################################
    # CLEAN VARIABLES
    #####################################################

    # Columns contain only 1 optimized data type
    int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
                'sex_id', 'nid', 'representative_id',
                'metric_id']
    
    str_cols = ['source', 'facility_id', 'outcome_id']

    if df[str_cols].isnull().any().any():
        warnings.warn("\n\n There are NaNs in the column(s) {}".
                      format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                      "\n These NaNs will be converted to the string 'nan' \n")

    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
    for col in str_cols:
        df[col] = df[col].astype(str)

    df.loc[df['age'] > 95, 'age'] = 95  

    # drop null ages
    pre = df.shape[0]
    df = df[df.age.notnull()]
    assert df.shape[0] - pre <= 220, "We expected 220 null ages but more than this were lost"
    pre = df.shape[0]
    df = hosp_prep.age_binning(df)
    if df.shape[0] < pre:
        print(df)
        assert False, "Age binning has caused rows to be dropped"
    df.drop('age', axis=1, inplace=True)  # changed age_binning to not drop age col

    # AUT marks infants as 1 year olds
    df.loc[df['age_start'] == 1, 'age_start'] = 0

    # mark unknown sex_id
    df.loc[(df['sex_id'] != 1) & (df['sex_id'] != 2), 'sex_id'] = 3

    #####################################################
    # SWAP E AND N CODES
    # AUT does not appear to contain external cause of injury codes
    #####################################################

    diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

    for feat in diagnosis_feats:
        df[feat] = hosp_prep.sanitize_diagnoses(df[feat])
    
    # store the data wide for the EN matrix
    # data is being processed by year groups so take the min year_start value and use
    # that to name the file
    min_year = df.year_start.unique().min()
    df_wide = df.copy()
    df_wide['metric_discharges'] = 1
    df_wide = df_wide.groupby(df_wide.columns.drop('metric_discharges').tolist()).agg({'metric_discharges': 'sum'}).reset_index()
    df_wide.to_stata(r"FILEPATH/AUT_HDD_{}.dta".format(min_year),
                write_index=False)
    # save the data wide
    wide_path = "FILEPATH/AUT_HDD_{}.H5".format(min_year)
    hosp_prep.write_hosp_file(df_wide, wide_path, backup=True)
    del df_wide
    #####################################################
    # IF MULTIPLE DX EXIST:
        # TRANSFORM FROM WIDE TO LONG
    #####################################################

    if len(diagnosis_feats) > 1:
        
        print("Reshaping long")

        # check primary dx
        pre_dx1_counts = df.dx_1.value_counts()
        
        pre_otherdx_counts = df[diagnosis_feats.drop('dx_1')]\
            .apply(pd.Series.value_counts).sum(axis=1).reset_index().\
            sort_values('index').reset_index(drop=True)

        # create index of everything that's not a dx column
        indx = df.columns.drop(df.filter(regex="^(dx)").columns)

        df = pd.melt(df, id_vars=list(indx), value_vars=list(diagnosis_feats))
        # drop nulls and blanks
        df = df[df['value'].notnull()]

        post_dx1_counts = df[df.variable == 'dx_1'].value.value_counts()

        post_otherdx_counts = df[df.variable != 'dx_1'].value.value_counts().\
            reset_index().sort_values('index').reset_index(drop=True)
    
        assert (pre_dx1_counts == post_dx1_counts).all(),\
            "Reshaping long has altered the primary diagnoses"
        # compare non primary icd codes
        assert (pre_otherdx_counts['index'] ==
                post_otherdx_counts['index']).all(),\
            "Reshaping long has altered the nonprimary diagnoses"
        # compare non primary value counts
        assert (pre_otherdx_counts[0] == post_otherdx_counts['value']).all(),\
            "Reshaping long has altered the nonprimary diagnoses"
        # rename to cause code
        df.rename(columns={'variable': 'diagnosis_id', 'value': 'cause_code'},
                  inplace=True)

        pre_dx1 = df[df.diagnosis_id == 'dx_1'].shape[0]
        pre_other = df.shape[0] - pre_dx1  
        df.diagnosis_id = np.where(df.diagnosis_id == 'dx_1', 1, 2)
        assert pre_dx1 == df[df.diagnosis_id == 1].shape[0],\
            "Counts of primary diagnosis levels do not match"
        assert pre_other == df[df.diagnosis_id == 2].shape[0],\
            "Counts of nonprimary diagnosis levels do not match"

        print("Done reshaping long in {}".format((time.time()-start)/60))

    elif len(diagnosis_feats) == 1:
        df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
        df['diagnosis_id'] = 1

    else:
        print("Something went wrong, there are no ICD code features")

    # drop the rows which were blank in the original data, this shouldn't affect
    # primary dx
    pre = df[df.diagnosis_id == 1].shape[0]
    df = df[df.cause_code != ""]
    assert pre == df[df.diagnosis_id == 1].shape[0]
    
    df['val'] = 1

    #####################################################
    # GROUPBY AND AGGREGATE
    #####################################################

    # Check for missing values
    print("Are there missing values in any row?\n")
    null_condition = df.isnull().values.any()
    if null_condition:
        warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
    else:
        print(">> No.")

    # Group by all features we want to keep and sums 'val'
    group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
                  'age_end', 'year_start', 'year_end', 'location_id', 'nid',
                  'age_group_unit', 'source', 'facility_id', 'code_system_id',
                  'outcome_id', 'representative_id', 'metric_id']
    df = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()

    #####################################################
    # ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
    #####################################################

    # Arrange columns in our standardized feature order
    columns_before = df.columns
    hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                       'year_start', 'year_end',
                       'location_id',
                       'representative_id',
                       'sex_id',
                       'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                       'source', 'nid',
                       'facility_id',
                       'code_system_id', 'cause_code']
    df = df[hosp_frmat_feat]
    columns_after = df.columns

    # check if all columns are there
    assert set(columns_before) == set(columns_after),\
        "You lost or added a column when reordering"
    for i in range(len(hosp_frmat_feat)):
        assert hosp_frmat_feat[i] in df.columns,\
            "%s is missing from the columns of the DataFrame"\
            % (hosp_frmat_feat[i])

    # check data types
    for i in df.columns.drop(['cause_code', 'source', 'facility_id', 'outcome_id']):
        assert df[i].dtype != object, "%s should not be of type object" % (i)

    # verify column data
    onetwo = set([1, 2])
    assert df.nid.unique().size == df.year_start.unique().size,\
        "There should be a unique nid for each year"
    assert set(df.diagnosis_id.unique()).issubset(onetwo),\
        "There are undefined diagnosis id levels"
    assert set(df.sex_id.unique()).issubset(set([1, 2, 3])),\
        "There are undefined sex ids present"
    assert set(df.code_system_id.unique()).issubset(onetwo),\
        "There are undefined code system ids present"

    # check number of unique feature levels
    assert len(df['year_start'].unique()) == len(df['nid'].unique()),\
        "number of feature levels of years and nid should match number"
    assert len(df['age_start'].unique()) == len(df['age_end'].unique()),\
        "number of feature levels age start should match number of feature " +\
        r"levels age end"
    assert len(df['diagnosis_id'].unique()) <= 2,\
        "diagnosis_id should have 2 or less feature levels"
    assert len(df['sex_id'].unique()) <= 3,\
        "There should at most three feature levels to sex_id"
    assert len(df['code_system_id'].unique()) <= 2,\
        "code_system_id should have 2 or less feature levels"
    if len(df['source'].unique()) != 1:
        print("source should only have one feature level {} {}".
              format(df.source.unique(), df.source.value_counts(dropna=False)))
        assert False  # break the script

    assert (df.val >= 0).all(), ("for some reason there are negative case counts")
    final_list.append(df)

final_df = pd.concat(final_list, ignore_index=True)

final_df = final_df[final_df.cause_code != ""]
#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH/formatted_AUT_HDD.H5"
hosp_prep.write_hosp_file(final_df, write_path, backup=True)
