"""Format {SOURCE} for the CoD prep code.

Inputs:
    Data should come with sex, year, location, and cause of death
    (or just deaths if prepping for mortality)
Outputs:
    A processed data frame with the standard columns

Notes:
    Anything usual about the data can be described here.
"""

# universal imports, these may change as needed
import sys
import os
import pandas as pd

# import repo specific modules
this_dir = os.path.dirname(os.path.abspath(__file__))
repo_dir = os.path.abspath(os.path.join(this_dir, '../../..'))
sys.path.append(repo_dir)
from cod_prep.claude.formatting import finalize_formatting
from cod_prep.downloaders.ages import get_cod_ages

# set the incoming data directory
IN_DIR = "FILEPATH"

# these columns will not change
# identifier columns
ID_COLS = [
    'nid', 'location_id', 'year_id', 'age_group_id', 'sex_id', 'data_type_id',
    'representative_id', 'code_system_id', 'cause', 'site'
]
# value column
VALUE_COL = ['deaths']
# output columns needed for the final data frame
FINAL_FORMATTED_COLS = ID_COLS + VALUE_COL

# integer columns
INT_COLS = ['nid', 'location_id', 'year_id', 'age_group_id', 'sex_id',
            'data_type_id', 'representative_id', 'code_system_id'] + VALUE_COL

# set the source, this is how we will recognize the data
# this is a variable NOT a column in the dataframe
# source will differ by code system (e.g. Iran_VR_ICD9, Iran_VR_ICD10)
SOURCE = "strSourceName"


def format_source():
    """Format source."""
    # read in data
    # e.g. df = pd.read_csv("FILEPATH")
    # for multiple files, best to append and process as one file (if possible)

    # rename columns or reshape as needed
    # data should be long on year, age, sex, cause of death

    # year variable is 'year_id', should be an integer
    # year of recorded death (NOT year the indiviudal died)
    # e.g. date of death registration or when a survey/census was conducted

    # location_id, look up in shared.location table
    # check if this country is modeled at the country or sub national level

    # if age groups are detailed, ages over 1 should be split into 5 year bins
    # if not, then keep ages as is (aggregates will be split later)
    # next, encode w/ age_group_id in shared.age_group table
    # get_cod_ages() will provide a list of the ideal age groups, but those
    # are not always present in the data

    # encode sex: male = 1, female = 2, unknown = 9
    assert df['sex_id'].isin([1, 2, 9]).values.all()

    # create nid column, this is provided by data indexer

    # create data_type_id based on how deaths were reported
    # descriptions in cod.data_type table (e.g. vital registration (VR) = 9)

    # create site column
    df['site'] = ""

    # create representative_id column
    # 1 = data are representative of the location_id, 0 = not representative
    # will usually be 1
    df['representative_id'] = 1

    # create code_system_id column, names found in DATABASE
    # this will vary by year
    # if prepping for all cause mortality:
    # df['code_system_id'] = 666

    # create cause column
    # if prepping for all cause mortality:
    # df['cause'] = "cc_code"

    # create deaths column if data are by individual record
    # df['deaths'] = 1

    # these lines will never change
    # subset to final columns, check for missing values, and collapse
    df = df[FINAL_FORMATTED_COLS]
    # convert all integer data types
    for col in INT_COLS:
        df[col] = df[col].astype(int)
    assert df.notnull().values.any()
    df = df.groupby(ID_COLS, as_index=False)[VALUE_COL].sum()

    # run finalize formatting for this source
    # this will output a formatted file and update tables in the cod database
    # finalize_formatting(df, SOURCE, write=True)

    # if prepping for all cause mortality then the add extract_type:
    # finalize_formatting(df, SOURCE, write=True,
    #                     extract_type='all cause mortality')


if __name__ == '__main__':
    # if you have multiple sources in one formatting script,
    # then set source here and loop by source
    format_source()
