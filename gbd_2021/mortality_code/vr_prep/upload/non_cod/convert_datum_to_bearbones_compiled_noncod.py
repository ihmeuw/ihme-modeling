"""
This script converts combined and priorizited noncod empirical deaths data
from the datum (wide) age format into the age_group_id (long) aka bearbones
format.

It imports the functions that performs the conversion from another script
This script handles the importing and saving of the data.
"""

import sys
import getpass
import pandas as pd

sys.path.append("".format(getpass.getuser()))
from convert_datum_to_bearbones import convert_datum_to_bearbones

# Globals / passed in arguments
NEW_RUN_ID = sys.argv[1]
OUTPUT_FOLDER = "".format(NEW_RUN_ID)


def _test_output(df):
    """
    Basic checks on dataframe
    """
    if df.empty:
        raise ValueError("No data in dataframe.")

    if df.duplicated().any():
        raise ValueError("Duplicates are present in the data.")

    if df['nid'].isnull().any():
        raise KeyError("Found null NIDS.")


def standardize_format(df):
    """
    Function that drops the outlier column. Any other changes to the format
    of the data should be added to this function in the future.

    There are some columns that need to be different than how
    convert_datum_to_bearbones specified them. editing
    convert_datum_to_bearbones is undesirable because it needs to be used
    in more than one place
    """

    # assert everything is not an outlier
    assert not (df.outlier == 1).any(), "Shouldn't have outliers here."
    df = df.drop("outlier", axis=1)

    return df


def main(df, output_folder):

    df = convert_datum_to_bearbones(df)

    _test_output(df)

    df = standardize_format(df)

    # change age group id 161 to 28
    df.loc[df.age_group_id == 161, 'age_group_id'] = 28

    print("Saving data...")
    data_save_file = output_folder + ""
    df.to_csv(data_save_file, index=False)

    print("Finished.\nData written to \n{}.".format(data_save_file))


if __name__ == '__main__':
    print("Loading data...")
    non_cod_datum = pd.read_stata(""
                                  .format(OUTPUT_FOLDER))

    main(df=non_cod_datum, output_folder=OUTPUT_FOLDER)
