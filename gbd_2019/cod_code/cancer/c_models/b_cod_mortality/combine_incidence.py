
'''
Description: Subsets incidence data to those that can have population values and
    combines those data to create one estimate by location_id

How To Use:
Contributors:
'''

import pandas as pd
from cancer_estimation.py_utils import (
    common_utils as utils,
    modeled_locations
)
from cancer_estimation.a_inputs.a_mi_registry import (
    populations as pop,
    mi_dataset
)
from cancer_estimation.py_utils.gbd_cancer_tools import add_year_id
from cancer_estimation.b_staging import staging_functions


def get_uid_columns():
    return(['location_id', 'year_id', 'sex_id', 'age_group_id', 'acause'])


def combine_incidence(df):
    ''' Combines data by uid, preserving national_registry and full_coverage
            status for later reference
    '''
    print("   combining registries")
    uid_cols = get_uid_columns()
    combine_uids = uid_cols + ['country_id',
                               'national_registry', 'full_coverage']
    dont_combine = (df['sdi_quintile'].eq(5) & df['full_coverage'].eq(1) &
                    (df['national_registry'].eq(1)
                     | df['is_subnational'].eq(1))
                    )
    df = df[combine_uids + ['sdi_quintile', 'is_subnational', 'registry_index',
                            'dataset_id', 'NID', 'cases', 'pop']]
    combined_data = staging_functions.combine_uid_entries(df[~dont_combine],
                                                          uid_cols=combine_uids,
                                                          metric_cols=['cases', 'pop'])
    # Preferentially keep full_coverage data for the same uid
    existing = df.loc[dont_combine, :]
    output = existing.append(combined_data)
    output.sort_values(uid_cols + ['full_coverage'],
                       ascending=False).reset_index()
    output = output.drop_duplicates(subset=uid_cols, keep="first")
    # Re-set sdi quintile to account for merges
    output = modeled_locations.add_sdi_quintile(output, delete_existing=True)
    assert not output[output.duplicated(uid_cols)].any().any(), \
        "combine_incidence produced redundant entries"
    print("incidence estimates combined")
    return(output)


def run():
    ''' Runs pipeline to combine previously-selected incidence data
        Requires incidence data that are unique by location_id-year-sex-age-acause
    '''
    output_file = utils.get_path("combined_incidence", process="cod_mortality")
    input_file = utils.get_path("prepped_incidence", process="cod_mortality")
    utils.ensure_dir(output_file)
    df = pd.read_csv(input_file)
    print("Combining data to one single entry per uid...")
    df = combine_incidence(df)
    df.to_csv(output_file, index=False)
    return(df)


if __name__ == "__main__":
    run()
