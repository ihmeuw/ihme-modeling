
# -*- coding: utf-8 -*-
'''
Description: Runs the pipeline to estimate mortality and upload into the CoD
    database. Runs all processes necessary to convert prepped incidence to mortality
            estimates, ready for the CoD prep process
'''

import pandas as pd
from c_models.b_cod_mortality import (
    prep_incidence,
    combine_incidence,
    project_data,
    calculate_mortality,
    format_CoD_input
)



def validate_result(df, step_name):
    ''' Validates results of any given step in the pipeline
    '''
    required_cols = ['country_id', 'location_id', 'year_id', 'sex_id',
                     'age_group_id', 'acause', 'registry_index', 'NID',
                     'dataset_id']
    for c in required_cols:
        if df[c].isnull().any():
            print("   null values in {}".format(c))
    assert not df[required_cols].isnull().any().any(), \
        "Required column(s) missing values after {}".format(step_name)
    return(df)


def main():
    ''' Runs all processes necessary to convert prepped incidence to mortality
            estimates, ready for the CoD prep process
    '''
    print("Starting pipeline...")
    validate_result(prep_incidence.run(), "prep_incidence")
    validate_result(combine_incidence.run(), "combine_incidence")
    validate_result(project_data.project_incidence(), "project_data")
    validate_result(calculate_mortality.run(), "calculate_mortality")
    format_CoD_input.run()



if __name__ == "__main__":
    main()
