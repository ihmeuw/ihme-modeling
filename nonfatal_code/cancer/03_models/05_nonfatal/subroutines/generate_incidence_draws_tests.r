#!/usr/local/bin/R
#########################################
## Description: test functions used by generate_incidence_draws
## Input(s)/Output(s): see individual functions
## How To Use: intended to be sourced by the generate_incidence_draws script
#########################################
source(get_path("r_test_utilities"))

calc_inc.test_incidence_draws <- function(df, uid_columns){
    results = list('missing columns'=c(), 'duplicates check'=c())
    results['duplicates check'] = test_utils.duplicateChecker(df, uid_columns)
    results['missing columns'] = test_utils.findMissingColumns(df, uid_columns)
    results['columns missing values'] = test_utils.findMissingValues(df, uid_columns)
    test_utils.checkTestResults(results, "generate_incidence")
}
