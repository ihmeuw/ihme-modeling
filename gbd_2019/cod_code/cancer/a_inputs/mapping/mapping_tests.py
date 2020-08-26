'''
Description:
'''

import pandas as pd
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    pipeline_tests as pt
)
from cancer_estimation.py_utils import test_utilities as tests


def validate_mapping(in_df, out_df, metric):
    ''' Tests the mapping output to verify results.  
    '''
    uid_cols = md.get_uid_cols(3)
    test_results = {'missing entries': [], 'new entries': []}
    in_df = md.stdz_col_formats(in_df)
    out_df = md.stdz_col_formats(out_df)
    test_df = pd.merge(in_df[uid_cols], out_df[uid_cols],
                       on=uid_cols, how='outer', indicator=True)
    if test_df['_merge'].isin(["left_only"]).any():
        test_results['missing entries'] = test_df.loc[test_df._merge == "left_only",
                                                      uid_cols].to_dict()
    if test_df['_merge'].isin(["right_only"]).any():
        test_results['new entries'] = test_df.loc[test_df._merge ==
                                                  "right_only", uid_cols].to_dict()
    if len(out_df) != len(in_df):
        test_results['missing or extra uids'] = "fail"
    pt.verify_metric_total(in_df, out_df, metric, "mapping test")
    tests.checkTestResults(test_results, 'validate mapping', displayOnly=False)