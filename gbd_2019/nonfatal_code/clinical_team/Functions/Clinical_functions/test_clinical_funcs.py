"""
Test the clinical functions
"""





    
    
import re


def readable_errors(df):
    if df.shape[0] < 1:
        msg = """The data you've requested doesn't seem to exist.
        Check that the demographic information (age, sex location, year) and cause are present"""
        assert False, msg
    return


def test_merge_name(df, pre, on_col, new_cols):
    
    if not isinstance(new_cols, list):
        new_cols = [new_cols]
    for col in new_cols:
        
        if df[col].isnull().sum() > 0:
            no_names = df.loc[df[col].isnull(), on_col].unique()
            msg = "There are some missing names. {}s which don't have names "\
                  "are {}".format(on_col, no_names)
            warnings.warn(msg)

    
    assert pre[0] == df.shape[0],\
        "Row counts have changed from {} to {}, we don't expect this".format(pre[0], df.shape[0])
    assert pre[1] + len(new_cols) == df.shape[1],\
        "Column counts have changed from {} to {}, we expect the addition "\
        "of exactly 1 column, 'bundle_name'".format(pre[1], df.shape[1])
    return


def test_condition(df, col, cond_tuple, hardfail=False):
    """
    Check to make sure that any values passed in the condition show up
    in the resulting data. ie if we give it location_id (545, 546) we expect
    to get those back with location_name
    """
    diffs = set(df[col]).symmetric_difference(set(cond_tuple))
    if diffs:
        msg = "The full names were not obtained for these values {}".format(diffs)
        if hardfail:
            assert False, msg
        else:
            warnings.warn(msg)
    return

def test_agg_query(query):
    test = re.split(pattern=",| |\.|=", string=query)
    assert len(test) > 1, "Something went wrong with this test, the length of the split string is too short"
    assert not ('bundle_id' in test and 'icg_id' in test),\
        "You must select either 1 or more bundle_ids OR 1 or more icg_ids. You cannot select both types"

    return

def test_kwargs(data_type, **kwargs):
    if data_type == 'intermediate' or data_type == 'icg':
        if 'bundle_id' in kwargs.keys():
            assert False, "Cannot return bundle and icg data simultaneously"
    if data_type == 'bundle':
        if 'icg_id' in kwargs.keys():
            assert False, "Cannot return icg and bundle data simultaneously"
    return
