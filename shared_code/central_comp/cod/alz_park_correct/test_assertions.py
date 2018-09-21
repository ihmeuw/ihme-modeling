import numpy as np
import pandas as pd

def compare_dfs(df1, df2, strict=True, equal_nan=False):
    '''assert two dataframes have same values at same positions.
    Column order doesn't matter

    if strict == False, use np.allclose instead to avoid machine precision
    issues and cast all unicode as str
    
    if equal_nan == True and strict == False, treat NaNs in the same
    position as equal'''

    df1 = df1.reindex_axis(sorted(df1.columns), axis=1)
    df2 = df2.reindex_axis(sorted(df2.columns), axis=1)

    if strict:
        assert df1.equals(df2)
    else:
        string_cols = [c for c in df1 if df1[c].dtype.name == 'object']
        num_cols = [c for c in df1 if c not in string_cols]
        if string_cols:
            assert (df1[string_cols] == df2[string_cols]).all().all()
        assert np.allclose(df1[num_cols].values, df2[num_cols].values,
                           equal_nan=equal_nan)
    
    return True
        
def assertions(target_df, source_codem, scaled, source_dismod, index_cols):
    ################################################
    #Perform some comparisons that should hold true
    #if oldCorrect has run correctly
    ################################################
    pre = pd.concat([target_df, source_codem])
    pre = pre.groupby(index_cols).sum().reset_index()
    pre['cause_id'] = 999
    print(pre)
    
    post = pd.concat([scaled, source_dismod])
    post = post.groupby(index_cols).sum().reset_index()
    post['cause_id'] = 999
    print(post)
    
    return compare_dfs(pre, post, strict=False)