import pandas as pd

def unpack_list(df, col, dtype=None):
    '''
    unpack the elements in list to df columns. 

    '''

    # The index must be aligned in order for the 
    # temp_col assignement to work
    df.reset_index(drop=True, inplace=True)

    temp = pd.DataFrame(df[col].tolist())
    
    if dtype:
        temp_cols = [dtype(e) for e in list(temp.columns)]
    else:
        temp_cols = [e for e in list(temp.columns)]
    
    df[temp_cols] = temp
    
    return df, temp_cols