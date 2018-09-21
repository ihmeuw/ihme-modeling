def set_sort_index(df, index_cols):
    df.sort_values(by=index_cols, inplace=True)
    df.set_index(index_cols, inplace=True)
    
    return df