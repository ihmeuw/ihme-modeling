def wide(df, stub='', value_var='value', variable='variable'):
    """Convenience function for reshaping a wide pandas dataset to long
    Based on: http://stackoverflow.com/questions/17333644/ \
              pandas-dataframe-transforming-frame-using- \
              unique-values-of-a-column
    """
    dfwide = df.copy()
    dfwide[variable] = dfwide[variable].apply(lambda x: stub+str(x))
    cols = [c for c in df.columns if c not in {value_var, variable}]
    dfwide = dfwide.set_index(cols + [variable]).unstack(variable)
    dfwide.columns = dfwide.columns.levels[1]
    dfwide.columns.name = None
    dfwide = dfwide.reset_index()
    return dfwide


