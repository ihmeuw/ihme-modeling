class SpanMap(object):

    def __init__(self, map_df, id_var, span_start, span_end):
        self.map_df = map_df
        self.id_var = id_var
        self.span_start = span_start
        self.span_end = span_end


class SpanTarget(object):

    def __init__(self, target_df, span_start, span_end):
        self.target_df = target_df
        self.span_start = span_start
        self.span_end = span_end


def span_2_id(span_map, span_target):
    # make copies
    df = span_target.target_df.copy()
    map_df = span_map.map_df.copy()
    map_df = map_df[
        [span_map.id_var, span_map.span_start, span_map.span_end]]

    # cross join
    df["cross"] = 1
    map_df["cross"] = 1
    xed = map_df.merge(df, on="cross")
    xed = xed.drop(["cross"], axis=1)

    # add spans
    xed = xed.loc[
        (
            (xed[span_map.span_start] > xed[span_target.span_start]) &
            (xed[span_map.span_end] < xed[span_target.span_end])
        ) |
        (
            (xed[span_map.span_start] < xed[span_target.span_end]) &
            (xed[span_map.span_end] > xed[span_target.span_start])
        ) |
        (
            (xed[span_target.span_start] == xed[span_target.span_end]) &
            (xed[span_map.span_start] == xed[span_target.span_end])
        )]
    return xed


def df_mean(df, mean_col_name, input_cols):
    df[mean_col_name] = df[input_cols].mean(axis=1)
    return df


def df_round(df, round_col_name, input_col, base=1):

    def rounder(x, base=base):
        return int(base * round(float(x) / base))

    df[round_col_name] = df[input_col].apply(rounder, args=(base,))
    return df
