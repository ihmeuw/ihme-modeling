import pandas as pd


def make_ratio(numerator: pd.DataFrame, denominator: pd.DataFrame, duration: pd.DataFrame = 0) -> pd.DataFrame:
    if isinstance(duration, int):
        return numerator / denominator.groupby('location_id').shift(duration)
    else:
        out = []
        for draw in numerator.columns:      
            draw_duration = duration[draw].max()
            out.append(numerator[draw] / denominator[draw].groupby('location_id').shift(draw_duration))
        out = pd.concat(out, axis=1)
        return out
