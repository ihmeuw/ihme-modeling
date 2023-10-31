import numpy as np
import pandas as pd

def logit(p):
    return np.log(p / (1 - p))


def expit(x):
    return 1 / (1 + np.exp(-x))


def scale_to_bounds(data: pd.Series, floor: float, ceiling: float) -> pd.Series:
    # determine boundaries, rescale data
    floor = max(floor, data.min())
    ceiling = min(ceiling, data.max())
    ceiling = max(floor, ceiling)
    
    data = ((data - data.min()) / data.values.ptp()) * (ceiling - floor) + floor
    
    return data
