from typing import List

from covid_historical_model.utils.misc import get_random_state

CVI_LIMITS = [0.3, 0.7]


def get_cvi_dist(n_samples: int,
                 cvi_limits: List[float] = CVI_LIMITS) -> List[float]:
    cvi = [sample_cvi(n, *cvi_limits) for n in range(n_samples)]
    ## SENSITIVITY ANALYSIS -- 100% cross-variant immunity
    # cvi = [1 for n in range(n_samples)]
    
    return cvi


def sample_cvi(draw: int, lower: float, upper: float) -> float:
    random_state = get_random_state(f'chi_{draw}')
    
    return random_state.uniform(lower, upper)
