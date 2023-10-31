from typing import Dict, List

import numpy as np

from covid_historical_model.utils.misc import get_random_state


RISK_RATIOS = [
    # https://www.bmj.com/content/372/bmj.n579
    {'mean': 1.64, 'lower': 1.32, 'upper': 2.04},
    # # LSHTM
    # {'mean': 1.35, 'lower': 1.08, 'upper': 1.65},
    # # Imperial
    # {'mean': 1.29, 'lower': 1.07, 'upper': 1.54},
    # # Exeter
    # {'mean': 1.91, 'lower': 1.35, 'upper': 2.71},
    # # PHE
    # {'mean': 1.65, 'lower': 1.21, 'upper': 2.25},
]


def get_variant_severity_rr_dist(n_samples: int,
                                 risk_ratios: List[Dict[str, float]] = RISK_RATIOS,) -> List[float]:
    mus = [np.log(risk_ratio['mean']) for risk_ratio in risk_ratios]
    sigmas = [(np.log(risk_ratio['upper']) - np.log(risk_ratio['lower'])) / 3.92 for risk_ratio in risk_ratios]
    
    n_studies = len(risk_ratios)
    study_samples = [int(n_samples / n_studies) for _ in range(n_studies)]
    study_samples[0] += n_samples - (n_studies * study_samples[0])
    study_samples = [0] + np.cumsum(study_samples).tolist()
    
    sample_ranges = [range(study_samples[s_i - 1], study_samples[s_i]) for s_i in range(1, n_studies + 1)]
    
    rr = []
    for mu, sigma, sample_range in zip(mus, sigmas, sample_ranges):
        rr += [np.exp(sample_rr(n, mu, sigma)) for n in sample_range]
        ## SENSITIVITY ANALYSIS - No increased risk of severe disease from variants
        # rr += [1 for n in sample_range]
    
    return rr


def sample_rr(draw: int, mu: float, sigma: float,) -> float:
    random_state = get_random_state(f'rr_{draw}')
    
    return random_state.normal(mu, sigma)
