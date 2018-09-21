""" Module for setting up statistical models

This makes an initial table, returns potentials as penalties for 
disobeying the priors and margins.
"""

# Setup
# -----
import pandas as pd, numpy as np, pymc as pm, scipy.stats as sp

# Avoid Log of 0
# -----------------
offset = 1.e-6
def offset_log(x):

    # Replace nan with a real number.
    forced_real = np.log(x.astype('float') + offset)
    if type(forced_real) == pd.core.series.Series:
        forced_real.fillna(value=np.log(offset), inplace=True)
    else:
        forced_real[np.where(np.isnan(forced_real))] = np.log(offset)
        
    return forced_real


# Make Initial Independent Table (also currently includes potentials to obey the margins)
# --------------------------------
def contingency_table(data, last_result, draw):
    """ data parameter is expected to have rows, cols, row_sums, and col_sums"""
    # create model parameters
    p = pm.Uniform('p', 0, 1, size=(len(data.rows), len(data.cols)))

    # set initial values based on independence
    if last_result is None: p.value = np.outer(data.row_sums.ix[draw], data.col_sums)
    else: p.value = last_result


    # row and col potentials
    @pm.potential
    def row_sums(p=p, value=np.array(offset_log(data.row_sums.ix[draw]))):
        return pm.normal_like(value,
                              offset_log(p.sum(1)),
                              data.log_row_sum_sd**-2)

    @pm.potential
    def col_sums(p=p, value=np.array(offset_log(data.col_sums))):
        return pm.normal_like(value,
                              offset_log(p.sum(0)),
                              data.log_col_sum_sd**-2)

    return locals()

# Hemoglobin Shift Potentials
# ---------------------------
def hb_shift_prior(data, vars):
    # create prior on the hb shift of each subtype as a potential
    @pm.potential
    def hb_shift(p=vars['p'], hb_levels_by_severity=np.array(data.hb_levels), hb_shifts=np.array(data.hb_shifts)):
        hb_levels = np.dot(p / (p.sum(axis=1, keepdims=True)+offset), hb_levels_by_severity)
        return pm.normal_like(hb_levels, 120. - hb_shifts, .1**-2)

    # normal likelihood of the hb shifts implied by the current table
    return hb_shift


# Variation Rank Potentials
# -------------------------
def variation_rank_prior(data, vars):
    # add priors on the rank order of the variation of each cause
    prior_rank = np.array(np.argsort(data.prior_variation.ix[data.rows]))

    @pm.potential
    def variation_rank_potential(p=vars['p'], hb_shifts=np.array(data.hb_levels)):

        # calculate variation ranks of p
        row_normalized_p = p / (p.sum(axis=1, keepdims=True)+offset)
        row_var = np.dot(row_normalized_p, hb_shifts**2) \
            - np.dot(row_normalized_p, hb_shifts)**2

        penalty = 0.

        for i in range(len(data.rows)-1):
            diff = row_var[prior_rank[i+1]] - row_var[prior_rank[i]]
            if diff > 0:
                penalty += diff
        return -1.e2*penalty

    return variation_rank_potential
