import pandas as pd
import numpy as np
from matplotlib.pyplot import *
import math

def plot_fit(data, m):
    p = m.p.trace()
    S, I, J = p.shape

    ax = None
    figure(figsize=(12,6))
    for i, r in enumerate(data.rows):
        ax = subplot(math.ceil(len(data.rows)/3.0), 3, i+1, sharex=None, sharey=None)
        p_r = 0
        for j, c in enumerate(data.cols):
            hist(p[:,i,j], histtype='step', linewidth=2, normed=False, label=c)


        xmin, xmax, ymin, ymax = axis()
        vlines([data.row_sums[r]], 0, ymax, label='row sum')

        ylabel(r, rotation=0, ha='right')
        yticks([])
        t,tl = xticks()
        xticks(t[1::2])
        axis(xmin=0, ymin=ymin, ymax=ymax)

    subplots_adjust(left=.1, hspace=.35, wspace=.25)
    legend(loc=(1.1, .1))


def plot_conv(m):
    p = m.p.trace()
    S, I, J = p.shape

    clf()
    for i in range(I):
        for j in range(J):
            subplot(I, J, 1+i+j*I)

            plot(p[:, i, j])
            x, t = xticks()
            xticks(x, ['' for ti in t])
            y, t = yticks()
            yticks(y, ['' for ti in t])
    subplots_adjust(hspace=0)


def summary(data, m, draw):
    p = m.p.trace()
    S, I, J = p.shape

    results = []
    for i, r in enumerate(data.rows):
        result = [r]
        for j, c in enumerate(data.cols):
            result += [np.round(np.mean(p[:, i, j]), 3)]
        result += [np.round(np.mean(np.sum([p[:, i, j]
                   for j in range(J)], axis=0)), 3)]
        result += [np.round(data.row_sums.ix[draw][r], 3)]

        # append prior argmax column
        result += [data.prior_largest[r]]

        # append observed argmax column (max argmax across samples)
        arg_max = pd.Series(np.argmax(p[:, i, :], axis=1))
        result += [arg_max.value_counts().index[0]]

        # append prior on variation order
        result += [data.prior_variation[r]]

        # calculate observed variation ranks
        pp = p / (p.sum(axis=2, keepdims=True))
        t = ((pp[:, i, 0] + pp[:, i, 1]*4 + pp[:, i, 2]*9) -
             (pp[:, i, 0] + pp[:, i, 1]*2 + pp[:, i, 2]*3)**2)
        result += [np.round(np.mean(t), 3)]

        # calculate prior and observed hb level
        result += [data.hb_shift * data.row_sums.mean(axis=0)[i]]
        t = np.dot(pp[:, i, :], data.hb_levels)
        result += [data.hb_shift *
                   np.round(np.mean(np.sum([p[:, i, j] for j in range(J)],
                            axis=0)), 3)]

        results += [result]
    results = pd.DataFrame(
            results,
            columns=[
                'subtype', 'mild', 'moderate', 'severe', 'observed_total',
                'prior_total', 'prior_largest', 'observed_largest',
                'prior_variance_rank', 'observed_variance_rank',
                'prior_hb_shift', 'observed_hb_shift'])
    results['observed_variance_rank'] = (
            len(data.rows) -
            np.argsort(np.argsort(results.observed_variance_rank)))

    col_sums = results.sum()
    col_sums['subtype'] = 'observed_total'
    results = results.append(col_sums, ignore_index=True)

    col_sums = data.col_sums.round(3)
    col_sums = col_sums.append(pd.Series({'subtype': 'prior_total'}))
    results = results.append(col_sums, ignore_index=True)

    # add unique identifiers so files can easily be concatenated together
    ids = pd.DataFrame(np.repeat(data.location_id, results.shape[0]))
    ids.columns = ['location_id']
    ids['year_id'] = data.year_id
    ids['sex_id'] = data.sex_id
    ids['age_group_id'] = data.age_group_id
    results = pd.merge(
            results, ids, left_index=True, how='left', right_index=True)
    return results


def summary_pctle(data, m):
    p = m.p.trace()
    S, I, J = p.shape

    results = []
    for i, r in enumerate(data.rows):
        p_r = 0
        result =  [r]
        for j, c in enumerate(data.cols):
            result += [np.round(np.percentile(p[:,i,j],5), 3)]
            result += [np.round(np.percentile(p[:,i,j],95), 3)]

        results += [result]
    results = pd.DataFrame(results, columns=['subtype', 'mild_5th', 'moderate_5th', 'severe_5th', 'mild_95th', 'moderate_95th', 'severe_95th'])
    return results
