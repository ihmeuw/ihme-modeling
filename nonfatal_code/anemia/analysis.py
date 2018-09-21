""" Analysis

This script initializes and executes the model for anemia CT sampling

If no arguments are specified, it dUSERts to lines 25, 40, 42, 44 and 46
"""


# Setup
# -----
import sys
import timeit
import pymc as pm
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages
import models
import data
import graphics
import scipy.optimize
from hierarchies import dbtrees

reload(data)
reload(models)
reload(graphics)
start = timeit.dUSERt_timer()
np.random.seed({RANDOM SEED})

np.seterr(invalid='warn')

def run_analysis(location_id, age_group_id, ndraws, max_iters,
                 sex_id_list,year_id_list,
                 consecutive_small_changes,small_change,
                 scale_factor,
                 data_dir,
                 h5_dir,
                 outdir):


    # Define Function to Optimize
    # ---------------------------
    def subspace_obj(vals, items, axis, update_p):
        """ use vals to fill in mc.p.value[r1, 0:-1],
        and then complete values so that row sum is unchanged,
        then change r2 to that col sums are unchanged,
        with some added error
        items is the indices of the rows or columns to operated on
        axis is whether to operate on rows (0) or columns (1)

        return -mc.logp, to be used in scipy.optimize.minimize"""


        vals = vals.copy() / scale_factor

        p = mc.p.value.copy()
        p_initial = p.copy()

        err = vals[-1]  # error in the margins
        moves = vals[0:-1]

        # there must be an even number of rows or columns to work on
        assert len(items) % 2 == 0

        # alter rows
        if axis == 0:
            # iterate through whatever list of rows was given, two at a time
            it = iter(items)
            n = 0
            for r1, r2 in zip(it, it):
                curr_move = moves[n:n+2]
                # alter rows 1 and 2 according to curr_move
                p[r1, 0] += curr_move.sum()+err
                p[r1, 1:] -= curr_move

                p[r2, 0] -= curr_move.sum()
                p[r2, 1:] += curr_move-(err/len(curr_move))
                n += 2

        # alter columns
        if axis == 1:
            # there will always be only two columns to work with at a time
            r1 = items[0]
            r2 = items[1]
            p[0, r1] += moves.sum()+err
            p[1:, r1] -= moves

            p[0, r2] -= moves.sum()
            p[1:, r2] += moves-(err/len(moves))

        # return new posterior
        mc.p.value = p
        try:
            obj = -mc.logp
        except pm.ZeroProbability:
            obj = np.inf

        # reset mc.p.value unless told to update it
        if not update_p:
            mc.p.value = p_initial
        return obj

    ################################
    # Rescaling functions
    ################################
    def rescale_prior_hb_shift(df):
        df['prior_subtype_prop'] = (
                df['prior_hb_shift'] / df['prior_hb_shift'].sum())
        return df

    def apply_sev_levels(df, d, draw):
        subtype = df.subtype.values[0]

        # Don't allow zero allocation for non-zero subtype priors
        df.loc[df[['mild', 'moderate', 'severe']].sum(axis=1) == 0,
               ['mild', 'moderate', 'severe']] = (1, 1, 1)
        scalar = df[['mild', 'moderate', 'severe']].sum(axis=1)

        try:
            input_prev = d.prevalence.loc['draw_'+str(draw), subtype]
            df['input_prevalence'] = input_prev
        except Exception, e:
            input_prev = 1
            df['input_prevalence'] = 'resid'
            print(e)

        for level in ['mild', 'moderate', 'severe']:
            df[level] = (df[level] / scalar *
                         df['prior_subtype_prop'] * d.total_anemia)

        df['prior_subtype_prop'] = df['prior_subtype_prop'] * d.total_anemia

        # Make sure the output prevalences do not exceed the input prevalences.
        # Take preferentially from the mild categories.
        allocated_prev = df[['mild', 'moderate', 'severe']].sum(
                axis=1).values[0]
        if (allocated_prev > input_prev) & (subtype != 'malaria'):
            to_redistribute = allocated_prev - input_prev
            if to_redistribute < df[['mild']].values[0]:
                df['mild'] = df['mild'] - to_redistribute
                df['mild_redist'] = to_redistribute
            else:
                df['mild_redist'] = df['mild']
                df['mild'] = 0
                to_redistribute = (
                        to_redistribute - df[['mild_redist']].values[0])
                if to_redistribute < df[['moderate']].values[0]:
                    df['moderate'] = df['moderate'] - to_redistribute
                    df['moderate_redist'] = to_redistribute
                else:
                    df['moderate_redist'] = df['moderate']
                    df['moderate'] = 0
                    to_redistribute = (to_redistribute -
                                       df[['moderate_redist']].values[0])
                    df['severe'] = df['severe'] - to_redistribute
                    df['severe_redist'] = to_redistribute
        elif subtype == 'malaria':
            df['input_prevalence'] = 'ignore'

        for col in ['mild_redist', 'moderate_redist', 'severe_redist']:
            if col not in df.columns:
                df[col] = 0

        return df

    def squeeze_bottom_margin(df, d):

        resid_subtypes = df.loc[(df.input_prevalence == 'resid') &
                                (df.prior_subtype_prop != 0), 'subtype']
        all_subtypes = df.loc[(df.prior_subtype_prop != 0), 'subtype']

        for level in ['mild', 'moderate', 'severe']:

            # Allocate direct-cause residuals proportionally across resid
            # subtypes
            if (df.loc[df.subtype.isin(resid_subtypes), level].sum() == 0):
                subtype_normalizer = 1
            else:
                subtype_normalizer = df.loc[df.subtype.isin(resid_subtypes),
                                            level].sum()
            subtype_proportions = df.loc[
                    df.subtype.isin(resid_subtypes),
                    level] / subtype_normalizer
            resid_to_add = df[level+'_redist'].sum() * subtype_proportions
            df.loc[df.subtype.isin(resid_subtypes), level] = df.loc[
                    df.subtype.isin(resid_subtypes), level] + resid_to_add

            # Squeeze/expand residual categories so that bottom margin is
            # satisfied
            target_resid_sum = (
                    d.col_sums['prev_'+level]*d.total_anemia -
                    df.loc[df.input_prevalence != 'resid', level].sum())
            resid_sum = df.loc[df.subtype.isin(resid_subtypes), level].sum()

            print '%s target_resid_sum: %s' % (level, target_resid_sum)
            if (target_resid_sum > 0) and (resid_sum > 0):
                subtype_normalizer = df.loc[df.subtype.isin(resid_subtypes),
                                            level].sum()
                subtype_proportions = df.loc[df.subtype.isin(resid_subtypes),
                                             level] / subtype_normalizer
                df.loc[df.subtype.isin(resid_subtypes),
                       level] = subtype_proportions * target_resid_sum
            else:
                target_resid_sum = d.col_sums['prev_'+level]*d.total_anemia
                subtype_normalizer = df.loc[df.subtype.isin(all_subtypes),
                                            level].sum()
                subtype_proportions = df.loc[df.subtype.isin(all_subtypes),
                                             level] / subtype_normalizer
                df.loc[df.subtype.isin(all_subtypes),
                       level] = subtype_proportions * target_resid_sum
        return df

    all_years = []
    iters_per_year = []

    for sex_id in sex_id_list:
        for year_id in year_id_list:
            # Load Data and Priors
            # --------------------
            d = data.Data(location_id, year_id, sex_id, age_group_id, 0, data_dir, h5_dir)
            d.main()

            # initialize model
            last_result = None
            m = models.contingency_table(d, last_result, draw=0)
            m['variation_rank_prior'] = models.variation_rank_prior(d, m)
            m['hb_shift_prior'] = models.hb_shift_prior(d, m)
            mc = pm.MCMC(m)

            # Loop Over Draws and Optimize Two Rows at a Time, Two Cols at a
            # Time
            # -------------------------------------------------------------------

            # stopping rules
            # stop after X consecutive iterations with a small change in
            # posterior

            # if it can't converge after Y iterations, stop anyway ...
            max_iterations = max_iters

            # loop over draws
            d.ndraws = ndraws
            summary_tables = []
            for draw in range(d.ndraws):
                print 'Draw:', draw, 'Initial Values:\n', np.round(
                        m['p'].value, 3)

                # set convergance identifiers and counter
                # iterate over optimization function until convergence is
                # acheived
                converged = 0
                previous_logp = mc.logp
                i = 0
                while (converged < consecutive_small_changes and
                       i < max_iterations):

                    # work on rows
                    # nrows must be an even number and less than or equal to
                    # the total number of rows
                    nrows = 4
                    rows = np.random.choice(
                            range(len(d.rows)), size=nrows, replace=False)
                    print 'rows:', rows
                    res = scipy.optimize.minimize(
                            subspace_obj,
                            np.zeros(nrows),
                            (rows, 0, False),
                            method='Powell',
                            options={'disp': False})
                    subspace_obj(res.x, rows, axis=0, update_p=True)

                    # work on columns. it would be good to get this to ignore
                    # rows with zero also...
                    for cols in [[0, 1], [0, 2], [1, 2]]:
                        res = scipy.optimize.minimize(
                                subspace_obj,
                                np.zeros(len(d.rows)),
                                (cols, 1, False),
                                method='Powell',
                                options={'disp': False})
                        subspace_obj(res.x, cols, axis=1, update_p=True)

                    # assess convergence
                    if (np.absolute(1-(previous_logp/mc.logp)) < small_change):
                        converged = converged+1
                    else:
                        converged = 0
                    previous_logp = mc.logp
                    sys.stdout.flush()
                    print 'Draw:', draw, 'iter', i, 'logp', mc.logp
                    i = i+1
                iters_per_year.append(i)
                logp = previous_logp
                # update initial values of the search to the previous solution
                # to speed up every successive search
                last_result = m['p'].value
                print 'Draw:', draw, 'Solution:\n', np.round(m['p'].value, 3)

                # Summarize and Store Results
                # ---------------------------
                # one sample to make a trace (just because graphics.py expects
                # mcmc output)
                mc.sample(1)
                summary_table = graphics.summary(d, mc, draw)
                summary_table['draw'] = draw

                print 'Summary: '
                print summary_table

                #Make a summary table of percentiles:
                pctle = graphics.summary_pctle(d, mc)
                print 'Percentile: '
                print pctle

                # load file and drop repeated variable names/rows and columns
                # that the database upload doesn't need
                print "Cleaning Up..."
                st = summary_table
                for obs in ['prior_total', 'observed_total', 'subtype']:
                    st = st[st['subtype'] != obs]
                st = st[[
                    'location_id', 'year_id', 'sex_id', 'age_group_id', 'draw',
                    'subtype', 'mild', 'moderate', 'severe', 'prior_hb_shift',
                    'observed_hb_shift']]

                # Aggregate to reporting level
                st.rename(
                        columns={'subtype': 'attribution_group'}, inplace=True)
                st = st.merge(
                        d.subin[['attribution_group', 'report_group']],
                        on='attribution_group')
                st.drop('attribution_group', axis=1, inplace=True)
                st['prior_hb_shift'] = st['prior_hb_shift'].astype('float')
                st['observed_hb_shift'] = st['observed_hb_shift'].astype(
                        'float')
                st = st.groupby([
                    "location_id", "year_id", "sex_id", "age_group_id", "draw",
                    "report_group"]).sum().reset_index()
                st.rename(columns={'report_group': 'subtype'}, inplace=True)

                # Rescale to margins
                st = st.groupby([
                    'location_id', 'year_id', 'sex_id', 'age_group_id',
                    'draw']).apply(rescale_prior_hb_shift).reset_index()
                st = st.groupby([
                    'location_id', 'year_id', 'sex_id', 'age_group_id', 'draw',
                    'subtype']).apply(apply_sev_levels, d, draw).reset_index()
                st.drop(['index', 'level_0'], axis=1, inplace=True)
                st = st.groupby([
                    'location_id', 'year_id', 'sex_id', 'age_group_id',
                    'draw']).apply(squeeze_bottom_margin, d).reset_index()
                st.drop([
                    'index', 'input_prevalence', 'mild_redist',
                    'moderate_redist', 'severe_redist'],
                    axis=1, inplace=True)
                summary_tables.append(st)

                # update model for next iteration
                if draw < d.ndraws-1:

                    d.draw_num = draw+1
                    d.compute_shifts()

                    # Uncomment to unchain draws...
                    # i.e. solve each independently of the solution to the
                    # last draw
                    last_result = None

                    m = models.contingency_table(d, last_result, draw+1)
                    m['variation_rank_prior'] = models.variation_rank_prior(
                            d, m)
                    m['hb_shift_prior'] = models.hb_shift_prior(d, m)
                    mc = pm.MCMC(m)

            # Clean Up and Save
            # -----------------
            ysumm = pd.concat(summary_tables)
            all_years.append(ysumm)

    # Collect all years into a single data frame
    all_years = pd.concat(all_years)
    print 'Saving...'
    all_years['subtype'] = all_years.subtype.astype(str)
    all_years.to_hdf('%s/%s_%s.h5' % (outdir, location_id, age_group_id),
                     'solution', mode='w', format='table')


    # print run time
    stop = timeit.dUSERt_timer()
    print 'run time:', np.round(stop - start, 2), "seconds"

    return all_years, logp, stop-start

if __name__ == "__main__":

    # Arguments
    # ---------
    try:
        location_id = int(sys.argv[1])
    except:
        location_id = [{LOCATION ID}]
    try:
        age_group_id = int(sys.argv[2])
    except:
        age_group_id = [{AGE GROUP ID}]
    try:
        ndraws = int(sys.argv[3])
    except:
        ndraws = 5
    try:
        max_iters = int(sys.argv[4])
    except:
        max_iters = 3
    try:
        sex_id_list = int(sys.argv[5])
    except:
        sex_id_list = [{SEX IDS}]
    try:
        year_id_list = int(sys.argv[6])
    except:
        year_id_list = year_id_list = [{YEAR IDS}]
    try:
        consecutive_small_changes = int(sys.argv[7])
    except:
        consecutive_small_changes = 6
    try:
        small_change = float(sys.argv[8])
    except:
        small_change = 1e-6
    try:
        scale_factor = int(sys.argv[9])
    except:
        scale_factor = 1000.
    try:
       data_dir = str(sys.argv[10])
    except:
        data_dir = '/{FILEPATH}/priors/'
    try:
        h5_dir = str(sys.argv[11])
    except:
        h5_dir = '{FILEPATH}'
    try:
        outdir = str(sys.argv[12])
    except:
        outdir = '{FILEPATH}'

    print 'Arguments: %s %s' % (location_id, age_group_id)

    res, logp, runtime = run_analysis(
            location_id, age_group_id, ndraws, max_iters,
            sex_id_list, year_id_list,
            consecutive_small_changes, small_change,
            scale_factor,
            data_dir,
            h5_dir,
            outdir)
