import logging
import os
import time
import traceback
from functools import partial
from multiprocessing import Pool

import gbd.constants as gbd
import pandas as pd
from core_maths.summarize import get_summary
from cluster_utils.pandas_utils import get_index_columns

from dalynator import get_input_args
from dalynator.age_aggr import AgeAggregator
from dalynator.aggregate_causes import AggregateCauses
from dalynator.apply_pafs import risk_attr_burden_to_paf
from dalynator.apply_pafs_to_df import ApplyPafsToDf
from dalynator.compute_dalys import ComputeDalys
from dalynator.compute_summaries import MetricConverter
from dalynator.data_container import DataContainer
from dalynator.data_container import remove_unwanted_stars
from dalynator.data_filter import PAFInputFilter
from dalynator.data_sink import HDFDataSink
from dalynator.get_rei_type_id import get_rei_type_id_df
from dalynator.makedirs_safely import makedirs_safely
from dalynator.sex_aggr import SexAggregator
from dalynator.write_csv import write_csv

# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE write DF"

# CONSTANTS for switching rei vs. eti files
RISK_REI_TYPE = 1
ETI_REI_TYPE = 2

logger = logging.getLogger(
    "dalynator.tasks.run_pipeline_burdenator_most_detailed")


class MPGlobals(object):
    """Container for 'global' variables used in multiprocessing"""


def aggregate_ages(df, draw_cols, index_cols, gbd_compare_ags=True,
                   age_groups={22: (0, 200), 27: (0, 200)}):
    """Takes a DataFrame of 'most detailed' age groups, aggregates them
    to GBD compare groups. Function is immutable, and only returns the
    aggregate age groups.
    """
    my_age_aggr = AgeAggregator(df, draw_cols, index_cols,
                                data_container=MPGlobals.data_container,
                                age_groups=age_groups,
                                gbd_compare_ags=gbd_compare_ags,
                                include_pre_df=False)
    df = my_age_aggr.get_data_frame()
    MPGlobals.logger.debug("age_aggregation complete, df shape "
                           "{}".format(df.shape))
    return df


def aggregate_sexes(df, draw_cols, index_cols):
    """Takes a DataFrame of sex-specific draws, aggregates them
    to both sexes combined. Function is immutable, and only returns the
    both sex draws.
    """
    my_sex_aggr = SexAggregator(df, draw_cols, index_cols,
                                data_container=MPGlobals.data_container,
                                include_pre_df=False)
    df = my_sex_aggr.get_data_frame()
    MPGlobals.logger.debug("sex_aggregation complete, df shape "
                           "{}".format(df.shape))
    return df


def get_dimensions(df, index_cols=None):
    """Creates a DataFrame that is the unique combination of index columns"""
    if not index_cols:
        draw_cols = list(df.filter(like='draw').columns)
        index_cols = list(set(df.columns) - set(draw_cols))
    df = df[index_cols].drop_duplicates()
    df = df.sort_values(index_cols).reset_index(drop=True)
    return df


def aggregate_dimensions(df, index_cols=None):
    # Keep just the index columns
    if not index_cols:
        draw_cols = list(df.filter(like='draw').columns)
        index_cols = list(set(df.columns) - set(draw_cols))
    df = df.loc[df['metric_id'] == 1]
    df = get_dimensions(df, index_cols)

    # Create single draw column to aggregate (will be dropped at the end)
    df['draw_0'] = 1.0
    draw_cols = ['draw_0']

    # Aggregate
    df_sagg = aggregate_sexes(df, draw_cols, index_cols)
    df = pd.concat([df, df_sagg], sort=True)
    df_aagg = aggregate_ages(df, draw_cols, index_cols)
    df = pd.concat([df, df_aagg])

    df = df.loc[df['draw_0'] != 0]

    # Return data
    df = get_dimensions(df, index_cols)
    return df


def match_with_dimensions(df, dimensions_df):
    merge_cols = ['measure_id', 'location_id', 'year_id', 'sex_id',
                  'age_group_id', 'cause_id', 'rei_id', 'star_id']
    if 'star_id' not in df.columns:
        merge_cols.remove('star_id')
    dimensions_df = get_dimensions(dimensions_df, merge_cols)
    MPGlobals.logger.info(
        "match_with_dimensions df {}".format(get_index_columns(df)))
    MPGlobals.logger.info("match_with_dimensions dimensions_df {}".format(
        get_index_columns(dimensions_df)))
    df = pd.merge(df, dimensions_df, on=merge_cols)
    return df


def aggregate_summaries(meas_df):
    """Take the base-case DataFrame containing sex-specific, most-detailed-age
    draws in number space and compute aggregates + paf-back calculations,
    uploadable results
    """
    draw_cols = list(meas_df.filter(like='draw').columns)
    index_cols = list(set(meas_df.columns) - set(draw_cols))
    MPGlobals.logger.info("AS index columns {}".format(repr(index_cols)))

    szdr = partial(summarize_draws, index_cols=index_cols)
    agga = partial(aggregate_ages, index_cols=index_cols,
                   draw_cols=draw_cols)
    aggs = partial(aggregate_sexes, index_cols=index_cols,
                   draw_cols=draw_cols)
    bcp = partial(back_calc_pafs, n_draws=MPGlobals.data_container.n_draws)

    # Number space summaries
    base_summs = szdr(meas_df)
    bs_summs = szdr(aggs(meas_df))
    aa_ss_summs = szdr(agga(meas_df))
    aa_bs_summs = szdr(agga(aggs(meas_df)))

    # Back-calculated paf summaries
    MPGlobals.logger.info("Begin bcp(meas_df)")
    bcp_summs = szdr(bcp(meas_df))
    MPGlobals.logger.info("Begin bcp(aggs(meas_df))")
    bcp_bs_summs = szdr(bcp(aggs(meas_df)))
    MPGlobals.logger.info("Begin bcp(agga(meas_df))")  # THIS
    bcp_aa_ss_summs = szdr(bcp(agga(meas_df)))  # this includes age std
    MPGlobals.logger.info("Begin bcp(agga(aggs(meas_df)))")
    bcp_aa_bs_summs = szdr(bcp(agga(aggs(meas_df))))  # this includes age std

    # Rate space summaries
    meas_df = convert_to_rates(meas_df)
    rate_summs = szdr(meas_df)
    rbs_summs = szdr(aggs(meas_df))
    raa_ss_summs = szdr(agga(meas_df))
    raa_bs_summs = szdr(agga(aggs(meas_df)))

    # Age standardized summaries have already been calculated
    # in the NUMBER space summarization phase
    raa_ss_summs = raa_ss_summs[raa_ss_summs.age_group_id !=
                                gbd.age.AGE_STANDARDIZED]
    raa_bs_summs = raa_bs_summs[raa_bs_summs.age_group_id !=
                                gbd.age.AGE_STANDARDIZED]

    summs = pd.concat([base_summs, bs_summs, aa_ss_summs, aa_bs_summs,
                       bcp_summs, bcp_bs_summs, bcp_aa_ss_summs,
                       bcp_aa_bs_summs, rate_summs, rbs_summs, raa_ss_summs,
                       raa_bs_summs])

    return summs


def back_calc_pafs(df, n_draws):
    """
    Back calculate PAFs for each cause-risk pair
    We should only back-calc in number space, or age-standardized.
    """
    MPGlobals.logger.info("start back-calculating PAFs, n_draws = {}, time = "
                          "{}".format(n_draws, time.time()))
    index_cols = get_index_columns(df)
    MPGlobals.logger.info("BCP columns = {}".format(index_cols))
    default_draw_cols = ['draw_{}'.format(dn) for dn in range(n_draws)]
    pafs_df = risk_attr_burden_to_paf(
        df[df.metric_id == gbd.metrics.NUMBER],
        MPGlobals.data_container['cause_risk_metadata'],
        default_draw_cols)
    age_std_pafs_df = risk_attr_burden_to_paf(
        df[df.age_group_id == gbd.age.AGE_STANDARDIZED],
        MPGlobals.data_container['cause_risk_metadata'],
        default_draw_cols)
    pafs_df = pd.concat([pafs_df, age_std_pafs_df])
    pafs_df = pafs_df.loc[pafs_df['rei_id'] != 0]
    MPGlobals.logger.info("back-calculating PAFs complete, time = "
                          "{}".format(time.time()))
    return pafs_df


def burdenate(key):
    """Apply pafs, write draws, and summarize burden for the given 'key.' The
    key here is a human-friendly label for the measures of interest: yll,
    yld, and death.
    """
    meas_df = MPGlobals.data_container[key]
    n_draws = MPGlobals.data_container.n_draws
    agg_causes = MPGlobals.data_container.agg_causes
    MPGlobals.logger.info("burdenate n_draws {}".format(n_draws))

    # Apply PAFs to key (yll, yld, or death)
    paf_index_columns = ['location_id', 'year_id', 'sex_id',
                         'age_group_id', 'cause_id', 'rei_id',
                         'measure_id', 'star_id']
    my_apply_pafs = ApplyPafsToDf(
        MPGlobals.pafs_filter.get_data_frame(), meas_df,
        MPGlobals.args.n_draws)
    meas_paf_df = my_apply_pafs.get_data_frame()
    draw_cols = list(meas_paf_df.filter(like='draw').columns)
    index_cols = list(set(meas_paf_df.columns) - set(draw_cols))
    if agg_causes:
        for cause_hierarchy in MPGlobals.data_container['cause_hierarchy']:
            ac = AggregateCauses(
                cause_hierarchy,
                meas_paf_df,
                index_columns=index_cols)
            meas_paf_df = ac.get_data_frame()
    # Add cause envelope to df_list
    meas_df['rei_id'] = gbd.risk.TOTAL_ATTRIBUTABLE
    meas_df['star_id'] = gbd.star.ANY_EVIDENCE_LEVEL
    meas_df = pd.concat([meas_df, meas_paf_df])
    del meas_paf_df

    # Concatenate cause envelope with data
    meas_df = meas_df.loc[(
        (meas_df['sex_id'].isin([gbd.sex.MALE, gbd.sex.FEMALE])) &
        (meas_df['age_group_id'].isin(MPGlobals.most_detailed_age_groups)) &
        (meas_df['metric_id'] == gbd.metrics.NUMBER))]

    MPGlobals.logger.info("Writing draws {}".format(key))
    meas_df = pd.concat([meas_df, back_calc_pafs(meas_df, n_draws)])
    write_draws(meas_df, MPGlobals.args.out_dir, key,
                MPGlobals.args.location_id, MPGlobals.args.year_id,
                MPGlobals.args.write_out_star_ids)
    MPGlobals.logger.info("Done writing draws {}".format(key))
    # By restricting to just NUMBER this function only returns Attributable
    # Burden, not back-calculated PAFs
    return {'key': key,
            'draws': meas_df[meas_df.metric_id == gbd.metrics.NUMBER]}


def burdenate_caught(key):
    """Try/except wrapper so that burdenate can be used in a
    multiprocessing.Pool without individual Processes getting hung on
    exceptions
    """
    try:
        return burdenate(key)
    except Exception as e:
        dump = traceback.format_exc()
        MPGlobals.logger.error("From traceback {}".format(dump))
        return e


def compute_dalys(ylds_df, ylls_df):
    """Compute DALYs as YLDs+YLLs, returning the resulting DataFrame"""
    MPGlobals.logger.info("Computing DALYs")
    n_draws = MPGlobals.data_container.n_draws
    draw_cols = list(ylds_df.filter(like='draw').columns)
    index_cols = list(set(ylds_df.columns) - set(draw_cols))
    daly_ce = ComputeDalys(
        ylls_df, ylds_df, draw_cols, index_cols)
    daly_df = daly_ce.get_data_frame()
    MPGlobals.logger.info("Writing draws daly")
    daly_df = pd.concat([daly_df, back_calc_pafs(daly_df, n_draws)], sort=True)
    write_draws(daly_df, MPGlobals.args.out_dir, 'daly',
                MPGlobals.args.location_id, MPGlobals.args.year_id,
                MPGlobals.args.write_out_star_ids)
    MPGlobals.logger.info("Done writing draws daly")
    return daly_df[daly_df.metric_id == gbd.metrics.NUMBER]


def map_and_raise(pool, func, arglist):
    """A wrapper around Pool.map that raises a RuntimeError if any of the
    sub-processes raise an Exception. Otherwise, closes the pool and returns
    the result.
    """
    results = pool.map(func, arglist)
    pool.close()
    pool.join()

    reszip = zip(arglist, results)
    exceptions = list(filter(lambda x: isinstance(x[1], Exception), reszip))
    if len(exceptions) > 0:
        exc_strs = ["Args: {}. Error: {}".format(e[0], e[1].__repr__())
                    for e in exceptions]
        exc_str = "\n".join(exc_strs)
        raise RuntimeError("Found errors in mapping '{}':"
                           "\n\n{}".format(func.__name__, exc_str))
    else:
        return results


def summarize_draws(df, index_cols):
    """Summarize the draws down to mean/lower/upper columns"""
    col_order = ['measure_id', 'year_id', 'location_id', 'sex_id',
                 'age_group_id', 'cause_id', 'rei_id', 'star_id', 'metric_id',
                 'mean', 'upper', 'lower']
    if 'star_id' not in df.columns:
        col_order.remove('star_id')
    sumdf = get_summary(df, list(df.filter(like='draw_').columns))
    sumdf = sumdf.reset_index()
    del sumdf['index']
    del sumdf['median']
    return sumdf[col_order]


def summarize_caught(key):
    """Try/except wrapper so that summaries can be produced in a
    multiprocessing.Pool without individual Processes getting hung on
    exceptions
    """
    try:
        meas_dict = [res for res in MPGlobals.results if res['key'] == key][0]
        meas_df = meas_dict['draws']
        summs = aggregate_summaries(meas_df)
        return summs
    except Exception as e:
        MPGlobals.logger.error("Summarize_caught Caught: {}\n".format(key))
        dump = traceback.format_exc()
        MPGlobals.logger.error("From traceback {}".format(dump))
        return dump


def convert_to_rates(df):
    """Convert the values in df from number to rate space"""
    MPGlobals.logger.info("start converting to rates, time = "
                          "{}".format(time.time()))
    mc = MetricConverter(df, to_rate=True,
                         data_container=MPGlobals.data_container,
                         include_pre_df=False)
    newdf = mc.get_data_frame()
    MPGlobals.logger.info("converting to rates complete, time = "
                          "{}".format(time.time()))
    return newdf


def get_summ_filename(draw_dir, risk_type, location_id, year_id, measure_id):
    """Return the summary-file (i.e. csv file) name for the given argset,
    creating the directory if necessary
    """
    if risk_type == RISK_REI_TYPE:
        file_label = 'risk'
    elif risk_type == ETI_REI_TYPE:
        file_label = 'eti'
    fn = ("FILEPATH".format(dd=draw_dir, fl=file_label,
                            loc=location_id, m=measure_id,
                            y=year_id))
    makedirs_safely(os.path.dirname(fn))
    return fn


def write_draws(df, out_dir, measure_label, location_id, year_id,
                write_out_star_ids):
    """Write draws to the appropriate file for the given loc-year-measure"""
    if measure_label == 'death':
        measure_id = gbd.measures.DEATH
    elif measure_label == 'yll':
        measure_id = gbd.measures.YLL
    elif measure_label == 'yld':
        measure_id = gbd.measures.YLD
    elif measure_label == 'daly':
        measure_id = gbd.measures.DALY

    filename = get_input_args.calculate_output_filename(out_dir, measure_id,
                                                        location_id, year_id)
    sink = HDFDataSink(filename)
    remove_unwanted_stars(df, write_out_star_ids=write_out_star_ids)
    cols = get_index_columns(df)
    sink.write(df, id_cols=cols)
    MPGlobals.logger.info("DONE write this_df {}, for measure_id={}, file {}".
                          format(time.time(), measure_id, filename))


def run_pipeline_burdenator(args):
    """
    Run the entire burdenator pipeline. Typically called from
    run_all->qsub->run_pipeline->here

    Will raise ValueError if input files are not present.
    """
    # Start logger
    start_time = time.time()
    logger.info("START pipeline burdenator at {}".format(start_time))
    logger.info("START pipeline burdenator n_draws {}".format(args.n_draws))
    # Validate args before doing any heavy-lifting
    if not args.measure_ids:
        raise ValueError("must pass some measure_ids to burdenator runs ")

    # Share args across processes
    MPGlobals.args = args
    MPGlobals.logger = logger

    # Get detailed ages
    MPGlobals.most_detailed_age_groups = MetricConverter.get_detailed_ages()

    logger.info("START pipeline burdenator, before data_container")
    # Create a DataContainer, cache data to be shared across processes
    data_container = DataContainer(
        cache_granularity_dict={'location_id': args.location_id,
                                'year_id': args.year_id},
        n_draws=args.n_draws,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,
        epi_dir=args.epi_dir,
        cod_dir=args.cod_dir,
        cod_pattern=args.cod_pattern,
        daly_dir=args.daly_dir,
        paf_dir=args.paf_dir,
        turn_off_null_and_nan_check=args.turn_off_null_and_nan_check,
        cache_dir=args.cache_dir,
        raise_on_paf_error=args.raise_on_paf_error,
    )

    # Fetch PAF input from RF team
    logger.info("start apply PAFs, time = {}".format(time.time()))
    draw_columns = ['draw_{}'.format(x) for x in range(args.n_draws)]
    pafs_filter = PAFInputFilter(draw_columns=draw_columns,
                                 index_columns=['location_id', 'year_id',
                                                'sex_id', 'age_group_id',
                                                'cause_id', 'rei_id',
                                                'star_id', 'measure_id'])
    paf_df = data_container['paf']

    pafs_filter.set_input_data_frame(paf_df)
    MPGlobals.pafs_filter = pafs_filter

    # Cache data and burdenate
    measure_names = []
    if gbd.measures['YLL'] in args.measure_ids:
        data_container['yll']
        measure_names.append('yll')
    if gbd.measures['YLD'] in args.measure_ids:
        data_container['yld']
        measure_names.append('yld')
    if gbd.measures['DEATH'] in args.measure_ids:
        data_container['death']
        measure_names.append('death')

    MPGlobals.data_container = data_container
    pool_size = len(args.measure_ids)
    pool = Pool(pool_size)
    results = map_and_raise(pool, burdenate_caught, measure_names)

    # Compute DALYs and associated summaries, if requested
    if gbd.measures['DALY'] in args.measure_ids:
        if gbd.measures.YLL and gbd.measures.YLD not in args.measure_ids:
            raise ValueError("Can't compute risk-attributable DALYs unless "
                             "both ylls and ylds are also provided")
        measure_names.append('daly')
        yld_df = [i['draws'] for i in results if i['key'] == 'yld'][0]
        yll_df = [i['draws'] for i in results if i['key'] == 'yll'][0]
        daly_df = compute_dalys(yld_df[yld_df.measure_id == gbd.measures.YLD],
                                yll_df)
        results.append({'key': 'daly', 'draws': daly_df})

    # Write out meta-information for downstream aggregation step
    meta_df = pd.concat([get_dimensions(r['draws']) for r in results])
    meta_df = aggregate_dimensions(meta_df)

    # Set the results as a Global, for use in summarization Pool
    MPGlobals.results = results

    # Summarize
    pool_size = len(args.measure_ids)
    pool = Pool(pool_size)
    summ_df = map_and_raise(pool, summarize_caught, measure_names)

    summ_df = pd.concat(summ_df)
    summ_df = match_with_dimensions(summ_df, meta_df)
    summ_df.reset_index(drop=True, inplace=True)

    logger.info(
        "Risk attribution & daly computation complete, df shape {}".format(
            (summ_df.shape)))

    logger.info("  FINAL burdenator result shape {}".format(summ_df.shape))

    # Write out the year summaries as CSV files
    rei_types = get_rei_type_id_df()
    summ_df = summ_df.loc[summ_df['rei_id'] != 0]

    for measure_id in summ_df.measure_id.unique():
        for risk_type in [RISK_REI_TYPE, ETI_REI_TYPE]:

            # Get list of rei_ids of this type
            risks_of_type = rei_types[rei_types.rei_type_id == risk_type]
            risks_of_type = risks_of_type.rei_id.squeeze()

            # Compute filename
            summ_fn = get_summ_filename(args.out_dir, risk_type,
                                        args.location_id, args.year_id,
                                        measure_id)
            logger.info("Writing {}".format(summ_fn))
            # Write appropriate subset to file
            write_csv(summ_df[((summ_df.measure_id == measure_id) &
                               (summ_df.rei_id.isin(risks_of_type)))],
                      summ_fn, write_out_star_ids=args.write_out_star_ids,
                      dual_upload=False)

    end_time = time.time()
    elapsed = end_time - start_time
    logger.info("DONE location-year pipeline at {}, elapsed seconds= "
                "{}".format(end_time, elapsed))
    logger.info("{}".format(SUCCESS_LOG_MESSAGE))

    return summ_df.shape


def main():
    get_input_args.create_logging_directories()

    parser = get_input_args.construct_parser_burdenator()
    args = get_input_args.get_args_and_create_dirs(parser)

    logger.debug("year_id= {}, location_id= {}".format(
        args.location_id, args.year_id))

    shape = run_pipeline_burdenator(args)
    logger.debug(" shape= {}".format(shape))


if __name__ == "__main__":
    main()
