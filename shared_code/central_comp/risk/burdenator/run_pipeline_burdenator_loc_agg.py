import logging
import pandas as pd
import os
import time

from db_queries import get_location_metadata
from adding_machine.agg_locations import agg_all_locs_mem_eff
import gbd.constants as gbd

import dalynator.get_input_args as get_input_args


# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE write DF"


def check_for_agg_errors(result_list, logger):
    errors = list(filter(lambda x: x[0] > 0, result_list))
    if len(errors) > 0:
        for error in errors:
            params, exc, tb = error[1:]
            logger.error("Aggregation error. Params: {}. Exception: {}. "
                         "Traceback: {}".format(params, exc, tb))
        raise RuntimeError("Found errors in location aggregation. See logs.")


def find_h5_tablename(drawdir, location_set_id, year_id, measure_id,
                      gbd_round_id):
    '''Inspect one most detailed draw file to find the h5 tablename used.
    This will raise RuntimeError if the h5 file has more than one table in it.

    We're assuming tablenames don't vary depending on location.
    '''
    locations = get_location_metadata(location_set_id=location_set_id,
                                      gbd_round_id=gbd_round_id)
    loc_id = locations[locations.most_detailed == 1].location_id.tolist()[0]
    test_file = os.path.join(drawdir, str(loc_id),
                             '{}_{}_{}.h5'.format(measure_id, loc_id, year_id))
    return get_tablename_from_file(test_file)


def get_tablename_from_file(filepath):
    with pd.HDFStore(filepath, mode='r') as store:
        tablenames = store.keys()
        if len(tablenames) > 1:
            logger = logging.getLogger(__name__)
            msg = "File {} has more than one h5 tablename: {}".format(
                    filepath, tablenames)
            logger.critical(msg)
            raise RuntimeError(msg)

    return tablenames[0].replace('/', '')


def run_loc_agg(out_dir, location_set_id, year_id, sex_id, rei_id, measure_id,
                gbd_round_id):
    """Aggregate a set of burdenated draws up the location hierarchy specified
    by location_set_id.  Aggregation is scoped to a specific
    year-sex-rei-measure combo.

    Args:
        out_dir (str): the root directory for this burdenator run
        location_set_id (int): the set identifier for the location tree to
            aggregate
        year_id (int): year to aggregate
        sex_id (int): sex_id to aggregate
        rei_id (int): rei_id to aggregate
        measure_id (int): measure_id to aggregate
    """
    # Start logger
    logger = logging.getLogger(__name__)
    start_time = time.time()
    logger.info("START pipeline burdenator loc agg at {}".format(start_time))

    # Aggregate draws
    logger.info("start aggregate locations, time = {}".format(time.time()))
    drawdir = os.path.join(out_dir, 'draws')
    stagedir = os.path.join(out_dir, 'loc_agg_draws/burden')
    index_cols = ['measure_id', 'metric_id', 'sex_id', 'cause_id', 'rei_id',
                  'year_id', 'age_group_id']
    draw_filters = {'rei_id': rei_id, 'metric_id': gbd.metrics.NUMBER}
    custom_file_pattern = ('{{location_id}}/'
                           '{measure_id}_{{location_id}}_{year_id}.h5')
    custom_file_pattern = custom_file_pattern.format(measure_id=measure_id,
                                                     year_id=year_id)

    out_pattern = ('{{location_id}}/{measure_id}/'
                   '{measure_id}_{year_id}_{{location_id}}_'
                   '{rei_id}_{sex_id}.h5')
    out_pattern = out_pattern.format(measure_id=measure_id, year_id=year_id,
                                     sex_id=sex_id, rei_id=rei_id)
    h5_tablename = find_h5_tablename(drawdir, location_set_id, year_id,
                                     measure_id, gbd_round_id)
    res = agg_all_locs_mem_eff(
        drawdir,
        stagedir,
        location_set_id,
        index_cols,
        year_id,
        sex_id,
        measure_id,
        draw_filters=draw_filters,
        include_leaves=False,
        operator='sum',
        weight_col='pop_scaled',
        normalize='auto',
        custom_file_pattern=custom_file_pattern,
        output_file_pattern=out_pattern,
        h5_tablename=h5_tablename,
        poolsize=15,
        return_exceptions=True)
    check_for_agg_errors(res, logger)
    logger.info("location aggregation complete, time = {}".format(time.time()))

    end_time = time.time()
    elapsed = end_time - start_time
    logger.info("DONE location agg pipeline at {}, elapsed seconds= {}".format(
        end_time, elapsed))
    logger.info("{}".format(SUCCESS_LOG_MESSAGE))


def main():
    parser = get_input_args.construct_parser_burdenator_loc_agg()
    args = get_input_args.get_args_burdenator_loc_agg(parser)

    run_loc_agg(args.out_dir, args.location_set_id, args.year_id, args.sex_id,
                args.rei_id, args.measure_id, args.gbd_round_id)


if __name__ == "__main__":
    main()
