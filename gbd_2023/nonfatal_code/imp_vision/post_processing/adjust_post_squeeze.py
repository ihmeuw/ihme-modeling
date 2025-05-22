"""
Project: GBD Vision Loss
Purpose: Functions for assisting with the post squeeze adjustments for
updating the measure for near vision loss and creating best corrected
blindness.
"""

# ---IMPORTS-------------------------------------------------------------------

import logging
import numpy as np
import pandas as pd

from get_draws.api import get_draws

from . process_utils import (get_vision_draws)

logging.basicConfig(
        level=logging.INFO,
        format=' %(asctime)s - %(levelname)s - %(message)s')

# ---FUNCTIONS-----------------------------------------------------------------


def get_best_corrected_blindness(squeezed_blind_re, blindness_envelope,
                                 location_id):
    """

    Arguments:
        squeezed_blind_re (dataframe): Dataframe of estimates for blindness due
            to refractive error.
        blindness_envelope (dataframe): Dataframe of estimates for the
            blindness envelope.
        location_id (int): The location to pull and adjust draws for.

    Returns:
        Dataframe of best corrected blindness estimates.

    """
    logging.info("running the bc blindness code for {}".format(location_id))
    # logging.info("running the bc blindness code for {}".format(location_id))
    keepcols = ['measure_id', 'metric_id', 'sex_id', 'year_id', 'age_group_id',
                'location_id']
    keepcols.extend(('draw_{i}'.format(i=i) for i in range(1000)))

    logging.info("now getting results for location id {}".format(location_id))
    merged = pd.merge(squeezed_blind_re,
                      blindness_envelope,
                      on=['location_id',
                          'year_id',
                          'sex_id',
                          'age_group_id',
                          'measure_id',
                          'metric_id'],
                      suffixes=['_RE', '_ENV'])
    # subtract out refractive error
    for i in range(0, 1000):
        merged['draw_{}'.format(i)] = merged[
            'draw_{}_ENV'.format(i)] - merged['draw_{}_RE'.format(i)]

    merged = merged[keepcols]
    merged['modelable_entity_id'] = ID

    envelope = blindness_envelope.query(
        "sex_id == ID and year_id == 2023 and age_group_id == ID"
    ).draw_0.values[0]
    re = squeezed_blind_re.query(
        "sex_id ==ID and year_id == 2023 and age_group_id == ID"
    ).draw_0.values[0]
    final = merged.query(
        "sex_id == ID and year_id == 2023 and age_group_id == ID"
    ).draw_0.values[0]

    if (envelope - re) != final:
        raise ValueError("""
                         The difference of the blindness and the squeezed re
                         are not equal to the final estimates""")
    return merged


def adjust_near_vision_prevalence(location_id, age_ids_list, release_id):
    """

    Arguments:
         location_id (int): The location to pull and adjust draws for.
         age_ids_list (intlist): The age_group_ids to use.
         release_id (int): The release of GBD to use.

    Returns:
        Dataframe of near vision estimates with prevalence as the measure.

    """
    logging.info("running the near vision prevalence adjustment for {}".format(
        location_id))

    near_vision = get_vision_draws(
        meid=ID,
        measure_id=ID,
        location_id=location_id,
        age_group_id=age_ids_list,
        release_id=release_id)
    near_vision['measure_id'] = ID
    near_vision['modelable_entity_id'] = ID
    return near_vision
