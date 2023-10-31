"""Tools for resampling draw-level data."""
from typing import Dict, List, TYPE_CHECKING

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    # The model subpackage is a library for the pipeline stage and shouldn't
    # explicitly depend on things outside the subpackage.
    from covid_model_seiir_pipeline.pipeline.postprocessing.specification import ResamplingSpecification


def build_resampling_map(deaths: pd.DataFrame, resampling_params: 'ResamplingSpecification'):
    cumulative_deaths = deaths.groupby(level='location_id').cumsum().reset_index()
    cumulative_deaths['date'] = pd.to_datetime(cumulative_deaths['date'])
    reference_date = pd.to_datetime(resampling_params.reference_date)
    reference_deaths = (cumulative_deaths[cumulative_deaths['date'] == reference_date]
                        .drop(columns=['date'])
                        .set_index('location_id'))
    upper_deaths = reference_deaths.quantile(resampling_params.upper_quantile, axis=1)
    lower_deaths = reference_deaths.quantile(resampling_params.lower_quantile, axis=1)
    resample_map = {}
    for location_id in reference_deaths.index:
        upper, lower = upper_deaths.at[location_id], lower_deaths.at[location_id]
        loc_deaths = reference_deaths.loc[location_id]
        to_resample = loc_deaths[(upper < loc_deaths) | (loc_deaths < lower)].index.tolist()
        np.random.seed(location_id)
        # Degenerate case
        keep_draws = loc_deaths.index.difference(to_resample)
        resample_count = min(len(keep_draws), len(to_resample))
        if resample_count:
            to_fill = np.random.choice(keep_draws, resample_count, replace=False).tolist()
            resample_map[location_id] = {'to_resample': to_resample,
                                         'to_fill': to_fill}
    return resample_map


def resample_draws(measure_data: pd.DataFrame, resampling_map: Dict[int, Dict[str, List[int]]]):
    output = []
    locs = measure_data.reset_index().location_id.unique()
    for location_id, loc_map in resampling_map.items():
        if location_id not in locs:
            continue
        loc_data = measure_data.loc[location_id]
        loc_data[loc_map['to_resample']] = loc_data[loc_map['to_fill']]
        loc_data = pd.concat({location_id: loc_data}, names=['location_id'])
        if isinstance(loc_data, pd.Series):
            loc_data = loc_data.unstack()
        output.append(loc_data)

    resampled = pd.concat(output)
    resampled.columns = [f'draw_{draw}' for draw in resampled.columns]
    return resampled
