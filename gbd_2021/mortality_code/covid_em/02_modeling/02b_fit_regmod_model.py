
"""
Description: Fits model for expected mortality in the absence of COVID-19
    pandemic, and generates predictions
Steps:
    1. Download config and all-cause and population inputs
    2. Fits model separately for all-ages and detailed ages
    3. Predict model and save summaries
    4. Save plots
"""

# ------------------------------------------------------------------------------

from itertools import product
from typing import Dict, List

import matplotlib.pyplot as plt
import numpy as np
from emmodel.data import DataManager
from emmodel.model import ExcessMortalityModel
from emmodel.model import plot_data
from emmodel.model import plot_model
from emmodel.model import plot_time_trend
from emmodel.variable import SeasonalityModelVariables
from emmodel.variable import TimeModelVariables
from pandas import DataFrame
from regmod.utils import SplineSpecs
from regmod.variable import SplineVariable

import argparse
import logging
import yaml

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')

np.random.seed(123)

# ------------------------------------------------------------------------------


def get_group_specific_data(dm: DataManager,
                            location: str,
                            age_group: str,
                            sex_group: str) -> List[DataFrame]:
    """
    Get location-age-sex-specific data
    """
    df = dm.read_data_location(location,
                               group_specs={"age_name": [age_group],
                                            "sex": [sex_group]})
    data = []
    for i in range(2):
        df_sub = dm.truncate_time_location(location, df, time_end_id=i)
        df_sub["offset_0"] = np.log(df_sub.population)
        data.append(df_sub)
    return data


def get_data(dm: DataManager) -> Dict[str, List[DataFrame]]:
    """
    Get all data
    """
    data = {}
    for location in dm.locations:
        for age_group, sex_group in product(dm.meta[location]["age_groups"],
                                            dm.meta[location]["sex_groups"]):
            dfs = get_group_specific_data(dm, location, age_group, sex_group)
            age_group = age_group.replace(" ", "_")
            data[f"{location}-{age_group}-{sex_group}"] = dfs
    return data


def get_time_knots(time_min: int,
                   time_max: int,
                   units_per_year: int,
                   knots_per_year: float,
                   tail_size: int) -> np.ndarray:
    """
    Select locations on knots for time trend spline
    """
    body_size = time_max - time_min - tail_size + 1
    num_body_knots = int(knots_per_year*body_size/units_per_year) + 1
    if num_body_knots < 2:
        time_knots = np.array([time_min, time_max])
    else:
        time_knots = np.hstack([
            np.linspace(time_min, time_max - tail_size, num_body_knots),
            time_max
        ])
    return time_knots


def get_mortality_pattern_model(df: DataFrame,
                                col_time: str = "time_start",
                                units_per_year: int = 12,
                                knots_per_year: float = 0.5,
                                tail_size: int = 18,
                                smooth_order: int = 1) -> ExcessMortalityModel:
    """
    Define one mortality pattern model
    """
    seas_spline_specs = SplineSpecs(knots=np.linspace(0.0, 1.0, 5),
                                    degree=3,
                                    knots_type="rel_domain")
    time_knots = get_time_knots(df.time.min(),
                                df.time.max(),
                                units_per_year,
                                knots_per_year,
                                tail_size)
    time_spline_specs = SplineSpecs(knots=time_knots,
                                    degree=1,
                                    knots_type="abs")
    seas_var = SplineVariable(col_time, spline_specs=seas_spline_specs)
    time_var = SplineVariable("time", spline_specs=time_spline_specs)
    variables = [
        SeasonalityModelVariables([seas_var], col_time, smooth_order),
        TimeModelVariables([time_var])
    ]
    return ExcessMortalityModel(df, variables)


def get_mortality_pattern_models(dm: DataManager,
                                 data: Dict[str, DataFrame]) -> Dict[str, ExcessMortalityModel]:
    """
    Define multiple mortality pattern models
    """
    models = {}
    for name, dfs in data.items():
        location = name.split("-")[0]
        logging.info('Setting up: {}'.format(name))
        col_time = dm.meta[location]["col_time"]
        units_per_year = dm.meta[location]["time_start"].units_per_year
        tail_size = dm.meta[location]["tail_size"]
        knots_per_year = dm.meta[location]["knots_per_year"]
        smooth_order = dm.meta[location]["smooth_order"]
        models[name] = get_mortality_pattern_model(dfs[0],
                                                   col_time,
                                                   units_per_year,
                                                   knots_per_year,
                                                   tail_size,
                                                   smooth_order)
    return models

# ------------------------------------------------------------------------------

def main(dm: DataManager, config):
    """
    Main run function
    """

    # get dataframes for each location, age_group and sex_group combination
    logging.info('Loading data')
    data = get_data(dm)

    # get mortality pattern models
    logging.info('Setting up models')
    mortality_pattern_models = get_mortality_pattern_models(dm, data)

    # fit mortality pattern models and predict results
    for name, model in mortality_pattern_models.items():
        logging.info('Running: {}'.format(name))
        model.run_models()
        include_ci=(config['n_draws'] > 1)
        data[name][1] = model.predict(data[name][1],
                                      col_pred="mortality_pattern",
                                      include_ci=include_ci,
                                      ci_bounds=config['summary_probs'],
                                      num_samples=config['n_draws'],
                                      return_draws=include_ci)
    results = {name: dfs[1] for name, dfs in data.items()}

    # save the mortality pattern results
    logging.info('Saving results')
    dm.write_data(results)

    logging.info('Done!')


# ------------------------------------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--main_dir', type = str, required = True,
                        help = "main versioned directory for this model run")
    parser.add_argument('--ihme_loc', type = str, required = True,
                        help = "ihme_loc_id for location to run")
    parser.add_argument('--ts', type = int, required = True,
                        help = "tail size in months")

    args = parser.parse_args()
    main_dir = args.main_dir
    ihme_loc = args.ihme_loc
    ts = args.ts

    # load config file
    with open("{}/covid_em_detailed.yml".format(main_dir)) as file:
        config = yaml.load(file, Loader = yaml.FullLoader)
        config = config.get("default")

    i_folder = "{}/inputs/data_all_cause".format(main_dir)
    o_folder = "{}/outputs/summaries_regmod_{}".format(main_dir, ts)
    meta_filename = "meta_{}.yaml".format(ts)
    locations = [ihme_loc]

    main(DataManager(i_folder, o_folder, meta_filename, locations), config)
