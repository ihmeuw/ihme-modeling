import sys
import yaml
from pathlib import Path
from loguru import logger

from covid_gbd_model.data.obs_loader import load_obs
from covid_gbd_model.data.cov_loader import load_covs
from covid_gbd_model.data.gbd_db import load_gbd_metadata
from covid_gbd_model.data.forecasting_products import (
    load_covid_location_hierarchy
)


def store_inputs(inputs_root: Path):
    logger.info('Storing GBD metadata (plus Covid location hierarchy).')
    location_metadata, location_metadata_covariate, age_metadata, population = load_gbd_metadata()
    location_metadata.to_parquet(inputs_root / 'location_metadata.parquet')
    location_metadata_covariate.to_parquet(inputs_root / 'location_metadata_covariate.parquet')
    age_metadata.to_parquet(inputs_root / 'age_metadata.parquet')
    population.to_frame().to_parquet(inputs_root / 'population.parquet')
    location_metadata_covid = load_covid_location_hierarchy()
    location_metadata_covid.to_parquet(inputs_root / 'location_metadata_covid.parquet')

    logger.info('Storing mortality observations.')
    obs_dict = load_obs(location_metadata, age_metadata)
    obs_dict['cod_data'].to_parquet(inputs_root / 'cod_data.parquet')
    obs_dict['provisional_data'].to_parquet(inputs_root / 'provisional_data.parquet')
    obs_dict['model_surveillance_data'].to_parquet(inputs_root / 'surveillance_data.parquet')
    obs_dict['overlap_surveillance_data'].to_parquet(inputs_root / 'overlap_surveillance_data.parquet')
    with open(inputs_root / 'age_map.yaml', 'w') as file:
        yaml.dump(obs_dict['age_map'], file)

    logger.info('Storing covariates.')
    covs = load_covs(location_metadata, age_metadata)
    covs.to_parquet(inputs_root / 'covariates.parquet')


if __name__ == '__main__':
    store_inputs(Path(sys.argv[1]))
