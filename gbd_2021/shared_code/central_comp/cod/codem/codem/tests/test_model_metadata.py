import pandas as pd
import pytest

from gbd import constants as gbd
from codem.metadata.model_metadata import ModelVersionMetadata
from db_queries import get_covariate_estimates


@pytest.mark.filterwarnings("ignore::UserWarning",
                            match="Both covariate_id and model_version_id")
def test_random_covariate_value():
    # SDI is covariate ID 881
    cov_est = get_covariate_estimates(covariate_id=881,
                                      decomp_step=gbd.decomp_step.FOUR,
                                      gbd_round_id=6,
                                      year_id=1980,
                                      location_id=8)['mean_value'][0]
    m = ModelVersionMetadata(model_version_id=632870, db_connection='ADDRESS',
                             debug_mode=True, update=True,
                             re_initialize=True)
    all_data = pd.concat([m.data_frame, m.covariates], axis=1)
    equivalent = all_data.query('location_id == 8 & year == 1980')['sdi'] == cov_est
    assert all(equivalent)

