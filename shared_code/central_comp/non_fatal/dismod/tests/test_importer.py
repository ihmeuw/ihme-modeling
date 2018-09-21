import pytest
import numpy as np

from conftest import teardown_t3s

data_coltypes = {
    'a_data_id': np.int,
    'age_lower': np.float,
    'age_upper': np.float,
    'atom': np.object,
    'data_like': np.object,
    'hold_out': np.int,
    'integrand': np.object,
    'location_id': np.int,
    'meas_stdev': np.float,
    'meas_value': np.float,
    'nid': np.int,
    'region': np.object,
    'subreg': np.object,
    'super': np.object,
    'time_lower': np.int,
    'time_upper': np.int,
    'x_local': np.int,
    'x_sex': np.float,
    'year_id': np.int}


def test_t2_to_t3_promotion(importer):
    teardown_t3s(importer.mvid)

    importer.promote_dm_t2_to_t3()
    with pytest.raises(ValueError):
        importer.promote_dm_t2_to_t3()

    importer.promote_csmr_t2_to_t3()
    with pytest.raises(ValueError):
        importer.promote_dm_t2_to_t3()

    importer.promote_asdr_t2_to_t3()
    with pytest.raises(ValueError):
        importer.promote_dm_t2_to_t3()

    teardown_t3s(importer.mvid)


def test_get_input_data(importer):

    # the importer loads t3 tables, so they must be emptied first to test
    teardown_t3s(importer.mvid)
    input_data = importer.get_t3_input_data()

    # Ensure some data is returned
    assert len(input_data) > 0

    # Ensure expected (and only expected) columns are returned
    expected_cols = data_coltypes.keys()
    assert set(expected_cols) == set(input_data.columns)

    # Ensure there are no nulls in the data
    assert not input_data.isnull().any().any()

    # Esnure all the dtypes match up
    for col, typ in data_coltypes.iteritems():
        assert input_data.dtypes[col] == typ
