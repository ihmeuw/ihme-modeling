import os
import sys
import pytest
from pandas.util.testing import assert_frame_equal, assert_series_equal

from conftest import teardown_t3s

# Old style "import" structure based on CWD
here = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(here, '..'))


# List attributes that are subject to import/export
casc_attrs = [
        "value_prior",
        "simple_prior",
        "rate_prior",
        "effects_prior",
        "model_version_meta",
        "integrand_bounds",
        "age_mesh",
        "dUSERt_age_mesh",
        "study_covariates",
        "ccov_means",
        "data",
        "mtall",
        "single_param",
        "model_params",
        "covdata",
        "ccov_map",
        "scov_map",
        "measure_map",
        "age_weights"]
casc_attrs = [pytest.mark.xfail(attr) for attr in casc_attrs]
casc_loc_attrs = []


@pytest.mark.parametrize("attr", casc_attrs)
def test_cascade_db_import(cascade_from_db, cascade_from_files, attr):
    """Tests that a freshly imported cascade from the database matches the
    fixture files (which are a straight copy from the GBD2015 "production"
    cascade)"""
    c = cascade_from_db
    cff = cascade_from_files

    cdf = getattr(c, attr).reset_index(drop=True)
    cffdf = getattr(cff, attr).reset_index(drop=True)
    assert_df_equal(cdf, cffdf)


@pytest.mark.parametrize("attr", casc_attrs)
def test_cascade_coltypes(cascade_from_db, cascade_from_files, attr):
    """Tests that the column types of a freshly impoted cascade from the
    database match the column types when importing from fixture files (which
    are a straight copy from the GBD2015 "production" cascade)"""
    c = cascade_from_db
    cff = cascade_from_files

    cdf = getattr(c, attr)
    cffdf = getattr(cff, attr)
    assert_series_equal(cdf.dtypes, cffdf.dtypes)


@pytest.mark.parametrize("attr", casc_loc_attrs)
def test_cascade_loc_db_import(cascade_from_db, cascade_from_files, attr):
    """Tests that a freshly imported cascade_loc from the database matches the
    fixture files (which are a straight copy from the GBD2015 "production"
    cascade)"""
    pass


@pytest.mark.parametrize("attr", casc_loc_attrs)
def test_cascade_loc_coltypes(cascade_from_db, cascade_from_files, attr):
    """Tests that the column types of a freshly imported cascade_loc from the
    database match the column types when importing from fixture files (which
    are a straight copy from the GBD2015 "production" cascade)"""
    pass


def test_cascade_loc_result(mvid):
    """Test that the results of running dismod_ode at the top level (Global) of
    the cascade match the results from the final GBD2015 production code
    base"""
    from cascade_ode.drill import Cascade, Cascade_loc

    teardown_t3s(mvid)
    c = Cascade(mvid)
    cl = Cascade_loc(1, 0, 2015, c, timespan=50, reimport=True)

    # TODO: Consider moving this to a separate test, __init__ of the
    # Cascade_loc object is substantial enough to be a test on it's own
    cl.run_dismod()
    cl.summarize_posterior()
    cl.draw()
    cl.predict()

    # TODO: Compare appropriate cascade_loc result-related attributes with
    # fixture


def test_casc_refactor():
    #from cascade_ode.cascade import Cascade
    #from cascade_ode.cascade_loc import Cascade_loc
    from cascade_ode.drill import Cascade as CascadeRef, \
        Cascade_loc as Cascade_locRef

    # TODO: Compare refactored classes to original bin/ classes


# UTILITIES ##########
def assert_df_equal(df1, df2):
    """Sorts the columns before asserting equality... for our purposes,
    DataFrames with differing column orders but the same row-content
    are equivalent"""
    return assert_frame_equal(df1.sort(axis=1), df2.sort(axis=1),
                              check_names=True)
