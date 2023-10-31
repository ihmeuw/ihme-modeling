import numpy as np
import pandas as pd
import pytest
from codem.stgpr.gpr_smooth import gpr


@pytest.fixture
def df():
    df = pd.DataFrame({
        'year': [1987, 1988, 1989, 1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997,
                 1998, 1999, 2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008,
                 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 1980, 1981, 1982,
                 1983, 1984, 1985, 1986, 2017, 2018, 2019],
        'ln_rate': [-6.8714485, -6.771848, -6.9873214, -6.640334, -7.0597105,
                    -7.0226083, -7.105315, -7.8787208, -7.855964, -7.8350534,
                    -7.9551945, -7.7668524, -7.6750026, -7.7207623, -8.154961,
                    -7.753655, -7.8759427, -8.184664, -7.9031677, -8.050006,
                    -8.192574, -7.9611206, -8.366542, -8.580969, -9.057668,
                    -8.699102, -9.0056715, -8.533488, -8.320164, -8.913113,
                    np.nan, np.nan, np.nan, np.nan, np.nan,
                    np.nan, np.nan, np.nan, np.nan, np.nan],
        'ln_rate_sd': [0.6026707, 1.0095001, 0.9826264, 0.7607806, 0.7672633,
                       0.86156416, 0.9719179, 0.7895598, 0.8269946, 0.8869043,
                       0.8886906, 0.83594084, 0.7812219, 0.83534795, 0.81883794,
                       0.86878526, 1.051394, 0.8337666, 0.9162412, 0.98927724,
                       0.81802344, 0.9079545, 0.8697554, 0.9052173, 0.93457884,
                       1.197042, 0.9344866, 1.1535388, 0.8826767, 0.79143393,
                       np.nan, np.nan, np.nan, np.nan, np.nan,
                       np.nan, np.nan, np.nan, np.nan, np.nan],
        'ln_rate_nsv': [0.03674062, 0.03674062, 0.03674062, 0.03674062, 0.03674062,
                        0.03674062, 0.03674062, 0.03674062, 0.03674062, 0.03674062,
                        0.03674062, 0.03674062, 0.03674062, 0.03674062, 0.03674062,
                        0.03674062, 0.03674062, 0.03674062, 0.03674062, 0.03674062,
                        0.03674062, 0.03674062, 0.03674062, 0.03674062, 0.03674062,
                        0.03674062, 0.03674062, 0.03674062, 0.03674062, 0.03674062,
                        0.03674062, 0.03674062, 0.03674062, 0.03674062, 0.03674062,
                        0.03674062, 0.03674062, 0.03674062, 0.03674062, 0.03674062],
        'ko01_train': [False, False, False, False, False, False, False, False, False,
                       False, False, False, True, True, True, True, True, False,
                       True, True, True, True, True, True, True, True, True,
                       True, True, True, False, False, False, False, False, False,
                       False, False, False, False],
        'ko01_test1': [True, True, True, True, True, True, True, True, True,
                       True, True, True, False, False, False, False, False, False,
                       False, False, False, False, False, False, False, False, False,
                       False, False, False, False, False, False, False, False, False,
                       False, False, False, False],
        'ko01_test2': [False, False, False, False, False, False, False, False, False,
                       False, False, False, False, False, False, False, False, True,
                       False, False, False, False, False, False, False, False, False,
                       False, False, False, False, False, False, False, False, False,
                       False, False, False, False],
        'variance_type': ['aight', 'aight', 'aight', 'aight', 'aight', 'aight', 'aight',
                          'aight', 'aight', 'aight', 'aight', 'aight', 'aight', 'aight',
                          'aight', 'aight', 'aight', 'aight', 'aight', 'aight', 'aight',
                          'aight', 'aight', 'aight', 'aight', 'aight', 'aight', 'aight',
                          'aight', 'aight', 'aight', 'aight', 'aight', 'aight', 'aight',
                          'aight', 'aight', 'aight', 'aight', 'aight']})
    return df[['year', 'ln_rate', 'ln_rate_sd', 'ln_rate_nsv',
               'ko01_train', 'ko01_test1', 'ko01_test2', 'variance_type']]


@pytest.fixture
def prior():
    return np.array([-7.551469 , -7.563094 , -7.578105 , -7.5981216, -7.6220284,
                     -7.6484833, -7.67557  , -7.7019544, -7.7271185, -7.7466145,
                     -7.760415 , -7.76542  , -7.7415276, -7.7827744, -7.8738637,
                     -7.860794 , -7.919349 , -7.9828715, -8.007423 , -8.071341 ,
                     -8.125244 , -8.157651 , -8.24362  , -8.32487  , -8.4012   ,
                     -8.4098215, -8.443773 , -8.414494 , -8.408151 , -8.472776 ,
                     -7.4166703, -7.4373565, -7.4599004, -7.4838705, -7.5065255,
                     -7.525656 , -7.539917 , -8.450915 , -8.451434 , -8.455121 ])


@pytest.fixture
def result():
    return np.array([-7.5515468, -7.56321801, -7.57829481, -7.59840504, -7.622443,
                     -7.64908026, -7.67641794, -7.70314462, -7.72877153, -7.74888725,
                     -7.7635091, -7.76958846, -7.74707812, -7.79005129, -7.88320043,
                     -7.872534, -7.93397346, -8.0009992, -8.02975472, -8.09858261,
                     -8.15798532, -8.19624072, -8.28803597, -8.37453489, -8.45494199,
                     -8.46603779, -8.50073378, -8.47056343, -8.4619816, -8.52330357,
                     -7.41666297, -7.43735057, -7.4598975, -7.48387299, -7.50653686,
                     -7.5256815, -7.53996346, -8.49729829, -8.49316631, -8.49203343])


@pytest.fixture
def amplitude():
    return 0.18061948


@pytest.fixture
def response():
    return 'ln_rate'


@pytest.fixture
def scale():
    return 10


@pytest.fixture
def has_data():
    return True


def test_gpr(df, response, amplitude, prior, scale, has_data, result):
    g = gpr(df2=df, response=response, amplitude=amplitude,
            prior=prior, scale=scale, has_data=has_data)
    assert g[np.isnan(g)].size == 0
    assert len(g) == len(df)
    assert np.isclose(g, result, atol=0.0000001).all()


def test_variance(df, response, amplitude, prior, scale, has_data, result):
    """
    Double the variance going into GPR, and then the residuals should be
    larger between the GPR mean function and the data.
    """
    df2 = df.copy()
    df2['ln_rate_sd'] = df2['ln_rate_sd']*2
    g = gpr(df2=df, response=response, amplitude=amplitude,
            prior=prior, scale=scale, has_data=has_data)
    data = df2[response].values
    assert sum(abs(data[~np.isnan(data)] - g[~np.isnan(data)])) > \
           sum(abs(data[~np.isnan(data)] - result[~np.isnan(data)]))

