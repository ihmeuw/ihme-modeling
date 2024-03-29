import os
import pathlib

import pandas
import pytest


# Data fixtures
#
# These represent actual data in various parts of the system that needs to be
# marshalled/unmarshalled
@pytest.fixture
def parameters():
    "Example parameters data from an ODE model."
    return pandas.DataFrame([
        ['alpha', 0.9967029839013677],
        ['sigma', 0.2729460588151238],
        ['gamma1', 0.5],
        ['gamma2', 0.809867822982699],
        ['day_shift', 5.0],
    ], columns=['params', 'values'])


@pytest.fixture
def coefficients():
    "Example coefficients data from regression."
    return pandas.DataFrame([
        [523, -0.2356716680846281, 0.010188535227465946, 0.27234473836297995, -598.8754409180113],
        [526, -0.24334559662138652, 0.019963189989381125, 0.27234473836297995, -598.8754409180113],
        [533, -0.214475560389406, 0.011172361940536456, 0.27234473836297995, -598.8754409180113],
        [537, -0.09571280930682702, 0.011990915850960831, 0.27234473836297995, -598.8754409180113],
        [538, 0.0988105530655817, 0.0187165992693182, 0.27234473836297995, -598.8754409180113],
    ], columns=["location_id", "intercept", "mobility", "proportion_over_1k", "testing"]).set_index('location_id')


@pytest.fixture
def regression_beta():
    "Example beta from regression. Not be confused with fit beta."
    return pandas.DataFrame([
        [523, pandas.Timestamp("2020-03-07"),
         1, 2.5739807207048058, 4969159.687697555, 345.52948890230834, 99.9253559769042,
         30.788494966654106, 8.07443820131525, 253.22564174849842, 203.997904056312, 523, 1.0, 5.00183501163904,
         0.9999738227721054, 3.49564272841683e-07, 0.9741367332124444],
        [523, pandas.Timestamp("2020-03-08"),
         2, 1.6254293885720643, 4968898.260275219, 483.625404827741, 158.98914338609208,
         68.82741106261548, 34.30607014785281, 269.80262968215254, 249.16085187099705, 523, 1.0, 4.8648570764761,
         0.9999738227721054, 5.44970658216924e-07, 0.9725766504737438],
        [523, pandas.Timestamp("2020-03-09"),
         3, 1.1884395563959436, 4968619.718949269, 601.1713114959216, 224.2559581939473,
         115.57601170740372, 83.28614944679695, 287.46480206654036, 52.025099521832, 523, 1.0, 4.46264044478716,
         0.9999738227721054, 9.1305795481005e-07, 0.9681075127865911],
        [523, pandas.Timestamp("2020-03-10"),
         4, 0.9567975916014266, 4968322.943386977, 704.4042770583011, 289.31826711,
         168.3186879239724, 159.02196884064057, 306.2831986645444, 441.972798383768, 523, 1.0, 3.7269486319883103,
         0.9999738227721054, 1.6100022880651301e-06, 0.9599749904639846],
    ], columns=[
        "loc_id", "date", "days", "beta", "S", "E", "I1", "I2", "R", "newE",
        "newE_obs", "location_id", "intercept", "mobility", "proportion_over_1k",
        "testing", "beta_pred",
    ]).set_index(['location_id', 'date'])
