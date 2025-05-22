import pandas as pd

from pydisagg.ihme.splitter import (
    SexSplitter,
    SexDataConfig,
    SexPatternConfig,
    SexPopulationConfig,
)

pattern_df = pd.DataFrame(
    {
        "location_id": [78, 130, 120, 30, 141],
        "year_id": range(2015, 2020),
        "mean": [0.82236405, 0.82100016, 0.81961923, 0.81874504, 0.81972812],
        "standard_error": [
            0.00688405,
            0.00552016,
            0.00413923,
            0.00326504,
            0.00424812,
        ],
    }
)

data_df = pd.DataFrame(
    {
        "seq": [303284043, 303284062, 303284063, 303284064, 303284065],
        "location_id": [78, 130, 120, 30, 141],
        "mean": [0.5, 0.5, 0.5, 0.5, 0.5],
        "standard_error": [0.1, 0.1, 0.1, 0.1, 0.1],
        "year_id": [2015, 2019, 2018, 2017, 2016],
        "sex_id": [2, 3, 3, 3, 3],
    }
)

population_df = pd.DataFrame(
    {
        "location_id": [78, 130, 120, 30, 141, 78, 130, 120, 30, 141],
        "year_id": list(range(2015, 2020)) * 2,
        "sex_id": [2] * 5 + [1] * 5,
        "population": [
            10230,
            19980,
            29870,
            40120,
            49850,
            10234,
            19876,
            30245,
            39789,
            50234,
        ],
    }
)

# Configurations
data_config = SexDataConfig(
    index=["seq", "location_id", "year_id", "sex_id"],
    val="mean",
    val_sd="standard_error",
)

pattern_config = SexPatternConfig(
    by=["year_id"],
    val="mean",
    val_sd="standard_error",
)

population_config = SexPopulationConfig(
    index=["year_id"], sex="sex_id", sex_m=1, sex_f=2, val="population"
)

sex_splitter = SexSplitter(
    data=data_config, pattern=pattern_config, population=population_config
)

try:
    result_df = sex_splitter.split(
        data=data_df,
        pattern=pattern_df,
        population=population_df,
        model="logodds",
    )
    print("Split Data:")
    print(result_df)
except ValueError as e:
    print(f"Error: {e}")
