import numpy as np
import pandas as pd

from pydisagg.ihme.splitter import (
    AgeSplitter,
    AgeDataConfig,
    AgePatternConfig,
    AgePopulationConfig,
)


def data():
    np.random.seed(123)
    return pd.DataFrame(
        dict(
            uid=range(10),
            sex_id=[1] * 5 + [2] * 5,
            location_id=[1, 2] * 5,
            year_id=[2010] * 10,
            age_start=[0, 5, 10, 17, 20] * 2,
            age_end=[4, 10, 22, 21, 25] * 2,
            val=5.0,
            val_sd=1.0,
        )
    )


def pattern():
    np.random.seed(123)
    pattern_df1 = pd.DataFrame(
        dict(
            sex_id=[1] * 5 + [2] * 5,
            age_start=[0, 5, 10, 15, 20] * 2,
            age_end=[5, 10, 15, 20, 25] * 2,
            age_group_id=list(range(5)) * 2,
            draw_0=np.random.rand(10),
            draw_1=np.random.rand(10),
            draw_2=np.random.rand(10),
            year_id=[2010] * 10,
            location_id=[1] * 10,
        )
    )
    pattern_df2 = pattern_df1.copy()
    pattern_df2["location_id"] = 2
    return pd.concat([pattern_df1, pattern_df2]).reset_index(drop=True)


def population():
    np.random.seed(123)
    sex_id = pd.DataFrame(dict(sex_id=[1, 2]))
    year_id = pd.DataFrame(dict(year_id=[2010]))
    location_id = pd.DataFrame(dict(location_id=[1, 2]))
    age_group_id = pd.DataFrame(dict(age_group_id=range(5)))

    population = (
        sex_id.merge(location_id, how="cross")
        .merge(age_group_id, how="cross")
        .merge(year_id, how="cross")
    )
    population["population"] = 1000
    return population


df_data = data()
df_pattern = pattern()
df_pop = population()


last_row = df_data.iloc[-1].copy()
last_row["uid"] = 10
last_row["val"] = 0
df_data_zero = df_data._append(last_row, ignore_index=True)

data_config = AgeDataConfig(
    index=["uid", "sex_id", "location_id", "year_id"],
    age_lwr="age_start",
    age_upr="age_end",
    val="val",
    val_sd="val_sd",
)

pat_config = AgePatternConfig(
    by=["sex_id", "year_id", "location_id"],
    age_key="age_group_id",
    age_lwr="age_start",
    age_upr="age_end",
    draws=["draw_0", "draw_1", "draw_2"],
)

pop_config = AgePopulationConfig(
    index=["sex_id", "year_id", "location_id", "age_group_id"],
    val="population",
)

age_splitter = AgeSplitter(data=data_config, pattern=pat_config, population=pop_config)
result_df = age_splitter.split(
    data=df_data, pattern=df_pattern, population=df_pop, propagate_zeros=True
)
print("Split Data:")
print(result_df)

result_df_zero = age_splitter.split(
    data=df_data_zero,
    pattern=df_pattern,
    population=df_pop,
    propagate_zeros=True,
)
print("Split Data with Zero:")
print(result_df_zero)
