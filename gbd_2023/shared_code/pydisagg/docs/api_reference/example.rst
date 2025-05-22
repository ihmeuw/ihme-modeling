PyDisagg Example
================

Age Split Example
-----------------

.. code-block:: python

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
               age_end=[12, 10, 22, 21, 25] * 2,
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


Sex Split Example
-----------------

.. code-block:: python

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
         "pat_val": [0.82236405, 0.82100016, 0.81961923, 0.81874504, 0.81972812],
         "pat_val_sd": [
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
         "mean": [5, 5, 5, 5, 5],
         "standard_error": [1, 1, 1, 1, 1],
         "year_id": [2015, 2019, 2018, 2017, 2016],
         "sex_id": [3, 3, 3, 3, 3],
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
      by=["year_id"], val="pat_val", val_sd="pat_val_sd"
   )

   population_config = SexPopulationConfig(
      index=["year_id"], sex="sex_id", sex_m=1, sex_f=2, val="population"
   )
   sex_splitter = SexSplitter(
      data=data_config, pattern=pattern_config, population=population_config
   )


   result_df = sex_splitter.split(
      data=data_df, pattern=pattern_df, population=population_df
   )
   print("Split Data:")
   print(result_df)

