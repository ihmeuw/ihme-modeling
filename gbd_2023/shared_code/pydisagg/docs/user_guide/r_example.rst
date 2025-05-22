PyDisagg Example in R
=====================

Age Split Example
-----------------

.. code-block:: r

   library(reticulate)
   reticulate::use_python("/some/path/to/miniconda3/envs/your-conda-env/bin/python")
   splitter <- import("pydisagg.ihme.splitter")

   # Create data
   data <- function() {
     set.seed(123)
     data.frame(
       uid = 0:9,
       sex_id = c(rep(1, 5), rep(2, 5)),
       location_id = rep(c(1, 2), 5),
       year_id = rep(2010, 10),
       age_start = rep(c(0, 5, 10, 17, 20), 2),
       age_end = rep(c(12, 10, 22, 21, 25), 2),
       val = 5.0,
       val_sd = 1.0
     )
   }

   # Create pattern data
   pattern <- function() {
     set.seed(123)
     pattern_df1 <- data.frame(
       sex_id = c(rep(1, 5), rep(2, 5)),
       age_start = rep(c(0, 5, 10, 15, 20), 2),
       age_end = rep(c(5, 10, 15, 20, 25), 2),
       age_group_id = rep(0:4, 2),
       draw_0 = runif(10),
       draw_1 = runif(10),
       draw_2 = runif(10),
       year_id = rep(2010, 10),
       location_id = rep(1, 10)
     )
     pattern_df2 <- pattern_df1
     pattern_df2$location_id <- 2
     rbind(pattern_df1, pattern_df2)
   }

   # Create population data
   population <- function() {
     set.seed(123)
     sex_id <- data.frame(sex_id = c(1, 2))
     year_id <- data.frame(year_id = 2010)
     location_id <- data.frame(location_id = c(1, 2))
     age_group_id <- data.frame(age_group_id = 0:4)
     
     population <- merge(sex_id, location_id)
     population <- merge(population, age_group_id)
     population <- merge(population, year_id)
     population$population <- 1000
     population
   }

   df_data <- data()
   df_pattern <- pattern()
   df_pop <- population()

   # Add zero row
   df_data_zero <- rbind(
     df_data,
     c(
       uid = 10,
       sex_id = df_data[nrow(df_data), "sex_id"],
       location_id = df_data[nrow(df_data), "location_id"],
       year_id = df_data[nrow(df_data), "year_id"],
       age_start = df_data[nrow(df_data), "age_start"],
       age_end = df_data[nrow(df_data), "age_end"],
       val = 0,
       val_sd = df_data[nrow(df_data), "val_sd"]
     )
   )

   # Configure splitters
   data_config <- splitter$AgeDataConfig(
     index = c("uid", "sex_id", "location_id", "year_id"),
     age_lwr = "age_start",
     age_upr = "age_end",
     val = "val",
     val_sd = "val_sd"
   )

   pat_config <- splitter$AgePatternConfig(
     by = c("sex_id", "year_id", "location_id"),
     age_key = "age_group_id",
     age_lwr = "age_start",
     age_upr = "age_end",
     draws = c("draw_0", "draw_1", "draw_2")
   )

   pop_config <- splitter$AgePopulationConfig(
     index = c("sex_id", "year_id", "location_id", "age_group_id"),
     val = "population"
   )

   age_splitter <- splitter$AgeSplitter(
     data = data_config, pattern = pat_config, population = pop_config
   )

   # Perform splits
   result_df <- age_splitter$split(
     data = df_data, pattern = df_pattern, population = df_pop, propagate_zeros = TRUE
   )
   cat("Split Data:\n")
   print(result_df)

   result_df_zero <- age_splitter$split(
     data = df_data_zero, pattern = df_pattern, population = df_pop, propagate_zeros = TRUE
   )
   cat("Split Data with Zero:\n")
   print(result_df_zero)


Sex Split Example
-----------------

.. code-block:: r

   # Create data
   pattern_df <- data.frame(
     location_id = c(78, 130, 120, 30, 141),
     year_id = 2015:2019,
     pat_val = c(0.82236405, 0.82100016, 0.81961923, 0.81874504, 0.81972812),
     pat_val_sd = c(0.00688405, 0.00552016, 0.00413923, 0.00326504, 0.00424812)
   )

   data_df <- data.frame(
     seq = c(303284043, 303284062, 303284063, 303284064, 303284065),
     location_id = c(78, 130, 120, 30, 141),
     mean = rep(5, 5),
     standard_error = rep(1, 5),
     year_id = 2015:2019,
     sex_id = rep(3, 5)
   )

   population_df <- data.frame(
     location_id = rep(c(78, 130, 120, 30, 141), 2),
     year_id = rep(2015:2019, 2),
     sex_id = c(rep(2, 5), rep(1, 5)),
     population = c(10230, 19980, 29870, 40120, 49850, 10234, 19876, 30245, 39789, 50234)
   )

   # Configure splitters
   data_config <- splitter$SexDataConfig(
     index = c("seq", "location_id", "year_id", "sex_id"),
     val = "mean",
     val_sd = "standard_error"
   )

   pattern_config <- splitter$SexPatternConfig(
     by = c("year_id"),
     val = "pat_val",
     val_sd = "pat_val_sd"
   )

   population_config <- splitter$SexPopulationConfig(
     index = c("year_id"),
     sex = "sex_id",
     sex_m = 1,
     sex_f = 2,
     val = "population"
   )

   sex_splitter <- splitter$SexSplitter(
     data = data_config, pattern = pattern_config, population = population_config
   )

   result_df <- sex_splitter$split(
     data = data_df, pattern = pattern_df, population = population_df
   )
   cat("Split Data:\n")
   print(result_df)
