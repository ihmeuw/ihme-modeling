#' Compile and output draws for cause_id 618 based on CoD VR data
compile_other_hemog <- function(cod_data, year_id, age_group_id, sex_id) {
  other_hemog_dr_vr <- data.table::copy(
    cod_data[, .(age_group_id, sex_id, year_id, deaths, pop)]
  )
  
  # Pool cod data across all data rich locations --------------------------
  cli::cli_progress_step("Pooling mean rate, sample size, and variance for other hemog...",
                         msg_done = "Pooling calculations complete for other hemog.")
  
  pooled_dat <- other_hemog_dr_vr |>
    pool_other_hemog(year_id = year_id) |>
    dplyr::select(c(age_group_id, sex_id, year_id, pooled_rate, wilson_se))
  
  # Check for missing rows in cod data output -----------------------------
  cli::cli_progress_step("Checking for expected age/sex/year combinations...",
                         msg_done = "Finished checking for missing rows.")
  
  # check for missing age/sex/year combinations
  check_for_missing_cod(
    pooled_dat = pooled_dat,
    age_group_id = age_group_id,
    sex_id = sex_id,
    year_id = year_id,
    error_if_missing = TRUE)
  
  # Generate 1000 draws of the data rich pooled deaths by age/sex/year ----
  cli::cli_progress_step("Generating draws for other hemog...",
                         msg_done = "Draws generated for other hemog.")
  
  draws_colnames <- paste0("draw_", 0:999)
  final_df <- pooled_dat
  for (draw in draws_colnames) {
    final_df[[draw]] <- NA
  }
  # generate draws and directly assign them to the new df
  final_df[, (draws_colnames) := lapply(.SD, as.numeric), .SDcols = draws_colnames]
  for (n in 1:nrow(final_df)) {
    input_row <- pooled_dat[n, ]
    new_norm <- rnorm(1000, mean = input_row$pooled_rate, sd = input_row$wilson_se)
    final_df[n, (draws_colnames) := as.list(new_norm)]
  }
  cli::cli_progress_done()
  return(final_df)
}

#' Calculate pooled mean rate and Wilson standard error
pool_other_hemog <- function(cod_data, year_id) {
  pooling_dat <- data.frame()
  for (i in year_id) {
    i <- as.integer(i)
    # set a year range to pool across
    if (i %in% c(2019, 2020, 2021, 2022, 2023, 2024)) {
      year_range <- c(2014:2024)
    } else {
      year_range <- c((i-5):(i+5))
    }
    # subset to years within the year range, then pool counts grouped by age and sex
    pool <- cod_data[
      !(is.na(deaths)) & year_id %in% year_range,
      .(
        sum_deaths = sum(deaths),
        sum_pop = sum(pop)
      ),
      by = c("age_group_id", "sex_id")
    ]
    # calculate pooled rate and standard error
    pool$pooled_rate <- pool$sum_deaths / pool$sum_pop
    pool$wilson_se <- nch::wilson_score_interval_se(p = pool$pooled_rate, n = pool$sum_pop)
    pool$year_id <- i
    
    pooling_dat <- rbind(pooling_dat, pool)
  }
  return(pooling_dat)
}

#' Check for missing age/sex/year combinations by comparing present to expected
check_for_missing_cod <- function(pooled_dat, age_group_id, sex_id, year_id, error_if_missing = TRUE) {
  dat <- data.table::copy(pooled_dat)
  # create a data frame of all expected age/sex/year combinations
  expected_combinations <- expand.grid(
    age_group_id = age_group_id,
    sex_id = sex_id,
    year_id = unique(year_id) 
  )
  # define present age/sex/year combinations
  present_combinations <- dat |>
    dplyr::select(c(age_group_id, sex_id, year_id)) |> 
    dplyr::distinct() 
  # identify expected age/sex/year combinations that are missing from cod output
  missing_combinations <- dplyr::anti_join(
    expected_combinations, 
    present_combinations, 
    by = c("age_group_id", "sex_id", "year_id")
  )
  # if any expected combinations are missing, return informative error/warning
  if (nrow(missing_combinations) > 0) {
    missing_details <- apply(missing_combinations, 1, function(row) {
      paste0("age group ", row['age_group_id'], "/sex ", row['sex_id'], 
             "/year ", row['year_id'])
    })
    missing_details_str <- paste(missing_details, collapse=", ")
    if (error_if_missing == TRUE) {
      stop(paste("Unable to fill missing estimates for the following", 
                 "age/sex/year combinations:", missing_details_str))
    } else {
      warning(paste("Unable to fill missing estimates for the following", 
                    "age/sex/year combinations:", missing_details_str))
    }
  } else if (nrow(missing_combinations) == 0) {
    message("All expected age/sex/year combinations present")
  }
}

#' Identify expected age group IDs based on metadata
get_age_group_ids <- function(release_id) {
  ihme::get_age_metadata(
    release_id = release_id
  ) |>
    dplyr::pull("age_group_id") |> 
    unique()
}
