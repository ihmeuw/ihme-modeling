#' @title Result differences in Handoffs
#'
#' @description Pulls two versions of a handoff and creates a summary of differences
#'    between the two
#'
#' @param old_handoff_file \[`data.table()`\]\cr
#'   filepath of the old handoff parquet
#' @param new_handoff_file \[`data.table()`\]\cr
#'   filepath of the new handoff parquet
#' @param id_cols \[`list`\]\cr
#'   list of id columns used to merge old and new files
#' @param measure_cols \[`list`\]\cr
#'   list of measure columns to compare between old and new files
#' @param handoff_name \[`vector`\]\cr
#'   name to use when saving datasets
#' @param old_run_id \[`vector`\]\cr
#'   name to use to indicate which id has been used as comparison
#' @param age_map \[`data.table()`\]\cr
#'   dataset containing at least age_group_name, age_group_id, age_start,
#'   and age_end
#' @param location_map \[`data.table()`\]\cr
#'   dataset containing at least ihme_loc_id and location_id
#' @param digits \[`integer`\]\cr
#'   number of digits to use for rounding, default = 6
#'
#' @return \[`data.table()`\]\cr
#'   1. dataset with differences in deaths, sample_size, and mx columns
#'   2. datasets highlighting:
#'    a. added data
#'    b. dropped data
#'    c. changed data
#'   3. dataset with summary of difference in outliering
#'   4. summary location/year/sources with value or outliering changes
#'   5. summary mx rmse ranked highest to lowest

summarize_result_differences <- function(old_handoff,
                                         new_handoff,
                                         id_cols,
                                         measure_cols,
                                         handoff_name,
                                         old_run_id,
                                         age_map,
                                         location_map,
                                         digits = 6) {
  keep <- c(id_cols, measure_cols)

  if(!"age_start" %in% names(new_handoff)) {
    old_handoff[age_map, ':=' (age_start = i.age_start, age_end = i.age_end), on = "age_group_id"]
    new_handoff[age_map, ':=' (age_start = i.age_start, age_end = i.age_end), on = "age_group_id"]
  }

  if(!"ihme_loc_id" %in% names(new_handoff)) {
    old_handoff[location_map, ihme_loc_id := i.ihme_loc_id, on = "location_id"]
    new_handoff[location_map, ihme_loc_id := i.ihme_loc_id, on = "location_id"]
  }

  old_handoff <- old_handoff[, ..keep]
  new_handoff <- new_handoff[, ..keep]

  dt <- merge(
    new_handoff,
    old_handoff,
    by = id_cols,
    suffixes = c("_new", "_old"),
    all = TRUE
  )

  # 1. datasets with summary of differences in deaths, sample_size, and mx columns
  message("Making Table 1: measure sums")
  dt_summary <- copy(dt)

  dt_summary_all <- dt_summary[
    ,
    .(sample_size_sum_new = sum(sample_size_new, na.rm = TRUE),
      sample_size_sum_old = sum(sample_size_old, na.rm = TRUE),
      population_sum_new = sum(population_new, na.rm = TRUE),
      population_sum_old = sum(population_old, na.rm = TRUE),
      deaths_sum_new = sum(deaths_new, na.rm = TRUE),
      deaths_sum_old = sum(deaths_old, na.rm = TRUE),
      mx_mean_new = weighted.mean(mx_new, sample_size_new, na.rm = TRUE),
      mx_mean_old = weighted.mean(mx_old, sample_size_old, na.rm = TRUE)
    )
  ][, data := "all"]

  dt_summary_no_new <- dt_summary[
    outlier_new == 0,
    .(sample_size_sum_new = sum(sample_size_new),
      population_sum_new = sum(population_new),
      deaths_sum_new = sum(deaths_new, na.rm = T),
      mx_mean_new = weighted.mean(mx_new, sample_size_new)
    )
  ]

  dt_summary_no_old <- dt_summary[
    outlier_old == 0,
    .(sample_size_sum_old = sum(sample_size_old),
      population_sum_old = sum(population_old),
      deaths_sum_old = sum(deaths_old, na.rm = T),
      mx_mean_old = weighted.mean(mx_old, sample_size_old)
    )
  ]

  dt_summary_no <- cbind(dt_summary_no_new, dt_summary_no_old)[, data := "un-outliered"]

  dt_combine <- rbind(dt_summary_all, dt_summary_no)

  for (mm in c("sample_size_sum", "population_sum", "deaths_sum", "mx_mean")) {
    mm_new <- paste0(mm, "_new")
    mm_old <- paste0(mm, "_old")
    mm_diff <- paste0(mm, "_diff")
    mm_perc_diff <- paste0(mm, "_perc_diff")
    # round to 8 digits
    dt_combine[, paste0(mm, "_diff") := get(mm_new) - get(mm_old)]
    dt_combine[is.na(get(mm_diff)), paste0(mm, "_diff") := 0]
    dt_combine[, paste0(mm, "_perc_diff") := ((get(mm_new) - get(mm_old)) / get(mm_old)) * 100]
    dt_combine[is.na(get(mm_perc_diff)), paste0(mm, "_perc_diff") := 0]
  }

  new_col_order <- sort(colnames(dt_combine))
  dt_save <- dt_combine[, ..new_col_order]

  readr::write_csv(
    dt_save,
    fs::path("FILEPATH")
  )

  # 2. dataset highlighting new data, dropped data, or changed data
  message("Making Table 2: data changes summary")
  dt_changes <- copy(dt)

  # calculate differences
  for (mm in setdiff(measure_cols, c("outlier", "outlier_note"))) {
    mm_new <- paste0(mm, "_new")
    mm_old <- paste0(mm, "_old")
    mm_diff <- paste0(mm, "_diff")
    mm_perc_diff <- paste0(mm, "_perc_diff")
    # round to 6 digits
    dt_changes[,
      paste0(mm, "_diff") := round(get(mm_new), digits) - round(get(mm_old), digits)]
    dt_changes[is.na(get(mm_diff)), paste0(mm, "_diff") := 0]
    dt_changes[,
      paste0(mm, "_perc_diff") := ((round(get(mm_new), digits) - round(get(mm_old), digits)) / round(get(mm_old), digits)) * 100]
    dt_changes[is.na(get(mm_perc_diff)), paste0(mm, "_perc_diff") := 0]
  }

  # identify type of change for each data point
  new_diff_cols <- grep("_diff$", names(dt_changes), value = TRUE)
  dt_changes[, diff_sum := rowSums(.SD), .SDcols = new_diff_cols]

  dt_changes[!is.na(mx_new) & is.na(mx_old), data_change_type := "added"]
  dt_changes[is.na(mx_new) & !is.na(mx_old), data_change_type := "dropped"]
  dt_changes[!is.na(mx_new) & !is.na(mx_old) & !diff_sum == 0, data_change_type := "changed"]
  dt_changes[!is.na(mx_new) & !is.na(mx_old) & diff_sum == 0, data_change_type := "identical"]
  dt_changes[, diff_sum := NULL]

  # drop identical rows for simplicity
  dt_changes <- dt_changes[!data_change_type == "identical"]

  readr::write_csv(
    dt_changes[data_change_type == "added"],
    fs::path("FILEPATH")
  )

  readr::write_csv(
    dt_changes[data_change_type == "dropped"],
    fs::path("FILEPATH")
  )

  readr::write_csv(
    dt_changes[data_change_type == "changed"],
    fs::path("FILEPATH")
  )

  # 3. dataset with summary of differences in outliering
  message("Making Table 3: outlier changes summary")
  dt_outliers <- copy(dt)

  dt_outliers <- dt_outliers[!(outlier_new == outlier_old)]

  keep_outliers <- c(id_cols, "outlier_new", "outlier_old", "outlier_note_new", "outlier_note_old")
  dt_outliers <- dt_outliers[, ..keep_outliers]

  readr::write_csv(
    dt_outliers,
    fs::path("FILEPATH")
  )

  # 4. dataset with list of location/year/sources with any kind of change
  message("Making Table 4: any location/year/source summary")
  dt_value_changes <- unique(dt_changes[, c("location_id", "ihme_loc_id", "source_type_name", "year_id")])
  dt_outlier_changes <- unique(dt_outliers[, c("location_id", "ihme_loc_id", "source_type_name", "year_id")])

  dt_any_changes <- unique(rbind(dt_value_changes, dt_outlier_changes))

  # save a file with unique location/years with any change
  readr::write_csv(
    dt_any_changes,
    fs::path("FILEPATH")
  )

  # 5. dataset with list of location/year with mx rmse ranked highest to lowest
  message("Making Table 5: list of mx changes ranked")
  dt_mx_changes <- dt_changes[mx_diff != 0]
  dt_mx_changes <- dt_mx_changes[, c(..id_cols, "mx_old", "mx_new")]
  setnames(dt_mx_changes, "outlier2", "outlier")

  dt_mx_changes <- dt_mx_changes[
    outlier == 0,
    .(mx_rmse = sqrt(mean((mx_new - mx_old)^2))),
    by = c("location_id", "ihme_loc_id")
  ]
  dt_mx_changes <- dt_mx_changes[order(-mx_rmse)]

  # save a file ranked by mx rmse
  readr::write_csv(
    dt_mx_changes,
    fs::path("FILEPATH")
  )

}

#' @title Compare only mx in datasets
#'
#' @description Takes two datasets and compares mx
#'
#' @param test_file \[`data.table()`\]\cr
#'   data table with new mx to test
#' @param compare_file \[`data.table()`\]\cr
#'   data table for comparison with older mx
#' @param id_cols \[`list`\]\cr
#'   list of id columns used to merge old and new files
#' @param warn_only \[`bool`\]\cr `default FALSE`
#'   changing to true only warns about high values rather than errors
#' @param age_map \[`data.table()`\]\cr
#'   dataset containing at least age_group_name, age_group_id, age_start,
#'   and age_end
#' @param location_map \[`data.table()`\]\cr
#'   dataset containing at least ihme_loc_id and location_id
#'
#' @return \[`data.table()`\]\cr
#'   1. dataset with differences in mx

quick_mx_compare <- function(test_file,
                             compare_file,
                             id_cols,
                             warn_only = FALSE) {

  subset_cols <- c(id_cols, "mx")

  dt <- merge(
    test_file[outlier == 0, ..subset_cols],
    compare_file[outlier == 0, ..subset_cols],
    by = id_cols,
    suffixes = c("_new", "_old"),
    all = TRUE
  )

  # calculate differences - round to 8 digits
  dt[, mx_diff := mx_new - mx_old]
  dt[is.na(mx_diff), mx_diff := 0]
  dt[, mx_perc_diff := ((mx_new - mx_old) / mx_old) * 100]
  dt[is.na(mx_perc_diff), mx_perc_diff := 0]

  # identify type of change for each data point
  dt[!is.na(mx_new) & is.na(mx_old), data_change_type := "added"]
  dt[is.na(mx_new) & !is.na(mx_old), data_change_type := "dropped"]
  dt[!is.na(mx_new) & !is.na(mx_old) & !mx_new == mx_old, data_change_type := "changed"]
  dt[!is.na(mx_new) & !is.na(mx_old) & mx_new == mx_old, data_change_type := "identical"]

  # drop identical rows for simplicity
  dt <- dt[!data_change_type == "identical"]

  # output warnings
  num_added <- nrow(dt[data_change_type == "added"])
  message(paste0("Un-outliered data has ", num_added, " rows of data added"))

  num_dropped <- nrow(dt[data_change_type == "dropped"])
  message(paste0("Un-outliered data has ", num_dropped, " rows of data dropped"))

  num_changed <- nrow(dt[data_change_type == "changed"])
  message(paste0("Un-outliered data has ", num_changed, " rows of data changed"))

  # TODO: experiment with threshold or allowing to specify threshold
  large_changes <- dt[mx_perc_diff > 30]
  if (warn_only == TRUE & nrow(large_changes) > 0) {
    message("Waning message:\nThere are rows with >30% difference in mx - review")
  } else if ((warn_only == FALSE & nrow(large_changes) > 0)){
    stop("There are rows with >30% difference in mx - review")
  }

  message("Summary of mx percent differences: ")
  print(summary(dt[data_change_type == "changed", mx_perc_diff]))

  return(dt)

}


