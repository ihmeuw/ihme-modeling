
# Validation Functions ----------------------------------------------------
validate_shock_aggregator <- function(dataset) {
  # assert that location_id matches the given location_id
  assert_values(dataset, colnames = c('location_id'), test = 'equal', test_val = LOCATION_ID, quiet = T)
  # No data should be missing
  assert_values(dataset, colnames = names(dataset), test = "not_na", quiet = T)
  assert_values(dataset, colnames = names(dataset), test = "gte", test_val = 0, quiet = T)

  # assert_ids for ages, years, sexes, that are required
  ages <- c(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 30, 31, 32, 235)
  sexes <- c(1, 2)

  id_vars <- list("age_group_id" = ages, "sex_id" = sexes, "year_id" = INPUT_YEARS)
  assert_ids(dataset, id_vars = id_vars, quiet = T)
}

validate_mx_ax <- function(dataset) {
  # assert that location_id matches the given location_id
  assert_values(dataset, colnames = c('location_id'), test = 'equal', test_val = LOCATION_ID, quiet = T)
  # No data should be missing
  assert_values(dataset, colnames = names(dataset), test = "not_na", quiet = T)

  # assert_ids for ages, years, sexes, that are required
  ages <- c(5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 28, 30, 31, 32, 33, 44, 45, 148)
  sexes <- c(1, 2)

  id_vars <- list("age_group_id" = ages, "sex_id" = sexes, "year_id" = INPUT_YEARS, "draw" = 0:999)
  assert_ids(dataset, id_vars = id_vars, quiet = T)
}

# make sure mx is: no_hiv_lt < with_hiv_lt < shock_lt
validate_draw_lt_mx <- function(no_hiv_lt, with_hiv_lt, shock_lt) {
  no_hiv <- no_hiv_lt[, list(location_id, year_id, sex_id, draw, age, mx)]
  with_hiv <- with_hiv_lt[, list(location_id, year_id, sex_id, draw, age, mx)]
  shock <- shock_lt[, list(location_id, year_id, sex_id, draw, age, mx)]

  setnames(no_hiv, "mx", "no_hiv_mx")
  setnames(with_hiv, "mx", "with_hiv_mx")
  setnames(shock, "mx", "shock_mx")

  comparison <- merge(no_hiv, with_hiv, by = c("location_id", "year_id", "sex_id", "draw", "age"))
  comparison <- merge(comparison, shock, by = c("location_id", "year_id", "sex_id", "draw", "age"))

  comparison[, no_hiv_mx := signif(no_hiv_mx, 5)]
  comparison[, with_hiv_mx := signif(with_hiv_mx, 5)]
  comparison[, shock_mx := signif(shock_mx, 5)]

  diagnostics <- list(
    "with_hiv_vs_no_hiv" = assert_values(comparison, colnames = c("with_hiv_mx"), test = "gte", test_val = comparison[, no_hiv_mx], warn_only = T, quiet = T),
    "shock_vs_with_hiv" = assert_values(comparison, colnames = c("shock_mx"), test = "gte", test_val = comparison[, with_hiv_mx], warn_only = T, quiet = T)
  )
  return(diagnostics)
}

validate_abridged_qx <- function(dataset, log_dir) {
  rows_over_1_5 <- dataset[qx >1]
  if (nrow(rows_over_1_5) > 0) {
    diagnostic_file <- paste0(log_dir, "/", LOCATION_ID, ".csv")
    warning(paste0("Found rows where abridged qx is over 1, saving affected rows here: ", diagnostic_file))
    write_csv(rows_over_1_5, diagnostic_file)
  }

  rows_inf <- dataset[mx == Inf]
  if (nrow(rows_inf) > 0) {
    print(rows_inf)
    stop("Found rows where mx is Inf after conversion.")
  }

}

validate_full_lifetable_draw <- function(dataset) {
  lt_ids <- list(location_id = LOCATION_ID,
                 year_id = INPUT_YEARS,
                 sex_id = 1:2,
                 age = 0:MAX_AGE,
                 draw = 0:999)

  assert_ids(dataset, id_vars = lt_ids, quiet = T)
  assert_values(dataset, colnames = c("qx", 'ax'), test = "gte", test_val = 0, quiet = T)
  assert_values(dataset, colnames = c("qx"), test = "lte", test_val = 1, quiet = T)
}

validate_abridged_lifetable_draw <- function(dataset, compare_dataset = NULL, log_dir = NULL) {
  lt_ids <- list(location_id = LOCATION_ID,
                 year_id = INPUT_YEARS,
                 sex_id = 1:2,
                 age = unique(age_groups[, age]),
                 draw = 0:999)

  assert_ids(dataset, id_vars = lt_ids, quiet = T)
  assert_values(dataset, colnames = c("qx", 'ax'), test = "gte", test_val = 0, quiet = T)
  assert_values(dataset, colnames = "ax", test = "lte", test_val = 5, quiet = T)
  assert_values(dataset[age == 0], colnames = "ax", test = "lte", test_val = 1, quiet = T)
  assert_values(dataset, colnames = c("qx"), test = "lte", test_val = 1, quiet = T)
  assert_values(dataset[age != 110], colnames = c("qx"), test = "lt", test_val = 1, quiet = T)

  ## Make draw-level diagnostic if ax drifts more than 25%
  if(!is.null(compare_dataset)) {
    lt_cols <- names(lt_ids)
    setnames(compare_dataset, "ax", "ax_input")
    abridged_ax <- merge(dataset[, .SD, .SDcols = c(lt_cols, "ax")],
                         compare_dataset[, .SD, .SDcols = c(lt_cols, "ax_input")],
                         by = lt_cols)
    abridged_ax <- abridged_ax[abs((ax - ax_input) / ax_input) > .25]
    if(nrow(abridged_ax) > 0) {
      warning(paste0(nrow(abridged_ax), " Rows have ax drift over 25% in abridged lifetable"))
      write_csv(abridged_ax, paste0(log_dir, "/", LOCATION_ID, ".csv"))
    }
  }
}

validate_full_lifetable_summary <- function(dataset) {
  lt_ids <- list(location_id = LOCATION_ID,
                 year_id = INPUT_YEARS,
                 sex_id = 1:2,
                 age = 0:MAX_AGE,
                 life_table_parameter_name = c("ax", "qx", "nLx", "lx", "Tx", "ex", "mx"))

  assert_ids(dataset, id_vars = lt_ids, quiet = T)
  assert_values(dataset, colnames = c("mean"), test="gte", test_val = 0, quiet = T)
}

validate_abridged_lifetable_summary <- function(dataset) {
  lt_ids <- list(location_id = LOCATION_ID,
                 year_id = INPUT_YEARS,
                 sex_id = 1:2,
                 age_group_id = age_groups[, age_group_id],
                 life_table_parameter_name = c("ax", "mx"))

  assert_ids(dataset, id_vars = lt_ids, quiet = T)
  assert_values(dataset, colnames = c("mean"), test="gte", test_val = 0, quiet = T)

  assert_values(dataset[life_table_parameter_name == "ax"], colnames = "mean", test = "lte", test_val = 5, quiet = T)
  assert_values(dataset[life_table_parameter_name == "ax" & age_group_id == 28], colnames = "ax", test = "lte", test_val = 1, quiet = T)
}
