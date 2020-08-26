reconcile_hiv <- function(full_lt, full_hiv_draws, pnn_env_draw, pop_dt_sy, hiv_group, id_vars) {
  ## Age 0-15:
  ##    Group 1A, 1B, 2B, 2C:
  ##    Group 2A: 
  ## Age 15 plus:
  ##    Group 1A, 1B, 2B, 2C:
  ##    Group 2A:

  value_vars <- c( "ax", "reconciled_hiv","mx_spec_hiv", "with_hiv_mx", "no_hiv_mx")

  full_rows <- nrow(full_lt)
  hiv_rows <- nrow(full_hiv_draws)
  if(full_rows != hiv_rows) stop("Number of rows of full LT and HIV draws do not match")
  full_lt <- merge(full_lt, full_hiv_draws, by = id_vars)
  if(full_rows != nrow(full_lt)) stop("Merge issue between full LT and HIV rows")

  full_lt[, reconciled_hiv := as.numeric(NA)]
  full_lt[, implied_lt_hiv := with_hiv_mx - no_hiv_mx]

  ## Extract variables for HIV adjustment vetting
  ## Only keep under-94 as the 95+ category will be added on at the end.
  hiv_adj_values <- c("implied_lt_hiv", "with_hiv_mx", "no_hiv_mx", "mx_spec_hiv")
  no_age_vars <- id_vars[id_vars != "age"]

  input_mx_hiv <- full_lt[age <= 94, .SD, .SDcols = c(id_vars, hiv_adj_values)]
  input_mx_hiv[age < 5, age_group_id := 1]
  input_mx_hiv[age >= 5 & age < 15, age_group_id := 23]
  input_mx_hiv[age >= 15 & age < 50, age_group_id := 24]
  input_mx_hiv[age >= 50, age_group_id := 40]

  input_mx_hiv <- merge(input_mx_hiv, pop_dt_sy, by = id_vars[id_vars != "draw"])
  input_mx_hiv[, (hiv_adj_values) := lapply(.SD, function(x) x * input_mx_hiv$population), .SDcols = hiv_adj_values]
  input_mx_hiv <- input_mx_hiv[, lapply(.SD, sum), by = c(no_age_vars, "age_group_id"), .SDcols = hiv_adj_values]

  if(hiv_group %in% c("1A", "1B")) {
    full_lt[age < 15, reconciled_hiv := with_hiv_mx * (1 - hiv_free_ratio)]
    full_lt[age >= 15, reconciled_hiv := (mx_spec_hiv + implied_lt_hiv) / 2]
  } else {
    full_lt[, reconciled_hiv := mx_spec_hiv]
  }

  u1_reconciled <- merge(full_lt[age == 0], pnn_env_draw, by = id_vars[id_vars != "age"])
  u1_reconciled[reconciled_hiv > (.9 * with_hiv_mx_pnn), reconciled_hiv := .9 * with_hiv_mx_pnn]
  u1_reconciled <- u1_reconciled[, .SD, .SDcols = c(id_vars, value_vars)]

  full_lt <- rbindlist(list(full_lt[age != 0, .SD, .SDcols = c(id_vars, value_vars)], 
                            u1_reconciled[, .SD, .SDcols = c(id_vars, value_vars)]), 
                       use.names = T)

  assertable::assert_values(full_lt, "reconciled_hiv", "not_na", quiet = T)

  ## Apply reconciliation: for high-HIV locations 
  if(hiv_group %in% c("1A", "1B")) {
    full_lt[age < 15 & reconciled_hiv > (.9 * with_hiv_mx), reconciled_hiv := .9 * with_hiv_mx]
    full_lt[age < 15, no_hiv_mx := with_hiv_mx - reconciled_hiv]

    if(hiv_group == "1A") {
      full_lt[age >= 15, with_hiv_mx := no_hiv_mx + reconciled_hiv]
    } else {
      full_lt[age >= 15 & reconciled_hiv > (.9 * with_hiv_mx), reconciled_hiv := .9 * with_hiv_mx]
      full_lt[age >= 15, no_hiv_mx := with_hiv_mx - reconciled_hiv]
    }
  } else {
    full_lt[reconciled_hiv > (.9 * with_hiv_mx), reconciled_hiv := .9 * with_hiv_mx]
    full_lt[, no_hiv_mx := with_hiv_mx - reconciled_hiv]
  }

  # full_lt <- full_lt[, .SD, .SDcols = c(id_vars, "with_hiv_mx", "no_hiv_mx")]

  assert_values(full_lt, "with_hiv_mx", "gte", full_lt$no_hiv_mx, quiet = T)
  assert_values(full_lt, c("with_hiv_mx", "no_hiv_mx"), "not_na", quiet = T)
  assert_values(full_lt, "no_hiv_mx", "gte", 0, quiet = T)

  return(list(full_lt = full_lt, hiv_adjust_input = input_mx_hiv))
}
