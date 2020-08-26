#' Iterate terminal qx & lx values to minimize the difference to observed terminal mx
#'
#' @param dt data.table with variables: ihme_loc_id, year, source_name, sex, age, age_length, dx, ax, qx, terminal_age_start, mx_term
#' @param ax_params parameters for ax extension from mlt
#' @param id_vars character vector of id variables that uniquely identify each observation (last one must be age)
#' @param n_iterations numeric, number of iterations to run
#'
#'
#' @return None. Modifies given data.table in-place
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable


## helper functions =====================================================================

# recalc ax
gen_80plus_ax <- function(dt, ax_params) {
  dt <- merge(dt, ax_params, by = c("sex", "age"), all.x=T)
  dt[age >= 80, ax := par_qx * qx + par_sqx * (qx^2) + par_con]
  dt[, c("par_qx", "par_sqx", "par_con") := NULL]
  return(dt)
}

# recalculate qx from betas representing age pattern
qx_from_beta <- function(dt, id_vars){
  setorderv(dt, id_vars)
  by_vars <- id_vars[id_vars != "age"]
  dt[, q95_for100 := shift(qx, n = 1, type = "lag"), by = by_vars]
  dt[, q95_for105 := shift(qx, n = 2, type = "lag"), by = by_vars]
  dt[age == 100, qx := beta1 * q95_for100]
  dt[age == 105, qx := beta2 * q95_for105]
  return(dt)
}

# create flags for qx > 1 or qx < 5q90
gen_flags <- function(dt, id_vars){
  by_vars <- id_vars[id_vars != "age"]
  setorderv(dt, id_vars)
  if("flag" %in% names(dt)) dt$flag <- NULL
  if("reset_type" %in% names(dt)) dt$reset_type <- NULL
  # flag if qx > 1
  dt[age >= terminal_age_start & qx > 1, flag := 1]
  # flag if qx < 5q90 (or whatever pre-terminal age is)
  dt[age == (terminal_age_start - 5), qx_before_terminal := qx]
  dt[, qx_before_terminal := max(qx_before_terminal, na.rm = T), by = by_vars]
  dt[age >= terminal_age_start & qx < qx_before_terminal, flag := 10]
  # create empty flag if no issues
  dt[is.na(flag), flag := 0]
  # summarize flags for the full life table
  dt[, flag := sum(flag), by = by_vars]
  # create "reset_type" column based on flag summary
  dt[flag == 0, reset_type := "none"]
  dt[flag == 3, reset_type := "all_term_qx_over1"]
  dt[flag == 30, reset_type := "all_term_qx_under_q90"]
  dt[is.na(reset_type), reset_type := "decrease_slope"]
}

## MAIN FUNCTION =============================================================================

iterate_qx <- function(dt, ax_params, id_vars, n_iterations) {

  dt <- copy(dt)
  setkeyv(dt, id_vars)
  setorderv(dt, id_vars)
  by_vars <- id_vars[id_vars != "age"]

  # save initial qx val
  dt[, qx_initial := qx]

  holdouts_master <- dt[is.na(mx_term) | mx_term < 0.05 | mx_term > 2 | ihme_loc_id %like% "ZAF"]
  holdouts <- data.table()
  dt <- dt[!(is.na(mx_term) | mx_term < 0.05 | mx_term > 2 | ihme_loc_id %like% "ZAF")]

  # calculate age-pattern terminal qx
  dt[, qxplus1 := shift(qx, type = "lead"), by = by_vars]
  dt[, qxplus2 := shift(qx, n = 2, type = "lead"), by = by_vars]
  dt[age == 95, beta1 := qxplus1/qx]
  dt[age == 95, beta2 := qxplus2/qx]
  dt[, beta1 := max(beta1, na.rm = T), by = by_vars]
  dt[, beta2 := max(beta2, na.rm = T), by = by_vars]

  # LEVEL ITERATIONS ===========================================================================
  run_iterations <- function(dt, holdouts, n_iterations, quiet = F, ax_scale = 1){
    dt <- copy(dt)
    holdouts <- copy(holdouts)
    iter_num <- 1
    while(nrow(dt) > 0 & iter_num < n_iterations) {
      if(quiet == F) print(paste0("Iteration ", iter_num))
      setorderv(dt, id_vars)

      # calculate l100, l105, a100, a105
      dt[, l100 := shift(lx, type = "lead"), by = by_vars]
      dt[, l105 := shift(lx, n = 2, type = "lead"), by = by_vars]
      dt[, a100 := shift(ax, type = "lead"), by = by_vars]
      dt[, a105 := shift(ax, n = 2, type = "lead"), by = by_vars]

      # get 5L95, 5L100, 5L105
      dt[age == 95, L95 := 5*lx*(1-qx) + ax*lx*qx]
      dt[age == 95, L100 := 5*l100*(1-qx*beta1) + a100*l100*qx*beta1]
      dt[age == 95, L105 := 5*l105*(1-qx*beta2) + a105*l105*qx*beta2]

      # calculate T95 = sum of Lx vals
      dt[age == 95, Tx := L95 + L100 + L105]

      # calculate est of m95 = l95 / T95
      dt[age == 95, m95_est := lx / Tx]

      # calculate diff
      dt[age == 95, diff := abs(m95_est - mx_term)]
      dt[age != 95, diff := NA]
      dt[, diff := max(diff, na.rm = T), by = by_vars]

      # remove if diff is < 0.001
      holdouts <- rbindlist(list(holdouts, dt[diff <= .0001]), use.names = T, fill = T)
      dt <- dt[diff > .0001]

      # change qx
      if("qx_new" %in% names(dt)) dt$qx_new <- NULL
      if(iter_num == 1){
        dt[m95_est > mx_term & age == 95, qx_new := qx*0.8]
        dt[m95_est < mx_term & age == 95, qx_new := qx*1.2]
        dt[, qx_latest := qx_initial]
      } else {
        # qx needs to be decreased, and we're above our last qx try
        dt[m95_est > mx_term & age == 95 & qx > qx_latest, qx_new := qx - 0.5*abs(qx - qx_latest)]
        # qx needs to be increased, and we're below our latest qx try
        dt[m95_est < mx_term & age == 95 & qx < qx_latest, qx_new := qx + 0.5*abs(qx - qx_latest)]
        # qx needs to be decreased further, we're below our latest qx try but above second-latest
        dt[m95_est > mx_term & age == 95 & qx < qx_latest & qx > qx_latest2, qx_new := qx - 0.5*abs(qx - qx_latest2)]
        # qx needs to be increased further, we're above our latest qx try but below second-latest
        dt[m95_est < mx_term & age == 95 & qx > qx_latest & qx < qx_latest2, qx_new := qx + 0.5*abs(qx - qx_latest2)]
        # none of these scenarios applies, increase/decrease based on proportion
        dt[m95_est > mx_term & age == 95 & is.na(qx_new), qx_new := qx*0.98]
        dt[m95_est < mx_term & age == 95 & is.na(qx_new), qx_new := qx*1.02]
        # cap at qx = 0.999
        dt[qx_new * beta2 > 0.9999, qx_new := 0.9999 / beta2]
      }
      dt[, qx_latest2 := qx_latest]
      dt[, qx_latest := qx]
      dt[age == 95, qx := qx_new]
      dt <- qx_from_beta(dt, id_vars)

      # recalc lx, dx, ax
      setkeyv(dt, id_vars)
      qx_to_lx(dt, assert_na = T)
      dt[, dx := lx * qx]
      dt <- gen_80plus_ax(dt, ax_params)
      dt[age >= terminal_age_start, ax := ax * ax_scale]
      setkeyv(dt, id_vars)

      if(quiet == F){
        print(paste0("Number of remaining iteration rows: ", nrow(dt)))
        print(summary(dt$diff))
      }

      iter_num <- iter_num + 1
    }
    print(nrow(dt))
    print(nrow(holdouts))
    dt <- rbindlist(list(dt, holdouts), use.names = T, fill = T)
    return(dt)
  }
  dt <- run_iterations(dt, holdouts, n_iterations, ax_scale = 1)


  # SLOPE ITERATIONS ===============================================================================
  # iterate to fix slope if it caused issues in the initial iteration
  run_slope_iteration <- function(dt, holdouts_master, n_iterations, ax_scale = 1){
    iter_num_2 <- 1
    done <- FALSE
    holdouts <- data.table()
    while(done == FALSE & iter_num_2 < n_iterations){

      dt <- gen_flags(dt, id_vars)

      # if this adjustment caused qx over 1, or a decrease in qx with age,
      #     FOR SOME terminal age groups, decrease slope of age-pattern
      #     and try rescale again
      # Hold out if beta already at 1
      dt[reset_type == "none" & (beta1 + beta2) != 2 & diff <= 0.0001, completed := 1]
      dt[is.na(completed), completed := 0]
      holdouts_master <- rbind(holdouts_master, dt[completed == 1], fill = T)
      holdouts <- dt[completed == 0 & (beta1 + beta2) == 2]
      dt <- dt[completed == 0 & (beta1 + beta2) != 2]
      print(paste0(round(nrow(dt)/length(unique(dt$age)), 0),
                   " life tables to fix still"))
      if(nrow(dt) == 0) done <- T
      dt[,`:=` (beta1 = max(0.98 * beta1, 1),
                beta2 = max(0.98 * beta2, 1))]
      dt <- qx_from_beta(dt, id_vars)
      setkeyv(dt, id_vars)

      # recalc lx, dx, ax -- need to do because we changed qx
      qx_to_lx(dt, assert_na = T)
      dt[, dx := lx * qx]
      dt <- gen_80plus_ax(dt, ax_params)

      # iterate qx to scale to term mx
      setkeyv(dt, id_vars)
      dt <- run_iterations(dt, holdouts, 30, quiet = T, ax_scale)
      iter_num_2 <- iter_num_2 + 1
    }
    return(list(dt, holdouts_master))
  }
  slope_iter_results <- run_slope_iteration(dt, holdouts_master, n_iterations)
  dt <- slope_iter_results[[1]]
  holdouts_master <- slope_iter_results[[2]]

  # combine and set status
  dt <- gen_flags(dt, id_vars)
  dt[beta1 + beta2 == 2, reset_type := "betas"]
  print(paste0("Beta issue: ", nrow(unique(dt[reset_type == "betas"], by = by_vars))))

  ## ITERATE AX ==================================================================================
  # if slope was flattened and we still have qx below 5q90, try adjusting ax
  #       if you adjust ax down, you get larger qx for the same mx
  holdouts_master <- rbind(holdouts_master, dt[reset_type == "none" & diff <= 0.0001], fill = T)
  dt <- dt[!(reset_type == "none" & diff <= 0.0001)]

  recalc_beta_plus_lt_params <- function(dt){
    # reset qx and betas
    dt[, qxplus1 := shift(qx, type = "lead"), by = by_vars]
    dt[, qxplus2 := shift(qx, n = 2, type = "lead"), by = by_vars]
    dt[age == 95, beta1 := qxplus1/qx]
    dt[age == 95, beta2 := qxplus2/qx]
    dt[, beta1 := max(beta1, na.rm = T), by = by_vars]
    dt[, beta2 := max(beta2, na.rm = T), by = by_vars]

    # recalc other vals because qx changes
    setkeyv(dt, id_vars)
    qx_to_lx(dt, assert_na = T)
    dt[, dx := lx * qx]
    dt <- gen_80plus_ax(dt, ax_params)
    setkeyv(dt, id_vars)
  }
  dt[, qx := qx_initial]
  dt <- recalc_beta_plus_lt_params(dt)

  # save initial ax
  dt[, ax_initial := ax]

  iter_num_3 <- 1
  done <- FALSE
  holdouts <- data.table()
  while(done == FALSE & iter_num_3 <= 6){

    scale_factor <- 1 + 0.05 * iter_num_3
    print(paste0("scaling ax by ", scale_factor))
    setkeyv(dt, id_vars)
    dt[, qx := qx_initial]
    dt <- recalc_beta_plus_lt_params(dt)
    dt[, ax := ax_initial * scale_factor]
    dt <- run_iterations(dt, holdouts, n_iterations, quiet = T, ax_scale = scale_factor)

    dt <- gen_flags(dt, id_vars)
    dt[reset_type == "none", ax_scale_factor := scale_factor]
    holdouts_master <- rbind(holdouts_master, dt[reset_type == "none" & diff <= 0.0001], fill = T)
    dt <- dt[!(reset_type == "none" & diff <= 0.0001)]
    if(nrow(dt) == 0) done <- T

    iter_num_3 <- iter_num_3 + 1

  }

  ## ITERATE SLOPE, new AX =====================================================================
  # if this hasn't worked, set ax scale factor to 30% and iterate over slope

  dt[, ax_scale_factor := 1.3]
  slope_ax_iter_results <- run_slope_iteration(dt, holdouts_master, n_iterations, ax_scale = 1.3)
  dt <- slope_ax_iter_results[[1]]
  holdouts_master <- slope_ax_iter_results[[2]]
  dt <- gen_flags(dt, id_vars)
  dt[reset_type != "none", ax_scale_factor := NA]

  ## resets as last resort ======================================================================

  dt <- rbind(dt, holdouts_master, fill = T)
  dt <- gen_flags(dt, id_vars)

  dt[reset_type == "all_term_qx_under_q90" & age >= terminal_age_start, qx := (qx + qx_initial)/2]
  dt[reset_type != "none" & reset_type != "all_term_qx_under_q90" & age >= terminal_age_start, qx := qx_initial]
  dt <- recalc_beta_plus_lt_params(dt)
  reset <- dt[reset_type != "none"]
  reset <- reset[, .SD, .SDcols = c(by_vars, "reset_type")]
  reset <- unique(reset)

  # ==============================================================================================

  # save ax adjustments
  ax_adjustments <- dt[!is.na(ax_scale_factor)]
  ax_adjustments <- ax_adjustments[, .SD, .SDcols = c(by_vars, "ax_scale_factor", "terminal_age_start")]
  ax_adjustments <- unique(ax_adjustments)

  # clear out unneeded columns
  dt<- dt[, .SD, .SDcols = c(id_vars, "mx", "ax", "qx", "lx", "dx")]

  # combine dt and reset for return
  dt_reset_list <- list(dt, reset, ax_adjustments)

  print("Iterations done")
  return(dt_reset_list)
}
