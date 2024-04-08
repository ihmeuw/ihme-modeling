#' @title Iterate terminal qx
#'
#' @description Iterate terminal qx & lx values to minimize the difference to
#'   observed terminal mx
#'
#' @param dt \[`data.table()`\]\cr
#'   Life tables with variables: 'ihme_loc_id', 'year', 'source_name', 'sex',
#'   'age', 'age_length', 'dx', 'ax', 'qx', 'terminal_age_start', 'mx_term'
#' @param ax_params \[`data.table()`\]\cr
#'   Parameters for qx --> ax conversion (originally from MLT process).
#'   Columns: 'age', 'sex', 'par_con', 'par_qx', 'par_sqx'. Regression equation
#'   is ax = par_con + par_qx * qx + par_sqx * qx^2.
#' @param id_vars \[`character()`\]\cr
#'   ID variables that uniquely identify each observation in `dt`. Must include
#'   'age'.
#' @param n_iterations \[`numeric()`\]\cr
#'   Number of iterations to run.
#'
#' @return \[`data.table()`\]\cr
#'   Life tables with age-specific old-age qx scaled to align with terminal mx.
#'
#' @export
iterate_qx <- function(dt, ax_params, id_vars, n_iterations = 50) {

  dt <- copy(dt)

  # Validate ----------------------------------------------------------------

  assertable::assert_colnames(dt, c("age_length", "dx", "ax",
                                    "qx", "mx", "lx", "terminal_age_start",
                                    "mx_term", id_vars),
                              only_colnames = FALSE)

  assertable::assert_colnames(ax_params, c("age", "sex", "par_con",
                                           "par_qx", "par_sqx"))
  assertthat::assert_that(all(c(seq(95, 105, 5)) %in% ax_params$age))

  # save information so that we can check later that no rows are dropped
  test1 <- nrow(unique(dt, by = id_vars))

  # Prep --------------------------------------------------------------------

  # sort, prep id variables
  setkeyv(dt, id_vars)
  setorderv(dt, id_vars)
  by_vars <- id_vars[id_vars != "age"]

  # generate starting ax
  dt <- gen_ax_from_qx(dt, ax_params)

  # calculate age-pattern terminal qx
  dt[
    age >= terminal_age_start,
    beta := qx / .SD[age == terminal_age_start, qx],
    by = by_vars
  ]

  assertable::assert_values(dt, "beta", test = "gte", test_val = 1, na.rm = TRUE)

  # Holdouts ----------------------------------------------------------------

  ## Main holdouts list
  #
  #   1. missing input terminal mx
  #   2. input terminal mx < 0.05 or > 2
  #   3. input terminal mx is less than last non-terminal mx
  #   4. mx is NA for ages before terminal_age_start, indicating some qx
  #      values were removed prior to qx extention
  #

  dt[is.na(mx_term), holdout := TRUE]
  dt[mx_term < 0.05, holdout := TRUE]
  dt[mx_term < .SD[age == terminal_age_start - 5, mx], holdout := TRUE, by = by_vars]
  dt[is.na(.SD[age == terminal_age_start - 5, mx]), holdout := TRUE, by = by_vars]
  dt[is.na(holdout), holdout := FALSE]

  assertable::assert_values(dt, "holdout", test = "not_na", quiet = TRUE)
  holdouts_main <- dt[(holdout)]
  holdouts_main[, terminal_scale_type := "not_attempted"]
  dt <- dt[!(holdout)]

  # Make some noise if everything is a holdout
  if (nrow(dt) == 0) {
    message("All inital data meets holdout criteria, skipping to end")
  }

  # initialize data.table to store new holdouts
  holdouts <- data.table()


  # Adjust level ------------------------------------------------------------

  ## Description
  #
  #  Optimize for all life tables, adds "qx_solution" variable.
  #
  #  Keeps age-pattern constant and solves for input terminal qx to match
  #  terminal mx.

  if (nrow(dt) > 0) {

    dt <- rbindlist(
      lapply(
        split(dt, by = by_vars),
        solve_qx,
        ax_params = ax_params
      )
    )

    solution_dt <- check_solution(dt, "qx_solution", by_vars)

    dt[
      solution_dt[solution_gte_previous & terminal_under_1],
      `:=`(holdout = TRUE, terminal_scale_type = "level_adjusted"),
      on = by_vars
    ]

  }

  holdouts <- rbind(holdouts, dt[(holdout)], fill = TRUE)
  dt <- dt[!(holdout)]


  # More holdouts -----------------------------------------------------------

  ## Description
  #
  #  We can test if slope adjustment will be successful by setting a flat
  #  slope, optimizing input terminal qx, and requiring that
  #  q_previous < q_terminal < 1.

  if (nrow(dt) > 0) {

    dt[!is.na(beta), `:=`(beta_init = beta, beta = 1)]

    dt <- rbindlist(
      lapply(
        split(dt, by = by_vars),
        solve_qx,
        ax_params = ax_params
      )
    )

    solution_dt <- check_solution(dt, "qx_solution", by_vars)

    dt[
      solution_dt[!(solution_gte_previous & solution_under_1)],
      `:=`(holdout = TRUE, terminal_scale_type = "no_solution"),
      on = by_vars
    ]

  }

  no_solution_holdouts <- dt[(holdout)]
  dt <- dt[!(holdout)]

  # Among the no solution holdouts, if the flat qx that was assigned is lower than the 
  # last non terminal qx, set the terminal qx values to the last non-terminal value
  if(nrow(no_solution_holdouts) > 0) {
    
    no_solution_holdouts[
      solution_dt[solution_gte_previous == FALSE],
      qx_dip := TRUE,
      on = by_vars
      ]
    
    last_non_terminal <- no_solution_holdouts[max_age == age]
    
    no_solution_holdouts[last_non_terminal, new_qx_solution := i.qx, on = by_vars]
    no_solution_holdouts[qx_dip == TRUE, qx_solution := new_qx_solution]
  }

  # Adjust slope ------------------------------------------------------------

  ## Description
  #
  #  Define a range of potential betas between 1 and the inital beta to
  #  iterate over for each source year.
  #
  #  Iterate over an evenly spaced geometric sequence of betas between the
  #  initial beta and 1. This has the desirable property of ensuring all betas
  #  for a source-year will converge to 1 at a rate proportional to their
  #  initial value. Moreover, the absolute difference between betas (i.e. in
  #  linear space) decreases as beta approaches 1, giving finer-grained slope
  #  tests toward the end of the iteration cycle.

  if (nrow(dt) > 0) {

    ## NOTE:
    #
    #  Make the length of the sequence of betas one greater than the desired
    #  number of iteration, so that beta = 1 is never tried.

    dt[, beta_ratio := -log(beta_init) / (n_iterations + 1)]

    for (iter in 1:n_iterations) {

      print(paste0(
        "Slope iteration ", iter, "/", n_iterations, ": ",
        nrow(unique(dt[, ..by_vars])), " remaining unsolved groups"
      ))

      dt[, beta := beta_init * exp(beta_ratio * iter)]

      # solve for input terminal qx with new betas
      dt <- rbindlist(
        lapply(
          split(dt, by = by_vars),
          solve_qx,
          ax_params = ax_params
        )
      )
      
      solution_dt <- check_solution(dt, "qx_solution", by_vars)
      
      dt[
        solution_dt[solution_gte_previous & terminal_under_1],
        `:=`(holdout = TRUE, terminal_scale_type = "slope_adjusted"),
        on = by_vars
        ]
      
      holdouts <- rbind(holdouts, dt[(holdout)], fill = TRUE)
      dt <- dt[!(holdout)]
      
      # Exit loop early if all sources have a solution
      if (nrow(dt) == 0) { break }
    }
    
    if (nrow(dt) > 0) {
      print(paste0(nrow(unique(dt[, ..by_vars])), " unsolved groups from slope iteration"))
    } else {
      print("All slope iteration groups solved")
    }
    
  }
  
  # Adjust high terminal mx life tables -------------------------------------
  # If the qx solution is above 1, double the unscaled logit qx
  # in the terminal ages (where mx is na)
  
  if (nrow(no_solution_holdouts) > 0) {
    no_solution_holdouts[is.na(mx) & qx_solution > 1, logit_qx := logit_qx * 2]
    no_solution_holdouts[qx_solution > 1, terminal_scale_type := "double_unscaled"]
    no_solution_holdouts[terminal_scale_type == "double_unscaled", qx_solution := invlogit(logit_qx)]
  }
  
  
  # Perform scaling ---------------------------------------------------------

  # everything in `holdouts` now has a solution
  dt_solved <- rbind(
    holdouts,
    no_solution_holdouts,
    use.names = TRUE,
    fill = TRUE
  )

  # predict scaling w/ terminal qx solution
  dt_solved[age >= terminal_age_start, qx := beta * qx_solution, by = by_vars]

  # calculate ax, lx, mx, dx
  dt_solved <- gen_ax_from_qx(dt_solved, ax_params)
  dt_solved[, mx := ltcore::qx_ax_to_mx(qx, ax, age_length)]
  setkeyv(dt_solved, id_vars)
  dt_solved[, lx := cumprod(shift(1 - qx, type = "lag", fill = 1))]
  dt_solved[, dx := lx * qx]


  # Clean up and return -----------------------------------------------------

  # label remaining cases, where no solution was found
  dt[, terminal_scale_type := "no_solution"]

  # combine all holdouts
  dt <- rbind(dt, dt_solved, holdouts_main, fill = TRUE)

  # save info about scaling success/failure/type
  reset <- unique(dt[, .SD, .SDcols = c(by_vars, "terminal_scale_type")])

  # clear out unneeded columns
  dt <- dt[, .SD, .SDcols = c(id_vars, "mx", "ax", "qx", "lx", "dx")]

  # combine dt and reset for return
  dt_reset_list <- list(dt, reset)

  # testing that no rows are dropped
  test2 <- nrow(unique(dt, by = id_vars))
  assertthat::assert_that(
    test1 == test2,
    msg = paste0("Terminal scaling started with ", test1, " rows, and ",
                 "ended with ", test2, " rows. Check function.")
  )

  return(dt_reset_list)
}


# Helper functions --------------------------------------------------------

solve_root <- function (q_test, dt, ax_params) {

  dt <- copy(dt)

  # Modify post-terminal qx values with test qx
  dt[age >= terminal_age_start, qx := q_test * beta]

  # Recalculate life table
  dt <- gen_ax_from_qx(dt, ax_params)
  
  #fill in missing ax values
  dt[is.na(ax) & age >= 60, ax := 2.5]
  
  setkey(dt, "age")
  dt[, lx := cumprod(shift(1 - qx, type = "lag", fill = 1))]
  dt[, nLx := 5 * lx * (1 - qx) + (ax * qx * lx)]
  dt[, Tx := rev(cumsum(rev(nLx)))]

  # calculate est of input terminal mx
  dt[age == terminal_age_start, mx := lx / Tx]

  # return what we want to equal zero
  zero <- dt[age == terminal_age_start, mx_term - mx]
  return(zero)

}


#' Solve for optimal input terminal qx
#'
#' Optimize input terminal qx for one life table based on given terminal mx and
#' age pattern.
#'
#' @param dt \[`data.table()`\]\cr
#'   A single life table to optimize.
#' @param ax_params \[`data.table()`\]\cr
#'   Table of ax regression parameters for caclulating ax.
#'
#' @return \[`data.table()`\]\cr
#'   A copy of `dt` with a new column appended with the qx solution.
#'
solve_qx <- function (dt, ax_params) {

  dt <- copy(dt)

  # check that we have one life table
  assertthat::assert_that(dt[age == terminal_age_start, .N] == 1)

  # optimize
  solution <- tryCatch({
    stats::uniroot(f = solve_root, dt = dt, ax_params = ax_params, interval = c(0, 1))
  }, error = function(e) {
    # purposely return bad qx so later steps will run
    list(root = 2)
  })

  dt[, qx_solution := solution$root]
  return(dt)
}


gen_ax_from_qx <- function(dt, ax_params) {

  # prep and merge on regression parameters
  dt <- copy(dt)
  dt <- merge(dt, ax_params, by = c("sex", "age"), all.x = TRUE)

  # predict out regression equation to get ax
  dt[age >= terminal_age_start, ax := par_qx * qx + par_sqx * (qx^2) + par_con]

  # clean and return
  dt[, c("par_qx", "par_sqx", "par_con") := NULL]
  return(dt)
}


#' Check qx Optimization Solution
#'
#' Create a table checking different optimization solution conditions for each
#' grouping in the data.
#'
#' The conditions checked are:
#'
#'  1) Is the solution qx >= the qx of the preceding age group?
#'  2) Is the solution qx < 1?
#'  3) Is the implied terminal qx < 1?
#'
#' @param dt \[`data.table()`\]\cr
#'   Collection of life tables from a single location, with columns representing
#'   the betas and optomized qx solution.
#' @param solution_qx_col \[`character(1)`\]\cr
#'   Column name of `dt` containing the optimized qx solution. There is expected
#'   to be only one unique value by group.
#' @param by_vars \[`character()`\]\cr
#'   Column names in `dt` uniquely identifying a life table.
#'
#' @return \[`data.table()`\]\cr
#'   Table including `by_vars` and columns for each condition checked.
check_solution <- function(dt, solution_qx_col, by_vars) {

  dt_solution <- dt[
    ,
    .(
      qx_solution = unique(get(solution_qx_col)),
      qx_previous = .SD[age == terminal_age_start - 5, qx],
      beta_terminal = .SD[age == 105, beta]
    ),
    by = by_vars
  ]

  dt_solution[, `:=`(
    solution_gte_previous = qx_solution >= qx_previous,
    solution_under_1 = qx_solution < 1,
    terminal_under_1 = qx_solution * beta_terminal < 1
  )]

  return(dt_solution)

}
