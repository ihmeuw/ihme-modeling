#' Abridge life table
#'
#' Calculate life tables for abridged age groups from more granular age groups.
#'
#' @param lt Table of life tables to abridge with all parameters and population.
#' @param age_map_abridged Table of age groups to calculate for the returned
#'  abridged life tables.
#' @param id_cols Columns of `lt` distinguishing life tables. Must also include
#'   `age_start` and `age_end`.
#' @param require_terminal Logical value determining if a terminal age group
#'   needs to be present in the abridged age map. `TRUE` by default.
#'
#' @return Table of life tables abridged from `lt` to the age groups present in
#'   `age_map_abridged`.
#'
#' @details # Abridged Age Group Alignment
#'
#' This function is intended to take in "canonical life tables" with detailed
#' under 1 age groups and single year age groups above 1 and output life tables
#' with the standard GBD age groups. This ensures that the boundaries of the
#' abridged age groups always align with the input age group boundaries.
#'
#' Put another way, an abridged age group must completely span a continuous
#' and distinct subset of the input life table age groups.
#'
#' @details # Life Table Parameter Abridging Strategies
#'
#' Life table parameters \eqn{l_x}{lx}, \eqn{T_x}{Tx}, and \eqn{e_x}{ex} are
#' defined not for an age group but a specific starting age, and are equivalent
#' between the input and abridged life table.
#'
#' \eqn{q_x}{qx} can be abridged by calculating 1 minus the product of
#' \eqn{p_x}{px} values.
#'
#' \eqn{m_x}{mx} can be abridged using compatible person-years (or population)
#' values to calculate the sum of deaths.
#'
#' \eqn{a_x}{ax} is abridged using the formula:
#'
#' \deqn{a_{x'} = \frac{\sum_{x}(D_x * (a_x + x - x'))}{\sum_{x}(D_x)}}
#'
#' or, in words: the sum of deaths times \eqn{a_x}{ax} plus full years lived
#' in the abridged age group, over the sum of deaths.
#'
#' For the terminal age, standard life table parameter calculation are used:
#'
#' - \eqn{m_x = 1 / e_x}{mx = 1 / ex}
#' - \eqn{a_x = e_x}{ax = ex}
#' - \eqn{q_x = 1}{qx = 1}
#' - \eqn{_{n}L_x = l_x / m_x}{nLx = lx / mx}
#'
#' @export
abridge_lt <- function(lt, age_map_abridged, id_cols, require_terminal = TRUE) {

  age_cols <- c("age_start", "age_end")
  lt_param_cols <- c("mx", "ax", "qx", "lx", "dx", "nLx", "Tx", "ex")

  stopifnot(
    all(age_cols %in% id_cols),
    all(id_cols %in% names(lt)),
    all(c(lt_param_cols, "pop") %in% names(lt))
  )

  id_cols_abr <- c(setdiff(id_cols, age_cols), "age_start_abr")

  term_age_abridged <- age_map_abridged[is.infinite(age_end), age_start]
  if (length(term_age_abridged) != 1) {
    require_terminal && stop("A single terminal age was not found in `age_map_abridged")
    term_age_abridged <- Inf
  }

  lt[age_map_abridged, age_start_abr := i.age_start, on = .(age_start >= age_start)]

  # Separate non-abridged ages ----

  lt_nonabridged <- lt[
    age_map_abridged[, ..age_cols],
    -c(..age_cols, "dx", "pop"),
    on = age_cols,
    nomatch = NULL
  ]

  # Abridge non-terminal ages ----

  lt_abridged <- lt[age_end <= term_age_abridged][
    !age_map_abridged[, ..age_cols],
    on = age_cols
  ]
  lt_abridged[, deaths := mx * pop]

  # Some life table parameters are defined entirely be the start of the age
  # group, we use these as-is for the new abridged ages
  lt_abridged_start <- lt_abridged[
    age_start == age_start_abr,
    c(..id_cols_abr, "lx", "Tx", "ex")
  ]

  # Other life table parameters can be abridged directly, or through some
  # transformation
  lt_abridged <- lt_abridged[
    j = .(
      mx = sum(deaths) / sum(pop),
      ax = sum(deaths * (ax + age_start - age_start_abr)) / sum(deaths),
      qx = 1 - prod(1 - qx),
      nLx = sum(nLx)
    ),
    by = id_cols_abr
  ]

  lt_abridged[
    lt_abridged_start,
    c("lx", "Tx", "ex") := list(lx, Tx, ex),
    on = id_cols_abr
  ]

  # Abridge terminal age ----

  lt_abridged_term <- lt[
    age_start == term_age_abridged,
    c(..id_cols_abr, c("lx", "Tx", "ex"))
  ]

  lt_abridged_term[, `:=`(
    mx = 1 / ex,
    ax = ex,
    qx = 1
  )]

  lt_abridged_term[, nLx := lx / mx]

  # Combine ----

  lt_final <- rbindlist(
    list(lt_nonabridged, lt_abridged, lt_abridged_term),
    use.names = TRUE
  )
  setnames(lt_final, "age_start_abr", "age_start")
  lt_final[age_map_abridged, age_end := i.age_end, on = "age_start"]
  setcolorder(lt_final, c(id_cols, setdiff(lt_param_cols, "dx")))

  # Return ----

  lt[, age_start_abr := NULL]
  return(lt_final)

}
