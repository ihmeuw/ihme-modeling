
reset_terminal_ages <- function(dt, reset_dt, source_ids) {

  # Prep affected source IDs ------------------------------------------------

  only_input <- fsetdiff(unique(dt[, ..source_ids]), reset_dt[, -"terminal_age_reset"])
  only_reset <- fsetdiff(reset_dt[, -"terminal_age_reset"], unique(dt[, ..source_ids]))

  # First, find source years that already have the desired terminal age from the
  # total set of source years we want to reset
  sources_to_keep <- dt[reset_dt, on = source_ids][
    is.na(age_end) & age == terminal_age_reset,
    ..source_ids
  ]

  # Second, we want to remove the source years that already have the desired
  # terminal age from the set of source years to reset, leaving only the source
  # years we want to modify
  sources_to_reset <- reset_dt[!sources_to_keep, on = source_ids]

  sources_to_reset <- dt[reset_dt, on = source_ids][
    is.na(age_end) & age != terminal_age_reset
  ]

  # Finally, split this set of source years into two group conditioned on if
  # the current terminal age in the source year needs to be increased or
  # decreased to match the desired terminal age
  dt_to_reset <- merge(
    dt,
    sources_to_reset[, c(..source_ids, "terminal_age_reset")],
    by = source_ids
  )

  source_reset_type <- dt_to_reset[
    is.na(age_end),
    .(reset_higher = terminal_age_reset > age),
    by = source_ids
  ]


  # Case when new terminal age < current ------------------------------------

  ## NOTE:
  #  When the new terminal age is *lower* than the current terminal age, simply
  #  sum all the values for age groups at or above the start of the new terminal
  #  age and set that to the new terminal value.
  dt_reset_lower <- dt_to_reset[source_reset_type[!(reset_higher)], on = source_ids]

  new_lower_terminal <- dt_reset_lower[
    age >= terminal_age_reset,
    .(value = sum(value), age = unique(terminal_age_reset), age_end = NA_integer_),
    by = c(source_ids, "measure")
  ]

  new_lower_dt <- rbindlist(list(
    dt_reset_lower[age < terminal_age_reset, -c("terminal_age_reset", "reset_higher")],
    new_lower_terminal
  ), use.names = TRUE)


  # Combine output ----------------------------------------------------------

  return_dt <- rbindlist(list(
    dt[sources_to_keep, on = source_ids],
    # new_higher_dt, TODO: add later or just fix VRP
    new_lower_dt
  ), use.names = TRUE)

  # Verify no source-years were lost (there are)
  lost_sy <- fsetdiff(unique(dt[, ..source_ids]), unique(return_dt[, ..source_ids]))

  return(return_dt)

}
