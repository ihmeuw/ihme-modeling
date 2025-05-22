ccmpp_female_male <- function(baseline, surv, mig, fert, srb, migration_type = "proportion",
                              years, age_int = 5, value_name = "mean") {

  require(assertable)

  ## setting the number of age groups and the number of projection/backcasting steps
  ages <- unique(baseline$age_group_years_start)
  terminal_age_group <- max(ages)
  n_age_grps <- length(ages)

  last_year <- max(years) + age_int
  proj_steps <- (last_year - min(years)) / age_int

  # set id vars for assertion check later
  id_vars_baseline <- list(year_id = min(years),
                           sex_id = 1:2,
                           age_group_years_start = ages)
  id_vars_surv <- list(year_id = years,
                       sex_id = 1:2,
                       age_group_years_start = c(-1, ages))
  id_vars_mig <- list(year_id = years,
                      sex_id = 1:2,
                      age_group_years_start = c(-1, ages))
  id_vars_fert <- list(year_id = years,
                       age_group_years_start = ages)
  id_vars_srb <- list(year_id = years)
  assertable::assert_ids(baseline, id_vars = id_vars_baseline, quiet = T)
  assertable::assert_ids(surv, id_vars = id_vars_surv, quiet = T)
  assertable::assert_ids(mig, id_vars = id_vars_mig, quiet = T)
  assertable::assert_ids(fert, id_vars = id_vars_fert, quiet = T)
  assertable::assert_ids(srb, id_vars = id_vars_srb, quiet = T)

  # convert data tables to matrices with year wide, age as rownames
  baseline_matrix <- list("male" = matrix_w_rownames(dcast(baseline[sex_id == 1], age_group_years_start ~ year_id, value.var=value_name)),
                          "female" = matrix_w_rownames(dcast(baseline[sex_id == 2], age_group_years_start ~ year_id, value.var=value_name)))
  surv_matrix <- list("male" = matrix_w_rownames(dcast(surv[sex_id == 1], age_group_years_start ~ year_id, value.var=value_name)),
                      "female" = matrix_w_rownames(dcast(surv[sex_id == 2], age_group_years_start ~ year_id, value.var=value_name)))
  mig_matrix <- list("male" = matrix_w_rownames(dcast(mig[sex_id == 1], age_group_years_start ~ year_id, value.var=value_name)),
                     "female" = matrix_w_rownames(dcast(mig[sex_id == 2], age_group_years_start ~ year_id, value.var=value_name)))
  fert_matrix <- matrix_w_rownames(dcast(fert, age_group_years_start ~ year_id, value.var=value_name))
  srb_matrix <- matrix(srb[[value_name]], ncol=nrow(srb))

  # initialize population matrix
  pop_mat <- list("male" = matrix(0, nrow = n_age_grps, ncol = 1 + proj_steps),
                  "female" = matrix(0, nrow = n_age_grps, ncol = 1 + proj_steps))
  pop_mat[["male"]][, 1] <- baseline_matrix[["male"]]
  pop_mat[["female"]][, 1] <- baseline_matrix[["female"]]

  births_mat <- list("male" = matrix(0, nrow = n_age_grps, ncol = proj_steps),
                     "female" = matrix(0, nrow = n_age_grps, ncol = proj_steps))
  deaths_mat <- list("male" = matrix(0, nrow = n_age_grps + 1, ncol = proj_steps),
                     "female" = matrix(0, nrow = n_age_grps + 1, ncol = proj_steps))
  net_migrants_mat <- list("male" = matrix(0, nrow = n_age_grps + 1, ncol = proj_steps),
                           "female" = matrix(0, nrow = n_age_grps + 1, ncol = proj_steps))
  total_births_mat <- list("male" = matrix(0, nrow = 1, ncol = proj_steps),
                           "female" = matrix(0, nrow = 1, ncol = proj_steps))

  # project population forward
  for(i in 1:proj_steps) {

    # Calculate the net number of migrants that occur in a cohort -------------

    # if using proportions, convert to counts
    if (migration_type == "proportion") {
      # okay to not account for the migrants of babies yet since they will not be used in asfr * pop calculations
      # cohort migration rate times population at the beginning of the projection period
      half_net_migrants_cohort <- list("male" = mig_matrix[["male"]][, i] * c(0, pop_mat[["male"]][, i]) * 0.5 * age_int,
                                       "female" = mig_matrix[["female"]][, i] * c(0, pop_mat[["female"]][, i]) * 0.5 * age_int)
    } else if (migration_type == "count") {
      # already given the number of migrants in each cohort (parallelogram)
      half_net_migrants_cohort <- list("male" = mig_matrix[["male"]][, i] * 0.5,
                                       "female" = mig_matrix[["female"]][, i] * 0.5)
    }

    # to be added on at the beginning of each projection interval
    half_net_migrants_cohort_beginning <- list("male" = half_net_migrants_cohort[["male"]][2:(n_age_grps + 1)],
                                               "female" = half_net_migrants_cohort[["female"]][2:(n_age_grps + 1)])

    # to be added on after projecting to the end of the interval
    half_net_migrants_cohort_end <- list("male" = half_net_migrants_cohort[["male"]][1:(n_age_grps)],
                                         "female" = half_net_migrants_cohort[["female"]][1:(n_age_grps)])
    # the terminal age group and the one below need to be added on to the terminal age group population of the next projection period
    half_net_migrants_cohort_end[["male"]][n_age_grps] <- half_net_migrants_cohort_end[["male"]][n_age_grps] + half_net_migrants_cohort[["male"]][n_age_grps + 1]
    half_net_migrants_cohort_end[["female"]][n_age_grps] <- half_net_migrants_cohort_end[["female"]][n_age_grps] + half_net_migrants_cohort[["female"]][n_age_grps + 1]
    # need to add one baby migration later since when given migration proportions, this depends on the number of births
    half_net_migrants_cohort_end[["male"]][1] <- 0
    half_net_migrants_cohort_end[["female"]][1] <- 0

    # Project the population forward once -------------------------------------

    ## project population forward one projection period
    leslie <- list("male" = make_leslie_matrix(fert_matrix[, i], surv_matrix[["male"]][, i], srb=srb_matrix[, i], age_int=age_int, female=F),
                   "female" = make_leslie_matrix(fert_matrix[, i], surv_matrix[["female"]][, i], srb=srb_matrix[, i], age_int=age_int, female=T))
    pop_mat[["male"]][, i + 1] <- leslie[["male"]] %*% (pop_mat[["male"]][,i] + half_net_migrants_cohort_beginning[["male"]]) + half_net_migrants_cohort_end[["male"]]
    pop_mat[["female"]][, i + 1] <- leslie[["female"]] %*% (pop_mat[["female"]][,i] + half_net_migrants_cohort_beginning[["female"]]) + half_net_migrants_cohort_end[["female"]]

    # total births calculated in the first row of the female matrix, need to split into sex specific births
    pop_mat[["male"]][1, i + 1] <- pop_mat[["female"]][1, i + 1] * (srb_matrix[, i] / (1 + srb_matrix[, i]))
    pop_mat[["female"]][1, i + 1] <- pop_mat[["female"]][1, i + 1] * (1 / (1 + srb_matrix[, i]))

    # use births to calculate the number of baby migrants.
    # if given in counts already have migrants for the baby cohort
    if (migration_type == "proportion") {
      half_net_migrants_cohort[["male"]][1] <- mig_matrix[["male"]][1, i] * pop_mat[["male"]][1, i + 1] * 0.5
      half_net_migrants_cohort[["female"]][1] <- mig_matrix[["female"]][1, i] * pop_mat[["female"]][1, i + 1] * 0.5
    }

    # need to add on migrating babies and apply survival ratios to get the population in this first age group
    pop_mat[["male"]][1, i + 1] <- ((pop_mat[["male"]][1, i + 1] + half_net_migrants_cohort[["male"]][1]) * surv_matrix[["male"]][1, i]) + half_net_migrants_cohort[["male"]][1]
    pop_mat[["female"]][1, i + 1] <- ((pop_mat[["female"]][1, i + 1] + half_net_migrants_cohort[["female"]][1]) * surv_matrix[["female"]][1, i]) + half_net_migrants_cohort[["female"]][1]


    # Calculate other consistent counts ---------------------------------------

    # assign migrant totals to age group the cohort started in
    net_migrants_mat[["male"]][, i] <- half_net_migrants_cohort[["male"]] + half_net_migrants_cohort[["male"]]
    net_migrants_mat[["female"]][, i] <- half_net_migrants_cohort[["female"]] + half_net_migrants_cohort[["female"]]

    ## calculate births before accounting for deaths or migrants of births, only accounts for migrants of potential mothers
    leslie_births <- make_births_leslie_matrix(fert_matrix[, i], surv_matrix[["female"]][, i], age_int=age_int)
    births_mat[["male"]][, i] <- leslie_births %*% (pop_mat[["female"]][,i] + half_net_migrants_cohort_beginning[["female"]]) * (srb_matrix[, i] / (1 + srb_matrix[, i]))
    births_mat[["female"]][, i] <- leslie_births %*% (pop_mat[["female"]][,i] + half_net_migrants_cohort_beginning[["female"]]) * (1 / (1 + srb_matrix[, i]))
    total_births_mat[["male"]][, i] <- sum(births_mat[["male"]][, i])
    total_births_mat[["female"]][, i] <- sum(births_mat[["female"]][, i])

    ## calculate deaths and assign to age group the cohort started in
    leslie_deaths <- list("male" = make_deaths_leslie_matrix(fert_matrix[, i], surv_matrix[["male"]][, i]),
                          "female" = make_deaths_leslie_matrix(fert_matrix[, i], surv_matrix[["female"]][, i]))
    deaths_mat[["male"]][2:(n_age_grps + 1), i] <- leslie_deaths[["male"]] %*% (pop_mat[["male"]][,i] + half_net_migrants_cohort_beginning[["male"]])
    deaths_mat[["female"]][2:(n_age_grps + 1), i] <- leslie_deaths[["female"]] %*% (pop_mat[["female"]][,i] + half_net_migrants_cohort_beginning[["female"]])

    # add on deaths to youngest age group (newborns)
    deaths_mat[["male"]][1, i] <- (1 - surv_matrix[["male"]][1, i]) * (total_births_mat[["male"]][i] + half_net_migrants_cohort[["male"]][1])
    deaths_mat[["female"]][1, i] <- (1 - surv_matrix[["female"]][1, i]) * (total_births_mat[["female"]][i] + half_net_migrants_cohort[["female"]][1])

  }

  # convert to data table
  melt_matrix <- function(mat, ages, years, varaible_name) {
    data <- lapply(1:2, function(s) {
      colnames(mat[[s]]) <- years
      d <- data.table(mat[[s]])
      d[, age_group_years_start := ages]
      d[, sex_id := s]
      d <- melt(d, id.vars=c("sex_id", "age_group_years_start"), variable.name="year_id", value.name=value_name)
      d[, year_id := as.integer(as.character(year_id))]
      d[, variable := varaible_name]
      setcolorder(d, c("year_id", "sex_id", "age_group_years_start", "variable", value_name))
      return(d)
    })
    data <- rbindlist(data)
  }

  # convert back to data.tables and bind together
  pop <- melt_matrix(pop_mat, ages, years = c(years, last_year), "population")
  births <- melt_matrix(births_mat, ages, years, "births")
  deaths <- melt_matrix(deaths_mat, c(-age_int, ages), years, "deaths_cohort")
  net_migrants <- melt_matrix(net_migrants_mat, c(-age_int, ages), years, "net_migrants_cohort")
  result <- rbind(pop, deaths, births, net_migrants)

  result[, year_id := as.integer(year_id)]
  result[, sex_id := as.integer(sex_id)]
  result[, age_group_years_start := as.integer(age_group_years_start)]
  setkeyv(result, c("year_id", "sex_id", "age_group_years_start", "variable"))


  # Checks for various measures of consistency ------------------------------

  # check for total population consistency
  check_result_total <- result[, list(value = sum(get(value_name))), by = c("year_id", "variable")]
  check_result_total <- dcast(check_result_total, formula = year_id ~ variable, value.var = "value")
  check_result_total[, sum_components := births - deaths_cohort + net_migrants_cohort]
  check_result_total[, delta_population := c(diff(population), NA)]
  check_result_total[, difference := abs(sum_components - delta_population)]
  if (nrow(check_result_total[difference > 1]) > 0) stop("inconsistent at the total population level")

  # check births and initial cohort size
  check_births <- result[variable == "births"]
  check_births <- check_births[, list(age_group_years_start = -age_int, variable = "births", value = sum(get(value_name))), by = c("year_id", "sex_id")]
  check_others <- result[(variable %in% c("deaths_cohort", "net_migrants_cohort") & age_group_years_start == -age_int) | (variable == "population" & age_group_years_start == 0)]
  setnames(check_others, value_name, "value")
  check_births <- rbind(check_births, check_others)
  check_births[, age_group_years_start := NULL]
  check_births <- dcast(check_births, formula = year_id + sex_id ~ variable, value.var = "value")
  check_births[, sum_components := births - deaths_cohort + net_migrants_cohort]
  check_births[, next_cohort_initial_size := shift(population, type = "lead", fill = NA),
               by = c("sex_id")]
  check_births[, difference := abs(sum_components - next_cohort_initial_size)]
  if (nrow(check_births[difference > 1]) > 0) stop("inconsistent initial cohort sizes")

  # check for cohort consistency over time
  check_initial_cohort_size <- result[variable == "births"]
  check_initial_cohort_size <- check_initial_cohort_size[, list(age_group_years_start = -age_int, variable = "population",
                                                                value = sum(get(value_name))),
                                                         by = c("year_id", "sex_id")]
  check_result <- result[variable != "births"]
  setnames(check_result, value_name, "value")
  check_result <- rbind(check_initial_cohort_size, check_result)
  check_result[, cohort := year_id - age_group_years_start]
  setkeyv(check_result, c("cohort", "year_id", "sex_id", "age_group_years_start", "variable"))
  check_result <- check_result[age_group_years_start != terminal_age_group] # terminal age group mixes cohort and period checks
  check_result <- dcast(check_result, formula = cohort + year_id + sex_id + age_group_years_start ~ variable, value.var = "value")
  check_result[, sum_components := net_migrants_cohort - deaths_cohort]
  check_result[, delta_population := c(diff(population), NA), by = c("cohort", "sex_id")]
  check_result[, difference := abs(sum_components - delta_population)]
  if (nrow(check_result[difference > 1]) > 0) stop("results do not satisfy age specific demographic balancing equation")

  # check the terminal age group population size
  check_terminal <- result[age_group_years_start >= (terminal_age_group - age_int) & variable != "births"]
  check_result <- check_terminal[sex_id == 1]
  check_result <- dcast(check_result, formula = year_id + sex_id + age_group_years_start ~ variable, value.var = value_name)
  setkeyv(check_result, c("year_id", "sex_id", "age_group_years_start"))
  check_result <- check_result[, list(deaths_cohort = sum(deaths_cohort), net_migrants_cohort = sum(net_migrants_cohort), population = sum(population), terminal_population = population[2]), by = c("year_id", "sex_id")]
  check_result <- check_result[, terminal_population_next_interval := shift(terminal_population, type = "lead"), by = "sex_id"]
  check_result[, sum_components := population + net_migrants_cohort - deaths_cohort]
  check_result[, difference := abs(sum_components - terminal_population_next_interval)]
  if (nrow(check_result[difference > 1]) > 0) stop("terminal age group population does not change as expected")

  return(result)
}

make_leslie_matrix <- function(fert, surv, srb = 1.05, age_int = 5, female = T) {
  # Create basic leslie matrix
  #
  # Args:
  #   fert: vector of fertility rates for A age groups for one year
  #   surv: vector of survival rates for A + 1 age groups for one year
  #   srb: sex ratio at birth for that year
  #   age_int: age interval
  #   female: leslie matrix for female matrix
  # Returns:
  #   leslie matrix

  n_fert <- length(fert)
  n_surv <- length(surv)
  if (n_surv != n_fert + 1) stop("Lengths of fertility and survival vectors are not compatible, survival should have one more age group")

  # Initialize Leslie matrix
  leslie <- matrix(0, nrow = n_fert, ncol = n_fert)

  # fill fertility to calculate total number of male and female births
  if (female) leslie[1,] <- age_int * (fert + (c(fert[-1], 0) * surv[-1])) * 0.5

  # fill survival
  leslie[2:n_fert, 1:(n_fert - 1)] <- diag(surv[2:n_fert])
  leslie[n_fert, n_fert] <- surv[n_surv]

  # add dimension names
  dimnames(leslie) <- list(names(fert), names(fert))
  return(leslie)
}

make_deaths_leslie_matrix <- function(fert, surv) {
  # Create modified leslie matrix to calculate number of deaths that occur in each age group
  #
  # Args:
  #   fert: vector of fertility rates for A age groups for one year
  #   surv: vector of survival rates for A + 1 age groups for one year
  # Returns:
  #   leslie matrix

  n_fert <- length(fert)
  n_surv <- length(surv)
  if (n_surv != n_fert + 1) stop("Lengths of fertility and survival vectors are not compatible, survival should have one more age group")

  # Initialize Leslie matrix
  leslie <- matrix(0, nrow = n_fert, ncol = n_fert)
  death_ratio <- 1 - surv

  # fill survival
  leslie[1:n_fert, 1:n_fert] <- diag(death_ratio[2:(n_fert + 1)])

  # add dimension names
  dimnames(leslie) <- list(names(fert), names(fert))
  return(leslie)
}

make_births_leslie_matrix <- function(fert, surv, age_int = 5) {
  # Create modified leslie matrix to calculate number of births
  #
  # Args:
  #   fert: vector of fertility rates for A age groups for one year
  #   surv: vector of survival rates for A + 1 age groups for one year
  #   age_int: age interval
  # Returns:
  #   leslie matrix

  n_fert <- length(fert)
  n_surv <- length(surv)
  if (n_surv != n_fert + 1) stop("Lengths of fertility and survival vectors are not compatible, survival should have one more age group")

  # Initialize Leslie matrix
  leslie <- matrix(0, nrow = n_fert, ncol = n_fert)

  dbl_fert <- age_int * (fert + (c(fert[-1], 0) * surv[-1])) * 0.5
  leslie[1:n_fert, 1:n_fert] <- diag(dbl_fert)

  # add dimension names
  dimnames(leslie) <- list(names(fert), names(fert))
  return(leslie)
}

matrix_w_rownames <- function(data) {
  # Convert a data.table to matrix with the first column as the rownames
  #
  # Args:
  #   data: data.table with age in the first column and years after that
  # Returns:
  #   matrix with age column as row names

  m <- as.matrix(data[, names(data)[-1], with=F])
  rownames(m) <- data[[1]]
  return(m)
}

# Switch from cohort to period counts. Assumes that we should split these counts
# in half between the starting age group of the cohort and the age group the cohort
# ends up in.
cohort_to_period_counts <- function(data, id_vars, use_age_int, value_name = "mean") {

  period_data <- copy(data)
  setnames(period_data, value_name, "value")
  data_adjust <- period_data[between(age_group_years_start, 0, (terminal_age - use_age_int))]
  period_data <- period_data[!between(age_group_years_start, 0, (terminal_age - use_age_int))]

  # split counts in half between the starting cohort age and the next age group
  data_adjust <- data_adjust[, list(period_age = c(age_group_years_start, age_group_years_start + use_age_int), value = value / 2), by = id_vars]
  data_adjust[, age_group_years_start := NULL]
  setnames(data_adjust, "period_age", "age_group_years_start")

  # assign all birth counts to the first age group
  period_data[age_group_years_start == -use_age_int, age_group_years_start := 0]

  period_data <- rbind(period_data, data_adjust)
  period_data <- period_data[, list(value = sum(value)), keyby = id_vars]
  setnames(period_data, "value", value_name)

  return(period_data)
}

