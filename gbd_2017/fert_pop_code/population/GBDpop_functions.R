#' Calculates age group variables 'age_end' and 'n' given a data.table with a column for id_vars, 'age_start'.
#' For the terminal age group, 'age_end' = NA, 'n' = 0.
#' For unknown age group, 'age_end' = -1, 'n' = -1.
#'
#' @param censuses data.table for a given census that includes a column for id_vars, 'age_start'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return data.table for a given census that includes columns for 'age_start', 'age_end', 'n'.
#'
#' @export
#' @import data.table
calculate_full_age_groups <- function(censuses, id_vars) {

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_start") %in% names(censuses))) stop("'id_vars', 'age_start' column not in data.table")

  setorderv(censuses, c(id_vars, "age_start"))

  censuses[, n := c(diff(age_start), NA), by = id_vars]
  censuses[, age_end := age_start + n - 1]

  # manual corrections for how we format things
  censuses[, terminal_age_group := ifelse(age_start == max(age_start), T, F), by = id_vars]
  censuses[terminal_age_group == T, age_end := NA]
  censuses[terminal_age_group == T, n := 0]
  censuses[, terminal_age_group := NULL]

  censuses[age_start == -1, age_end := -1]
  censuses[age_start == -1, n := 0]

  return(censuses)
}

#' Collapses census counts to a lower terminal age group if possible.
#'
#' @param census data.table for a given census that includes a column for 'age_start', 'pop'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @param terminal_age terminal age to collapse the data to.
#' @return census data.table with age groups collapsed down if possible
#'
#' @export
#' @import data.table
collapse_terminal_age <- function(census, id_vars, terminal_age = 95) {

  # initial checks
  if(!any(class(census) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_start", "pop") %in% names(census))) stop("'id_vars', 'age_start', 'pop' columns not in data.table")

  # collapse to terminal age group or maximum age in the data
  census[, collapse_terminal_age := max(age_start[age_start <= terminal_age]), by = id_vars] # must be less than intended terminal age
  census[age_start > collapse_terminal_age, age_start := collapse_terminal_age]
  census <- census[, list(pop = sum(pop)), by = c(id_vars, "age_start")]

  return(census)
}

#' Collapse census age groups to given set of age groups.
#'
#' @param censuses data.table for a given census that includes a column for id_vars, 'age_start', 'pop'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return census data.table with age groups collapsed down if possible
#'
#' @export
#' @import data.table
collapse_age_groups <- function(censuses, age_groups, id_vars) {

  censuses <- copy(censuses)

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_start", "pop") %in% names(censuses))) stop("'id_vars', 'age_start', 'pop' columns not in data.table")

  # must take intersection of age groups and each censuses age groups in order to avoid
  # rounding down age groups (like from 7 to 5) to include ages they shouldn't
  censuses[, age_start := cut(age_start,
                              breaks = c(intersect(age_start, age_groups), Inf),
                              labels = intersect(age_start, age_groups), right = F),
           by = id_vars]
  censuses[, age_start := as.integer(as.character(age_start))]
  censuses <- censuses[, list(pop = sum(pop)), by = c(id_vars, "age_start")]
  return(censuses)
}

#' Find the common set of age groups in a set of census counts
#'
#' @param censuses data.table for a given census that includes a column for id_vars, 'age_start'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return census data.table with age groups collapsed down if possible
#'
#' @export
#' @import data.table
find_common_age_groups <- function(censuses, id_vars) {

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_start") %in% names(censuses))) stop("'id_vars', 'age_start' columns not in data.table")

  # collapse across id vars to determine age starts for each census
  age_groups <- censuses[, list(age_start = list(unique(age_start))), by = id_vars]

  # find common age starts across all censuses
  common_age_groups <- Reduce(intersect, age_groups$age_start)
  return(common_age_groups)
}


#' Distributes counts of unknown sex proportionally by age group. If no counts of known sex available, distribute 50/50. Unknown sex value assumed to be -1.
#'
#' @param censuses data.table for censuses that includes columns for 'sex_id', 'age_start', 'pop'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return data.table for censuses that includes columns for 'id_vars', 'sex_id', 'age_start', 'pop'.
#'
#' @export
#' @import data.table
distribute_unknown_sex <- function(censuses, id_vars) {

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "sex_id", "age_start", "pop") %in% names(censuses))) stop("'id_vars', 'sex_id', 'age_start', 'pop' columns not in data.table")

  original_total_pop <- sum(as.numeric(censuses$pop))

  # split out the tabulations of unknown sex
  unknown_sex_id <- censuses[sex_id == -1 & pop > 0]
  unknown_sex_id[, sex_id := NULL]
  censuses <- censuses[sex_id != -1]

  # calculate sex proportions by age group
  proportions <- copy(censuses)
  proportions[, total_pop := sum(pop), by = c(id_vars, "age_start")]
  proportions[, prop := pop / total_pop]
  proportions[, c("pop", "total_pop") := NULL]

  # merge proportions onto the data set of tabulations of unknown sex
  unknown_sex_id <- merge(unknown_sex_id, proportions, by = c(id_vars, "age_start"), all.x = T, allow.cartesian = T)

  # some ages don't have any one of known sex, so just split evenly between male and female
  still_unknown_males <- unknown_sex_id[is.na(prop)]
  still_unknown_females <- unknown_sex_id[is.na(prop)]
  still_unknown_males[, c("sex_id", "prop") := list(1L, 0.5)]
  still_unknown_females[, c("sex_id", "prop") := list(2L, 0.5)]
  unknown_sex_id <- unknown_sex_id[!is.na(prop)]
  unknown_sex_id <- rbind(unknown_sex_id, still_unknown_males, still_unknown_females)

  # split population proportionally
  unknown_sex_id[, pop := pop * prop]
  unknown_sex_id[, prop := NULL]

  # combine data back together
  censuses <- rbind(censuses, unknown_sex_id, use.names=T)
  censuses <- censuses[, list(pop = sum(as.numeric(pop))), by = c(id_vars, "sex_id", "age_start")]

  # final checks
  if ((original_total_pop - sum(censuses$pop)) > 0.01) stop("population doesn't match before and after distribution")
  assertable::assert_values(censuses, colnames="sex_id", test="in", test_val = c(1, 2), quiet = T)

  return(censuses)
}


#' Distributes counts of unknown age proportionally. Unknown age value assumed to be -1.
#'
#' @param censuses data.table for censuses that includes columns for 'age_start', 'pop'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return data.table for censuses that includes columns for 'id_vars', 'age_start', 'pop'.
#'
#' @export
#' @import data.table
distribute_unknown_age <- function(censuses, id_vars) {

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_start", "pop") %in% names(censuses))) stop("'id_vars', 'age_start', 'pop' columns not in data.table")

  original_total_pop <- sum(censuses$pop)

  # split out the tabulations of unknown age
  if(nrow(censuses[age_start == -1]) > 0) {
    unknown_age_pop <- censuses[age_start == -1]
    unknown_age_pop[, age_start := NULL]
    censuses <- censuses[age_start != -1]

    # For each set of censuses tabulations, calculate the proportion in each age group
    proportions <- copy(censuses)
    proportions[, total_pop := sum(pop), by = c(id_vars)]
    proportions[, prop := pop / total_pop]
    proportions[, c("pop", "total_pop") := NULL]

    # merge proportions onto the data set of tabulations of unknown age
    unknown_age_pop <- merge(unknown_age_pop, proportions, by = c(id_vars), all.x = T)

    # split population proportionally
    unknown_age_pop[, pop := pop * prop]
    unknown_age_pop[, prop := NULL]

    # combine data back together
    censuses <- rbind(censuses, unknown_age_pop, use.names=T)
  }

  censuses <- censuses[, list(pop = sum(pop)), by = c(id_vars, "age_start")]

  # final checks
  if ((original_total_pop - sum(censuses$pop)) > 0.01) stop("population doesn't match before and after distribution")
  assertable::assert_values(censuses, colnames="age_start", test="not_equal", test_val = -1, quiet = T)

  return(censuses)
}

#' Calculates age-sex accuracy index and its components as described in "Population Analysis with Microcomputers, Volume I"
#'
#' @param censuses data.table for a given census that includes columns for id_vars, 'sex_ratio', 'male_age_ratio', 'female_age_ratio'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return data.table for a given census that includes columns for 'SRS' (sex-ratio score), 'ARSM' (age-ratio score males),
#' 'ARSF' (age-ratio score females), 'age_sex_accuracy_index'.
#'
#' @export
#' @import data.table
calculate_age_sex_accuracy_index <- function(censuses, id_vars, age_start_min = 10, age_start_max = 69) {

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "sex_ratio", "male_age_ratio", "female_age_ratio") %in% names(censuses))) stop("'id_vars', 'sex_ratio', 'male_age_ratio', 'female_age_ratio' columns not in data.table")

  index <- censuses[between(age_start, age_start_min, age_start_max), list(SRS = mean(abs(diff(sex_ratio))),
                                                                           ARSM = mean(abs(100 - male_age_ratio), na.rm = T),
                                                                           ARSF = mean(abs(100 - female_age_ratio), na.rm = T)),
                    by = id_vars]
  index <- index[, age_sex_accuracy_index := (3 * SRS) + ARSM + ARSF]
  return(index)
}

#' Calculates age and sex ratios as described in "Population Analysis with Microcomputers, Volume I"
#'
#' @param censuses data.table for a given census that includes columns for id_vars, 'sex_id', 'age_start', 'pop'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return data.table for a given census that includes columns for 'age_start', 'sex_ratio', 'male_age_ratio', 'female_age_ratio'.
#'
#' @export
#' @import data.table
calculate_age_sex_ratios <- function(censuses, id_vars) {

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "sex_id", "age_start", "pop") %in% names(censuses))) stop("'id_vars', 'sex_id', 'age_start', 'pop' columns not in data.table")

  ratios <- censuses[, list(age_start = unique(age_start),
                            sex_ratio = ((pop[sex_id == 1] / pop[sex_id == 2]) * 100),
                            male_age_ratio = ((c(NA, (pop[sex_id == 1][c(-1, -((.N / 2) - 1), -(.N / 2))]), NA, NA) / ((shift(pop[sex_id == 1], type = "lag") + shift(pop[sex_id == 1], type = "lead")) / 2)) * 100),
                            female_age_ratio = ((c(NA, (pop[sex_id == 2][c(-1, -((.N / 2) - 1), -(.N / 2))]), NA, NA) / ((shift(pop[sex_id == 2], type = "lag") + shift(pop[sex_id == 2], type = "lead")) / 2)) * 100)),
                     by = c(id_vars)]

  return(ratios)
}

#' Apply feeney correction to a given census-year-sex.
#'
#' @param censuses data.table for a given census that includes columns for id_vars, 'age_start' and 'pop'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @return data.table with feeney corrected population values.
#'
#' @export
#' @import data.table
feeney_ageheaping_correction <- function(censuses, id_vars = NULL) {

  corrected <- copy(censuses)

  # initial checks
  if(!any(class(corrected) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_start", "pop") %in% names(corrected))) stop("'id_vars', 'age_start', 'pop' columns not in data.table")

  # collapse to highest multiple of five age group
  corrected[, max_age_multiple_five := max(floor(age_start / 5) * 5)]
  corrected[age_start > max_age_multiple_five, age_start := max_age_multiple_five]
  corrected <- corrected[, list(pop = sum(pop)), by = c(id_vars, "age_start")]
  original_pop_total <- corrected[, list(original_pop_total = sum(pop)), by = id_vars]

  # drop missing age groups, will still scale up to total that includes them
  corrected <- corrected[!is.na(age_start) & age_start >= 0]

  corrected[, c("age1", "age5") := list(age_start, floor(age_start / 5) * 5)]
  corrected[, age_multiple_five := age1 == age5]

  corrected <- corrected[, list(p_x = pop[age_multiple_five],
                                p_x_plus = sum(pop[!age_multiple_five])), by = c(id_vars, "age5")]

  # save this for later
  census_before_convergence <- corrected[, list(age5 = age5[c(1, (.N - 1), .N)],
                                                replace_p_x_interp = p_x[c(1, (.N - 1), .N)] +
                                                  p_x_plus[c(1, (.N - 1), .N)]),
                                         by = id_vars]

  iterations <- 0
  tol <- 1e-10
  corrected[, total_abs_residual := 1]
  while(any(corrected$total_abs_residual > tol)) {
    iterations <- iterations + 1

    # calculate delta x
    corrected[, p_x_minus_and_p_x_plus := shift(p_x_plus) + p_x_plus, by = id_vars]
    corrected[, delta_x := (8 / 9) * ((p_x_minus_and_p_x_plus + p_x) / p_x_minus_and_p_x_plus), by = id_vars]

    # edge cases
    corrected[age5 == 0, p_x_minus_and_p_x_plus := p_x_plus, by = id_vars]
    corrected[age5 == 0 | age5 == max(age5), delta_x := 1, by = id_vars]

    # increment
    corrected[, p_x := p_x - ((delta_x - 1) * p_x_minus_and_p_x_plus), by = id_vars]
    corrected[, p_x_plus := (delta_x + shift(delta_x, type = "lead") - 1) * p_x_plus, by = id_vars]
    corrected[, p_x_plus := c(p_x_plus[-.N], 0), by = id_vars]

    corrected[, total_abs_residual := abs(sum(delta_x - 1)), by = id_vars]
  }

  # apply linear interpolation as described by Feeney with weights 0.6 and 0.4
  corrected[, p_x_interp := (0.6 * p_x) + (0.4 * shift(p_x, type = "lead")), by = id_vars]

  # multiplied by 5 to scale up to 5 year age groups
  corrected[, p_x_interp := p_x_interp * 5, by = id_vars]

  # original recorded values are used for the youngest, terminal
  # and one below the terminal age groups
  corrected <- merge(corrected, census_before_convergence, by = c(id_vars, "age5"), all = T)
  corrected[!is.na(replace_p_x_interp), p_x_interp := replace_p_x_interp]

  # scale back up to original total
  corrected <- merge(corrected, original_pop_total, by = id_vars, all = T)
  corrected[, interp_pop_total := sum(p_x_interp), by = id_vars]
  corrected[, p_x_interp := p_x_interp * (original_pop_total / interp_pop_total)]
  corrected <- corrected[, list(age_start = age5, pop = p_x_interp), by = id_vars]

  return(corrected)
}

#' Smooth age distribution using various methods as described in "Population Analysis with Microcomputers, Volume I".
#' Input census data must be in five or ten year age groups to start
#'
#' @param censuses data.table for a given census that includes columns for id_vars, 'sex_ratio', 'male_age_ratio', 'female_age_ratio'.
#' @param id_vars vector of id variables for censuses table ('ihme_loc_id', 'year_id', etc.)
#' @param method smoothing method to use ('carrier_farrag', 'karup_king_newton', 'arriaga', 'arriaga_strong')
#' @param split_10_year whether to split the smoothed 10 year counts into 5 year counts using arriaga forumulas
#' @return data.table for a given census that gives the smoothed counts back.
#' If the method does not smooth a given age group (youngest and oldest for non "arriaga" methods) or the smoothed value is negative,
#' uses the original data for that age group.
#'
#' @export
#' @import data.table
smooth_age_distribution <- function(censuses, id_vars, method, split_10_year = T) {

  # Table II-1 example
  # censuses <- data.table(sex_id = c(rep(1, 17), rep(2, 17)), age_start = rep(seq(0, 80, 5), 2),
  #                        pop = c(642367, 515520, 357831, 275542, 268336, 278601, 242515, 198231,
  #                                165937, 122756, 96775, 59307, 63467, 32377, 29796, 16183, 34729,
  #                                654258, 503070, 323460, 265534, 322576, 306329, 245883, 179182,
  #                                145572, 95590, 81715, 48412, 54572, 28581, 26733, 14778, 30300))
  # id_vars <- "sex_id"
  # method <- "arriaga_strong"
  censuses <- copy(censuses)

  # initial checks
  if(!any(class(censuses) == "data.table")) stop("data.table required")
  if(!all(c(id_vars, "age_start", "pop") %in% names(censuses))) stop("'id_vars', 'age_start', 'pop' columns not in data.table")
  if(!method %in% c("carrier_farrag", "karup_king_newton", "arriaga", "united_nations", "arriaga_strong")) stop("method must be one of 'carrier_farrag', 'karup_king_newton', 'arriaga', 'united_nations', 'arriaga_strong'")

  # round down to the 10 year age groups
  censuses[, age_start_10 := as.integer(plyr::round_any(age_start, 10, f = floor))]

  # collapse to terminal age group ending in zero
  censuses[, max_age_10 := max(age_start_10), by = id_vars]
  censuses[age_start > max_age_10, age_start := max_age_10, by = c(id_vars, "age_start", "age_start_10")]
  censuses <- censuses[, list(pop = sum(pop)), by = c(id_vars, "age_start", "age_start_10")]

  # sum to 10 year age groups
  adjusted_censuses <- censuses[, list(pop_10 = sum(pop)), by = c(id_vars, "age_start_10")]
  adjusted_censuses <- adjusted_censuses[, list(age_start_10 = age_start_10[-.N], pop_10 = pop_10[-.N]), by = id_vars] # drop the terminal age group

  # smooth 10 year age groups
  if (method == "arriaga_strong") {
    adjusted_censuses <- adjusted_censuses[, list(age_start_10, pop_10,
                                                  pop_10_adjusted = ((shift(pop_10, type = "lag") +
                                                                        (2 * pop_10) +
                                                                        shift(pop_10, type = "lead")) / 4)),
                                           by = id_vars]

    # rescale to total population in the non-extreme age groups
    adjusted_censuses[, total_original_pop := sum(pop_10[!is.na(pop_10_adjusted)]), by = id_vars]
    adjusted_censuses[, total_adjusted_pop := sum(pop_10_adjusted[!is.na(pop_10_adjusted)]), by = id_vars]
    adjusted_censuses[, pop_10_adjusted_scaled := pop_10_adjusted * (total_original_pop / total_adjusted_pop)]
    adjusted_censuses[, total_adjusted_scaled_pop := sum(pop_10_adjusted_scaled[!is.na(pop_10_adjusted_scaled)]), by = id_vars]

    adjusted_censuses[is.na(pop_10_adjusted_scaled), pop_10_adjusted_scaled := pop_10]
    adjusted_censuses[, c("pop_10", "pop_10_adjusted", "total_original_pop", "total_adjusted_pop", "total_adjusted_scaled_pop") := NULL]
    setnames(adjusted_censuses, "pop_10_adjusted_scaled", "pop_10")
  }

  # apply Carrier-Farrag formula
  if (split_10_year) {
    if (method == "carrier_farrag") {
      adjusted_censuses <- adjusted_censuses[, list(age_start_10, pop_10,
                                                    age_start = age_start_10 + 5,
                                                    pop = (pop_10 / (1 + ((shift(pop_10, type = "lag") / shift(pop_10, type = "lead")) ^ (1/4))))),
                                             by = c(id_vars)]
      adjusted_censuses <- adjusted_censuses[, list(pop_10, age_start = c(age_start_10, age_start_10 + 5),
                                                    adjusted_pop = c(pop_10 - pop, pop)), by = c(id_vars, "age_start_10")]
    } else if (method == "karup_king_newton") {
      adjusted_censuses <- adjusted_censuses[, list(age_start_10, pop_10,
                                                    age_start = age_start_10,
                                                    pop = (((1 / 2) * pop_10) + ((1 / 16) * (shift(pop_10, type = "lag") - shift(pop_10, type = "lead"))))),
                                             by = c(id_vars)]
      adjusted_censuses <- adjusted_censuses[, list(pop_10, age_start = c(age_start_10, age_start_10 + 5),
                                                    adjusted_pop = c(pop, pop_10 - pop)), by = c(id_vars, "age_start_10")]
    } else if ((method == "arriaga" | method == "arriaga_strong")) {
      adjusted_censuses <- adjusted_censuses[, list(age_start_10, pop_10,
                                                    age_start = age_start_10 + 5,
                                                    pop = (((-1 * shift(pop_10, type = "lag")) + (11 * pop_10) + (2 * shift(pop_10, type = "lead"))) / 24)),
                                             by = c(id_vars)]
      adjusted_censuses <- adjusted_censuses[, list(pop_10, age_start = c(age_start_10, age_start_10 + 5),
                                                    adjusted_pop = c(pop_10 - pop, pop)), by = c(id_vars, "age_start_10")]
      # fill extreme age groups
      adjusted_censuses[, adjusted_pop := c(NA, (((8 * pop_10[age_start == age_start_10][1]) +
                                                    (5 * pop_10[age_start == age_start_10][2]) +
                                                    (-1 * pop_10[age_start == age_start_10][3])) / 24),
                                            adjusted_pop[c(-1, -2, -(.N - 1), -.N)],
                                            (((-1 * pop_10[age_start == age_start_10][(.N / 2) - 2]) +
                                                (5 * pop_10[age_start == age_start_10][(.N / 2) - 1]) +
                                                (8 * pop_10[age_start == age_start_10][(.N / 2)])) / 24), NA),
                        by = id_vars]
      adjusted_censuses[, adjusted_pop := c(pop_10[1] - adjusted_pop[2],
                                            adjusted_pop[c(-1, -.N)],
                                            pop_10[.N] - adjusted_pop[(.N - 1)]),
                        by = id_vars]
    }
  } else {
    adjusted_censuses[, age_start := age_start_10]
    adjusted_censuses[, adjusted_pop := pop_10]
  }

  # merge on original totals in order to get original total for youngest and oldest 10 year age groups
  adjusted_censuses <- merge(adjusted_censuses, censuses, by = c(id_vars, "age_start", "age_start_10"), all = T)
  adjusted_censuses[is.na(adjusted_pop) | adjusted_pop <= 0, adjusted_pop := pop] # sometimes oldest ages can be negative with arriaga formula

  # scale up to original census total
  adjusted_censuses[, total_pop := sum(pop, na.rm = T), by = id_vars]
  adjusted_censuses[, adjusted_total_pop := sum(adjusted_pop, na.rm = T), by = id_vars]
  adjusted_censuses[, adjusted_pop := adjusted_pop * (total_pop / adjusted_total_pop)]

  adjusted_censuses[, c("age_start_10", "pop", "pop_10", "total_pop", "adjusted_total_pop") := NULL]
  setnames(adjusted_censuses, "adjusted_pop", "pop")
  return(adjusted_censuses)
}

#' Split census age groups into single year age groups using nLx proportions from a lifetable
#'
#' @param census data.table for a given census that includes columns for 'age_start', 'pop'.
#' @param lifetable data.table for a given lifetable that includes columns for 'age', 'nLx'.
#' @param terminal_age final terminal age group wanted in the split data
#' @return data.table for a given census that includes columns for 'age_start', 'pop' with single year age groups.
#'
#' @export
#' @import data.table
split_age_groups <- function(census, lifetable, terminal_age = 95) {

  lifetable <- copy(lifetable)

  # initial checks
  if(!any(class(census) == "data.table")) stop("data.table required")
  if(!any(class(lifetable) == "data.table")) stop("data.table required")
  if(!all(c("age_start", "pop") %in% names(census))) stop("'age_start', 'pop' columns not in census data.table")
  if(!all(c("age", "nLx") %in% names(lifetable))) stop("'age', 'nLx' columns not in lifetable data.table")

  # Use census age groups to split lifetable
  age_groups <- unique(census$age_start)
  lifetable[, age_start := cut(age, breaks = c(age_groups, Inf), labels = age_groups, right = F)]
  lifetable[, age_start := as.integer(as.character(age_start))]

  # calculate nLx proportions in each aggregate age group
  lifetable[, prop := nLx / sum(nLx), by = c("age_start")]

  # use nLx proportions to split into single year age groups
  census <- merge(census, lifetable, by = c("age_start"), all = T, allow.cartesian = T)
  census <- census[, list(age_start = age, pop = pop * prop)]

  # collapse to original census terminal age group or terminal_age
  census[age_start > terminal_age, age_start := terminal_age]
  census <- census[, list(pop = sum(pop)), by = "age_start"]

  return(census)
}


#' Smoothes population age patterns using smooth.spline in log space
#'
#' @param census data.table for a given census that includes columns for 'age_start', 'pop'.
#' @param max_age_int maximum age interval in census to determine smoothing parameter.
#' @return data.table for a given census that includes columns for 'age_start', 'pop' with smoothed populations.
#'
#' @export
#' @import data.table
smooth_age_pattern <- function(census, max_age_int) {

  use_census <- copy(census)
  original_census <- copy(use_census)

  # initial checks
  if(!any(class(use_census) == "data.table")) stop("data.table required")
  if(!all(c("age_start", "pop") %in% names(use_census))) stop("'age_start', 'pop' columns not in census data.table")

  original_total_pop <- sum(use_census$pop)

  # use smoothing spline in log space, by default uses leave-one-out to determine hyperparameters
  # fit <- smooth.spline(x = census$age_start, y = log(census$pop))
  # census[, pop := exp(predict(fit, x = age_start)$y)]

  ## Interpolate to smooth inner age groups
  bw <- ifelse(max_age_int <= 5, 2, 5) # We want more smoothing when less granular age intervals are included
  ages <- use_census[age_start != max(age_start), age_start]
  log_pop <- log(use_census[age_start != max(age_start), pop])
  fit <- KernSmooth::locpoly(x = ages, y = log_pop, degree = 1, bandwidth = bw,
                             gridsize = length(ages), range.x = c(min(ages), max(ages)))
  use_census[age_start != max(age_start), pop := exp(fit$y)]

  # rescale to original total population
  use_census[, pop := pop * (original_total_pop / sum(pop))]

  # original_census[, step := "original"]
  # use_census[, step := "smoothed"]
  # combined <- rbind(use_census, original_census)
  # ggplot(data = combined, aes(x = age_start, y = pop, colour = step)) +
  #   geom_point(data = combined[step == "original"]) +
  #   geom_line(data = combined[step == "smoothed"]) +
  #   theme_bw()

  return(use_census)
}

#' Calculates the nLx survival ratio using a lifetable according to the Preston Demography book.
#'
#' @param lifetable data.table for a given lifetable that includes columns for 'age', 'lx', 'nLx' and 'Tx'.
#' @param terminal_age terminal age group wanted in the output survival data
#' @param age_int age interval used in the lifetable and wanted in the survival data
#' @return data.table Survival ratio calculated for each age group, negative age
#' signifies survival value for newborns.
#'
#' @export
#' @import data.table
calculate_nLx_survival_ratio <- function(lifetable, terminal_age, age_int) {

  # initial checks
  if(!any(class(lifetable) == "data.table")) stop("data.table required")
  if(!all(c("age", "lx", "nLx", "Tx") %in% names(lifetable))) stop("'age', 'lx', 'nLx', 'Tx' columns not in data.table")

  survival <- lifetable[age <= (terminal_age + age_int),
                        list(age = seq(-age_int, terminal_age, age_int),
                             value = c((nLx[1] / (age_int * lx[1])),
                                       (shift(nLx, type="lead") / nLx)[-c(.N - 1, .N)],
                                       (Tx[.N] / Tx[.N - 1])))]
  return(survival)
}


#' Averages the nLx survival ratio between two year periods since cohorts are exposed
#' to survival ratios over half of each year.
#'
#' @param survival data.table of nLx survival ratios that includes columns for 'year_id', 'age' 'and 'value'.
#' @return data.table survival ratios calculated for each year and age group,
#' will not return a value for the most recent year in the input data.
#'
#' @export
#' @import data.table
average_survival <- function(survival, age_int = 1) {

  # initial checks
  if(!any(class(survival) == "data.table")) stop("data.table required")
  if(!all(c("year_id", "age", "value") %in% names(survival))) stop("'year_id', 'age', 'value' columns not in data.table")

  # shift values up one year, replace the last year value with the same value
  survival[, next_year_value := shift(value, type = "lead"), by = c("age")]
  survival <- survival[!is.na(next_year_value)]

  # average the survival values over the two year period
  survival <- survival[, list(value = ((value + next_year_value) / 2)), by = c("year_id", "age")]
  return(survival)
}


#' Projects population backwards in time using a modified Leslie matrix. Cannot
#' predict population for the oldest age groups because it is impossible to know how
#' many people to resurect or when to split them out of the terminal age group. This
#' currently assumes zero migration when projecting backwards.
#'
#' @param baseline data.table of the initial population in the most recent year, should have at least columns for 'age', 'year_id', 'value'.
#' @param surv data.table of the nLx survival ratios over the projection period of interest, should have at least columns for 'age', 'year_id', 'value'.
#' @param years vector of years to do the projection over.
#' @param value_name name of the value column in the input data.tables.
#' @return data.table with the new baseline population estimates,
#' NAs filled in for the missing triangle.
#'
#' @export
#' @import data.table
backwards_ccmpp <- function(baseline, surv,
                            years, value_name = "value") {

  ## setting the number of age groups and the number of projection/backcasting steps
  ages <- unique(baseline$age)
  terminal_age_group <- max(ages)
  n_age_grps <- length(ages)
  proj_steps <- length(years) - 1

  # convert data tables to matrices with year wide, age as rownames
  baseline_matrix <- matrix_w_rownames(dcast(baseline, age ~ year_id, value.var = value_name))
  surv_matrix <- matrix_w_rownames(dcast(surv, age ~ year_id, value.var = value_name))

  # initialize population matrix
  pop_mat <- matrix(0, nrow = n_age_grps, ncol = 1 + proj_steps)
  pop_mat[, proj_steps + 1] <- baseline_matrix

  ## project population backward one projection period
  for (i in proj_steps:1) {
    leslie <- make_backwards_leslie_matrix(surv_matrix[, i])
    pop_mat[, i] <- leslie %*% pop_mat[, i + 1]
  }

  # Assign NA the missing upper triangle (we don't know how many dead people to resurrect)
  for (i in proj_steps:1) {
    pop_mat[n_age_grps:(n_age_grps - (proj_steps - i + 1)), i] <- NA
  }

  # convert to data table
  melt_matrix <- function(mat, ages, years) {
    colnames(mat) <- years
    data <- data.table(mat)
    data[, age := ages]
    data <- melt(data, id.vars=c("age"), variable.name = "year_id", value.name = "value")
    data[, year_id := as.integer(as.character(year_id))]
    setcolorder(data, c("year_id", "age", value_name))
    return(data)
  }
  population <- melt_matrix(pop_mat, ages, years)

  return(population)
}


#' Fills in the Leslie matrix for projecting populations back in time
#'
#' @param nLx_survival vector of nLx survival ratio values.
#' @return Leslie matrix with survival values filled in just above the diagonal instead of below
#' like in forwards ccmpp.
#'
#' @export
#' @import data.table
make_backwards_leslie_matrix <- function(nLx_survival) {
  age_groups <- length(nLx_survival) - 1

  # Initialize Leslie matrix
  leslie <- matrix(0, nrow = age_groups, ncol = age_groups)

  # Take recipricol of survival values in order to go backwards
  nLx_survival <- 1 / nLx_survival

  # fill survival
  leslie[1:(age_groups - 1), 2:age_groups] <- diag(nLx_survival[2:age_groups])

  # Terminal age group and age group just below need to be missing
  leslie[age_groups - 1, age_groups] <- 0
  leslie[age_groups, age_groups] <- 0

  # add dimension names
  dimnames(leslie) <- list(names(nLx_survival)[-1], names(nLx_survival)[-1])
  return(leslie)
}

#' Used internally to convert a data.table to a matrix with the rownames
#' equal to the first column which is usually 'age'
#'
#' @param data data.table with 'age' in the first column and 'years' in the remaining columns.
#' @return matrix with yearly values and rownames equal to the age groups.
#'
#' @import data.table
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

#' Uses the ratio between the oldest age group population count that is known
#' and the nLx value of the same age group to scale up the nLx series in the missing
#' triangle. This creates a smooth older age population that is just the nLx age patter
#' scaled up.
#' @param censuses data.table of the backwards projected populations for a given location-sex with missing upper triangle,
#' should have at columns for 'age', 'value'.
#' @param lifetable data.table of the nLx values for a lifetable in the same location-year-sex,
#' should have at least columns for 'age', 'nLx'.
#' @param terminal_age terminal age group in the input census.
#' @return data.table with the new baseline population estimates,
#' but with the missing triangle filled in with predicted values from the nLx values in the lifetable
#'
#' @export
#' @import data.table
fill_missing_triangle <- function(censuses, lifetable, terminal_age = 95) {

  # collapse the lifetable to the correct terminal age group
  lifetable <- lifetable[, list(year_id, age_start = age, nLx)]
  lifetable[age_start > terminal_age, age_start := terminal_age]
  lifetable <- lifetable[, list(nLx = sum(nLx)), by = c("year_id", "age_start")]

  # determine the oldest age group we have a population count for
  censuses <- censuses[, list(year_id, age_start = age, pop = value)]
  oldest_age_not_missing <- censuses[!is.na(pop), list(age_start = max(age_start)), by = "year_id"]

  # determine the relationship between the oldest age group we have a population count for
  # and the nLx value for the same age group. Use this scalar to scale up the nLx values for
  # ages above this oldest age group.
  oldest_age_not_missing <- merge(oldest_age_not_missing, censuses, by = c("year_id", "age_start"), all.x = T)
  oldest_age_not_missing <- merge(oldest_age_not_missing, lifetable, by = c("year_id", "age_start"), all.x = T)
  oldest_age_not_missing <- oldest_age_not_missing[, list(scalar = pop / nLx), by = c("year_id")]
  censuses <- merge(censuses, lifetable, by = c("year_id", "age_start"), all = T)
  censuses <- merge(censuses, oldest_age_not_missing, by = c("year_id"), all = T)
  censuses[, missing_triangle := is.na(pop)]
  censuses[missing_triangle == T, pop := nLx * scalar]

  censuses <- censuses[, list(year_id = as.integer(year_id),
                              age_start = as.integer(age_start),
                              pop, missing_triangle)]
  return(censuses)
}
