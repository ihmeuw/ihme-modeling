################################################################
## Purpose: Split 5 year estimates into single-year age groups
################################################################

library(data.table)
library(readr)
library(stringr)
library(boot)

rm(list=ls())

if (interactive()){
  username <- USERNAME
  root <- FILEPATH
  version <-
  ihme_loc <-
  gbd_year <-
  year_start <-
  year_end  <-
} else {
  task_id <- as.integer(Sys.getenv('SGE_TASK_ID'))
  username <- USERNAME
  args <- commandArgs(trailingOnly = T)
  root <- FILEPATH
  version <- param[task_id, version]
  ihme_loc <- param[task_id, model_locs]
  gbd_year <- param[task_id, gbd_year]
  year_start <- param[task_id, year_start]
  year_end <- param[task_id, year_end]
}

print(ihme_loc)

## model options
asfr_keys <- c('ihme_loc_id', 'year_id')
original_age_int <- 5
age_start <- 10
age_end <- 54
terminal_age <- 95
split5year_pop <- T

## setting directories
jbase <- FILEPATH
fertility_dir <- FILEPATH
fertility_transformation_dir <- FILEPATH
out_dir <- FILEPATH
srb_dir <- FILEPATH
mort_dir <- FILEPATH


# Set up id vars for assertion checks later -------------------------------

fertility_1_id_vars <- list(ihme_loc_id = ihme_loc,
                            year_id = year_start:year_end,
                            age = seq(0, terminal_age, 1),
                            draw = 0:999)
fertility_5_id_vars <- list(ihme_loc_id = ihme_loc,
                            year_id =  seq(year_start, year_end, 5),
                            age = seq(0, terminal_age, 5),
                            draw = 0:999)

srb_1_id_vars <- list(ihme_loc_id = ihme_loc,
                      year_id = seq(year_start, year_end, 1))


## read in data
location_hierarchy <- fread(paste0(jbase, 'loc_map.csv'))
age_map <- data.table(get_age_map())[,.(age_group_id, age_group_name_short)]
setnames(age_map, 'age_group_name_short', 'age')

loc_name <- location_hierarchy[ihme_loc_id == ihme_loc, location_name]
loc_id <- location_hierarchy[ihme_loc_id == ihme_loc, location_id]


fertility_draws <- lapply(seq(age_start, age_end, original_age_int), function(this_age) {
  draws <- fread(paste0(fertility_dir, this_age, '/gpr_', ihme_loc, '_', this_age, '_sim.csv'))
  draws[, age := this_age]
  draws <- draws[, list(ihme_loc_id, year_id = floor(year), age, draw = sim, value = val)]
})

fertility_draws <- rbindlist(fertility_draws)

## population
pop <- fread(paste0(jbase, 'pop_input.csv'))
pop[,pvid := NULL]
pop <- merge(pop, age_map, all.x=T, by= 'age_group_id')
pop <- pop[age %in% seq(10, 50, 5)]
pop <- merge(pop, location_hierarchy[,.(location_id, ihme_loc_id)], by = 'location_id', all.x=T)
pop <- pop[ihme_loc_id == ihme_loc]
pop[,age := as.numeric(age)]
pop <- pop[,.(ihme_loc_id, year_id, age, population)]

pop5year <- copy(pop)

if(split5year_pop == T){
  pop1year <- copy(pop)
  pop1year[,population := population /5]
  temp_pop <- list()
  for(ages in unique(pop1year$age)){
    for(i in 1:(original_age_int-1)){
      temp_pop[[paste(ages, i)]] <- pop1year[age == ages]
      temp_pop[[paste(ages, i)]][,age := ages + i]
    }
  }
  temp_pop <- rbindlist(temp_pop)
  pop1year <- rbind(pop1year, temp_pop)

}


#########################
## Fertility functions ##
#########################

# add on younger and older age groups with zero ASFR
add_zero_ages <- function(asfr, ages_needed) {
  zero_ages_needed <- setdiff(ages_needed, unique(asfr$age))
  zero_ages_needed <- zero_ages_needed[!between(zero_ages_needed, age_start, age_end)]
  zero_asfr <- CJ(ihme_loc_id=unique(asfr$ihme_loc_id),
                  year_id=unique(asfr$year_id),
                  age = zero_ages_needed,
                  draw = unique(asfr$draw),
                  value = 0)
  asfr <- rbind(asfr, zero_asfr)
  return(asfr)
}

# interpolate asfr into single year ages
interpolate_asfr <- function(asfr, rake_to_tfr = T, rake_to_asfr = F, log_space = T) {

  asfr1by5 <- copy(asfr)
  setkeyv(asfr, c(asfr_keys, 'age', 'draw'))

  asfr[between(age, age_start, age_end), age := age + (original_age_int / 2)]
  if(log_space == T) asfr[,value := log(value)]

  # spline interpolation
  asfr <- asfr[, list(age = age_start:age_end,
                      value = spline(age, value, xout=age_start : age_end)$y),
               by=c(asfr_keys, 'draw')]
  if(log_space == T) asfr[, value := exp(value)]

  asfr <- asfr[age>=age_start & age <= age_end]

  if(rake_to_asfr == T){
    ## getting 1 by 1 population
    asfr <- merge(asfr, pop1year, by = c('ihme_loc_id', 'year_id', 'age'), all.x=T)
    asfr[,births1year := value * population]

    ## collapsing to 5 year age groups to get scaling factor
    agg_asfr <- copy(asfr)
    for(ages in seq(10,50,5)){
      for(i in 1:4) agg_asfr[age == ages + i, age:= ages]
    }

    agg_asfr <- agg_asfr[,.(births1year = sum(births1year)),
                         by = c('ihme_loc_id', 'year_id', 'age', 'draw')]

    asfr1by5 <- merge(asfr1by5, pop5year, all.x=T, by = c('ihme_loc_id', 'year_id', 'age'))
    asfr1by5[,births5years := value * population]
    asfr1by5[,c('value', 'population') := NULL]

    scalar <- merge(asfr1by5, agg_asfr, all=T, by =c('ihme_loc_id', 'year_id', 'age', 'draw'))
    scalar[,scalar := births5years / births1year]

    temp_scalar <- list()
    for(ages in seq(age_start,age_end, original_age_int)){
      for(i in 1:(original_age_int-1)){
        temp_scalar[[paste(ages, i)]] <- scalar[age == ages]
        temp_scalar[[paste(ages, i)]][,age := ages + i]
      }
    }
    temp_scalar <- rbindlist(temp_scalar)
    scalar <- rbind(scalar, temp_scalar)

    asfr <- merge(asfr[,.(ihme_loc_id, year_id, age, draw, value)],
                  scalar[,.(ihme_loc_id, year_id, age, draw, scalar)],
                  by = c('ihme_loc_id', 'year_id', 'age', 'draw'), all.x =T)
    asfr[,value := value * scalar]
  }

  if(rake_to_tfr == T){
    tfr <- asfr[, list(new_tfr = sum(value) * 1), keyby=c(asfr_keys, 'draw')]
    asfr <- merge(asfr, tfr, by=c(asfr_keys, 'draw'), all=T)
    original_tfr <- copy(original_tfr_draws)
    setnames(original_tfr, 'value', 'original_tfr')
    asfr <- merge(asfr, original_tfr, by=c(asfr_keys, 'draw'), all=T)
    asfr[, correction := original_tfr / new_tfr]
    asfr[, value := value * correction]
    corrected_tfr_draws <- asfr[, list(original_tfr = sum(value) * 1), keyby=c(asfr_keys, 'draw')]
    if(!all.equal(original_tfr, corrected_tfr_draws)) stop('Adjustment based on TFR did not work')
    rm(tfr, corrected_tfr_draws)
    asfr[, c('new_tfr', 'original_tfr', 'correction')]
  }

  return(asfr)
}

# Average fertility over five year periods
average_asfr <- function(asfr) {
  asfr <- asfr[, year_id := cut(year_id, breaks=c(fertility_5_id_vars$year_id, Inf),
                                labels=fertility_5_id_vars$year_id, right=F)]
  asfr[, year_id := as.integer(as.character(year_id))]
  asfr <- asfr[, list(value = mean(value)), by=c(asfr_keys, 'age', 'draw')]
  return(asfr)

}

## collapse draws
# Collapse draws to mean, lb, ub, se estimates.
#
# Args:
#   draws: data.table of draws to collapse
#   var: variable to collapse
#   id_vars: variables to collapse over
# Returns:
#   data.table with mean, lb, ub and se estimates

collapse_draws <- function(draws, var='value', id_vars=c('year', 'age')) {

  calc_metric <- c('mean', 'lb', 'ub', 'se')
  col_names <- paste(var, calc_metric, sep='_')

  setnames(draws, var, 'value')
  est <- draws[, as.list(c(mean(value),
                           quantile(value, c(0.025, 0.975), type=5),
                           sd(value))),
               by=id_vars]

  setnames(draws, 'value', var)
  setnames(est, c(id_vars, col_names))
  setcolorder(est, c(id_vars, col_names))
  setkeyv(est, id_vars)
  return(est)
}


# Calculate values of interest --------------------------------------------

fertility_draws <- fertility_draws[between(year_id, year_start, year_end)]
original_tfr_draws <- fertility_draws[between(age, 10, 54), list(value = sum(value) * original_age_int), keyby = c(asfr_keys, 'draw')]
original_tfr_data <- collapse_draws(original_tfr_draws, var = 'value', id_vars = c(asfr_keys))

# 1x1 fertility
fertility_1_draws <- copy(fertility_draws)
fertility_1_draws <- interpolate_asfr(fertility_1_draws, rake_to_tfr = F, rake_to_asfr = T, log_space = T)

# scarlars -- for diagnostic purposes
scalars <- fertility_1_draws[,.(ihme_loc_id, year_id, age, draw, scalar)]

fertility_1_draws <- fertility_1_draws[,scalar :=  NULL]
fertility_1_draws <- add_zero_ages(fertility_1_draws, ages_needed = fertility_1_id_vars$age)
setkeyv(fertility_1_draws, c(asfr_keys, 'age', 'draw'))
fertility_1_data <- collapse_draws(fertility_1_draws, var = 'value', id_vars = c(asfr_keys, 'age'))


# 5x5 fertility
fertility_5_draws <- add_zero_ages(fertility_draws, ages_needed = fertility_5_id_vars$age)
fertility_5_draws <- average_asfr(fertility_5_draws)
setkeyv(fertility_5_draws, c(asfr_keys, 'age', 'draw'))
fertility_5_data <- collapse_draws(fertility_5_draws, var = 'value', id_vars = c(asfr_keys, 'age'))


# Assertion checks --------------------------------------------------------

assertable::assert_ids(fertility_1_draws, id_vars = fertility_1_id_vars)
assertable::assert_values(fertility_1_draws, colnames='value', test='gte', test_val=0)

assertable::assert_ids(fertility_5_draws, id_vars = fertility_5_id_vars)
assertable::assert_values(fertility_5_draws, colnames='value', test='gte', test_val=0)



# Save prepped data -------------------------------------------------------

write_csv(fertility_1_data, path = paste0(out_dir, 'one_by_one/', ihme_loc, '_fert_1by1.csv'))
write_csv(fertility_5_data, path = paste0(out_dir, 'five_by_five/', ihme_loc, '_fert_5by5.csv'))
write_csv(original_tfr_data, path = paste0(out_dir, 'orig_tfr/', ihme_loc, '_original_tfr.csv'))

write_csv(fertility_1_draws, path = paste0(out_dir, 'one_by_one/', ihme_loc, '_fert_1by1_draws.csv'))
write_csv(fertility_5_draws, path = paste0(out_dir, 'five_by_five/', ihme_loc, '_fert_5by5_draws.csv'))

