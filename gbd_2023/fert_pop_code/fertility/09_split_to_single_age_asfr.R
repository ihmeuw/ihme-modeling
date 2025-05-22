####################################################################################################
## Description: Prep draws and estimates of age-specific fertility rate (asfr) by location id. Uses
##              spline interpolation to get single year age groups if needed but makes sure
##              that TFR values are maintained at the draw level.
##
## Outputs:     fertility_draws
##                '[temp_dir]/input_fertility_draws.rdata'
##              fertility_data, fertility_matrix, fertility_sigma2_matrix
##                '[temp_dir]/input_fertility_data.rdata'
####################################################################################################

library(data.table)
library(readr)
library(stringr)
library(ggplot2)
library(boot)
library(mortdb)

rm(list=ls())

if (interactive()){
  username <- "USERNAME"
  version_id <- "Run id"
  loc <- "input location"
  gbd_year <- 2020
  year_start <- 1950
  year_end <- 2022
} else {
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  username <- "USERNAME"
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  parser$add_argument('--loc', type = 'character')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  loc <- args$loc
}

input_dir <- "FILEPATH"
gpr_dir <- "FILEPATH"
output_dir <- "FILEPATH"

model_ages <- seq(10, 50, 5)
age_start <- 10
age_end <- 50
terminal_age <- 95
original_age_int <- 5

gpr <- rbindlist(lapply(model_ages, function(age) fread(paste0(gpr_dir, 'gpr_', loc, '_', age, '_sim.csv'))), fill=T)[,V1:=NULL]
gpr[,year_id := floor(year)]

pop <- fread(paste0(input_dir, 'input_pop_single.csv'))
age_map <- fread(paste0(input_dir, 'age_map.csv'))

pop <- merge(pop, age_map[,.(age_group_id, age_group_name_short)], by='age_group_id')
setnames(pop, 'age_group_name_short', 'age')
pop[,age := as.integer(age)]

asfr1x1 <- copy(gpr)
asfr1x1[, age := age + 2.5]
asfr1x1 <- asfr1x1[, list(age = age_start:age_end, value = spline(age, fert, xout=age_start : age_end)$y), by=c('ihme_loc_id', 'year_id', 'sim')]

asfr1x1 <- merge(asfr1x1, pop, by=c('ihme_loc_id', 'year_id', 'age'))


#####################################################

fert_1_id_vars <- list(ihme_loc_id = loc,
                       year_id = year_start:year_end,
                       age = seq(0, terminal_age, 1),
                       sim = 0:999)

fert_5_id_vars <- list(ihme_loc_id = loc,
                       year_id =  seq(year_start, year_end, 5),
                       age = seq(0, terminal_age, 5),
                       sim = 0:999)

srb_1_id_vars <- list(ihme_loc_id = loc,
                      year_id = seq(year_start, year_end, 1))

## read in data
loc_map <- fread(paste0(input_dir, 'loc_map.csv'))

loc_name <- loc_map[ihme_loc_id == loc, location_name]
loc_id <- loc_map[ihme_loc_id == loc, location_id]

fert_draws <- rbindlist(lapply(model_ages, function(age) fread(paste0(gpr_dir, 'gpr_', loc, '_', age, '_sim.csv'))), fill=T)[,V1:=NULL]
fert_draws[,year_id := floor(year)]
fert_draws[,year := NULL]
fert_draws[,'V1' := NULL]
setnames(fert_draws, 'fert', 'value')

pop1year <- copy(pop)
pop5year <- fread(paste0(input_dir, 'input_pop.csv'))
pop5year <- merge(pop5year, age_map[,.(age_group_id, age_group_name_short)], by='age_group_id')
setnames(pop5year, 'age_group_name_short', 'age')
pop5year[,age := as.integer(age)]

#########################
## Fertility functions ##
#########################

# add on younger and older age groups with zero births
add_zero_ages <- function(asfr, ages_needed) {
  zero_ages_needed <- setdiff(ages_needed, unique(asfr$age))
  zero_ages_needed <- zero_ages_needed[!between(zero_ages_needed, age_start, age_end)]
  zero_asfr <- CJ(ihme_loc_id=unique(asfr$ihme_loc_id),
                  year_id=unique(asfr$year_id),
                  age = zero_ages_needed,
                  sim = unique(asfr$sim),
                  value = 0)
  asfr <- rbind(asfr, zero_asfr)
  return(asfr)
}

# interpolate asfr into single year ages
interpolate_asfr <- function(asfr, rake_to_tfr = T, rake_to_asfr = F, log_space = T) {
  
  asfr1by5 <- copy(asfr)
  setkeyv(asfr, c('ihme_loc_id', 'year_id', 'age', 'sim'))
  
  # relabel ages as mid-year age
  asfr[between(age, age_start, age_end), age := age + (original_age_int / 2)]
  if(log_space == T) asfr[,value := log(value)]
  
  # spline interpolation
  asfr <- asfr[, list(age = age_start:age_end,
                      value = spline(age, value, xout=age_start : age_end)$y),
               by=c('ihme_loc_id', 'year_id', 'sim')]
  if(log_space == T) asfr[, value := exp(value)]
  
  asfr <- asfr[age>=age_start & age <= age_end]
  
  if(rake_to_asfr == T){
    ## getting 1 by 1 population
    asfr <- merge(asfr, pop1year, by = c('ihme_loc_id', 'year_id', 'age'), all.x=T)
    asfr[, births1year := value * population]
    
    ## collapse to 5 year age groups to get scaling factor
    agg_asfr <- copy(asfr)
    for(ages in seq(age_start, age_end, 5)){
      for(i in 1:4) agg_asfr[age == ages + i, age := ages]
    }
    
    agg_asfr <- agg_asfr[, .(births1year = sum(births1year)),
                         by = c('ihme_loc_id', 'year_id', 'age', 'sim')]
    
    asfr1by5 <- merge(asfr1by5, pop5year, all.x = TRUE, by = c('ihme_loc_id', 'year_id', 'age'))
    asfr1by5[, births5years := value * pop]
    asfr1by5[, c('value', 'pop') := NULL]
    
    scalar <- merge(asfr1by5, agg_asfr, all = T, by =c('ihme_loc_id', 'year_id', 'age', 'sim'))
    scalar[, scalar := births5years / births1year]
    
    ## expand the scalars to be able to merge on by single year ages 
    temp_scalar <- list()
    for(ages in seq(age_start,age_end, original_age_int)){
      for(i in 1:(original_age_int - 1)){
        temp_scalar[[paste(ages, i)]] <- scalar[age == ages]
        temp_scalar[[paste(ages, i)]][, age := ages + i]
      }
    }
    temp_scalar <- rbindlist(temp_scalar)
    scalar <- rbind(scalar, temp_scalar)
    
    asfr <- merge(asfr[, .(ihme_loc_id, year_id, age, sim, value)],
                  scalar[, .(ihme_loc_id, year_id, age, sim, scalar)],
                  by = c('ihme_loc_id', 'year_id', 'age', 'sim'), all.x =T)
    asfr[, value := value * scalar]
  }
  
  if(rake_to_tfr == TRUE){
    # adjust interpolated asfr based on original tfr levels
    tfr <- asfr[, list(new_tfr = sum(value) * 1), keyby = c('ihme_loc_id', 'year_id', 'sim')]
    asfr <- merge(asfr, tfr, by = c('ihme_loc_id', 'year_id', 'sim'), all = TRUE)
    original_tfr <- copy(original_tfr_draws)
    setnames(original_tfr, 'value', 'original_tfr')
    asfr <- merge(asfr, original_tfr, by = c('ihme_loc_id', 'year_id', 'sim'), all = TRUE)
    asfr[, correction := original_tfr / new_tfr]
    asfr[, value := value * correction]
    corrected_tfr_draws <- asfr[, list(original_tfr = sum(value) * 1), keyby = c('ihme_loc_id', 'year_id', 'sim')]
    if(!all.equal(original_tfr, corrected_tfr_draws)) stop('Adjustment based on TFR did not work')
    rm(tfr, corrected_tfr_draws)
    asfr[, c('new_tfr', 'original_tfr', 'correction')]
  }
  
  return(asfr)
}

# Average fertility over five year periods
average_asfr <- function(asfr) {
  asfr <- asfr[, year_id := cut(year_id, breaks = c(fert_5_id_vars$year_id, Inf),
                                labels=fert_5_id_vars$year_id, right=F)]
  asfr[, year_id := as.integer(as.character(year_id))]
  asfr <- asfr[, list(value = mean(value)), by = c('ihme_loc_id', 'year_id', 'age', 'sim')]
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

collapse_draws <- function(draws, var = 'value', id_vars = c('year', 'age')) {
  
  calc_metric <- c('mean', 'lb', 'ub', 'se')
  col_names <- paste(var, calc_metric, sep = '_')
  
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

fert_draws <- fert_draws[between(year_id, year_start, year_end)]
original_tfr_draws <- fert_draws[between(age, age_start, age_end), list(value = sum(value) * original_age_int), keyby = c('ihme_loc_id', 'year_id', 'sim')]
original_tfr_data <- collapse_draws(original_tfr_draws, var = 'value', id_vars = c('ihme_loc_id', 'year_id'))

# 1x1 fertility
fert_1_draws <- copy(fert_draws)
fert_1_draws <- interpolate_asfr(fert_1_draws, rake_to_tfr = FALSE, rake_to_asfr = TRUE, log_space = TRUE)

# scalars
scalars <- fert_1_draws[, .(ihme_loc_id, year_id, age, sim, scalar)]

fert_1_draws <- fert_1_draws[, scalar :=  NULL]
fert_1_draws <- add_zero_ages(fert_1_draws, ages_needed = fert_1_id_vars$age)
setkeyv(fert_1_draws, c('ihme_loc_id', 'year_id', 'age', 'sim'))
fert_1_data <- collapse_draws(fert_1_draws, var = 'value', id_vars = c('ihme_loc_id', 'year_id', 'age'))


# 5x5 fertility
fert_5_draws <- add_zero_ages(fert_draws, ages_needed = fert_5_id_vars$age)
fert_5_draws <- average_asfr(fert_5_draws)
setkeyv(fert_5_draws, c('ihme_loc_id', 'year_id', 'age', 'sim'))
fert_5_data <- collapse_draws(fert_5_draws, var = 'value', id_vars = c('ihme_loc_id', 'year_id', 'age'))

# Assertion checks --------------------------------------------------------

assertable::assert_ids(fert_1_draws, id_vars = fert_1_id_vars)
assertable::assert_values(fert_1_draws, colnames = 'value', test = 'gte', test_val=0)

assertable::assert_ids(fert_5_draws, id_vars = fert_5_id_vars)
assertable::assert_values(fert_5_draws, colnames = 'value', test = 'gte', test_val=0)

# Save prepped data -------------------------------------------------------

dir.create(paste0(output_dir, 'FILEPATH/'))
dir.create(paste0(output_dir, 'FILEPATH/'))
dir.create(paste0(output_dir, 'FILEPATH/'))

write_csv(fert_1_data, path = paste0(output_dir, 'FILEPATH/', loc, '_fert_1by1.csv'))
write_csv(fert_5_data, path = paste0(output_dir, 'FILEPATH/', loc, '_fert_5by5.csv'))
write_csv(original_tfr_data, path = paste0(output_dir, 'FILEPATH/', loc, '_original_tfr.csv'))

write_csv(fert_1_draws, path = paste0(output_dir, 'FILEPATH/', loc, '_fert_1by1_draws.csv'))
write_csv(fert_5_draws, path = paste0(output_dir, 'FILEPATH/', loc, '_fert_5by5_draws.csv'))
