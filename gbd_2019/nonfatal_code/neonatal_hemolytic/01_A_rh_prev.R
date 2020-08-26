## ******************************************************************************
##
## Purpose: Convert Rh negativity to counts of Rh incompatible pregnancies
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH/j/"
  h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(readxl)
library(openxlsx)

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_covariate_estimates.R")

locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 6)
locs <- locs[location_id != 1]

#' A. Pull modeled prevalence of Rh negativity
results <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = 24625, measure_id = 5, status = 'best',
                     gbd_round_id = 6, decomp_step = 'iterative', location_id = locs$location_id, sex_id = 3, year_id = 2010, 
                     source = 'epi', metric_id = 3, age_group_id = 5)
results <- results[, -c('metric_id', 'age_group_id', 'measure_id', 'sex_id', 'model_version_id', 'modelable_entity_id')]

write.csv(results, file = 'FILEPATH/rh_neg_prev_all_draws.csv',
          row.names = FALSE, na = '')


#' B. Find proportion Rh-incompatible pregnancies:
#' incompatible_prop = rh_negative_prop * (1-rh_negative_prop)
draw_cols <- paste0("draw_", 0:999)
results[, (draw_cols) := lapply(.SD, function(x) x * (1-x)), .SDcols = draw_cols]

#save draws
write.csv(results, file = 'FILEPATH/rh_incompatible_prev_all_draws.csv',
          row.names = FALSE, na = '')


#' C. Multiply by births to get counts of Rh-incompatible pregnancies
#' incompatible_counts = incompatible_prop * births
live_births <- get_covariate_estimates(covariate_id = 60, sex_id = 3, year_id = c(1980:2019), gbd_round_id = 6,
                                       decomp_step = 'step4')[,.(location_id, year_id, sex_id, mean_value)]
setnames(live_births, 'mean_value', 'births')
live_births[, births := births * 1000]

live_births_by_sex <- get_covariate_estimates(covariate_id = 1106, sex_id = c(1,2), year_id = c(1980:2019), gbd_round_id = 6,
                                       decomp_step = 'step4')[,.(location_id, year_id, sex_id, mean_value)]
setnames(live_births_by_sex, 'mean_value', 'births')

live_births <- rbind(live_births, live_births_by_sex)

results <- results[, -c('year_id')]
count_results <- merge(live_births, results, by = c('location_id'), all.x = TRUE)
count_results[, (draw_cols) := lapply(.SD, function(x) x * births), .SDcols = draw_cols]
setnames(count_results, c('year_id', 'sex_id'), c('year','sex'))

#save draws
write.csv(count_results, file = 'FILEPATH/rh_incompatible_count_all_draws.csv',
          row.names = FALSE, na = '')
