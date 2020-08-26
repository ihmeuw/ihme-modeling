## ******************************************************************************
##
## Purpose: Identify any data in a bundle that spans multiple GBD age groups or
##          sexes, and split the data into specific age groups and sexes
## Input:   Bundle ID and measure (either prevalence or incidence)
## Output:  - Saves a troubleshooting file of just the split data
##          - Saves a fully agesex split version of the entire bundle
##
## ******************************************************************************

rm(list=ls())

args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
measure_name <- args[2]

print(args)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH/j/"
  h <- paste0("FILEPATH/", Sys.getenv("USER"),"/")
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')

source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_population.R")
source("FILEPATH/interpolate.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")

# source custom functions
source(paste0(h, "repos/functions_agesex_split.R"))
source(paste0(h, "repos/divide_data.R"))

#-----------------------------------------------------------------------------------
### constants
save_dir <- "FILEPATH"
map <- fread("FILEPATH/all_me_bundle.csv")

me_id <- map[bundle_id == bun_id, me_id]

age_map <- fread("FILEPATH/age_map.csv")

bundle_data_output_filepath <- 'FILEPATH'

#-----------------------------------------------------------------------------------
#load bundle data
#-----------------------------------------------------------------------------------
bun_data <- get_bundle_data(bundle_id = bun_id, decomp_step = 'step4', export = F)
bun_data <- as.data.table(bun_data)

bun_data <- pull_bundle_data(measure_name = measure_name, bun_id = bun_id, bun_data = bun_data)

bun_data <- bun_data[!is.na(mean)]

#-----------------------------------------------------------------------------------
#subset data into an aggregate dataset and a fully-specified dataset
#-----------------------------------------------------------------------------------
data <- divide_data(input_data = bun_data)
good_data <- data[need_split == 0]
aggregate <- fsetdiff(data, good_data, all = TRUE)

if (nrow(aggregate)==0) { 
  print(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
  stop(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
}

#-----------------------------------------------------------------------------------
#expand the aggregate dataset into its constituent age and sexes
#-----------------------------------------------------------------------------------
expanded <- expand_test_data(agg.test = aggregate)
aggregate <- NULL

#-----------------------------------------------------------------------------------
#merge populations and age group ids onto the expanded dataset
#-----------------------------------------------------------------------------------
if ("age_group_id" %in% names(expanded) == TRUE) {
  expanded$age_group_id <- NULL
}

expanded[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]

#label each row with the closest dismod estimation year for matching to dismod model results
#round down
expanded[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
#round up
expanded[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]

expanded[est_year_id < 1990, est_year_id := 1990]
expanded[year_id == 2017 | year_id == 2018, est_year_id := 2017]
expanded[year_id > 2018, est_year_id := 2019]

expanded <- add_pops(expanded)
print("Loaded populations")
print(paste0('Number of rows in dataset where pop is NA: ',nrow(expanded[is.na(population)])))

#-----------------------------------------------------------------------------------
# Pull model results to use as age/sex weights
#-----------------------------------------------------------------------------------

#' SAMPLE FROM THE WEIGHTS:
#' Pull draw data for each age-sex-yr for every location in the current aggregated test data 
#' needed to be split.
weight_draws <- pull_model_weights(me_id, measure_name)
print("Pulled DisMod results")

#' Append draws to the aggregated dataset
draws <- merge(expanded, weight_draws, by = c("age_group_id", "sex_id", "location_id", "est_year_id"), 
               all.x=TRUE)

#' Take all the columns labeled "draw" and melt into one column. This means there is now a column called draw.id with
#' values from "draw_0" through "draw_999" and a column called "model.result" which contains the draw value for each
#' of the 1000 draws.
draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                         variable.name = "draw.id", value.name = "model.result")

#' SAMPLE FROM THE RAW INPUT DATA: 
#' Save a dataset of 1000 rows per every input data point that needs to be split.
#' Keep columns for mean and standard error, so that you now have a dataset with 
#' 1000 identical copies of the mean and standard error of each input data point.
orig.data.to.split <- unique(draws[, .(split.id, draw.id, mean, standard_error)])

#' Generate 1000 draws from the input data (assuming a normal distribution), and replace the identical means
#' in orig.data.to.split with draws of that mean
set.seed(123)
mean.vector <- orig.data.to.split$mean
se.vector <- orig.data.to.split$standard_error
#Generate a random draw for each mean and se 
input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
orig.data.to.split[, input.draw := input.draws]

#' Now each row of the dataset draws has a random draw from the distribution N(mean, SE) of the original
#' data point in that row. The mean of these draws will not be exactly the same as the input data point 
#' mean that it was randomly sampled from (because the sampling is random and the standard error can be
#' large).
draws <- merge(draws, orig.data.to.split[,.(split.id, draw.id, input.draw)], by = c('split.id','draw.id'))

####################################################################################

#' This is the numerator of the equation, the total number of cases for a specific age-sex-loc-yr
#' based on the modeled prevalence
draws[, numerator := model.result * population]

#' This is the denominator, the sum of all the numerators by both draw and split ID. The number of cases in the aggregated age/sex 
#' group.
#' The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point
draws[, denominator := sum(numerator), by = .(split.id, draw.id)]

#' Calculate the actual estimate of the split point from the input data (mean) and a unique draw from the modelled 
#' prevalence (model.result)
draws[, estimate := input.draw * model.result / denominator * pop.sum]
draws[, sample_size_new := sample_size * population / pop.sum]

# If the numerator and denominator are zero, set the estimate to zero
draws[numerator == 0 & denominator == 0, estimate := 0]

# Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
#' Each expand ID has 1000 draws associated with it, so just take summary statistics by expand ID
final <- draws[, .(mean.est = mean(estimate),
                   sd.est = sd(estimate),
                   upr.est = quantile(estimate, .975),
                   lwr.est = quantile(estimate, .025),
                   sample_size_new = unique(sample_size_new),
                   cases.est = mean(numerator),
                   agg.cases = mean(denominator),
                   agg.standard_error = unique(standard_error)), by = expand.id] %>% merge(expanded, by = "expand.id")

#if the standard deviation is zero, calculate the standard error using Wilson's formula instead of the standard deviation of the mean
z <- qnorm(0.975)
final[sd.est == 0 & (measure == 'prevalence' | measure == 'proportion'), 
      sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]

final[, se.est := sd.est]
final[, agg.sample.size := sample_size]
final[, sample_size := sample_size_new]
final[,sample_size_new:=NULL]

#' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
final[mean==0, mean.est := 0]
final[, case_weight := cases.est / agg.cases]
final$agg.cases <- NULL
final$standard_error <- NULL
setnames(final, c("mean", "cases"), c("agg.mean", "agg.cases"))
setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
setnames(final, 'seq','crosswalk_parent_seq')
final[, seq := ''] 

#drop rows that don't match the cause sex-restrictions
if (bun_id == 437) { final <- final[sex_id == 2] }
if (bun_id == 438) { final <- final[sex_id == 1] }

#-----------------------------------------------------------------------------------
#' Save off just the split data for troubleshooting/diagnostics
#-----------------------------------------------------------------------------------
split_data <- final[, c('nid','age_start','age_end','sex_id','mean',
                        'standard_error','case_weight','sample_size',
                        'agg_age_start','agg_age_end','agg_sex_id', 'agg.mean','agg.standard_error','agg.cases','agg.sample.size',
                        'population','pop.sum',
                        'age_group_id','age_demographer','n.age','n.sex',
                        'location_id','est_year_id', 'year_start','year_end', 'split.id')]
split_data <- split_data[order(nid)]

write.xlsx(split_data, 
           file = paste0(bundle_data_output_filepath, 'FILENAME.xlsx'),
           sheetName = 'extraction',
           showNA = FALSE)


#-----------------------------------------------------------------------------------
#' Append split data back onto fully-specified data and save the fully split bundle
#-----------------------------------------------------------------------------------
good_data <- good_data[,names(bun_data),with = FALSE]
good_data[, crosswalk_parent_seq := seq]
good_data[, seq := '']

final[, sex := ifelse(sex_id == 1, "Male", "Female")]
final[, `:=` (lower = lwr.est, upper = upr.est,
              cases = mean * sample_size, effective_sample_size = NA)]
final$cases.est <- NULL

final <- final[,names(good_data),with = FALSE]

full_bundle <- rbind(good_data, final)

write.xlsx(full_bundle, 
           file = paste0(bundle_data_output_filepath, 'FILENAME.xlsx'),
           sheetName = 'extraction',
           showNA = FALSE)
