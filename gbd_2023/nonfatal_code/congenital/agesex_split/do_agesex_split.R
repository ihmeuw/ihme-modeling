## ******************************************************************************
##'
##' Purpose: Identify any data in a bundle that spans multiple GBD age groups or
##'          sexes, and split the data into specific age groups and sexes. 
##'
## ******************************************************************************


args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
measure_name <- args[2]
bv_id <- args[3]
type <- args[4]
cascade <- args[5]

print(args)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library('openxlsx')
library('dplyr')
library('data.table')
library('matrixStats')
library('ggplot2')
library('msm', lib.loc = paste0(h, '/R_packages'))



source("FILEPATH")

#source custom functions
### change to your own repo path if necessary
source(paste0(h, "FILEPATH", "functions_agesex_split.R"))
source(paste0(h, "FILEPATH", "agesex_split_diagnostics.R"))
source(paste0(h, "FILEPATH", "adjust_under1.R"))

#-----------------------------------------------------------------------------------
save_dir <- "FILEPATH"

outdir <- paste0(save_dir, bun_id, "_", measure_name, "_CURRENT/")

dir.create(outdir)

map <- fread("FILEPATH")

me_id <- map[bundle_id == bun_id, me_id]

#-----------------------------------------------------------------------------------
#' Load bundle data, subset to only your measure of interest, and remove any means of NA
#-----------------------------------------------------------------------------------
bun_data <- get_bundle_version(bundle_version_id = bv_id, fetch = 'all')

bun_data <- pull_bundle_data(measure_name = measure_name, bun_id = bun_id, bun_data = bun_data)

bun_data_nas <- bun_data[is.na(mean)]
write.csv(bun_data_nas, file = paste0("FILEPATH", bun_id, measure_name, '.csv'), row.names =  FALSE)
bun_data <- bun_data[!is.na(mean)]


#collapse rows that should've been collapsed earlier b/c only diff is case_name
'%ni%' <- Negate('%in%')
names_df <- copy(bun_data)
cols <- colnames(names_df[, c('mean', 'cases', 'sample_size', 'standard_error', 'upper', 'lower', 'case_name', 'case_definition', 'seq') := NULL])

test <- bun_data[, new_cases := sum(cases), by = cols]
test <- test[, cases := new_cases]
test$new_cases <- NULL

test <- test[, new_case_name := lapply(.SD, paste0, collapse=" + "), by = cols, .SDcols = 'case_name']
test <- test[, case_name := new_case_name]
test$new_case_name <- NULL

cols <- c(cols, 'cases')
test <- unique(test, by = cols, with = FALSE)
bun_data <- test


### Apply outliers to any point where the original standard error is greater than 6 times the mean
bun_data <- bun_data[, is_outlier := 0]
bun_data <- bun_data[standard_error > 6*(mean), is_outlier := 1]


#ADDED FOR GBD2020 Step 2: aggregate old norway data into new subnat groups
norway_map <- fread(input = paste0("FILEPATH"))
setnames(norway_map, 'gbd19_loc_id', 'location_id')
bun_data <- merge(bun_data, norway_map, by = 'location_id', all.x = TRUE)
bun_data[!is.na(gbd20_loc_id), `:=`(cases = sum(.SD$cases),
                                    sample_size = sum(.SD$sample_size)),
         by=c('gbd20_loc_id', 'year_start', 'sex', 'age_start', 'age_end','nid')]
#replace old norway data with new subnat rows
norway_data <- bun_data[!is.na(gbd20_loc_id)]
bun_data <- bun_data[is.na(gbd20_loc_id)]
norway_data <- norway_data[!duplicated(norway_data[,c('gbd20_loc_id', 'year_start', 'sex', 'age_start', 'age_end', 'nid')]),]
bun_data <- rbind(bun_data, norway_data)
bun_data[!is.na(gbd20_loc_id), location_id := gbd20_loc_id]
bun_data$gbd20_loc_id <- NULL

#drop any location data where the location_id is not in the model location set.
if (type == 'dismod') {
  dismod_locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 7, decomp_step = 'iterative')
}
if (type == 'stgpr') {
  dismod_locs <- get_location_metadata(location_set_id = 22, gbd_round_id = 7, decomp_step = 'iterative')
}
bun_data <- bun_data[location_id %in% dismod_locs$location_id]

# adding age end restriction for anencephale for bun_id 610
if (bun_id == 610){
  bun_data <- bun_data[, age_start := 0]
  bun_data <- bun_data[, age_end := 0]
}


#-----------------------------------------------------------------------------------
#' Perform the under 1 inpatient cascade if cascade = TRUE 
#-----------------------------------------------------------------------------------
#The function will return your dataframe with adjusted under-1 inpatient data
if (cascade == TRUE){
  bun_data <- adjust_under1(df = bun_data, bundle_id = bun_id, diagnostics = TRUE)
}else{
  bun_data <- bun_data
}


#-----------------------------------------------------------------------------------
# Expand each row into its constituent GBD age and sex groups. Subset off data 
# that is already GBD-age and sex-specific, and therefore does not need to be 
# split ("good_data")
#-----------------------------------------------------------------------------------
expanded <- expand_test_data(agg.test = bun_data)

good_data <- expanded[need_split == 0]
expanded <- fsetdiff(expanded, good_data, all = TRUE)

if (nrow(expanded)==0) { 
  print(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
  stop(paste("Bundle",bun_id,"does not contain any",measure_name,"data that needs to be split"))
}

#-----------------------------------------------------------------------------------
#merge populations and age group ids onto the expanded dataset
#-----------------------------------------------------------------------------------
expanded[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]

#label each row with the closest dismod estimation year for matching to dismod model results
#round down
expanded[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
#round up
expanded[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]

expanded[est_year_id < 1990, est_year_id := 1990]
expanded[year_id == 2018 | year_id == 2019, est_year_id := 2019]
expanded[year_id > 2019, est_year_id := year_id]


expanded <- add_pops(expanded)
print("Loaded populations")
print(paste0('Number of rows in dataset where pop is NA: ',nrow(expanded[is.na(population)])))



#-----------------------------------------------------------------------------------
# Pull model results from DisMod to use as age/sex weights
# - TO DO: parameterize this section so you can choose dismod, stgpr, or mr-brt
# - TO DO: clean up into a function
#-----------------------------------------------------------------------------------

#' logit transform the original data mean and se
expanded$mean_logit <- log(expanded$mean / (1- expanded$mean))
expanded$standard_error_logit <- sapply(1:nrow(expanded), function(i) {
  mean_i <- as.numeric(expanded[i, "mean"])
  se_i <- as.numeric(expanded[i, "standard_error"])
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

#if the original mean is 0, the logit will be infinite, so recode to 0 to allow the immediate next steps to run as intended, 
#and then adjust later in the script
expanded[is.infinite(mean_logit), `:=` (mean_logit = 0, standard_error_logit = 0)]

#' SAMPLE FROM THE WEIGHTS:
#' Pull draw data for each age-sex-yr for every location in the current aggregated test data 
#' needed to be split. This is in rate space.
weight_draws <- pull_model_weights(me_id, measure_name)
print("Pulled model results")
print(paste0('Number of rows in dataset where draw_0 is NA: ',nrow(weight_draws[is.na(draw_0)])))

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
orig.data.to.split <- unique(draws[, .(split.id, draw.id, mean_logit, standard_error_logit)])

#' Generate 1000 draws from the input data (assuming a normal distribution), and replace the identical means
#' in orig.data.to.split with draws of that mean

# split the data table into a list, where each data point to be split is now its own data table 
orig.data.to.split <- split(orig.data.to.split, by = "split.id")

# then apply this function to each of those data tables in that list
orig.data.to.split <- lapply(orig.data.to.split, function(input_i){
  
  mean.vector <- input_i$mean_logit
  se.vector <- input_i$standard_error_logit
  #Generate a random draw for each mean and se 
  set.seed(123)
  input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
  input_i[, input.draw := input.draws]
  
  return(input_i)
  
})

orig.data.to.split <- rbindlist(orig.data.to.split)


#' Now each row of the dataset draws has a random draw from the distribution N(mean, SE) of the original
#' data point in that row. The mean of these draws will not be exactly the same as the input data point 
#' mean that it was randomly sampled from (because the sampling is random and the standard error can be
#' large).
draws <- merge(draws, orig.data.to.split[,.(split.id, draw.id, input.draw)], by = c('split.id','draw.id'))


####################################################################################

#' Calculate count of cases for a specific age-sex-loc-yr
#' based on the modeled prevalence
draws[, numerator := model.result * population]

#' Calculate count of cases across all the age/sex groups that cover an original aggregate data point, 
#' based on the modeled prevalence.
#' (The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point)
draws[, denominator := sum(numerator), by = .(split.id, draw.id)]

#' Calculate the weight for a specific age/sex group as: model prevalence of specific age/sex group (logit_split) divided by 
#' model prevalence of aggregate age/sex group (logit_aggregate)
draws[, logit_split := log(model.result / (1- model.result))]
draws[, logit_aggregate := log( (denominator/pop.sum) / (1 - (denominator/pop.sum)))]
draws[, logit_weight := logit_split - logit_aggregate]

#' Apply the weight to the original mean (in draw space and in logit space)
draws[, logit_estimate := input.draw + logit_weight]
draws[ ,estimate := logit_estimate]

#' If the original mean is 0, or the modeled prevalance is 0, set the estimate draw to 0
draws[is.infinite(logit_weight) | is.nan(logit_weight), estimate := 0]

draws[, sample_size_new := sample_size * population / pop.sum]

#' Save weight and input.draws in linear space to use in numeric check (recalculation of original data point)
draws <- draws[, weight := model.result / (denominator/pop.sum)]

# Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
#' Each expand ID has 1000 draws associated with it, so just take summary statistics by expand ID
final <- draws[, .(mean.est = mean(estimate),
                   sd.est = sd(estimate),
                   upr.est = quantile(estimate, .975),
                   lwr.est = quantile(estimate, .025),
                   sample_size_new = unique(sample_size_new),
                   cases.est = mean(numerator),
                   orig.cases = mean(denominator),
                   orig.standard_error = unique(standard_error),
                   mean.input.draws = mean(input.draw),
                   mean.weight = mean(weight),
                   mean.logit.weight = mean(logit_weight)), by = expand.id] %>% merge(expanded, by = "expand.id")

#' Convert mean and SE back to linear space 
final$sd.est <- sapply(1:nrow(final), function(i) {
  mean_i <- as.numeric(final[i, "mean.est"])
  mean_se_i <- as.numeric(final[i, "sd.est"])
  deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
})
final[, mean.est := exp(mean.est) / (1+exp(mean.est))]
final[, lwr.est := exp(lwr.est) / (1+exp(lwr.est))]
final[, upr.est := exp(upr.est) / (1+exp(upr.est))]
final[, mean.input.draws := exp(mean.input.draws) / (1+exp(mean.input.draws))]


#' If the original mean is 0, recode the mean estimate to 0, and calculate the adjusted standard error using Wilson's formula
#' and the split sample sizes.
final[mean == 0, mean.est := 0]
final[mean == 0, lwr.est := 0]
final[mean == 0, mean.input.draws := NA]

final[mean == 1, mean.est := 1]
final[mean == 1, upr.est := 1]
final[mean == 1, mean.input.draws := NA]

z <- qnorm(0.975)
final[(mean.est == 0 | mean.est == 1) & (measure == "prevalence" | measure == "proportion"),
      sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]
final[(mean.est == 0 | mean.est == 1) & measure == "incidence", 
      sd.est := ((5-mean.est*sample_size_new)/sample_size_new+mean.est*sample_size_new*sqrt(5/sample_size_new^2))/5]
final[mean.est == 0, upr.est := mean.est + 1.96 * sd.est]
final[mean.est == 1, lwr.est := mean.est - 1.96 * sd.est]


#' Print a warning for any seqs where the model did not contain any results for all the age/sex groups that cover an original data point
if (nrow(final[orig.cases == 0]) > 0) { 
  print(paste0('No model results for any of the age/sex groups you want to split the following seqs into: ',unique(final[orig.cases == 0, seq]),
               '. Please double-check the model you are using to inform the split, and confirm it contains results for all the age and sex groups you need.'))
}

final[, se.est := sd.est]
final[, orig.sample.size := sample_size]
final[, sample_size := sample_size_new]
final[,sample_size_new:=NULL]

#' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
final[, case_weight := cases.est / orig.cases]
final$orig.cases <- NULL
final$standard_error <- NULL
setnames(final, c("mean", "cases"), c("orig.mean", "orig.cases"))
setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
setnames(final, 'seq','crosswalk_parent_seq')
final[, seq := ''] 

#drop rows that don't match the cause sex-restrictions
if (bun_id == 437) { final <- final[sex_id == 2] }
if (bun_id == 438) { final <- final[sex_id == 1] }

final[, sex := ifelse(sex_id == 1, "Male", "Female")]
final[, `:=` (lower = lwr.est, upper = upr.est,
              cases = mean * sample_size, effective_sample_size = NA)]
final$cases.est <- NULL
final[is.nan(case_weight), `:=` (mean = NaN, cases = NaN)]

setnames(final, c('agg_age_start','agg_age_end','agg_sex_id'),
         c('orig_age_start', 'orig_age_end', 'orig_sex_id'))

#recalculate the original mean as a check
final[, reagg_mean := sum(cases) / sum(sample_size), by = .(split.id)]
final[, mean_diff_percent := (abs(reagg_mean - orig.mean)/orig.mean) * 100]
final[reagg_mean == 0 & orig.mean == 0, mean_diff_percent := 0]



#-----------------------------------------------------------------------------------
#' Save off just the split data for troubleshooting/diagnostics
#-----------------------------------------------------------------------------------
split_data <- final[, c('split.id', 'nid','crosswalk_parent_seq','age_start','age_end','sex_id','mean',
                        'standard_error','cases','sample_size',
                        'orig_age_start','orig_age_end','orig_sex_id', 'orig.mean',
                        'reagg_mean', 'mean_diff_percent','mean.input.draws',
                        'orig.standard_error','orig.cases','orig.sample.size',
                        'population','pop.sum',
                        'age_group_id','age_demographer','n.age','n.sex',
                        'location_id','est_year_id', 'year_start','year_end')]
split_data <- split_data[order(split.id)]

write.xlsx(split_data, 
           file = paste0(outdir, bun_id,'_',measure_name,'_split_only.xlsx'),
           sheetName = 'extraction',
           showNA = FALSE)

age_summary <- split_data[n.age > 1, .N, by = .(orig_age_start, orig_age_end, age_demographer, n.age, age_start, age_end)]
age_summary <- age_summary[order(orig_age_start)]
write.xlsx(age_summary, 
           file = paste0(outdir, bun_id,'_',measure_name,'_summary_age_ranges_post_split.xlsx'),
           sheetName = 'extraction',
           showNA = FALSE)

#-----------------------------------------------------------------------------------
#' Append split data back onto fully-specified data 
#-----------------------------------------------------------------------------------
good_data <- good_data[,names(bun_data),with = FALSE]
good_data[, crosswalk_parent_seq := seq]
good_data[, seq := '']

final <- final[,names(good_data),with = FALSE]

full_bundle <- rbind(good_data, final)


#-----------------------------------------------------------------------------------
#' Save the fully split bundle
#-----------------------------------------------------------------------------------

write.xlsx(full_bundle, 
           file = paste0(outdir, bun_id,'_',measure_name,'_full.xlsx'),
           sheetName = 'extraction',
           showNA = FALSE)

#-----------------------------------------------------------------------------------
#' Save a diagnostic plot of original mean data versus reaggregated split data
#-----------------------------------------------------------------------------------
create_diagnostics(bun_id = bun_id, measure_name = measure_name, outdir = outdir)
