## ******************************************************************************
##
## Purpose: Run data processing on enceph birth prevalence data
##          This includes: running under 1 aggregation on clinical data, and
##          applying the clinical correction factor. This data is then uploaded
##          to a bundle and a bundle version, then crosswalk versions are saved.
## Input:   
## Output:  crosswalk version ID, ready for ST-GPR
## Created: 2020-05-12
## Last updated: 2020-07-09
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "PATHNAME"
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
} else {
  j<- "PATHNAME"
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
}

out_dir <- "OUTPUT_DIR_PATHNAME"

pacman::p_load(data.table, openxlsx, tidyr, msm)
source("PATHNAME/get_bundle_data.R")
source("PATHNAME/get_crosswalk_version.R")
source("PATHNAME/save_crosswalk_version.R")
source("PATHNAME/get_bundle_version.R")
source("PATHNAME/save_bundle_version.R")
source("PATHNAME/upload_bundle_data.R")
source("PATHNAME/get_location_metadata.R")
source("PATHNAME/validate_input_sheet.R")
source("PATHNAME/get_age_metadata.R")

source("PATHNAME/adjust_under1.R")
source("PATHNAME/functions_agesex_split.R")

bun_id <- 338

#if you have new or modified lit data, run these first two lines to update any literature data in the bundle itself
bun_data <- get_bundle_data(bundle_id = bun_id, gbd_round_id = 7, decomp_step = 'iterative')
upload_bundle_data(bundle_id = bun_id, decomp_step = 'iterative', gbd_round_id = 7, 
                   filepath = "PATHNAME")


#pull prevalence data and apply the under 1 adjustment on all inpatient data
bv <- save_bundle_version(bundle_id = bun_id, decomp_step = 'iterative', gbd_round_id = 7, 
                          include_clinical = c('inpatient', 'claims, inpatient only')) 
bv_id <- bv$bundle_version_id
bv_id <- 28793
bv_data <- get_bundle_version(bundle_version_id = bv_id, fetch = 'all')

# Note: the mean, cases, and sample size are adjusted. the lower, upper and standard error are not.
bv_data_under1 <- adjust_under1(df = bv_data, bundle_id = bun_id, diagnostics = TRUE)

bv_data_under1 <- fread('PATHNAME/BUNDLE')
bv_data_under1[is.na(cv_literature) & extractor != '', cv_literature := 1]
bv_data_under1 <- bv_data_under1[age_end < 1 | (cv_literature == 1)]
bv_data_under1[cv_literature == 1, age_end := 0]

bv_data_under1 <- calculate_cases_fromse(raw_dt = bv_data_under1)

#outlier rows where the standard_error is more than 6x the mean
bv_data_under1[standard_error/mean > 6, is_outlier := 1]

#apply the clinical CF to all the clinical data
# - pull clinical CF coefficients
# - predict out for males and females
# - generate draws of the CFs
# - generate draws of the clinical data
# - merge by sex
# - multiply clinical data by CF in draw space
# - collapse back into adjusted mean, lower, upper
predict_values <- fread("PATHNAME")

#transform out of logit space
predict_values[, est_proportion := exp(Y_mean) / (1+exp(Y_mean))]
predict_values[, est_lower := exp(Y_mean_lo) / (1+exp(Y_mean_lo))]
predict_values[, est_upper := exp(Y_mean_hi) / (1+exp(Y_mean_hi))]
predict_values[, est_se := (est_upper-est_lower)/3.92]

predict_values <- predict_values[, .(X_male, est_proportion, est_se)]
predict_values[X_male == 1, sex := 'Male']
predict_values[X_male == 0, sex := 'Female']

#----------------------------------------------------------------------------------
# Simulate uncertainty around the beta coefficients of the correction factor
#----------------------------------------------------------------------------------
draw_cols <- paste0("draw_", 0:999)
predict_values[,draw_cols] <- 0 

draws <- melt.data.table(predict_values, id.vars = names(predict_values)[!grepl("draw", names(predict_values))], 
                         measure.vars = patterns("draw"),
                         variable.name = 'draw.id')

draws <- split(draws, by = "X_male")

draws <- lapply(draws, function(input_i){
  
  mean.vector <- input_i$est_proportion
  se.vector <- input_i$est_se
  #Generate a random draw for each mean and se 
  set.seed(123)
  input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
  input_i[, value := input.draws]
  
  return(input_i)
  
})

predict_draws <- rbindlist(draws)
setnames(predict_draws, 'value', 'cf')

#----------------------------------------------------------------------------------
# Simulate uncertainty around the inpatient input data
#----------------------------------------------------------------------------------

# Subset off and set aside the lit and claims data, which does not need to be corrected
bv_lit_and_claims <- bv_data_under1[clinical_data_type != 'inpatient']
bv_data <- bv_data_under1[clinical_data_type == 'inpatient']

#if standard error is 0 (can happen in clinical processing when the mean is 0), calculate it from sample_size
nrow(bv_data[standard_error == 0])
z <- qnorm(0.975)
bv_data[(is.na(standard_error) | standard_error == 0) & (measure == "prevalence" | measure == "proportion"),
              standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]

bv_data_draws <- copy(bv_data)
bv_data_draws[,draw_cols] <- 0 

bv_data_draws <- melt.data.table(bv_data_draws, id.vars = names(bv_data_draws)[!grepl("draw", names(bv_data_draws))], 
                         measure.vars = patterns("draw"),
                         variable.name = 'draw.id')

bv_data_draws <- split(bv_data_draws, by = "seq")

# then apply this function to each of those data tables in that list
bv_data_draws <- lapply(bv_data_draws, function(input_i){
  
  mean.vector <- input_i$mean
  se.vector <- input_i$standard_error
  #Generate a random draw for each mean and se 
  set.seed(123)
  input.draws <- rnorm(length(mean.vector), mean.vector, as.numeric(se.vector))
  input_i[, value := input.draws]
  
  return(input_i)
  
})

bv_data_draws <- rbindlist(bv_data_draws)

bv_data_draws[mean == 0, value := 0]

# code to investigate the simulated uncertainty
# final <- bv_data_draws[, .(mean_clinical_data = mean(value)), by = seq] %>% merge(bv_data, by = "seq")
# final[, diff := abs(mean_clinical_data - mean)]
# final[, percent_diff := (diff / mean) * 100]
# final[, rel_se := standard_error / mean]
# final <- final[order(-percent_diff)]

#merge by sex
bv_data_draws <- merge(bv_data_draws, predict_draws, by = c('draw.id', 'sex'), all.x = TRUE)

#multiply the clinical data by the cf
bv_data_draws[, adjusted_value := value * cf]

#collapse back into means
final <- bv_data_draws[, .(adjusted_mean = mean(adjusted_value),
                     adjusted_se = sd(adjusted_value),
                     adjusted_upper = quantile(adjusted_value, .975),
                     adjusted_lower = quantile(adjusted_value, .025),
                     mean_clinical_data = mean(value),
                     mean_cf = mean(cf)), by = seq] %>% merge(bv_data, by = "seq")

#outlier rows where the standard_error is more than 6x the mean
# final[standard_error/mean > 6, is_outlier := 1]

final[adjusted_mean > 1, adjusted_mean := 1]
final[adjusted_upper > 1, adjusted_upper := 1]
final[adjusted_lower < 0, adjusted_lower := 0]

# If the mean is zero, the draws were all 0 and the adjusted_se is now 0, so replace with standard_error, which was
# calculated above with a formula based on sample size. Recalculate the lower and upper as well.
final[mean == 0, `:=` (adjusted_se = standard_error, 
                       adjusted_lower = adjusted_mean - 1.96*standard_error,
                       adjusted_upper = adjusted_mean + 1.96*standard_error)]

# Calculate the corrected number of cases based on the new mean and the original sample size
final[, cases := adjusted_mean * sample_size]


#----------------------------------------------------------------------------------
# Write out files for future diagnostics
#----------------------------------------------------------------------------------
#save one file for generating diagnostics
diagnostics <- final[clinical_data_type == 'inpatient', 
                     .(location_id, age_start, age_end, sex, year_start, year_end, mean, adjusted_mean, is_outlier)]
write.csv(diagnostics, file = paste0('PATHNAME'), row.names = FALSE)

#save one file that is the full processed dataset
# add bv_lit_and_claims back onto the clinical data in final
final <- subset(final, select = -c(mean, standard_error, lower, upper, mean_clinical_data, mean_cf))
setnames(final, c('adjusted_mean', 'adjusted_se', 'adjusted_lower', 'adjusted_upper'),
         c('mean', 'standard_error', 'lower', 'upper'))

final <- rbind(final, bv_lit_and_claims, fill = TRUE)

write.xlsx(final, 
           file = "PATHNAME", sheetName = "SHEETNAME")


bv_lit <- bv_data_under1[clinical_data_type == '']
bv_lit_plots <- bv_lit[is.na(group_review) | group_review == 1, 
                       c('location_id', 'age_start', 'age_end', 'sex', 'year_start', 'year_end','mean', 'is_outlier')]
setnames(bv_lit_plots, 'mean', 'val')
bv_lit_plots$var <- 'Literature'
write.csv(bv_lit_plots, "PATHNAME", row.names = FALSE)



#----------------------------------------------------------------------------------
#format for st-gpr
#----------------------------------------------------------------------------------

#save one file that is the at birth data only, to be run in ST-GPR. upload to bundle 8066.
#can be from any source, as long as its age and sex specific
bun_id <- 338
final <- data.table(read.xlsx())
final[, measure := 'proportion']
final[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
setnames(final, 'mean', 'val')
final$year_start <- NULL
final$year_end <- NULL

final <- final[sex != 'Both']
final <- final[!(age_start == 0 & age_end == 0.999)]

ages <- get_age_metadata(age_group_set_id = 19)[, .(age_group_id, age_group_years_start, age_group_years_end)]
setnames(ages, c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
ages[age_start == 0.5, age_end := 0.999]

final <- merge(final, ages, by = c('age_start', 'age_end'), all.x = TRUE)
final[age_start == 0 & age_end == 0, age_group_id := 164]

# one last standard error check
final[(is.na(standard_error) | standard_error == 0),
        standard_error := (1/(1+z^2/sample_size)) * sqrt(val*(1-val)/sample_size + z^2/(4*sample_size^2))]
final[, variance := standard_error^2]

bun_id <- 8066
upload_filepath <- paste0("PATHNAME")
write.xlsx(final, 
           file = upload_filepath,
           sheetName = 'extraction')

#upload to stgpr bundle, then save a bundle version:
#1. delete all the existing data in the bundle
dt <- get_bundle_data(bundle_id = bun_id, decomp_step = 'iterative', gbd_round_id = 7)
cols <- names(dt)
dt[, (cols[cols != 'seq']) := '']
filepath <- "PATHNAME"
write.xlsx(dt, file = filepath, sheetName = 'extraction')
upload_bundle_data(bundle_id = bun_id, filepath = filepath, decomp_step = 'iterative', gbd_round_id = 7)

#2. upload new data
upload_bundle_data(bundle_id = bun_id, filepath = upload_filepath, decomp_step = 'iterative', gbd_round_id = 7)

bv <- save_bundle_version(bundle_id = bun_id, decomp_step = 'iterative', gbd_round_id = 7,
                          include_clinical = NULL)

test <- get_bundle_version(bundle_version_id = bv$bundle_version_id, fetch = 'all')

