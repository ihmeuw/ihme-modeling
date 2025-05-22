## ******************************************************************************
##
## Purpose: Run regression of HAQI on EMR in MR-BRT
## Input:   EMR data extracted from literature and calculated from prevalence and 
##          CSMR
## Output:  Model fit object
## Created: 2020-07-22
## Last updated: 2020-07-22
##
## ******************************************************************************

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "PATHNAME"
  h <-"PATHNAME"
  my_libs <- "PATHNAME"
} else {
  j<- "PATHNAME"
  h<-"PATHNAME"
  my_libs <- "PATHNAME"
}

out_dir <- "PATHNAME"

pacman::p_load(data.table, msm, ggplot2, plotly)
source("PATHNAME/get_bundle_version.R")
source("PATHNAME/save_bundle_version.R")
source("PATHNAME/get_covariate_estimates.R")
source("PATHNAME/get_location_metadata.R")

prep_mrbrt <- function(df) {
  df$mean_log <- log(df$mean)
  df$se_log <- sapply(1:nrow(df), function(i) {
    mean_i <- as.numeric(df[i, "mean"])
    se_i <- as.numeric(df[i, "se"])
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  return(df)
}



## RUN
acause <- 'neonatal_enceph'
bun_id <- 337

#pull haqi to use as covariate
haqi <- get_covariate_estimates(1099, decomp_step='iterative', gbd_round_id = 7)[,.(location_id,year_id,mean_value)]
setnames(haqi,"mean_value","haqi")

#pull bundle data, drop any locs that are not part of standard loc set, and merge haqi
#bv <- save_bundle_version(bundle_id = bun_id, decomp_step = 'iterative', gbd_round_id = 7)
dt <- get_bundle_version(bundle_version_id = 30329, fetch = 'all')
dt[year_id < 1980, year_id := 1980]

# set up fixed effect on age and on sex
dt[age_group_id == 2, early_neonatal := 1]
dt[age_group_id == 3, early_neonatal := 0]

dt[sex == 'Male', male := 1]
dt[sex == 'Female', male := 0]

# try a run leaving the outliers in
#dt <- dt[is_outlier != 1]

# std_locs <- get_location_metadata(location_set_id = 101, gbd_round_id = 7)[, .(location_id, developed)]
# 
# model_data <- merge(dt, std_locs, by = 'location_id')
model_data <- merge(dt, haqi, by = c('location_id', 'year_id'))


#log transform data
model_data[, mean := mean + 0.001]
setnames(model_data, c('standard_error'), c('se'))
df <- prep_mrbrt(model_data)

#save data to file
write.csv(df, file = paste0(out_dir, "/gbd2020_mrbrt_data_with_outliers.csv"), row.names = FALSE)


#create dataset to predict proportions from MR-BRT results
all_locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 7)[, .(location_id)]
predict_values <- data.table(expand.grid(location_id = all_locs$location_id, year_id = c(1980:2022),
                                         early_neonatal = c(0,1), male = c(0,1)))
predict_values <- merge(predict_values, haqi, by = c('location_id', 'year_id'))
write.csv(predict_values, file = "PATHNAME")

# Launch MR-BRT run
cv <- 'haqi'
trim_percent_list <- c(10,20,30,40)

#Job specifications
username <- Sys.getenv("USER")
m_mem_free <- "-l m_mem_free=20G"
fthread <- "-l fthread=4"
runtime_flag <- "-l h_rt=01:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q i.q"
shell_script <- "-cwd PATHNAME/r_shell_ide.sh"

### change to your own repo path if necessary
script <- paste0(h, "/PATHNAME/07b_emr_mrbrt.R")
errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o PATHNAME")

for (i in trim_percent_list) {
  trim_percent <- i
  job_name <- paste0("-N", " mrbrt", trim_percent)
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P proj_neonatal", outputs_flag, errors_flag, shell_script, script, cv, trim_percent, out_dir)
  
  system(job)
}



#' GENERATE DRAWS FROM MODEL RESULTS
#' Create 1000 columns labeled "draw_*"
#' Generate 1000 draws from the input data (assuming a normal distribution)
#' Take all the columns labeled "draw" and melt into one column. This means there is now a column called draw.id with
#' values from "draw_0" through "draw_999" and a column called "value" which will contain the draw value for each
#' of the 1000 draws.
trim <- 20
predict_values <- fread('PATHNAME/model_summaries.csv')

#transform out of log space, and remove the offset
predict_values[, emr := exp(Y_mean)]
predict_values[, emr_lower := exp(Y_mean_lo)]
predict_values[, emr_upper := exp(Y_mean_hi)]

predict_values[, emr := emr - 0.001]
predict_values[, emr_lower := emr_lower - 0.001]
predict_values[, emr_upper := emr_upper - 0.001]

predict_values[(emr_upper - emr) > (emr - emr_lower), emr_se := emr_lower / 0.975]
predict_values[(emr_upper - emr) < (emr - emr_lower), emr_se := emr_upper / 0.975]

#' SAVE OUTPUT FILES
predict_values <- predict_values[, .(X_early_neonatal, X_male, X_haqi, emr, emr_se)]
predict_values[X_male == 1, sex_id := 1]
predict_values[X_male == 0, sex_id := 2]
predict_values[X_early_neonatal == 1, age_group_id := 2]
predict_values[X_early_neonatal == 0, age_group_id := 3]
predict_values$X_early_neonatal <- NULL
predict_values$X_male <- NULL

setnames(predict_values, c('X_haqi'), c('haqi'))
predict_values$haqi <- as.character(predict_values$haqi)

labels <- fread(file = paste0(out_dir,'/gbd2020_mrbrt_predict_template.csv'))
labels$haqi <- as.character(labels$haqi)
labels[male == 1, sex_id := 1]
labels[male == 0, sex_id := 2]
labels[early_neonatal == 1, age_group_id := 2]
labels[early_neonatal == 0, age_group_id := 3]
labels$early_neonatal <- NULL
labels$male <- NULL

final <- merge(labels, predict_values, by = c('haqi', 'sex_id', 'age_group_id'))

#save summary files (means instead of draws)
write.csv(final, file = paste0(out_dir, '/neonatal_enceph_emr_summary_gbd20_mrbrt_results_trim',trim,'_0728.csv'), row.names=F)


draw_cols <- paste0("draw_", 0:999)
final[,draw_cols] <- 0 
final[, split.id := .I]

draws <- melt.data.table(final, id.vars = names(final)[!grepl("draw", names(final))], 
                         measure.vars = patterns("draw"),
                         variable.name = 'draw.id')


username <- Sys.getenv("USER")
m_mem_free <- "-l m_mem_free=20G"
fthread <- "-l fthread=4"
runtime_flag <- "-l h_rt=01:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q i.q"
shell_script <- "-cwd PATHNAME/r_shell_ide.sh"
### change to your own repo path if necessary
script <- paste0("PATHNAME/07c_emr_generate_draws.R")
errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o PATHNAME")

for (year_i in c(1990:2022)) {
  write.csv(draws[year_id == year_i], row.names = FALSE,
            file = paste0(out_dir, '/emr_draws_by_year/trim',trim,'/',year_i,'.csv'))
  
  job_name <- paste0("-N emr_draws_", year_i)
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P proj_neonatal", outputs_flag, errors_flag, shell_script, script, year_i, out_dir, trim)
  
  system(job)
}

means <- data.table()
for (year_i in c(1990:2022)) {
  dt <- fread(file = paste0(out_dir, "PATHNAME", '/collapsed_',year_i,'.csv'))
  
  means <- rbind(means, dt, fill = TRUE)
}


#save draws
write.csv(means, file = paste0(out_dir, '/neonatal_enceph_modeled_emr_draws_trim',trim,'_0728.csv'), row.names=F)