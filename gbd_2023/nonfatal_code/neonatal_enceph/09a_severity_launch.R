## ******************************************************************************
##
## Purpose: Run regression of mild and mod/sev proportions on HAQI using
##          standard locations, then predict mild and mod/sev proportions for
##          all locations based on the results of the regression. 
## Input:   Locations
## Output:  Draws of mild and mod/sev proportions
## Created: 2019-02-07
## Last updated: 2020-05-06
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
  my_libs <- paste0(h, "/cluster_packages")
}

out_dir <- "/PATHNAME/s"

pacman::p_load(data.table, msm, ggplot2, plotly)
source("PATHNAME/get_bundle_version.R")
source("PATHNAME/get_covariate_estimates.R")
source("PATHNAME/get_location_metadata.R")

prep_mrbrt <- function(df) {
  df$mean_logit <- log(df$mean / (1- df$mean))
  df$se_logit <- sapply(1:nrow(df), function(i) {
    mean_i <- as.numeric(df[i, "mean"])
    se_i <- as.numeric(df[i, "se"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  return(df)
}



## RUN
acause <- 'neonatal_enceph'

#pull haqi to use as covariate
haqi <- get_covariate_estimates(1099, decomp_step='iterative', gbd_round_id = 7)[,.(location_id,year_id,mean_value)]
setnames(haqi,"mean_value","haqi")

#pull bundle data, drop any locs that are not part of standard loc set, and merge haqi
dt <- get_bundle_version(bundle_version_id = 24812, fetch = 'all')
dt[bundle_name == 'mild', mild := 1]
dt[bundle_name == 'moderate-severe', mild := 0]

dt[, year_id := floor((year_end + year_start) / 2)]
dt[year_id < 1980, year_id := 1980]
# try a run leaving the outliers in
#dt <- dt[is_outlier != 1]

std_locs <- get_location_metadata(location_set_id = 101, gbd_round_id = 7)[, .(location_id, developed)]

model_data <- merge(dt, std_locs, by = 'location_id')
model_data <- merge(model_data, haqi, by = c('location_id', 'year_id'))

#pool sex-specific data into both sex aggregates
model_data[sex != 'Both', `:=` (cases = sum(.SD$cases),
                                sample_size = sum(.SD$sample_size),
                                sex = 'Both'),
           by=c('nid', 'location_id', 'year_id', 'mild')]

model_data[nid == 127888, cases := 4]

model_data <- model_data[!duplicated(model_data[,c('nid', 'location_id', 'year_id', 'mild', 'sex','case_definition')]),]


#logit transform data
model_data[, mean := mean + 0.001]
setnames(model_data, c('standard_error'), c('se'))
df <- prep_mrbrt(model_data)

#save data to file
write.csv(df, file = "PATHNAME", row.names = FALSE)


#create dataset to predict proportions from MR-BRT results
all_locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 7)[, .(location_id, developed)]
predict_values <- data.table(expand.grid(location_id = all_locs$location_id, year_id = c(1980:2022),
                                         mild = c(0,1)))
predict_values <- merge(predict_values, haqi, by = c('location_id', 'year_id'))
write.csv(predict_values, file = paste0(out_dir,'/gbd2020_mrbrt_predict_template.csv'), row.names = FALSE)

# Launch MR-BRT run
cv <- 'haqi'
trim_percent_list <- c(20)

#Job specifications
username <- Sys.getenv("USER")
m_mem_free <- "-l m_mem_free=12G"
fthread <- "-l fthread=4"
runtime_flag <- "-l h_rt=01:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q i.q"
shell_script <- "-cwd PATHNAME/r_shell_ide.sh"

### change to your own repo path if necessary
script <- "/PATHNAME"
errors_flag <- paste0("-e PATHNAME")
outputs_flag <- paste0("-o ut")

for (i in trim_percent_list) {
  trim_percent <- i
  job_name <- paste0("-N", " mrbrt", trim_percent)
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P proj_neonatal", outputs_flag, errors_flag, shell_script, script, cv, trim_percent)
  
  system(job)
}



#' GENERATE DRAWS FROM MODEL RESULTS
#' Create 1000 columns labeled "draw_*"
#' Generate 1000 draws from the input data (assuming a normal distribution)
#' Take all the columns labeled "draw" and melt into one column. This means there is now a column called draw.id with
#' values from "draw_0" through "draw_999" and a column called "value" which will contain the draw value for each
#' of the 1000 draws.
predict_values <- fread(paste0(out_dir,"PATHNAME.csv"))

#transform out of logit space, and remove the offset
predict_values[, est_proportion := exp(Y_mean) / (1+exp(Y_mean))]
predict_values[, est_lower := exp(Y_mean_lo) / (1+exp(Y_mean_lo))]
predict_values[, est_upper := exp(Y_mean_hi) / (1+exp(Y_mean_hi))]

predict_values[, est_proportion := est_proportion - 0.001]
predict_values[, est_lower := est_lower - 0.001]
predict_values[, est_upper := est_upper - 0.001]

predict_values[, est_se := (est_upper-est_lower)/3.92]

#' SAVE OUTPUT FILES
predict_values <- predict_values[, .(X_mild, X_haqi, est_proportion, est_se)]
setnames(predict_values, c('X_mild', 'X_haqi'), c('mild', 'haqi'))
predict_values$haqi <- as.character(predict_values$haqi)

labels <- fread(file = paste0(out_dir,'/gbd2020_mrbrt_predict_template.csv'))
labels$haqi <- as.character(labels$haqi)
final <- merge(labels, predict_values, by = c('haqi', 'mild'))

#save summary files (means instead of draws)
write.csv(final[mild == 1], file = paste0(out_dir, '/neonatal_enceph_long_mild_summary_gbd20_mrbrt_inc_spline.csv'), row.names=F)
write.csv(final[mild == 0], file = paste0(out_dir, '/neonatal_enceph_long_modsev_summary_gbd20_mrbrt_inc_spline.csv'), row.names=F)


draw_cols <- paste0("draw_", 0:999)
final[,draw_cols] <- 0 

draws <- melt.data.table(final, id.vars = names(final)[!grepl("draw", names(final))], 
                         measure.vars = patterns("draw"),
                         variable.name = 'draw.id')

mean.vector <- draws$est_proportion
se.vector <- draws$est_se
set.seed(123)
input.draws <- rnorm(length(mean.vector), mean.vector, se.vector)

draws[, value := input.draws]

#dcast back into rows of draws
final <- dcast(draws, location_id + year_id + mild + haqi + est_proportion + est_se ~ draw.id, value.var = 'value')

#duplicate by sex
final[, sex := 1]
final_female <- copy(final)
final_female[, sex := 2]
final <- rbind(final, final_female)

setnames(final, 'year_id', 'year')

#save draws
write.csv(final[mild == 1, -c('mild', 'haqi', 'est_proportion', 'est_se')], file = paste0(out_dir, '/neonatal_enceph_long_mild_draws.csv'), row.names=F)
write.csv(final[mild == 0, -c('mild', 'haqi', 'est_proportion', 'est_se')], file = paste0(out_dir, '/neonatal_enceph_long_modsev_draws.csv'), row.names=F)