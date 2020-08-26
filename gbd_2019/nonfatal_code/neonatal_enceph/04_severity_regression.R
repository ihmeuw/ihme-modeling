## ******************************************************************************
##
## Purpose: Run regression of mild and mod/sev proportions on HAQI using
##          standard locations, then predict mild and mod/sev proportions for
##          all locations based on the results of the regression. 
## Input:   Locations
## Output:  Draws of mild and mod/sev proportions
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
}

out_dir <- "FILEPATH"

pacman::p_load(data.table, ggplot2, openxlsx)
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_location_metadata.R")

acause <- 'neonatal_enceph'
guide <- fread('FILEPATH/dimensions_GBD2017.csv')
guide <- guide[acause == 'neonatal_enceph' & (standard_grouping == "mild_prop" | standard_grouping == "modsev_prop")]

me_id_list <- guide$modelable_entity_id
bundle_id <- guide$bundle_id
ref_me_id <- guide[!is.na(ref_me_id), ref_me_id]
dummy_me_id <- me_id_list[me_id_list != ref_me_id]
me_id_count <- length(me_id_list)

#pull haqi to use as covariate
haqi <- get_covariate_estimates(1099, decomp_step='step4')[,.(location_id,year_id,mean_value)]
setnames(haqi,"mean_value","haqi")

#pull bundle data, drop any locs that are not part of standard loc set, and merge haqi
mild <- data.table(read.xlsx("FILEPATH/mild_proportion_data.xlsx"))
setnames(mild, 'note_SR', 'note_sr')
mild[, mild := 1]
modsev <- get_bundle_data(bundle_id = 91, decomp_step = 'step1', export = FALSE)
modsev[, mild := 0]
dt <- rbind(mild, modsev)

dt[, year_id := floor((year_end + year_start) / 2)]
dt[year_id < 1980, year_id := 1980]
dt <- dt[is_outlier != 1]

std_locs <- get_location_metadata(location_set_id = 101, gbd_round_id = 6)[, .(location_id, developed)]

model_data <- merge(dt, std_locs, by = 'location_id')
model_data <- merge(model_data, haqi, by = c('location_id', 'year_id'))


#run model
model_data[, mean := mean + 0.001]
model_data[, log_mean := log(mean)]

model <- lm(log_mean ~ haqi + mild, data = model_data)

#predict out for all locations based on HAQI
all_locs <- get_location_metadata(location_set_id = 9, gbd_round_id = 6)[, .(location_id, developed)]
predict_values <- data.table(expand.grid(location_id = all_locs$location_id, year_id = c(1980:2019), 
                                         mild = c(0,1)))
predict_values <- merge(predict_values, haqi, by = c('location_id','year_id'))
predict_values[, `:=` (unique_id = 1:.N)]
predict_values_vector <- predict(model, newdata = predict_values, se.fit = TRUE)
predict_values <- cbind(predict_values, est_proportion = predict_values_vector$fit, est_se = predict_values_vector$se.fit)

 
draw_cols <- paste0("draw_", 0:999)
predict_values[,draw_cols] <- 0 

draws <- melt.data.table(predict_values, id.vars = names(predict_values)[!grepl("draw", names(predict_values))], 
                         measure.vars = patterns("draw"),
                         variable.name = 'draw.id')

mean.vector <- draws$est_proportion
se.vector <- draws$est_se
set.seed(123)
input.draws <- rnorm(length(mean.vector), mean.vector, se.vector)

draws[, value := input.draws]

#transform out of log space, and remove the offset
draws[, value := exp(value)]
draws[, value := value - 0.001]

#dcast back into rows of draws
final <- dcast(draws, location_id + year_id + mild + haqi + unique_id + est_proportion + est_se ~ draw.id, value.var = 'value')


#' SAVE OUTPUT FILES
setnames(final, 'year_id', 'year')
final <- final[, -c('haqi', 'unique_id')]

#save summary files (means instead of draws)
write.csv(final[mild == 1, .(location_id, year, est_proportion, est_se)], file = paste0(out_dir, '/neonatal_enceph_long_mild_summary.csv'), row.names=F)
write.csv(final[mild == 0, .(location_id, year, est_proportion, est_se)], file = paste0(out_dir, '/neonatal_enceph_long_modsev_summary.csv'), row.names=F)

#duplicate by sex
final[, sex := 1]
final_female <- copy(final)
final_female[, sex := 2]
final <- rbind(final, final_female)

#save draws
write.csv(final[mild == 1, -c('mild', 'est_proportion', 'est_se')], file = paste0(out_dir, '/neonatal_enceph_long_mild_draws.csv'), row.names=F)
write.csv(final[mild == 0, -c('mild', 'est_proportion', 'est_se')], file = paste0(out_dir, '/neonatal_enceph_long_modsev_draws.csv'), row.names=F)

