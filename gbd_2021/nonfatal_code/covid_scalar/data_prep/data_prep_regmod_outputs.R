##################################################
## Project: CVPDs
## Script purpose: Prep outputs of regmod to be inputs to cascading spline model
## Date: May 4, 2021
## Author: USERNAME
##################################################
rm(list=ls())

pacman::p_load(data.table, openxlsx, ggplot2, matrixStats, magrittr)
username <- Sys.info()[["user"]]
gbd_round_id <- 7
decomp_step <- "step3"

# SOURCE SHARED FUNCTIONS ------------------------------
shared_functions <- "FILEPATH"
functions <- c("get_location_metadata.R")
for (func in functions) {
 source(paste0(shared_functions, func))
}
source("/FILEPATH/collapse_point.R")

# ARGUMENTS -------------------------------------
data_prep_descrip <- "Added december for IRQ and EGY to flu drop. TUR in drop locs for flu. Switch to RegMod with last knot in 2017 for Flu with more locs added, updated addl drop locs. Switch to mean of janfeb obs and pred vals (because sum caused probs with loc-months with NA obs), then take ratio for std. Drop fake 0 rows. Keep J/F China. Floor at 99pct disrup (reset anything < 1% of expected, instead of only resetting 0s). Adding measles drop locs beyond MDG (visually idenitified). Dropping rows with inf draws. Using point prediction to calculate ratio. standardizing to Jan/Feb. Regmod output data prep -- v3 (population offset) model. Flu - Dropping locs with >20% missing months or cases below the 5th percentile of reported cases in DR countries. data_drop column to ID rows to drop."
apply_offset <- "99_disrup_cap" # options are "offset_all" (apply offset to both 2020 and prev_yrs_avg values that are 0),
              # "only_2020" (drop rows with prev_yrs_avg of 0, of new dt offset rows with '2020'==0)
              # "only_one" (only offset rows that have either 2020==0 OR prev_yrs_avg==0, drop rows with prev_yrs_avg==0 AND 2020==0)
              # "drop_all" (drop all rows that have 2020==0 or prev_yrs_avg==0)
              # "99_disrup_cap" (cap disruptions at 99% reduction (so can't have 0 cases if nonzero in prev yr))
last_flu_knot <- 2017 #use flu RegMod results from model with last knot when?
keep_fake_zeros <- FALSE   ## in RegMod outputs, NAs in obs cases after Mar 2020 have been replaced by 0s for some early model runs. keep (TRUE) or drop (FALSE) these 0s?
drop_inf_draw_rows <- TRUE
offset <- NA
error_caps_by_cause <- TRUE
measles_standardization <-"janfeb" #"none"
flu_standardization <- "janfeb"#"none"
flu_drop_locs <- "5pctle and 20% missing with added manual locs"
meas_drop_locs <- "visually identified locs" #"visually identified locs"

date <- "2021-06-07-B"
## Directories -------------------------------------------------------------
out_dir <- file.path("/FILEPATH/spline_cascade_models", date)
if(!dir.exists(out_dir)) dir.create(out_dir, recursive = T)

## Read in RegMod results
measles_model_name <- "v3"
measles_regmod_dir <- paste0("FILEPATH/Flu_Measles_final/measles/", measles_model_name)
measles_regmod_out_dir <- paste0(measles_regmod_dir, "/Output/")
all.files <- list.files(path = measles_regmod_out_dir, pattern = ".csv", full.names = T)
temp <- lapply(all.files, read.csv)
meas_data <- as.data.table(rbindlist(temp))
setnames(meas_data, "deaths", "observed_cases")
setnames(meas_data, "cases", "predicted_cases")
meas_data[, cause := "measles"]

if(last_flu_knot == 2019){
  flu_model_name <- "v3"
  flu_regmod_dir <- paste0("FILEPATH/Flu_Measles_final/flu/", flu_model_name)
} else if (last_flu_knot == 2017){
  flu_regmod_dir <- paste0("FILEPATH/Flu_Measles_final/pred2021/3versions_missing/flu/Jan2017_0604") # this version includes new, unoutliered locations
}

flu_regmod_out_dir <- paste0(flu_regmod_dir, "/Output/")
all.files <- list.files(path = flu_regmod_out_dir, pattern = ".csv", full.names = T)
temp <- lapply(all.files, read.csv)
flu_data <- as.data.table(rbindlist(temp))
setnames(flu_data, "deaths", "observed_cases")
setnames(flu_data, "cases", "predicted_cases")
flu_data[, cause := "flu"]

data <- rbind(meas_data, flu_data)
data <- data[year_id == 2020]

# merge on location info
hierarchy <-  get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
countries <- hierarchy[level == 3]$location_id
data <- merge(data, hierarchy[,.(location_name, location_id, ihme_loc_id, super_region_name, super_region_id, region_name, region_id, level)],
              by = "location_id")

## Flag rows to drop and/or apply offset --------------------------------------------------------------
data[, data_drop := 0]

if(!keep_fake_zeros){
  flu_regmod_input_dir <- paste0(flu_regmod_dir, "/Input/")
  all.files <- list.files(path = flu_regmod_input_dir, pattern = ".csv", full.names = T)
  temp <- lapply(all.files, read.csv)
  flu_input_data <- as.data.table(rbindlist(temp))
  setnames(flu_input_data, "deaths","observed_cases")

  flu_fake_0 <- flu_input_data[real_0 == 0 & observed_cases == 0 & year_id == 2020, .(location_id, month, year_id, fake_0 = 1, cause = "flu")] # when last knot in 2017, no fake 0s because had also updated RegMod code to keep NAs instead of replacing with 0s before reran with last knot in 2017

  # repeat for measles
  meas_regmod_input_dir <- paste0(measles_regmod_dir, "/Input/")
  all.files <- list.files(path = meas_regmod_input_dir, pattern = ".csv", full.names = T)
  temp <- lapply(all.files, read.csv)
  meas_input_data <- as.data.table(rbindlist(temp))
  setnames(meas_input_data, "deaths","observed_cases")

  meas_fake_0 <- meas_input_data[real_0 == 0 & observed_cases == 0 & year_id == 2020, .(location_id, month, year_id, fake_0 = 1, cause = "measles")] # 80 rows

  fake_0 <- rbind(meas_fake_0, flu_fake_0)

  # ID rows of data that are fake 0s
  data <- merge(data, fake_0, by = c("location_id", "year_id", "month", "cause"), all.x = TRUE) %>% .[is.na(fake_0), fake_0 := 0]

  data <- data[fake_0 == 1, data_drop := 1]
}

# flag to drop location months with NA for 2017-2019 (ie prev_yrs_avg is NA) and/or 2020 because this will lead to ratio_avg = NA
data[is.na(observed_cases), data_drop := 1]
data[is.na(predicted_cases), data_drop := 1]



## OFFSET zeros so ratio not undefined and log(ratio) not undefined or DROP ALL 0s or CAP at 99% disrup (0 cases wld be 100% disrup)
if(apply_offset == 'offset_all'){
  data[observed_cases == 0, observed_cases := offset]
  data[predicted_cases == 0, predicted_cases := offset]

} else if(apply_offset == "only_2020"){
  data[(predicted_cases == 0), data_drop := 1] # only keep rows that will have a non-zero denominator
  data[observed_cases == 0, observed_cases := offset]

} else if (apply_offset == "only_one"){
  data[(predicted_cases == 0 & observed_cases == 0), data_drop := 1] # drop rows with 0 in previous years and 2020
  data[observed_cases == 0 & predicted_cases != 0, `2020` := offset]
  data[predicted_cases == 0, predicted_cases := offset]
  # check that worked!
  if(nrow(data[predicted_cases==0 & data_drop == 0])>0) message("Caution - offset didn't work: still have prev_yrs_avg == 0 that aren't offset or being dropped")
  if(nrow(data[observed_cases==0 & data_drop ==0])>0) message("Caution - offset didn't work: still have rows `2020` == 0 and not offset or being dropped")
  if(nrow(data[observed_cases==0 & predicted_cases==0 & data_drop == 0])>0) message(paste0("Caution - data drop flag for offset type (type = ", use_offset, ") didn't work: still have rows with `2020` == 0 AND prev_yrs_avg == 0 that aren't flagged to be dropped"))

} else if (apply_offset == "drop_all") {
  data[(predicted_cases == 0 | observed_cases == 0), data_drop := 1]

} else if (apply_offset == "99_disrup_cap"){
  data[observed_cases < predicted_cases * (1-0.99), observed_cases := predicted_cases * (1-0.99)] # cap at 99% disrup from expected (otherwise would have undefined log ratio for these rows and drop many rows)
  data[predicted_cases == 0, data_drop := 1]

}

if(flu_drop_locs=="5pctle and 20% missing"){
  # drop the flu data from locations with >20% missing or under 5pctle number of cases in DR locs
  sorted_flu_locs <- read.xlsx("/FILEPATH/misc_input_data/flu_drop_locs.xlsx")
  drop_locs <- unique(c(sorted_flu_locs$`5.percentile`, sorted_flu_locs$`20%.missing`))
  data[(cause == "flu" & (ihme_loc_id %in% drop_locs)), data_drop := 1]
} else if (flu_drop_locs == "5pctle and 20% missing with added manual locs"){
  if(last_flu_knot == 2017){
    flu_drop_locs <- c("IND", "JAM", "IRN", "NIC", "QAT", "LTU", "TUR")
    data[cause == "flu" & ihme_loc_id %in% flu_drop_locs, data_drop := 1]
    flu_dec_drop_locs <- c("EGY", "IRQ")
    data[cause == "flu" & ihme_loc_id %in% flu_dec_drop_locs & month == 12, data_drop := 1]
  } else if (last_flu_knot == 2019){
    flu_include_locs <- read.xlsx("FILEPATH/misc_input_data/loc_include_final.xlsx") %>% as.data.table()
    setnames(flu_include_locs, "loc_list", "ihme_loc_id")
    data[(cause == "flu" & !(ihme_loc_id %in% flu_include_locs$ihme_loc_id)), data_drop := 1]

  }


}

if(meas_drop_locs == "visually identified locs"){
  measles_drop_locs <- read.xlsx("/FILEPATH/misc_input_data/meas_drop_locs.xlsx")
  data[cause == "measles" & ihme_loc_id %in% measles_drop_locs$ihme_loc_id, data_drop := 1]
} else if (meas_drop_locs == "MDG"){
  #don't use madagascar because 2019 outbreak giving high ratios when std. to jan feb
  data[location_id == hierarchy[ihme_loc_id == "MDG", location_id] & cause == "measles", data_drop := 1]
}

## Start ratio calculation ------------------------------------------------------------------------
case_draw_cols <- paste0("cases_draw_", 0:999)
data[, eval(case_draw_cols) := lapply(.SD, as.numeric), .SDcols = case_draw_cols]

# calculate draws of ratio of obs (reported number cases) to expected (reg mod prediction)
ratio_draws <- paste0("ratio_draw_", 0:999)
data[, (ratio_draws) := observed_cases/.SD, .SDcols = case_draw_cols]

if(drop_inf_draw_rows){
  data[, inf_case_flag := rowMeans(as.matrix(.SD)), .SDcols = case_draw_cols]
  data[is.infinite(inf_case_flag), data_drop := 1] # 110 total rows, 81 add'l getting dropped rows if not dropping fake 0s, 45 addl if drop fake 0s, 45 addl with flu last knot 2017 and drop fake 0s
}

# calculate ratio using mean obs and pred rather than mean of ratio draws (rows with inf draws do not have inf for mean pred)
data[, ratio := observed_cases/predicted_cases]

if(nrow(data[is.na(ratio) & data_drop != 1]) > 0) message("You have ",nrow(data[is.na(ratio) & data_drop != 1]), " rows with ratio = NA and not flagged to be dropped. Something didn't work with flagging rows with ratio = NA or observed_cases = NA to be dropped!" )
if(nrow(data[is.infinite(ratio) & data_drop != 1]) > 0) message("You have ",nrow(data[is.infinite(ratio) & data_drop != 1]), " rows with ratio_avg = inf that are not flagged to be dropped or have not been offset. Make sure offsetting process worked!" )

## calculate jan/feb combo ratio to standardize to
janfeb <- data[month %in% c(1,2)]
janfeb_combo <- janfeb[,.(janfeb_obs = mean(observed_cases, na.rm = T), janfeb_pred = mean(predicted_cases, na.rm=T)), by = c("location_id", "cause")]
janfeb_combo[, janfeb_ratio_avg := janfeb_obs/janfeb_pred]

# Calculate standardized ratios for sources with ratio of ratio ---------------------------------------------
standardized_data <- data[!(month %in% c(1, 2)) | location_id == 6]
standardized_data <- merge(standardized_data,
                           janfeb_combo[,.(location_id, janfeb_obs, janfeb_pred, janfeb_ratio_avg, cause)],
                           by=c("location_id", "cause"))

# drop loc-months where jan-feb were flagged to drop but later months were not previously
standardized_data[(janfeb_ratio_avg == 0 | is.na(janfeb_ratio_avg) | is.infinite(janfeb_ratio_avg)), data_drop := 1]
if(measles_standardization == "janfeb") {
  standardized_data[cause == "measles", standardized_ratio := ratio/janfeb_ratio_avg]

  std_ratio_draws <- paste0("std_ratio_draw_", 0:999)
  standardized_data[, (std_ratio_draws) := .SD/janfeb_ratio_avg, .SDcols = ratio_draws]

  # this eqn from the data_prep_vaccines script, orig source: https://sphweb.bumc.bu.edu/otlt/MPH-Modules/PH717-QuantCore/PH717_ComparingFrequencies/PH717_ComparingFrequencies8.html
  standardized_data[cause == "measles", ratio_se_log := sqrt(1/predicted_cases + 1/observed_cases + 1/janfeb_obs + 1/janfeb_pred)]
} else if (measles_standardization == "none"){
  # calculate mean and se of ratio in log space to pass to mrbrt
  log_ratio_draws <- paste0("log_ratio_draw_",0:999)
  standardized_data[cause == "measles", (log_ratio_draws) := log(.SD), .SDcols = ratio_draws]
  standardized_data[cause == "measles", `:=` (log_ratio = rowMeans(as.matrix(.SD)), ratio_se_log = rowSds(as.matrix(.SD))), .SDcols = log_ratio_draws]
}
if(flu_standardization == "janfeb"){
  standardized_data[cause == "flu", standardized_ratio := ratio/janfeb_ratio_avg]

  std_ratio_draws <- paste0("std_ratio_draw_", 0:999)
  standardized_data[, (std_ratio_draws) := .SD/janfeb_ratio_avg, .SDcols = ratio_draws]

  standardized_data[cause == "flu", ratio_se_log := sqrt(1/predicted_cases + 1/observed_cases + 1/janfeb_obs + 1/janfeb_pred)]
} else if (flu_standardization == "none"){
  # calculate mean and se of ratio in log space to pass to mrbrt
  log_ratio_draws <- paste0("log_ratio_draw_",0:999)
  standardized_data[cause == "flu", (log_ratio_draws) := log(.SD), .SDcols = ratio_draws]
  standardized_data[cause == "flu", `:=` (log_ratio = rowMeans(as.matrix(.SD)), ratio_se_log = rowSds(as.matrix(.SD))), .SDcols = log_ratio_draws] # note: the mean of the log ratio draws is the same as log(ratio) where ratio is the mean of the ratio draws in linear space

}

## Examine and cap standard error -------------------------
# visualize standard error
pdf(paste0(out_dir, "/se_hist_by_cause_undropped_rows.pdf"))
  print(hist(standardized_data[cause == "measles" & data_drop == 0]$ratio_se_log, main = "Histogram of measles log(standard error of modeled ratio)", xlab = "Standard error (log space)"))
  print(hist(standardized_data[cause == "flu" & data_drop == 0]$ratio_se_log, main = "Histogram of flu log(standard error of modeled ratio)", xlab = "Standard error (log space)"))
dev.off()

# cap standard error
if(error_caps_by_cause){
  error_lo_meas <- quantile(standardized_data[data_drop == 0 & cause == "measles", ratio_se_log], 0.05)
  error_hi_meas <- quantile(standardized_data[data_drop == 0 & cause == "measles", ratio_se_log], 0.95)
  error_lo_flu <- quantile(standardized_data[data_drop == 0 & cause == "flu", ratio_se_log], 0.05)
  error_hi_flu <- quantile(standardized_data[data_drop == 0 & cause == "flu", ratio_se_log], 0.95)
  standardized_data_capped  <- copy(standardized_data)
  standardized_data_capped[ratio_se_log > error_hi_meas & cause == "measles", ratio_se_log := error_hi_meas] #35 rows
  standardized_data_capped[ratio_se_log < error_lo_meas & cause == "measles", ratio_se_log := error_lo_meas] #31 rows
  standardized_data_capped[ratio_se_log > error_hi_flu & cause == "flu", ratio_se_log := error_hi_flu]  #57 rows
  standardized_data_capped[ratio_se_log < error_lo_flu & cause == "flu", ratio_se_log := error_lo_flu] #17 rows
} else {
  error_lo <- quantile(standardized_data[data_drop == 0, ratio_se_log], 0.05)
  error_hi <- quantile(standardized_data[data_drop == 0, ratio_se_log], 0.95)
  standardized_data_capped  <- copy(standardized_data)
  standardized_data_capped[ratio_se_log > error_hi, ratio_se_log := error_hi]
  standardized_data_capped[ratio_se_log < error_lo, ratio_se_log := error_lo]
}


## Clean data to feed into mr-brt ------------------------
if(measles_standardization == "janfeb"){
  cols_keep <- c("location_id", "month", "standardized_ratio", "ratio_se_log", "cause", "data_drop", "janfeb_ratio_avg", "janfeb_obs", "janfeb_pred", "ratio", "observed_cases", "predicted_cases", std_ratio_draws, "fake_0")
  out_w_ratio_draws <- standardized_data_capped[ ,cols_keep, with = FALSE]

  out <- standardized_data_capped[,.(location_id, month, standardized_ratio, ratio_se_log, cause, data_drop, janfeb_ratio_avg, janfeb_obs, janfeb_pred, ratio, observed_cases, predicted_cases, fake_0)]
  out[, lower := exp(log(standardized_ratio) - 1.96*ratio_se_log)]
  out[, upper := exp(log(standardized_ratio) + 1.96*ratio_se_log)]
  out[, log_ratio := log(standardized_ratio)]
  out[, plotting_ratio := standardized_ratio]
} else if (measles_standardization == "gbd_modeled_cases"){
  out <- standardized_data_capped[,.(location_id, month, standardized_ratio, ratio_se_log, cause, data_drop, modeled_meas_ratio_avg, gbd_modeled_meas_2020, modeled_meas_prev_yrs_avg, ratio_avg, `2020`, prev_yrs_avg,  janfeb_ratio_avg, janfeb_combo_2020, prev_yrs_janfeb_avg)]
  out[, lower := exp(log(standardized_ratio) - 1.96*ratio_se_log)]
  out[, upper := exp(log(standardized_ratio) + 1.96*ratio_se_log)]
  out[, log_ratio := log(standardized_ratio)]
} else if (measles_standardization == "none"){
  out <- standardized_data_capped[,.(location_id, month, ratio, log_ratio, ratio_se_log, cause, data_drop, observed_cases, predicted_cases)]
  out[, lower := exp(log(ratio) - 1.96*ratio_se_log)]
  out[, upper := exp(log(ratio) + 1.96*ratio_se_log)]
  out[, plotting_ratio := ratio]
}
out[, weight := 1/ratio_se_log^2] # this is what mr-brt calcs for weight behind the scenes, just calculating here for visualization purposes
out[, end_date := timeLastDayInMonth(paste0("2020-", month, "-1")) %>% as.Date()]
out[cause == "measles", measles := 1]
out[cause == "flu", flu := 1]
out[is.na(measles), measles := 0]
out[is.na(flu), flu := 0]


## Save data and descrip/metadata
cat(paste0(data_prep_descrip, "\nOffset type: ", apply_offset, ". Offset value: ", offset),
    file=file.path(out_dir, "DATA_PREP_DESCRIPTION.txt"), sep = "\n")
cat(paste0("SE caps: error_lo_meas = ", error_lo_meas, " error_hi_meas = ", error_hi_meas, "; error_lo_flu = ", error_lo_flu, " error_hi_flu = ", error_hi_flu), append = TRUE,
    file=file.path(out_dir, "DATA_PREP_DESCRIPTION.txt"))
cat(paste0("\nStandardization: measles = ", measles_standardization, "; flu = ", flu_standardization,
           "\nFlu data drop: ", paste0(flu_drop_locs, sep = ", "), "\nMeasles data drop: ", meas_drop_locs),
    file=file.path(out_dir, "DATA_PREP_DESCRIPTION.txt"),
    append= TRUE)
cat(paste0("\nDrop infinite draw rows: ", drop_inf_draw_rows),
    file=file.path(out_dir, "DATA_PREP_DESCRIPTION.txt"),
    append= TRUE)

write.csv(standardized_data, file.path(out_dir, "data_prepped_uncapped_se.csv"), row.names = F)

write.csv(out, file.path(out_dir, "data_mrbrt_source.csv"), row.names = F)

write.csv(out_w_ratio_draws, file.path(out_dir, "data_capped_w_ratio_draws.csv"), row.names = F)
