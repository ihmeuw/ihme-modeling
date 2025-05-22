########################################################################
##                                                                    ##
## Purpose: Graph stillbirth results and compare to previous versions ##
##                                                                    ##
########################################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(data.table)
library(ggplot2)
library(ggrepel)
library(grid)
library(gridExtra)
library(RColorBrewer)

library(plyr)
library(dplyr)

library(mortcore)
library(mortdb)

user <- "USERNAME"
root <- "FILEPATH"

## Get settings
args <- commandArgs(trailingOnly = TRUE)
new_settings_dir <- args[1]

if (interactive()) {
  version <- "Run id"
  main_std_def <- "28_weeks"
  new_settings_dir <- paste0("FILEPATH/new_run_settings_", main_std_def, ".csv")
}

load(new_settings_dir)
list2env(new_settings, envir = environment())

locs <- mortdb::get_locations(
  level = "all",
  gbd_year = gbd_year
)

source(paste0(shared_functions_dir, "get_location_metadata.R"))
locs_metadata <- get_location_metadata(
  location_set_id = 35,
  release_id = release_id
)

###############
## Load Data ##
###############

## Input data
data <- mortdb::get_mort_outputs(
  model_name = "stillbirth",
  model_type = "data",
  run_id = version_data,
  gbd_year = gbd_year,
  outlier_run_id = "active",
  hostname = hostname
)

setnames(data, "mean", "sbr")

data[is.na(outlier), outlier := 0]

## Add standard definition
std_defs <- mortdb::get_mort_ids(type = "std_def")

data <- merge(
  data,
  std_defs[, c("std_def_id", "std_def_short")],
  by = "std_def_id",
  all.x = TRUE
)

setnames(data, "std_def_short", "std_def")

## Update data types
source_types <- mortdb::get_mort_ids(type = "source_type")[, c("source_type_id", "type_short")]

data <- merge(
  data,
  source_types,
  by = "source_type_id",
  all.x = TRUE
)

setnames(data, "type_short", "source_type")

data[source_type == "Census", data_type := "census"]
data[source_type == "Survey", data_type := "survey"]
data[source_type == "Statistical Report", data_type := "gov_report"]
data[source_type == "Sci Lit", data_type := "literature"]
data[source_type == "VR", data_type := "vr"]

## Unadjusted data
data_unadj <- fread(
  paste0(data_dir, version_data, "/FILEPATH/stillbirth_input_data.csv")
)

data_unadj[data_type == "vital registration", data_type := "vr"]

data_unadj[, stage := "original data"]
data_unadj[, outlier_status := "included"]

data_unadj <- data_unadj[, c("ihme_loc_id", "year_id", "year", "data_type", "std_def", "stage", "sbr_unadj", "outlier_status")]

setnames(data_unadj, "sbr_unadj", "sbr")

## Prediction data
pred <- fread(
  paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirth_prediction_data_", main_std_def, ".csv")
)

## GPR input data
gpr_data_orig <- fread(
  paste0(estimate_dir, version_estimate, "/FILEPATH/gpr_input_", main_std_def, ".csv")
)

if (model == "SBR/NMR") {

  gpr_data_orig <- merge(
    gpr_data_orig,
    pred[, c("ihme_loc_id", "year_id", "q_nn_med")],
    by = c("ihme_loc_id", "year_id"),
    all.x = TRUE
  )

  gpr_data_orig[, sbr := exp(log_mean) * q_nn_med]
  gpr_data_orig[, sbr_adj := exp(log_mean_adj) * q_nn_med]

  gpr_data_orig[, pred_sbr := exp(pred_log_mean) * q_nn_med]
  gpr_data_orig[, pred_sbr_st := exp(pred_log_mean_st) * q_nn_med]

} else if (model == "SBR + NMR") {

  gpr_data_orig <- merge(
    gpr_data_orig,
    pred[, c("ihme_loc_id", "year_id", "q_nn_med")],
    by = c("ihme_loc_id", "year_id"),
    all.x = TRUE
  )

  gpr_data_orig[, sbr := exp(log_mean) - q_nn_med]
  gpr_data_orig[, sbr_adj := exp(log_mean_adj) - q_nn_med]

  gpr_data_orig[, pred_sbr := exp(pred_log_mean) - q_nn_med]
  gpr_data_orig[, pred_sbr_st := exp(pred_log_mean_st) - q_nn_med]

} else if (model == "SBR") {

  gpr_data_orig[, sbr := exp(log_mean)]
  gpr_data_orig[, sbr_adj := exp(log_mean_adj)]
  gpr_data_orig[, pred_sbr := exp(pred_log_mean)]
  gpr_data_orig[, pred_sbr_st := exp(pred_log_mean_st)]

}

gpr_data <- gpr_data_orig[ , c("year_id", "ihme_loc_id", "sbr", "sbr_adj",
                               "pred_sbr", "pred_sbr_st", "data_type", "std_def", "mse")]

setnames(gpr_data, c("sbr", "sbr_adj"), c("sbr_orig", "sbr"))

gpr_data[, year := year_id]

mse <- round(gpr_data_orig$mse[1], 3)

gpr_data <- melt(
  gpr_data,
  id.vars = c("ihme_loc_id", "year", "data_type", "std_def", "year_id"),
  value.name = "sbr",
  variable.name = "stage"
)

gpr_data <- gpr_data[!is.na(gpr_data$sbr),]

gpr_data$stage <- as.character(gpr_data$stage)

gpr_data[stage == "sbr_orig", stage := "completeness-adjusted data"]
gpr_data[stage == "sbr", stage := "crosswalk-adjusted data"]
gpr_data[stage == "pred_sbr", stage := "stage 1 estimates"]
gpr_data[stage == "pred_sbr_st", stage := "stage 2 estimates"]

##################
## Get Outliers ##
##################

crosswalk_outliers1 <- fread(
  paste0("FILEPATH/crosswalk_", defs[1], "_weeks_outliers.csv")
)
crosswalk_outliers2 <- fread(
  paste0("FILEPATH/crosswalk_", defs[2], "_weeks_outliers.csv")
)

crosswalk_outliers <- rbind(crosswalk_outliers1, crosswalk_outliers2)

crosswalk_outliers[, sbr := mean * q_nn_med]

crosswalk_outliers[, loc_yr_nid_def_mean := paste0(ihme_loc_id, " ", year_id, " ", nid, " ", std_def_short, " ", sbr)]
data[, loc_yr_nid_def_mean := paste0(ihme_loc_id, " ", year_id, " ", nid, " ", std_def, " ", sbr)]

data[loc_yr_nid_def_mean %in% crosswalk_outliers$loc_yr_nid_def_mean, outlier := 2]

out <- data[outlier %in% 1:2, c("ihme_loc_id", "year_id", "sbr", "data_type", "std_def", "outlier")]

out[outlier == 1, stage := "completeness outlier"]
out[outlier == 2, stage := "crosswalk outlier"]
out[, outlier := NULL]
out[, sbr := sbr * 1000]
out[, year := year_id]

####################
## Load Estimates ##
####################

## Estimates from GPR before scaling/aggregating
scaled_files <- as.data.table(
  list.files(path = paste0(estimate_dir, version_estimate, "/FILEPATH"))
)
colnames(scaled_files) <- "filepath"

scaled_files <- scaled_files[filepath %like% main_std_def]

scaled_files[, ihme_loc_id := gsub("gpr_", "", filepath)]
scaled_files[, ihme_loc_id := ifelse(filepath %like% "sim",
                                     gsub(paste0("_sim_", main_std_def, ".csv"), "", ihme_loc_id),
                                     gsub(paste0("_", main_std_def, ".csv"), "", ihme_loc_id))]

gpr_unscaled <- fread(
  paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirths_unscaled_", main_std_def, ".csv")
)

gpr_unscaled <- merge(
  gpr_unscaled,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

gpr_unscaled[, stage := "GPR Initial"]
gpr_unscaled[, year := year_id]

setnames(gpr_unscaled, "sbr_mean", "sbr")

gpr_unscaled <- gpr_unscaled[, c("ihme_loc_id", "year", "year_id", "stage", "sbr", "sbr_lower", "sbr_upper")]

gpr_unscaled <- gpr_unscaled[ihme_loc_id %in% scaled_files$ihme_loc_id, ]

## Final estimates
gpr <- fread(
  paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirths_estimates_database.csv")
)

gpr <- merge(
  gpr,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

gpr[, std_def := paste0(wk_cutoff, "_weeks")]
gpr[, stage := paste0("GPR Final Current (V", version_estimate, ")")]
gpr[, year := year_id]

gpr <- gpr[std_def == main_std_def & dem_measure_id == 6]

setnames(gpr, c("mean", "lower", "upper"), c("sbr", "sbr_lower", "sbr_upper"))

gpr <- gpr[!is.na(sbr)]

gpr <- gpr[, c("ihme_loc_id", "year", "year_id", "stage", "sbr", "sbr_lower", "sbr_upper")]

gpr_missing <- gpr[is.na(sbr)]
gpr_missing_locs <- unique(gpr_missing$location_id)
missing_locs <- locs[location_id %in% gpr_missing_locs, c("ihme_loc_id")]
if (nrow(missing_locs) > 0) warning("There are countries missing sbr in the final results.")

## Estimates from comparison version
gpr_prev <- fread(
  paste0(estimate_dir, test_version, "/FILEPATH/stillbirths_estimates_database.csv")
)

gpr_prev <- merge(
  gpr_prev,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

gpr_prev[, std_def := paste0(wk_cutoff, "_weeks")]
gpr_prev[, stage := paste0("GPR Final Previous (V", test_version, ")")]
gpr_prev[, year := year_id]

gpr_prev <- gpr_prev[std_def == main_std_def & dem_measure_id == 6]

setnames(gpr_prev, c("mean", "lower", "upper"), c("sbr", "sbr_lower", "sbr_upper"))

gpr_prev <- gpr_prev[!is.na(sbr)]

gpr_prev <- gpr_prev[, c("ihme_loc_id", "year", "year_id", "stage", "sbr", "sbr_lower", "sbr_upper")]

## Estimates from prev round
gpr_old_version <- mortdb::get_proc_version("stillbirth", "estimate", gbd_year = gbd_year_prev)

gpr_old <- fread(
  paste0(estimate_dir, gpr_old_version, "/FILEPATH/stillbirths_estimates_database.csv")
)

gpr_old <- merge(
  gpr_old,
  locs[, c("location_id", "ihme_loc_id")],
  by = "location_id",
  all.x = TRUE
)

gpr_old[, std_def := paste0(wk_cutoff, "_weeks")]
gpr_old[, stage := paste0("GPR Final ", gbd_year_prev)]
gpr_old[, year := year_id]

gpr_old <- gpr_old[std_def == main_std_def & dem_measure_id == 6]

setnames(gpr_old, c("mean", "lower", "upper"), c("sbr", "sbr_lower", "sbr_upper"))

gpr_old <- gpr_old[!is.na(sbr)]

gpr_old <- gpr_old[, c("ihme_loc_id", "year", "year_id", "stage", "sbr", "sbr_lower", "sbr_upper")]

###################################
## Load LSEIG Data and Estimates ##
###################################

## LSEIG data
lseig_data_orig <- fread(
  paste0(data_dir, version_data, "/FILEPATH/lseig_data_clean.csv")
)
lseig_data <- lseig_data_orig[, c("ihme_loc_id", "sbr", "year")]
lseig_data[, stage := "LSEIG data"]

## LSEIG estimates
lseig_estimates <- fread(
  paste0(data_dir, version_data, "/FILEPATH/lseig_estimates_clean.csv")
)
lseig_estimates[, stage := "LSEIG estimate"]

setnames(lseig_estimates, "year", "year_id")

## Get Unused LSEIG Data
lseig_data_clean <- copy(lseig_data_orig)

setnames(lseig_data_clean, c("year", "source_type"), c("year_id", "data_type"))

lseig_data_clean <- lseig_data_clean[, c("ihme_loc_id", "year_id", "sbr", "std_def", "data_type")]

lseig_data_clean[, sbr := round(sbr/1000, 4)]

data_for_lseig <- data[, c("ihme_loc_id", "year_id", "sbr", "std_def", "blencowe", "data_type")]
data_for_lseig[, sbr := round(sbr, 4)]

lseig_test <- merge(
  lseig_data_clean,
  data_for_lseig,
  by = c("ihme_loc_id", "year_id", "sbr", "std_def", "data_type"),
  all.x = TRUE
)

lseig_data_unused <- lseig_test[is.na(blencowe)]

lseig_data_unused$blencowe <- NULL

lseig_data_unused[, stage := "LSEIG data (for comparison)"]
lseig_data_unused[, sbr := sbr * 1000]
lseig_data_unused[, year := year_id]

##################
## Combine Data ##
##################

combined <- rbind(
  data_unadj, gpr_data, gpr, gpr_unscaled, gpr_prev, gpr_old,
  fill = TRUE
)

combined[, sbr := sbr * 1000]
combined[, sbr_lower := sbr_lower * 1000]
combined[, sbr_upper := sbr_upper * 1000]

combined <- rbind(
  combined, out, lseig_estimates, lseig_data, lseig_data_unused,
  fill = TRUE
)

combined <- combined[stage != "mse"]

combined <- combined[order(combined$ihme_loc_id, combined$stage, combined$year_id, combined$year),]

##############
## Graphing ##
##############

## Set color arguments
colors <- brewer.pal(9, "Set1")[c(2, 4, 3, 6:8)]
colors <- rev(colors)
colnames <- c(
  "stage 1 estimates", "stage 2 estimates", "GPR Initial",
  paste0("GPR Final Current (V", version_estimate, ")"),
  paste0("GPR Final Previous (V", test_version, ")"),
  paste0("GPR Final ", gbd_year_prev)
)
names(colors) <- colnames
colScale <- scale_color_manual(name = "", values = colors, drop = FALSE)

## Set fill arguments
cols <- c("#FB9A99", "#B2DF8A", "#E31A1C", "#1F78B4", "white", "white", "#999999")
colsnames <- c("gov_report", "literature", "survey", "vr", "LSEIG data",
               "LSEIG data (for comparison)", "LSEIG estimate")
names(cols) <- colsnames
colfScale_right <- scale_fill_manual(
  name = "",
  values = cols,
  drop = FALSE,
  limits = c("gov_report", "literature", "survey", "vr",
             "LSEIG data", "LSEIG data (for comparison)", "LSEIG estimate"),
  breaks = c("gov_report", "literature", "survey", "vr")
)
colfScale_left <- scale_fill_manual(
  name = "",
  values = cols,
  drop = FALSE,
  limits = c("gov_report", "literature", "survey", "vr")
)

## Set shape arguments
shs <- c(21, 21, 21, 22, 22, 21, 21, 24)
shsnames <- c("original data", "completeness-adjusted data", "crosswalk-adjusted data",
              "completeness outlier", "crosswalk outlier",
              "LSEIG data", "LSEIG data (for comparison)", "LSEIG estimate")
names(shs) <- shsnames
shapescale_right <- scale_shape_manual(
  name = "",
  values = shs,
  drop = FALSE,
  limits = shsnames,
  breaks = c("completeness outlier", "crosswalk outlier",
             "LSEIG data (for comparison)", "LSEIG estimate")
)
shapescale_left <- scale_shape_manual(
  name = "",
  values = shs,
  drop = FALSE,
  breaks = "outliered",
  limits = c("original data", "completeness-adjusted data", "crosswalk-adjusted data",
             "completeness outlier", "crosswalk outlier")
)

## Add hyper-parameters (new and old)
params <- fread(
  paste0(estimate_dir, version_estimate, "/FILEPATH/updated_hyperparameters.csv")
)
combined <- merge(
  combined,
  params,
  by = "ihme_loc_id",
  all.x = TRUE
)

params_old <- fread(
  paste0(estimate_dir, test_version, "/FILEPATH/updated_hyperparameters.csv")
)
setnames(params_old, c("lambda", "zeta", "scale"), c("lambda_old", "zeta_old", "scale_old"))
combined <- merge(
  combined,
  params_old,
  by = "ihme_loc_id",
  all.x = TRUE
)

## Add on data to combined file
temp <- data[, c("year_id", "sbr", "data_type", "std_def", "ihme_loc_id", "blencowe"), with = F]

temp[, stage := "completeness-adjusted data"]
temp[, sbr := 1000 * sbr]

combined <- merge(
  combined,
  temp,
  by = c("ihme_loc_id", "year_id", "sbr", "data_type", "stage", "std_def"),
  all.x = TRUE
)

## Fill in missing variables
combined[, stage_shape := stage]
combined[, stage_alpha := stage]

combined[stage == "LSEIG estimate", stage_shape := "LSEIG estimate"]
combined[stage == "LSEIG data (for comparison)", stage_shape := "LSEIG data (for comparison)"]
combined[stage == "LSEIG data", stage_shape := "LSEIG data"]

combined[stage == "LSEIG estimate", data_type := "LSEIG estimate"]
combined[stage == "LSEIG data (for comparison)", data_type := "LSEIG data (for comparison)"]
combined[stage == "LSEIG data", data_type := "LSEIG data"]

## Drop data before year start
combined <- combined[year_id >= year_start]

## Set up ddata data.table for graphing
ddata <- combined[
  stage == "original data" |
  stage == "completeness-adjusted data" |
  stage == "crosswalk-adjusted data" |
  stage == "completeness outlier" |
  stage == "crosswalk outlier" |
  stage == paste0("GPR Final Current (V", version_estimate, ")") |
  stage == paste0("GPR Final Previous (V", test_version, ")") |
  stage == "LSEIG data" |
  stage == "LSEIG data (for comparison)" |
  stage == "LSEIG estimate",
]

ddata[, data_type := factor(data_type, levels = colsnames, labels = names(cols))]

## Set up combined data.table for graphing
combined <- combined[stage != "original data" & stage != "completeness-adjusted data" &
                      stage != "crosswalk-adjusted data" & stage != "LSEIG data",]

combined[, stage := factor(stage, levels = colnames, labels = names(colors))]

combined <- merge(
  combined,
  locs[, c("ihme_loc_id", "region_name")],
  by = c("ihme_loc_id"),
  all.x = TRUE
)

combined[is.na(region_name), region_name := "Aggregates"]

## Add on nmr to calculate log ratio
nmr <- pred[, c("ihme_loc_id", "year_id", "q_nn_med")]

combined <- merge(
  combined,
  nmr,
  by = c("ihme_loc_id", "year_id"),
  all.x = TRUE
)

combined[, ratio := (sbr/1000) / q_nn_med]
combined[, log_ratio := log(ratio)]

## Add on location variables
combined <- combined[!is.na(ihme_loc_id)]

locsdf <- get_locations(gbd_year = gbd_year, level = "all")
locsdf[is.na(region_name), region_name := "Aggregates"]

locsdf <- merge(
  locsdf,
  locs_metadata[, c("location_id", "sort_order")],
  by = "location_id",
  all.x = TRUE
)

combined <- merge(
  combined,
  locsdf[, c("ihme_loc_id", "level")],
  by = "ihme_loc_id",
  all.x = TRUE
)

## Read in source list

source_list_filepath <- paste0("FILEPATH/source_list.csv")
source_list <- fread(source_list_filepath)

setnames(source_list, "source_name", "source_type")

source_list <- source_list[year_id >= 1980]

years <- year_start:year_end

source_list <- source_list[
  (source_type %like% "Statistical Report" | source_type %like% "VR") &
  !(nid %in% c(33839, 256736)) &
  !(title %like% "CANSIM") & !(title %like% "CARICOM"),
  title2 := gsub(paste(years, collapse = "|"), "", title)
]

source_list[title2 %like% "Morocco Health in Figures", title2 := "Morocco Health in Figures"]
source_list[title2 %like% "United Nations Demographic Yearbook", title2 := "United Nations Demographic Yearbook"]

substrRight <- function(x, n) {
  substr(x, nchar(x) - n + 1, nchar(x))
}

source_list[substrRight(title2, 2) == " -", title2 := substr(title2, 1, nchar(title2) - 2)]

source_list[, title := ifelse(!is.na(title2), title2, title)]

loc_yr_list <- as.data.table(
  source_list %>% group_by(ihme_loc_id, title, std_def, source_type, outlier) %>%
  summarize(years = paste(sort(unique(year_id)), collapse = ", "))
)

conseq <- function(s) {
  s <- as.numeric(unlist(strsplit(s, ",")))
  dif <- s[seq(length(s))][-1] - s[seq(length(s) - 1)]
  new <- !c(0, dif == 1)
  cs <- cumsum(new)
  res <- vector(mode = "list", max(cs))
  for (i in seq(res)) {
    s.i <- s[which(cs == i)]
    if (length(s.i) > 2) {
      res[[i]] <- paste(min(s.i), max(s.i), sep = "-")
    } else {
      res[[i]] <- as.character(s.i)
    }
  }
  paste(unlist(res), collapse = ", ")
}

loc_yr_list[, years := conseq(years), by = c("ihme_loc_id", "title", "std_def", "source_type", "outlier")]

source_list <- merge(
  source_list,
  loc_yr_list,
  by = c("ihme_loc_id", "title", "std_def", "source_type", "outlier"),
  all.x = TRUE
)

std_def_list <- as.data.table(
  source_list %>% group_by(ihme_loc_id, title, years, source_type, outlier) %>%
  summarize(std_defs = paste(sort(unique(std_def)), collapse = ", "))
)

source_list <- merge(
  source_list,
  std_def_list,
  by = c("ihme_loc_id", "title", "years", "source_type", "outlier"),
  all.x = TRUE
)

source_list <- source_list[, c("ihme_loc_id", "std_defs", "years", "outlier", "source_type", "title")]

source_list <- unique(source_list)

source_count_by_loc <- as.data.table(table(source_list$ihme_loc_id))
colnames(source_count_by_loc) <- c("ihme_loc_id", "count")

if (nrow(source_count_by_loc[count > 27]) > 0) warning ("Some locations have more sources than what can be shown on vetting graph")

## Reassign sort order

locsdf[ihme_loc_id == "CHN_44533", sort_order := 736.1]

locsdf[ihme_loc_id == "KEN_44793", sort_order := 946.1]
locsdf[ihme_loc_id == "KEN_44794", sort_order := 946.2]
locsdf[ihme_loc_id == "KEN_44795", sort_order := 946.3]
locsdf[ihme_loc_id == "KEN_44796", sort_order := 946.4]
locsdf[ihme_loc_id == "KEN_44797", sort_order := 946.5]
locsdf[ihme_loc_id == "KEN_44798", sort_order := 946.6]
locsdf[ihme_loc_id == "KEN_44799", sort_order := 946.7]
locsdf[ihme_loc_id == "KEN_44800", sort_order := 946.8]

## Subset combined to national locations only

if (graph_nat_est_only) {

  subnats <- locsdf[level > 3]
  locsdf <- locsdf[(level <= 3 | ihme_loc_id == "CHN_44533") & ihme_loc_id != "CHN"]

}

if (graph_ind_only) {

  locsdf <- locsdf[ihme_loc_id %like% "IND" | ihme_loc_id %in% c("R4", "S5", "G")]

}

## Set filepaths for saving graphs
if (length(unique(combined$ihme_loc_id)) > 1 & !graph_nat_est_only & !graph_ind_only) {

  pdf(paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirth_model_results_log_ratios_",
             main_std_def, ".pdf"), width = 42, height = 12)

} else if (length(unique(combined$ihme_loc_id)) > 1 & graph_nat_est_only) {

  pdf(paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirth_model_results_log_ratios_",
             main_std_def, "_nat_only.pdf"), width = 42, height = 12)

} else if (length(unique(combined$ihme_loc_id)) > 1 & graph_ind_only) {

  pdf(paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirth_model_results_log_ratios_",
             main_std_def, "_ind_only.pdf"), width = 42, height = 12)

} else {

  pdf(paste0(estimate_dir, version_estimate, "/FILEPATH/stillbirth_model_results_log_ratios_",
             main_std_def, "_", test_loc, ".pdf"), width = 42, height = 12)

}

## Graph panel graphs

for (loc in locsdf$ihme_loc_id[order(locsdf$sort_order)]) {

  loc_name <- locs[ihme_loc_id == loc]$location_name

  ## Plot SBR graphs
  tmp <- combined[ihme_loc_id == loc & !is.na(ihme_loc_id),]
  tmp2 <- ddata[ihme_loc_id == loc & !is.na(ihme_loc_id),]

  p1 = ggplot() +
    geom_ribbon(data = tmp[ihme_loc_id == loc & stage == paste0("GPR Final Current (V", version_estimate, ")"),],
                aes(x = year_id, ymax = sbr_upper, ymin = sbr_lower), fill = "green", alpha = .1) +
    geom_ribbon(data = tmp[ihme_loc_id == loc & stage == paste("GPR Final ", gbd_year_prev),],
                aes(x = year_id, ymax = sbr_upper, ymin = sbr_lower), fill = "blue", alpha = .1) +
    geom_line(data = tmp[stage == "stage 1 estimates" | stage == "stage 2 estimates" |
                           stage == paste0("GPR Final Current (V", version_estimate, ")") |
                           stage == paste0("GPR Final Previous (V", test_version, ")") |
                           stage == "GPR Initial" | stage == paste0("GPR Final ", gbd_year_prev),],
              aes(x = year_id, y = sbr, colour = stage), size = 1.25, show.legend = TRUE) +
    geom_point(data = tmp2[stage == "LSEIG data (for comparison)"],
               aes(x = year, y = sbr, shape = stage_shape), size = 2.5, stroke = 2) +
    geom_point(data = tmp2[stage == "original data" | stage == "crosswalk-adjusted data" |
                             (stage == "completeness-adjusted data" & (blencowe == 0 | is.na(blencowe))),],
               aes(x = year, y = sbr, fill = data_type, alpha = factor(stage_alpha),
                   shape = stage_shape), size = 2.5) +
    geom_point(data = tmp2[stage == "completeness-adjusted data" & blencowe == 1,],
               aes(x = year, y = sbr, fill = data_type, alpha = factor(stage_alpha),
                   shape = stage_shape), size = 2.5, show.legend = F) +
    geom_point(data = tmp2[stage == "completeness-adjusted data" & blencowe == 1,],
               aes(x = year, y = sbr, shape = stage_shape), size = 2.5, stroke = 2) +
    geom_point(data = tmp2[stage == "completeness outlier" | stage == "crosswalk outlier"],
               aes(x = year, y = sbr, fill = data_type, alpha = factor(stage_alpha),
                   shape = stage_shape), size = 2.6, show.legend = F) +
    geom_point(data = tmp[data_type == "LSEIG estimate",],
               aes(x = year_id, y = sbr, shape = stage_shape), size = 2.5, stroke = 2) +
    scale_alpha_manual(
      values = c(0.2, 0.55, 0.55, 1, 1),
      name = "",
      limits = c("original data", "completeness-adjusted data", "completeness outlier",
                 "crosswalk-adjusted data", "crosswalk outlier"),
      drop = FALSE
    ) +
    colScale +
    colfScale_right +
    shapescale_right +
    labs(title = paste(loc_name, "  ", loc, "  ", locs$region_name[locs$ihme_loc_id == loc], "  Stillbirth Rate (SBR) \n",
                       "lambda =", unique(tmp$lambda), "; zeta =", unique(tmp$zeta),
                       "; scale =", unique(tmp$scale), "; amp =", mse,
                       "; lambda_prev =", unique(tmp$lambda_old), "; zeta_prev =",
                       unique(tmp$zeta_old), "; scale_prev =", unique(tmp$scale_old))) +
    xlab("Year") +
    ylab("SBR") +
    labs(colour = " ", fill = " ", alpha = " ", shape = " ") +
    guides(fill = guide_legend(override.aes = list(shape = 21))) +
    theme_bw()

  ## Plot ratio graphs (SBR/NMR)

  ddata2 <- merge(
    ddata,
    nmr,
    by = c("ihme_loc_id", "year_id"),
    all.x = TRUE
  )

  ddata2[stage == "original data" | stage == "completeness-adjusted data"| stage == "crosswalk-adjusted data", ratio := (sbr/1000) / q_nn_med]
  ddata2[stage == "original data" | stage == "completeness-adjusted data"| stage == "crosswalk-adjusted data", log_ratio := log(ratio)]

  tmp <- combined[combined$ihme_loc_id == loc & !is.na(combined$ihme_loc_id) & !(stage %like% "LSEIG" | data_type %like% "LSEIG"),]
  tmp2 <- ddata2[ihme_loc_id == loc & !is.na(ihme_loc_id) & !(stage %like% "LSEIG" | data_type %like% "LSEIG"),]
  tmp2[stage %like% "outlier", ratio := (sbr/1000) / q_nn_med]
  tmp2[stage %like% "outlier", log_ratio := log(ratio)]

  p2 = ggplot(data = tmp) +
    geom_line(data = tmp[stage == "stage 1 estimates" | stage == "stage 2 estimates" |
                           stage == paste0("GPR Final Current (V", version_estimate, ")") |
                           stage == "GPR Initial" | stage == paste0("GPR Final ", gbd_year_prev),],
              aes(x = year_id, y = ratio, colour = stage), size = 1.5, show.legend = TRUE) +
    geom_point(data = tmp2[stage == "crosswalk-adjusted data" | stage == "completeness-adjusted data" | stage == "original data",],
               aes(x = year, y = ratio, alpha = factor(stage_alpha), fill = data_type, shape = stage_shape), size = 2.5) +
    geom_point(data = tmp2[stage == "completeness outlier" | stage == "crosswalk outlier",],
               aes(x = year, y = ratio, fill = data_type, shape = stage_shape, alpha = factor(stage_alpha)), size = 2.6, show.legend = FALSE) +
    geom_label_repel(data = tmp2[stage == "completeness-adjusted data" & std_def != main_std_def,],
                     aes(x = year_id, y = ratio, label = std_def), size = 1.8, label.padding = 0.15) +
    scale_alpha_manual(
      values = c(0.2, 0.55, 0.55, 1, 1),
      name = "",
      limits = c("original data", "completeness-adjusted data", "completeness outlier",
                 "crosswalk-adjusted data", "crosswalk outlier"),
      drop = FALSE
    ) +
    colScale +
    colfScale_left +
    shapescale_left +
    labs(title = paste(loc_name, "  ", loc, "  ", locs$region_name[locs$ihme_loc_id == loc],
                       "  Stillbirth Rate / Neonatal Mortality Rate (SBR/NMR) \n",
                       "lambda =", unique(tmp$lambda), "; zeta =", unique(tmp$zeta),
                       "; scale =", unique(tmp$scale), "; amp =", mse,
                       "; lambda_prev =", unique(tmp$lambda_old), "; zeta_prev =",
                       unique(tmp$zeta_old), "; scale_prev =", unique(tmp$scale_old))) +
    xlab("Year") +
    ylab("SBR/NMR") +
    labs(colour = " ", fill = " ", alpha = " ", shape = " ") +
    guides(fill = guide_legend(override.aes = list(shape = 21))) +
    theme_bw()

  ## Add source list
  source_list_loc <- source_list[ihme_loc_id == loc, !c("ihme_loc_id")]
  source_list_loc <- source_list_loc[
    order(source_list_loc$outlier, source_list_loc$std_def, source_list_loc$years),
  ]

  std_defs <- source_list_loc$std_defs
  std_defs_wrapped <- strwrap(std_defs, width = 30, simplify = FALSE)
  std_defs_new <- sapply(std_defs_wrapped, paste0, collapse = "\n")

  source_list_loc[, std_defs := std_defs_new]

  years <- source_list_loc$years
  years_wrapped <- strwrap(years, width = 30, simplify = FALSE)
  years_new <- sapply(years_wrapped, paste0, collapse = "\n")

  source_list_loc[, years := years_new]

  titles <- source_list_loc$title
  title_wrapped <- strwrap(titles, width = 50, simplify = FALSE)
  title_new <- sapply(title_wrapped, paste0, collapse = "\n")

  source_list_loc[, title := title_new]

  source_list_loc <- as.matrix(source_list_loc[, c("std_defs", "years", "outlier", "source_type", "title")])

  blank_table <- data.frame(
    std_defs = " ",
    years = " ",
    outlier = " ",
    source_type = " ",
    title = " "
  )

  blank_table <- as.matrix(blank_table)

  theme <- gridExtra::ttheme_default()

  if (nrow(source_list_loc) > 0) {

    p3 <- gridExtra::tableGrob(source_list_loc, theme = theme)

  } else {

    p3 <- gridExtra::tableGrob(blank_table, theme = theme)

  }

  p3$widths <- unit(c(0.2, 0.2, 0.075, 0.1, 0.3), "npc")

  ## Combine
  gridExtra::grid.arrange(p2, p1, p3, ncol = 3)

  ## Delete source list
  source_list_loc <- NULL

}

dev.off()
