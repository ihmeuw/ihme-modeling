################################################################################
# Description: Correct for underenumeration/overenumeration.
# - fit model of pes underenumeration on sdi using only all-age pes data
# - create 1000 draws of predicted pes underenumeration for each possible sdi value
# - use DISMOD PES model global age pattern to calculate value for each possible census age grouping
# - shift pes age pattern up/down to match the predicted net pes underenumeration
# - data_stage: "processed, pes corrected"
################################################################################

library(data.table)
library(readr)
library(MASS)
library(ggplot2)
library(mortdb, lib.loc = "FILEPATH/r-pkg")

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", "/population/data_processing/census_processing/")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_processing_vid", type = "character",
                    help = "The version number for this run of population data processing, used to read in settings file")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_processing_vid <- "99999"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_processing_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "helper_functions.R"))

location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))
# determine which locations are national locations that will be included in the pes regression
CHN_GBR_locids <- location_hierarchy[ihme_loc_id %in% c("CHN", "GBR"), location_id]
location_hierarchy[, national_location := as.numeric((level == 3 & !location_id %in% CHN_GBR_locids) | parent_id %in% CHN_GBR_locids)]

age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
census_id_vars <- c(census_id_vars, "split", "aggregate", "smoother")

census_specific_settings <- fread(paste0(output_dir, "/inputs/census_specific_settings.csv"))

# create mapping to national parent ihme_loc_id
national_mapping <- fread(paste0(output_dir, "/inputs/national_mapping.csv"))
subnational_mappings <- fread(paste0(output_dir, "/inputs/subnational_mappings.csv"))
# combine all mappings together
full_location_mapping <- rbind(location_hierarchy[level >= 3, list(location_id, ihme_loc_id)],
                               subnational_mappings[, list(location_id = parent_location_id, ihme_loc_id = parent_id)],
                               subnational_mappings[, list(location_id = child_location_id, ihme_loc_id = child_id)],
                               national_mapping[, list(location_id = parent_location_id, ihme_loc_id = parent_id)],
                               national_mapping[, list(location_id = child_location_id, ihme_loc_id = child_id)],
                               use.names = T)
full_location_mapping <- unique(full_location_mapping)
full_location_mapping[, national_parent_ihme_loc_id := substr(ihme_loc_id, 1, 3)]
full_location_mapping[national_parent_ihme_loc_id == "CHN" & !ihme_loc_id %in% c("CHN", "CHN_354", "CHN_361"), national_parent_ihme_loc_id := "CHN_44533"]
full_location_mapping[national_parent_ihme_loc_id == "GBR" & !ihme_loc_id %in% c("GBR", "GBR_433", "GBR_434", "GBR_4636"), national_parent_ihme_loc_id := "GBR_4749"]
full_location_mapping[ihme_loc_id == "USSR", national_parent_ihme_loc_id := "USSR"]
full_location_mapping <- merge(full_location_mapping,
                               unique(full_location_mapping[, list(national_parent_location_id = location_id,
                                                                   national_parent_ihme_loc_id = ihme_loc_id)]),
                               by = "national_parent_ihme_loc_id", all.x = T)


# Fit pes model -----------------------------------------------------------

# read in pes data
pes_data <- fread(paste0(output_dir, "/inputs/pes_data.csv"))
pes_data <- pes_data[age_group_id == 22 & sex_id == 3]
pes_data <- pes_data[, list(location_id, year_id, sex_id, underenum_pct = mean)]

# make adjustment to make data identical to GBD2017, data at time was assigned to GBD location rather than national historical location
historical_loc_pes_data <- pes_data[!location_id %in% location_hierarchy$location_id]
pes_data <- pes_data[location_id %in% location_hierarchy$location_id]
setnames(historical_loc_pes_data, "location_id", "child_location_id")

historical_loc_pes_data <- merge(historical_loc_pes_data, national_mapping[, list(location_id = parent_location_id, child_location_id)], all.x = T, by = "child_location_id")
historical_loc_pes_data[child_location_id == 427, location_id := location_hierarchy[ihme_loc_id == "YEM", location_id]]
historical_loc_pes_data[, child_location_id := NULL]
pes_data <- rbind(pes_data, historical_loc_pes_data, use.names = T)

# read in sdi estimates
sdi <- fread(paste0(output_dir, "/inputs/sdi.csv"))
sdi <- sdi[, list(location_id, year_id, sdi = mean_value)]
pes_data <- merge(pes_data, sdi, by = c("location_id", "year_id"), all.x = T)

# merge on national location
pes_data <- merge(pes_data, location_hierarchy[, list(location_id, national_location)], by = "location_id", all.x = T)

# fit model
pes_correction_model <- lm(underenum_pct ~ sdi, data = pes_data[national_location == 1])

# simulate 1000 draws of intercept and sdi coefficient
set.seed(49023)
simbetas <- MASS::mvrnorm(mu = coef(pes_correction_model), Sigma = vcov(pes_correction_model), n = 1000)
simbetas <- data.table(simbetas)
setnames(simbetas, c(1,2), c("int", "sdi_coef"))
simbetas[, sim := seq(.N)]

# use coefficient draws to calculate 1000 draws of the predicted net_pes_correction for each possible sdi value
possible_sdi_values <- seq(0, 1, 0.0001)
correction_draws <- simbetas[, list(sdi = possible_sdi_values,
                                    pred_net_pes_correction = int + sdi_coef * possible_sdi_values),
                             by = c("sim")]
corrections <- correction_draws[, list(pred_net_pes_correction = mean(pred_net_pes_correction),
                                       pred_net_pes_correction_lb = quantile(pred_net_pes_correction, 0.025),
                                       pred_net_pes_correction_ub = quantile(pred_net_pes_correction, 0.975),
                                       pred_net_pes_correction_var = var(pred_net_pes_correction)),
                                by = c("sdi")]

# plot pes regression
plotdatadf <- melt(pes_data[national_location == 1, .(location_id, year_id, sdi, underenum_pct)], measure = patterns("pct"), variable.name = "space", value.name = "pes_correction")
plotdatadf <- merge(plotdatadf, location_hierarchy[, list(location_id, ihme_loc_id)], by = "location_id", all.x = T)
plotdatadf[, label := paste0(ihme_loc_id, " ", year_id)]
pdf(paste0(output_dir, "/diagnostics/net_pes_regression.pdf"), width = 15, height = 10)
plot <- ggplot(data = plotdatadf) + geom_line(data = corrections, aes(x = sdi, y = pred_net_pes_correction), color = "red") +
  geom_ribbon(data = corrections, aes(x = sdi, ymin = pred_net_pes_correction_lb, ymax = pred_net_pes_correction_ub), fill = "red", alpha = .3) +
  geom_point(aes(x = sdi, y = pes_correction), shape = 19, size = 2, alpha = .7) +
  ggrepel::geom_text_repel(aes(x = sdi, y = pes_correction, label = label), size = 2.5) +
  theme_bw()
print(plot)
dev.off()


# Calculate PES age pattern -----------------------------------------------

dismod_fit <- fread(paste0(output_dir, "/inputs/pes_dismod_age_pattern.csv"))

# interpolate to get single year age group values
dismod_fit <- merge(dismod_fit, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
dismod_fit <- dismod_fit[, list(sex_id, age_group_years_start = as.integer(age_group_years_start), underenum_pct = mean)]
dismod_fit <- dismod_fit[, list(underenum_pct = mean(underenum_pct)), by = c("sex_id", "age_group_years_start")] # average neonatal age groups
dismod_fit <- dismod_fit[, list(age = 0:terminal_age,
                                underenum_pct = approx(x = age_group_years_start, y = underenum_pct,
                                                       method = "linear", xout = 0:terminal_age)$y),
                         by = "sex_id"]
dismod_fit[, underenum_pct := (1 - underenum_pct)]
dismod_fit[, underenum_pct := underenum_pct + 1]

# plot age pattern
pdf(paste0(output_dir, "/diagnostics/pes_age_patterns.pdf"), width = 15, height = 10)
plot <- ggplot(dismod_fit, aes(x = age, y = underenum_pct, colour = factor(sex_id, labels = c("Male", "Female")))) +
  geom_hline(yintercept = 0) +
  geom_line() + theme_bw() +
  labs(title = "Underenumeration global age pattern averaged over 1990-2017", colour = "Sex")
print(plot)
dev.off()

# determine all possible age group combinations
census_data <- fread(paste0(output_dir, "/outputs/07_make_national_location_adjustments.csv"))
census_data <- merge(census_data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
census_age_groups <- unique(census_data[, list(age_group_years_start, age_group_years_end)])
census_age_groups <- census_age_groups[, list(age = age_group_years_start:(age_group_years_end - 1)), by = c("age_group_years_start", "age_group_years_end")]
census_age_groups <- census_age_groups[age <= terminal_age]

# calculate mean underenumeration value for each possible age group
dismod_fit <- merge(census_age_groups, dismod_fit, by = c("age"), all.x = T, allow.cartesian = T)
dismod_fit <- dismod_fit[, list(underenum_pct = mean(underenum_pct)), by = c("sex_id", "age_group_years_start", "age_group_years_end")]


# Apply pes correction ----------------------------------------------------

# merge on predicted net pes correction for all locations based on sdi value
census_data <- merge(census_data, sdi, by = c("location_id", "year_id"), all.x = T)
census_data[, sdi := plyr::round_any(sdi, 0.0001)]
census_data <- merge(census_data, corrections[, list(sdi, pred_net_pes_correction)], by = c("sdi"), all.x = T)
census_data[is.na(pred_net_pes_correction), pred_net_pes_correction := 0] # These locations don't have an sdi value
census_data[, sdi := NULL]

# find actual raw pes corrections in locations we trust
use_raw_net_pes_adjustment_locs <- lapply(use_raw_net_pes_adjustment_locs, function(loc) {
  locs <- location_hierarchy[grepl(loc, ihme_loc_id), ihme_loc_id]
})
use_raw_net_pes_adjustment_locs <- unlist(use_raw_net_pes_adjustment_locs)
use_raw_net_pes_adjustment_locs <- get_location_id(use_raw_net_pes_adjustment_locs, location_hierarchy)
no_adjust <- pes_data[location_id %in% use_raw_net_pes_adjustment_locs, list(location_id, year_id, raw_net_pes_correction = underenum_pct)]
no_adjust <- merge(no_adjust, full_location_mapping[, list(location_id, national_parent_location_id)], by = "location_id")

# merge these raw pes corrections on
census_data <- merge(census_data, full_location_mapping[, list(location_id, national_parent_location_id)], by = "location_id")
census_data <- merge(census_data, no_adjust[, list(location_id, year_id, raw_net_pes_correction)], by = c("location_id", "year_id"), all.x = T)
census_data <- merge(census_data, no_adjust[national_parent_location_id == location_id, list(national_parent_location_id, year_id, national_raw_net_pes_correction = raw_net_pes_correction)],
                     by = c("national_parent_location_id", "year_id"), all.x = T)
census_data[!is.na(raw_net_pes_correction), pred_net_pes_correction := raw_net_pes_correction] # if pes correction exists for specific location, use that
census_data[is.na(raw_net_pes_correction) & !is.na(national_raw_net_pes_correction), pred_net_pes_correction := national_raw_net_pes_correction] # otherwise use if the national adjustment exists
census_data[, c("raw_net_pes_correction", "national_raw_net_pes_correction") := NULL]

# merge on pes age pattern and shift the age pattern to match the predicted net pes correction
census_data[, pred_net_pes_correction := 1 + (pred_net_pes_correction / 100)]
census_data <- merge(census_data, dismod_fit, by = c("sex_id", "age_group_years_start", "age_group_years_end"), all = T)
census_data[, adjusted_mean := mean * underenum_pct]
census_data[, adjusted_total_mean := sum(adjusted_mean), by = census_id_vars]
census_data[, total_mean := sum(mean), by = census_id_vars]
census_data[, dismod_net_pes_correction := adjusted_total_mean / total_mean]
census_data[, constant := pred_net_pes_correction - dismod_net_pes_correction]
census_data[, adjusted_mean := mean * (underenum_pct + constant)]
census_data[, adjusted_total_mean := sum(adjusted_mean), by = census_id_vars]
census_data[, dismod_net_pes_correction := adjusted_total_mean / total_mean]

# don't use the pes corrected values in specified locations
census_data <- merge(census_data, census_specific_settings[!is.na(pes_adjust), list(location_id, year_id, pes_adjust)], by = c("location_id", "year_id"), all.x = T)

# don't adjust data in locations with population registries
registry_data_locations <- sort(location_hierarchy[level == 3 & location_id %in% census_data[record_type == "population registry" & outlier_type == "not outliered", unique(location_id)], location_id])
census_data[national_parent_location_id %in% registry_data_locations & is.na(pes_adjust), pes_adjust := FALSE]

# except these locations we do want to
adjust_non_registry_data_locations <- location_hierarchy[ihme_loc_id %in% adjust_non_registry_data_locations, location_id]
census_data[record_type != "population registry" & national_parent_location_id %in% adjust_non_registry_data_locations, pes_adjust := TRUE]

# actually turn off the pes adjustment
census_data[is.na(pes_adjust), pes_adjust := TRUE]
census_data[!(pes_adjust), adjusted_mean := mean]

census_data[, c("pes_adjust", "pred_net_pes_correction", "underenum_pct", "adjusted_total_mean", "total_mean", "dismod_net_pes_correction", "constant") := NULL]
census_data[, c("national_parent_location_id", "age_group_years_start", "age_group_years_end", "mean") := NULL]
setnames(census_data, "adjusted_mean", "mean")

census_data[, data_stage := "pes corrected"]
setcolorder(census_data, c(census_id_vars, "sex_id", "age_group_id", "mean"))
setkeyv(census_data, c(census_id_vars, "sex_id", "age_group_id"))
readr::write_csv(census_data, path = paste0(output_dir, "/outputs/08_apply_pes_correction.csv"))
readr::write_csv(corrections, path = paste0(output_dir, "/outputs/08_pes_correction_variance.csv"))
