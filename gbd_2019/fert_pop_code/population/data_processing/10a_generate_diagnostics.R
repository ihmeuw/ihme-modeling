################################################################################
# Description: Generate diagnostics for the data processing outputs.
# - compile together data processing stages, comparators and gbd estimates
# - plot total population diagnostics
# - plot baseline population diagnostics
# - plot age pattern and age heaping diagnostics
################################################################################

library(data.table)
library(ggplot2)
library(RColorBrewer)
library(parallel)

rm(list = ls())
SGE_TASK_ID <- Sys.getenv("SGE_TASK_ID")
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", USER, "/population/data_processing/census_processing/")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_processing_vid", type = "character",
                    help = "The version number for this run of population data processing, used to read in settings file")
parser$add_argument('--task_map_dir', type="character",
                    help="The filepath to the task map file that specifies other arguments to run for")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_processing_vid <- "99999"
  args$task_map_dir <- "FILEPATH"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# use task id to get arguments from task map
task_map <- fread(task_map_dir)
loc_id <- task_map[task_id == SGE_TASK_ID, location_id]

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_processing_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "helper_functions.R"))
source(paste0(code_dir, "plotting_functions.R"))

location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))[location_id == loc_id]
ihme_loc <- location_hierarchy[location_id == loc_id, ihme_loc_id]
loc_name <- location_hierarchy[location_id == loc_id, location_name]
loc_name <- paste0(loc_name, " (", ihme_loc, ")")

age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
location_specific_settings <- fread(paste0(output_dir, "/inputs/location_specific_settings.csv"))
census_specific_settings <- fread(paste0(output_dir, "/inputs/census_specific_settings.csv"))


# Load processed census data ----------------------------------------------

files <- c("01_raw.csv",
           "02_distribute_unknown_sex_age.csv",
           "03_split_historical_subnationals.csv",
           "04_aggregate_subnationals.csv",
           "05_correct_age_heaping.csv",
           "06_make_manual_age_group_changes.csv",
           "07_make_national_location_adjustments.csv",
           "08_apply_pes_correction.csv",
           "09_generate_baseline.csv")

census_data <- lapply(files, function(f) {
  print(f)
  data <- fread(paste0(output_dir, "/outputs/", f))
  return(data)
})
census_data <- rbindlist(census_data, use.names = T, fill = T)
census_data <- census_data[location_id == loc_id]

# drop all the actual raw data and mark the distributed unknown as raw
plot_processing_steps <- c("aggregated subnationals",
                           "national location adjustments",
                           "baseline included")
census_data <- census_data[data_stage %in% plot_processing_steps]
census_data <- census_data[data_stage == "aggregated subnationals", data_stage := "raw"]
census_data <- census_data[data_stage == "national location adjustments", data_stage := "unheaped"]
census_data <- census_data[data_stage == "baseline included", data_stage := "pes_corrected"]

census_data[source_name == "Mitchell 2010", source_group := "Mitchell"]
census_data[grepl("IPUMS", source_name), source_group := "IPUMS"]
census_data[grepl("DYB", source_name), source_group := "DYB"]
census_data[source_name == "backwards ccmpp", source_group := "Backwards ccmpp baseline"]
census_data[source_name == "split_location", source_group := "Adjusted split location"]
census_data[is.na(source_group), source_group := "Custom"]

census_data[, outlier_type := paste0("Data: ", outlier_type)]

census_data <- census_data[, list(location_id, year_id, sex_id, age_group_id, source_name, smoother,
                                  source_group, outlier_type, data_stage, aggregate, mean)]


# Load age sex ratios -----------------------------------------------------

# ratios
age_sex_ratios_reported <- fread(paste0(output_dir, "/diagnostics/age_sex_ratios_reported.csv"))
age_sex_ratios_reported <- age_sex_ratios_reported[location_id == loc_id & outlier_type == "not outliered"]
age_sex_ratios_reported[, type := "reported"]

age_sex_ratios_smoothed <- fread(paste0(output_dir, "/diagnostics/age_sex_ratios_smoothed.csv"))
age_sex_ratios_smoothed <- age_sex_ratios_smoothed[location_id == loc_id & outlier_type == "not outliered"]
age_sex_ratios_smoothed[, type := "smoothed"]

age_sex_ratios <- rbind(age_sex_ratios_reported, age_sex_ratios_smoothed, use.names = T)

# scores
age_sex_scores_reported <- fread(paste0(output_dir, "/diagnostics/age_sex_index_reported.csv"))
age_sex_scores_reported <- age_sex_scores_reported[location_id == loc_id & outlier_type == "not outliered"]
age_sex_scores_reported[, type := "reported"]

age_sex_scores_smoothed <- fread(paste0(output_dir, "/diagnostics/age_sex_index_smoothed.csv"))
age_sex_scores_smoothed <- age_sex_scores_smoothed[location_id == loc_id & outlier_type == "not outliered"]
age_sex_scores_smoothed[, type := "smoothed"]

age_sex_scores <- rbind(age_sex_scores_reported, age_sex_scores_smoothed, use.names = T)


# Load the baseline pops --------------------------------------------------

baseline_census_data <- fread(paste0(output_dir, "/diagnostics/backwards_ccmpp_results.csv"))
baseline_census_data <- baseline_census_data[location_id == loc_id]

# sum across sexes
baseline_census_data <- baseline_census_data[, list(outlier_type = "backwards ccmpp", mean = sum(mean)),
                                             by = c("location_id", "year_id", "age_group_id", "missing_triangle")]


# Load the comparison pops ------------------------------------------------

comparator <- fread(paste0(output_dir, "/inputs/comparators.csv"))
comparator <- comparator[location_id == loc_id, list(location_id, year_id, sex_id, age_group_id, source_name = detailed_source, mean)]

comparator[, outlier_type := source_name]
comparator[, outlier_type := gsub("WPP_", "WPP", outlier_type)]
comparator[source_name != "US Census Bureau" & !grepl("WPP", source_name), outlier_type := "National Comparator"]
comparator[, source_group := "Estimate"]
comparator[, data_stage := "estimate"]
comparator[, aggregate := F]

comparator <- comparator[, list(location_id, year_id, sex_id, age_group_id, source_name,
                                source_group, outlier_type, data_stage, aggregate, mean)]


# Prep the GBD2017 pops ---------------------------------------------------

gbd_pop_current <- fread(paste0(output_dir, "/inputs/gbd_pop_current_round_best.csv"))
gbd_pop_current <- gbd_pop_current[location_id == loc_id]
gbd_pop_current[, c("run_id") := NULL]
gbd_pop_current[, outlier_type := paste0("GBD", gbd_year, "_v", pop_current_round_run_id)]
gbd_pop_current[, source_name := outlier_type]
gbd_pop_current[, source_group := "Estimate"]
gbd_pop_current[, data_stage := "estimate"]
gbd_pop_current[, aggregate := F]

gbd_pop_previous <- fread(paste0(output_dir, "/inputs/gbd_pop_previous_round_best.csv"))
gbd_pop_previous <- gbd_pop_previous[location_id == loc_id]
gbd_pop_previous[, c("run_id") := NULL]
gbd_pop_previous[, outlier_type := paste0("GBD", gbd_year_previous, "_v", pop_previous_round_run_id)]
gbd_pop_previous[, source_name := outlier_type]
gbd_pop_previous[, source_group := "Estimate"]
gbd_pop_previous[, data_stage := "estimate"]
gbd_pop_previous[, aggregate := F]

gbd_pop <- rbind(gbd_pop_current, gbd_pop_previous, use.names = T)
gbd_pop <- gbd_pop[, list(location_id, year_id, sex_id, age_group_id, source_name,
                          source_group, outlier_type, data_stage, aggregate, mean)]


# Combine data ------------------------------------------------------------

data <- rbind(census_data, comparator, gbd_pop, use.names = T, fill = T)
data[, smoother := NULL]

## factors needed for plotting
# data outlier_type which is used for coloring (data and estimates)
wpp_sources <- sort(grep("WPP", unique(data$outlier_type), value = T), decreasing = T)[1:2]
data[, outlier_type := factor(outlier_type, levels = c(wpp_sources, "US Census Bureau",
                                                       "National Comparator",
                                                       paste0("GBD", gbd_year, "_v", pop_current_round_run_id),
                                                       paste0("GBD", gbd_year_previous, "_v", pop_previous_round_run_id),
                                                       "Data: duplicated", "Data: check extraction",
                                                       "Data: not census data", "Data: excluded",
                                                       "Data: not outliered", "aggregate"))]

# data sources which is used for the shapes
data[, source_group := factor(source_group, levels = c("Estimate", "Mitchell", "DYB", "IPUMS",
                                                       "Backwards ccmpp baseline", "Custom",
                                                       "Adjusted split location"))]
data[, data_stage := factor(data_stage, levels = c("estimate", "pes_corrected", "unheaped", "raw"))]

# fill outlier_type use to make aggregates have no fill
data[, fill_type := outlier_type]
data[aggregate == T, fill_type := "aggregate"]

# drop wpp total populations where we have age specific data
# most recent WPP
wpp_age_specific_locs <- data[outlier_type == wpp_sources[1] & age_group_id != 22, unique(location_id)]
data <- data[!(outlier_type == wpp_sources[1] & age_group_id == 22 & location_id %in% wpp_age_specific_locs)]
# second most recent WPP
wpp_age_specific_locs <- data[outlier_type == wpp_sources[2] & age_group_id != 22, unique(location_id)]
data <- data[!(outlier_type == wpp_sources[2] & age_group_id == 22 & location_id %in% wpp_age_specific_locs)]

# save total populations
total_data <- data[, list(mean = sum(mean)),
                   by = c("location_id", "year_id", "source_name", "source_group", "outlier_type", "data_stage", "aggregate", "fill_type")]
setcolorder(total_data, c("location_id", "year_id", "source_name", "source_group",
                          "outlier_type", "data_stage", "aggregate", "fill_type", "mean"))
setkeyv(total_data, c("location_id", "year_id", "source_name", "source_group",
                      "outlier_type", "data_stage", "aggregate", "fill_type"))

# age specific populations
data <- data[, list(mean = sum(mean)), by = c("location_id", "year_id", "age_group_id", "outlier_type", "source_name", "source_group", "data_stage", "fill_type")]
setkeyv(data, c("location_id", "year_id", "age_group_id", "outlier_type", "source_name", "source_group", "data_stage", "fill_type"))

# divide pop by age group width
data <- merge(data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
data[, n := age_group_years_end - age_group_years_start]
data[age_group_years_end == age_group_table_terminal_age, n := terminal_age - age_group_years_start]
data[age_group_years_end == age_group_table_terminal_age & age_group_years_start == terminal_age, n := 1] # if it's the terminal age group
data[, mean := mean / n]
setcolorder(data, c("location_id", "year_id", "outlier_type", "source_name", "source_group", "data_stage", "fill_type",
                    "age_group_years_start", "age_group_years_end", "n", "age_group_id", "mean"))
setkeyv(data, c("location_id", "year_id", "outlier_type", "source_name", "source_group", "data_stage", "fill_type",
                "age_group_years_start", "age_group_years_end", "n", "age_group_id"))

# add estimates in 1950 to baseline censuses
baseline_data <- rbind(baseline_census_data,
                       data[data_stage == "estimate" & year_id == 1950,
                            list(location_id, year_id, age_group_id, outlier_type, missing_triangle = F, mean)],
                       use.names = T)
baseline_data <- merge(baseline_data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
setcolorder(baseline_data, c("location_id", "year_id", "outlier_type", "age_group_years_start",
                             "age_group_years_end", "age_group_id", "missing_triangle", "mean"))
setkeyv(baseline_data, c("location_id", "year_id", "outlier_type", "age_group_years_start",
                         "age_group_years_end", "age_group_id", "missing_triangle"))


# Plot settings -----------------------------------------------------------

# pick colors
gg_color_hue <- function(n) {
  hues = seq(15, 375, length = n + 1)
  hcl(h = hues, l = 65, c = 100)[1:n]
}

set_scale_values <- c("#000000", "#808080",
                      "#E41A1C", "#EEC414",
                      "#004BBC", "#FF7F00",
                      "#A55B3B", "#AED6F1",
                      "#FFFFFF")
names(set_scale_values) <- c(wpp_sources[1], wpp_sources[2],
                             "US Census Bureau", "National Comparator",
                             paste0("GBD", gbd_year, "_v", pop_current_round_run_id),
                             paste0("GBD", gbd_year_previous, "_v", pop_previous_round_run_id),
                             "Data: not outliered", "Data: duplicated",
                             "aggregate")

other_scale_names <- setdiff(levels(data$outlier_type), names(set_scale_values))
other_scale_values <- gg_color_hue(length(other_scale_names))
names(other_scale_values) <- other_scale_names

colour_scale_values <- c(set_scale_values, other_scale_values)


# Plot location specific diagnostics --------------------------------------

# drop IPUMS data not being marked as best
data <- data[!(source_group == "IPUMS" & outlier_type != "Data: not outliered")]
data <- data[year_id >= year_start]

if (ihme_loc %in% location_hierarchy$ihme_loc_id & ihme_loc %in% location_specific_settings$ihme_loc_id) {
  baseline_subtitle <- paste0(ifelse(location_specific_settings[ihme_loc_id == ihme_loc, use_backprojected_baseline], "Using", "Not using"), " backwards ccmp baseline. ")
  if (location_specific_settings[ihme_loc_id == ihme_loc, scale_backprojected_baseline]) baseline_subtitle <- paste0(baseline_subtitle, "Scaling to arc backcasted total pop baseline. ")
  if (location_specific_settings[ihme_loc_id == ihme_loc, fill_missing_triangle_above] < terminal_age) baseline_subtitle <- paste0(baseline_subtitle, "Filling missing triangle above: ", location_specific_settings[ihme_loc_id == ihme_loc, fill_missing_triangle_above])
} else {
  baseline_subtitle <- ""
}
# subset to location data
total_plot_data <- total_data[location_id == loc_id]
baseline_plot_data <- baseline_data[location_id == loc_id]
age_specific_plot_data <- data[location_id == loc_id]
age_sex_ratio_plot_data <- age_sex_ratios[location_id == loc_id & between(age_group_years_start, 10, 69)]
age_sex_scores_plot_data <- age_sex_scores[location_id == loc_id & outlier_type == "not outliered"]

# determine if national comparator exists and edit "outlier_type" and "fill_type" columns if so
national_comparator <- as.character(unique(total_plot_data[outlier_type == "National Comparator", source_name]))
national_comparator_exists <- length(national_comparator) > 0
total_plot_data[outlier_type == "National Comparator", c("outlier_type", "fill_type") := list(source_name, source_name)]
age_specific_plot_data[outlier_type == "National Comparator", c("outlier_type", "fill_type") := list(source_name, source_name)]

# edit colour scale values if national comparator exists so that we know what source was
location_scale_values <- copy(colour_scale_values)
if (national_comparator_exists) names(location_scale_values)[names(location_scale_values) == "National Comparator"] <- national_comparator

pdf(paste0(output_dir, "/diagnostics/location_specific/", ihme_loc, "_total_pop.pdf"), width=15, height=10)
total_pop_plot(total_plot_data, loc_name, baseline_subtitle, location_scale_values)
dev.off()

pdf(paste0(output_dir, "/diagnostics/location_specific/", ihme_loc, "_baseline_pop.pdf"), width=15, height=10)
baseline_age_pattern_plot(baseline_plot_data, loc_name, baseline_subtitle, location_scale_values)
dev.off()

pdf(paste0(output_dir, "/diagnostics/location_specific/", ihme_loc, "_age_pattern_pop.pdf"), width=15, height=10)
years <- unique(age_specific_plot_data[data_stage != "estimate", year_id])
for (year in years) {
  baseline_subtitle <- ifelse(year == min(years), baseline_subtitle, "")
  smoother_used <- census_data[location_id == loc_id & year_id == year & outlier_type == "Data: not outliered" & !is.na(smoother), unique(smoother)]
  smoother_subtitle <- ifelse(smoother_used == "none", "No smoother used", paste0("Smoothing method used: ", smoother_used))
  accuracy_index_subtitle <- paste0("Age-sex accuracy index, reported data: ",
                                    round(age_sex_scores_plot_data[year_id == year & type == "reported", age_sex_accuracy_index], 2),
                                    " smoothed data: ",
                                    round(age_sex_scores_plot_data[year_id == year & type == "smoothed", age_sex_accuracy_index], 2))

  subtitle <- paste0(smoother_subtitle, ".\n", accuracy_index_subtitle, ".\n", baseline_subtitle)

  year_specific_plot_data <- age_specific_plot_data[year_id == year]

  age_pattern <- age_pattern_plot(year_specific_plot_data, subtitle, location_scale_values)

  sex_ratio_score_reported <- round(age_sex_scores_plot_data[year_id == year & type == "reported", SRS], 2)
  sex_ratio_score_smoothed <- round(age_sex_scores_plot_data[year_id == year & type == "smoothed", SRS], 2)
  sex_ratio_subtitle <- paste0("SRS reported: ", sex_ratio_score_reported, ". SRS smoothed: ", sex_ratio_score_smoothed)
  sex_ratio <- ggplot(data = age_sex_ratio_plot_data[year_id == year], aes(x = age_group_years_start, y = sex_ratio, colour = type, group = type)) +
    geom_point() + geom_line() +
    geom_hline(yintercept = 100) +
    theme_bw() + theme(legend.position = "bottom") +
    labs(x = "Start of age group", y = "Sex ratio (males / females)", title = "Sex ratio", subtitle = sex_ratio_subtitle)

  male_age_ratio_score_reported <- round(age_sex_scores_plot_data[year_id == year & type == "reported", ARSM], 2)
  male_age_ratio_score_smoothed <- round(age_sex_scores_plot_data[year_id == year & type == "smoothed", ARSM], 2)
  male_age_ratio_subtitle <- paste0("ARSM reported: ", male_age_ratio_score_reported, ". ARSM smoothed: ", male_age_ratio_score_smoothed)
  male_age_ratio <- ggplot(data = age_sex_ratio_plot_data[year_id == year], aes(x = age_group_years_start, y = male_age_ratio, colour = type, group = type)) +
    geom_point() + geom_line() +
    geom_hline(yintercept = 100) +
    theme_bw() + theme(legend.position = "bottom") +
    labs(x = "Start of age group", y = "Age ratio", title = "Male age ratio", subtitle = male_age_ratio_subtitle)

  female_age_ratio_score_reported <- round(age_sex_scores_plot_data[year_id == year & type == "reported", ARSF], 2)
  female_age_ratio_score_smoothed <- round(age_sex_scores_plot_data[year_id == year & type == "smoothed", ARSF], 2)
  female_age_ratio_subtitle <- paste0("ARSF reported: ", female_age_ratio_score_reported, ". ARSF smoothed: ", female_age_ratio_score_smoothed)
  female_age_ratio <- ggplot(data = age_sex_ratio_plot_data[year_id == year], aes(x = age_group_years_start, y = female_age_ratio, colour = type, group = type)) +
    geom_point() + geom_line() +
    geom_hline(yintercept = 100) +
    theme_bw() + theme(legend.position = "bottom") +
    labs(x = "Start of age group", y = "Age ratio", title = "Female age ratio", subtitle = female_age_ratio_subtitle)

  # combine all graphics
  grid::grid.newpage()
  layout <- matrix(c(rep(c(rep(1,10), rep(2,5)), 5), rep(c(rep(1,10), rep(3,5)), 5), rep(c(rep(1,10), rep(4,5)), 5)), nrow=15)
  grid::grid.draw(gridExtra::arrangeGrob(age_pattern, sex_ratio, male_age_ratio, female_age_ratio,
                                         layout_matrix=layout,
                                         top = grid::textGrob(paste0(loc_name, " ", year, " census diagnostics"), just = "center")))
}
dev.off()
