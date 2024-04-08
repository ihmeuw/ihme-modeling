################################################################################
# Description: Make location-drop_age specific set of diagnostic plots
# - run separately by location and drop_age. Also has option to plot the
#   drop_age selected as best.
# - combines together population data, comparators, prior and posterior with and without raking
# - combines together migration data, comparators, prior and posterior
# - make multiple plots
#   - total population by sex time series
#   - female population by age time series
#   - male population by age time series
#   - total net migration count time series
#   - female migration proportion by age time series
#   - male migration proportion by age time series
# - future plots that would be helpful to add
#   - summary plot we used in GBD2017 showing population change, net migration count, births and deaths for both GBD and WPP
#   - population age pattern in census years and non-census years
#   - migration age pattern
#   - population time series by cohort
################################################################################

library(data.table)
library(ggplot2)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

rm(list = ls())
SLURM_ARRAY_TASK_ID <- Sys.getenv("SLURM_ARRAY_TASK_ID")
USER <- Sys.getenv("USER")
code_dir <- "FILEPATH"
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_vid", type = "character",
                    help = "The version number for this run of population, used to read in settings file")
parser$add_argument('--task_map_dir', type="character",
                    help="The filepath to the task map file that specifies other arguments to run for")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_vid <- "9999"
  args$task_map_dir <- "FILEPATH"
  args$test <- "T"
  SLURM_ARRAY_TASK_ID <- "1"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# use task id to get arguments from task map
task_map <- fread(task_map_dir)
loc_id <- task_map[task_id == SLURM_ARRAY_TASK_ID, location_id]
drop_above_age <- task_map[task_id == SLURM_ARRAY_TASK_ID, drop_above_age]

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "functions/formatting.R"))
source(paste0(code_dir, "functions/diagnostic_plots.R"))
source(paste0(code_dir, "functions/add_data_history.R"))

age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))
location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population" & location_id == loc_id]
ihme_loc <- location_hierarchy[, ihme_loc_id]
loc_name <- location_hierarchy[, location_name]
loc_label <- paste0(loc_name, " (", ihme_loc, ")")
national_location <- as.logical(location_hierarchy[, national_location])

knockouts <- F
location_specific_settings <- fread(paste0(output_dir, "/database/location_specific_settings.csv"))[ihme_loc_id == ihme_loc]
use_migration_data <- ifelse(is.na(location_specific_settings$use_migration_data), F,
                             as.logical(location_specific_settings$use_migration_data))
use_backprojected_baseline <- ifelse(is.na(location_specific_settings$use_backprojected_baseline), T,
                                     as.logical(location_specific_settings$use_backprojected_baseline))

best <- drop_above_age == "best"
if (best) {
  drop_above_age <- fread(paste0(output_dir, "/versions_best.csv"))[ihme_loc_id == ihme_loc, drop_age]
  if (is.na(drop_above_age)) stop("No version is marked as best")
}


# Load population plot inputs ---------------------------------------------

aggregate_plot_age_groups <- function(data, id_vars, value_vars, agg_age_groups) {
  agg_age_groups <- age_groups[age_group_id %in% agg_age_groups, age_group_years_start]
  data <- merge(data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], all.x = T, by = "age_group_id")
  data[, aggregate_ages := paste0("c(", paste0(agg_age_groups, collapse = ","), ")")]
  data <- agg_age_data(data, id_vars = id_vars, value_vars = value_vars, age_grouping_var = "aggregate_ages")
  return(data)
}

# census data
census_data <- fread(file = paste0(output_dir, "/inputs/population.csv"))[location_id == loc_id]
# drop age groups above the modeling pooling age except for the baseline year
census_data[year_id != year_start & age_group_years_start > drop_above_age, drop := T]
census_data[, c("age_group_years_start", "age_group_years_end", "n") := NULL]
census_data <- aggregate_plot_age_groups(census_data, id_vars = census_data_id_vars, value_vars = c("drop", "mean"), agg_age_groups = age_groups[(five_year_model), age_group_id])
census_data[, drop := drop > 0] # if any part of the aggregate age group was marked as drop, then mark as completely drop
census_data[outlier_type == "knockout", drop := T]
census_data <- census_data[, list(location_id, year_id, nid, underlying_nid, detailed_source = source_name, source = "data", sex_id, age_group_id, outlier_type, drop, mean)]

# Previous round census data sources
prev_round_census_nids <- fread(paste0(output_dir, "/database/prev_round_sources.csv"))

# determine if posterior draws exist
posterior_unraked_dir <- paste0(output_dir, loc_id, "/outputs/summary/population_reporting_unraked_ui.csv")
posterior_unraked_exists <- file.exists(posterior_unraked_dir)
posterior_raked_dir <- paste0(output_dir, loc_id, "/outputs/summary/population_reporting_raked_ui.csv")
posterior_raked_exists <- file.exists(posterior_raked_dir)

# prior
prior <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/counts_prior_drop", drop_above_age, ".csv"))
prior_pop <- prior[variable == "population", list(location_id, year_id, sex_id, age_group_id, source = "prior", mean)]
prior_pop <- aggregate_plot_age_groups(prior_pop, id_vars = c("location_id", "year_id", "source"), value_vars = c("mean"), agg_age_groups = age_groups[(five_year_model), age_group_id])

# posterior counts
posterior <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/counts_drop", drop_above_age, ".csv"))
if (!posterior_unraked_exists & !posterior_raked_exists) {
  posterior_pop <- posterior[variable == "population", list(location_id, year_id, sex_id, age_group_id, source = "posterior_unraked", mean)]
  posterior_pop <- aggregate_plot_age_groups(posterior_pop, id_vars = c("location_id", "year_id", "source"), value_vars = c("mean"), agg_age_groups = age_groups[(five_year_model), age_group_id])
  posterior_total_pop <- aggregate_plot_age_groups(posterior_pop, id_vars = c("location_id", "year_id", "source"), value_vars = c("mean"), agg_age_groups = 22)
} else {
  posterior_pop <- NULL
  posterior_total_pop <- NULL
}

# unraked population estimates with ui
if (posterior_unraked_exists & ((!national_location) | (national_location & !posterior_raked_exists))) {
  pop_unraked_ui <- fread(posterior_unraked_dir)
  pop_unraked_ui[, source := ifelse(national_location, "posterior", "posterior_unraked")]
  total_pop_unraked_ui <- pop_unraked_ui[age_group_id == 22]
  pop_unraked_ui <- pop_unraked_ui[age_group_id %in% age_groups[(five_year_model), age_group_id]]
} else {
  pop_unraked_ui <- NULL
  total_pop_unraked_ui <- NULL
}

# raked population estimates with ui
if (posterior_raked_exists) {
  pop_raked_ui <- fread(posterior_raked_dir)
  pop_raked_ui[, source := ifelse(national_location, "posterior", "posterior_raked") ]
  total_pop_raked_ui <- pop_raked_ui[age_group_id == 22]
  pop_raked_ui <- pop_raked_ui[age_group_id %in% age_groups[(five_year_model), age_group_id]]
} else {
  pop_raked_ui <- NULL
  total_pop_raked_ui <- NULL
}

# previous GBD round's best population estimates
gbd_pop_previous <- fread(paste0(output_dir, "/database/gbd_population_previous_round_best.csv"))[location_id == loc_id]
if (nrow(gbd_pop_previous) > 0) { # new locations don't exist in the previous GBD round
  gbd_pop_previous <- gbd_pop_previous[, list(location_id, year_id, sex_id, age_group_id, source = paste0("GBD", gbd_year_previous), mean = population)]
  gbd_pop_previous <- aggregate_plot_age_groups(gbd_pop_previous, id_vars = c("location_id", "year_id", "source"), value_vars = c("mean"), agg_age_groups = age_groups[(five_year_model), age_group_id])
} else {
  gbd_pop_previous <- NULL
}

# current GBD round's best population estimates
gbd_pop_current <- fread(paste0(output_dir, "/database/gbd_population_current_round_best.csv"))[location_id == loc_id]
gbd_pop_current <- gbd_pop_current[, list(location_id, year_id, sex_id, age_group_id, source = paste0("GBD", gbd_year, "_v", pop_current_round_run_id), mean = population)]
gbd_pop_current <- aggregate_plot_age_groups(gbd_pop_current, id_vars = c("location_id", "year_id", "source"),
                                             value_vars = c("mean"), agg_age_groups = age_groups[(five_year_model), age_group_id])

# comparator population estimates
comparators <- fread(paste0(output_dir, "/database/comparators.csv"))[location_id == loc_id]
wpp_sources <- sort(grep("WPP", unique(comparators$detailed_source), value = T), decreasing = T)[1:2]
if (nrow(comparators) > 0) {
  comparators <- comparators[, list(location_id, year_id, sex_id, age_group_id, source = detailed_source, mean)]
  comparators <- aggregate_plot_age_groups(comparators, id_vars = c("location_id", "year_id", "source"),
                                           value_vars = c("mean"), agg_age_groups = age_groups[(five_year_model), age_group_id])

  # only keep the two most recent wpp versions
  comparators <- comparators[!grepl("WPP", source) | source %in% wpp_sources]
} else {
  comparators <- NULL
}


# Combine together population inputs --------------------------------------

## age-specific population
population <- rbind(census_data, prior_pop, posterior_pop,
                    pop_unraked_ui, pop_raked_ui,
                    gbd_pop_previous, gbd_pop_current, comparators,
                    use.names = T, fill = T)
population[is.na(drop), drop := F]
population[is.na(outlier_type), outlier_type := "not outliered"]
if (all(!c("lower", "upper") %in% names(population))) population[, c("lower", "upper") := mean]

# add age group type
population <- merge(population, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], all.x = T, by = "age_group_id")
population[, n := age_group_years_end - age_group_years_start]
population[age_group_years_end == age_group_table_terminal_age, n := 0]
population[, age_group_type := ifelse(n == 0, "Terminal age group", ifelse(n == 5, "5-year age group", "Non 5-year age group"))]
population[, c("age_group_years_end", "n") := NULL]
# final formatting
setcolorder(population, c("location_id", "year_id", "nid", "underlying_nid",
                          "source", "detailed_source", "outlier_type", "drop",
                          "sex_id", "age_group_id", "age_group_years_start", "age_group_type",
                          "mean", "lower", "upper"))
setkeyv(population, c("location_id", "year_id", "nid", "underlying_nid",
                      "source", "detailed_source", "outlier_type", "drop",
                      "sex_id", "age_group_years_start"))

## total population
total_population <- population[!source %in% c("posterior_unraked", "posterior_raked", "posterior")]
total_population[, c("drop", "age_group_years_start", "age_group_type", "lower", "upper") := NULL]
total_population <- aggregate_plot_age_groups(total_population, id_vars = c("location_id", "year_id", "nid", "underlying_nid", "source", "detailed_source", "outlier_type"),
                                              value_vars = c("mean"), agg_age_groups = 22)
total_population <- rbind(total_population, total_pop_unraked_ui, total_pop_raked_ui, posterior_total_pop,
                          use.names = T, fill = T)
total_population <- total_population[sex_id %in% 1:2]
total_population[is.na(outlier_type), outlier_type := "not outliered"]
total_population[, outlier_type := factor(outlier_type, levels=c("not outliered", "knockout", "duplicated", "check extraction", "excluded", "not census year"))]
total_population[, sex_id := factor(sex_id, labels = c("males", "female"))]
if (all(!c("lower", "upper") %in% names(total_population))) total_population[, c("lower", "upper") := mean]
# final formatting
setcolorder(total_population, c("location_id", "year_id", "nid", "underlying_nid",
                                "source", "detailed_source", "outlier_type",
                                "sex_id", "age_group_id", "mean", "lower", "upper"))
setkeyv(total_population, c("location_id", "year_id", "nid", "underlying_nid",
                            "source", "detailed_source", "outlier_type",
                            "sex_id", "age_group_id"))


# Load migration plot inputs ----------------------------------------------

# prior
prior_migration <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/net_migration_prior_drop", drop_above_age, ".csv"))
prior_migration[, source := "prior"]
prior_total_migration <- prior_migration[measure_id == 19 & sex_id == 3 & age_group_id == 22]
prior_migration_proportion <- prior_migration[measure_id == 18 & sex_id %in% 1:2 & age_group_id %in% age_groups[(plot_migration), age_group_id]]

# migration estimates
migration <- fread(paste0(output_dir, loc_id, "/outputs/model_fit/net_migration_posterior_drop", drop_above_age, ".csv"))
migration[, source := "posterior"]
posterior_total_migration <- migration[measure_id == 19 & sex_id == 3 & age_group_id == 22]
posterior_migration_proportion <- migration[measure_id == 18 & sex_id %in% 1:2 & age_group_id %in% age_groups[(plot_migration), age_group_id]]

# current GBD round's best migration estimates
if (!is.na(migration_current_round_run_id)) {
  gbd_migration_current <- fread(paste0(output_dir, "/database/gbd_migration_current_round_best.csv"))[location_id == loc_id]
  gbd_total_migration_current <- gbd_migration_current[measure_id == 19 & age_group_id == 22,
                                                       list(sex_id = 3, source = paste0("GBD", gbd_year, "_v", pop_current_round_run_id), mean = sum(mean)),
                                                       by = c("location_id", "year_id", "age_group_id")]
  gbd_migration_proportion_current <- gbd_migration_current[measure_id == 18 & sex_id %in% 1:2 & age_group_id %in% age_groups[(plot_migration), age_group_id],
                                                            list(location_id, year_id, sex_id, age_group_id, measure_id, mean, source = paste0("GBD", gbd_year, "_v", pop_current_round_run_id))]
} else {
  gbd_total_migration_current <- NULL
  gbd_migration_proportion_current <- NULL
}

# previous GBD round's best migration estimates
gbd_migration_previous <- fread(paste0(output_dir, "/database/gbd_migration_previous_round_best.csv"))[location_id == loc_id]

if (nrow(gbd_migration_previous) > 0) { # new locations don't exist in the previous GBD round
  if (gbd_year_previous == 2017) {
    gbd_pop_previous <- fread(paste0(output_dir, "/database/gbd_population_previous_round_best.csv"))[location_id == loc_id]
    # aggregate from single year age groups to plotting age groups, only needed for GBD2017
    aggregate_migrants <- function(migrants, population) {

      # aggregate net migrant counts
      net_migrants_reporting <- mortcore::agg_results(migrants[, list(location_id, year_id, sex_id, age_group_id, mean)],
                                                      id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                                      value_vars = "mean", agg_hierarchy = F, agg_sex = T, age_aggs = age_groups[(reporting_migration), age_group_id])
      # aggregate population
      pop_net_migrants_reporting <- mortcore::agg_results(population[, list(location_id, year_id, sex_id, age_group_id, mean)],
                                                          id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                                          value_vars = "mean", agg_hierarchy = F, agg_sex = T, age_aggs = age_groups[(reporting_migration), age_group_id])
      pop_net_migrants_reporting <- pop_net_migrants_reporting[age_group_id %in% unique(net_migrants_reporting[, age_group_id])]
      setnames(pop_net_migrants_reporting, "mean", "pop")

      # merge together and calculate net migrant proportions
      net_migration_proportion_reporting <- merge(pop_net_migrants_reporting, net_migrants_reporting,
                                                  by = c("location_id", "year_id", "sex_id", "age_group_id"), all = T)
      net_migration_proportion_reporting <- net_migration_proportion_reporting[, list(mean = mean / pop),
                                                                               by = c("location_id", "year_id", "sex_id", "age_group_id")]

      net_migrants_reporting[, measure_id := 19] # measure id 19 is for continuous counts
      net_migration_proportion_reporting[, measure_id := 18] # measure id 18 is for proportions
      net_migration <- rbind(net_migration_proportion_reporting, net_migrants_reporting)
      setcolorder(net_migration, c("location_id", "year_id", "sex_id", "age_group_id", "measure_id", "mean"))
      setkeyv(net_migration, c("location_id", "year_id", "sex_id", "age_group_id", "measure_id"))
      return(net_migration)
    }
    gbd_migration_previous <- aggregate_migrants(gbd_migration_previous[measure_id == 19, list(location_id, year_id, sex_id, age_group_id, mean)],
                                                 gbd_pop_previous[, list(location_id, year_id, sex_id, age_group_id, mean = population)])
  }
  gbd_total_migration_previous <- gbd_migration_previous[measure_id == 19 & age_group_id == 22 & sex_id == 3,
                                                         list(location_id, year_id, sex_id, age_group_id, measure_id, source = paste0("GBD", gbd_year_previous), mean)]
  gbd_migration_proportion_previous <- gbd_migration_previous[measure_id == 18 & sex_id %in% 1:2 & age_group_id %in% age_groups[(plot_migration), age_group_id],
                                                              list(location_id, year_id, sex_id, age_group_id, measure_id, mean, source = paste0("GBD", gbd_year_previous))]

} else {
  gbd_total_migration_previous <- NULL
  gbd_migration_proportion_previous <- NULL
}

# comparator UN WPP total migration estimates
wpp_total_migration <- fread(paste0(output_dir, "/database/wpp_total_net_migration.csv"))
wpp_total_migration <- wpp_total_migration[ihme_loc_id == ihme_loc,
                                           list(location_id = loc_id, year_id, sex_id, age_group_id,
                                                measure_id, source, mean = mean / 5)]


# Combine together migration inputs ---------------------------------------

total_migration <- rbind(prior_total_migration, posterior_total_migration, wpp_total_migration,
                         gbd_total_migration_current, gbd_total_migration_previous, use.names = T, fill = T)
if (all(!c("lower", "upper") %in% names(total_migration))) total_migration[, c("lower", "upper") := mean]

migration_proportion <- rbind(prior_migration_proportion, posterior_total_migration, posterior_migration_proportion,
                              gbd_migration_proportion_current, gbd_migration_proportion_previous, use.names = T, fill = T)
if (all(!c("lower", "upper") %in% names(migration_proportion))) migration_proportion[, c("lower", "upper") := mean]
migration_proportion <- merge(migration_proportion, age_groups[, list(age_group_id, age_group_years_start)], all.x = T, by = "age_group_id")


# Plot set up -------------------------------------------------------------

# colour scales
scale_values <- c("GBD" = "#FF00FF", "GBD" = "#FF7F00",
                  "wpp1" = "#000000", "wpp2" = "#808080",
                  "US Census Bureau" = "#E41A1C",
                  "posterior" = "#004BBC", "posterior_raked" = "#004BBC", "posterior_unraked" = "#4DAF4A",
                  "prior" = "#4DAF4A", "data" = "#A55B3B")
names(scale_values)[1] <- paste0("GBD", gbd_year, "_v", pop_current_round_run_id)
names(scale_values)[2] <- paste0("GBD", gbd_year_previous)
names(scale_values)[3] <- wpp_sources[1]
names(scale_values)[4] <- wpp_sources[2]
scale_values <- c(scale_values)
scale_names <- names(scale_values)

# shape scales
outlier_type_shape_names <- c("not outliered", "knockout", "duplicated", "check extraction", "excluded", "not census data")
outlier_type_shape_values <- c(16, 16, 17, 18, 15, 3)
names(outlier_type_shape_values) <- outlier_type_shape_names

age_group_type_shape_names <- c("5-year age group", "Non 5-year age group", "Terminal age group")
age_group_type_shape_values <- c(21, 22, 24)
names(age_group_type_shape_values) <- age_group_type_shape_names

# make subtitle containing version information
current_round_last_drop_above_age <- fread(paste0("FILEPATH", pop_current_round_run_id, "/versions_best.csv"))[ihme_loc_id == ihme_loc, drop_age]
if (gbd_year_previous == 2017) {
  previous_round_last_drop_above_age <- readRDS(paste0(output_dir, "/database/GBD2019_best_versions.RDS"))
  previous_round_last_drop_above_age <- previous_round_last_drop_above_age[ihme_loc_id == ihme_loc, as.numeric(gsub("v", "", model_version))]
} else {
  previous_round_last_drop_above_age <- fread(paste0("FILEPATH", pop_previous_round_run_id, "/versions_best.csv"))[ihme_loc_id == ihme_loc, drop_age]

}
terminal_age_subtitle <- paste0("Census data points over age ", drop_above_age, " excluded. GBD", gbd_year_previous, ": ", previous_round_last_drop_above_age,
                                ". ", paste0("GBD", gbd_year, "_v", pop_current_round_run_id), ": ", current_round_last_drop_above_age, ".")
version_subtitle <- paste0("New pop version: ", pop_reporting_vid, ", Previous best pop version: ", pop_current_round_run_id,
                           ", Full lifetable/no shocks death number version: ", no_shock_death_number_vid, "/", full_life_table_run_id, ", Under 5 envelope version: ", u5_vid,
                           ", Fertility version: ", asfr_vid, ", SRB version: ", srb_vid)
migration_subtitle <- paste0(if(use_backprojected_baseline) "Using backprojected baseline.",
                             if (use_backprojected_baseline & use_migration_data) " ",
                             if(use_migration_data) "Using migration data.")

full_subtitle <- paste0(terminal_age_subtitle, "\n", version_subtitle, "\n", migration_subtitle)


# Make plots --------------------------------------------------------------

plot_fpath <- paste0(output_dir, "/diagnostics/", ifelse(best, "location_best", "location_drop_age"), "/model_fit_", ihme_loc, ifelse(best, "_best", paste0("_drop", drop_above_age)), ".pdf")
pdf(plot_fpath, width = 15, height = 10)

plot_sources <- c("data", wpp_sources, "US Census Bureau",
                  paste0("GBD", gbd_year, "_v", pop_current_round_run_id),
                  paste0("GBD", gbd_year_previous),
                  "posterior", "posterior_raked", "posterior_unraked", "prior")

## total population
plot_data <- total_population[source %in% plot_sources]
p <- plot_time_series(plot_data = plot_data,
                      scale_values = scale_values[scale_names %in% unique(plot_data$source)],
                      plot_title = paste(loc_label, "total population"), plot_subtitle = full_subtitle, plot_y_var = "population",
                      facet_var = "sex_id", facet_scales = "fixed",
                      data_shape_var = "outlier_type", data_shape_values = outlier_type_shape_values[outlier_type_shape_names %in% unique(plot_data$outlier_type)], data_shape_label_name = "Outlier Type",
                      data_size = 4)
# add a layer which shows which data points are new
p <- add_data_history(plot_data = plot_data,
                      plot = p,
                      prev_round_census_nids = prev_round_census_nids
                      )
print(p)

## age-specific population time series
# females
plot_data <- population[sex_id == 2 & source %in% plot_sources]
# remove data not used in modeling process because of space issues
plot_data <- plot_data[!(source == "data" & !outlier_type %in% c("not outliered", "knockout"))]
# remove non-standard age groups that aren't data
plot_data <- plot_data[!(source != "data" & !age_group_id %in% age_groups[(five_year_model), age_group_id])]
p <- plot_time_series(plot_data = plot_data,
                      scale_values = scale_values[scale_names %in% unique(plot_data$source)],
                      plot_title = paste(loc_label, "female age-specific population"), plot_subtitle = full_subtitle, plot_y_var = "population",
                      facet_var = "age_group_years_start", facet_scales = "free_y",
                      data_shape_var = "age_group_type", data_shape_values = age_group_type_shape_values, data_shape_label_name = "Age group type",
                      data_size = 2, line_size = 0.5)
# add a layer which shows which data points are new
p <- add_data_history(plot_data = plot_data,
                      plot = p,
                      prev_round_census_nids = prev_round_census_nids
)
print(p)

# males
plot_data <- population[sex_id == 1 & source %in% plot_sources]
# remove data not used in modeling process because of space issues
plot_data <- plot_data[!(source == "data" & !outlier_type %in% c("not outliered", "knockout"))]
# remove non-standard age groups that aren't data
plot_data <- plot_data[!(source != "data" & !age_group_id %in% age_groups[(five_year_model), age_group_id])]
p <- plot_time_series(plot_data = plot_data,
                      scale_values = scale_values[scale_names %in% unique(plot_data$source)],
                      plot_title = paste(loc_label, "male age-specific population"), plot_subtitle = full_subtitle, plot_y_var = "population",
                      facet_var = "age_group_years_start", facet_scales = "free_y",
                      data_shape_var = "age_group_type", data_shape_values = age_group_type_shape_values, data_shape_label_name = "Age group type",
                      data_size = 2, line_size = 0.5)
# add a layer which shows which data points are new
p <- add_data_history(plot_data = plot_data,
                      plot = p,
                      prev_round_census_nids = prev_round_census_nids
)
print(p)

## total net migration counts
plot_data <- total_migration[source %in% plot_sources]
p <- plot_time_series(plot_data = plot_data,
                      scale_values = scale_values[scale_names %in% c(unique(plot_data$source), "data")],
                      plot_title = paste(loc_label, "total net number of migrants"), plot_subtitle = full_subtitle, plot_y_var = "net number of migrants",
                      data_size = 3)
print(p)

## female age-specific net migration proportion time series
plot_data <- migration_proportion[sex_id == 2 & source %in% plot_sources]
p <- plot_time_series(plot_data = plot_data, scale_values = scale_values[scale_names %in% c(unique(plot_data$source), "data")],
                      plot_title = paste(loc_label, "female net migration proportion"), plot_subtitle = full_subtitle, plot_y_var = "net migration proportion",
                      facet_var = "age_group_years_start", facet_scales = "fixed")
print(p)

## male age-specific net migration proportion time series
plot_data <- migration_proportion[sex_id == 1 & source %in% plot_sources]
p <- plot_time_series(plot_data = plot_data, scale_values = scale_values[scale_names %in% c(unique(plot_data$source), "data")],
                      plot_title = paste(loc_label, "male net migration proportion"), plot_subtitle = full_subtitle, plot_y_var = "net migration proportion",
                      facet_var = "age_group_years_start", facet_scales = "fixed")
print(p)

dev.off()
