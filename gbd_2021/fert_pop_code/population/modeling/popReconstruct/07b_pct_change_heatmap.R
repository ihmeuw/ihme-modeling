################################################################################
# Description: Create percent change heatmaps comparing to previously marked
# best population estimate version.
# - read in newly created population estimates and last population estimates
# - make percent change heatmaps for specified age groups
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
  args$pop_vid <- "99999"
  args$task_map_dir <- "FILEPATH"
  args$test <- "T"
  SLURM_ARRAY_TASK_ID <- "1"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# use task id to get arguments from task map
task_map <- fread(task_map_dir)
comparison_round <- task_map[task_id == SLURM_ARRAY_TASK_ID, comparison_round]

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population"][order(path_to_top_parent)]
location_hierarchy[, order := .I]
location_hierarchy[, loc_label := paste0(location_name, " (", ihme_loc_id, ", ", location_id, ")")]
location_hierarchy[, loc_label := factor(loc_label, levels = location_hierarchy[order(-order), loc_label])]

age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))


# Get new and old populations ---------------------------------------------

# determine whether the raking/aggregating step was run
use_unraked_estimates <- length(list.files(paste0(output_dir, "raking_draws"))) == 0
if (use_unraked_estimates) location_hierarchy <- location_hierarchy[is_estimate == 1]

new_pop <- lapply(location_hierarchy[, location_id], function(loc_id) {
  fpath <- paste0(output_dir, loc_id, "/outputs/summary/population_", ifelse(use_unraked_estimates, "un", ""), "raked_ui.csv")
  if (file.exists(fpath)) {
    summary <- fread(fpath)
  } else {
    print(paste0("No population estimates available for ", location_hierarchy[location_id == loc_id, ihme_loc_id]))
    summary <- NULL
  }
  return(summary)
})
new_pop <- rbindlist(new_pop)
new_pop <- new_pop[, list(location_id, year_id, sex_id, age_group_id, new = mean)]
new_pop <- mortcore::agg_results(new_pop, id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                 value_vars = c("new"), agg_hierarchy = F, age_aggs = heatmap_age_groups,
                                 agg_sex = use_unraked_estimates)
new_pop <- new_pop[age_group_id %in% heatmap_age_groups]

if (comparison_round == "WPP") {
  old_pop <- fread(paste0(output_dir, "/database/comparators.csv"))
  wpp_source <- sort(grep("WPP", unique(old_pop$detailed_source), value = T), decreasing = T)[1]
  old_pop <- old_pop[detailed_source == wpp_source & location_id, list(location_id, year_id, sex_id, age_group_id, old = mean)]

  # aggregate age specific wpp data
  old_pop_age_specific <- old_pop[!(age_group_id == 22 & sex_id == 3)]
  old_pop_age_specific <- mortcore::agg_results(old_pop_age_specific, id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                                value_vars = c("old"), agg_hierarchy = F, age_aggs = heatmap_age_groups, agg_sex = T)
  old_pop_age_specific <- old_pop_age_specific[age_group_id %in% heatmap_age_groups]

  # find locations wpp only estimates all age both sexes
  old_pop_total <- old_pop[age_group_id == 22 & sex_id == 3 & !location_id %in% unique(old_pop_age_specific$location_id)]

  old_pop <- rbind(old_pop_total, old_pop_age_specific, use.names = T)

} else {
  old_pop <- fread(paste0(output_dir, "/database/gbd_population_", comparison_round, "_round_best.csv"))
  old_pop <- old_pop[, list(location_id, year_id, sex_id, age_group_id, old = population)]
  old_pop <- mortcore::agg_results(old_pop, id_vars = c("location_id", "year_id", "sex_id", "age_group_id"),
                                   value_vars = c("old"), agg_hierarchy = F, age_aggs = heatmap_age_groups, agg_sex = T)
  old_pop <- old_pop[age_group_id %in% heatmap_age_groups]
}

pop <- merge(new_pop, old_pop, by = c("location_id", "year_id", "sex_id", "age_group_id"), all = T)
pop <- pop[!is.na(new) & !is.na(old)]
if (nrow(pop) == 0) stop("No population estimates for both the new run and comparison run")

# Make heatmaps -----------------------------------------------------------

# Define heatmap function
plot_heatmap <- function(data, plot_title, plot_age_group_id = 22,
                         plot_sex_id = 3, max_pct_chg = 25) {

  plot_year_ids <- c(unique(seq(data[!is.na(new) & !is.na(old), min(year_id)],
                                data[!is.na(new) & !is.na(old), max(year_id)], 5)),
                     data[!is.na(new) & !is.na(old), max(year_id)])

  data <- data[year_id %in% plot_year_ids]
  data <- data[age_group_id %in% plot_age_group_id]
  data <- data[sex_id %in% plot_sex_id]

  data[, pct_chg := ((new - old) / old) * 100]

  data[pct_chg < -max_pct_chg, fill_pct_chg := -max_pct_chg]
  data[pct_chg > max_pct_chg, fill_pct_chg := max_pct_chg]
  data[is.na(fill_pct_chg), fill_pct_chg := pct_chg]

  data[, label_pct_chg := round(pct_chg, 0)]

  data <- merge(data, location_hierarchy[, list(location_id, loc_label)],
                by = c("location_id"), all.x = T)
  heatmap <- ggplot(data, aes(x = factor(year_id), y = loc_label)) +
    geom_tile(aes(fill = fill_pct_chg)) +
    geom_text(aes(label = label_pct_chg)) +
    scale_fill_distiller(palette = "RdYlBu", limits = c(-max_pct_chg, max_pct_chg), breaks = seq(-20, 20, 10)) +
    scale_x_discrete(position = "top") +
    theme_bw() + theme(legend.position = "top") +
    labs(x = "Year", y = "Location", fill = "% Change ((new_pop - old_pop) / old_pop)",
         title = plot_title)
  print(heatmap)
}


# plot title
if (comparison_round != "WPP") {
  comparison_round_vid <- ifelse(comparison_round == "current", pop_current_round_run_id, pop_previous_round_run_id)
  overall_title <- paste0("population percent change between ", ifelse(test, "test", "new"), " version ", pop_reporting_vid,
                          ifelse(use_unraked_estimates, " (unraked)", ""), " and ", comparison_round, " round best version ", comparison_round_vid)
} else {
  overall_title <- paste0("population percent change between ", ifelse(test, "test", "new"), " version ", pop_reporting_vid,
                          ifelse(use_unraked_estimates, " (unraked)", ""), " and ", wpp_source)
}

# determine pdf height
num_locations <- length(pop[!is.na(new) & !is.na(old), unique(location_id)])
pdf_height <- max(num_locations / 3, 10) # about locations per inch

plot_fpath <- paste0(output_dir, "/diagnostics/pct_change_heatmap_", comparison_round, ".pdf")
pdf(plot_fpath, width = 15, height = pdf_height)
for (age_group in heatmap_age_groups) {
  print(age_group)

  plot_heatmap(pop, plot_title = paste0(age_groups[age_group_id == age_group, age_group_alternative_name], ", both sexes, ", overall_title),
               plot_age_group_id = age_group, plot_sex_id = 3)

}
dev.off()
