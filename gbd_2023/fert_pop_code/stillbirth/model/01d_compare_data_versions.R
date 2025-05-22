#################################################################
##                                                             ##
## Description: Makes diagnostic plots for changed data        ##
##                                                             ##
#################################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(data.table)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(readr)

library(mortcore)
library(mortdb)

args <- commandArgs(trailingOnly = TRUE)
settings_dir <- args[1]

if (interactive()) {
  version_data <- "Run id"
  main_std_def <- "28_weeks"
  settings_dir <- "FILEPATH/run_settings.csv"
}

load(settings_dir)
list2env(settings, envir = environment())

if (is.na(test_version_data)) test_version_data <- "Run id"

main_std_def <- "28_weeks"

# get location hierarchy
location_hierarchy <- mortdb::get_locations(gbd_year = gbd_year)
location_hierarchy <- location_hierarchy[, .(ihme_loc_id, location_id, location_name)]

########## Download inputs ##########

# Download both versions of the data
old_data <- mortdb::get_mort_outputs(
  model_name = "stillbirth",
  model_type = "data",
  run_id = test_version_data,
  gbd_year = gbd_year,
  hostname = hostname,
  demographic_metadata = TRUE
)

if (!exists("old_data")) {
  old_data <- fread(
    paste0(data_dir, test_version_data, "/FILEPATH/stillbirth_data_database.csv")
  )
  old_data <- merge(
    old_data,
    location_hierarchy,
    by = "location_id",
    all.x = TRUE
  )
}

current_data <- mortdb::get_mort_outputs(
  model_name = "stillbirth",
  model_type = "data",
  run_id = version_data,
  gbd_year = gbd_year,
  outlier_run_id = "active",
  hostname = hostname,
  demographic_metadata = TRUE
)

if (!exists('current_data')) {
  current_data <- fread(
    paste0(data_dir, version_data, "/FILEPATH/stillbirth_data_database.csv")
  )
  current_data <- merge(
    current_data,
    location_hierarchy,
    by = "location_id",
    all.x = TRUE
  )
}

outlier_data <- current_data[outlier == 1, c("year_id", "location_id", "ihme_loc_id", "nid", "std_def_id", "mean")]
outlier_data[, data_change_type := "outliered"]
outlier_data[, run_id_type := "new"]

old_data <- old_data[, c("year_id", "location_id", "ihme_loc_id", "nid", "std_def_id", "mean")]
current_data <- current_data[, c("year_id", "location_id", "ihme_loc_id", "nid", "std_def_id", "mean")]

old_data <- unique(old_data, by = c("year_id", "ihme_loc_id", "nid", "std_def_id"))
current_data <- unique(current_data, by = c("year_id", "ihme_loc_id", "nid", "std_def_id"))

old_data[, mean := round(mean, 5)]
current_data[, mean := round(mean, 5)]

setnames(old_data, "mean", "old")
setnames(current_data, "mean", "new")

# Gets Estimates
estimate_id <- mortdb::get_proc_version(
  model_name = "stillbirth",
  model_type = "estimate",
  run_id = "best",
  gbd_year = gbd_year,
  hostname = hostname
)

estimate_orig <- mortdb::get_mort_outputs(
  model_name = "stillbirth",
  model_type = "estimate",
  run_id = estimate_id,
  gbd_year = gbd_year,
  hostname = hostname
)

estimate <- estimate_orig[dem_measure_id == 6]
estimate <- estimate[, list(
  year_id, location_id, ihme_loc_id,
  data_change_type = paste0("estimate_", wk_cutoff),
  run_id_type = paste0("estimate_", wk_cutoff),
  source_name = paste0("GBD", gbd_year, "_", estimate_id),
  mean, lower, upper
)]

########## Combine together data versions and identify changed points ##########

all_data <- merge(
  current_data,
  old_data,
  by = c("year_id", "location_id", "ihme_loc_id", "nid", "std_def_id"),
  all = TRUE
)

# identify type of change for each data point
all_data[!is.na(new) & is.na(old), data_change_type := "new"]
all_data[is.na(new) & !is.na(old), data_change_type := "dropped"]
all_data[!is.na(new) & !is.na(old) & new != old, data_change_type := "changed"]
all_data[!is.na(new) & !is.na(old) & new == old, data_change_type := "identical"]

# melt mean values
all_data_total <- setDT(melt(
  all_data,
  id.vars = c("year_id", "location_id", "ihme_loc_id", "nid", "std_def_id", "data_change_type"),
  measure.vars = c("new", "old"),
  variable.name = "run_id_type",
  value.name = "mean"
))
all_data_total[, run_id_type := as.character(run_id_type)]

# add outliers
all_data_total <- rbind(all_data_total, outlier_data, fill = TRUE)

# add on standard definitions
std_definitions <- get_mort_ids("std_def")
std_definitions <- std_definitions[, c("std_def_id", "std_def_short")]

all_data_total <- merge(
  all_data_total,
  std_definitions,
  by = "std_def_id",
  all.x = TRUE
)

# add on stillbirth estimates
all_data_total <- rbind(
  all_data_total,
  estimate,
  use.names = TRUE,
  fill = TRUE
)

all_data_total[, data_change_type := factor(data_change_type)]
all_data_total[, run_id_type := factor(run_id_type)]

# remove data before 1980
all_data_total <- all_data_total[year_id >= 1980]

########## Set colors for data by change type ##########

data_change_type_colours <- c("blue", "red", "green", "black", "gray")
names(data_change_type_colours) <- c("new", "dropped", "changed", "identical", "outliered")

########## Make plots for changed locations ##########

changed_locations <- sort(unique(all_data_total[data_change_type %in% c("new", "dropped", "changed"), location_id]))
changed_locations <- as.data.table(changed_locations)
setnames(changed_locations, "changed_locations", "location_id")

matchlist <- c(331, 339, 392, 53433)
other_locs <- changed_locations$location_id %in% matchlist
changed_locations <- rbind(changed_locations[!other_locs,], changed_locations[other_locs,])
changed_locations <- unlist(changed_locations)

changed_locations <- changed_locations[changed_locations %in% location_hierarchy$location_id]

changed_locs_plot_dir <- paste0(
  "FILEPATH/compare_version_", version_data, "_v_",
  test_version_data, "_changed_locs.pdf"
)
pdf(changed_locs_plot_dir, width = 11, height = 8.5)

for (loc_id in changed_locations) {

  ihme_loc <- location_hierarchy[location_id == loc_id, ihme_loc_id]
  loc_name <- location_hierarchy[location_id == loc_id, location_name]

  print(ihme_loc)
  plotdata <- all_data_total[location_id == loc_id]

  p <- ggplot(plotdata, aes(x = year_id, y = mean)) +
       geom_point(data = plotdata[!(data_change_type %like% "estimate") & data_change_type != "outliered"], size = 2.5, aes(fill = data_change_type, colour = data_change_type, alpha = run_id_type)) +
       geom_point(data = plotdata[data_change_type == "outliered"], size = 2.75, shape = 21, aes(fill = data_change_type, colour = data_change_type, alpha = run_id_type)) +
       geom_line(data = plotdata[data_change_type == paste0("estimate_", defs[1])], colour = "#004BBC") +
       geom_line(data = plotdata[data_change_type == paste0("estimate_", defs[2])], colour = "#BC0000") +
       geom_label_repel(data = plotdata[data_change_type != "estimate" & data_change_type != "identical" & !(data_change_type == "dropped" & std_def_id == 10) & std_def_short != main_std_def,],
                        aes(x = year_id, y = mean, label = std_def_short), size = 1.8, label.padding = 0.15, show.legend = F) +
       scale_colour_manual(values = data_change_type_colours, limits = names(data_change_type_colours), name = "Data change type") +
       scale_fill_manual(values = data_change_type_colours, limits = names(data_change_type_colours), name = "Data change type") +
       scale_alpha_manual(values = c(0.4, 0.9, 0.9), limits = c("old", "new", "estimate"), name = "Run id type") +
       theme_bw() +
       labs(title = paste0(loc_name, " (", ihme_loc, ", ", loc_id, "). Stillbirth data"),
            subtitle = paste0("old data run_id: ", test_version_data, ", new data run_id: ", version_data, ", gbd", gbd_year, " best estimate run_id: ", estimate_id, " (", defs[1], " weeks = blue, ", defs[2], " weeks = red)")) +
       scale_x_continuous(name = "Year") +
       scale_y_continuous(name = "SBR per 1000 live births", labels = scales::comma)

  print(p)

}

dev.off()

########## Make plots for all locations ##########

all_locations <- unique(location_hierarchy[, location_id])

all_locs_plot_dir <- paste0("FILEPATH/compare_version_", version_data, "_v_", test_version_data, "_all_locs.pdf")
pdf(all_locs_plot_dir, width = 11, height = 8.5)

for (loc_id in all_locations) {

  ihme_loc <- location_hierarchy[location_id == loc_id, ihme_loc_id]
  loc_name <- location_hierarchy[location_id == loc_id, location_name]

  print(ihme_loc)
  plotdata <- all_data_total[location_id == loc_id]

  p <- ggplot(plotdata, aes(x = year_id, y = mean)) +
       geom_point(data = plotdata[!(data_change_type %like% "estimate") & data_change_type != "outliered"], size = 2.5, aes(fill = data_change_type, colour = data_change_type, alpha = run_id_type)) +
       geom_point(data = plotdata[data_change_type == "outliered"], size = 2.75, shape = 21, aes(fill = data_change_type, colour = data_change_type, alpha = run_id_type)) +
       geom_line(data = plotdata[data_change_type == paste0("estimate_", defs[1])], colour = "#004BBC") +
       geom_line(data = plotdata[data_change_type == paste0("estimate_", defs[2])], colour = "#BC0000") +
       geom_label_repel(data = plotdata[data_change_type != "estimate" & std_def_id == 10,], aes(x = year_id, y = mean, label = std_def_short), size = 1.8, label.padding = 0.15, show.legend = F) +
       scale_colour_manual(values = data_change_type_colours, limits = names(data_change_type_colours), name = "Data change type") +
       scale_fill_manual(values = data_change_type_colours, limits = names(data_change_type_colours), name = "Data change type") +
       scale_alpha_manual(values = c(0.4, 0.9, 0.9), limits = c("old", "new", "estimate"), name = "Run id type") +
       theme_bw() +
       labs(title = paste0(loc_name, " (", ihme_loc, ", ", loc_id, "). Stillbirth data"),
            subtitle = paste0("old data run_id: ", test_version_data, ", new data run_id: ", version_data, ", gbd", gbd_year, " best estimate run_id: ", estimate_id, " (22 weeks = blue, 28 weeks = red)")) +
       scale_x_continuous(name = "Year") +
       scale_y_continuous(name = "SBR per 1000", labels = scales::comma)

  print(p)

}

dev.off()

########## Output files about the changes ##########

new_data <- all_data[data_change_type == "new"]
new_csv_dir <- paste0(
  data_dir, version_data, "/FILEPATH/new_data_",
  version_data, "_v_", test_version_data, ".csv"
)
readr::write_csv(new_data, new_csv_dir)

changed_data <- all_data[data_change_type == "changed"]
changed_csv_dir <- paste0(
  data_dir, version_data, "/FILEPATH/changed_data_",
  version_data, "_v_", test_version_data, ".csv"
)
readr::write_csv(changed_data, changed_csv_dir)

dropped_data <- all_data[data_change_type == "dropped"]
dropped_csv_dir <- paste0(
  data_dir, version_data, "/FILEPATH/dropped_data_",
  version_data, "_v_", test_version_data, ".csv"
)
readr::write_csv(dropped_data, dropped_csv_dir)
