# Meta -------------------------------------------------------------------------

# Description: Compare current processing output with comparison output
# Steps:
#   1. Download data from current and comparison output files
#   2. Prep date for plotting
#   3. Create comparison plots
#      a. plot comparing model covariates
# Inputs:
#   * Current Data
#   * Comparison Data
# Outputs:
#   * 


# Load libraries ---------------------------------------------------------------

library(data.table)
library(ggplot2)
library(stringr)


# Command line arguments -------------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)


# Setup ------------------------------------------------------------------------

# get config
config <- config::get(
  file = paste0(),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

run_id <- str_sub(main_dir, start=-16)


# Load Data --------------------------------------------------------------------

plot_cols <- c("deaths", "deaths_covid", "mobility_lagged", "idr_lagged",
               "confirmed_cases")
plot_cols_ext <- c("deaths_excess", "deaths_covid", "mobility_lagged", "idr_lagged",
                   "confirmed_cases")

# comparison
comparison <- assertable::import_files(
  filenames = list.files(paste0(),
                         pattern = ".csv"),
  folder = paste0()
)
comp_plot_cols <- unlist(lapply(plot_cols, function(x) paste0(x,"_comparison")))
setnames(comparison, plot_cols, comp_plot_cols)

# comparison external
comparison_ext <- assertable::import_files(
  filenames = list.files(paste0(),
                         pattern = ".csv"),
  folder = paste0()
)
comp_plot_cols <- unlist(lapply(plot_cols_ext, function(x) paste0(x,"_comparison")))
setnames(comparison_ext, plot_cols_ext, comp_plot_cols)

# current
current <- assertable::import_files(
  filenames = list.files(),
  folder = paste0()
)
curr_plot_cols <- unlist(lapply(plot_cols, function(x) paste0(x,"_current")))
setnames(current, plot_cols, curr_plot_cols)

# current external
current_ext <- assertable::import_files(
  filenames = list.files(paste0(), pattern = ".csv"),
  folder = paste0()
)
curr_plot_cols <- unlist(lapply(plot_cols_ext, function(x) paste0(x,"_current")))
setnames(current_ext, plot_cols_ext, curr_plot_cols)

# combine external
comparison <- rbind(comparison, comparison_ext, fill = T)
current <- rbind(current, current_ext, fill = T)

# add version
current[, version := run_id]
comparison[, version := comparison_run_id]


# Compare ----------------------------------------------------------------------

dt <- merge(
  current, comparison,
  by = c("location_id", "age_start", "age_end", "year_start",
         "time_start", "time_unit", "sex", "age_name","model_type"),
  all = T
)
dt <- dt[age_name == "0 to 125" & sex == "all"]
dt[is.na(model_type), model_type := "Stage 1 input"]
dt <- dt[(model_type == "Stage 1 input" & !is.na(deaths_current)) |
         (model_type == "external" & !is.na(deaths_excess_current))]


# Covariates -------------------------------------------------------------------

dt <- demInternal::ids_names(
  dt, id_cols = "location_id", extra_output_cols = c("ihme_loc_id"),
  gbd_year = 2020, warn_only = T
)
dt[location_id == 432, `:=` (location_name = "England & Wales", ihme_loc_id = "GBR_432")]
dt[location_id == 60412, `:=` (location_name = "Wuhan", ihme_loc_id = "CHN_60412")]
dt[location_id == 99999, `:=` (location_name = "Trentino-Alto Adige/Sudtirol", ihme_loc_id = "ITA_99999")]
dt[is.na(location_id), `:=` (location_id = 999, location_name = "Mumbai", ihme_loc_id = "IND_999")]
dt[time_unit == "month", n_units := 12]
dt[time_unit == "week", n_units := 53]
dt[, time_id := year_start + (time_start - 1) / n_units]
dt[ihme_loc_id=="IND_999", time_id := 2020]
weekly_locs <- unique(dt[time_unit == "week" &
                           ((!is.na(deaths_current) & model_type == "Stage 1 input") |
                            (!is.na(deaths_excess_current) & model_type == "external"))]$location_id)
dt <- dt[(location_id %in% weekly_locs & time_unit == "week") |
           (!location_id %in% weekly_locs & time_unit == "month")]
# color palette
color_palette <- c("blue", "darkorange")
names(color_palette) <- c(
  paste0("old: ", comparison_run_id),
  paste0("new: ", run_id)
)

# Time series of all locations
for (var in c("deaths_excess", plot_cols)) {
  message(var)
  pdf(paste0(),
      width = 13, height = 8)
  for (loc in unique(dt$ihme_loc_id)) {
    temp <- dt[ihme_loc_id == loc]
    if (var != "deaths"){
      temp <- temp[time_id >= 2020]
    }
    if (var == "deaths") {
      temp <- temp[model_type == "Stage 1 input"]
    }
    if (var == "deaths_excess") {
      temp <- temp[model_type == "external"]
    }
    loc_name <- unique(temp$location_name)
    for (type in unique(temp$model_type)){
      gg <- ggplot(data = temp[model_type == type], aes(x = time_id)) +
      geom_point(aes(y = get(paste0(var,"_comparison")), color = paste0("old: ", comparison_run_id)), alpha = 0.5) +
      geom_point(aes(y = get(paste0(var,"_current")), color = paste0("new: ", run_id)), alpha = 0.5) +
      theme_bw() +
      labs(x = "time", y = var, color = "version",
           title = paste0(loc_name,", comparison id: ",comparison_run_id,
                          ", model type: ", type)) +
      scale_color_manual(values = color_palette)
    print(gg)
    }
  }
  dev.off()
}


# Ratios -----------------------------------------------------------------------

message("Ratio - external")
pdf(paste0(),
    width = 13, height = 8)
for (loc in unique(dt[model_type=="external" & (!deaths_covid_current == 0)]$ihme_loc_id)) {
  temp <- dt[ihme_loc_id == loc & time_id >= 2020 & model_type == "external"]
  loc_name <- unique(temp$location_name)
  type <- unique(temp$model_type)

  gg <- ggplot(data = temp, aes(x = time_id)) +
    geom_point(aes(y = deaths_excess_comparison / deaths_covid_comparison,
                   color = paste0("old: ", comparison_run_id)), alpha = 0.5) +
    geom_point(aes(y = deaths_excess_current / deaths_covid_current,
                   color = paste0("new: ", run_id)), alpha = 0.5) +
    theme_bw() +
    labs(x = "time", y = "ratio: excess / covid", color = "version",
         title = paste0(loc_name,", comparison id: ",comparison_run_id,
                        ", model type: ", type)) +
    scale_color_manual(values = color_palette)
  print(gg)
}
dev.off()


# Covariate Comparison ---------------------------------------------------------

message("Covariates by location")
pdf(paste0(),
    width = 13, height = 8)
for (loc in unique(dt$ihme_loc_id)) {
  temp <- dt[ihme_loc_id == loc]
  temp[, idr_lagged_current := idr_lagged_current * 1000]
  loc_name <- unique(temp$location_name)
  for (type in unique(temp$model_type)){
    gg <- ggplot() +
      # deaths covid
      geom_point(temp, mapping = aes(x = time_id, y = deaths_covid_current, color = "Covid Deaths")) +
      geom_line(temp, mapping = aes(x = time_id, y = deaths_covid_current, color = "Covid Deaths")) +
      # IDR
      geom_point(temp, mapping = aes(x = time_id, y = idr_lagged_current, color = "IDR (lagged * 1000)")) +
      geom_line(temp, mapping = aes(x = time_id, y = idr_lagged_current, color = "IDR (lagged * 1000)")) +
      theme_bw() +
      labs(x = "time", y = "", color = "version",
           title = paste0(loc_name)) +
      scale_color_manual(values = c("VR Deaths" = "#3fa95c",
                                    "Covid Deaths" = "purple",
                                    "Excess Deaths" = "orange",
                                    "IDR (lagged)" = "pink"))

      if (nrow(temp[model_type=="Stage 1 input"]) > 0) {
        gg <- gg +
        # deaths
        geom_point(temp[model_type=="Stage 1 input"], mapping = aes(x = time_id, y = deaths_current, color = "VR Deaths")) +
        geom_line(temp[model_type=="Stage 1 input"], mapping = aes(x = time_id, y = deaths_current, color = "VR Deaths"))
      }
      if (nrow(temp[model_type=="external"]) > 0) {
        gg <- gg +
        # deaths excess
        geom_point(temp[model_type=="external"], mapping = aes(x = time_id, y = deaths_excess_current, color = "Excess Deaths")) +
        geom_line(temp[model_type=="external"], mapping = aes(x = time_id, y = deaths_excess_current, color = "Excess Deaths"))
      }

    print(gg)

  }
}
dev.off()
