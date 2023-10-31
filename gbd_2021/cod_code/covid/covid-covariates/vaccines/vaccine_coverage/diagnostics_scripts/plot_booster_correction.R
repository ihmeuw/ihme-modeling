# Description:
# Plot cumulative last shots in arms by location as distributed by the brand code, and time-corrected by booster_correction.
# Shows point values for the most recent date the boosters are corrected against. 
# Shows both "reference" and "optimal" delivery scenarios.
# Show current date as a vertical line.
# These plots are now required since booster_correction is parallelized.

# Setup ----

library(argparse)
library(data.table)
library(dplyr)
library(ggplot2)
library(stringr)

# Parser ----

if (!interactive()) {
  parser <- argparse::ArgumentParser(
    description = 'Plot booster correction.',
    allow_abbrev = FALSE
  )
  
  parser$add_argument(
    '--output_path',
    type = "character",
    help = 'Full path to vaccine covariate version'
  )
  
  message(paste(c('COMMAND ARGS:',commandArgs(TRUE)), collapse = ' '))
  args <- parser$parse_args(commandArgs(TRUE))
  
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  
  for (arg in names(args)) {
    message(paste0(arg, ": ", get(arg), "\n"))
  }
  
}


# Paths ----
source(file.path("FILEPATH", Sys.info()["user"], "FILEPATH/paths.R"))
source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
model_parameters <- vaccine_data$load_model_parameters(output_path)
write_out_path <- file.path(output_path, 'booster_correction.pdf')


# Data & hierarchy ----
data_list <- list(
  reference_pre = fread(file.path(output_path, "uncorrected_last_shots_in_arm_by_brand_w_booster_reference.csv")),
  reference_post = fread(file.path(output_path, "last_shots_in_arm_by_brand_w_booster_reference.csv")),
  optimal_pre = fread(file.path(output_path, "uncorrected_last_shots_in_arm_by_brand_w_booster_optimal.csv")),
  optimal_post = fread(file.path(output_path, "last_shots_in_arm_by_brand_w_booster_optimal.csv"))
)

point_est <- fread(file.path(output_path, "booster_point_estimate_for_plots.csv"))
setnames(point_est, "date", "date_point")

hierarchy <- gbd_data$get_covid_modeling_hierarchy()


# PREP DATA ----------------------------------------------------------------
# Calculate daily and cumulative quantities by location and course

for (name in names(data_list)) {
  
  df <- data_list[[name]]
  
  data_list[[name]] <- rbindlist(
    
    lapply(split(df, list(df$location_id, df$vaccine_course)), function(x) {
      
      x$daily <- rowSums(x[,-c(1:4)])
      x$cumul <- cumsum(x$daily)
      return(x)
    })
  )
  
  # Filter for only boosters, select plotting columns
  data_list[[name]] <- data_list[[name]][vaccine_course >= 2,]
  data_list[[name]] <- data_list[[name]][, fix_status := name]
  data_list[[name]] <- data_list[[name]][, .(location_id, date, vaccine_course, daily, cumul, fix_status)]
  
}

# create tibble for plotting
all_scenarios <- rbindlist(data_list) %>% as_tibble()
all_scenarios <- 
  all_scenarios %>% 
  mutate(booster_course = paste0("booster_", vaccine_course - 1L))

# PLOTS -----------------------------------------------------------------------

message("Plotting booster correction - cumulative plots.")
pdf(write_out_path, width = 21, height = 11, onefile = TRUE)

for(LOC in unique(all_scenarios$location_id)) {

  plot_data <- all_scenarios %>% filter(location_id == LOC) 
  plot_points <- point_est %>% filter(location_id == LOC)
  
  title <- hierarchy[location_id == LOC, .(location_id, location_name)]
  message(title$location_id, " : ", title$location_name)
  
  p <-
    plot_data %>%
    ggplot(aes(x = date, y = cumul)) +
    geom_line(aes(color = fix_status), size = 1) +
    geom_point(data = plot_points, mapping = aes(x = date_point, y = value), shape = 18, size = 4) +
    # geom_point(data = plot_points, mapping = aes(x = date_point, y = match_val, color = 'post'), size = 2, shape = 18) +
    geom_point(data = plot_points, mapping = aes(x = obs_dat, y = value), shape = 19, size = 3) +
    # geom_point(data = plot_points, mapping = aes(x = obs_dat, y = value, color = 'pre'), size = 2, shape = 18) +
    geom_vline(xintercept = Sys.Date(), linetype = 'dashed') +
    geom_text(aes(x=Sys.Date(), label=paste0("\n", Sys.Date()), y=max(cumul * 0.1)), colour="black", angle=90, size=3) +
    ggtitle(unique(title$location_name), title$location_id)+
    facet_wrap(~booster_course) +
    scale_color_manual(values = c("#1F78B4", "#85C3E4", "#E31A1C", "#FD7776")) +
    # scale_shape_manual(values = c(18, 19)) +
    theme_minimal()
  
  print(p)
  
}

dev.off()

# scoping --------------------------------------------------------------------
# output_path <- .output_path
# checkpoint <- copy(data_list)
# data_list <- copy(checkpoint)
# str(data_list)
# str(data_list[[1]])
# df <- data_list[[1]]
# df <- split(df, list(df$location_id, df$vaccine_course))
# scoping --------------------------------------------------------------------