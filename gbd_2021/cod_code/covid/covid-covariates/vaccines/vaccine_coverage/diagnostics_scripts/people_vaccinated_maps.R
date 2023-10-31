
rm(list=ls())

library(argparse)
library(data.table)
library(ggplot2)
library(scales)
library(gridExtra)
library(RColorBrewer)

source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))

source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
source(CODE_PATHS$MAPPING_FUNCTION_PATH)

source(file.path(CODE_PATHS$VACCINE_CODE_ROOT, "vaccine_projection_functions.R"))

## Setup and background
# Set up command line argument parser
parser <- argparse::ArgumentParser(
  description = 'Launch a location-specific COVID results brief',
  allow_abbrev = FALSE
)

parser$add_argument('--version', help = 'Full path to vaccine covariate version')
parser$add_argument('--scenario_name', help = 'Vaccine scenario')

message(paste(c('COMMAND ARGS:',commandArgs(TRUE)), collapse = ' '))
args <- parser$parse_args(commandArgs(TRUE))

vaccine_path <- args$version
scenario_name <- args$scenario_name

hierarchy <- gbd_data$get_covid_modeling_hierarchy()

## Read in data and make plots
  plot_dt <- vaccine_data$load_scenario_forecast(vaccine_path, scenario_name)
  
  #plot_dt <- clean_up(plot_dt)
  loc_order <- hierarchy[location_id %in% unique(plot_dt$location_id)]
  location_plots <- loc_order[order(sort_order)]$location_id

# Map results
  lsvid <- 771
  location_id <- 1
  loc_id <- 1
  
  colors <- c("gray", (brewer.pal(7, "Spectral")))
  colors <- c("gray", viridis(7))
  
pdf(file.path(vaccine_path, glue("{scenario_name}_scenario_vaccinated_maps.pdf")), height = 5.5, width = 11)  
# Dates are April 1, July 1, December 31 2021 (percent of people)
  for(d in c("2021-01-01","2021-02-01","2021-04-01","2021-07-01","2021-12-31")){
    map_dt <- plot_dt[date == d]
    map_dt <- map_dt[!(location_id %in% c(102, 163, 135, 130, 95, 86, 101, 165, 81, 92))]
    map_dt[, percent := cumulative_all_vaccinated / adult_population * 100]
    map_dt[, bin := cut(percent, breaks = c(-1, 0, 5, 10, 25, 50, 70, 100),
           labels = c("0%","0-4%","5-9%","10-24%","25-49%","50-74%",">75%"))]
    generic_map_function(map_dt, 
                         title = paste0("Percent of all adults vaccinated on ", format(as.Date(d), "%B %-d, %Y")), 
                         colors = colors)
    
    # Effectively vaccinated 
    map_dt[, percent := (cumulative_all_effective) / adult_population * 100]
    map_dt[, bin := cut(percent, breaks = c(-1, 0, 5, 10, 25, 50, 70, 100),
                        labels = c("0%","0-4%","5-9%","10-24%","25-49%","50-74%",">75%"))]
    generic_map_function(map_dt, 
                         title = paste0("Percent of all adults effectively vaccinated on ", format(as.Date(d), "%B %-d, %Y")), 
                         colors = colors)
  }

dev.off()

