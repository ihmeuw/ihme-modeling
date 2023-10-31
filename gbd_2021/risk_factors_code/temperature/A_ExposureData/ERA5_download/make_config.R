################################################################################
## DESCRIPTION: Makes a config file for the desired ERA5 download.
## INPUTS: None
## OUTPUTS: CSV file
## AUTHOR: EOD team
## DATE CREATED: 31 October 2022
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]



## LOAD DEPENDENCIES -----------------------------------------------------
library(data.table)
library(argparse)



## PARSE ARGS ------------------------------------------------------------
parser <- ArgumentParser()
parser$add_argument("--gbd_round", help = "Round to use in output filepath",
                    default = 'GBD2021', type = "character")
parser$add_argument("--endyear", help = "Last year to calculate for",
                    default = 2021, type = "integer")
parser$add_argument("--config_id", help = "ID, date, or other string to distinguish the output config",
                    default = "20221031", type = "character")
args <- parser$parse_args()
list2env(args, environment()); rm(args)




## BODY ------------------------------------------------------------------
product_types <- c("reanalysis", "ensemble_spread")
variables <- c('2m_temperature', '2m_dewpoint_temperature', 'surface_pressure', '10m_u_component_of_wind', '10m_v_component_of_wind', 'surface_solar_radiation_downwards')
years <- 1980:endyear

output <- CJ("product_type" = product_types, "variable" = variables, "year" = years)



## SAVE OUTPUTS ----------------------------------------------------------
fwrite(output, sprintf("/FILEPATH/A_ExposureData/ERA5_download/config_%s.csv", user, gbd_round, config_id))
