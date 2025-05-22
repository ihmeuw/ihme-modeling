#######################
# Trim 2019 model values to only the remaining reported locations for the year.
# Additionally, add in recent Angola case for GBD study year 2019.
# Additionally, Ensure Nigeria and subnationals are 0 from 2010 onwards.
# Adding non-endemic cases post-2019 directly
# Note: check bundles for non-endemic cases - if not in bundle, either add or create new bundle
#######################

### ======================= BOILERPLATE ======================= ###

rm(list=ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

## Define paths 
# Toggle btwn production arg parsing vs interactive development
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  parser$add_argument("--release_id", type = "character")  
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir  <- paste0(data_root, "/", cause, "/params")
  draws_dir   <- paste0(data_root, "/", cause, "/runs/run1_development/draws")
  interms_dir <- paste0(data_root, "/", cause, "/runs/run1_development/interms")
  logs_dir    <- paste0(data_root, "/", cause, "/runs/run1_development/logs")
}

##	Source relevant libraries
library(data.table)
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_demographics.R")



### Define Constants
release_id = release_id
loc_meta = get_location_metadata(ADDRESS, release_id = release_id)

### ======================= MAIN EXECUTION ======================= ###

### LOAD DRAWS 
draws_df = fread(paste0(interms_dir, '/FILEPATH'))
draw_cols <- paste0('draw_',0:999)
                
end_locs <- subset(loc_meta, location_name %in% c(loc_names'))$location_id
                                                  
### Zero locs not currently endemic to zero 2018+
locs_to_zero <- setdiff(unique(draws_df$location_id),end_locs)
draws_df[location_id %in% locs_to_zero & year_id %in% 2017:2024, paste0("draw_", 0:999) := 0]

### ADD IN ANGOLA CASE, AS DRAW RATE
### ADD IN SINGLE CASE REPORTS FOR CAMEROON (2023) & CAF (2022,2023) & MISSING ANGOLA CASE IN 2018

# Angola - April 2018 - 8 yr old girl
case_pop <- get_population(age_group_id=6, location_id=ADDRESS, year_id=2018, sex_id=2, release_id = release_id)
draws_df[sex_id==2 & age_group_id==6 & location_id==ADDRESS & year_id == 2018, paste0("draw_", 0:999) := lapply(0:999, function(x)
  1 / case_pop$population)]

# Angola - January 2019 - 48 yr old female
case_pop <- get_population(age_group_id=14, location_id=ADDRESS, year_id=2019, sex_id=2, release_id = release_id)
draws_df[sex_id==2 & age_group_id==14 & location_id==ADDRESS & year_id == 2019, paste0("draw_", 0:999) := lapply(0:999, function(x)
  1 / case_pop$population)]

# Angola - March 2020 - 15 yr old boy
case_pop <- get_population(age_group_id=8, location_id=16ADDRESS8, year_id=2020, sex_id=1, release_id = release_id)
draws_df[sex_id==1 & age_group_id==8 & location_id==ADDRESS & year_id == 2020, paste0("draw_", 0:999) := lapply(0:999, function(x)
  1 / case_pop$population)]


# Central African Republic - July 2022 - 45 yr old female
case_pop <- get_population(age_group_id=14, location_id=ADDRESS, year_id=2022, sex_id=2, release_id = release_id)
draws_df[sex_id==2 & age_group_id==14 & location_id==ADDRESS & year_id == 2022, paste0("draw_", 0:999) := lapply(0:999, function(x)
  1 / case_pop$population)]

# Central African Republic - October 2023 - 47 yr old female
case_pop <- get_population(age_group_id=14, location_id=ADDRESS, year_id=2023, sex_id=2, release_id = release_id)
draws_df[sex_id==2 & age_group_id==14 & location_id==ADDRESS & year_id == 2023, paste0("draw_", 0:999) := lapply(0:999, function(x)
  1 / case_pop$population)]

# Cameroon - May 2023 - 7 yr old girl
case_pop <- get_population(age_group_id=6, location_id=ADDRESS, year_id=2023, sex_id=2, release_id = release_id)
draws_df[sex_id==2 & age_group_id==6 & location_id==ADDRESS & year_id == 2023, paste0("draw_", 0:999) := lapply(0:999, function(x)
  1 / case_pop$population)]


### Zero Nigeria + Subnats for 2010 onward
nigeria_locs <- subset(loc_meta, grepl('Nigeria', location_name)) %>% select(location_id,location_name)
nigeria_subs <- subset(loc_meta, parent_id == 214) %>% select(location_id,location_name)
nigeria_locs <- rbind(nigeria_subs,nigeria_locs)$location_id

draws_df[location_id %in% nigeria_locs & year_id %in% 2010:2024, paste0("draw_", 0:999) := 0]

#write.csv(draws_df, paste0(interms_dir, '/FILEPATH'), row.names = FALSE)
write.csv(draws_df, paste0(interms_dir, '/FILEPATH'), row.names = FALSE)

