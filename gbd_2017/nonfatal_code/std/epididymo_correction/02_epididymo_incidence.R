#####################################INTRO#############################################
#' Author: 
#' 5/4/18
#' Purpose: Apply the severity split for epididymo-orchitis (EO) for males and upload 
#'          after wiping previous data 
#'          1) Get draws from parent model. Save the incidence without touching it
#'          2) Apply the proportion split of those who develop EO
#'          3) Calculate mean, upper, and lower and save csv of epididymo incidence draws wide
#'
#' OUTPUTS: EO incidences as csv in form of: FILEPATH/{me_id}_eo_{locationid}.csv
#'
#####################################INTRO#############################################


library("ihme", lib.loc = "FILEPATH")
ihme::setup()

library(dplyr)
library(readr)
library(magrittr)
library(data.table)

source_functions(get_draws = T, get_location_metadata = T)


# Read in commandline args and get draws -------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
location_id <- args[1]
me_id       <- args[2]
main_dir    <- args[3]
# location_id <- 102
# me_id <- 1635

message(paste(Sys.time(), "Beginning epididymo-orchitis correction for cause id", me_id,
               "in location_id", location_id))

# get incidence draws for the parent model

gbd_type_id <- "modelable_entity_id"
incidence   <- 6                   # measure id for incidence
males       <- 1                   # only males are affected by EO
age_groups  <- c(2:20, 30:32, 235) # 5 year age groups
status      <- "best"              # grab the best model


message(paste(Sys.time(), "Getting incidence draws from parent model in location", location_id))

parent_inc_wide <- get_draws(gbd_id_type = gbd_type_id, gbd_id = me_id, source = "epi", 
                    sex_id = males, location_id = location_id, measure_id = incidence, 
                    age_group_id = age_groups, status = status)


message(paste(Sys.time(), "Saving parent incidence for location", location_id))

# save parent incidence in the form of: {me_id}_{location_id}.csv
readr::write_csv(parent_inc_wide, paste0(main_dir, "parent_inc_draws/", me_id, "_",location_id, ".csv"))


# melt long
parent_inc <- melt(parent_inc_wide, 
               id.vars = c("location_id","year_id", "age_group_id", "sex_id", "measure_id"), 
               measure.vars = paste0("draw_", 0:999),
               variable.name = "draw_num", value.name = "draw")


# Apply EO severity split -------------------------------------------------

message(paste(Sys.time(), "Applying epididymo-orchitis severity split to incidence in location", 
              location_id))


# get location data for the location, specifically we care about the region_id
locs <- get_location_metadata(35) %>% 
  filter(location_id == .GlobalEnv$location_id)

# grab map of region_id -> developed/developing status and filter on location's region
dev_by_region <- readr::read_csv("FILEPATH/developed_developing_by_region.csv") %>% 
  filter(location_id == locs$region_id)

# grab just the draws for this ME and development status
sev_draws <- readr::read_csv(paste0(main_dir, "EX/severity_draws.csv")) %>% 
  filter(me_id == .GlobalEnv$me_id &   
         location_type == dev_by_region$location_type)

# merge on the severity split draws and apply the split
eo_incidence <- left_join(parent_inc, sev_draws, by = "draw_num") %>% 
  mutate(draw = draw.x * draw.y) %>% 
  select(location_id, year_id, age_group_id, sex_id, measure_id, draw_num, draw)


# Cast EO incidence draws wide and save -----------------------------------

message(paste(Sys.time(), "Saving EO incidence in location", location_id))

eo_incidence_wide <- eo_incidence %>% as.data.table() %>% 
  dcast(location_id + year_id + age_group_id + sex_id + measure_id ~ draw_num, 
        value.var = c("draw"), fun.aggregate = sum)

draw_cols <- paste0("draw_", 0:999)

# Create the mean, upper and lower using quantiles and then drop draws
eo_incidence_wide[, mean  := apply(.SD, 1, mean), .SDcols = draw_cols]
eo_incidence_wide[, upper := apply(.SD, 1, quantile, c(0.975)), .SDcols = draw_cols]
eo_incidence_wide[, lower := apply(.SD, 1, quantile, c(0.025)), .SDcols = draw_cols]
eo_incidence_wide[, (draw_cols) := NULL]

readr::write_csv(eo_incidence_wide, paste0(main_dir, "EX/", me_id,
                                           "_eo_", location_id, ".csv"))


