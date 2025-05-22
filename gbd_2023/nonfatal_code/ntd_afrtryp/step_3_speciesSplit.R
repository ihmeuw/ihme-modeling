###############################################################################
# Description: Create a file indicating the proportion of HAT cases accounted #
# for by each species in all endemic locations for years 1980-2023            #
###############################################################################

### ========================= BOILERPLATE ========================= ###

rm(list=ls())
data_root <- "FILEPATH"
cause <- "ntd_afrtryp"

## Define paths 
params_dir  <- "FILEPATH"
draws_dir   <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir    <- "FILEPATH"
release_id <- ADDRESS

##	Source relevant libraries
library(tidyr)
library(dplyr)
library(data.table)

## Define Constants
end_year <- 2023

### ========================= MAIN EXECUTION ========================= ###

## (1) Process geographic restrictions - species R ##
gr_specie_r_long <- fread(paste0("FILEPATH"))
# Convert status into binary var
gr_specie_r_long[, `:=` (status_r = value_endemicity)]


## (2) Process geographic restrictions - species G ##
gr_specie_g_long <- fread(paste0("FILEPATH"))
# Convert status into binary var
gr_specie_g_long[, `:=` (status_g = value_endemicity)]


## (3) Merge species files
specie_split_dt <- merge(gr_specie_r_long, gr_specie_g_long, by=c('loc_id', 'year_id'))


## (4) Prep for merge with case data

# Convert year_id to numeric
specie_split_dt[, year_id := as.numeric(as.character(year_start))]

## (5) Merge species split with case data
Data2Model<- readRDS(file = paste0("FILEPATH"))
Data2Model <- Data2Model[,.(location_id, year_id, reported_tgb, reported_tgr, total_reported)]
dt <- merge(Data2Model, specie_split_dt, by=c('location_id', 'year_id'), all.x = TRUE)


## (6) Define species splitting proportions (pr_g and pr_r)
# create pr_g and pr_r variables  (prop of cases attributable to each species)
dt[,`:=`(pr_r = reported_tgr/total_reported, pr_g = reported_tgb/total_reported)]

#(a) If only 1 species is present, set pr_g and pr_r accordingly
dt[(status_g==1 & status_r==0), `:=` (pr_g=1, pr_r = 0)]
dt[(status_g==0 & status_r==1), `:=` (pr_g=0, pr_r = 1)]

#(b) If pr_g or pr_r is missing or NaN, inherit the values from adjacent year (w/i location)
# Subset and ensure sorted by location_id, year_id
dt <- dt[, .(location_id, year_id, pr_r, pr_g)][order(location_id, year_id)]
# Set NaNs to NA
dt[pr_r=='NaN', pr_r := NA]
dt[pr_g=='NaN', pr_g := NA]

# If pr_g or pr_r is NA, inherit value from previous year
# If no previous year, inherit value from subsequent year
dt <- dt %>% 
  group_by(location_id) %>% 
  fill(pr_g, pr_r) %>% #default direction down
  fill(pr_g, pr_r, .direction = "up")
dt <- as.data.table(dt)


## (7) Location-specific data edits

#(a) For Ethiopia, Liberia, and Niger, set pr_g and pr_r to 0 (no cases reported)
dt[(is.na(pr_g) & location_id %in% c(179, 210, 213)), pr_g := 0]
dt[(is.na(pr_r) & location_id %in% c(179, 210, 213)), pr_r := 0]

# (b) Set grs to 0 if the location has no reported data since before 1990
dt[location_id %in% c(175, 179, 185, 193, 206, 209, 210, 213, 216, 522, 35620), pr_g := 0]
dt[location_id %in% c(175, 179, 185, 193, 206, 209, 210, 213, 216, 522, 35620), pr_r := 0]
#Burundi, Ethiopia, Rwanda, Botswana, Gambia, Guinea-Bissau, Liberia, Niger, Sengal, Sudan, Busia

# (c) Update Busia (Kenya subnational)
dt[location_id == 35620, pr_r := 1]


## (8) Ensure that all locations have a row for year 1980 and onward
# If we update the code to drop locations in step 1, this likely wont be necessary
location_id <- unique(dt$location_id)
year_id <- 1980:end_year
full <- tidyr::crossing(location_id, year_id)
dt <- merge(full, dt, by=c('location_id', 'year_id'), all = TRUE)
dt <- as.data.table(dt)

# Ensure sorted by location_id, year_id
dt <- dt[order(location_id, year_id)]
# If pr_g or pr_r is NA, inherit value from previous year
# If no previous year, inherit value from subsequent year
dt <- dt %>% 
  group_by(location_id) %>% 
  fill(pr_g, pr_r) %>% #default direction down
  fill(pr_g, pr_r, .direction = "up")
dt <- as.data.table(dt)


## (9) Clean up and save intermediate file
# Drop any pre-1980 data 
speciesSplit <- dt[year_id >= 1980, .(location_id, year_id, pr_r, pr_g)]
# Save species split file
saveRDS(speciesSplit, file = paste0("FILEPATH"))
