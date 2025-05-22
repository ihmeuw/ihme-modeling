# Purpose: convert draw-level all-age/sex LF prevalence estimates from MBG model to age/sex-specific estimates for GBD

### ======================= BOILERPLATE ======================= ###
rm(list = ls())
user <- Sys.info()[["user"]]
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"
options(max.print=999999)

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- "FILEPATH"
  draws_dir <- "FILEPATH"
  interms_dir <- "FILEPATH"
  logs_dir <- "FILEPATH"
}

# 1. Setup libraries, functions, parameters, filepaths, etc. ---------------------------------------------------------------------
# load libraries
library(dplyr)
library(ggplot2)
library(data.table)
library(magrittr)
library(lbd.loader, lib.loc = "FILEPATH")
library(lbd.mbg, lib.loc =lbd.loader::pkg_loc("lbd.mbg"))

# source functions
source("FILEPATH")

# establish run dates
xwalk_run_date <- "FILEPATH" # the run_date directory for xwalk results to use for age pattern
non_mbg_run_date <- "FILEPATH" # the run_date directory for non-MBG models

# output directories
gen_rundir(data_root = data_root, acause = 'ntd_lf', message = 'for 10-30-2024, updated lfmda covariate measure')
code_dir <- "FILEPATH"
shell <- "FILEPATH"
params_dir <- "FILEPATH"
run_file <- "FILEPATH"
run_folder_path <- "FILEPATH"
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
crosswalks_dir <- "FILEPATH"
inf_draws_dir <- "FILEPATH"
if (!dir.exists(file.path(inf_draws_dir))){dir.create(inf_draws_dir)}

# input directories
root_dir <- "FILEPATH"
mbg_dir <- "FILEPATH" # mbg model run directory
cw_dir <- "FILEPATH" # directory of saved crosswalk model objects
non_mbg_dir <- "FILEPATH" # directory of saved non-mbg model results

# set regional run directories for MBG results
reg_runs <- c("FILEPATH",
              "FILEPATH", 
              "FILEPATH",
              "FILEPATH",
              "FILEPATH",
              "FILEPATH")

# specify soas directory for level-5 aggregation data
soas_dir <- "FILEPATH"

# set parameters
draws <- 1000
release_id <- ADDRESS
age_group_set_id <- 24
modeling_shapefile_version <- "current"

# create inverse logit function to convert from logit to prevalence space
logit <- function(x) {
  log(x/(1-x))
}

inv.logit <- function(x) {
  exp(x)/(1+exp(x))
}

# 2. Load GBD location information ---------------------------------------------------------------------
# get location metadata
gbd_loc_hierarchy <- get_location_metadata(release_id = release_id, location_set_id=35) 
lbd_gbd <- fread("FILEPATH")
setnames(lbd_gbd, c("loc_id","loc_name","loc_nm_sh","ihme_lc_id","ADM_CODE"), c("location_id","location_name","location_name_short","ihme_loc_id","ADM_CODE"))

# load LF geographic restrictions
lf_endems <- fread("FILEPATH")[value_endemicity == 1,]
lf_endems <- sort(unique(lf_endems$location_id))
## add India level-5 sub-nationals
### add Rural units to lf_endems and Urban to zero_locs
lvl5_rural <- c(43908,43909,43910,43911,43913,43916,43917,43918,43919,43920,43921,43922,
               43923,43924,43926,43927,43928,43929,43930,43931,43932,43934,43935,43936,
               43937,43938,43939,43940,43941,43942,44539)
lvl5_urban <- c(43872,43873,43874,43875,43877,43880,43881,43882,43883,43884,43885,43886,
               43887,43888,43890,43891,43892,43893,43894,43895,43896,43898,43899,43900,
               43901,43902,43903,43904,43905,43906,44540)
lvl_india <- c(lvl5_rural,lvl5_urban)
lf_endems <- c(lf_endems,lvl_india)
lf_endems <- unique(lf_endems)

# get years and locations for current GBD 
demo <- get_demographics(gbd_team = 'epi', release_id = release_id)
gbd_years <- sort(demo$year_id)
gbd_loc_ids <- sort(demo$location_id)
gbd_ages <- sort(demo$age_group_id)
gbd_sex_id <- sort(demo$sex_id)

# get intersection of GBD locs and LF locs
print(paste0("There are ", length(gbd_loc_ids), " locations required for the current GBD round."))
print(paste0("There are ", length(lf_endems), " locations required for LF."))
lf_locs <- sort(intersect(gbd_loc_ids,lf_endems))
print(paste0("There are ", length(lf_locs), " locations required for both the current GBD round and LF."))

## get lbd ADM_CODE to location_id
shp_lvls <- c(1:2)
for (i in shp_lvls){
  shp <- i - 1
  sf <- sf::st_read("FILEPATH")
  sf_df <- as.data.frame(sf)
  setnames(sf_df,"loc_id","location_id")
  assign(paste0("shp_",shp), sf_df)
}

setnames(shp_0,"ADM0_NAME","ADM0_NAME_shp")
setnames(shp_1,"ADM1_NAME","ADM1_NAME_shp")
shp_0 <- shp_0[,c("ADM0_CODE","ADM0_NAME_shp","location_id")]
shp_1 <- shp_1[,c("ADM1_CODE","ADM1_NAME_shp","location_id")]

# 3. Load non-MBG estimates ---------------------------------------------------------------------
### establish non-mbg geographies
non_mbg_prev_files <- list.files(path = non_mbg_dir)
non_mbg_prev_files[names(non_mbg_prev_files) != "BRA_Pernambuco.csv"] # remove ADM1 files

## compile a data.frame of all the prevalence draws for non-mbg geographies
### returns a data.frame of 1000 draws, the year, and the iso3 code
non_mbg <- data.frame()
for (i in non_mbg_prev_files) {
  non <- read.csv(paste0(non_mbg_dir, i))
  iso3 <- str_remove_all(i, ".csv")
  non$iso3 <- iso3
  ## put iso3 column first, followed by the year and then the draws
  non <- non[, c(1002, 1, 2:1001)]
  non_mbg <- rbindlist(list(non_mbg, non), use.names=TRUE, fill=TRUE)
}

## add location_id to non_mbg data
non_mbg <- merge(non_mbg, gbd_loc_hierarchy, by.x="iso3", by.y="ihme_loc_id", all.x=TRUE, all.y=FALSE)
colnames(non_mbg)[3:1002] <- paste0("draw_", 0:999)

## get admin1 draws for Pernambuco, Brazil
BRA_Pernambuco <- fread("FILEPATH")
colnames(BRA_Pernambuco)[2:1001] <- paste0("draw_", 0:999)

## add location_id to BRA_Pernambuco
BRA_Pernambuco$location_id <- gbd_loc_hierarchy[location_name == "Pernambuco", location_id]

# 4. Load regional MBG estimates ---------------------------------------------------------------------
## load and combine MBG admin-level aggregated draws
levels <- c("ADM0", "ADM1")
adm0_list <- data.table()
adm1_list <- data.table()

for (i in 1:length(reg_runs)) {
  rr <- reg_runs[i]
  
  # get region name abbreviation
  reg <- gsub(user, "", rr)
  reg <- substring(reg, 13)
  
  load("FILEPATH") 
  
  for (a in 1:(length(levels))) {
    ad <- levels[a]
    
    # establish admin-level-specific columns and data
    ad_list <- if (ad == "ADM0") list(c("ADM0_CODE", "ADM0_NAME", "region"),"ADM0_CODE",admin_0,"ADM0_NAME") else
      if (ad == "ADM1") list(c("ADM1_CODE", "ADM1_NAME", "region"),"ADM1_CODE",admin_1,"ADM1_NAME") else
        list(c("ADM2_CODE", "ADM2_NAME", "region"),"ADM2_CODE",admin_2,"ADM2_NAME") 
    
    # subset to just the admin-level in current loop
    sp_hierarchy_list_ADM <- unique(subset(sp_hierarchy_list, select = ad_list[[1]]))
    
    # subset to just the admin-level in current loop
    df_admin <- merge(ad_list[[3]], sp_hierarchy_list_ADM, by = ad_list[[2]])
    
    # add rows to the correct admin list by matching ad with the empty list 
    if (ad == "ADM0") {
      adm0_list <- rbind(adm0_list, df_admin, use.names=TRUE, fill=TRUE)
    }
    if (ad == "ADM1") {
      adm1_list <- rbind(adm1_list, df_admin, use.names=TRUE, fill=TRUE)
    } 
  }
}

# add location_id to MBG data
adm0_list <-merge(adm0_list, shp_0, by.x = "ADM0_CODE", by.y = "ADM0_CODE", all.x = T, all.y = F)
adm1_list <-merge(adm1_list, shp_1, by.x = "ADM1_CODE", by.y = "ADM1_CODE", all.x = T, all.y = F)

# rename draw columns
colnames(adm0_list)[3:1002] <- paste0("draw_", 0:999)
colnames(adm1_list)[3:1002] <- paste0("draw_", 0:999)

# get level-5 locations (currently only need for India in the SOAS region)
lvl5 <- readRDS("FILEPATH") # update this
setnames(lvl5,"std_loc_code","location_id")
colnames(lvl5)[3:1002] <- paste0("draw_", 0:999)

# 5. Combine MBG and non-MBG data --------------------------------------------------------------------
# standardize columns
non_mbg <- non_mbg[, c(1005,2,3:1002)]
BRA_Pernambuco <- BRA_Pernambuco[, c(1002,1,2:1001)]
adm0_list <- adm0_list[, c(1007,2,3:1002)]
adm1_list <- adm1_list[, c(1007,2,3:1002)]
lvl5 <- lvl5[, c(2,1,3:1002)]

# combine data
## combine adm0 data and non-MBG data
adm0_list <- rbindlist(list(adm0_list, non_mbg), use.names=TRUE, fill=TRUE)
## combine adm1 and BRA_Pernambuco data
adm1_list <- rbindlist(list(adm1_list, BRA_Pernambuco), use.names=TRUE, fill=TRUE)
## combine adm0 and subnationals
df_combo <- rbindlist(list(adm0_list, adm1_list), use.names=TRUE, fill=TRUE)
## add level-5 locations to data
df_combo <- rbindlist(list(df_combo, lvl5), use.names=TRUE, fill=TRUE)

## drop rows with no location_id value
df_combo <- df_combo[!is.na(location_id),] 

# drop location_ids not in lf_locs 
df_combo <- df_combo[df_combo$location_id %in% lf_locs,] 

# 6. Adjust for elimination years -----------------------
# adjust prevalence to zeros based on known elimination years
## Egypt (location_id 141): >= 2010
## Cambodia (location_id 10): >= 2010
## Thailand (location_id 18): >= 2010
## Togo (location_id 218): >= 2015
## Sri Lanka (location_id 17): >= 2010

## hard-code elimination values for togo, egypt, cambodia, thailand, sri lanka - achieved before 2017
draw.cols <- paste0("draw_", 0:999)
df_combo<-setDT(df_combo)
df_combo[, id := .I]

### replace prevalence estimates for location_id loc and year_id yr-current with zeros 
df_combo[location_id==141 & year_id>=2010, (draw.cols):=0, by=id] # Egypt
df_combo[location_id==10 & year_id>=2010, (draw.cols):=0, by=id] # Cambodia
df_combo[location_id==18 & year_id>=2010, (draw.cols):=0, by=id] # Thailand
df_combo[location_id==218 & year_id>=2015, (draw.cols):=0, by=id] # Togo
df_combo[location_id==17 & year_id >= 2010, (draw.cols):=0, by=id] # Sri Lanka

# drop id column
df_combo <- subset(df_combo, select=-c(id))

# 7. Generate zero-draws files and remove zero-locs from dataset ---------------------------------------------------------------------
# generate zero-draw files for non-endemic locations, missing locations, and locations that were zero for entire Focal 3 time-series in GBD 2021

## get location_ids for non-endemic gbd locations
non_endemic <- gbd_loc_ids[!(gbd_loc_ids %in% df_combo$location_id)] 
## set location_ids that were zero for entire Focal 3 time-series in GBD 2021
gbd_2021_zeros <- c(5, 6, 28, 68, 118, 119, 124, 126, 175, 183, 185, 
                    186, 203, 206, 212, 491, 493, 494, 496, 497, 498, 499, 
                    502, 503, 504, 506, 507, 513, 514, 516, 521, 4751, 
                    4754, 4763, 25342, 60908, 94364)
batanes <- c(53547)

## combine location_id lists for 
zero_locs <- c(non_endemic,gbd_2021_zeros,batanes)
## ensure no location_ids are duplicated
zero_locs <- unique(zero_locs)

for (i in 1:length(zero_locs)) {
  loc <- zero_locs[[i]]
  loc_zeros <- gen_zero_draws(modelable_entity_id = ADDRESS, location_id = loc, measure_id = c(5), metric_id = c(3), year_id = gbd_years, release_id = release_id, team = 'epi')
  
  write.csv(loc_zeros, "FILEPATH", row.names = FALSE)
  
  print(paste0("Finished writing-out loc id ", loc, ': ', i, " of ", length(zero_locs)))
}

## remove zero-loc locations from dataset 
df_combo <- df_combo[ ! df_combo$location_id %in% zero_locs,]

# 8. Export final pre-splitting estimates --------------------------------------------------------------------
# write-out final pre-splitting estimates (combined non-MBG & MBG)
write.csv(df_combo, "FILEPATH", row.names = FALSE)



