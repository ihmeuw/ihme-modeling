# ---HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Prep industry major groups for modelling
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# load packages
pacman::p_load(data.table, ggplot2, lme4, magrittr, parallel, stringr, openxlsx)

source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/save_crosswalk_version.R")
source("/FILEPATH/get_bundle_version.R")

# set working directories
home.dir <- file.path("FILEPATH")
setwd(home.dir)

#set values for project
collapse_version <- 1
by_sex <- 1
by_age <- 0
relevant.ages <- c(8:18) # only ages 15-69
emp.version <- 21 
current_round <- "gbd_2020" ## update this as needed
gbd_round_id <- 7 ## update this as needed
decomp_step <- "step2" ## update this as needed
bundles_toggle <- T
version_toggle <- F
xwalk_description <- "Step 2 upload, Gbd 2020"

##in##
in.dir <- file.path("/FILEPATH", current_round)
out.dir <- file.path("/FILEPATH", current_round, decomp_step)
if(!dir.exists(out.dir)) dir.create(out.dir, recursive = TRUE)
out.bundle <- file.path("/FILEPATH", current_round, "uploader")
microdata.dir <- "FILEPATH"
tabs <- rbindlist(lapply(file.path(microdata.dir, paste0(c('ind_regular_','ind_census_'),collapse_version,'.csv')),fread),fill = T)
cw.dir <- file.path(home.dir, "FILEPATH")
doc.dir <- file.path(home.dir, "FILEPATH")
isic.map <- file.path(doc.dir, "ISIC_MAJOR_GROUPS_BY_REV.xlsx")
isic.3.map <- read.xlsx(isic.map, sheet = 2) %>% as.data.table
emp.dir <- file.path("/FILEPATH", current_round, decomp_step, emp.version)
bundles <- fread("/FILEPATH/occ_me_ids.csv")[grepl("occ_ind_",me_name)]

#***********************************************************************************************************************

do_outliering <- function() {
  ## mining
  dt[isic_code == "C" & nid %in% c(282234,310640,73721,197035,197036,46580,310625,310675,310675,196464,
                                   112254,112121,197037,164436,197038,164439,164437,42180,42304,310631,
                                   280043,280812,280819,280050,280826,280051,280061,280069,280854,12146,
                                   280869,280075,280875,280881,280891,227194,12105),is_outlier := 1]
  dt[isic_code == "C" & nid == 294537 & location_id %in% c(4850,43881,43917),is_outlier := 1]

  ## manufacturing
  dt[isic_code == "D" & nid %in% c(40155,40179,105306,106684,46682,8442,58419,196464,112254,
                                   112121,197037,164436,197038,164439,164437,282234),is_outlier := 1]

  ## electrical
  dt[isic_code == "E" & nid %in% c(40186),is_outlier := 1]
  dt[isic_code == "E" & nid == 144369 & location_id == 173 & year_id == 1993,is_outlier := 1]
  dt[isic_code == "E" & nid == 144369 & location_id == 57 & year_id == 2010,is_outlier := 1]
  dt[isic_code == "E" & nid == 144369 & location_id == 145 & year_id == 2005,is_outlier := 1]

  ## construction
  dt[isic_code == "F" & nid %in% c(767,742,56952,56968,57014,57030,57051,57138,57210,196464,112254,
                                   112121,197037,164436,197038,164439,164437),is_outlier := 1]

  ## retail
  dt[isic_code == "G" & nid %in% c(131297,131333),is_outlier := 1]

  ## hospitality
  dt[isic_code == "H" & nid == 144369 & location_id == 135 & year_id == 2008,is_outlier := 1]
  dt[isic_code == "H" & nid == 144369 & location_id == 123 & year_id == 2014,is_outlier := 1]

  ## transportation
  dt[isic_code == "I" & nid %in% c(131297,131333,294171,41080,134371,40186,767,742,56952,
                                   56968,57014,57030,57051,57138,57210),is_outlier := 1]

  ## real estate
  dt[isic_code == "K" & nid %in% c(131297,131333,280043,280812,280819,280050,280826,280051,
                                   280061,280069,280854,12146,12105,280869,280075,280875,
                                   280881,280891,227194,767,742,56952,56968,57014,57030,
                                   57051,57138,57210,164434,112113,164435,112255,12652),is_outlier := 1]
  dt[isic_code == "K" & nid == 227167 & sex_id == 1,is_outlier := 1]
  dt[isic_code == "K" & nid == 144369 & location_id == 89 & year_id == 1992,is_outlier := 1]
  dt[isic_code == "K" & nid == 144369 & location_id == 168 & year_id == 1992,is_outlier := 1]

  ## public service
  dt[isic_code == "L" & nid %in% c(39396),is_outlier := 1]
  dt[isic_code == "L" & nid == 144369 & location_id == 150 & year_id %in% c(1993,1996,2000),is_outlier := 1]
  dt[isic_code == "L" & nid == 144369 & location_id == 349 & year_id == 2011,is_outlier := 1]
  dt[isic_code == "L" & nid == 144369 & location_id == 153 & year_id %in% seq(2008,2010),is_outlier := 1]

  ## education
  dt[isic_code == "M" & nid %in% c(236205),is_outlier := 1]
  dt[isic_code == "M" & nid == 144369 & location_id == 150 & year_id == 2010,is_outlier := 1]

  ## health
  dt[isic_code == "N" & nid == 144369 & location_id == 186 & year_id %in% c(2011,2015),is_outlier := 1]

  ## other community activities
  dt[isic_code == "O" & nid %in% c(280043,280812,280819,280050,280826,280051,280061,
                                   280069,280854,12146,280869,280075,280875,280881,280891,
                                   227194,767,742,56952,56968,57014,57030,57051,57138,57210),is_outlier := 1]
  dt[isic_code == "O" & nid == 144369 & location_id == 168 & year_id == 1992,is_outlier := 1]

  ## private household activities
  dt[isic_code == "P" & nid %in% c(12146),is_outlier := 1]
  dt[isic_code == "P" & nid == 144369 & location_id == 27 & year_id %in% c(2012,2014),is_outlier := 1]

  ## extra-territorial organizations
  dt[isic_code == "Q" & nid %in% c(73721,306360,306359,306358),is_outlier := 1]
  dt[isic_code == "Q" & nid == 144369 & location_id == 107 & year_id == 2015,is_outlier := 1]
  dt[isic_code == "Q" & nid == 144369 & location_id == 150 & year_id == 2010,is_outlier := 1]
  dt[isic_code == "Q" & nid == 144369 & location_id == 305 & year_id %in% c(2009,2010,2012,2013),is_outlier := 1]
  dt[isic_code == "Q" & nid == 144369 & location_id == 24 & year_id %in% c(2009,2010),is_outlier := 1]
  dt[isic_code == "Q" & nid == 144369 & location_id == 140 & year_id == 2010,is_outlier := 1]
}

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "db_tools.r") %>% source
# central functions
source("/FUNCTION/get_population.R")
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#files#
#specify the function to define RDS to hold emp.occ/industry draws
fileName <- function(dir,
                     loc,
                     value,
                     suffix) {

  file.path(dir, paste0(loc, '_', value, suffix)) %>% return

}
#***********************************************************************************************************************

#***********************************************************************************************************************
# ---PREP INDUSTRIES------------------------------------------------------------------------------------------------
#read in and prep data
dt <- file.path(in.dir, "ind_ilo_bysex_04_15_20.csv") %>% fread
## clean it up
# fix column names
setnames(dt, c("ref_area","ref_area.label","time","obs_value"),
         c("ihme_loc_id","ilo_location","year_id","data"))
dt[, data := data/100] # change to per-one space
dt[, data_unadj := data] # keep a copy of original data
dt[classif1 %like% "ISIC2", isic_version := "ISIC2"]
dt[classif1 %like% "ISIC3", isic_version := "ISIC3"]
dt[classif1 %like% "ISIC4", isic_version := "ISIC4"]
# add GBD location names and drop any locations not in the GBD hierarchy
source("/FILEPATH/get_location_metadata.R")
locs <- get_location_metadata(gbd_round_id = gbd_round_id, location_set_id = 22)
dt <- merge(locs[, .(ihme_loc_id, location_id, location_name)], dt, by = "ihme_loc_id")
dt[, ilo_location := NULL] # delete ILO location name column
# drop non-ISIC data
dt <- dt[!is.na(isic_version)]
# for loc-years that have multiple ISIC versions, keep the ISIC3 data
dt[, keep := 1]
dt[year_id %in% 2004:2008 & location_name == "Republic of Korea" & isic_version == "ISIC4", keep := 0] # drop ISIC4 data
dt[year_id == 2008 & location_name == "Australia" & isic_version == "ISIC4", keep := 0] # drop ISIC4 data
dt <- dt[keep == 1][, keep := NULL]
# add isic code column
dt[, isic_code := substr(classif1, 11, 11)]
dt[classif1 %like% "TOTAL", isic_code := "TOTAL"]
# fix sex column
dt[sex == "SEX_M", `:=` (sex = "Male", sex_id = 1)]
dt[sex == "SEX_F", `:=` (sex = "Female", sex_id = 2)]
dt[sex == "SEX_T", `:=` (sex = "Both", sex_id = 3)]
# delete extraneous columns
dt <- dt[, .(ihme_loc_id, location_name, location_id, sex, sex_id, year_id, source, indicator.label, classif1.label, data, isic_version, isic_code)]
# drop missing data (isic_code X or 0) and total (aggregate) data
dt <- dt[isic_code %unlike% "X|0|TOTAL"]

# for location-year-isic_codes that have both-sex, male, and female data, delete both-sex (since including that would be double-counting)
# also track loc-year-isic_codes that have both-sex and one of male or female (will merge on population and then calculate whichever is missing)
dt[, index := .GRP, by = .(location_id, year_id, isic_code, isic_version)]
all_sex_ids <- c()
two_sex_ids <- c()
for (n in unique(dt$index)) {
  temp <- dt[index == n]
  if (nrow(temp) == 3) {
    all_sex_ids <- append(all_sex_ids, temp[, unique(index)])
  } else if (nrow(temp) == 2 & any(temp$sex_id == 3)) {
    two_sex_ids <- append(two_sex_ids, temp[, unique(index)])
  }
}

dt <- dt[!(index %in% all_sex_ids & sex_id == 3)] # delete both-sex data where we already have sex-specific data

# calculate the data for the missing sex for loc-year-isic_codes with both-sex and one single-sex data
pops <- get_population(age_group_id = 8:18, location_id = "all", location_set_id = 22, year_id = min(dt$year_id):max(dt$year_id), sex_id = c(1,2,3),
                       gbd_round_id = 7, decomp_step = decomp_step)
pops[, pop_agg := sum(population), by = c("location_id", "year_id", "sex_id")]
pops <- unique(pops[, .(location_id, year_id, sex_id, pop_agg)])
dt <- merge(dt, pops, by = c("location_id", "year_id", "sex_id")) # merge on population

to_fix <- dt[index %in% two_sex_ids]
sex_ids <- c(1,2,3)
sex_split <- function(n) {
  ids <- to_fix[index == n, sex_id]
  missing <- setdiff(sex_ids, ids) # identify which sex is missing
  
  # make a copy of original data and fill in with the missing sex
  new.dt <- to_fix[index == n][1] 
  new.dt[, `:=` (sex_id = missing, data = NA)]
  new.dt[sex_id == 1, sex := "Male"]
  new.dt[sex_id == 2, sex := "Female"]
  # merge on population again to get population for missing sex
  new.dt[, pop_agg := NULL]
  new.dt <- merge(new.dt, pops, by = c("location_id", "year_id", "sex_id"))
  # rbind the original rows and the new row with the missing sex
  new.dt <- rbind(to_fix[index == n], new.dt)
  
  new.dt[, total := data*pop_agg] # calculate total number employed for both-sex and the one single-sex data point we have
  missing_total <- new.dt[sex_id == 3, total] - new.dt[sex_id %ni% c(missing,3), total] # calculate total number employed for the missing single-sex data point
  new.dt[sex_id == missing, total :=  missing_total]
  new.dt[sex_id == missing, data := total/pop_agg] # calculate proportion for missing sex
  
  new.dt <- new.dt[sex_id != 3] # delete the both-sex data
  return(new.dt)
}

# now combine all the sex split data, delete the old data, and replace with the split data
sex_split_dt <- rbindlist(lapply(unique(to_fix$index), sex_split), use.names = TRUE)
sex_split_dt[, total := NULL]
dt <- dt[index %ni% two_sex_ids]
dt <- rbind(dt, sex_split_dt)

## for surveys that are exhaustive (i.e. have data for every ISIC code), rescale industry codes to sum to one after dropping missingness
## corrects for dropping missingness from complete surveys, but doesn't do so for incomplete ones (i.e. doesn't rescale surveys with some ISIC codes missing)
x <- dt[, paste(unique(sort(isic_code)), collapse = ","), by=list(ihme_loc_id,year_id,sex_id,isic_version,source)] # list out all isic codes in a given source
setnames(x, "V1", "unique_codes")
isic2_codes <- dt[isic_version == "ISIC2", paste(unique(sort(isic_code)), collapse = ",")] # every ISIC2 code
isic3_codes <- dt[isic_version == "ISIC3", paste(unique(sort(isic_code)), collapse = ",")] # every ISIC3 code
isic4_codes <- dt[isic_version == "ISIC4", paste(unique(sort(isic_code)), collapse = ",")] # every ISIC4 code
# mark surveys that are exhaustive
x[isic_version == "ISIC2" & unique_codes == isic2_codes, cv_rescale := 1]
x[isic_version == "ISIC3" & unique_codes == isic3_codes, cv_rescale := 1]
x[isic_version == "ISIC4" & unique_codes == isic4_codes, cv_rescale := 1]
# rescale
dt <- merge(dt, x[,-c("unique_codes"), with = F], by=c("ihme_loc_id","year_id","sex_id","isic_version","source"), all.x=T)
dt[cv_rescale == 1, scale_factor := sum(data), by=list(ihme_loc_id,year_id,sex_id,isic_version,source)]
dt[cv_rescale == 1, data := data/scale_factor]

# merge on levels
locations <- unique(locs[is_estimate==1, location_id]) %>% sort
levels <- locs[,grep("region_id|location_id|ihme_loc_id", names(locs)), with=F]
dt <- merge(dt, levels, by=c("location_id", 'ihme_loc_id'))

tabs <- tabs[!(ihme_loc_id %in% c('QCH', 'TAP', 'KSV', 'QAR', 'QES'))] #remove random iso3s from PISA
tabs[ihme_loc_id == "MAC", ihme_loc_id := "CHN_361"]
tabs[, year_id := floor((year_start + year_end)/2)]

#fix the major groups
tabs[, isic_code := substr(var, start=7, stop=7)]

## compensate for collapse code issues
tabs <- tabs[mean <= 1 & mean >= 0]
tabs <- tabs[!duplicated(tabs,by=c('isic_code', 'ihme_loc_id', 'year_id', 'sex_id', 'survey_name'))]

#calculate variance
tabs[(is.na(standard_error) | standard_error < .000001) & !is.na(standard_deviation) & !is.na(sample_size), standard_error := standard_deviation / sqrt(sample_size)]
tabs[, variance := standard_error^2]
tabs[, data := mean]

#Gen variables for crosswalking between different versions.
tabs[is.na(industrial_code_type) & industry_code_type %in% c("ISIC-3", "ISIC-3.1", "ISIC", "IPUMS"), industrial_code_type := "ISIC3"] # assume ISIC version is reference (ISIC3) if missing
tabs[is.na(industrial_code_type) & industry_code_type == "ISIC-2", industrial_code_type := "ISIC2"]
tabs[is.na(industrial_code_type) & industry_code_type == "ISIC-4", industrial_code_type := "ISIC4"]
tabs[industrial_code_type %in% c("ISIC rev 3", "ISIC rev 3.1"), industrial_code_type := "ISIC3"]
tabs[industrial_code_type == "ISIC rev 2", industrial_code_type := "ISIC2"]
tabs[industrial_code_type == "ISIC rev 4", industrial_code_type := "ISIC4"]
setnames(tabs, "industrial_code_type", "isic_version")
tabs[, cv_subgeo := 0] #all of these should be nationally rep

# fix mis-coded data points
tabs[isic_code %in% c("R","S","T","U") & isic_version == "ISIC3", isic_version := "ISIC4"] # these are ISIC4 codes; ISIC3 only goes from A-Q

#cleanup
setnames(tabs, 'survey_name', 'source')
tabs <- tabs[, list(file_path, nid, source, ihme_loc_id, year_id, sex_id, sample_size, isic_version,
                    isic_code, data, variance, cv_subgeo)]
# Merge dt to the location hierarchy
tabs <- merge(tabs, levels, by=c('ihme_loc_id'))

#merge
dt <- list(dt, tabs) %>% rbindlist(use.names=T, fill=T)
dt[, age_group_id := 201] #GBD notation for the 15-69 (workers) age group

# Save your raw data/variance for reference
dt[, "raw_data" := copy(data)]
dt[, "raw_variance" := copy(variance)]

##crosswalk isic versions using splits defined by the major groups correspondance tables
#we will be crosswalking everything to ISIC3
ref <- dt[isic_version=="ISIC3" | isic_code=="TOTAL"] #totals do not need to be crosswalked

#crosswalk ISIC 4 to ISIC 3
split.new <- dt[isic_version=="ISIC4" & isic_code != "TOTAL"]
split.new[, major_isic_4 := as.character(isic_code)]
#calculate the count of categories to inform uncertainty around splits
split.new[, count := unique(major_isic_4) %>% length, by=list(ihme_loc_id, year_id, sex_id, source)]
isic4.map <- file.path(cw.dir, "ISIC_4_TO_3_LVL_1_CW.csv") %>% fread
split.new <- merge(split.new, isic4.map[, list(major_isic_3, major_isic_4, prop)],
                     by="major_isic_4", allow.cartesian = T)

#now use the proportion to xwalk each ISIC4 category to ISIC3, then collapse
split.new[, split_data := lapply(.SD, function(x, w) sum(x*w), w=prop), .SDcols='data',
            by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id')]
split.new <- unique(split.new, by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id'))
split.new[, isic_code := major_isic_3]
split.new[count < 9, natlrep := 0] #increase uncertainty of the lower count points

#crosswalk ISIC 2 to ISIC 3
split.old <- dt[isic_version=="ISIC2" & isic_code != "TOTAL"]
split.old[, major_isic_2 := as.numeric(isic_code)]
#calculate the count of categories to inform uncertainty around splits
split.old[, count := unique(major_isic_2) %>% length, by=list(ihme_loc_id, year_id, sex_id, source)]
isic2.map <- file.path(cw.dir, "ISIC_2_TO_3_LVL_1_CW.csv") %>% fread
split.old <- merge(split.old, isic2.map[, list(major_isic_2, major_isic_3, prop)],
                     by="major_isic_2", allow.cartesian = T)

#now use the proportion to xwalk each ISIC2 category to ISIC3, then collapse
split.old[, split_data := lapply(.SD, function(x, w) sum(x*w), w=prop), .SDcols='data',
            by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id')]
split.old <- unique(split.old, by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id'))
split.old[, isic_code := major_isic_3]
split.old[count<5, natlrep := 0] #increase uncertainty of the lower count points

dt <- rbindlist(list(ref,
                      split.new[, -c('major_isic_3', 'major_isic_4', 'prop'), with = F],
                      split.old[, -c('major_isic_3', 'major_isic_2', 'prop'), with = F]),
                   use.names=T,
                   fill=T)

dt[!is.na(split_data), data := split_data]
dt[, isic_version := "ISIC3"]

# add column with ISIC3 code descriptions
for (code in unique(dt$isic_code)) {
  dt[isic_code == code, isic_code_label := unique(dt[classif1.label %like% "Rev.3" & isic_code == code, classif1.label])]
}
dt[, classif1.label := NULL]


#read employment ratio from outputs
#saved by country so needs to be appended
appendFiles <- function(country, dir, pfx, sfx) {

  message('reading ', country)

  dt <- fileName(dir, country, pfx, sfx) %>% fread

}

workers <- lapply(locations, appendFiles, dir=emp.dir, pfx='emp', sfx='_mean.csv') %>% rbindlist

#merge pops to your data
dt <- merge(dt, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))

##age/sex splitting
#create country year indicator to display which you need to split
dt[, id := paste(ihme_loc_id, year_id, sep="_")]
#create reference dataset
ref <- dt[sex_id!=3]

#create reference age/sex pattern at the region, super-region, global level
ref[, sex_ratio := (data*workers_mean)/sum(data*workers_mean), by=list(age_group_id, location_id, year_id, isic_code)]
ref[, super_sex_ratio := mean(sex_ratio), by=list(sex_id, isic_code, super_region_id)]
ref[, reg_sex_ratio := mean(sex_ratio), by=list(sex_id, isic_code, region_id)]

super.sex <- ref[, list(super_region_id, sex_id, age_group_id, isic_code, super_sex_ratio)] %>% unique
reg.sex <- ref[, list(region_id, sex_id, age_group_id, isic_code, reg_sex_ratio)] %>% unique

#first find out which countries need splitting
splits.list <- dt[, id %>% unique][dt[, id %>% unique] %ni% ref[, id %>% unique]]

#create splitting datasets
message("need to split just sex for:", paste(splits.list, collapse=","))
sex.splits <- dt[id %in% c(splits.list)]

# split the sex split data
# first drop the totals
setnames(sex.splits, c('data', 'workers_mean', 'sex_id'),
         paste0(c('data', 'workers_mean', 'sex_id'), "_og"))
sr.merge <- sex.splits[, region_id %>% unique][sex.splits[, region_id %>% unique] %ni% reg.sex[, region_id %>% unique]]
if (length(sr.merge) > 0) {
  message("need to merge on SR for region #", paste(sr.merge, collapse=","))
  sex.split.sr <- sex.splits[region_id%in%sr.merge]
  sex.split.sr <- merge(sex.split.sr, super.sex, by=c('super_region_id', 'age_group_id', 'isic_code'), allow.cartesian=TRUE)
  sex.split.sr <- merge(sex.split.sr, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))
  sex.split.sr[, data := data_og*workers_mean_og/workers_mean*super_sex_ratio]
  sex.split.sr[, data_diff := data - data_og]
}

sex.splits <- merge(sex.splits, reg.sex, by=c('region_id', 'age_group_id', 'isic_code'), allow.cartesian=TRUE)
sex.splits <- merge(sex.splits, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))
sex.splits[, data := data_og*workers_mean_og/workers_mean*reg_sex_ratio]
sex.splits[, data_diff := data - data_og]

#combine and clean up
if (length(sr.merge) > 0) {
  splits <- list(sex.splits, sex.split.sr) %>% rbindlist(fill=TRUE)
} else splits <- sex.splits
splits <- splits[abs(data_diff) < .15] # remove any points with very sharp swings
clean.splits <- splits[, -c('data_diff', names(splits)[names(splits) %like% 'ratio' | names(splits) %like% 'og']),
                       with=FALSE]

clean.splits[, natlrep := 0] #give less weight to split points
clean.ref <- ref[, -c(names(ref)[names(ref) %like% 'ratio' | names(ref) %like% 'og']), with=FALSE]

dt <- list(clean.ref, clean.splits) %>% rbindlist(use.names=T)

#use lowess to predict a smoothed version
#note if there are less than 3 points we cannot predict
#will use region/sr cv to predict here
predLow <- function(data) {

  if (nrow(data) > 3) {

  mod <- loess(data~year_id, data=data, span=nrow(data))
  pred <- predict(mod)

  } else pred <- NA

  return(pred)

}

## lowess over time for places with over 3 data points for a given location-sex-age-isic_code from one
## nid (ie ILOSTAT tabulated data)
dt[, lowess := predLow(.SD) %>% as.numeric, .SDcols=c("data", 'year_id'),
   by=list(ihme_loc_id, sex_id, age_group_id, isic_code, nid)]

#calculate residuals, then take the SD for each lowess model and use it to compute the variance
#this means that more unstable datasets will have higher variance
dt[, residual := (lowess-data)]
dt[, sd := sd(residual), by=list(ihme_loc_id, sex_id, age_group_id, isic_code)]

#use cv to impute the variance where we didnt have enough points to use the lowess
ref_data_min <- .0000001
dt[, cv := sd / data]
dt[cv == Inf, cv := sd / ref_data_min]
dt[, reg_cv := mean(cv, na.rm=T), by=list(region_id)]
dt[, sr_cv := mean(cv, na.rm=T), by=list(super_region_id)]
dt[is.na(sd), sd := data * reg_cv]
dt[sd == 0, sd := ref_data_min * reg_cv]
dt[is.na(sd), sd := data * sr_cv]
dt[sd == 0, sd := ref_data_min * sr_cv]

#calculate variance
dt[is.na(variance), variance := sd^2]

#set covariates
dt[natlrep==0, cv_subgeo := 1] #set to proper st-gpr format
dt[natlrep==1, cv_subgeo := 0]

#cleanup
dt <- dt[, -c('lowess', 'residual', 'cv', 'sd', 'reg_cv', 'sr_cv', 'split_data', 'natlrep', 'indicator.label')]
dt[sex_id == 1, sex := "Male"]
dt[sex_id == 2, sex := "Female"]
dt[, location_name := NULL]
dt <- merge(locs[, .(location_id, ihme_loc_id, location_name)], dt, by = c("location_id", "ihme_loc_id"))
dt[is.na(nid), nid := 436802] # ILOSTAT data (new NID for GBD 2020)
setcolorder(dt, c("nid","file_path","source","location_id","ihme_loc_id","location_name","year_id","sex_id","sex","age_group_id","data","variance","sample_size",
                  "isic_version","isic_code","isic_code_label"))

#final gpr prep/cleanup
dt[, age_group_id := 22]
setnames(dt, "data", "val")
dt[, `:=` (measure_id = 18, measure = "proportion")]

## Outliering
dt[,is_outlier := 0]
do_outliering()

#output all
write.csv(dt, file=file.path(out.dir, "occ_ind_major_all.csv"), row.names = F)

#merge on the model me names (short ISIC categories) for saving
dt <- merge(dt, isic.3.map[, list(major, major_label_me)] %>% unique,
            by.x="isic_code", by.y="major", all.x=T)


# Now loop over the different levels, merge the square, fill in study level covs, and save a csv
saveData <- function(cat, dt) {
  
  message("saving -> ", cat)
  
  cat.dt <- dt[isic_code==cat]
  
  this.entity <- paste("occ_ind_major", cat, cat.dt[, unique(major_label_me)], sep = "_")
  
  cat.dt[, me_name := this.entity]
  
  ## remove "cv_" from colnames to appease bundle restrictions
  setnames(cat.dt, names(cat.dt)[names(cat.dt) %like% "cv"], gsub("cv_", "", names(cat.dt)[names(cat.dt) %like% "cv"]))
  
  ## write outputs
  write.csv(cat.dt, file=file.path(out.dir, paste0(this.entity, ".csv")), row.names = F)
  
  return(cat.dt)
  
}

list <- lapply(dt[, isic_code] %>% unique, saveData, dt=dt)
#***********************************************************************************************************************

# Bundle Uploads for Decomp -----------------------------------------------
if (bundles_toggle) {
  for (me in c("occ_ind_major_A_agg", "occ_ind_major_D_manuf")) {
    logs <- fread(file.path(out.bundle,"ind_bid_logs.csv"))

    print(paste0("UPLOADING ",me))
    bid <- bundles[me_name == me,bundle_id]

    ## clear out bundle
    print("CLEARING BUNDLE")
    wipe_db(bid,decomp_step)

    ## upload new data
    print("UPLOADING BUNDLE")
    upload_db(bid,decomp_step,file.path(out.dir,paste0(me,".csv")),"proportion")

    ## save bundle version
    print("SAVING BUNDLE VERSION")
    bversion <- save_bundle_version(bid,decomp_step)
    bversion <- bversion[,.(me_name = me,bid,dstep = decomp_step,bundle_version_id)]
    print(bversion)

    ## create crosswalk version for bundle version (no additional data step required)
    print("GETTING BUNDLE VERSION")
    df <- get_bundle_version(bversion$bundle_version_id)
    df[,crosswalk_parent_seq := seq]
    df[,unit_value_as_published := 0]
    df[,`unit_value_as_published` := as.double(`unit_value_as_published`)]
    path <- paste0(out.bundle,me,"_xwalked_",decomp_step,".xlsx")
    write.xlsx(df,path,sheetName="extraction")
    print("SAVING XWALK VERSION")
    cversion <- save_crosswalk_version(bversion$bundle_version_id,path,description = xwalk_description)
    cversion <- cversion[,.(bid,crosswalk_version_id)]
    print(cversion)

    ## save log of IDs
    log <- merge(bversion,cversion,by="bid")
    log[, c("timestamp","notes"):= .(as.character(Sys.time()),xwalk_description)]
    setcolorder(log,names(logs))
    logs <- rbind(log,logs)
    write.csv(logs, "/FILEPATH/ind_bid_logs.csv", row.names=F)
  }
} else if (version_toggle) { ## Update bundle version with new outliers, so no need for new bundle version
  logs <- fread(file.path(out.bundle,"ind_bid_logs.csv"))
  bvids <- logs[,max(bundle_version_id),by=me_name]$V1

  for (bvid in bvids) {
    logs <- fread(file.path(out.bundle,"ind_bid_logs.csv"))
    log <- unique(logs[bundle_version_id == bvid,.(me_name,dstep,bid,bundle_version_id)])
    print("GETTING BUNDLE VERSION")
    dt <- get_bundle_version(bvid)
    dt[,sex_id := ifelse(sex == "Male",1,2)]
    do_outliering()

    ## prep for xwalk versioning
    dt[,crosswalk_parent_seq := seq]
    dt[,unit_value_as_published := 0]
    dt[,`unit_value_as_published` := as.double(`unit_value_as_published`)]
    path <- paste0(out.bundle,log$me_name,"_xwalked_",decomp_step,".xlsx")
    write.xlsx(dt,path,sheetName="extraction")
    print("SAVING XWALK VERSION")
    cversion <- save_crosswalk_version(bvid,path,description = xwalk_description)
    cversion <- cversion[,.(bid = log$bid,crosswalk_version_id)]

    ## save log of IDs
    log <- merge(log,cversion,by="bid")
    log[, c("timestamp","notes"):= .(as.character(Sys.time()),xwalk_description)]
    setcolorder(log,names(logs))
    logs <- rbind(log,logs)
    write.csv(logs,file.path(out.bundle,"ind_bid_logs.csv"),row.names=F)
  }
}

