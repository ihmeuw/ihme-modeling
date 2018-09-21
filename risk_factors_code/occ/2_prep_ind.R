# ---HEADER-------------------------------------------------------------------------------------------------------------

# Project: OCC - OCC
# Purpose: Prep economic activities for modellling
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# set toggles

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  # necessary to set this option in order to read in a non-english character shapefile on a linux system (cluster)
  Sys.setlocale(category = "LC_ALL", locale = "C")
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

# load packages
pacman::p_load(data.table, ggplot2, lme4, magrittr, parallel, stringr, readxl)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

#set values for project
location_set_version_id <- 149
year_start <- 1970
year_end <- 2016
by_sex <- 1
by_age <- 0
relevant.ages <- c(8:18) #only ages 15-69
emp.version <- 7 #check most recent version of worker.version from 3_squeeze code

##in##
data.dir <- file.path(home.dir, "FILEPATH")
microdata.dir <- file.path(home.dir, "FILEPATH")
tabs <- file.path(microdata.dir, 'collapse_06_26_appended.csv') %>% fread
cw.dir <- file.path(home.dir, "FILEPATH")
doc.dir <- file.path(home.dir, 'FILEPATH')
isic.map <- file.path(doc.dir, 'ISIC_MAJOR_GROUPS_BY_REV.xlsx')
isic.3.map <- read_xlsx(isic.map, sheet = 2) %>% as.data.table
emp.dir <- file.path(home.dir, "FILEPATH", emp.version)

##out##
out.dir <- file.path(home.dir, "FILEPATH")
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source
# central functions
file.path('FILEPATH') %>% source
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
# ---PREP FATAL INJURIES------------------------------------------------------------------------------------------------
#read in and prep data
dt <- file.path(out.dir, "pre_cw_occ_ind.csv") %>% fread
dt[, data := exp_unadj]
dt <- dt[!is.na(data) & outlier==0] #remove missing points and points marked as outliers

# #merge on levels
locs <- get_location_hierarchy(location_set_version_id)
locations <- unique(locs[is_estimate==1, location_id]) %>% sort
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]
dt <- merge(dt, levels, by=c("location_id", 'ihme_loc_id'))

#add in the microdata
tabs <- tabs[!(var %like% 'hrh')] #remove hrh vars
tabs <- tabs[!(var %like% 'occ')] #remove occ vars
tabs <- tabs[!(ihme_loc_id %in% c('QCH', 'TAP', 'KSV', 'QAR', 'QES'))] #remove random iso3s from PISA
tabs[, year_id := floor((year_start + year_end)/2)]

tabs[ihme_loc_id == "MAC", ihme_loc_id := "CHN_361"]
tabs[, year_id := floor((year_start + year_end)/2)]
setnames(tabs, 'cv_industrial_code_type', 'cv_isic_version')

#fix the major groups
tabs[, isic_code := substr(var, start=7, stop=7)]

#calculate variance
tabs[is.na(standard_error), standard_error := standard_deviation / sqrt(sample_size)]
tabs[, variance := standard_error^2]
tabs[, data := mean]

#outliers
tabs <- tabs[data < .75] 
tabs <- tabs[!(ihme_loc_id %like% 'GBR' & year_id < 1991)] 
tabs <- tabs[!(file_path == 'FILEPATH')] 
tabs <- tabs[!(file_path == 'FILEPATH')] 

#Gen variables for crosswalking between different versions.
tabs[cv_isic_version == "", cv_isic_version := "ISIC3"] #must assume ref if version is missing
tabs[cv_isic_version == "ISIC rev 3", cv_isic_version := "ISIC3"]
tabs[cv_isic_version == "ISIC rev 3.1", cv_isic_version := "ISIC3"]
tabs[cv_isic_version == "ISIC rev 4", cv_isic_version := "ISIC4"]
setnames(tabs, "cv_isic_version", "isic_version")
tabs[, cv_subgeo := 0] #all of these should be nationally rep

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
split.new <- split.new[abs(split_data-raw_data) < .25] #remove unstable points
split.new[count<9, natlrep := 0] #increase uncertainty of the lower count points

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
split.old <- split.old[abs(split_data-raw_data) < .25] #remove unstable points
split.old <- split.old[!(count<5 & prop <.75)] #if low counts, only keep the most informed categories
split.old[count<5, natlrep := 0] #increase uncertainty of the lower count points

dt <- rbindlist(list(ref,
                     split.new[, -c('major_isic_3', 'major_isic_4', 'prop'), with = F],
                     split.old[, -c('major_isic_3', 'major_isic_2', 'prop'), with = F]),
                use.names=T,
                fill=T)

#create a graph of the split
ggplot(data=dt[!(isic_version %like% "ISIC3")],
       aes(x=data, y=split_data, color=level_1 %>% as.factor)) +
  geom_text(aes(label=ihme_loc_id)) +
  facet_wrap(~isic_code) +
  theme_bw()

dt[!is.na(split_data), data := split_data]
dt[, isic_version := "ISIC3"]

#read employment ratio from outputs
#saved by country so needs to be appended
appendFiles <- function(country, dir, pfx, sfx) {
  
  message('reading ', country)
  
  dt <- fileName(dir, country, pfx, sfx) %>% fread
  
}

workers <- lapply(locations, appendFiles, dir=emp.dir, pfx='emp', sfx='_mean.csv') %>% rbindlist
workers[, V1 := NULL]

#merge pops to your data
dt <- merge(dt, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))

##age/sex splitting
#create country year indicator to display which you need to split
dt[, id := paste(ihme_loc_id, year_id, sep="_")]
#create reference dataset
ref <- dt[sex_id!=3]

#create reference age/sex pattern at the region, super-region, global level
ref[, sex_ratio := (data*workers_mean)/sum(data*workers_mean), by=list(age_group_id, location_id, year_id, isic_code)]
ref[, super_sex_ratio := mean(sex_ratio), by=list(sex_id, isic_code, level_1)]
ref[, reg_sex_ratio := mean(sex_ratio), by=list(sex_id, isic_code, level_2)]

super.sex <- ref[, list(level_1, sex_id, age_group_id, isic_code, super_sex_ratio)] %>% unique
reg.sex <- ref[, list(level_2, sex_id, age_group_id, isic_code, reg_sex_ratio)] %>% unique

#first find out which countries need splitting
splits.list <- dt[, id %>% unique][dt[, id %>% unique] %ni% ref[, id %>% unique]]

#create splitting datasets
message("need to split just sex for:", paste(splits.list, collapse=","))
sex.splits <- dt[id %in% c(splits.list)]

# split the sex split data
# first drop the totals
setnames(sex.splits, c('data', 'workers_mean', 'sex_id'),
         paste0(c('data', 'workers_mean', 'sex_id'), "_og"))
sr.merge <- sex.splits[, level_2 %>% unique][sex.splits[, level_2 %>% unique] %ni% reg.sex[, level_2 %>% unique]]
if (length(sr.merge) > 0) {
  message("need to merge on SR for region #", paste(sr.merge, collapse=","))
  sex.split.sr <- sex.splits[level_2%in%sr.merge]
  sex.split.sr <- merge(sex.split.sr, super.sex, by=c('level_1', 'age_group_id', 'isic_code'), allow.cartesian=TRUE)
  sex.split.sr <- merge(sex.split.sr, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))
  sex.split.sr[, data := data_og*workers_mean_og/workers_mean*super_sex_ratio]
  sex.split.sr[, data_diff := data - data_og]
}

sex.splits <- merge(sex.splits, reg.sex, by=c('level_2', 'age_group_id', 'isic_code'), allow.cartesian=TRUE)
sex.splits <- merge(sex.splits, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id'))
sex.splits[, data := data_og*workers_mean_og/workers_mean*reg_sex_ratio]
sex.splits[, data_diff := data - data_og]

#combine and clean up
if (length(sr.merge) > 0) {
  splits <- list(sex.splits, sex.split.sr) %>% rbindlist(fill=TRUE)
} else splits <- sex.splits
splits <- splits[abs(data_diff) < .15] # remove any points with very sharp swings, investigate further
clean.splits <- splits[, -c('sex', 'age', 'age_short.x', 'age_short.y', 'process_version_map_id.x', 'process_version_map_id.y',
                            'age_short', 'age_group_name', 'age_group_name_short', 'data_diff',
                            names(splits)[names(splits) %like% 'ratio' | names(splits) %like% 'og']),
                       with=FALSE]

clean.splits[, natlrep := 0] #give less weight to split points
clean.ref <- ref[, -c('sex', 'age','age_short.x', 'age_short.y', 'process_version_map_id',
                      'age_short', 'age_group_name_short', 'data_diff',
                      names(ref)[names(ref) %like% 'ratio' | names(ref) %like% 'og']),
                 with=FALSE]

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

dt[, lowess := predLow(.SD) %>% as.numeric, .SDcols=c("data", 'year_id'),
   by=list(ihme_loc_id, sex_id, age_group_id, isic_code, nid)]

#calculate residuals, then take the SD for each lowess model and use it to compute the variance
#this means that more unstable datasets will have higher variance
dt[, residual := (lowess-data)]
dt[, sd := sd(residual), by=list(ihme_loc_id, sex_id, age_group_id, isic_code)]

#use cv to impute the variance where we didnt have enough points to use the lowess
dt[, cv := sd / data]
dt[, reg_cv := mean(cv, na.rm=T), by=list(level_2)]
dt[, sr_cv := mean(cv, na.rm=T), by=list(level_1)]
dt[is.na(sd), sd := data * reg_cv]
dt[is.na(sd), sd := data * sr_cv]

#calculate variance
dt[is.na(variance), variance := sd^2]

#set covariates
dt[natlrep==0, cv_subgeo := 1] #set to proper st-gpr format
dt[natlrep==1, cv_subgeo := 0]

#cleanup
dt <- dt[, -c('lowess', 'residual', 'cv', 'sd', 'reg_cv', 'sr_cv', 'exp_unadj', 'split_data', 'sourcelabel', 'natlrep')]

#final gpr prep/cleanup
dt[, sample_size := NA]
dt[, age_group_id := 22] #must replace age group 201 with all ages since covariates arent made for age group 201

#outliers

#output all
write.csv(dt, file=file.path(out.dir, "occ_ind_major_all.csv"))

#merge on the model me names (short ISIC categories) for saving
dt <- merge(dt, isic.3.map[, list(major, major_label_me)] %>% unique,
            by.x="isic_code", by.y="major", all.x=T)


# Now loop over the different levels, merge the square, fill in study level covs, and save a csv
saveData <- function(cat, dt) {
  
  message("saving -> ", cat)
  
  cat.dt <- dt[isic_code==cat]
  
  this.entity <- paste("occ_ind_major", cat, cat.dt[1, major_label_me], sep = "_")
  
  cat.dt[, me_name := this.entity]
  
  write.csv(cat.dt, file=file.path(out.dir, paste(this.entity, ".csv", sep = "")))
  
  return(cat.dt)
  
}

list <- lapply(dt[, isic_code] %>% unique, saveData, dt=dt)
#***********************************************************************************************************************

# ---SCRAP--------------------------------------------------------------------------------------------------------------

#***********************************************************************************************************************