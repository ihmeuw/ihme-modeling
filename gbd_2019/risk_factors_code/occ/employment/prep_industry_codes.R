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

source("FUNCTION")
source("FUNCTION")
source("FUNCTION")
source("FUNCTION")
source("FUNCTION")

# set working directories
home.dir <- file.path("FILEPATH")
setwd(home.dir)

#set values for project
collapse_version <- 1
by_sex <- 1
by_age <- 0
relevant.ages <- c(8:18) # only ages 15-69
emp.version <- 19 
decomp_step <- "step4"
bundles_toggle <- T
version_toggle <- F
xwalk_description <- "Step 4 upload, new microdata added"

##in##
in.dir <- "FILEPATH"
out.dir <- "FILEPATH"
if(!dir.exists(out.dir)) dir.create(out.dir, recursive = TRUE)
out.bundle <- "FILEPATH"
microdata.dir <- "FILEPATH"
  tabs <- rbindlist(lapply(file.path(microdata.dir, paste0(c('ind_regular_','ind_census_'),collapse_version,'.csv')),fread),fill = T)
cw.dir <- "FILEPATH"
doc.dir <- "FILEPATH"
  isic.map <- "FILEPATH"
    isic.3.map <- read.xlsx(isic.map, sheet = 2) %>% as.data.table
emp.dir <- "FILEPATH"
bundles <- fread("FILEPATH")[grepl("occ_ind_",me_name)]

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
file.path(ubcov.function.dir, "FUNCTION") %>% source
# central functions
source("FUNCTION")
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
dt <- file.path(in.dir, "ind_ilo_bysex_new.csv") %>% fread
dt[, data := exp_unadj]
dt <- dt[!is.na(data) & !grepl("X|0",isic_code) & outlier==0 & data != 1] #remove missing points and points marked as outliers

## rescale industry codes to sum to one after dropping missingness (only if remaining codes are exhaustive)
x <- dt[,paste(unique(isic_code),collapse = ", "),by=list(ihme_loc_id,year_id,sex_id,isic_version,source)] # list out all isic codes in a survey
x[isic_version == "ISIC2" & grepl(1,V1) & grepl(2,V1) & grepl(3,V1) & grepl(4,V1) &
    grepl(5,V1) & grepl(6,V1) & grepl(7,V1) & grepl(8,V1) & grepl(9,V1), cv_rescale := 1] # mark those that are exhaustive
x[isic_version == "ISIC3" & grepl("A",V1) & grepl("B",V1) & grepl("C",V1) & grepl("D",V1) & grepl("E",V1) &
    grepl("F",V1) & grepl("G",V1) & grepl("H",V1) & grepl("I",V1) & grepl("J",V1) & grepl("K",V1) &
    grepl("L",V1) & grepl("M",V1) & grepl("N",V1) & grepl("O",V1) & grepl("P",V1) & grepl("Q",V1), cv_rescale := 1]
x[isic_version == "ISIC4" & grepl("A",V1) & grepl("B",V1) & grepl("C",V1) & grepl("D",V1) & grepl("E",V1) &
    grepl("F",V1) & grepl("G",V1) & grepl("H",V1) & grepl("I",V1) & grepl("J",V1) & grepl("K",V1) &
    grepl("L",V1) & grepl("M",V1) & grepl("N",V1) & grepl("O",V1) & grepl("P",V1) & grepl("Q",V1) &
    grepl("R",V1) & grepl("S",V1) & grepl("T",V1) & grepl("U",V1), cv_rescale := 1]
dt <- merge(dt,x[,-c("V1"),with = F],by=c("ihme_loc_id","year_id","sex_id","isic_version","source"),all.x=T)
dt[cv_rescale == 1, scale_factor := sum(data), by=list(ihme_loc_id,year_id,sex_id,isic_version,source)]
dt[cv_rescale == 1, data := data/scale_factor]

# merge on levels
source("FUNCTION")
locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 22)
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
tabs[industrial_code_type == "", industrial_code_type := "ISIC3"] #must assume ref if version is missing
tabs[industrial_code_type == "ISIC rev 3", industrial_code_type := "ISIC3"]
tabs[industrial_code_type == "ISIC rev 3.1", industrial_code_type := "ISIC3"]
tabs[industrial_code_type == "ISIC rev 4", industrial_code_type := "ISIC4"]
setnames(tabs, "industrial_code_type", "isic_version")
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
splits <- splits[abs(data_diff) < .15] # remove any points with very sharp swings, investigate further
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
dt <- dt[, -c('lowess', 'residual', 'cv', 'sd', 'reg_cv', 'sr_cv', 'exp_unadj', 'split_data', 'sourcelabel', 'natlrep')]

#final gpr prep/cleanup
dt[, age_group_id := 22]

## Outliering
dt[,is_outlier := 0]
do_outliering()

#output all
write.csv(dt, file="FILEPATH")

#merge on the model me names (short ISIC categories) for saving
dt <- merge(dt, isic.3.map[, list(major, major_label_me)] %>% unique,
            by.x="isic_code", by.y="major", all.x=T)


# Now loop over the different levels, merge the square, fill in study level covs, and save a csv
saveData <- function(cat, dt) {

  message("saving -> ", cat)

  cat.dt <- dt[isic_code==cat]

  this.entity <- paste("occ_ind_major", cat, cat.dt[1, major_label_me], sep = "_")

  cat.dt[, me_name := this.entity]

  ## remove "cv_" from colnames to appease bundle restrictions
  setnames(cat.dt,names(cat.dt)[grepl("cv_",names(cat.dt))],gsub("cv_","",names(cat.dt)[grepl("cv_",names(cat.dt))]))

  ## write outputs, unless facing decomp restrictions in which case susbet to just the
  ## nids used in decomp step 1
  if (decomp_step %in% c("step4","iterative")) {
    write.csv(cat.dt, file=file.path(out.dir, paste0(this.entity, ".csv")), row.names = F)
  } else {
    bid <- bundles[me_name == this.entity,bundle_id]
    bvid <- get_bvid_documentation(bid)[,min(bundle_version_id)]
    if (this.entity == "occ_ind_major_C_mining") bvid <- 4718 ## bundles whose first uploads were unsuccessful
    if (this.entity == "occ_ind_major_E_elec") bvid <- 4724
    if (this.entity == "occ_ind_major_F_construction") bvid <- 4727
    if (this.entity == "occ_ind_major_I_trans") bvid <- 4742
    if (this.entity == "occ_ind_major_K_real") bvid <- 4754
    if (this.entity == "occ_ind_major_O_other_comm") bvid <- 4772
    if (this.entity == "occ_ind_major_P_private") bvid <- 5249
    if (this.entity == "occ_ind_major_Q_et") bvid <- 4781
    step1 <- get_bundle_version(bvid)

    cat.dt <- cat.dt[nid %in% step1$nid]
    if (decomp_step == "step2") cat.dt[,step2_location_year := "reextracted"]

    write.csv(cat.dt, file=file.path(out.dir, paste0(this.entity, ".csv")), row.names = F)
  }

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
    write.csv(logs, "FILEPATH", row.names=F)
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

