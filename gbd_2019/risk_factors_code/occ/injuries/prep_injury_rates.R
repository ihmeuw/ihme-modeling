# ---HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Prep fatal injury rates for modelling (by economic activities)
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# load packages
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, ini, openxlsx)

# set working directories
home.dir <- "FILEPATH"
setwd(home.dir)

#set values for project
location_set_version_id <- 443
year_start <- 1970
year_end <- 2019
by_sex <- 1
by_age <- 0
relevant.ages <- c(8:20, 30, 31) #only ages 15-85
worker.version <- 20 

#varlists
id.vars <- c('location_id', 'year_id', 'sex_id', 'age_group_id')

##in##
data.dir <- "FILEPATH"
cw.dir <- "FILEPATH"
doc.dir <- "FILEPATH"
isic.map <- "FILEPATH"
isic.3.map <- read.xlsx(isic.map, sheet = "ISIC_REV_3_1") %>% as.data.table
worker.dir <- "FILEPATH"

##out##
out.dir <- "FILEPATH"
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
ubcov.function.dir <- "FILEPATH"
file.path(ubcov.function.dir, "FUNCTION") %>% source
# central functions
source("FUNCTION")
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

fileName <- function(dir,
                     loc,
                     value,
                     suffix) {
  
  file.path(dir, paste0(loc, '_', value, suffix)) %>% return
  
}
#***********************************************************************************************************************

#***********************************************************************************************************************
# ---PREP FATAL INJURIES------------------------------------------------------------------------------------------------
#rate of fatal injuries
#read in and prep data
dt <- file.path(out.dir, "pre_cw_occ_inj.csv") %>% fread

#cleanup
dt[sex=="SEX_M", sex_id := 1] #use GBD notation
dt[sex=="SEX_F", sex_id := 2] #use GBD notation
dt[sex=="SEX_T", sex_id := 3] #use GBD notation
dt[, age_group_id := 201] #GBD notation for the 15-69 (workers) age group

dt[, data := rate_unadj]
dt <- dt[!is.na(data) & outlier==0 & !grepl("X|0",isic_code)] #remove missing points and points marked as outliers
dt[, index_col := 1:.N] # label each data point with a unique identifier

# mark surveys for custom aggregation of TOTAL injury rates when complete industry-specific rates are provided
x <- dt[,paste(unique(isic_code),collapse = ", "),by=c("ihme_loc_id","year_id","sex_id","isic_version")]
x[isic_version == "ISIC2" & !grepl("TOTAL",V1) & grepl(1,V1) & grepl(2,V1) & grepl(3,V1) & grepl(4,V1) &
    grepl(5,V1) & grepl(6,V1) & grepl(7,V1) & grepl(8,V1) & grepl(9,V1), cv_custom_agg := 1]
x[isic_version == "ISIC3" & !grepl("TOTAL",V1) & grepl("A",V1) & grepl("B",V1) & grepl("C",V1) & grepl("D",V1) &
    grepl("E",V1) & grepl("F",V1) & grepl("G",V1) & grepl("H",V1) & grepl("I",V1) & grepl("J",V1) &
    grepl("K",V1) & grepl("L",V1) & grepl("M",V1) & grepl("N",V1) & grepl("O",V1) & grepl("P",V1) & grepl("Q",V1), cv_custom_agg := 1]
x[isic_version == "ISIC4" & !grepl("TOTAL",V1) & grepl("A",V1) & grepl("B",V1) & grepl("C",V1) & grepl("D",V1) &
    grepl("E",V1) & grepl("F",V1) & grepl("G",V1) & grepl("H",V1) & grepl("I",V1) & grepl("J",V1) &
    grepl("K",V1) & grepl("L",V1) & grepl("M",V1) & grepl("N",V1) & grepl("O",V1) & grepl("P",V1) & grepl("Q",V1) &
    grepl("R",V1) & grepl("S",V1) & grepl("T",V1) & grepl("U",V1), cv_custom_agg := 1]
dt <- merge(dt,x[,-c("V1"),with = F],by=c("ihme_loc_id","year_id","sex_id","isic_version"),all=T)


#merge on levels
source("FUNCTION")
locations <- get_location_metadata(gbd_round_id = 6, location_set_id = 22)
locations <- unique(locations[is_estimate==1, location_id]) %>% sort
locs <- get_location_hierarchy(location_set_version_id)
levels <- locs[,grep("level|location_id|ihme_loc_id", names(locs)), with=F]

#first fix CHN iso3s
dt[ihme_loc_id=="CHN", 'ihme_loc_id' := "CHN_44533"]
dt[ihme_loc_id=="HKG", 'ihme_loc_id' := "CHN_354"]
dt[ihme_loc_id=="MAC", 'ihme_loc_id' := "CHN_361"]
dt <- merge(dt, levels, by="ihme_loc_id")

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

dt[, lowess := predLow(.SD) %>% as.numeric, .SDcols=c("data", 'year_id'), by=list(ihme_loc_id, sex_id, age_group_id, isic_code)]

#calculate residuals, then take the SD for each lowess model and use it to compute the variance
dt[, residual := (lowess-data)]
dt[, sd := sd(residual), by=list(ihme_loc_id, sex_id, age_group_id, isic_code)]

#use cv to impute the variance where we didnt have enough points to use the lowess
dt[, cv := sd / data]
dt[, reg_cv := mean(cv, na.rm=T), by=list(region_id)]
dt[, sr_cv := mean(cv, na.rm=T), by=list(super_region_id)]
dt[is.na(sd), sd := data * reg_cv]
dt[is.na(sd), sd := data * sr_cv]

#calculate variance & add standard error column
dt[, variance := sd^2]
dt[, standard_error := sqrt(variance)]

#crosswalk data sources to insurance records in order to correct for underreporting
#logic here is that insurance records are the better source because more incentive to report
# create a column for study_id in run_mr_brt
dt[, rand_eff := .GRP, by = c("level_1", "level_2")]

# convert to log space
dt[, log_data := log(data)]
dt[, log_se := sapply(1:nrow(dt), function(i) {
  mean_i <- dt[i, data]
  se_i <- dt[i, standard_error]
  deltamethod(~log(x1), mean_i, se_i^2)
})]

# add dummy variables for each source type
dt[sourcelabel == "Insurance records", ins_records := 1]
dt[is.na(ins_records), ins_records := 0]
dt[sourcelabel == "Economic or establishment census", census := 1]
dt[is.na(census), census := 0]
dt[sourcelabel == "Establishment or business register", register := 1]
dt[is.na(register), register := 0]
dt[sourcelabel == "Establishment survey", estab_survey := 1]
dt[is.na(estab_survey), estab_survey := 0]
dt[sourcelabel == "Labour force survey", labor_survey := 1]
dt[is.na(labor_survey), labor_survey := 0]
dt[sourcelabel == "Labour inspectorate records", labor_records := 1]
dt[is.na(labor_records), labor_records := 0]
dt[sourcelabel == "Official estimate", off_est := 1]
dt[is.na(off_est), off_est := 0]
dt[sourcelabel == "Other administrative records and related sources", other_sources := 1]
dt[is.na(other_sources), other_sources := 0]
dt[sourcelabel == "Records of employers' organizations", emp_org_records := 1]
dt[is.na(emp_org_records), emp_org_records := 0]

write.csv(dt, "FILEPATH")

source("FUNCTION")

fit1 <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = "lmer",
  data = "FILEPATH/mrbrt_input.csv",
  mean_var = "log_data",
  se_var = "log_se",
  study_id = "rand_eff",
  covs = list(cov_info("census", "X"),
              cov_info("register", "X"),
              cov_info("estab_survey", "X"),
              cov_info("labor_survey", "X"),
              cov_info("labor_records", "X"),
              cov_info("off_est", "X"),
              cov_info("other_sources", "X"),
              cov_info("emp_org_records", "X")
  ),
  remove_x_intercept = FALSE,
  overwrite_previous = TRUE
)

# predict onto data
dt[sourcelabel == "Insurance records", sourcelabel_short := "ins_records"]
dt[sourcelabel == "Economic or establishment census", sourcelabel_short := "census"]
dt[sourcelabel == "Establishment or business register", sourcelabel_short := "register"]
dt[sourcelabel == "Establishment survey", sourcelabel_short := "estab_survey"]
dt[sourcelabel == "Labour force survey", sourcelabel_short := "labor_survey"]
dt[sourcelabel == "Labour inspectorate records", sourcelabel_short := "labor_records"]
dt[sourcelabel == "Official estimate", sourcelabel_short := "off_est"]
dt[sourcelabel == "Other administrative records and related sources", sourcelabel_short := "other_sources"]
dt[sourcelabel == "Records of employers' organizations", sourcelabel_short := "emp_org_records"]

pred1 <- predict_mr_brt(fit1, newdata = dt)

model_preds <- data.table(pred1$model_summaries)
model_preds[X_census == 1, sourcelabel_short := "census"]
model_preds[X_register == 1, sourcelabel_short := "register"]
model_preds[X_estab_survey == 1, sourcelabel_short := "estab_survey"]
model_preds[X_labor_survey == 1, sourcelabel_short := "labor_survey"]
model_preds[X_labor_records == 1, sourcelabel_short := "labor_records"]
model_preds[X_off_est == 1, sourcelabel_short := "off_est"]
model_preds[X_other_sources == 1, sourcelabel_short := "other_sources"]
model_preds[X_emp_org_records == 1, sourcelabel_short := "emp_org_records"]
model_preds[is.na(sourcelabel_short), sourcelabel_short := "ins_records"]
model_preds[, prediction_se_log := (Y_mean_hi - Y_mean_lo)/3.92]

model_preds <- unique(model_preds)[, .(sourcelabel_short, Y_mean)]
model_preds[, Y_mean_exp := exp(Y_mean)]
model_preds[, ratio := Y_mean_exp/model_preds[sourcelabel_short == "ins_records", Y_mean_exp]]
model_preds[, inv_ratio := 1/ratio]
saveRDS(model_preds, "FILEPATH/model_preds.RDS")

# merge the alternative def data with the crosswalk ratios
dt <- merge(dt, model_preds, by = "sourcelabel_short")

# crosswalk (multiply orig data by inv_ratio)
setnames(dt, "data", "orig.data")
dt[, data := orig.data * inv_ratio]

##crosswalk isic versions using splits defined by the major groups correspondance tables
#we will be crosswalking everything to ISIC3
ref <- dt[isic_version=="ISIC3" | isic_code == "TOTAL"] #totals and missings do not need to be crosswalked

#crosswalk ISIC 4 to ISIC 3
split.isic4 <- dt[isic_version=="ISIC4" & isic_code != "TOTAL"]
split.isic4[, major_isic_4 := as.character(isic_code)]
#calculate the count of categories to inform uncertainty around splits
split.isic4[, count := unique(major_isic_4) %>% length, by=list(ihme_loc_id, year_id, sex_id, source)]
isic4.map <- file.path(cw.dir, "ISIC_4_TO_3_LVL_1_CW.csv") %>% fread
split.isic4 <- merge(split.isic4, isic4.map[, list(major_isic_3, major_isic_4, prop)],
                     by="major_isic_4", allow.cartesian = T)

#now use the proportion to xwalk each ISIC4 category to ISIC3, then collapse
split.isic4[, split_data := lapply(.SD, function(x, w) sum(x*w), w=prop), .SDcols='data',
            by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id')]
split.isic4 <- unique(split.isic4, by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id'))
split.isic4[, isic_code := major_isic_3]
split.isic4[count<9, natlrep := 0] #increase uncertainty of the lower count points

#crosswalk ISIC 2 to ISIC 3
split.isic2 <- dt[isic_version=="ISIC2" & isic_code != "TOTAL"]
split.isic2[, major_isic_2 := as.numeric(isic_code)]
#calculate the count of categories to inform uncertainty around splits
split.isic2[, count := unique(major_isic_2) %>% length, by=list(ihme_loc_id, year_id, sex_id, source)]
isic2.map <- file.path(cw.dir, "ISIC_2_TO_3_LVL_1_CW.csv") %>% fread
split.isic2 <- merge(split.isic2, isic2.map[, list(major_isic_2, major_isic_3, prop)],
                     by="major_isic_2", allow.cartesian = T)

#now use the proportion to xwalk each ISIC2 category to ISIC3, then collapse
split.isic2[, split_data := lapply(.SD, function(x, w) sum(x*w), w=prop), .SDcols='data',
            by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id')]

split.isic2 <- unique(split.isic2, by=c('major_isic_3', 'location_id', 'year_id', 'sex_id', 'source', 'age_group_id'))
split.isic2[, isic_code := major_isic_3]
split.isic2[count<5, natlrep := 0] #increase uncertainty of the lower count points

dt <- rbindlist(list(ref,
                     split.isic4[, -c('major_isic_3', 'major_isic_4', 'prop'), with = F],
                     split.isic2[, -c('major_isic_3', 'major_isic_2', 'prop'), with = F]),
                use.names=T,
                fill=T)

dt[!is.na(split_data), data := split_data]
dt[, isic_version := "ISIC3"]

#read employment ratio from outputs
#saved by country so needs to be appended
appendFiles <- function(country, dir, pfx, sfx) {
  
  if (country > 53500) message('reading ', country)
  
  dt <- fileName(dir, country, pfx, sfx) %>% fread
  
}

workers <- lapply(locations, appendFiles, dir=worker.dir, pfx='ind', sfx='_workers_mean.csv') %>% rbindlist
workers[, me_name := NULL]

workers.total <- copy(workers[age_group_id == 201])
workers.total[, workers_mean := sum(workers_mean), by=list(location_id, year_id, age_group_id, sex_id)]
workers.total <- unique(workers.total, by=c("location_id", "year_id", "sex_id", "age_group_id"))
workers.total[,isic_code := "TOTAL"]
workers <- rbind(workers, workers.total)

#merge pops to your data
dt <- merge(dt, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id', 'isic_code'))

## custom aggregate the surveys with all the relevant industry-specific rates
x <- dt[cv_custom_agg == 1]
x[,workers_total := sum(workers_mean),by=list(ihme_loc_id,year_id,sex_id,source)]
x[,component_inj := data*workers_mean/workers_total]
x <- x[,sum(component_inj), by=list(ihme_loc_id,year_id,sex_id,source)]
x <- merge(dt[cv_custom_agg == 1 & isic_code == "A"], x, by=c("ihme_loc_id","year_id","sex_id","source"))
x[,isic_code := "TOTAL"]
x[,occ_label := "ISIC-Rev.2: Total"]
x[,c("split_data","data") := V1]
x[,V1 := NULL]
dt <- rbind(dt,x)

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
  sex.split.sr <- sex.splits[region_id %in% sr.merge]
  sex.split.sr <- merge(sex.split.sr, super.sex, by=c('super_region_id', 'age_group_id', 'isic_code'), allow.cartesian=TRUE)
  sex.split.sr <- merge(sex.split.sr, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id', 'isic_code'))
  sex.split.sr[, data := data_og*workers_mean_og/workers_mean*super_sex_ratio]
  sex.split.sr[, data_diff := data - data_og]
}

sex.splits <- merge(sex.splits, reg.sex, by=c('region_id', 'age_group_id', 'isic_code'), allow.cartesian=TRUE)
sex.splits <- merge(sex.splits, workers, by=c('location_id', 'year_id', 'sex_id', 'age_group_id', 'isic_code'))
sex.splits[, data := data_og*workers_mean_og/workers_mean*reg_sex_ratio]
sex.splits[, data_diff := data - data_og]

#combine and clean up
splits <- list(sex.splits, sex.split.sr) %>% rbindlist(fill=TRUE)
splits <- splits[abs(data_diff) < 500]
clean.splits <- splits[, -c('sex', 'data_diff',names(splits)[names(splits) %like% 'ratio' | names(splits) %like% 'og']),
                       with=FALSE]

clean.splits[, natlrep := 0] #give less weight to split points
clean.ref <- ref[, -c('sex',names(ref)[names(ref) %like% 'ratio' | names(ref) %like% 'og']),
                 with=FALSE]

dt <- list(clean.ref, clean.splits) %>% rbindlist(use.names=T)

#splits/xwalks can lead to some data going negative. remove these unstable points
dt <- dt[data > 0]

#set covariates
dt[natlrep==0, cv_subgeo := 1] #set to proper st-gpr format
dt[natlrep==1, cv_subgeo := 0]

#cleanup
dt <- dt[, -c('lowess', 'residual', 'cv', 'sd', 'reg_cv', 'sr_cv', 'rate_unadj', 'split_data', 'occ_label', 'natlrep')]

#final gpr prep/cleanup
dt[, nid := 144370]
dt[, sample_size := NA]
dt[, age_group_id := 22]

#output all
write.csv(dt, "FILEPATH")

#merge on the model me names (short ISIC categories) for saving
dt <- merge(dt, isic.3.map[, list(major, major_label_me)] %>% unique,
            by.x="isic_code", by.y="major", all.x=T)

# format for bundle upload
setnames(dt, c("data", "outlier"), c("val", "is_outlier"))
dt[, seq := NA]
dt[, underlying_nid := NA]
dt[sex_id == 1, sex := "Male"]
dt[sex_id == 2, sex := "Female"]
dt[, measure := "continuous"]

# Now loop over the different levels, merge the square, fill in study level covs, and save a csv
saveData <- function(cat, dt) {
  
  message("saving -> ", cat)
  
  cat.dt <- dt[isic_code==cat]
  
  this.entity <- ifelse(cat!="TOTAL",
                        paste("occ_inj_major", cat, cat.dt[1, major_label_me], sep = "_"),
                        "occ_inj_major_TOTAL")
  
  cat.dt[, me_name := this.entity]
  
  write.xlsx(cat.dt, file=file.path(out.dir, paste(this.entity, ".xlsx", sep = "")), sheetName = "extraction")
  
  return(cat.dt)
  
}

list <- lapply(c(dt[, isic_code] %>% unique), saveData, dt=dt)
