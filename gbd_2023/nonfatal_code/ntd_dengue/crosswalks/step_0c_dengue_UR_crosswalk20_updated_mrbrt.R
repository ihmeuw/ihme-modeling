################################################################################
### NTDs Dengue
# Description - Fit and apply under-reporting adjusment using MR-BRT

################################################################################
##1) LOAD PACKAGES AND CENTRAL FUNCTIONS
rm(list=ls())

##1) LOAD PACKAGES AND CENTRAL FUNCTIONS
# Sys.setenv("RETICULATE_PYTHON" = "FILEPATH")
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")
library(crosswalk002)
library(mrbrt002)

# library(crosswalk, lib.loc = "FILEPATH")
library(dplyr)
library(data.table)
library(openxlsx)
library(stringr)
library(tidyr)
library(boot)
library(ggplot2)

# central functions
source("FILEPATH/get_ids.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_population.R")
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_crosswalk_version.R')
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/upload_bundle_data.R")
################################################################################
# set paths and other variables
release_id <- ADDRESS
path <- paste0("FILEPATH")
date <- Sys.Date()
cod_m <- ADDRESS
cod_f <- ADDRESS
cod_shk_m <- ADDRESS
cod_shk_f <- ADDRESS
bundle_6575_cw <- ADDRESS

################################################################################

### read in the under-reporting data 
dat_metareg <- fread(paste0(path, "FILEPATH"))
dat_metareg<-dat_metareg[dat_metareg$group_review==1,]
dat_metareg[location_id == 53578, location_id := 16]
dat_metareg[, country := substr(ihme_loc_id, 1, 3)]

# Mark Brazil data as outlier
dat_metareg <- subset(dat_metareg, location_id != 135)

dat_metareg[, ratio_test_new := mean_1 / mean_2]
dat_metareg$ratio_test_new<-as.numeric(dat_metareg$ratio_test_new)
dat_metareg$mean<-dat_metareg$mean_2

# summary
summary(dat_metareg$ratio_test_new)

# drop missing data on SE from comparison group
dat_metareg<-dat_metareg[!is.na(dat_metareg$se_1),]

# create list of locations
locs_cov<-unique(dat_metareg$location_id)
yrs_cov<-unique(dat_metareg$year_start)

# get covariate estimates - sdi= ADDRESS
covid<-ADDRESS
covs1<-get_covariate_estimates(covariate_id=covid, year_id=yrs_cov,location_id=locs_cov, release_id = release_id)
covs1$year_start<-covs1$year_id
covs1$sex<-NULL
covs1$cov<-covs1$mean_value

# get covariate estimates - This pulls the COVID CSMR
covid_csmr<-ADDRESS
covs2<-get_covariate_estimates(covariate_id=covid_csmr, year_id=yrs_cov,location_id=locs_cov, release_id = release_id)
covs2$year_start<-covs2$year_id
covs2 <- subset(covs2, sex_id ==1)
covs2$sex_id<-NULL
covs2$sex<-NULL
covs2$csmr<-covs2$mean_value * 1000

# merge with covariate values
covs <- merge(covs1, covs2, by=c("location_id","year_id"))
covs <- subset(covs, select=c("location_id","year_id","cov","csmr"))

## merge covariate value to main dataset to fit xwalk model 
# keep mean, se, location_id, year_start, nid, site_memo
data<-dat_metareg %>% 
  dplyr::select(nid, location_id, mean, year_start, age_start, age_end, site_memo, mean_1, mean_2, se_1, se_2, sample_size1, sample_size2, def_1, def_2, ratio_test_new, country)

data$year_id<-data$year_start

data<-merge(data,covs,by=c("location_id","year_id"))

# need to check distribution of covar
summary(data$cov)
summary(data$cov)
summary(data$csmr)

# write data as csv to save copy
write.csv(data,paste0(path, "FILEPATH"), row.names = FALSE) 

# compare active (mean_1) to passive (mean_2) by year
ggplot(data, aes(x=mean_1, y=mean_2, color=cov)) +
  geom_point()

ggplot(data, aes(x=cov, y=log(ratio_test_new), color=as.factor(location_id))) +
  geom_point() +
  xlab("covariate") +
  ylab("log(ratio [active/passive])")


# note we are using linear_to_log since we are modeling incidence
# change to linear_to_logit if you model prevalence
dat_diff <- as.data.frame(cbind(
  delta_transform(
    mean = data$mean_2, 
    sd = data$se_2,
    transformation = "linear_to_log" ),
  delta_transform(
    mean = data$mean_1, 
    sd = data$se_1,
    transformation = "linear_to_log")
))

# recode names 
names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")

# note log_diff or logit_diff
data[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = dat_diff, 
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref" )

################################################################################
######## prep for running in crosswalk package ##############################
data[, location_id := as.character(location_id)]

# dataset for meta-regression (e.g., dataset with the M:F comparisons)
df1 <- CWData(
  df = data,          # dataset for metaregression
  obs = "log_diff",       # column name for the observation mean
  obs_se = "log_diff_se", # column name for the observation standard error
  alt_dorms = "def_2",     # column name of the variable indicating the alternative method
  ref_dorms = "def_1",     # column name of the variable indicating the reference method
  covs = list("cov"),     # names of columns to be used as covariates later- HAQI
  study_id = "country",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

# Here we run a model for intercept, just to get the sex-ratio  
fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_log", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    crosswalk002::CovModel(cov_name = "intercept"),
    crosswalk002::CovModel(cov_name = "cov")),
  #crosswalk002::CovModel(cov_name = "csmr")),
  #CovModel(cov_name = "cov", spline = XSpline(knots = c(0,.2, .4, .6), degree = 3L, l_linear = TRUE, r_linear = TRUE), spline_monotonicity = "decreasing")),
  gold_dorm = "active",  # the level of `alt_dorms` that indicates it's the gold standard
  use_random_intercept=TRUE)   


# store fit1
py_save_object(object = fit1, filename = paste0(path, "FILEPATH"), pickle = "dill")

# view the results--fixed effects 
fit1$fixed_vars
fit1$beta
fit1$beta_sd
fit1$gamma


################################################################################
##### apply crosswalk to incidence data 

# pull from crosswalk version save with dedup data - this data is only both sex
dat_original_a <- get_crosswalk_version(bundle_cw)

###
dat_original_b <- read.xlsx(paste0(path,"FILEPATH", sheet = "extraction")) 

## create counter of grouped observations
da_loc_years<-dat_original_a %>% group_by(location_id) %>% count(year_start)
da_loc_years$in_age_all<-1

# create counter of grouped observations
aa_loc_years<-dat_original_b %>% group_by(location_id) %>% count(year_start)
aa_loc_years$in_age_spec<-1

################################################################################
# compare year/locs combinations to avoid duplication of all-age data
check_locs<-merge(da_loc_years, aa_loc_years, by=c("location_id","year_start"), all=TRUE)

table(check_locs$in_age_all, check_locs$in_age_spec)

locs_to_drop<- subset(check_locs, (in_age_all ==1 & in_age_spec ==1))
locs_to_drop <- subset(locs_to_drop, select = c(location_id, year_start))
locs_to_drop$drop <-1

dat_original_b2 <- merge(dat_original_b, locs_to_drop, by=c("location_id","year_start"), all.x=TRUE)
dat_original_b2 <- subset(dat_original_b2, is.na(drop))

###### Data
dat_original <- rbind(dat_original_a, dat_original_b2, fill=T)

# drop if person*years with small sample size
dat_original<-dat_original[dat_original$unit_type!="Person*year"| dat_original$sample_size>1500,]

# recode any original incidence >.1 to "active"
dat_original<-dat_original%>%
  mutate(reporting=if_else(mean>.075,"active","passive"))


## drop if mean==0
dat_original_0s <- subset(dat_original, mean == 0)
dat_original<-dat_original[dat_original$mean>0,]

# get covariate estimates - based on covariate id argument passed above
locs2<-unique(dat_original$location_id)
yrs2<-unique(dat_original$year_start)

covs_dt<-get_covariate_estimates(covariate_id=covid, year_id=yrs2, location_id=locs2, release_id = release_id)
covs_dt$year_start<-covs_dt$year_id
covs_dt$sex<-NULL
covs_dt$cov<-covs_dt$mean_value

covs_dt_csmr<-get_covariate_estimates(covariate_id=covid_csmr, year_id=yrs2, location_id=locs2, release_id = release_id)
covs_dt_csmr<- subset(covs_dt_csmr, sex_id ==1)

covs_dt_csmr$year_start<-covs_dt_csmr$year_id
covs_dt_csmr$sex<-NULL
covs_dt_csmr$sex_id <-NULL
covs_dt_csmr$csmr<-covs_dt_csmr$mean_value*1000

# merge on covs
covs_dt2 <- merge(covs_dt, covs_dt_csmr, by=c("location_id","year_start"), all.x=TRUE)
covs_dt2 <- subset(covs_dt2, select= c( location_id, year_start, cov, csmr))


loc.country <- get_location_metadata(location_set_id = 35, release_id = release_id)
loc.country[, country := substr(ihme_loc_id,1,3)]
loc.country <- subset(loc.country, select = c(location_id, country))

dat_original$location_id <- as.integer(dat_original$location_id)

dat_original<-merge(dat_original,covs_dt2,by=c("location_id","year_start"))
dat_original<-merge(dat_original,loc.country,by=c("location_id"), all.x=T)


setDT(dat_original)

aa2 <- subset(dat_original, location_id ==135)
dat_original[, location_id := as.character(location_id)]

# convert passive to active
dat_original[, c("inc_adj", "inc_adj_se", "diff", "diff_se","data_id") ] <- adjust_orig_vals(
  fit_object = fit1,       # result of CWModel()
  df = dat_original,            # original data with obs to be adjusted
  orig_dorms = "reporting", # name of column with (all) def/method levels
  study_id = "country",
  orig_vals_mean = "mean",  # original mean
  orig_vals_se = "standard_error"  # standard error of original mean
)

# plot comparison between mean and inc2
aaa <- subset(dat_original,unique_id ==3732)
ggplot(dat_original, aes(x=mean, y=inc_adj, color=reporting)) +
  geom_point(shape=1) + theme_bw()+
  scale_colour_hue(l=50) + 
  # xlim(0, .25)+
  # ylim(0, .25)+ 
  xlab("Reported Incidence") + ylab("Adjusted Incidence") +ggtitle("Reported v. Adjusted Incidence")


summary(dat_original$inc_adj)

#############################################################
final_data$new_cases<-final_data$inc_adj*final_data$sample_size
final_data$old_cases<-final_data$cases
final_data$cases<-NULL

final_data$orig_mean<-final_data$mean
final_data$mean<-final_data$inc_adj
final_data$variance<-final_data$inc_adj_se^2

# data exceeding 1 is implausible
test<-final_data[final_data$inc_adj>.5,]

# plot observed v. crosswalked data
# Same, but with different colors and add regression lines
ggplot(final_data, aes(x=orig_mean, y=inc_adj, color=reporting)) +
  geom_point(shape=1) + theme_bw()+
  scale_colour_hue(l=50) + 
  xlim(0, .2)+
  ylim(0, .2)+ xlab("Reported Incidence") + ylab("Adjusted Incidence") +ggtitle("Reported v. Adjusted Incidence")

final_data$old_cases<-as.numeric(final_data$old_cases)

# create ratio variable
final_data$ratio<-final_data$inc_adj/final_data$orig_mean
summary(final_data$ratio)


########
data_plot <- copy(data)
data_plot[, ratio := ratio_test_new] 
data_plot[, type := "input_data"]


dat_pred_plot <- copy(final_data) 
dat_pred_plot[, type := "predicted"]
aa3<-subset(dat_pred_plot, location_id == 135)
data_plot2 <- rbind(data_plot,dat_pred_plot, fill=T )

ggplot(data= data_plot2, aes(x = cov, y = ratio)) +
  geom_point(aes(color = type),  alpha = ifelse(data_plot2$type == "input_data", 1, 0.1)) +
  #geom_point(shape=1) +
  #geom_point(data_plot2, aes(x=csmr, y=ratio)) +
  theme_bw()+
  scale_colour_hue(l=50) + 
  # xlim(0, .25)+
  # ylim(0, .25)+ 
  xlab("SDI") + ylab("Ratio") +ggtitle("")

ggplot(data= data_plot2, aes(x = (cov), y = log(ratio))) +
  geom_point(aes(color = type),  alpha = ifelse(data_plot2$type == "input_data", 1, 0.1)) +
  #geom_point(shape=1) +
  #geom_point(data_plot2, aes(x=csmr, y=ratio)) +
  theme_bw()+
  scale_colour_hue(l=50) + 
  # xlim(0, .25)+
  # ylim(0, .25)+ 
  xlab("SDI") + ylab("log(Ratio)") +ggtitle("")

aa <- subset(data_plot2, ratio ==1)
locs <- c(11, 135, 69, 130, 131, 18, 163, 20, 10, 522, 16, 13,29, 125)    
loctable <- subset(data_plot2, location_id %in% locs & year_start >2008 & year_end < 2012)
singapore <- subset(data_plot2,location_id == 69)
#correct inputs -return to original incidence >.5

#ratio_global <- mean(final_data$ratio, na.rm = T)
final_data2<-final_data%>%
  mutate(new_cases=if_else(inc_adj>.5,0.5*sample_size,new_cases))%>%
  mutate(inc_adj=if_else(inc_adj>.5,0.5,inc_adj))

data_plot3 <- subset(data_plot2, location_id %in% locs)

aa <- subset(final_data2, location_name == "Nicaragua")

ggplot(data= data_plot3, aes(x = (cov), y = log(ratio))) +
  geom_point(aes(color = country),  alpha = ifelse(data_plot3$type == "input_data", 1, 0.1)) +
  #geom_point(shape=1) +
  #geom_point(data_plot2, aes(x=csmr, y=ratio)) +
  theme_bw()+
  scale_colour_hue(l=50) + 
  # xlim(0, .25)+
  # ylim(0, .25)+ 
  xlab("SDI") + ylab("log(Ratio)") +ggtitle("")

summary(final_data2$inc_adj)

final_data2$mean<-final_data$inc_adj
final_data2$variance<-final_data$inc_adj_se^2

### Need to delete senegal data and Netherlands
final_data2 <- subset(final_data2, location_id != 216)

# copy results of crosswalk for reference/de-bugging as needed:   -- 
write.csv(final_data2, paste0(path,"FILEPATH"), row.names = FALSE) 

# output if you choose to further sex split, same file as above: 
write.csv(final_data2,paste0(path,"FILEPATH"), row.names = FALSE) 

################################################################################
########################  Apply UR correction using CFR
################################################################################
### Check CFR
## incidence
zzdengue_incidence <-as.data.table(read.csv(paste0(path,"FILEPATH"))) 
zzdengue_incidence <- subset(zzdengue_incidence, select= -c(csmr))
zzdengue_incidence[,year_id  := year_start]

location_list <- unique(zzdengue_incidence$location_id)

# pull cod
dengue_m <- get_model_results('ADDRESS', ADDRESS,  age_group_id=ADDRESS, sex_id=1, location_set_id=22, 
                              location_id = location_list,  release_id=release_id, 
                              measure_id=ADDRESS, model_version_id = cod_m)
dengue_f<-get_model_results('ADDRESS', ADDRESS,  age_group_id=ADDRESS, sex_id=2, location_set_id=22, 
                            location_id = location_list,   release_id=release_id, 
                            measure_id=ADDRESS, model_version_id = cod_f)

dengue_m_shk <- get_model_results('ADDRESS', ADDRESS,  age_group_id=ADDRESS, sex_id=1, location_set_id=22, 
                                  location_id = location_list,  release_id=release_id, 
                                  measure_id=1, model_version_id = cod_shk_m)
dengue_f_shk<-get_model_results('ADDRESS', ADDRESS,  age_group_id=ADDRESS, sex_id=2, location_set_id=22, 
                                location_id = location_list,   release_id=release_id, 
                                measure_id=1, model_version_id = cod_shk_f)
dengue_m_shk[,population := 0]
dengue_f_shk[,population := 0]

dengue_both <- rbind(dengue_m, dengue_f,dengue_m_shk , dengue_f_shk, fill=T)
dengue_both <- dengue_both[, lapply(.SD, sum), by = c("location_id", "year_id", "age_group_id"),
                           .SDcols = c("mean_death", "population")]

dengue_both[, csmr := mean_death/population]

zzdengue_incidence$location_id <- as.integer(zzdengue_incidence$location_id)
zz_cfr <- merge(zzdengue_incidence, dengue_both, by= c('location_id', "year_id" ), all.x=T)

zz_cfr[, cfr := csmr/inc_adj]

cfr_max <- 0.055

# Re-adjusting the cfrs that are still > cfr_max
# Only doing step 5
zz_cfr2 <- copy(zz_cfr)

#### NOTE: originally this was: zz_cfr2[new_cfr> cfr_max , ur_csmr_correction :=2 ]
#### it was changed since we are not running the first adjustment 
zz_cfr2[cfr> cfr_max , ur_csmr_correction :=2 ]
zz_cfr2[ur_csmr_correction ==2, inc_adj := csmr/cfr_max ]
table(zz_cfr2$ur_csmr_correction)
aaaa <- subset(zz_cfr2, ur_csmr_correction ==2)

zz_cfr3 <- copy(zz_cfr2)
zz_cfr3[, new_ratio := inc_adj/orig_mean]
zz_cfr3[, new_cfr := csmr/inc_adj]
zz_cfr3[, new_cases := inc_adj*sample_size]

write.csv(zz_cfr3,paste0(path, "FILEPATH"), row.names = FALSE) 

