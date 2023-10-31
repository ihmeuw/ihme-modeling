## ******************************************************************************
##
## Purpose: Crosswalk prevalence data from one alternate definiton to match
##          the reference defn
## Input:   - Ratios from MR-BRT for every covariate pair
##          - Original non-clinical data, to be adjusted as necessary per the crosswalk.
##            Do not adjust literature data, only adjust registry data.
## Output:  - Original bundle with the adjusted prevalence mean and standard errors
##            appended (.csv)
##            - Outliers applied
##
## ******************************************************************************

Sys.umask(mode = 002)
source("FILEPATH/gbd2020_launch_agesex_split.R")
source("FILEPATH/gbd2020_launch_agesex_plot.R")

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))

library(data.table)
library(magrittr)
library(dplyr)
library(openxlsx)
library(readxl)
library(ggplot2)
library(reticulate)
library("crosswalk", lib.loc = "FILEPATH")
library(metafor, lib.loc = "FILEPATH")
library(msm)
library(RhpcBLASctl, lib.loc = "FILEPATH")
RhpcBLASctl::omp_set_num_threads(threads = 4)


args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
step <- args[2]
model_description <- args[3]
gbd_round_id <- args[4]
print(bun_id)
print(step)
print(j)
bun <- bun_id
b <- bun_id
step <- "iterative"
gbd_round_id <- 7
measure_name <- "prevalence"
measure <- "prevalence"


## EUROCAT NIDs: 
nids<-list(159930, 159937, 128835 , 159941, 163937, 159924, 159938, 159927, 
           159926, 159927, 159928, 159929, 159931, 159932, 159933, 159934, 
           159935, 159936, 159939, 159940, 159942, 163938, 163939, 159925, 128835)


################################################################################################################################################################################
######  Read in map of which bundles get which crosswalks: ###### 
cov_map <- fread(paste0('FILEPATH/crosswalk_ratio_application_map.csv'))
cov_map <- as.data.table(cov_map)
################################################################################################################################################################################


################################################################################################################################################################################
###### Load all original bundle data ###### 

# prevalence data:
# pull the bundle ver so we have it in memory
bv <- fread(paste0("FILEPATH/gbd2020_", step, "_", bun_id, "_CURRENT.csv"))
b_v <- bv$bundle_version_id 
dat_original <- read.xlsx(paste0("FILEPATH/", bun_id, "_", measure_name, "_full.xlsx"))
dat_original <- as.data.table(dat_original)
dat_original <- dat_original[, crosswalk_parent_seq:=seq]
# non prevalence data: 
non_prev <- get_bundle_version(b_v, fetch = "all")
non_prev <- as.data.table(non_prev)
non_prev <- non_prev[! (measure %like% "prev"),]
non_prev <- non_prev[, crosswalk_parent_seq:=seq]


# separate clinical data as it isn't needed at this step: 
clin_data <- dat_original[clinical_data_type %like% "claims" | clinical_data_type %like% "inpatient",]
dat_original <- dat_original[ !(clinical_data_type %like% "claims" | clinical_data_type %like% "inpatient")]

# some housekeeping for year
dat_original <- dat_original[year_start>1984]
dat_original <- dat_original[,year_id := floor((as.numeric(year_end)+as.numeric(year_start))/2)]
dat_original <- as.data.table(dat_original)

# merge on haqi for correction later on: 
haqi <- get_covariate_estimates(covariate_id=1099, decomp_step = step, gbd_round_id=gbd_round_id)
haqi <- as.data.table(haqi)
message("trying to merge in haqi")
setnames(haqi, "mean_value", "haqi_mean")
vars <- c("haqi_mean", "location_id", "year_id")
haqi <- haqi[,..vars]
haqi <- haqi[,haqi_mean:=haqi_mean/100]
dat_original <- merge(dat_original, haqi, by=c("year_id", "location_id"), all.x=TRUE)
################################################################################################################################################################################



################################################################################################################################################################################
######  Pull and apply ratios  ######  

cvs <- c("cv_livestill", "cv_excludes_chromos")
all_data_adjusted_tmp <- data.table()
dat_original <- as.data.table(dat_original)
rows_to_keep <- dat_original[cv_livestill==0 & cv_excludes_chromos==0,]


for(cv in cvs){
  print(cv)
  
  rm(rows_to_adjust)
  rm(fit1)
  dat_original <- as.data.frame(dat_original)
  rows_to_adjust <- dat_original[dat_original[unlist(cv)]==1, ]
  rows_to_adjust <- as.data.table(rows_to_adjust)
  
  rows_to_adjust$obs_method <- "alternate"
  rows_to_adjust$sex_id <- 1
  rows_to_adjust <- rows_to_adjust[sex=="Female", sex_id :=2]
  if(cv=="cv_livestill"){
    nmr <- get_life_table(with_shock = 1, with_hiv = 1, life_table_parameter_id = 1,
                          gbd_round_id = 6, decomp_step = 'step1', age_group_id = 2)
    nmr$age_group_id <- NULL
    setnames(nmr, 'mean', 'nmr')
    rows_to_adjust <- merge(rows_to_adjust, nmr, by=c("location_id", "year_id", "sex_id"), all.x=TRUE)
    
  }
  rows_to_adjust <- as.data.table(rows_to_adjust)
  
  # apply offset
  offset <- 0.5 * median(rows_to_adjust[mean != 0, mean]) 
  rows_to_adjust[, `:=` (mean_offset = mean + offset)]
  rows_to_adjust[, `:=` (standard_error = standard_error + offset)]
  # Fill in missing SE 
  z <- qnorm(0.975)
  rows_to_adjust <- rows_to_adjust[is.na(standard_error), standard_error := ((1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2)))]
  rows_to_adjust <- rows_to_adjust[!(is.na(mean))]
  rows_to_adjust <- rows_to_adjust[!(is.na(standard_error))]
  rows_to_adjust <- rows_to_adjust[, group_id := as.character(group_id)]
  
  
  fit1 <- py_load_object(filename = paste0("FILEPATH/fit1", bun_id, ".pkl"), pickle = "dill")
  rows_to_adjust[, c("mean_adjusted_tmp", "standard_error_adjusted", "log_beta", "log_beta_se", "data_id")]  <- adjust_orig_vals(
    fit_object = fit1, # object returned by `CWModel()`
    df = rows_to_adjust,
    orig_dorms = "obs_method",
    orig_vals_mean = "mean_offset",
    orig_vals_se = "standard_error"
  )
  
  # take out the offset and make mean/mean_adjusted consistent 
  mr_brt_output <-fread(paste0("FILEPATH/", bun_id, ".csv"))
  beta <- mr_brt_output[dorms=="alternate"]$beta
  rows_to_adjust <- rows_to_adjust[mean_adjusted_tmp != 0,mean_adjusted:=mean_adjusted_tmp - offset/exp(beta)]
  rows_to_adjust <- rows_to_adjust[mean != 0,mean:=mean - offset]
  
  # bring it all together
  all_data_adjusted_tmp <- rbind(all_data_adjusted_tmp, rows_to_adjust, fill=TRUE)
  
}

rows_to_keep <- rows_to_keep[, mean_adjusted := mean]   
rows_to_keep <- rows_to_keep[, standard_error_adjusted := standard_error] 
all_data_for_modeling <- rbind(all_data_adjusted_tmp, rows_to_keep, fill=TRUE)
all_data_for_modeling <- all_data_for_modeling[,mean_unadjusted:=mean]
all_data_for_modeling <- all_data_for_modeling[,standard_error_unadjusted:=standard_error]
all_data_for_modeling <- all_data_for_modeling[,mean:=mean_adjusted/haqi_mean]
all_data_for_modeling <- all_data_for_modeling[,standard_error:=standard_error_adjusted]


################################################################################################################################################################################
###### Bring back the clinical data ###### 
all_data_for_modeling <- rbind(all_data_for_modeling, clin_data, fill=TRUE)

################################################################################################################################################################################
######  Do some stuff for xwalk validations: ###### 

all_data_for_modeling <- all_data_for_modeling[is.na(upper) & is.na(lower), uncertainty_type_value:=NA]
all_data_for_modeling <- all_data_for_modeling[is.na(upper) | is.na(lower) | is.na(uncertainty_type_value), upper:=NA]
all_data_for_modeling <- all_data_for_modeling[is.na(upper) | is.na(lower) | is.na(uncertainty_type_value), lower:=NA]
all_data_for_modeling <- all_data_for_modeling[is.na(upper) | is.na(lower) | is.na(uncertainty_type_value), uncertainty_type_value:=NA]
all_data_for_modeling <- as.data.table(all_data_for_modeling)
all_data_for_modeling <- all_data_for_modeling[upper >1, upper:=0.99999]
all_data_for_modeling <- all_data_for_modeling[lower<0, lower:=0.00001]

all_data_for_modeling <- all_data_for_modeling[, seq:=NA]
all_data_for_modeling <- all_data_for_modeling[!location_id %in% c(4621, 4623, 4624, 4618, 4619, 4625, 4626, 4622, 4620)]
if(any('cv_marketscan_all_2000' %in% colnames(all_data_for_modeling))){
  all_data_for_modeling <- all_data_for_modeling[(is.na(cv_marketscan_all_2000)) | cv_marketscan_all_2000==0]}
if(any('cv_marketscan_inp_2000' %in% colnames(all_data_for_modeling))){
  all_data_for_modeling <- all_data_for_modeling[(is.na(cv_marketscan_inp_2000)) | cv_marketscan_inp_2000==0]}
all_data_for_modeling <- all_data_for_modeling[standard_error >=1, standard_error:=0.999999]

all_data_for_modeling <- all_data_for_modeling[group_review %in% c(1, NA)]
all_data_for_modeling <- all_data_for_modeling[!(specificity %like% "dummy"),]
all_data_for_modeling <- all_data_for_modeling[mean<0, mean:=0]
all_data_for_modeling <- all_data_for_modeling[mean<1,]
all_data_for_modeling <- all_data_for_modeling[nid==424171, sample_size:=NA]
all_data_for_modeling <- all_data_for_modeling[nid==424171, effective_sample_size:=NA]
all_data_for_modeling <- all_data_for_modeling[is.na(unit_value_as_published), unit_value_as_published:=1]
all_data_for_modeling <- all_data_for_modeling[is.na(crosswalk_parent_seq), crosswalk_parent_seq:=seq]


if(any('group_review' %in% colnames(all_data_for_modeling))){
  all_data_for_modeling <- all_data_for_modeling[(group_review==1) | (is.na(group_review))]
  all_data_for_modeling <- all_data_for_modeling[is.na(group_review), specificity:=NA]
  all_data_for_modeling <- all_data_for_modeling[is.na(group_review), group:=NA]
  all_data_for_modeling <- all_data_for_modeling[group_review==1 & is.na(group), group:=1]
  all_data_for_modeling <- all_data_for_modeling[group_review==1 & is.na(specificity), specificity:="FLAG, THIS VALUE WAS MISSING"]
}



################################################################################################################################################################################



################################################################################################################################################################################
###### Apply outliers ###### 
##   
## Apply MAD outliers ## 
source(paste0(h, "FILEPATH/mad_function.R"))
mads <- mads_function(all_data_for_modeling)
message("mads worked")
df <- mads$df
mad3 <- mads$mad3 
mad_n1 <- mads$mad_n1 
message(mad3)
message(mad_n1)
## 
## Outlier data with age start 60+ ##
df <- df[age_start>=60, is_outlier:=1]

################################################################################################################################################################################

# Temp loc specific outliers #
us_china <- c(523:573, 102, 6, 491:521)

# total heart
poland <- c(51, 53660:53675)
kenya <- c(180, 35617:35663)
uganda <- c(190)
tibet <- 518
xianjiang <- 519
meghalaya <- c(4862, 43929, 43893)
uttar_pradesh <- c(4873, 43940, 43904)
russia <- c(44903:44987, 62)
# these locs are too low: 
australia <- 71
portugal <- 91
argentina <- 97
canada <- 101
thailand <- 18
too_low <- c(71, 91, 97, 101, 18)
if(bun_id==628){df <- df[location_id %in% kenya, is_outlier:=1]
                df <- df[location_id %in% poland & age_start > 19, is_outlier:=1]
                df <- df[location_id %in% uganda, is_outlier:=1]
                df <-df[location_id %in% c(518:519), is_outlier:=1]
                df <-df[location_id %in% meghalaya & nid==281819, is_outlier:=1]
                df <-df[location_id %in% uttar_pradesh, is_outlier:=1]
                df <-df[location_id %in% russia, is_outlier:=1]
                non_prev <- non_prev[measure %like% "mtwith", is_outlier:=1]
                df <- df [location_id %in% too_low, is_outlier:=1]
                 }


# single ventricle
india_locs <- c(163, 4841:4875, 43873:43942, 44538:44540)
china_locs <- c(6, 354, 361, 491:521, 8)
italy_japan_mex_bra <- c(35507, 35495, 35508, 35427, 4764, 4665)
mex <- c(4674, 4671, 4660, 4647, 4651, 4651, 4774)
italy <- c(35504, 35496)
nz_maori <- 44850
basilicata <- 35511
if(bun_id==630){df <- df[(location_id %in% india_locs | location_id %in% china_locs | (location_id == 193 & age_start < 5)), is_outlier:=1]
                df <- df[location_id %in% italy_japan_mex_bra, is_outlier:=1]  
                df <-df[location_id %in% mex, is_outlier:=1]
                df <-df[location_id %in% italy, is_outlier:=1]
                df <-df[location_id ==35511, is_outlier:=1]
                df <-df[location_id ==44850 & age_start>10, is_outlier:=1]
                  }   

# malformations of great vessels
uganda <- 190
sokoto <- 25351
turkey <- 155
saudi <- 152
tibet <- 518
mex <- c(4644, 4645, 4655, 4659, 4663, 4674, 4672)
bra <- c(4753, 4751, 4756, 4761, 4765, 4775)
queretaro_yucatan_nagano_miyagi_wakayama <- c(4664, 4673, 35443, 35427, 35453)
all_mex <- c(4643:4674, 130)
botswana <- 193
all_bra <- c(4750:4776, 135)
gunma <- 35433
if(bun_id== 632){df <- df[(location_id %in% china_locs | location_id %in% india_locs | location_id %in% c(190, 25351, 155, 152)), is_outlier:=1]
                df <- df[location_id == 518, is_outlier:=1]
                df <- df[location_id %in% queretaro_yucatan_nagano_miyagi_wakayama, is_outlier:=1]
                df <- df[location_id %in% mex, is_outlier:=1]
                df <- df[location_id %in% bra, is_outlier:=1]
                df <- df[location_id %in% all_mex & !(age_start==164), is_outlier:=1]
                df <- df[location_id == 193 & age_start > 4, is_outlier:=1]
                df <- df[location_id %in% all_bra & !(age_start==164), is_outlier:=1]
                df <- df[location_id == 35433, is_outlier:=1]
                non_prev <- non_prev[measure=="mtwith" & !(location_id %in% us_china), is_outlier:=1]
                  }

# vsd asd
uttar_pradesh <- c(4873, 43940, 43904)
tibet <- 518
china_india_italy_japan <- c(35453, 496, 4856, 43923, 43887, 35495, 35462, 35425)
if(bun_id== 634){
  df <- df[location_id == 518, is_outlier:=1]
  df <- df[location_id %in% uttar_pradesh, is_outlier:=1]
  df <- df[location_id %in% china_india_italy_japan, is_outlier:=1]
  df <- df[nid==138823, is_outlier:=1]
  non_prev <- non_prev[measure=="mtwith" & !(location_id %in% us_china), is_outlier:=1]
  italy_pol <- c(35494:35514, 86, 53660:53675, 51)
  df <- df[location_id %in% italy_pol & mean > 0.01, is_outlier:=1]
  df <- df[mean > 0.01, is_outlier:=1]
  df <- df[location_id==4761, is_outlier:=1]
  
}

# single ventricle
uttar_pradesh <- c(4873, 43940, 43904)
valle_molise <- c(35495, 35508)
japan <- c(35451, 35443, 35464, 35429)
mex <- c(4646, 4643, 4645, 4660, 4674)
us_china <- c(523:573, 102, 6, 491:521)
if(bun_id== 636){
  df <- df[location_id %in% valle_molise, is_outlier:=1]
  df <- df[location_id %in% uttar_pradesh, is_outlier:=1]
  df <- df[nid==138823, is_outlier:=1]
  df <- df[location_id %in% japan, is_outlier:=1]
  df <- df[location_id %in% mex, is_outlier:=1]
  df <- df[location_id == 109, is_outlier:=1]
  non_prev <- non_prev[measure=="mtwith", is_outlier:=1]
  
}


################################################################################################################################################################################
#### Bring back mortality data #####
non_prev <- as.data.table(non_prev)
duplicate <- non_prev[sex == 'Both']
duplicate[, sex := 'Male']
non_prev[sex == 'Both', sex := 'Female']
non_prev <- rbind(non_prev, duplicate)
df <- rbind(df, non_prev, fill=TRUE)
df <- as.data.table(df)
df <- df[,seq:=NA]
df <- df[sample_size > 1000000, sample_size:=1000000]
df <- df[sample_size > 1000000, standard_error:=NA]
df <- df[sample_size > 1000000, upper:=NA]
df <- df[sample_size > 1000000, lower:=NA]



################################################################################################################################################################################


################################################################################################################################################################################
###### Write out files ###### 
print("got to writing out files")
write.xlsx(df, paste0("FILEPATH/", bun_id, "_crosswalked_outliered_split_data_for_modeling_", step, "_", Sys.Date(), ".xlsx"), sheetName="extraction")
write.xlsx(df, paste0("FILEPATH/", bun_id, "_crosswalked_outliered_split_data_for_modeling_", step, "_CURRENT.xlsx"), sheetName="extraction")

################################################################################################################################################################################


################################################################################################################################################################################
###### Upload data ###### 
upload <- save_crosswalk_version(bundle_version_id=b_v,
                                 data_filepath = paste0("FILEPATH/", bun_id, "_crosswalked_outliered_split_data_for_modeling_", step, "_CURRENT.xlsx"),
                                 description = model_description)

# Save the current (just created) cw v id: 
cv <- data.frame(cw_version_current=upload$crosswalk_version_id)
bv <- fread(paste0("FILEPATH/gbd2020_", step, "_", bun_id, "_CURRENT.csv"))
bv <- bv[, cw_version_current:=NULL]
bv <- bv[, cw_version_best:=NULL]
bv <- bv[, cv:=NULL]
bundle_metadata <- cbind(bv, cv)

ids <- get_elmo_ids(gbd_round_id = 7, decomp_step = 'iterative', bundle_id=bun_id)
ids_best <- ids[is_best==1 & !((model_version_date=="")),]
ids_best <- unique(ids_best)
cw_version_best <- unique(ids_best$crosswalk_version_id)
bundle_metadata <- bundle_metadata[,cw_version_best:=cw_version_best]

write.csv(bundle_metadata, paste0("FILEPATH/gbd2020_", step, "_", bun_id, "_CURRENT.csv"), row.names=F)
write.csv(bundle_metadata, paste0("FILEPATH/gbd2020_", step, "_", bun_id, "_CURRENT.csv"), row.names=F)

###############################################################################################################################################################################


################################################################################################################################################################################
