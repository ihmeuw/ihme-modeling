#***************************************************************************************************************************************************************
# SECONDHAND SMOKE - GBD 2020
# Purpose: Prepare relative risk data for running MR-BRT models
#***************************************************************************************************************************************************************


#------------------Set-up--------------------------------------------------

#----CONFIG-------------------------------------------------------------------------------------------------------------
### qsub args
args <- commandArgs()[-(1:3)]
print(args)
hh <- args[1]
print(hh)
slots <- as.numeric(args[2])
print(slots)

### runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

### set scientific notation
options(scipen=15)

# load packages, install if missing
library(ggplot2)
library(data.table)
library(openxlsx)
library(pbapply)
library(dplyr)

set.seed(143)
'%ni%' <- Negate('%in%')
# ---------------------------Functions & directories ---------------------------------------------

home_dir <- "FILEPATH"
in_dir <- file.path("FILEPATH") #Where extractions are savedd

out_dir <- file.path(paste0(home_dir,"FILEPATH"))
dir.create(out_dir,recursive=T)

locations <- get_location_metadata(35, gbd_round_id = 7)

# ---------------------------Read in data------------------------------------------

# SHS
data.shs <- openxlsx::read.xlsx(paste0(in_dir,"/extractions.xlsm"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.shs) <- openxlsx::read.xlsx(paste0(in_dir,"/extractions.xlsm"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
data.shs <- data.shs[is_outlier==0 & !is.na(ier_source)]

# Ambient exposure data
air_pm <- get_covariate_estimates(covariate_id = 106,decomp_step="iterative")
air_pm[,ambient_exp_mean:=mean_value]

# --------------------------- PREP SHS data -------------------------------------
# create index variable for vectorized functions
data.shs[, index := seq_len(.N)]

# Updating to pull in cigs per smoker and calculate EXCESS pm2.5
cigs.ps <- lapply(paste0("FILEPATH",
                         setdiff(c(locations[level==3,location_id],data.shs$location_id),1),".csv"),fread) %>% rbindlist

#store draw colnames in vector
draw.colnames <- paste0("draw_", 0:999)

#merge on location names
cigs.ps <- merge(cigs.ps, locations[, c('location_name', 'location_id'), with=F])
#pm25 <- merge(pm25, locations[, c('location_name', 'location_id'), with=F])

#define midyear
data.shs[, year_id := round((as.integer(year_end) + year_start)/2, 0)]

#set location/year to Global if it's unknown or not in cigs ps dataset
data.shs$location_name <- NULL #old extracted location names
data.shs <- merge(data.shs, locations[, c('location_name', 'location_id')], all.x = T)
data.shs$location_name <- as.character(data.shs$location_name)
data.shs[year_id<min(cigs.ps$year_id), year_id := (min(cigs.ps$year_id))] 

# fill out with aggregates for datapoints that location/year is unknown (currently using average to collapse)
# unknown location
cigs.ps.global <- cigs.ps[, lapply(.SD, mean), by=year_id, .SDcols=c(draw.colnames)]
cigs.ps.global[, location_id := 1]

#combine
cigs.ps <- rbind(cigs.ps, cigs.ps.global, fill=T)

# set study cigarettes per smoker from which to derive ratios
study.cigs.ps.semple <- cigs.ps[location_name=="Scotland" & year_id %in% 2009:2013,c(draw.colnames),with=F] %>% as.matrix %>% mean() # using the average 2009-2013 Scotland cigarettes per smoker, as these are the years of the studies used in Semple's 2014 paper where we have drawn the distribution of PM2.5 from SHS
print(study.cigs.ps.semple)

#distribution of those exposed to SHS
log.sd <- (log(111)-log(10))/(2*qnorm(.75)) # formula calculates the log SD from Q3 and Q1 reported by Semple and assumption of lognormal
log.se <- log.sd/sqrt(93) #divide by the sqrt of reported sample size to get the SE
log.median <- log(31)
pm.dist.exposed <- exp(rnorm(1000, mean = log.median, sd = log.se)) # using a lognormal distribution to get draws of exposed

#distribution of those not exposed to SHS
log.sd.unexposed <- (log(6.5)-log(2))/(2*qnorm(.75)) # formula calculates the log SD from Q3 and Q1 reported by Semple and assumption of lognormal
log.se.unexposed <- log.sd.unexposed/sqrt(17) #divide by the sqrt of reported sample size to get the SE
log.median.unexposed <- log(3)
pm.dist.unexposed <- exp(rnorm(1000, mean = log.median.unexposed, sd = log.se.unexposed)) # using a lognormal distribution to get draws of unexposed

pm.excess.per.cig <- (pm.dist.exposed-pm.dist.unexposed)/study.cigs.ps.semple
mean(pm.excess.per.cig)

#scale draws of cigs.per.smoker by draws of pm2.5 per cigarette (PM2.5 * cigarettes smoked in each location/year)
cigs.ps[,c(draw.colnames):=Map("*",.SD,pm.excess.per.cig), .SDcols=c(draw.colnames)]

#merge on to the shs data
data.shs <- merge(data.shs, cigs.ps[,-c("location_name")], by=c('location_id', 'year_id'), all.x=T, all.y=FALSE)
#data.shs <- merge(data.shs, pm25[,-c("location_name")], by=c('location_id', 'year_id'), all.x=T, all.y=FALSE)

data.shs$conc_5 <- as.numeric(data.shs$conc_5)
data.shs$conc_95 <- as.numeric(data.shs$conc_95)

# based on Arden's previously defined qualitiative measure of intensity (current conc_95), pull a draw from the
# distribution of PM per cigarette, and then apply the country level of cigarettes per smoker
# this is probably because studies were extracted Arden's way for his study and then we changed the strategy (but didn't change the extractions)
data.shs[shs_cat == 20, conc_95 := quantile(.SD, .25), .SDcols=draw.colnames, by=index] # low (20) is the 25th percentile of the distribution
data.shs[shs_cat == 35, conc_95 := rowMeans(.SD), .SDcols=draw.colnames, by=index] # medium (35) is the median of the distribution (most of the studies extracted this way are 35)
data.shs[shs_cat == 50, conc_95 := quantile(.SD, .75), .SDcols=draw.colnames, by=index] # high (50) is the 75th percentile of the distribution
data.shs[is.na(shs_cat), conc_95 := rowMeans(.SD), .SDcols=draw.colnames, by=index]

data.shs[,c(draw.colnames):=NULL]

# if study publishes by cigs per day, calculate concentration accordingly
data.shs[is.na(shs_exposure_upper),shs_exposure_upper:=1.5*shs_exposure_lower] #based on smoking team, max of highest ctegory not provided, sub with 1.5 times lower bound
data.shs[!is.na(shs_exposure_upper), conc_95 := mean(pm.excess.per.cig) * (shs_exposure_upper+shs_exposure_lower)/2] #multiply calculated pm per cig by the midpoint of the category

# merge on ambient  exposures to calculate counterfactual
data.shs[year_id<min(air_pm$year_id), year_id := min(air_pm$year_id)]
data.shs <- merge(data.shs,air_pm[,.(location_id,year_id,ambient_exp_mean)], by=c("year_id","location_id"), all.x=T)

# replace global studies with global average ambient #TODO FIX
data.shs[location_name=="Global" & year_id %in% c(1999,2000),ambient_exp_mean:=46]
data.shs[location_name=="Global" & year_id %in% c(2004),ambient_exp_mean:=49]

data.shs[,conc_5:=ambient_exp_mean]
data.shs[,exp_linear := conc_95] 
data.shs[,ref := 0]
data.shs[,conc_95:=conc_95+conc_5]

## Set lower conc to TMREL average
data.shs[,conc_mean:= (conc_95+conc_5)/2]

# for SHS no need to shift
data.shs[, exp_rr := mean]
data.shs[, exp_rr_lower := lower]
data.shs[, exp_rr_upper := upper]

#----------------------- Combine datasets  ------------------------------------------

data <- copy(data.shs)

data$exp_rr <- as.numeric(data$exp_rr)
data$exp_rr_lower <- as.numeric(data$exp_rr_lower)
data$exp_rr_upper <- as.numeric(data$exp_rr_upper)
data[,exp_se:=(exp_rr_upper-exp_rr_lower)/3.92]

#generate log_rr and log_se
data[,log_rr:=log(exp_rr)]
data[,log_rr_lower:=log(exp_rr_lower)]
data[,log_rr_upper:=log(exp_rr_upper)]
data[,log_se:=(log_rr_upper-log_rr_lower)/3.92]

# rename variables
setnames(data,c("conc_5","conc_95", "conc_mean"),c("air_ref","air_conc", "air_mean"))
data <- data[!is.na(log_se)]

# estimate average follow up
data$value_of_duration_fup <- as.numeric(data$value_of_duration_fup)
data[duration_fup_units=="weeks",value_of_duration_fup:=value_of_duration_fup/52]
data[duration_fup_units=="months",value_of_duration_fup:=value_of_duration_fup/12]
data[duration_fup_measure %in% c("mean","median"),median_fup:=value_of_duration_fup]

# calculates an average ratio of median follow up to study length to calculate median follow up when it is missing
fup_ratio <- data[year_end-year_start>=median_fup,(median_fup)/(year_end-year_start)] %>% unique() %>% mean(na.rm=T)
data[is.na(median_fup),median_fup:=(year_end-year_start)*fup_ratio]

data$age_start <- as.numeric(data$age_start)
data$age_end<- as.numeric(data$age_end)

#In case we want to use age_start and age_end to model (CVD coutcomes), we estimate the missing age_start and age_end using age_mean and age_sd with 95% CI
data[is.na(age_start) & is.na(age_end) & is.na(age_sd)]
data[, age_start_pred := age_mean-1.96*age_sd]
data[, age_end_pred := age_mean+1.96*age_sd]

# use age_start_pred and age_end_pred if age_start or age_end is missing
data[is.na(age_start), age_start := age_start_pred]
data[is.na(age_end), age_end := age_end_pred]

# create child/adult indicator for LRI
data[ier_cause=="lri",cv_child:=0]
data[ier_cause=="lri" & age_end<=5,cv_child:=1]

# mortality v incidence variable
# assuming mortality unless evidence otherwise
data[,cv_incidence:=0]
data[grep("incidence",outcome,ignore.case = T),cv_incidence:=1]

# fill missingness in follow_up covariate with maximum
data[is.na(cv_selection_bias),cv_selection_bias:=2]


#  ----------------------- Recode covs for compatibility w/ new MRBRT--------------------------

# duration follow-up (these have already been converted so their units are "years")
data[,cv_duration_fup:=ifelse(value_of_duration_fup<median(data$value_of_duration_fup,na.rm=T),1,0)]
data[is.na(cv_duration_fup),cv_duration_fup:=1] # if duration follow-up is missing, count is as a 1

# confounding uncontrolled
setnames(data, "cv_counfounding.uncontroled","cv_confounding_uncontrolled")
data[cv_confounding_uncontrolled==20,cv_confounding_uncontrolled:=0] # one of the observations was extracted with an error
data[,cv_confounding_uncontrolled:=ifelse(cv_confounding_uncontrolled==2,1,0)]

# selection bias
data[,cv_selection_bias:=ifelse(cv_selection_bias==2,1,0)]

cov_names <- c("cv_duration_fup", "cv_subpopulation","cv_exposure_population", "cv_exposure_selfreport", "cv_exposure_study", "cv_outcome_selfreport", 
               "cv_outcome_unblinded", "cv_reverse_causation", "cv_confounding_nonrandom", 
               "cv_confounding_uncontrolled", "cv_selection_bias", "cv_child", "cv_incidence")

length(data)

# remove covariates that all have the same values
for (name in cov_names){
  if(nrow(unique(data[,..name]))==1){
    data[,(name):=NULL]
  }
}

length(data)

#----------------------- Weight sample sizes for studies with multiple observations --------------------------

weight_ss <- function(n){
  
  # make a temporary dataset for each nid
  temp <- data[nid==n]
  
  # if sample size is empty, assign it to 1 (so we can avoid NAs; we'll put it back at the end)
  temp[is.na(sample_size),sample_size:=1]
  
  if (nrow(temp)>1){
    
    # if there are >1 observation, loop through causes
    causes <- unique(temp$ier_cause)
    for(c in causes){
      
      # do each sample size separately (because we only want to weight observations that have the same sample size)
      ss <- unique(temp[ier_cause==c,sample_size])
      for (s in ss){
        
        n_observations <- nrow(temp[ier_cause==c & sample_size==s])
        temp[ier_cause==c & sample_size==s,log_se_weighted:=(log_se*sqrt(n_observations))]
      }
    }
    
  } else {
    
    # if there is only 1 observation for that nid, we can skip the whole weighting process
    temp[,log_se_weighted:=log_se]
  }
  
  # put back the NA for sample size
  temp[sample_size==1,sample_size:=NA]
  
  return(temp)
}

temp <- pblapply(unique(data$nid),weight_ss,cl=4) %>% rbindlist

# plot to check weighted vs original SEs
plot <- ggplot(temp, aes(x = log_se_weighted, y = log_se, color = nid)) +
  geom_point()+
  geom_abline(slope = 1, intercept = 0)
print(plot)

data <- temp

#  ----------------------- Save final RR datasets -------------------------------------------

# save each cause

for(this.cause in unique(data$ier_cause)){
  out <- data[ier_cause==this.cause,.(underlying_nid,nid, field_citation_value,study,source_type,location_name,location_id,ihme_loc_id,site_memo,
                                      ier_source,ier_cause,outcome,case_definition,sex,year_start,year_end,year_id,age_start, age_end, age_mean, age_sd,measure,
                                      air_mean,air_ref,air_conc, exp_linear, ref,
                                      exp_rr,exp_rr_lower,exp_rr_upper,exp_se,log_rr,log_rr_lower,log_rr_upper,log_se,
                                      log_se_weighted, new_gbd2020, cv_duration_fup, 
                                      cv_exposure_selfreport, cv_exposure_study, cv_outcome_selfreport, 
                                      cv_outcome_unblinded, 
                                      cv_confounding_uncontrolled, cv_selection_bias, cv_child, cv_incidence
  )]
                                    
                                    
  write.csv(out,paste0(out_dir,"/",this.cause,".csv"),row.names=F)
  
}
  