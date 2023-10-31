# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}


# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx","pbapply","ggplot2")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

version <- "VERSION"

hap.exp.date <- "DATE"

cvd_ages <- seq(25,95,5) #full ages

set.seed(143) # set seed for reproducibility

# ---------------------------Functions & directories ---------------------------------------------

home_dir <- "FILEPATH"

in_dir <- file.path("FILEPATH")

out_dir <- file.path(home_dir,"FILEPATH",version)
dir.create(out_dir,recursive=T)
dir.create(file.path(out_dir,"FILEPATHt"))

graphs_dir <- file.path(home_dir, 'FILEPATH', version)
dir.create(graphs_dir, recursive=T)

source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
locations <- get_location_metadata(35)
source(file.path(central_lib,"FILEPATH/get_covariate_estimates.R")) # pulls covariates, used to get air pollution

'%ni%' <- Negate('%in%')

# ---------------------------Read in data (RR, HAP mapping, OAP exposure)------------------------------------------

# HAP
data.hap <- openxlsx::read.xlsx(paste0(in_dir,"/FILEPATH.xlsm"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.hap) <- openxlsx::read.xlsx(paste0(in_dir,"/FILEPATH.xlsm"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
data.hap <- data.hap[!is.na(ier_source) & is_outlier==0]
data.hap <- data.hap[ier_cause %ni% c("bw","ga","cataract")]

# OAP
data.oap <- openxlsx::read.xlsx("FILEPATH.xlsm",sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.oap) <- openxlsx::read.xlsx(paste0("FILEPATH.xlsm"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
data.oap <- data.oap[is_outlier==0 & !is.na(ier_source)]
data.oap <- data.oap[!is.na(nid)]

# HAP exposure data
hap_exp <- fread(paste0("FILEPATH",hap.exp.date,"/lm_map_",hap.exp.date,".csv"))
hap_exp <- hap_exp[,.(location_id,year_id,median,grouping)] # we only need these columns
hap_exp <- dcast(hap_exp, location_id + year_id ~ grouping, value.var = "median") # make wide by group

# Ambient exposure data
air_pm <- get_covariate_estimates(covariate_id = 106,decomp_step="iterative")
air_pm[,ambient_exp_mean:=mean_value]


# --------------------------- PREP OAP data -------------------------------------
setnames(data.oap, c("ier_cause_measure","oap_conc_mean","oap_conc_sd","oap_conc_5","oap_conc_95","oap_conc_increment"),
         c("outcome","conc_mean","conc_sd","conc_5","conc_95","conc_increment"))

# if IQR is a range, convert to numeric
convert_to_numeric <- function(x){
  iqr <- strsplit(x, "-") # need to make this "--" for dementia
  iqr <- as.numeric(iqr[[1]])
  numeric <- iqr[2]-iqr[1]
  return(numeric)
}

data.oap <- data.oap[grepl("-",oap_conc_iqr), oap_conc_iqr:=convert_to_numeric(oap_conc_iqr)]
data.oap[,oap_conc_iqr:=as.numeric(oap_conc_iqr)]

#estimate mean/sd if missing
data.oap[is.na(conc_mean), conc_mean := oap_conc_median]
data.oap[is.na(conc_sd), conc_sd := oap_conc_iqr/1.35]  #SD can be estimated by IQR/1.35
data.oap[is.na(conc_sd), conc_sd := (oap_conc_max-oap_conc_min)/4]  #SD can be estimated by range/4

#estimate concentration p5/p95 from mean/sd using z if necessary
data.oap[is.na(conc_5), conc_5 := conc_mean - conc_sd * 1.645]
data.oap[is.na(conc_95), conc_95 := conc_mean + conc_sd * 1.645]

#if our estimates for 5th and 95th are outside the range of the study, fix to min/max
data.oap[conc_5 < oap_conc_min, conc_5 := oap_conc_min]
data.oap[conc_95 > oap_conc_max, conc_95 := oap_conc_max]

#if the mean/median ar both missing, set to midpoint of 5th and 95th
data.oap[is.na(conc_mean),conc_mean:=(conc_5+conc_95)/2]

data.oap[,conc_increment:=as.numeric(conc_increment)]

# shift the RRs using p95/p5 range concentration increment
data.oap[, exp_rr := mean ^ ((conc_95-conc_5)/conc_increment)]
data.oap[, exp_rr_lower := lower ^ ((conc_95-conc_5)/conc_increment)]
data.oap[, exp_rr_upper := upper ^ ((conc_95-conc_5)/conc_increment)]

# also generage RRs and betas per 1 unit
data.oap[, exp_rr_unit := mean ^ (1/conc_increment)]
data.oap[, exp_rr_unit_lower := lower ^ (1/conc_increment)]
data.oap[, exp_rr_unit_upper := upper ^ (1/conc_increment)]


# --------------------------- PREP HAP data -------------------------------------

setnames(data.hap, c("ier_cause_measure","oap_conc_mean","oap_conc_sd","oap_conc_5","oap_conc_95","oap_conc_increment"),
         c("outcome","conc_mean","conc_sd","conc_5","conc_95","conc_increment"))

# find midpoint of study
data.hap[,year_id:=round((year_start+year_end)/2)]

# If studies report some measure of HAP (includes both fuel use and ambient), use that
# some HAP studies already have concentration
data.hap[is.na(conc_mean), conc_mean := oap_conc_median]
data.hap[is.na(conc_sd), conc_sd := oap_conc_iqr/1.35]  #SD can be estimated by IQR/1.35
data.hap[is.na(conc_sd), conc_sd := (oap_conc_max-oap_conc_min)/4]  #SD can be estimated by range/4

# estimate concentration p5/p95 from mean/sd using z if necessary
data.hap[is.na(conc_5), conc_5 := conc_mean - conc_sd * 1.645]
data.hap[is.na(conc_95), conc_95 := conc_mean + conc_sd * 1.645]

# if our estimates for 5th and 95th are outside the range of the study, fix to min/max
data.hap[conc_5 < oap_conc_min, conc_5 := oap_conc_min]
data.hap[conc_95 > oap_conc_max, conc_95 := oap_conc_max]

data.hap[,conc_increment:=as.numeric(conc_increment)]

# shift the RRs using p95/p5 range concentration increment
data.hap[!is.na(conc_increment), exp_rr := mean ^ ((conc_95-conc_5)/conc_increment)]
data.hap[!is.na(conc_increment), exp_rr_lower := lower ^ ((conc_95-conc_5)/conc_increment)]
data.hap[!is.na(conc_increment), exp_rr_upper := upper ^ ((conc_95-conc_5)/conc_increment)]

# for categorical hap exposures, no need to shift
data.hap[is.na(conc_increment), exp_rr := mean]
data.hap[is.na(conc_increment), exp_rr_lower := lower]
data.hap[is.na(conc_increment), exp_rr_upper := upper]

# If studies don't report anything about HAP (what the actual PM2.5 concentration is), we use our ambient and HAP estimates
# merge on ambient and hap exposures to calculate 5th and 95th percentiles
# 5th percentile is ambient exposure; 95th percentile is the ambient + HAP exposure

# merge on ambient first
data.hap <- merge(data.hap,air_pm[,.(location_id,year_id,ambient_exp_mean)], by=c("year_id","location_id"), all.x=T)

# merge on hap exposure
data.hap[year_id<min(hap_exp$year_id),year_id:=min(hap_exp$year_id)]
data.hap <- merge(data.hap,hap_exp,by= c("year_id","location_id"),all.x=T)

# assign hap exposure based on age and gender
data.hap[age_mean<18, household_exp_mean:=child]
data.hap[age_mean>18 & sex=="Female", household_exp_mean:=female]
data.hap[age_mean>18 & sex=="Male", household_exp_mean:=male]
data.hap[age_mean>18 & sex=="Both", household_exp_mean:=((female+male)/2)]

data.hap[is.na(household_exp_mean) & (((age_start+age_end)/2)<18), household_exp_mean:=child]
data.hap[is.na(household_exp_mean) & (((age_start+age_end)/2)>18) & sex=="Female", household_exp_mean:=female]
data.hap[is.na(household_exp_mean) & (((age_start+age_end)/2)>18) & sex=="Male", household_exp_mean:=male]
data.hap[is.na(household_exp_mean) & (((age_start+age_end)/2)>18) & sex=="Both", household_exp_mean:=((female+male)/2)]

# setting multi country studies to global average for gbd2017
data.hap[location_id==1 & year_id==2000,ambient_exp_mean:=46]
data.hap[location_id==1 & year_id==2008,ambient_exp_mean:=51]

data.hap[,conc_5:=as.numeric(conc_5)]
data.hap[,conc_95:=as.numeric(conc_95)]
data.hap[is.na(conc_95),conc_95:=ambient_exp_mean+household_exp_mean]
data.hap[is.na(conc_5),conc_5:=ambient_exp_mean]

# also shift the RRs and betas such that they are for one unit
data.hap[!is.na(conc_increment), exp_rr_unit := mean ^ (1/conc_increment)]
data.hap[!is.na(conc_increment), exp_rr_unit_lower := lower ^ (1/conc_increment)]
data.hap[!is.na(conc_increment), exp_rr_unit_upper := upper ^ (1/conc_increment)]

# for categorical hap exposures, shift by range of exposed to unexposed
data.hap[is.na(conc_increment), exp_rr_unit := mean ^ (1/(conc_95-conc_5)) ]
data.hap[is.na(conc_increment), exp_rr_unit_lower := lower ^ (1/(conc_95-conc_5)) ]
data.hap[is.na(conc_increment), exp_rr_unit_upper := upper ^ (1/(conc_95-conc_5)) ]

# set the mean/median to be the midpoint
data.hap[is.na(conc_mean),conc_mean:=(conc_5+conc_95)/2]


#  ----------------------- Combine datasets  ------------------------------------------

# merge oap and hap
data <- rbind(data.oap,data.hap,fill=T,use.names=T)
# data <- data.oap # for dementia test

#generate log_rr and log_se
data[,log_rr:=log(exp_rr)]
data[,log_rr_lower:=log(exp_rr_lower)]
data[,log_rr_upper:=log(exp_rr_upper)]
data[,log_se:=(log_rr_upper-log_rr_lower)/3.92]

#generate log_rr and log_se
data[,log_rr_unit:=log(exp_rr_unit)]
data[,log_rr_unit_lower:=log(exp_rr_unit_lower)]
data[,log_rr_unit_upper:=log(exp_rr_unit_upper)]
data[,log_unit_se:=(log_rr_unit_upper-log_rr_unit_lower)/3.92]

# rename variables
setnames(data,c("conc_5","conc_95"),c("conc_den","conc"))

data <- data[!is.na(log_se)]

# create age variable to represent median age at followup

# estimate average follow up
data[duration_fup_units=="weeks",value_of_duration_fup:=value_of_duration_fup/52]
data[duration_fup_units=="months",value_of_duration_fup:=value_of_duration_fup/12]

data[duration_fup_measure %in% c("mean","median"),median_fup:=value_of_duration_fup]

# calculates an average ratio of median follow up to study length to calculate median follow up when it is missing
fup_ratio <- data[year_end-year_start>=median_fup,(median_fup)/(year_end-year_start)] %>% unique() %>% mean(na.rm=T)
data[is.na(median_fup),median_fup:=(year_end-year_start)*fup_ratio]

data[is.na(age_mean),age_mean:=(age_start+age_end)/2]

# this is an estimate of average age over the whole cohort. It takes the median age and half of median followup
data[,median_age_fup:=age_mean+median_fup/2]

# create child/adult indicator for LRI
data[ier_cause=="lri",child:=0]
data[ier_cause=="lri" & age_end<=5,child:=1]

# mortality v incidence variable
# assuming mortality unless evidence otherwise
data[,incidence:=0]
data[grep("incidence",outcome,ignore.case = T),incidence:=1]

# fill missingness in follow_up covariate with maximum
data[is.na(cv_selection_bias),cv_selection_bias:=2]

 # ----------------------- Recode covs for compatibility w/ new MRBRT--------------------------
# make a linear_exp and a log_exp
data[,linear_exp:=conc-conc_den]
data[,log_exp:=log(1+linear_exp)]

# duration follow-up (these have already been converted so their units are "years")
data[,cv_duration_fup:=ifelse(value_of_duration_fup<median(data$value_of_duration_fup,na.rm=T),1,0)]
data[is.na(cv_duration_fup),cv_duration_fup:=1] # if duration follow-up is missing, count is as a 1

# confounding uncontrolled
data[,cv_confounding_uncontrolled:=ifelse(cv_confounding_uncontrolled==2,1,0)]

# selection bias
data[,cv_selection_bias:=ifelse(cv_selection_bias==2,1,0)]

# ier_source covariate
data[,cv_hap:=ifelse(ier_source=="HAP",1,0)]

# make a covariate for mean age
data[,cv_age_mean:=age_mean]


cov_names <- c("cv_duration_fup", "cv_subpopulation","cv_exposure_population", "cv_exposure_selfreport", "cv_exposure_study", "cv_outcome_selfreport", 
               "cv_outcome_unblinded", "cv_reverse_causation", "cv_confounding_nonrandom", 
               "cv_confounding_uncontrolled", "cv_selection_bias","cv_hap","cv_age_mean")

length(data)

# remove covariates that all have the same values
for (name in cov_names){
  if(nrow(unique(data[,..name]))==1){
    data[,(name):=NULL]
  }
}

length(data)

cov_names <- names(data)[grep("cv",names(data))]


#  ----------------------- Weight sample sizes for studies with multiple observations --------------------------

# for multiple datapoints from the same study, we want to weight their SEs by sqrt(n)
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

# make a data.table for all the ages so we can use it for covariate selection later
# cvd_all_ages <- data.table()

for(this.cause in unique(data$ier_cause)){
  out <- data[ier_cause==this.cause,.(underlying_nid,nid,source_type,location_name,location_id,ihme_loc_id,
                                      study,ier_source,ier_cause,year_start,year_end,year_id,measure,
                                      conc_increment,conc_mean,conc_den,conc,
                                      exp_rr,exp_rr_lower,exp_rr_upper,log_rr,log_rr_lower,log_rr_upper,log_se,
                                      exp_rr_unit,exp_rr_unit_lower,exp_rr_unit_upper,log_rr_unit,log_rr_unit_lower,log_rr_unit_upper,log_unit_se,
                                      median_age_fup,incidence,child,
                                      new_gbd2020,log_se_weighted,linear_exp,log_exp,
                                      cv_subpopulation,cv_exposure_population,cv_exposure_selfreport,cv_exposure_study,
                                      cv_outcome_selfreport,cv_outcome_unblinded,cv_confounding_uncontrolled,cv_selection_bias,
                                      cv_duration_fup,cv_hap,cv_age_mean)]
  
  
  write.csv(out,paste0(out_dir,"/",this.cause,".csv"),row.names=F)
  
  
  
  print(paste0("Done with ", this.cause))
}



