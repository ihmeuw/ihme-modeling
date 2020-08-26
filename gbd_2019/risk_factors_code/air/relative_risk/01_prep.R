
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 8/7/2019
# Purpose: Prep MR-BeRT
#          
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
} else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
}

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# Directories -------------------------------------------------------------
version <- 45
hap.exp.date <- "021119" # which version of HAP mapping model?
cvd_ages <- seq(25,95,5) # full ages
terminal.age <- 110 # used for calculating age-specific CVD RRs

home_dir <- "FILEPATH"
in_dir <- file.path(home_dir,"FILEPATH")
out_dir <- file.path(home_dir,"FILEPATH",version)
dir.create(out_dir,recursive=T)
dir.create(file.path(out_dir,"FILEPATH"))
graphs_dir <- file.path(home_dir, 'FILEPATH', version)
dir.create(graphs_dir, recursive=T)

# this pulls the current locations list
source(file.path(central_lib,"FILEPATH.R"))
# this pulls covariates, used to pull air pollution
source(file.path(central_lib,"FILEPATH.R"))

# this pulls the general misc helper functions
central.function.dir <- file.path(h_root, "FILEPATH")
file.path(central.function.dir, "FILEPATH.R") %>% source()

# load locations
locations <- get_location_metadata(35)

'%ni%' <- Negate('%in%')
set.seed(143)

# Load data----------------------------------------------------------------

# Household air pollution
data.hap <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.hap) <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
data.hap <- data.hap[!is.na(ier_source) & is_outlier==0]
data.hap <- data.hap[ier_cause %ni% c("bw","ga","cataract")]

# Outdoor air pollution
data.oap <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.oap) <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
data.oap <- data.oap[is_outlier==0 & !is.na(ier_source)]

# Secondhand smoke
data.shs <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", startRow=3, colNames=F, skipEmptyRows = T, skipEmptyCols = F) %>% as.data.table
names(data.shs) <- openxlsx::read.xlsx(paste0(in_dir,"FILEPATH.xlsm"), sheet="extraction", colNames=T, skipEmptyCols = F, rows=1) %>% names
data.shs <- data.shs[is_outlier==0 & !is.na(ier_source)]
# Active smoking
data.as <- fread(file.path(in_dir,"FILEPATH.csv"))

# Read in hap exposure data
hap_exp <- fread(paste0(home_dir,"FILEPATH",hap.exp.date,"FILEPATH",hap.exp.date,".csv"))
# and ambient exposure data
air_pm <- get_covariate_estimates(covariate_id = 106,decomp_step="iterative")
air_pm[,ambient_exp_mean:=mean_value]

# Prep OAP data ----------------------------------------------------------------
setnames(data.oap, c("ier_cause_measure","oap_conc_mean","oap_conc_sd","oap_conc_5","oap_conc_95","oap_conc_increment"),
         c("outcome","conc_mean","conc_sd","conc_5","conc_95","conc_increment"))

# estimate mean/sd if missing
data.oap[is.na(conc_mean), conc_mean := oap_conc_median]
data.oap[is.na(conc_sd), conc_sd := oap_conc_iqr/1.35]  #SD can be estimated by IQR/1.35
data.oap[is.na(conc_sd), conc_sd := (oap_conc_max-oap_conc_min)/4]  #SD can be estimated by range/4

# estimate concentration p5/p95 from mean/sd using z if necessary
data.oap[is.na(conc_5), conc_5 := conc_mean - conc_sd * 1.645]
data.oap[is.na(conc_95), conc_95 := conc_mean + conc_sd * 1.645]

# if our estimates for 5th and 95th are outside the range of the study, fix to min/max
data.oap[conc_5 < oap_conc_min, conc_5 := oap_conc_min]
data.oap[conc_95 > oap_conc_max, conc_95 := oap_conc_max]

# if the mean/median ar both missing, set to midpoint of 5th and 95th
data.oap[is.na(conc_mean),conc_mean:=(conc_5+conc_95)/2]

# if the 5th percentile is less than the tmrel, set it to the tmrel
# data.oap[conc_5 < tmrel.mean, conc_5 := tmrel.mean]

data.oap[,conc_increment:=as.numeric(conc_increment)]

# shift the RRs using p95/p5 range concentration increment
data.oap[, exp_rr := mean ^ ((conc_95-conc_5)/conc_increment)]
data.oap[, exp_rr_lower := lower ^ ((conc_95-conc_5)/conc_increment)]
data.oap[, exp_rr_upper := upper ^ ((conc_95-conc_5)/conc_increment)]

# also generage RRs and betas per 1 unit
# shift the RRs using p95/p5 range concentration increment
data.oap[, exp_rr_unit := mean ^ (1/conc_increment)]
data.oap[, exp_rr_unit_lower := lower ^ (1/conc_increment)]
data.oap[, exp_rr_unit_upper := upper ^ (1/conc_increment)]

# Prep HAP data ------------------------------------------------------------

setnames(data.hap, c("ier_cause_measure","oap_conc_mean","oap_conc_sd","oap_conc_5","oap_conc_95","oap_conc_increment"),
         c("outcome","conc_mean","conc_sd","conc_5","conc_95","conc_increment"))

# find midpoint of study
data.hap[,year_id:=round((year_start+year_end)/2)]

# some HAP studies already have concentration
# estimate mean/sd if missing
data.hap[is.na(conc_mean), conc_mean := oap_conc_median]
data.hap[is.na(conc_sd), conc_sd := oap_conc_iqr/1.35]  #SD can be estimated by IQR/1.35
data.hap[is.na(conc_sd), conc_sd := (oap_conc_max-oap_conc_min)/4]  #SD can be estimated by range/4

# estimate concentration p5/p95 from mean/sd using z if necessary
data.hap[is.na(conc_5), conc_5 := conc_mean - conc_sd * 1.645]
data.hap[is.na(conc_95), conc_95 := conc_mean + conc_sd * 1.645]

# if our estimates for 5th and 95th are outside the range of the study, fix to min/max
data.hap[conc_5 < oap_conc_min, conc_5 := oap_conc_min]
data.hap[conc_95 > oap_conc_max, conc_95 := oap_conc_max]

# if the 5th percentile is less than the tmrel, set it to the tmrel
# data.hap[conc_5 < tmrel.mean, conc_5 := tmrel.mean]

data.hap[,conc_increment:=as.numeric(conc_increment)]

# shift the RRs using p95/p5 range concentration increment
data.hap[!is.na(conc_increment), exp_rr := mean ^ ((conc_95-conc_5)/conc_increment)]
data.hap[!is.na(conc_increment), exp_rr_lower := lower ^ ((conc_95-conc_5)/conc_increment)]
data.hap[!is.na(conc_increment), exp_rr_upper := upper ^ ((conc_95-conc_5)/conc_increment)]

# for categorical hap exposures, no need to shift
data.hap[is.na(conc_increment), exp_rr := mean]
data.hap[is.na(conc_increment), exp_rr_lower := lower]
data.hap[is.na(conc_increment), exp_rr_upper := upper]

# merge on ambient and hap exposures to calculate 5th and 95th percentiles
data.hap <- merge(data.hap,air_pm[,.(location_id,year_id,ambient_exp_mean)], by=c("year_id","location_id"), all.x=T)
data.hap[year_id<min(hap_exp$year_id),year_id:=min(hap_exp$year_id)]
data.hap <- merge(data.hap,hap_exp[,.(location_id,year_id,predict_pm)],by= c("year_id","location_id"),all.x=T)
setnames(data.hap,"predict_pm","household_exp_mean")

# set multi-country studies to global average for gbd2019 (for now)
data.hap[location_id==1 & year_id==2000,ambient_exp_mean:=46]
data.hap[location_id==1 & year_id==2008,ambient_exp_mean:=51]

data.hap[,conc_5:=as.numeric(conc_5)]
data.hap[,conc_95:=as.numeric(conc_95)]
data.hap[is.na(conc_95),conc_95:=ambient_exp_mean+household_exp_mean]
data.hap[is.na(conc_5),conc_5:=ambient_exp_mean]

# also shift the RRs and betas such that they are for a single unit
# shift the RRs using p95/p5 range concentration increment
data.hap[!is.na(conc_increment), exp_rr_unit := mean ^ (1/conc_increment)]
data.hap[!is.na(conc_increment), exp_rr_unit_lower := lower ^ (1/conc_increment)]
data.hap[!is.na(conc_increment), exp_rr_unit_upper := upper ^ (1/conc_increment)]

# for categorical hap exposures, shift by range of exposed to unexposed
data.hap[is.na(conc_increment), exp_rr_unit := mean ^ (1/(conc_95-conc_5)) ]
data.hap[is.na(conc_increment), exp_rr_unit_lower := lower ^ (1/(conc_95-conc_5)) ]
data.hap[is.na(conc_increment), exp_rr_unit_upper := upper ^ (1/(conc_95-conc_5)) ]

#set the mean/median to be the midpoint
data.hap[is.na(conc_mean),conc_mean:=(conc_5+conc_95)/2]

# Prep SHS data ----------------------------------------------------------------
# create index variable for vectorized functions
data.shs[, index := seq_len(.N)]

# updating to pull in cigs per smoker and calculate EXCESS pm2.5
cigs.ps <- lapply(paste0("FILEPATH",
                         setdiff(c(locations[level==3,location_id],data.shs$location_id),1),".csv"),fread) %>% rbindlist

cigs.ps[,c("age_group_id","sex_id","V1") := NULL]

# store draw colnames in vector
draw.colnames <- paste0("draw_", 0:999)

# merge on location names
cigs.ps <- merge(cigs.ps, locations[, c('location_name', 'location_id'), with=F])

# define midyear
data.shs[, year_id := round((as.integer(year_end) + year_start)/2, 0)]

# set year to min if below range
data.shs[year_id<min(cigs.ps$year_id), year_id := (min(cigs.ps$year_id))] 

# fill out with aggregates for datapoints with unknown location/year (currently using average to collapse)
# unknown location
cigs.ps.global <- cigs.ps[, lapply(.SD, mean), by=year_id, .SDcols=c(draw.colnames)]
cigs.ps.global[, location_id := 1]

# combine
cigs.ps <- rbind(cigs.ps, cigs.ps.global, fill=T)

# calculate draws of the PM2.5 per cigarette using information from Semple et al (2014)
# set study cigarettes per smoker from which to derive ratios
study.cigs.ps.semple <- cigs.ps[location_name=="Scotland" & year_id %in% 2009:2013,c(draw.colnames),with=F] %>% as.matrix %>% mean() # using the average 2009-2013 Scotland cigarettes per smoker, as these are the years of the studies used in Semple's 2014 paper where we have drawn the distribution of PM2.5 from SHS

# calculate distribution of those exposed to SHS
log.sd <- (log(111)-log(10))/(2*qnorm(.75)) # formula calculates the log SD from Q3 and Q1 reported by Semple and assumption of lognormal
log.se <- log.sd/sqrt(93) # divide by the sqrt of reported sample size to get the SE
log.median <- log(31)
pm.dist.exposed <- exp(rnorm(1000, mean = log.median, sd = log.se)) # use a lognormal distribution to get draws of exposed

# calculate distribution of those not exposed to SHS
log.sd.unexposed <- (log(6.5)-log(2))/(2*qnorm(.75)) # formula calculates the log SD from Q3 and Q1 reported by Semple and assumption of lognormal
log.se.unexposed <- log.sd.unexposed/sqrt(17) # divide by the sqrt of reported sample size to get the SE
log.median.unexposed <- log(3)
pm.dist.unexposed <- exp(rnorm(1000, mean = log.median.unexposed, sd = log.se.unexposed)) # use a lognormal distribution to get draws of unexposed

pm.excess.per.cig <- (pm.dist.exposed-pm.dist.unexposed)/study.cigs.ps.semple

# scale draws of cigs.per.smoker by draws of pm2.5 per cigarette  
cigs.ps[,c(draw.colnames):=Map("*",.SD,pm.excess.per.cig), .SDcols=c(draw.colnames)]

# merge on to the shs data
data.shs <- merge(data.shs, cigs.ps[,-c("location_name")], by=c('location_id', 'year_id'), all.x=T, all.y=FALSE)

data.shs$conc_5 <- as.numeric(data.shs$conc_5)
data.shs$conc_95 <- as.numeric(data.shs$conc_95)

# Based on previously defined qualitiative measure of intensity (current conc_95), pull draws from the
# distribution of PM per cigarette, and then apply country-specific level of cigarettes per smoker
data.shs[shs_cat == 20, conc_95 := quantile(.SD, .25), .SDcols=draw.colnames, by=index]
data.shs[shs_cat == 35, conc_95 := rowMeans(.SD), .SDcols=draw.colnames, by=index]
data.shs[shs_cat == 50, conc_95 := quantile(.SD, .75), .SDcols=draw.colnames, by=index]
data.shs[is.na(shs_cat), conc_95 := rowMeans(.SD), .SDcols=draw.colnames, by=index]

data.shs[,c(draw.colnames):=NULL]

# if study publishes by cigs per day, calculate concentration accordingly
data.shs[is.na(shs_exposure_upper),shs_exposure_upper:=1.5*shs_exposure_lower] # based on smoking team, if max of the highest category not provided, sub with 1.5 times lower bound
data.shs[!is.na(shs_exposure_upper), conc_new := mean(pm.excess.per.cig) * (shs_exposure_upper+shs_exposure_lower)/2] # multiply calculated pm per cig by the midpoint of the category

# merge on ambient exposures to calculate counterfactual
data.shs[year_id<min(air_pm$year_id), year_id := min(air_pm$year_id)]
data.shs <- merge(data.shs,air_pm[,.(location_id,year_id,ambient_exp_mean)], by=c("year_id","location_id"), all.x=T)

# replace global studies with global average ambient (for now)
data.shs[location_name=="Global" & year_id %in% c(1999,2000),ambient_exp_mean:=46]
data.shs[location_name=="Global" & year_id %in% c(2004),ambient_exp_mean:=49]

# set concentration
data.shs[,conc_5:=ambient_exp_mean]
data.shs[,conc_95:=conc_95+conc_5]
data.shs[,conc_mean:= (conc_95+conc_5)/2]

# for SHS no need to shift
data.shs[, exp_rr := mean]
data.shs[, exp_rr_lower := lower]
data.shs[, exp_rr_upper := upper]

# calculate unit RRs
data.shs[, exp_rr_unit := mean ^ (1/(conc_95-conc_5)) ]
data.shs[, exp_rr_unit_lower := lower ^ (1/(conc_95-conc_5)) ]
data.shs[, exp_rr_unit_upper := upper ^ (1/(conc_95-conc_5)) ]


# Prep AS data -----------------------------------------------------------------

# use custom function to replace old smoking team cause names with IER cause names
data.as <- findAndReplace(data.as,
                          input.vector=c("lung_cancer","ihd","diabetes","stroke","copd"),
                          output.vector=c("neo_lung","cvd_ihd","t2_dm","cvd_stroke","resp_copd"),
                          input.variable.name="cause",
                          output.variable.name="cause")

# set upper limit of cigs/day to be 1.5 times the lower for top category (based on smoking team's method)
data.as[cig_upper==999,cig_upper:=1.5*cig_lower]
data.as[,cig_midpoint:=(cig_upper+cig_lower)/2]

# active smoking concentrations have been given in # of cigarettes
# convert them to PM2.5 using method  described in NAMES's email on 11/19/14:
# CONVERSION:  666.6 ug/m3 = 1 cig/day (12ng of PM2.5 per 1 cig; average adult breathing rate of 18 m3/day)
# 12,000 ug/1cig/(18 m3.day) = 666.6  ug/m3 per day)
pm.per.cig <- 666.6667
data.as[,per_cig:=pm.per.cig]
data.as[,conc_95:=cig_midpoint*per_cig]

# define midyear
data.as[, year_id := round((as.integer(year_end) + year_start)/2, 0)]

# merge on ambient exposures to calculate counterfactual
data.as[year_id<min(air_pm$year_id), year_id := min(air_pm$year_id)]
data.as <- merge(data.as,locations[,.(location_id,location_name=location_ascii_name)],by="location_name",all.x=T)
data.as <- merge(data.as,air_pm[,.(location_id,year_id,ambient_exp_mean)], by=c("year_id","location_id"), all.x=T)

# replace global studies with global average ambient and regions for now (based on SOGA aggregates)
data.as[location_name=="Global" & year_id %in% c(1980,1985,1995), ambient_exp_mean:=45]
data.as[location_name=="Australasia" & year_id %in% c(1985), ambient_exp_mean:=9.9]
data.as[location_name=="Western Europe" & year_id %in% c(1991), ambient_exp_mean:=16]
data.as[location_name=="High-income North America" & year_id %in% c(2000), ambient_exp_mean:=9.4]

data.as[,conc_5:=ambient_exp_mean]

data.as[,conc_95:=conc_95+conc_5]

data.as[,conc_mean:= (conc_95+conc_5)/2]

# calculate unit RRs
data.as[, exp_rr_unit := exp_rr ^ (1/(conc_95-conc_5)) ]
data.as[, exp_rr_unit_lower := exp_rr_lower ^ (1/(conc_95-conc_5)) ]
data.as[, exp_rr_unit_upper := exp_rr_upper ^ (1/(conc_95-conc_5)) ]

data.as[,duration_fup_measure:="mean"]
data.as[,duration_fup_units:="years"]
data.as[,value_of_duration_fup:=avg_follow_up]

setnames(data.as,c("filepath","deaths","citation","source","cause"),
         c("file_path","cases","field_citation_value","ier_source","ier_cause"))

# set all covariates to 0
data.as[,c(grep("cv_",names(data.oap),value=T)):=0]

# Combine all data -----------------------------------------------------------------

# merge oap and hap
data <- rbind(data.oap,data.hap,fill=T,use.names=T)
data <- rbind(data,data.shs,fill=T,use.names=T)
data <- rbind(data,data.as,fill=T,use.names=T)

# generate log_rr and log_se
data[,log_rr:=log(exp_rr)]
data[,log_rr_lower:=log(exp_rr_lower)]
data[,log_rr_upper:=log(exp_rr_upper)]
data[,log_se:=(log_rr_upper-log_rr_lower)/3.92]

# generate log_rr and log_se units
data[,log_rr_unit:=log(exp_rr_unit)]
data[,log_rr_unit_lower:=log(exp_rr_unit_lower)]
data[,log_rr_unit_upper:=log(exp_rr_unit_upper)]
data[,log_unit_se:=(log_rr_unit_upper-log_rr_unit_lower)/3.92]

# rename variables
setnames(data,c("conc_5","conc_95"), c("conc_den","conc"))

# estimate average follow up
data[duration_fup_units=="weeks",value_of_duration_fup:=value_of_duration_fup/52]
data[duration_fup_units=="months",value_of_duration_fup:=value_of_duration_fup/12]

data[duration_fup_measure %in% c("mean","median"),median_fup:=value_of_duration_fup]

# calculate average ratio of median follow-up to study length to calculate median follow-up when it is missing
fup_ratio <- data[year_end-year_start>=median_fup,(median_fup)/(year_end-year_start)] %>% unique() %>% mean(na.rm=T)
data[is.na(median_fup),median_fup:=(year_end-year_start)*fup_ratio]

data[is.na(age_mean),age_mean:=(age_start+age_end)/2]

# estimate average age over the whole cohort (using the median age and half of median follow-up)
data[,median_age_fup:=age_mean+median_fup/2]

# create a child/adult indicator for LRI
data[ier_cause=="lri",child:=0]
data[ier_cause=="lri" & age_end<=5,child:=1]

# create a mortality versus incidence variable
# assume mortality unless evidence otherwise
data[,incidence:=0]
data[grep("incidence",outcome,ignore.case = T),incidence:=1]

# fill missingness in follow_up covariate with maximum for the indicator variable
data[is.na(cv_selection_bias),cv_selection_bias:=2]

# Save each cause -----------------------------------------------------------------

for(this.cause in unique(data$ier_cause)){
  out <- data[ier_cause==this.cause,.(underlying_nid,nid,source_type,location_name,location_id,ihme_loc_id,
                                      study,ier_source,ier_cause,year_start,year_end,year_id,measure,
                                      conc_increment,conc_mean,conc_den,conc,
                                      exp_rr,exp_rr_lower,exp_rr_upper,log_rr,log_rr_lower,log_rr_upper,log_se,
                                      exp_rr_unit,exp_rr_unit_lower,exp_rr_unit_upper,log_rr_unit,log_rr_unit_lower,log_rr_unit_upper,log_unit_se,
                                      cv_subpopulation,cv_exposure_population, cv_exposure_selfreport, cv_exposure_study, cv_outcome_selfreport, 
                                      cv_outcome_unblinded, cv_reverse_causation, cv_confounding_nonrandom, cv_counfounding.uncontroled, cv_selection_bias,
                                      median_age_fup,incidence,child,
                                      new_gbd2019)]
  
  write.csv(out,paste0(out_dir,"/",this.cause,".csv"),row.names=F)
  
  # age adjust cvd causes
  if(this.cause %like% "cvd"){
    for(age in cvd_ages){
      dt <- copy(out)
      
      dt[, log_rr := ((log_rr - 0)/(median_age_fup - terminal.age)) * (age - terminal.age)]
      dt[, log_se := ((log_se - 0)/(median_age_fup - terminal.age)) * (age - terminal.age)]
      
      write.csv(dt,paste0(out_dir,"FILEPATH",this.cause,"_",age,".csv"),row.names=F)
    }
  }
}