###########################
## Estimate sex ratio COPD
###########################

# SET UP ------------------------------------------------------------------

rm(list=ls())

# packages
require(pacman)
p_load(data.table, ggplot2)
library(nlme, lib.loc = "FILEPATH")
library(metafor, lib.loc = "FILEPATH")

date <- Sys.Date()

shared_functions_dir <- "FILEPATH"
source(paste0(shared_functions_dir,"/get_epi_data.R"))
source(paste0(shared_functions_dir,"/upload_epi_data.R"))

# GET DATA ----------------------------------------------------------------

dt <- get_epi_data(123) # 123 = raw COPD bundle
dt <- dt[is_outlier!=1 & (group_review==1 | is.na(group_review)) & sex!="Both" & measure=="prevalence",] # remove outliered, group-reviewed, or both-sex data
dt <- dt[,c("nid","field_citation_value","sex","year_start","year_end","location_id","location_name","age_start","age_end","mean","cases","sample_size","standard_error")]
dt <- unique(dt,by=c("nid","sex","year_start","year_end","location_id","age_start","age_end","mean")) # remove duplicates just in case
dt_wide <- dcast(dt, nid+field_citation_value+year_start+year_end+location_id+location_name+age_start+age_end~sex, value.var="mean") # reshape wide on sex
setnames(dt_wide,c("Male","Female"),c("male","female"))
dt_wide <- dt_wide[!is.na(male) & !is.na(female) & male!=0 & female!=0,] # remove rows where either male or female is missing or zero
dt_wide[,ratio:=male/female] # calculate male/female ratio

# PLOT RATIOS BY AGE ------------------------------------------------------

# plot by age to confirm there is no aparent age-pattern before we aggregate over age
age <- ggplot(data=dt_wide, aes(x=age_start,y=ratio))+geom_point()+xlab("age_start")+ylab("Ratio male/female COPD")
age

# AGGREGATE OVER AGE ------------------------------------------------------

dt[is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2] # calculate sample_size if missing
dt[is.na(cases), cases := sample_size * mean] # calculate cases if missing

dt[,`:=`(cases=sum(cases),sample_size=sum(sample_size)), by=c("nid","sex","year_start","year_end","location_id")] # sum cases and sample size
dt <- unique(dt, by=c("nid","sex","year_start","year_end","location_id")) # remove duplicates

dt[,standard_error:=sqrt(mean*(1-mean)/sample_size)] # re-calculate standard error

dt_wide <- dcast(dt, nid+field_citation_value+year_start+year_end+location_id+location_name~sex, value.var=c("mean","standard_error")) # reshape wide on sex
setnames(dt_wide,c("mean_Male","mean_Female","standard_error_Male","standard_error_Female"),c("male_mean","female_mean","male_standard_error","female_standard_error"))
dt_wide <- dt_wide[!is.na(male_mean) & !is.na(female_mean) & male_mean!=0 & female_mean!=0,] # remove rows where either male or female is missing or zero

dt_wide[,ratio:=male_mean/female_mean] # calculate male/female ratio
dt_wide[,ratio_se:=sqrt((male_mean/female_mean)*((male_standard_error^2/female_mean^2)+(female_standard_error^2/female_mean^2)))] # calculate standard error of ratio

# PLOT ALL AGE RATIOS -----------------------------------------------------

dt_wide_subset <- dt_wide[ratio<10,]
dt_wide_subset[,less_than_1:=(ratio<1)]
all_age <- ggplot(data=dt_wide[ratio<10,], aes(ratio))+
                geom_histogram(data=subset(dt_wide_subset, less_than_1==1), fill="red", binwidth = 0.1)+
                geom_histogram(data=subset(dt_wide_subset, less_than_1==0), fill="blue", binwidth = 0.1)+
                xlab("Ratio male/female COPD")+ ylab("Source count")
all_age

# META-ANALYSIS & FOREST PLOT ---------------------------------------------

dt_wide_only_ms <- dt_wide[nid %in% c(244369:244371,336847:336850),]
dt_wide_no_ms <- dt_wide[!(nid %in% c(244369:244371,336847:336850)),]

model_all <- rma.uni(data = dt_wide, yi = ratio, sei = ratio_se)
model_only_ms <- rma.uni(data = dt_wide_only_ms, yi = ratio, sei = ratio_se)
model_no_ms <- rma.uni(data = dt_wide_no_ms, yi = ratio, sei = ratio_se)

pdf(paste0("FILEPATH/copd_sex_ratio_meta_analysis_ALL_",date,".pdf"), width=5, height=40)
forest(model_all, showweights = T, slab = paste0(dt_wide$nid, " : ", dt_wide$location_name, " ", dt_wide$year_start), 
       xlab = "Ratio male/female COPD", refline = 1, title="All Data")
dev.off()

pdf(paste0("FILEPATH/copd_sex_ratio_meta_analysis_ONLY_MS_",date,".pdf"), width=5, height=40)
forest(model_only_ms, showweights = T, slab = paste0(dt_wide_only_ms$nid, " : ", dt_wide_only_ms$location_name, " ", dt_wide_only_ms$year_start), 
       xlab = "Ratio male/female COPD", refline = 1, title="Only Marketscan")
dev.off()

pdf(paste0("FILEPATH/copd_sex_ratio_meta_analysis_NO_MS_",date,".pdf"), width=8, height=40)
forest(model_no_ms, showweights = T, slab = paste0(dt_wide_no_ms$nid, " : ", dt_wide_no_ms$location_name), 
       xlab = "Ratio male/female COPD", refline = 1, title="Marketscan excluded", cex=0.8)
dev.off()


