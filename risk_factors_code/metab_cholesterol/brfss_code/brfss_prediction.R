####################################################################################################
## Author: USERNAME
## Description: Prep BRFSS hypertension data for running small area models:
##  (1) Load BRFSS data and keep appropriate variables
##  (2) Exclude respondents with missing values
##  (3) Apply correction model
##  (4) Output datasets for each outcome
## Modified from: FILEPATH
##  USERNAME: on 1/26/2017, added a line to get probability, and saving
####################################################################################################
rm(list=ls())
os <- .Platform$OS.type
if (os == "FILEPATH") {
  j <- "FILEPATH"
} else {
  j <- "FILEPATH"
}

library(data.table)
library(mvtnorm)
library(boot)
library(ggplot2)

################### PATHS AND ARGS #########################################
######################################################
me<-"chl"

set.seed(826)
imputations <- 10
brfss_year_start <- 2001 # started tracking bpmeds variable





root <- ifelse(USERNAMEs.info()[1]=="FILEPATH")
main_dir <- paste0(root, "FILEPATH")

brfss_map<-paste0(j, "FILEPATH")


output_folder<-paste0(j, "FILEPATH")
plot_output<-paste0(j, "FILEPATH")





################### READ IN MAP #########################################
######################################################


brfss_locs<-fread(brfss_map)[, .(fips, location_id, location_name)]



################### SETUP IMPUTATION #########################################
######################################################

##USERNAME: there are redundant pieces of code between the sbp and the chl version, but I don't have time to piece out what's unique
if(me=="sbp"){

## Load BRFSS data, keep only 1999+ (to match NHANES years) and respondents over 20
load(paste0(main_dir, "FILEPATH"))
data <- data[year >= brfss_year_start & age >= 15 & !is.na(highbp), ]

## Merge on county-level raking weights, where available (these will be used later for creating a gold standard)
load(paste0(main_dir, "FILEPATH"))
data <- merge(data, rakewt, by="id", all.x=T)
setnames(data, "rakewt", "area_wt")
rm(rakewt)

## Apply BMI correction model (BMI is used in the diabetes correction model and must be corrected first)
load(paste0(main_dir, "FILEPATH"))
data[, reported.bmi := weight/(height^2)]
data[, bmi := correct_bmi(.SD, sex[1], correct_bmi_fit, F), by='sex']
rm(correct_bmi, correct_bmi_fit); gc()



################### IMPUTE #########################################
######################################################


## Apply hypertension prediction model in order to get imputed values for true hypertension status
load(paste0(main_dir, "FILEPATH"))
temp <- format_brfss_hypertension(data)
temp[, (paste0("true", 1:imputations)) := pred_high_bp(.SD, high_bp_fit, diagnosed[1], sex[1], imputations), by='diagnosed,sex']
data <- cbind(data, temp[, paste0("true", 1:imputations), with=F])
rm(format_brfss_hypertension, pred_high_bp, high_bp_fit, temp); gc()




################### COLLAPSE TRUES #########################################
######################################################


setnames(data, "highbp", "diagnosed")
data <- data[, c("year", "state", "age", "sex", "design_wt", "bpmeds", "diagnosed", paste0("true", 1:imputations)), with=F]





##USERNAME:calculate probability of sbp>140 or dbp>90 based on USERNAME's imputations (the hypertensive col)  The prediction is sbp>140 or dbp>90
data[, hyper:=rowMeans(.SD, na.rm=T), .SDcols=c(paste0("true", 1:imputations))]
}






################### SETUP IMPUTATION #########################################
######################################################



if(me=="chl"){


## Load BRFSS data, keep only 1999+ (to match NHANES years) and respondents over 20
load(paste0(main_dir, "FILEPATH"))
data <- data[year >= 1999 & age >= 15 & !is.na(highchol), ]

## Merge on county-level raking weights, where available (these will be used later for creating a gold standard)
load(paste0(main_dir, "FILEPATH"))
data <- merge(data, rakewt, by="id", all.x=T)
setnames(data, "rakewt", "area_wt")
rm(rakewt)

## Apply BMI correction model (BMI is used in the cholesterol correction model and must be corrected first)
load(paste0(main_dir, "FILEPATH"))
data[, reported.bmi := weight/(height^2)]
data[, bmi := correct_bmi(.SD, sex[1], correct_bmi_fit, F), by='sex']
rm(correct_bmi, correct_bmi_fit); gc()




################### IMPUTE #########################################
######################################################


## Apply cholesterol correction model in order to get imputed values for true cholesterol status
load(paste0(main_dir, "FILEPATH"))
temp <- format_brfss_cholesterol(data)
temp[, (paste0("true", 1:imputations)) := pred_high_cholesterol(.SD, high_cholesterol_fit, diagnosed[1], sex[1], imputations), by='diagnosed,sex']
data <- cbind(data, temp[, paste0("true", 1:imputations), with=F])
rm(format_brfss_cholesterol, pred_high_cholesterol, high_cholesterol_fit, temp); gc()

setnames(data, "highchol", "diagnosed")
data <- data[, c("year", "state", "age", "sex", "design_wt", "diagnosed", paste0("true", 1:imputations)), with=F]









data[, hyper:=rowMeans(.SD, na.rm=T), .SDcols=c(paste0("true", 1:imputations))]
}












##USERNAME:drop individual imputations
data[, c(paste0("true", 1:imputations)):=NULL]




################### MERGE ON STATE DATA #########################################
######################################################

data<-merge(data, brfss_locs, by.x="state", by.y="fips")  ##USERNAME:double check this



setnames(data, c("state", "year", "age", "design_wt", "sex"), c("admin_1_id", "year_start", "age_year", "pweight", "sex_id"))



data[, ihme_loc_id:=paste0("USA_", location_id)]

##USERNAME: format some more, swap ihme_oc_id and admin_1_id
data[, admin_1_id:=ihme_loc_id]
data[, ihme_loc_id:="USA"]



################### RECREATE COLS THAT UBCOV WOULD MAKE #########################################
######################################################


data[, year_end:=year_start]
data[, survey_module:="HHM"]
data[, filepath:=paste0(main_dir, "FILEPATH")]
data[, survey_name:="BRFSS"]
data[, nid:=99999999999]
data[, psu:=NA]
data[, strata:=NA]

data[, strata:=as.numeric(strata)]
data[, psu:=as.numeric(psu)]
data[, location_name:=NULL]

data[, pweight:=as.numeric(format(data$pweight, scientific=F))]



################### LOOK AT DISTRIBUTION OF IMPUTATION #########################################
##########################################################


pdf(file=plot_output)

for(sex in unique(data$sex_id)){
  x<-data[sex_id==sex,]
  
  p<-ggplot(data=x, aes(x=hyper))+
    geom_histogram(fill="black", color="black")+
    ggtitle(paste("Imputation distr in sex_id:", sex))+
    theme_classic()
  print(p)
  
  
  
}
dev.off()





################### SAVE #########################################
##########################################################



##USERNAME: need to split brfss by year, getting size issues


##USERNAME; need to delete 2006, no hypertension measured in that one
for(year in unique(data$year_start)){
  x<-data[year_start==year,]
  print(paste(year, nrow(x)))
  print(paste("missing hyper:", table(is.na(x$hyper))))
  
  write.csv(x, file=paste0(output_folder, year, ".csv"), row.names = F)
  
  
  
}












