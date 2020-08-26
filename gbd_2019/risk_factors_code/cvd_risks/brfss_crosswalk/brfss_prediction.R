####################################################################################################
## Description: Apply prediction info
####################################################################################################
rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "/FILEPATH/"
} else {
  j <- "/FILEPATH/"
}

library(data.table)
library(mvtnorm)
library(boot)
library(ggplot2)

################### PATHS AND ARGS #########################################
me<-"chl"

set.seed(826)
imputations <- 10
brfss_year_start <- 2001 # started tracking bpmeds variable

root <- ifelse(Sys.info()[1]=="Windows", "/FILEPATH/", "/FILEPATH/")
main_dir <- paste0(root, "FILEPATH")

brfss_map<-paste0(j, "FILEPATH/locations.csv")

output_folder<-paste0("FILEPATH/brfss_temp/", me, "_imptd/")
plot_output<-paste0("FILEPATH/", me, "_imputd_micro.pdf")

################### READ IN MAP #########################################
brfss_locs<-fread(brfss_map)[, .(fips, location_id, location_name)]

################### IMPUTE #########################################
## Apply hypertension prediction model in order to get imputed values for true hypertension status
## for hypertension, have treatment information available
load(paste0(main_dir, "FILEPATH/fit.csv"))
temp <- format_brfss_hypertension(data)
temp[, (paste0("true", 1:imputations)) := pred_high_bp(.SD, high_bp_fit, diagnosed[1], sex[1], imputations), by='diagnosed,sex']
data <- cbind(data, temp[, paste0("true", 1:imputations), with=F])
rm(format_brfss_hypertension, pred_high_bp, high_bp_fit, temp); gc()

################### COLLAPSE TRUES #########################################
setnames(data, "highbp", "diagnosed")
data <- data[, c("year", "state", "age", "sex", "design_wt", "bpmeds", "diagnosed", paste0("true", 1:imputations)), with=F]

## calculate probability of sbp>140 or dbp>90 based on imputation using measured bp + treatment information
data[, hyper:=rowMeans(.SD, na.rm=T), .SDcols=c(paste0("true", 1:imputations))]
}

################### SETUP IMPUTATION #########################################
# treatment data not available for cholesterol
if(me=="chl"){

## Load BRFSS data, keep only 1999+ (to match NHANES years) and respondents over 20
load(paste0(main_dir, "FILEPATH/brfss_microdata.rdata"))
data <- data[year >= 1999 & age >= 15 & !is.na(highchol), ]

## Merge on county-level raking weights, where available (these will be used later for creating a gold standard)
load(paste0(main_dir, "FILEPATH/brfss_mcnty_rake_wts.rdata"))
data <- merge(data, rakewt, by="id", all.x=T)
setnames(data, "rakewt", "area_wt")
rm(rakewt)

## Apply BMI correction model (BMI is used in the cholesterol correction model and must be corrected first)
load(paste0(main_dir, "FILEPATH/bmi_correction_model.rdata"))
data[, reported.bmi := weight/(height^2)]
data[, bmi := correct_bmi(.SD, sex[1], correct_bmi_fit, F), by='sex']
rm(correct_bmi, correct_bmi_fit); gc()

################### IMPUTE #########################################
## Apply cholesterol correction model in order to get imputed values for true cholesterol status
load(paste0(main_dir, "FILEPATH/fit.csv"))
temp <- format_brfss_cholesterol(data)
temp[, (paste0("true", 1:imputations)) := pred_high_cholesterol(.SD, high_cholesterol_fit, diagnosed[1], sex[1], imputations), by='diagnosed,sex']
data <- cbind(data, temp[, paste0("true", 1:imputations), with=F])
rm(format_brfss_cholesterol, pred_high_cholesterol, high_cholesterol_fit, temp); gc()

setnames(data, "highchol", "diagnosed")
data <- data[, c("year", "state", "age", "sex", "design_wt", "diagnosed", paste0("true", 1:imputations)), with=F]

data[, hyper:=rowMeans(.SD, na.rm=T), .SDcols=c(paste0("true", 1:imputations))]
}

## Drop individual imputations
data[, c(paste0("true", 1:imputations)):=NULL]

################### MERGE ON STATE DATA #########################################
data<-merge(data, brfss_locs, by.x="state", by.y="fips")  ##sy:double check this
setnames(data, c("state", "year", "age", "design_wt", "sex"), c("admin_1_id", "year_start", "age_year", "pweight", "sex_id"))

data[, ihme_loc_id:=paste0("USA_", location_id)]

## Formatting
data[, admin_1_id:=ihme_loc_id]
data[, ihme_loc_id:="USA"]

################### RECREATE COLS THAT UBCOV WOULD MAKE #########################################
data[, year_end:=year_start]
data[, survey_module:="HHM"]
data[, filepath:=paste0(main_dir, "FILEPATH/brfss_microdata.rdata")]
data[, survey_name:="BRFSS"]
data[, nid:=99999999999]
data[, psu:=NA]
data[, strata:=NA]

data[, strata:=as.numeric(strata)]
data[, psu:=as.numeric(psu)]
data[, location_name:=NULL]

data[, pweight:=as.numeric(format(data$pweight, scientific=F))]



################### LOOK AT DISTRIBUTION OF IMPUTATION #########################################
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
## need to split brfss by year, getting size issues


## need to delete 2006, no hypertension measured in that one
for(year in unique(data$year_start)){
  x<-data[year_start==year,]
  print(paste(year, nrow(x)))
  print(paste("missing hyper:", table(is.na(x$hyper))))
  
  write.csv(x, file=paste0(output_folder, year, ".csv"), row.names = F)
}
