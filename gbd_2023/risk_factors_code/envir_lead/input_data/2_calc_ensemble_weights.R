#Ensemble weights for lead backcasting

rm(list=ls())

# libraries #######################################
library(readxl)

source("FILEPATH/fit_submit.R")

#Import ########################
#Load in only the microdata for blood lead
data<-as.data.table(read_excel("FILEPATH/GBD2023_reextracted_microdata_tabulated.xlsx"))

#change column name of val to data
setnames(data,"val","data")

#add age_year
data[,age_year:=round(year_id-age_end,0)]

#multiple the missingness column by 100 since it is in fraction form, so it'll be a percent
data[,missing_quant_frac:=100*missing_quant_frac]

#remove any sources that are above 15
data<-data[missing_quant_frac<=15 | is.na(missing_quant_frac),]

#only keep the required columns
data<-data[,.(location_id, year_id, age_year, sex_id, nid, data)]

# launch ##################################
#launch the function
fit_ensemble_weights("envir_lead_blood",microdata = data, cluster_account = "proj_erf",n_cores=30)
