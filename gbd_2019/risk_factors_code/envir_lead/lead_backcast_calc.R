#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: envir_lead_bone and envir_lead_blood
# Purpose: Backcast lead exposure (calc script)
#***********************************************************************************************************************
#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# load packages, install if missing
library(data.table)
library(magrittr)
library(plyr)
library(parallel)

# set working directories
coeff.dir <- file.path("FILEPATH")  # where the draws of the blood to bone lead conversion factor is
setwd(coeff.dir)

# Set parameters from input args
draw <- arg[3]
run_id <- arg[4]
output.version <- arg[5]
draws.required <- as.numeric(arg[6])
threads <- as.numeric(arg[7])

# years that will actually be modeled
years <- 1990:2019

#***********************************************************************************************************************

#----IN/OUT-------------------------------------------------------------------------------------------------------------
#this file will be read in by each parallelized run in order to preserve draw covariance
clean.envir <- file.path(coeff.dir, "bone_coeffs.Rdata")
load(clean.envir)
#objects imported:
#coeff.draws = draws of the conversion factor to estimate bone lead from CBLI

in.dir <- file.path("FILEPATH")
blood_out <- file.path("FILEPATH")
dir.create(blood_out,recursive = T,showWarnings = FALSE)
bone_out <- file.path("FILEPATH")
dir.create(bone_out,recursive = T,showWarnings = FALSE)

df <- fread(file.path(in.dir,paste0(draw,".csv")))

#************************************************************************************************************************
##### Fill in pre-1970 exposures #####

# create exposure as TMREL for all groups between 1891 (year of birth for someone who is 99 in 1990) and 1920
oldtime <- copy(df[year_id <2000]) # just happens to be the right number of years between 1891 and 1920
oldtime[,year_id := year_id - 79]
oldtime[,data := 2]

# create estimates for every group between 1920 and 1970 such that exposure increases linearly from TMREL to actual estimate for 1970
linear <- ddply(df[year_id == 1970],c("location_id","sex_id","age_group_id"),summarise,data = seq(2,data,(data-2)/(1970-1920))[2:50]) %>% data.table # this relies on the plyr package; if dplyr is loaded after plyr, it will mask 'summarise' and it won't work
linear$year_id <- NA
linear[,year_id:=as.numeric(year_id)][,year_id := (1:.N) + 1920, by=c("location_id","sex_id","age_group_id")]

# create one dataset for all groups' exposure from 1891 to 2019
dt <- rbind(oldtime, linear, df)

#************************************************************************************************************************
##### Format exposures for children under 1 and expand age groups into 1 year ages #####

#set data_og as the starting exposure for every group
dt[,data_og := data]

# create multipliers to aggregate exposure in children under 1 to the cumulative exposure over first year of life
dt[age_group_id == 2,multiplier := 7/365]
dt[age_group_id == 3,multiplier := 21/365]
dt[age_group_id == 4,multiplier := 337/365]
dt[age_group_id < 5,data := data*multiplier]

# reset exposure for age_group 4 as the cumulative exposure over the first year of life
dt[age_group_id < 5, babymean := sum(data) , by=c("location_id","year_id","sex_id")]
dt[age_group_id == 4,data := babymean]

# reset exposure for age_group 3 as the cumulative exposure over the first month of life
# currently not used for anything, but would be useful if we went back to calculating average exposure over younger age groups
dt[age_group_id < 4, babymean := sum(data) , by=c("location_id","year_id","sex_id")]
dt[age_group_id == 3,data := babymean]

# remove unnecessary columns and separate neonates from rest of dataset
dt <- dt[,-c("multiplier","babymean"),with=F]
neonates <- dt[age_group_id <= 3 & year_id %in% years]
dt <- dt[age_group_id > 3]

# expand age into 1-year groups for all non-neonates
age_expanded <- data.table(age_group_id = c(4, rep(5,4),rep(6,5),rep(7,5),rep(8,5),rep(9,5),rep(10,5),rep(11,5),rep(12,5),rep(13,5),
                                            rep(14,5),rep(15,5),rep(16,5),rep(17,5),rep(18,5),rep(19,5),rep(20,5),rep(30,5),rep(31,5),
                                            rep(32,5),rep(235,5)), age = seq(0,99,1))
dt <- merge(dt, age_expanded,by="age_group_id",allow.cartesian = T)

# use 1-year age to determine year of birth (yob)
dt[,yob := year_id - age]

#************************************************************************************************************************
##### Backcast exposure since birth #####

# backcast using dt1 (the main dataset) and dt2 (the reference that will be merged onto dt1 for each age group backcasted)
dt1 <- copy(dt[year_id %in% years])
dt2 <- copy(dt)
dt2 <- dt2[, .(location_id,sex_id,year_id,age,data)]
setnames(dt2,c("year_id","age","data"),c("year_match","age_match","data-1"))

for (i in seq(0,99)){
  setnames(dt2,paste0("data",i-1),paste0("data",i))
  dt1[age >= i,year_match := yob + i]
  dt1[age >= i,age_match := i]
  dt1 <- merge(dt1,dt2,by=c("location_id","sex_id","year_match","age_match"),all.x=T)
  dt1[,year_match := NA]
  dt1[,age_match := NA]
}

#************************************************************************************************************************
##### Calculate cumulative exposure and exposure for IQ effects #####

# get cumulative exposure since birth (for bone lead)
dt1[,total_exp := rowSums(.SD,na.rm=T),.SDcols = grep('data[0-9]',names(dt1),value =T)]

# get concurrent exposure at age where we calculate effect on IQ (24 months)
dt1[age > 2,iq_exp := data2]
dt1[age <= 2, iq_exp := data_og]

# aggregate exposures back into their corresponding age_group_id
dt1[,total_exp := mean(total_exp),by=c("location_id","year_id","sex_id","age_group_id")]
dt1[,iq_exp := mean(iq_exp),by=c("location_id","year_id","sex_id","age_group_id")]

# format neonate exposures to be merged back on
setnames(neonates,c("data","data_og"),c("total_exp","iq_exp"))

# subset output to the years and columns we are actually reporting and merge on neonatal groups
output <- rbind(unique(dt1[,list(location_id,year_id,sex_id,age_group_id,total_exp,iq_exp)]),neonates)

# convert cumulative exposure to bone lead
draw_num <- as.numeric(strsplit(draw,"_")[[1]][2])
output[,bone_exp := total_exp * coeff.draws[draw_num + 1]]
output[,total_exp := NULL]
output[,measure_id := 19]

#write output files
for (loc in unique(output$location_id)){
  bone <- output[location_id == loc & age_group_id >= 10,-c("iq_exp"),with=F]
  setnames(bone,"bone_exp","value")
  bone[,variable := draw]
  write.csv(bone,file.path(bone_out,paste0(loc,"_",draw,".csv")),row.names=F)

  blood <- output[location_id == loc,-c("bone_exp"),with=F]
  setnames(blood,"iq_exp","value")
  blood[,variable := draw]
  write.csv(blood,file.path(blood_out,paste0(loc,"_",draw,".csv")),row.names=F)
}
