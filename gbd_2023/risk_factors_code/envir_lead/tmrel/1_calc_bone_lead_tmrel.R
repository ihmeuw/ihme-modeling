#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: envir_lead_bone
# Purpose: calculate bone lead TMREL given that blood lead TMREL is 0.016 ug/dL

#***********************************************************************************************************************
#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"

} else {
  
  j_root <- "J:/"
  h_root <- "H:/"

}

# load packages
library(data.table)
library(magrittr)
library(plyr)
library(parallel)
source("FILEPATH/get_demographics.R")

# set working directories
coeff.dir <- file.path(j_root, "FILEPATH")  # where the draws of the blood to bone lead conversion factor is

# settings
draw.cols <- paste0("draw_", 0:999)
years <- 1990:2022 # years that will actually be modeled
out_dir <- "FILEPATH"

#***********************************************************************************************************************

#----IN/OUT-------------------------------------------------------------------------------------------------------------
#this file will be read in by each parallelized run in order to preserve draw covariance
clean.envir <- file.path(coeff.dir, "bone_coeffs.Rdata")
load(clean.envir)
#objects imported:
#coeff.draws = draws of the conversion factor to estimate bone lead from CBLI

demog <- get_demographics("epi")

# create a synthetic data file where data point for every year/age/sex is the TMREL
dt <- data.table(expand.grid(location_id = 1, year_id = 1891:2022, sex_id = 1:2, age_group_id = demog$age_group_id, data = 0.016))

##### Format exposures for children under 1 and expand age groups into 1 year ages #####

# create multipliers to aggregate exposure in children under 1 to the cumulative exposure over first year of life
dt[age_group_id == 2,multiplier := 7/365]
dt[age_group_id == 3,multiplier := 21/365]
dt[age_group_id == 388,multiplier := 155/365]
dt[age_group_id == 389,multiplier := 182/365]
dt[age_group_id %in% c(2,3,388,389),data := data*multiplier]

# remove unnecessary columns and remove neonates (age_group_ids 2 & 3) from rest of dataset
dt[, multiplier := NULL]
dt <- dt[age_group_id > 3]

# expand age into 1-year groups for all non-neonates
age_expanded <- data.table(age_group_id = c(388, 389, rep(238,2), rep(34,2), rep(6,5),rep(7,5),rep(8,5),rep(9,5),rep(10,5),rep(11,5),rep(12,5),rep(13,5),
                                            rep(14,5),rep(15,5),rep(16,5),rep(17,5),rep(18,5),rep(19,5),rep(20,5),rep(30,5),rep(31,5),
                                            rep(32,5),rep(235,5)), age = c(0,seq(0,99,1)))
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

# aggregate exposures back into their corresponding age_group_id
dt1[,total_exp := mean(total_exp),by=c("location_id","year_id","sex_id","age_group_id")]

# subset output to the ages & columns we are actually reporting
output <- unique(dt1[age_group_id %in% c(10:20,30:32,235), .(location_id, age_group_id, total_exp)])

# create 1000 draws in order to convert cumulative exposure to bone lead at the draw level
draw_dt <- data.table(expand.grid(location_id = 1, age_group_id = output$age_group_id, draws = paste0("draw_", 0:999)))
output <- merge(output, draw_dt, by = c("location_id", "age_group_id"))
# reshape wide
output <- dcast(output, location_id + age_group_id ~ draws, value.var = "total_exp")
# convert to bone lead using conversion factor
output[, (draw.cols) := lapply(1:1000, function(x) get(draw.cols[x]) * coeff.draws[x]), .SDcols = draw.cols]

# mean/lower/upper
output[, mean := rowMeans(.SD), .SDcols = draw.cols]
output[, lower := apply(.SD, 1, quantile, 0.025), .SDcols = draw.cols]
output[, upper := apply(.SD, 1, quantile, 0.975), .SDcols = draw.cols]

output_clean <- output[, -draw.cols, with = F]

# format for save_results_risk
save_dt <- data.table(expand.grid(location_id = 1, age_group_id = output$age_group_id, sex_id = 1:2, year_id = 1990:2022))
output_save <- merge(save_dt, output[, -c("mean", "lower", "upper")], by = c("location_id", "age_group_id"))

# save
write.csv(output_save, file.path(out_dir, "bone_lead_tmrel_draws.csv"), row.names = FALSE)
write.csv(output_clean, file.path(out_dir, "bone_lead_tmrel_summary.csv"), row.names = FALSE)

##### upload to database #####
# save results (run in qlogin with 24 threads & 60G mem)
source("FILEPATH/save_results_risk.R")
save_results_risk(input_dir = "FILEPATH", input_file_pattern = "bone_lead_tmrel_draws.csv", 
                  modelable_entity_id = 9212, description = "saving all years", risk_type = "tmrel", 
                  year_id = 1990:2022, gbd_round_id = 7, decomp_step = "iterative", mark_best = TRUE)
