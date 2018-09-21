
rm(list=ls())

os <- .Platform$OS.type

if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  my_libs <- NULL
  
  
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  my_libs <- "FILEPATH"
}


library(reshape2)
library(data.table)

## Load Central Functions
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

me_map <- fread("FILEPATH")

args <- commandArgs(trailingOnly = TRUE)
region <- args[1]

print(region)

usa_smr <- fread("FILEPATH")

sga_thresholds <- fread("FILEPATH")
sga_thresholds <- sga_thresholds[, list(ager5, sex, ga, bw, sga_category, sa_rr_non_imputed, ssa_rr_non_imputed, amer_rr_non_imputed)] 

get_exposures <- function(me_id, loc){
  
  print(me_id)

  dt <- get_model_results('epi', me_id, measure_id = 5, age_group_id = c(2,3), location_id = loc)
  dt[, modelable_entity_id := me_id]
  
  return(dt)
  
}

if(region == "ssa"){
  

  setnames(sga_thresholds, "ssa_rr_non_imputed", "regional_rr")
  
  loc <- 166
  exposure <- rbindlist(lapply(me_map$modelable_entity_id, get_exposures, loc), fill = T, use.names = T) 
  exposure <- exposure[, list(age_group_id, year_id, sex_id, mean, modelable_entity_id)]
  
}else if(region == "sa"){
  
  print(paste0("here in sa", region))
  print(names(sga_thresholds))
  
  setnames(sga_thresholds, "sa_rr_non_imputed", "regional_rr")
  
  
  loc <- c(9, 159)
  exposure <- rbindlist(lapply(me_map$modelable_entity_id, get_exposures, loc), fill = T, use.names = T)  
  exposure <- exposure[, list(age_group_id, year_id, sex_id, mean, modelable_entity_id)]
  exposure <- exposure[, mean := lapply(mean, mean, na.rm=TRUE), by=list(year_id, sex_id, age_group_id, modelable_entity_id) ]
  exposure <- unique(exposure)
  
}else if(region == "amer"){
  

  setnames(sga_thresholds, "amer_rr_non_imputed", "regional_rr")
  
  
  loc <- c(135, 98)
  exposure <- rbindlist(lapply(me_map$modelable_entity_id, get_exposures, loc), fill = T, use.names = T)  
  exposure <- exposure[, list(age_group_id, year_id, sex_id, mean, modelable_entity_id)]
  exposure <- exposure[, mean := lapply(mean, mean, na.rm=TRUE), by=list(year_id, sex_id, age_group_id, modelable_entity_id) ]
  exposure <- unique(exposure)
  
  
}


setnames(exposure, c("age_group_id", "sex_id"), c("ager5", "sex"))
exposure[, ager5 := ager5 + 1]

exposure <- exposure[, mean := lapply(mean, mean, na.rm=TRUE), by=list(sex, ager5, modelable_entity_id) ]

exposure <- unique(exposure[, list(sex, ager5, mean, modelable_entity_id)])

exposure <- merge(me_map[, c("modelable_entity_id", "ga", "bw")], exposure)

setnames(exposure, "mean", "exposure_mean")



combined <- merge(sga_thresholds, exposure, by = c("sex", "ga", "bw", "ager5"))

combined <- merge(combined, usa_smr[, list(sex, bw, ga, ager5, relative_risk, smr)], by = c("sex", "ga", "bw", "ager5"))

combined[, smr_div_exp := smr / exposure_mean]

combined[ smr_div_exp == Inf | smr_div_exp == 0, smr_div_exp := NA]

combined[, min_smr_div_exp := lapply(.SD, min, na.rm = T), by = list(sex, ager5), .SDcols = "smr_div_exp"]


combined[, rr := smr_div_exp / min_smr_div_exp]

combined[, average_rr_sga_category := lapply(.SD, mean, na.rm = T), by = list(sex, ager5, sga_category), .SDcols = "rr"]

combined[, min_average_rr_sga_category := lapply(.SD, min, na.rm = T), by = list(sex, ager5), .SDcols = "average_rr_sga_category"]

combined[, rescaled_lit_rr := regional_rr * min_average_rr_sga_category]

combined[, min_rescaled_lit_rr := lapply(.SD, min, na.rm = T), by = list(sex, ager5), .SDcols = "rescaled_lit_rr"]

combined[, rescaled_rr := rr * rescaled_lit_rr / min_rescaled_lit_rr]

combined[, average_rescaled_rr := lapply(.SD, mean, na.rm = T), by = list(sex, ager5, sga_category), .SDcols = "rescaled_rr"]

write.csv(combined, "FILEPATH", row.names=F, na="")

