#---------------------------------------------------
# Purpose: format draws for non-cvd outcomes only
# Date: 04/01/2021
#---------------------------------------------------

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
j <- if (os == "Linux") "/home/j/" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("/homes/", user, "/") else if (os == "Windows") "H:/"

library(dplyr)
library(ggplot2)
library(data.table)
source(FILEPATH)

# Set up arguments
if(interactive()){
  ro_pair <- "fractures"
  level_100 <- T
} else {
  args <- commandArgs(trailingOnly = TRUE)
  ro_pair <- args[1]
  level_100 <- as.logical(args[2])
}

if(level_100){
  message("100 exposure levels")
  rr_dir <- FILEPATH
  save_dir <- FILEPATH
} else {
  message("1000 exposure levels")
  rr_dir <- FILEPATH
  save_dir <- FILEPATH
}

ages <- get_age_metadata(19)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages <- ages[,.(age_start, age_end, age_group_id)]

# expand age group and sex
age_group_ids <- c(6:20, 30:32, 235)

# for fractures
if(ro_pair=="fractures"){
  rr_frac <- fread(paste0(FILEPATH, "smoking_", ro_pair, ".csv"))
  rr_frac[, rr := exp(rr)]
  
  rr_full <- expand.grid(cause_id=c(878,923), draw=0:999, sex_id=1:2, age_group_id=age_group_ids) %>% as.data.table
  rr_full <- merge(rr_full, rr_frac, by="draw")
  
} else {
  # reshape the data
  rr <- fread(paste0(FILEPATH, "smoking_", ro_pair, ".csv"))
  setnames(rr, "risk", "exposure")
  rr_long <- melt(rr, id.vars = "exposure", variable.name = "draw", value.name = "rr")
  rr_long <- rr_long[order(exposure)]
  rr_long[, rr:=exp(rr)]
  rr_long[, draw := as.numeric(draw)-1]
  
  rr_full <- expand.grid(exposure=seq(0,100,0.1), draw=0:999, sex_id=1:2, age_group_id=age_group_ids) %>% as.data.table
  rr_full <- merge(rr_full, rr_long, by=c("exposure", "draw"))
  setorder(rr_full, "exposure","draw", "sex_id", "age_group_id")
  
  if(ro_pair %in% c("breast_cancer", "cervical_cancer")){
    rr_full <- rr_full[sex_id==2]
  }
  
  if(ro_pair %in% c("prostate_cancer")){
    rr_full <- rr_full[sex_id==1]
  }  
}



# save the draws
write.csv(rr_full, paste0(FILEPATH, ro_pair, ".csv"), row.names = F)





