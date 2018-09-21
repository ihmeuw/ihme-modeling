# Compartmental model of HIV no ART

# Set up
rm(list=ls())
library(stats)
library(utils)

if (Sys.info()[1] == "Linux") {
  root <- FILEPATH
  user <- Sys.getenv("USER")
  code_dir <- paste0(FILEPATH)
} else {
  root <- FILEPATH
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0(FILEPATH)
}

root <- ifelse(Sys.info()[1]=="Windows", FILEPATH, FILEPATH)
setwd(code_dir)

##################################################################
## PARALLELIZE OPTIMIZATION FOR EACH STRATUM
##################################################################

ages <- c("15_25", "25_35", "35_45", "45_100")

for(age in unique(ages)) {
  qsub(paste0("hiv_04_",age), paste0(code_dir,"/04b_optimize.r"), pass = age, slots=5,submit=T)
}





