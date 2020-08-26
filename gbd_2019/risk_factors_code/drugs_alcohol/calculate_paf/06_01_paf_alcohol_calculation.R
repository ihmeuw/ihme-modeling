###############
# 6. Model PAF
###############
cat(format(Sys.time(), "%a %b %d %X %Y"))

arguments <- commandArgs()[-(1:7)]
print(arguments)

#Source packages
library(plyr           )
library(data.table     )
library(dplyr          )

# source functions from 06_02_paf_functions.R
source('FILEPATH')

param_map <- fread('FILEPATH')
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))


#Read in arguments
debug  <- ifelse(is.na(arguments[1]) == F, arguments[1], T)

if (debug == F){
  print("path 1")
  
  location   <- param_map[task_id, location]
  cause      <- param_map[task_id, cause]
  sex        <- param_map[task_id, sex]
  
  version             <- (arguments[2])
  code_directory      <- arguments[3]
  exposure_directory  <- arguments[4]
  paf_directory       <- arguments[5]
  rr_directory        <- arguments[6]
  
  years               <- as.numeric(unlist(strsplit(arguments[7][[1]], ",")))
  ages                <- as.numeric(unlist(strsplit(arguments[8][[1]], ",")))
  draws               <- arguments[9]
  
} else{
    
  print("path 2")
  draws       <- 999 # so, 0:999
  location    <- 102
  sex         <- 1
  cause       <- 688
  
  ages        <- c(8:20, 30:32, 235)
  years       <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
  
  directory          <- 'FILEPATH'
  exposure_directory <- 'FILEPATH'
  paf_directory      <- 'FILEPATH'
  code_directory     <- 'FILEPATH'
  rr_directory       <- 'FILEPATH'
}

print("read in arguements")
cat(format(Sys.time(), "%a %b %d %X %Y"))
  
#Read in exposures & change draw name to integer
exposure <- fread(sprintf("%s/alc_exp_%s.csv", exposure_directory, location)) %>%
  .[, draw := as.numeric(gsub("draw_", "", draw))] %>%
  .[(age_group_id %in% ages) & (year_id %in% years) & (sex_id == sex) & (draw <= draws),]

exposure <- exposure[sex_id==1, drink_gday_se := drink_gday*1.171]
exposure <- exposure[sex_id==2, drink_gday_se := drink_gday*1.258] 


#For each cause, calculate paf and save

cat(format(Sys.time(), "%a %b %d %X %Y"))

#Use sex_specific RR if IHD, Stroke, or Diabetes. Otherwise, use both-sex RR
if (cause %in% c(493, 495, 496, 587)){
  relative_risk <- fread(paste0(rr_directory, "/rr_", cause, "_", sex, ".csv"))
} else{
  relative_risk <- fread(paste0(rr_directory, "/rr_", cause, ".csv"))
}


#For each location, sex, year, age, & draw, calculate attributable risk. Pass to function only selected columns of
#the subset dataframe.

exposure <- exposure[drink_gday < 1, attribute := current_drinkers]

exposure[drink_gday>=1, attribute := attributable_risk(.BY$draw, .SD, relative_risk), 
         by=c("location_id", "sex_id", "year_id", "age_group_id", "draw"), 
         .SDcols = c("draw", "current_drinkers","drink_gday", "drink_gday_se")]

exposure[, tmrel := 1]

cat(format(Sys.time(), "%a %b %d %X %Y"))

#Using the calculated attributable risk, calculate PAF
exposure[, abstainers := 1 - current_drinkers] #added 10/10/2019
exposure[, paf := (abstainers+attribute-tmrel)/(abstainers+attribute)]

exposure[, cause_id := cause]
  

#For MVA, adjust for fatal harm caused to others
if (cause == 688){
  exposure <- mva_adjust(exposure)
}

cat(format(Sys.time(), "%a %b %d %X %Y"))

write.csv(exposure, 'FILEPATH', row.names=F)
  
cat(format(Sys.time(), "%a %b %d %X %Y"))
cat("Finished!")
