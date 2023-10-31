###############
# 6. Model PAF
###############
cat(format(Sys.time(), "%a %b %d %X %Y"))

arguments <- commandArgs()[-(1:7)]
print(arguments)

##Source packages
library(plyr           )
library(data.table     )
library(dplyr          )

# source functions from 06_02_paf_functions.R
source('FILEPATH')
locs <- fread('FILEPATH')

param_map <- fread('FILEPATH')
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))

##Read in arguments
debug  <- ifelse(is.na(arguments[1]) == F, arguments[1], T) 

if (debug == F){
  
  location   <- param_map[task_id, location]
  cause      <- param_map[task_id, cause]
  sex        <- param_map[task_id, sex]
  years      <- param_map[task_id, year]
  
  version             <- arguments[2]
  code_directory      <- arguments[3]
  exposure_directory  <- arguments[4]
  paf_directory       <- arguments[5]
  rr_directory        <- arguments[6]
  
  ages                <- as.numeric(unlist(strsplit(arguments[8][[1]], ",")))
  draws               <- arguments[9]
  specific            <- as.character(unlist(strsplit(arguments[10][[1]], ",")))
  
} else{
  
  draws       <- 999 # so, 0:999
  location    <- 102
  sex         <- 1
  cause       <- 688
  
  ages        <- c(8:20, 30:32, 235)
  years       <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
  
  version <- "rr_version"
  rr_version <- "fisher_information_boost"
  
  directory          <- 'FILEPATH'
  exposure_directory <- 'FILEPATH'
  paf_directory      <- 'FILEPATH'
  code_directory     <- 'FILEPATH'
  rr_directory       <- 'FILEPATH'
}

region <- locs[location_id == location]$region_id
decade <- years

rm(locs)
rm(param_map)

cat(format(Sys.time(), "%a %b %d %X %Y"))

##Read in exposures & change draw name to integer
exposure <- fread(sprintf("%s/alc_exp_%s.csv", exposure_directory, location)) %>%
  .[, draw := as.numeric(gsub("draw_", "", draw))] %>%
  .[(age_group_id %in% ages) & (year_id %in% years) & (sex_id == sex) & (draw <= draws),]

exposure <- exposure[sex_id==1, drink_gday_se := drink_gday*1.171]
exposure <- exposure[sex_id==2, drink_gday_se := drink_gday*1.258] 


##For each cause, calculate paf and save
cat(format(Sys.time(), "%a %b %d %X %Y"))

#For liver cancer & cirrhosis, set PAF = 1 and exit & alcoholic cardiomyopathy
if (cause %in% c(938)){
  
  exposure <- exposure[, paf := 1] %>%
    .[, tmrel := 1] %>%
    .[, attribute := 0] %>%
    .[, cause_id := cause]
  
  write.csv(exposure, 'FILEPATH', row.names=F)
  
  cat(format(Sys.time(), "%a %b %d %X %Y"))
  quit() 
}

cat(format(Sys.time(), "%a %b %d %X %Y"))

## Read in relative risks
relative_risk <- fread('FILEPATH')
relative_risk <- relative_risk[exposure <= 100, c("draw", "rr", "exposure")]

## Read in TMREL file
all_cause_rr <- fread('FILEPATH')

relative_risk <- merge(relative_risk, all_cause_rr, by = c("exposure","draw"), all =T)
rm(all_cause_rr)

relative_risk[,rr_tmrel := construct_tmrel(draw,age_group_id,tmrel,relative_risk), by = c("draw", "age_group_id")] 

relative_risk[exposure >= tmrel,rr_rescaled := rr/rr_tmrel]
relative_risk[exposure < tmrel, rr_rescaled := 1] # estimate burden of alcohol use in excess of TMREL
setnames(relative_risk, old = c("rr_rescaled", "rr"), new = c("rr", "rr_unscaled"))

##For each location, sex, year, age, & draw, calculate attributable risk. Pass to function only selected columns of
#the subset dataframe.
exposure <- exposure[drink_gday < 1, attribute := current_drinkers]

exposure[drink_gday>=1, attribute := attributable_risk(.BY$draw, .BY$age_group_id,.SD, relative_risk, u = 100), 
         by=c("location_id", "sex_id", "year_id", "age_group_id", "draw"), 
         .SDcols = c("draw", "current_drinkers","drink_gday", "drink_gday_se", specific)]

exposure[, tmrel := 1]

cat(format(Sys.time(), "%a %b %d %X %Y"))

## Calculate burden for abstainers
exposure[, abstainers := 1 - current_drinkers] 

relative_risk_zero <- relative_risk[exposure ==0,c("draw", "rr", specific), with = F]
exposure <- merge(exposure, relative_risk_zero, by = c("draw", specific), all.x = T)
exposure[, abstainers := abstainers*rr] 

##Using the calculated attributable risk, calculate PAF
exposure[, paf := (abstainers+attribute-tmrel)/(abstainers+attribute)]
exposure[, cause_id := cause]

cat(format(Sys.time(), "%a %b %d %X %Y"))

##For MVA, adjust for fatal harm caused to others
if (cause == 688){
  exposure <- mva_adjust(exposure)
}
cat(format(Sys.time(), "%a %b %d %X %Y"))

## Finish & Save
write.csv(exposure, 'FILEPATH', row.names=F)
cat(format(Sys.time(), "%a %b %d %X %Y"))
