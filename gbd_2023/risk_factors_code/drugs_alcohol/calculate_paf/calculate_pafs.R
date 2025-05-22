arguments <- commandArgs()[-(1:7)]
print(arguments)

library(data.table)  
library(plyr)        
library(dplyr)


invisible(sapply(list.files('FILEPATH', full.names = T), source)) 
source('FILEPATH')


# set parameters and directories
print("starting execution")
cat(format(Sys.time(), "%a %b %d %X %Y"))



#Source packages
library(plyr           )
library(data.table     )
library(dplyr          )
source('FILEPATH')
source('FILEPATH')
source('FILEPATH')

invisible(sapply(list.files('FILEPATH', full.names = T), source)) 
source('FILEPATH')

vals <- strsplit(arguments, '\\s+')[[1]]
spec_vals <- strsplit(vals[10], ",")[[1]]
specific            <- as.character(unlist(spec_vals))

param_map <- fread('FILEPATH')

task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))

locations <- get_location_metadata(location_set_id = 35, release_id = 16)

#Read in arguments

debug <- F

if (debug == F){
  print("calculating from submit")
  print(task_id)
  
  
  location   <- param_map[task_id, location] 
  sexes        <- param_map[task_id, sex]
  
  version             <- arguments[2]
  code_directory      <- arguments[3]
  exposure_directory  <- arguments[4]
  paf_directory       <- arguments[5]
  #rr_directory        <- arguments[6]
  rr_version <- arguments[6]
  rr_directory <- paste0('FILEPATH')
  years               <- as.numeric(unlist(as.list(strsplit(arguments[7], ","))))
  ages                <- as.numeric(unlist(as.list(strsplit(arguments[8], ","))))
  causes              <- as.numeric(unlist(as.list(strsplit(arguments[11], ","))))
  
  draws               <- arguments[9]
  comp_vers <- arguments[12]
  tmrel_vers <- arguments[13]
  
  print("ids:")
  print(task_id)
  print(location)
  print(years)
  print(sexes)
  print(ages)
  print(causes)
  print(draws)
  
} else{
  
  print("direct calculation")
  draws       <- 100 # so, 0:999
  location    <- 163
  sex         <- 1
  cause       <- 493
  
  # ages        <- 15
  years       <- 2010
  ages        <- c(8:20, 30:32, 235)
  #years       <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
  
  directory          <- 'FILEPATH'
  version <- "##"
  
  exposure_directory <- 'FILEPATH'
  paf_directory      <- 'FILEPATH'
  code_directory     <- 'FILEPATH'
  rr_directory       <- 'FILEPATH'
}



dir.create(paste0('FILEPATH'), showWarnings = FALSE, recursive = TRUE)
dir.create(paste0('FILEPATH'), showWarnings = FALSE, recursive = TRUE)
dir.create(paste0('FILEPATH'), showWarnings = FALSE, recursive = TRUE)


#Set up arguments to pass to calculation



tmrel_specific <- c("age_group_id", "sex_id", "location_id", "year_id")

########################################################################################################
draws       <- 100 

exposure_directory <- 'FILEPATH'

##################################################################################################################
# Loop through ages, sexes, causes
for(l in location){
  region <- locations[location_id == l, region_id]
  for(cause in causes){
    print(cause)
    for(sex in sexes){
      for(y in years){
        decade <- y
        print(y)
        
        ##################################################################################################################
        #Read in exposures & change draw name to integer
        if(y %in% c(2022, 2023, 2024)){
          exposure <- fread(sprintf("%salc_exp_%s.csv", exposure_directory, l)) %>%
            .[, draw := as.numeric(gsub("draw_", "", draw))] %>%
            .[(age_group_id %in% ages) & (year_id == 2021) & (sex_id == sex) & (draw <= max(draws)),] 
          exposure$year_id <- y
        } else{
          exposure <- fread(sprintf("%salc_exp_%s.csv", exposure_directory, l)) %>%
            .[, draw := as.numeric(gsub("draw_", "", draw))] %>%
            .[(age_group_id %in% ages) & (year_id %in% y) & (sex_id == sex) & (draw <= max(draws)),] #fixed
        }
        exposure <- exposure[sex_id==1, drink_gday_se := drink_gday*1.171]
        exposure <- exposure[sex_id==2, drink_gday_se := drink_gday*1.258] 
        
        ##################################################################################################################
        #set PAF = 1 and exit alcoholic cardiomyopathy
        if (cause %in% c(938)){
          
          exposure <- exposure[, paf := 1] %>%
            .[, tmrel := 1] %>%
            .[, attribute := 0] %>%
            .[, cause_id := cause]
          
          cat(format(Sys.time(), "%a %b %d %X %Y"))
          cat("Finished!")
          write.csv(exposure, paste0('FILEPATH'), row.names=F)
          
          quit() 
        }
        
        ###################################################################################################################
        # pull in RR 
        relative_risk <- read.csv(paste0(rr_directory, cause, ".csv"))
        relative_risk$exposure <- as.numeric(relative_risk$exposure)
        relative_risk <- relative_risk %>% dplyr::select(c("draw", "rr", "exposure"))
        
        relative_risk <- as.data.table(relative_risk)
        
        # pull in all cause RR 
        all_cause_rr <- fread(paste0('FILEPATH'))
        
        all_cause_rr$location_id <- l
        all_cause_rr$year_id <- as.numeric(all_cause_rr$year_id)
        
        all_cause_rr <- unique(all_cause_rr)
        table(all_cause_rr$draw)
        ######################################################################################################################
        # merge all cause RR and relative risk file , then construct TMREL.
        rr_out <- relative_risk
        
        relative_risk <- merge(relative_risk, all_cause_rr, by = c("exposure","draw"))
        relative_risk <- unique(relative_risk)
                
        relative_risk <- as.data.table(relative_risk)
        
        relative_risk[,rr_tmrel := construct_tmrel(draw,age_group_id,tmrel,relative_risk), by = c("draw", "age_group_id")] 
        
        
        relative_risk[exposure >= tmrel,rr_rescaled := rr/rr_tmrel] #rr/rr_tmrel
        relative_risk[exposure < tmrel, rr_rescaled := 1] # 1 #
        relative_risk[is.infinite(rr_rescaled), rr_rescaled := 1] # 
        relative_risk[is.na(rr_rescaled), rr_rescaled := 1] # 
        
        setnames(relative_risk, old = c("rr_rescaled", "rr"), new = c("rr", "rr_unscaled"),skip_absent = TRUE)
        relative_risk$rr <- as.numeric(relative_risk$rr)
        relative_risk$exposure <- as.numeric(relative_risk$exposure)
        #########################################################################################################################
        
        if(length(unique(exposure$draw)) == length(unique(relative_risk$draw))){
          
        } else {
          
        }
        
        max_draw <- 99 
        exposure <- exposure[draw <= max_draw,]
        relative_risk <- relative_risk[draw <= max_draw,]
        
       
        
        ##########################################################################################################################
        # Bring in exposure for paf calc
        if(nrow(exposure[drink_gday>=1])>0){
          try(
            exposure[drink_gday>=1, attribute := attributable_risk(.BY$draw, .BY$age_group_id,.SD, relative_risk, u = 100), 
                     by=c("location_id", "sex_id", "year_id", "age_group_id", "draw"), 
                     .SDcols = c("draw", "current_drinkers","drink_gday", "drink_gday_se", "age_group_id", "sex_id", "location_id", "year_id")]
          )
          
          exposure[, tmrel := 1] # RR at TMREL, as a result of the rr rescale. 
        } else{
          print("exposure is less than tmrel everywhere for this location")
          next
        }
        
        
        ###########################################################################################################################
        # Calculate PAF
        exposure[, abstainers := 1 - current_drinkers] 
        
       
        relative_risk_zero <- relative_risk[exposure ==0,c("draw", "rr", "age_group_id", "sex_id", "location_id", "year_id"), with = F]
        exposure <- merge(exposure, relative_risk_zero, by = c("draw", "age_group_id", "sex_id", "location_id", "year_id"), all.x = T)
        exposure[, abstainers := abstainers*rr] 
        
        #print("calculated abstainer burden")
        
        exposure[, paf := (abstainers+attribute-tmrel)/(abstainers+attribute)]
        exposure[, cause_id := cause]
        
        save_exposure_copy <- copy(exposure)
        
        ## change these filepaths as needed 
        print(paste0("printing to: ",paste0('FILEPATH')))
        write.csv(exposure, paste0('FILEPATH'), row.names=F)
      }}}}