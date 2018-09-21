## Launch code for 0-6, 7-27 days prevalence calculations


rm(list = ls())

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


## Get locations

source("FILEPATH")
loc_list <- get_location_metadata(location_set_id = 9)
loc_list <- loc_list[ (is_estimate == 1 & most_detailed == 1) , ]

distn <- "ensemble"


for(loc in loc_list[["location_id"]]){ # 

  job_name <- paste0("exp_cal_", loc) 
  ## QSUB TO NEXT SCRIPT
  
}
