
## Launch script for re-aggregating into <2500 grams and <28, 28-32, 32-37, and <37 wks MEs


rm(list = ls())

os <- .Platform$OS.type

if (os=="windows") {
  j<- "FILEPATH"
  h <- "FILEPATH"
  my_libs <- NULL
} else {
  j<- "FILEPATH"
  h<- "FILEPATH"
  my_libs <- "FILEPATH"
}


## Get locations


source("FILEPATH")

loc_list <- get_location_metadata(location_set_id = 9)

loc_list <- loc_list[ is_estimate == 1 & most_detailed == 1, ]

for(loc in loc_list$location_id){

  job_name <- paste0("cop_agg_", loc) 
  ## QSUB TO NEXT SCRIPT
  
}