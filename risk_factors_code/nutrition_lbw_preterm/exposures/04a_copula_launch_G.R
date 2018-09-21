## Launch joint distribution copula modeling

rm(list = ls())

os <- .Platform$OS.type

if (os=="windows") {
  j<- "FILEPATH"
  h <- "FILEPATH"
  my_libs <- NULL
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  my_libs <- "FILEPATH"
}

library(data.table)

## Get locations

source("FILEPATH")
loc_list <- get_location_metadata(location_set_id = 9)
loc_list <- loc_list[ (is_estimate == 1 & most_detailed == 1) | (location_id == 9 | location_id == 4 | location_id == 158 | location_id == 159 | location_id == 103 | location_id == 166), ]


for(loc in loc_list[["location_id"]]){ 

  copula_name <- paste0("copula_", loc)
  ## QSUB TO NEXT SCRIPT  
  
}
