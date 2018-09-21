## Launch Ensemble Distribution for both birthweigth and gestational age

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


source("FILEPATH")

loc_list <- get_location_metadata(location_set_id = 9)

loc_list <- loc_list[ (is_estimate == 1 & most_detailed == 1) | (location_id == 9 | location_id == 4 | location_id == 158 | location_id == 159 | location_id == 103 | location_id == 166), ]

## Keep only locations for which estimates need to be produced

sss <- 3

resub <- fread("FILEPATH")

for(loc in loc_list[["location_id"]]){

  bw_name <- paste0("bw_", loc, "_", sss)
  ## QSUB TO BIRTHWEIGHT ENSEMBLE SCRIPT
  
  ga_name <- paste0("ga_", loc, "_", sss)
  ## QSUB TO GESTATIONAL AGE ENSEMBLE SCRIPT
  
}