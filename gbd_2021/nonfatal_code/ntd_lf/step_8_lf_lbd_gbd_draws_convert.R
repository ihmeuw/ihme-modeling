# Process LF Focal 3 prevalence model draws and upload to
# GBD database
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
    library(argparse)
    print(commandArgs())
    parser <- ArgumentParser()
    parser$add_argument("--params_dir", type = "character")
    parser$add_argument("--draws_dir", type = "character")
    parser$add_argument("--interms_dir", type = "character")
    parser$add_argument("--logs_dir", type = "character")
    args <- parser$parse_args()
    print(args)
    list2env(args, environment()); rm(args)
    sessionInfo()
} else {
    params_dir <- paste0(data_root, "FILEPATH")
    draws_dir <- paste0(data_root, "FILEPATH")
    interms_dir <- paste0(data_root, "FILEPATH")
    logs_dir <- paste0(data_root, "FILEPATH")
}

# Source relevant libraries
source("FILEPATH")
source("FILEPATH")

# Constants
lbd_draws_dir <- "FILEPATH"
locs <- get_location_metadata(location_set_id = 35, gbd_round_id = "ADDRESS", decomp_step = "ADDRESS")
locs <- locs[locs$is_estimate==1,]

### ======================= MAIN ======================= ###

###### Split India subnational parent from urban to rural ######
india_list <- read.csv(paste0(params_dir, "ADDRESS"))
india_parent <- unique(india_list$parent_id)
india_locs <- merge(india_list, locs, by="parent_id")
unique_india <- unique(india_locs$location_id)

### Loop through this list and output files for location ids that identify India subnats urban/rural
for (i in india_parent) {
  upload_ind <- read.csv(paste0(params_dir, lbd_draws_dir, "/", i, ".csv"))
  upload_ind$parent_id <- upload_ind$location_id
  test1 <- merge(upload_ind,india_locs, by="parent_id")
  
  ### Output to urban rural
  ind_ids <- unique(test1$location_id.y)
  
  for (j in ind_ids) {
    test2 <- test1[test1$location_id.y==j,]
    test2$location_id <- j
    
    ### Output location ids for India urban v. rural subnats to the main folder of draws produced by the geospatial model
    write.csv(test2,(paste0(params_dir, lbd_draws_dir, "/", j, ".csv")))
  }
}
draw.cols <- paste0("draw_", 0:999)


###### Get endemic draws from focal 3 input files ######
lf_geo <- read.csv(paste0(params_dir, "ADDRESS"), stringsAsFactors = FALSE)
presence_list <- subset(lf_geo, value_endemicity == 1 & most_detailed == 1)
unique_lf_locations <- unique(presence_list$location_id)

for (i in unique_lf_locations) {
  ### Pull location-specific draws from geospatial model results
  upload_file <- as.data.table(read.csv(paste0(params_dir, lbd_draws_dir, "/", i, ".csv")))
  
  ### Set draws to zero for < 1s
  upload_file[age_group_id %in% c(2, 3, 388, 389), (draw.cols) := 0]
  
  ### Fix names variable
  names(upload_file)[names(upload_file) == "year"] <- "year_id"
  
  upload_file2 <- copy(upload_file) # legacy
  upload_file3 <- copy(upload_file2) # legacy
  upload_file3$model_id<- ADDRESS
  
  ### Output to format for GBD save results
  write.csv(upload_file3,(paste0(draws_dir, "ADDRESS", i, ".csv")))
}


###### Output zero prevalence draws for non-endemic locations ######

### Generate a list of non-endemic locations
### Diff all locs to endemic locs
location_list <- unique(locs$location_id)
ne_locs <- location_list[! location_list %in% unique_lf_locations]

### Pull in all draws shell
### Use the file called "FILENAME" created above as the country shell, recode location_id and set all draws to zero
upload_file3$id <- 1:nrow(upload_file3)
for (i in ne_locs) {
  upload_file3$location_id <- i
  upload_file3[, (draw.cols) := 0, by=id]
  write.csv(upload_file3,(paste0(draws_dir, "ADDRESS", i, ".csv")))
}

