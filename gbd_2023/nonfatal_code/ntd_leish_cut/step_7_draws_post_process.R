
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <- "FILEPATH"
data_root <- "FILEPATH"

# Source relevant libraries
library(dplyr)
library(data.table)
source("/FILEPATH/save_results_epi.R")
source("/FILEPATH/get_demographics.R")
source(paste0(code_root, "/FILEPATH/processing.R"))

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
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

# Set run dir
run_file <- fread(paste0(data_root, "/FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")
draws_dir    <- paste0(run_dir, "/draws/")
interms_dir    <- paste0(run_dir, "/interms/")

#Define constants
release_id <- release_id
meid <- ID

###########################################################################
##Pull estimation years
est_years <- get_demographics(gbd_team = 'ADDRESS', release_id = release_id)$year_id 

##Pull in geographic restrictions and drop non-endemic countries and drop restricted locations
draw.cols <- paste0("draw_", 0:999)

leish_endemic_cl <- fread(paste0(params_dir,"/FILEPATH"))
cl_locs <- unique(leish_endemic_cl[year_start == 2019 & most_detailed == 1, location_id])
cl_endemic_locs <- unique(leish_endemic_cl[value_endemicity == 1 & year_start == 2019 & most_detailed == 1, location_id])
cl_ne_locs <- setdiff(cl_locs, cl_endemic_locs)

#write out endemic locations
i <- 0

for(loc in cl_endemic_locs){
  
  inc_file <- fread(paste0(interms_dir, "/FILEPATH"))
  inc_file <- subset(inc_file, select = -c(totalPop, population, most_detailed, age_group_weight_value, age_group_years_start, age_group_years_end, age_group_name,
                                           age_group_name_short,age_group_alternative_name))

  prev_file <- fread(paste0(interms_dir, "/FILEPATH"))
  
  ## The following caps prevalence at draws >1 (this biases mean prevalence lower, so not ideal, needs further work in future)
  prev_file[, (paste0("draw_", 0:999)) := lapply(.SD, function(x) ifelse(x > 1.0, 0.999, x)), .SDcols=paste0("draw_", 0:999)]
  
  #prev_file <- subset(prev_file, select = -c(age_group_name, age_index_id))
  prev_file$modelable_entity_id <- ID
  prev_file$metric_id <- 3

  #append
  upload_file <- rbind(inc_file, prev_file)
  
  upload_file2<-
    upload_file %>% filter(year_id %in% est_years)
   
  #ensure prevalence < 6 months of age = zero per Mohsen's table
  draws <-setDT(upload_file2)
  draws[, id := .I]
  draws[age_group_id %in% c(2,3,388), (draw.cols) := 0, by=id]
  
  draws <- select(draws, -id)
  write.csv(draws,(paste0(draws_dir,"/FILEPATH")), row.names = F)  
  
  i <- i + 1
  cat(paste0("\n Done with: ", i, " of ", length(cl_locs)))
}

#write out non-endemic locations
zeros <- gen_zero_draws(modelable_entity_id = meid, location_id = NA, measure_id = c(5,6), metric_id = 3, release_id = release_id, team = 'ADDRESS', year_id = est_years)

for (loc in cl_ne_locs){
  
  zeros[, location_id := loc]
  zeros[, modelable_entity_id := as.integer(meid)]

  fwrite(zeros, paste0("FILEPATH"))
  
  i <- i + 1
  cat(paste0("\n Done with: ", i, " of ", length(cl_locs)))
}


#run save results --will run automatically in the cli
source("/FILEPATH/save_results_epi.R")
save_results_epi(input_dir =paste0(draws_dir, "FILEPATH"),
                 input_file_pattern = "{ID}.csv",
                 modelable_entity_id = ID,
                 description = "MESSAGE",
                 measure_id =c(5,6),
                 year_id = est_years,
                 release_id = release_id,
                 bundle_id=ID,
                 crosswalk_version_id=ID,
                 mark_best = FALSE)
