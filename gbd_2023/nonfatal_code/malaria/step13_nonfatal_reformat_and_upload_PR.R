
################################################################################
### ========================= BOILERPLATE ========================= ###

rm(list=ls())
data_root <- "FILEPATH"
cause <- "malaria"
run_date <- "ADDRESS"

## Define paths 
# Toggle btwn production arg parsing vs interactive development
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
  params_dir  <- paste0(FILEPATH)
  draws_dir   <- paste0(FILEPATH)
  interms_dir <- paste0(FILEPATH)
  logs_dir    <- paste0(FILEPATH)
}


library(dplyr)
library(data.table)
library(pbapply)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_population.R")

# 0. Settings ---------------------------------------------------------------------------

message("\nLoading settings...")

# Change the options in the block below as needed

gbd_year <- ADDRESS
release_id <- ADDRESS
run_reason <- "ADDRESS" 
xwalk_ver <- ADDRESS 
species_list <- c("Pv", "Pf") 
 
################################################################################

years_wanted <- c(1990:2025)
age_groups <- c(2,3,388,389,238,34,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)
date <- Sys.Date()

# Options for saving results
save_results <- FALSE 
mark_as_best <- FALSE
username <- Sys.info()["user"]
model_description <- "ADDRESS"

map_input_data_dir <- paste0(FILEPATH)
#map_input_age_def <- paste0(FILEPATH)

output_dir <- paste0(FILEPATH)

# # 1. Load objects -----------------------------------------------------------------------
message("\nLoading required objects...")

# age group config 
age_group_M_config <- fread(paste0("FILEPATH/M_age-group-defs.csv"))
setnames(age_group_M_config, "M_age-def", "M_age.def")

# location hierarchy
loc_metadata <- get_location_metadata(location_set_id=ADDRESS, release_id = ADDRESS)
loc_metadata <- loc_metadata[loc_metadata$level >= 3,]

# 2. Loop over Pf and Pv ----------------------------------------------------------------

for(species in species_list) {
  
  message(paste0("\nProcessing species: ", species))
  
  # Choose depending on whether Pf or Pv
  if(species == "Pf"){
    deliverable <- ADDRESS
    number <- 2
  }else if(species == "Pv"){
    deliverable <- ADDRESS
    number <- 3
  }else{
    print("Check spelling of species, should be Pf or Pv")
  }
  
  ## Load species PR table
  message("- Loading species PR table")
  load(paste0(map_input_data_dir, "combined_PRfinebins.", species, ".RData"))
  combined_PRfinebins <- p.table
  rm(p.table)
  full_pr <- combined_PRfinebins
  # 2a. Load and format draws -----------------------------------------------------------
  message("- Formatting species PR table")
  
  full_pr <- merge(full_pr, age_group_M_config, by.x = "ihme_age_group_id", by.y = "M_age.def")
  full_pr$notes <- NULL
  full_pr$ihme_age_group_id <- NULL
  
  full_pr <- full_pr %>% dplyr::select(ihme_location_id, year, age_group_id,
                                       grep("draw",names(full_pr)))
  
  setnames(full_pr, c("ihme_location_id","year"), c("location_id", "year_id"))
  
  pr_subset_zeroes <- full_pr
  
  pr_subset_males <- pr_subset_zeroes
  pr_subset_males$sex_id <- 1
  
  pr_subset_females <- pr_subset_zeroes
  pr_subset_females$sex_id <- 2
  
  pr_subset_both <- rbind(pr_subset_males, pr_subset_females) %>%
    dplyr::select(location_id, year_id, age_group_id, sex_id, everything())
  rm(pr_subset_males, pr_subset_females)
  
    
  stopifnot(all(between(as.matrix(pr_subset_both[,grep("draw", names(pr_subset_both))]), 0, 1)))
  
  # 2b. Write draw files ----------------------------------------------------------------
  message("- Writing draw files (one for each location)")
  # write out PR files
  anaemia_path <- paste0(output_dir, "/", deliverable, "/")
  dir.create(anaemia_path, recursive = TRUE, showWarnings = FALSE)
  
  pr_subset_both <- unique(pr_subset_both)
  
  invisible(pblapply(unique(pr_subset_both$location_id),
                     function(i, df=pr_subset_both)
                     {
                       fwrite(format(pr_subset_both[pr_subset_both$location_id == i,], nsmall = 2),
                              file = file.path(anaemia_path,paste(i, ".csv", sep = "")),
                              row.names = FALSE)
                     }
  ))
  
  # 2c. Data quality checks -------------------------------------------------------------
  
  message("- Data quality checks")
  message("-- Checking for missing locations")
  
  locs_required <- loc_metadata$location_id
  missing_locs <- locs_required[!(locs_required %in% pr_subset_both$location_id)]
  
  missing_locs <- missing_locs[!(missing_locs %in% c(4841, 4842, 4843, 4844, 4846, 4849, 4850, 4851,
                                                     4852, 4853, 4854, 4855, 4856, 4857, 4859, 4860, 4861,
                                                     4862, 4863, 4864, 4865, 44538, 4867, 4868, 4869, 4870,
                                                     4871, 4872, 4873, 4874, 4875, 44793, 44794, 44795, 44796,
                                                     44797, 44798, 44799, 44800, 44533))]
  
  missing_locs <- missing_locs[!(missing_locs %in% drop_locs$location_id),]
  
  if(any(missing_locs != 0)){
    
    missing_loc_names <- loc_metadata[location_id %in% missing_locs]$location_name
    
    message(paste0("---- Creating empty draw files for the following ", length(missing_locs), " missing locations: "))
    for (i in 1:length(missing_loc_names)) message(paste0("      ", i, ". ", missing_loc_names[i]))
    
    template_file <- read.csv(file.path(anaemia_path,"/7.csv"))
    template_file[, grep('draw',names(template_file))] <- 0
    template_file <- unique(template_file)
    
    invisible(lapply(unique(missing_locs),
                     function(i, df= pr_subset_both)
                     {
                       template_file_i <- template_file
                       template_file_i$location_id <- i
                       write.csv(format(template_file_i, nsmall = 2),
                                 file = file.path(anaemia_path,paste(i, ".csv", sep = "")),
                                 row.names = FALSE)
                     }
    ))
    
  }
  
  # Run tests to determine if
  # a) all the required files have been created and
  # b) that all of the printed files have the right pieces of information that we need
  # c) making sure the right number of files have been written. 
  
  message("-- Checking to make sure all required files have been created")
  
  if(length(list.files(anaemia_path)) < length(loc_metadata$location_id)){
    message("--- Missing file for a required location")
  } else if (length(list.files(anaemia_path)) > length(loc_metadata$location_id)){
    message("--- There are more locations than required")
  }else{
    message("--- All files have been written")
  }
  
 
  message("-- Checking to make sure each file has all required entries")
  
  wn_list <- pblapply(1:length(list.files(anaemia_path)), function(i) {
       
    ## when the file has the right number of entries, wrn == TRUE
    ## when the file doesn't have the right number of entries wrn == FALSE
    ## when a row is incomplete, wrn is a list with two objects: a messages and a summary of the call
    wrn <- tryCatch(nrow(fread(paste(anaemia_path, list.files(anaemia_path)[i], sep = "/"))) ==  length(demographics$sex_id)*length(age_groups)*length(years_wanted) &
                      ncol(fread(paste(anaemia_path, list.files(anaemia_path)[i], sep = "/"))) == 1004, warning=function(w) w)
    
    return(wrn)
    
  })
  
  # we anticipate 3 different outcomes for wn_list[[i]]. If TRUE (length(wn_list[[i]]) == 1), then the file contains everything it needs.
  # If FALSE (length(wn_list[[i]]) == 1), it is a rectangular file but is missing some rows/columns.
  # If length(wn_list[[i]]) >1, it is not a rectagular file and an error has been returned so the file should be checked
  for (i in 1:length(list.files(anaemia_path))){
    if(length(wn_list[[i]]) != 1){
      print(paste(list.files(anaemia_path)[i], "is missing information. Please check file."))
    }  else if(length(wn_list[[i]]) == 1 & !wn_list[[i]]){
      print(paste(list.files(anaemia_path)[i], "is missing information. Please check file."))
    }
  }
  
  message(paste0("- Done with species ", species))
  
    
} # Close loop over species_list
################################################################################
# # 3. Upload results ---------------------------------------------------------------------
if (save_results == TRUE) {
  
  message("Uploading results")
  
  for (species in species_list) {
    
    if(species == "Pf") {
      deliverable <- ADDRESS
    } else if(species == "Pv") {
      deliverable <- ADDRESS
    } else {
      print("Check spelling of species, should be Pf or Pv")
    }
    
    message(paste0("- ", species, "(", deliverable, ")"))
    draw_path <- paste0(output_dir, "/", deliverable, "/")
    
    index_df  <- save_results_epi(input_dir=draw_path,
                                  input_file_pattern="{measure_id}_{location_id}.csv",
                                  modelable_entity_id=deliverable,
                                  description=paste0(username, " - ", species, "PR for anemia - ", model_description),
                                  measure_id=ADDRESS,
                                  release_id = release_id,
                                  mark_best=mark_as_best,
                                  bundle_id = ADDRESS, 
                                  crosswalk_version_id = xwalk_ver,
                                  year_id = years_wanted)

  }
}
