
### ---------------------------------------------------------------------------------------- ######
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

library(data.table)
library(plyr)
library(tidyr)
library(dplyr)
library(pbapply)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_population.R")

# 0. Settings ---------------------------------------------------------------------------
################################################################################
message("\nLoading settings...")

# Change the options in the block below as needed
gbd_year <- ADDRESS
release_id <- ADDRESS
map_version <- ADDRESS
model_description <- ADDRESS
xwalk_ver <- ADDRESS 
################################################################################
#### update the map data data and file name as needed
map_data_date <- "ADDRESS" 
map_file_name <- "ADDRESS"
ihme_output_date <- Sys.time() %>% gsub(" |:", "-",.)

save_results  <- FALSE 

years_wanted <- c(1980:2025) 
age_groups <- c(2,3,388,389,238,34,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32,235)

username <- Sys.info()["user"]

# Generate path to input data directory
map_input_data_dir <- paste0(FILEPATH)

# generate list of endemic locations 
end_map <- fread(paste0(params_dir, "/endemicity_config.csv"))
end_map <- as.data.table(end_map)

end_map <- end_map[ , pf_overall:=sum(pf_endemic), by=c("location_id", "location_name")]
end_map <- end_map[ ,pv_overall:=sum(pv_endemic), by=c("location_id", "location_name")]
end_map <- end_map[ ,end_overall:=sum(any_malaria_endemic), by=c("location_id", "location_name")]

end_map <- end_map[,.(location_id, location_name, pf_overall, pv_overall, end_overall)]
end_map <- end_map %>% group_by(location_id, location_name) %>% unique()
end_map <- as.data.table(end_map)
end_sub <- end_map[end_map$end_overall >0,]
end_locs <-unique(end_sub$location_id)

output_dir <- draws_dir

short_term_path <- paste0(FILEPATH)
dir.create(path = short_term_path, recursive = TRUE, showWarnings = FALSE)

################################################################################
# 1. Load objects -----------------------------------------------------------------------
message("\nLoading required objects...")

## age group config
age_group_M_config <- fread(paste0("FILEPATH/M_age-group-defs.csv"))
setnames(age_group_M_config, "M_age-def", "M_age.def")

# location hierarchy
loc_metadata <- get_location_metadata(location_set_id=ADDRESS, release_id = ADDRESS) 
loc_metadata <- loc_metadata[loc_metadata$level >= 3,]
locs_required <- unique(loc_metadata$location_id)

locs_required <- locs_required[!(locs_required %in% c(4841, 4842, 4843, 4844, 4846, 4849, 4850, 4851,
                                                   4852, 4853, 4854, 4855, 4856, 4857, 4859, 4860, 4861,
                                                   4862, 4863, 4864, 4865, 44538, 4867, 4868, 4869, 4870,
                                                   4871, 4872, 4873, 4874, 4875, 44793, 44794, 44795, 44796,
                                                   44797, 44798, 44799, 44800, 44533))]

# load in the Pf + Pv incidence table
message("- Loading the Pf+PV incidence draws")
pf_pv_incidence <- get(load(paste0(map_input_data_dir, map_file_name)))
rm(combined.table)

# 2. Load and format draws -----------------------------------------------------------

# format the incidence table properly (removing extraneous columns). 
# reformat age_group_id
# remove extra info not needed in the demographics file

message("\nFormatting, processing, and saving draws...")
message("- Formatting Pf+Pv incidence draws")
message("-- Checking demographics for any missing age groups, years, or locations")
pf_pv_incidence <- as.data.table(pf_pv_incidence)

pf_pv_inc_males <- pf_pv_incidence
pf_pv_inc_males$sex_id <- 1

pf_pv_inc_females <- pf_pv_incidence
pf_pv_inc_females$sex_id <- 2

pf_pv_inc_both <- rbind(pf_pv_inc_males, pf_pv_inc_females) %>%
  dplyr::select(ihme_location_id, year, ihme_age_group_id, sex_id, everything())

pf_pv_inc_both$iso2 <- NULL
pf_pv_inc_both$iso3 <- NULL
pf_pv_inc_both$admin_unit_name <- NULL
pf_pv_inc_both$admin_unit_level <- NULL
pf_pv_inc_both$country_name <- NULL

rm(pf_pv_inc_males, pf_pv_inc_females)

pf_pv_inc_both <- merge(pf_pv_inc_both, age_group_M_config, by.x = "ihme_age_group_id", by.y = "M_age.def")

pf_pv_inc_both$notes <- NULL
pf_pv_inc_both$ihme_age_group_id <- NULL
pf_pv_inc_both$age_group_name <- NULL

pf_pv_inc_both <- pf_pv_inc_both[pf_pv_inc_both$age_group_id %in% age_groups,]

setnames(pf_pv_inc_both, c("ihme_location_id", "year"), c("location_id", "year_id"))

# 3. Generate prevalence draws ----------------------------------------------------------

message("- Generating prevalence draws")

## need to create the prevalence dataframe by adjusting by duration of fever
pf_pv_prev_both <- pf_pv_inc_both

## Create a vector with 1000 draws for fever duration
duration = (runif(1000, min = 14, max = 28))/365

## Multiply the draw matrix by the duration vector 
## *** IMPORTANT: ensure that the maximum for prevalence is set at 1
pf_pv_prev_both[,4:1003] <- data.frame(mapply('*', pf_pv_prev_both[,4:1003], duration))
pf_pv_prev_both_draws <- pf_pv_prev_both[,4:1003]
pf_pv_prev_both_draws[pf_pv_prev_both_draws > 1] <- 1
front <- pf_pv_prev_both[,c(1:3,1004)]
pf_pv_prev_both <- cbind(front, pf_pv_prev_both_draws)

## now we have the prevalence and incidence draws for all malaria-endemic areas
## write out the files
message("- Saving incidence and prevalence draws")
 
message("-- Incidence...")
invisible(pblapply(unique(pf_pv_inc_both$location_id),
                   function(i, df=pf_pv_inc_both)
                   {
                     fwrite(df[df$location_id==i,],
                            file = file.path(short_term_path, paste(i,".csv", sep = "")),
                            row.names = FALSE)
                   }
))

message("-- Prevalence...")
invisible(pblapply(unique(pf_pv_prev_both$location_id),
                   function(i, df=pf_pv_prev_both)
                   {
                     fwrite(df[df$location_id==i,],
                            file = file.path(short_term_path, paste(i,".csv", sep = "")),
                            row.names = FALSE)
                   }
))

# 4. Data quality checks -------------------------------------------------------------

message("Data quality checks...")

message("-- Checking for missing locations")

missing_locs <- locs_required[!(locs_required %in% pf_pv_inc_both$location_id)]

missing_locs <- missing_locs[!(missing_locs %in% c(4841, 4842, 4843, 4844, 4846, 4849, 4850, 4851,
                                                   4852, 4853, 4854, 4855, 4856, 4857, 4859, 4860, 4861,
                                                   4862, 4863, 4864, 4865, 44538, 4867, 4868, 4869, 4870,
                                                   4871, 4872, 4873, 4874, 4875, 44793, 44794, 44795, 44796,
                                                   44797, 44798, 44799, 44800, 44533))]

missing_locs <- missing_locs[!(missing_locs %in% drop_locs$location_id),]

if(any(missing_locs != 0)) {
  
  missing_loc_names <- loc_metadata[location_id %in% missing_locs]$location_name
  
  message(paste0("--- Creating empty draw files for the following ", length(missing_locs), " missing locations: "))
  for (i in 1:length(missing_loc_names)) message(paste0("      ", i, ". ", missing_loc_names[i]))
  
  # Pick a reference file which can be filled with zeroes for all of the non-endemic locations
  template_file <-read.csv(paste0(short_term_path, "/160.csv"))
  template_file[, grep('draw', names(template_file))] <- 0
  
  ## incidence files
  invisible(lapply(unique(missing_locs),
                   function(i_loc) {
                     template_file_i <- copy(template_file)
                     template_file_i$location_id <- i_loc
                     write.csv(format(template_file_i, nsmall = 2),
                               file = file.path(short_term_path, paste(i_loc,'.csv',sep='')), 
                               row.names=F,
                               quote=F)
                   }))
  
  ## prevalence files
  invisible(lapply(unique(missing_locs),
                   function(i_loc) {
                     template_file_i <- copy(template_file)
                     template_file_i$location_id <- i_loc
                     write.csv(format(template_file_i, nsmall = 2),
                               file = file.path(short_term_path, paste(i_loc,'.csv',sep='')), 
                               row.names=F,
                               quote=F)
                   }))
  
  
}

## tests to determine if a) all the required files have been created and 
## b) that all of the printed files have the right pieces of information that we need

## a) making sure the right number of files have been written. 
## locations so total number of files is 2*number of required locations

message("-- Checking to make sure all required files have been created")

if(length(list.files(short_term_path)) < 2*length(locs_required)){
  message("--- Missing file for a required location")
} else{
  message("--- All files have been written")
}

## b) to make sure each file has all the required entries. Simplistically, this can be done by checking the number of rows and columns
## the command nrow(fread()) also will return a warning if there is an incomplete row/column so these warnings can be
## used to determine if a file has not been written out completely

message("-- Checking to make sure each file has all required entries")

wn_list <- pblapply(1:length(list.files(short_term_path)), function(i) {
  
  ## when the file has the right number of entries, wrn == TRUE
  ## when the file doesn't have the right number of entries wrn == FALSE
  ## when a row is incomplete, wrn is a list with two objects: a messages and a summary of the call
  wrn <- tryCatch(nrow(fread(paste(short_term_path, list.files(short_term_path)[i], sep = "/"))) ==  length(c(1,2))*length(age_groups)*length(years_wanted) & 
                    ncol(fread(paste(short_term_path, list.files(short_term_path)[i], sep = "/"))) == 1004, warning=function(w) w)  
  
  return(wrn)
})

# we anticipate 3 different outcomes for wn_list[[i]]. If TRUE (length(wn_list[[i]]) == 1), then the file contains everything it needs. 
# If FALSE (length(wn_list[[i]]) == 1), it is a rectangular file but is missing some rows/columns. 
# If length(wn_list[[i]]) >1, it is not a rectagular file and an error has been returned so the file should be checked
for (i in 1:length(list.files(short_term_path))){
  if(length(wn_list[[i]]) != 1){
    print(paste(list.files(short_term_path)[i], "is missing information. Please check file."))
  } else if(length(wn_list[[i]]) == 1 & !(wn_list[[i]])){
    print(paste(list.files(short_term_path)[i], "is missing information. Please check file."))
  }
}


# 5. Upload results ---------------------------------------------------------------------
# # 
 # if (save_results == TRUE) {
 # 
 #   message("Uploading results")
 # 
 #   index_df  <- save_results_epi(input_dir=short_term_path,
 #                                 input_file_pattern="{measure_id}_{location_id}.csv",
 #                                 modelable_entity_id=ADDRESS,
 #                                 description=paste0(username, " - ", model_description),
 #                                 release_id = release_id,
 #                                 mark_best=mark_as_best,
 #                                 bundle_id = ADDRESS, 
 #                                 crosswalk_version_id = xwalk_ver,
 #                                 year_id = years_wanted)
 # 
 # }
