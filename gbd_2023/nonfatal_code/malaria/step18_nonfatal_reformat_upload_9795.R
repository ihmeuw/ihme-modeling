
## ------------------------------------------------------------------------------------------##
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
source('FILEPATH/get_draws.R')
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_location_metadata.R")

# 0. Settings ---------------------------------------------------------------------------
## use the model_version_id from DisMod
################################################################################
## upload model version
latest_model_version = ADDRESS
map_version <- "ADDRESS"
dismod_me_id <- ADDRESS
final_me_id <- ADDRESS
release_id <- ADDRESS 
gbd_year <- ADDRESS
################################################################################
## upload xwalk version
xwalk_ver <- ADDRESS 
date <- Sys.Date()

loc_metadata <- get_location_metadata(location_set_id=ADDRESS, release_id = release_id)
loc_metadata <- loc_metadata[loc_metadata$level >= 3,]
# Options for saving results
save_results <- TRUE 
mark_as_best <- TRUE
username <- Sys.info()["user"]
model_description <- "ADDRESS"

path_finalme <- paste0(ADDRESS)
params_path <- paste0(ADDRESS)
interm_finalme <- paste0(ADDRESS)
years_needed <- c(1990:2025) 

file_tag <- gsub("-","_",Sys.Date())

download <- TRUE 
 
# # 1. Load data ---------------------------------------------------------------------------
demographics <- as.data.table(get_demographics(gbd_team = "ADDRESS", release_id = release_id))

# # load in endemicity file (for zero-ing out non_endemic regions)
endemicity <- fread(paste0(params_dir, "/endemicity_config.csv"))
endemicity <- as.data.table(endemicity)

endemicity_sub <- endemicity[ , pf_overall:=sum(pf_endemic), by=c("location_id", "location_name")]
endemicity_sub <- endemicity_sub[ ,pv_overall:=sum(pv_endemic), by=c("location_id", "location_name")]
endemicity_sub <- endemicity_sub[ ,end_overall:=sum(any_malaria_endemic), by=c("location_id", "location_name")]

endemicity_sub <- endemicity_sub[,.(location_id, location_name, pf_overall, pv_overall, end_overall)]
endemicity_sub <- endemicity_sub %>% group_by(location_id, location_name) %>% unique()
endemicity_sub <- as.data.table(endemicity_sub)
endemicity_sub <- endemicity_sub[endemicity_sub$end_overall >0,]

ever_endemic_only <- endemicity[endemicity$location_id %in% endemicity_sub$location_id,]
ever_endemic_only <- ever_endemic_only[ever_endemic_only$year_id %in% demographics$year_id,]

if (download == TRUE){
 query_locations <- unique(ever_endemic_only$location_id)

 draws_endemic <- get_draws(gbd_id = ADDRESS,
                                 source = "ADDRESS",
                                 gbd_id_type = "ADDRESS",
                                 location_id = query_locations,
                                 version_id = latest_model_version,
                                 measure_id = ADDRESS,
                                 release_id = release_id,
                                 year_id = years_needed)

 save(draws_endemic,
      file = file.path(interm_finalme, paste0("draws_endemic_",file_tag,".RData")))

} else{ 
 draws_endemic <- as.data.frame(get(load(paste0(interm_finalme, paste0("draws_endemic_", file_tag, ".RData")))))

}

# # 2. Format data ---------------------------------------------------------------------------
endemicity_full <- ever_endemic_only
endemicity_full$age_group_id <- unique(draws_endemic$age_group_id)[1]

for(i in unique(draws_endemic$age_group_id)[2:length(unique(draws_endemic$age_group_id))]){
 print(i)
 endemicity_i <- ever_endemic_only
 endemicity_i$age_group_id <- i
 endemicity_full <- rbind(endemicity_full, endemicity_i)
}

endemicity_full_males <- endemicity_full
endemicity_full_males$sex_id <- 1

endemicity_full_females <- endemicity_full
endemicity_full_females$sex_id <- 2

endemicity_full_both <- rbind(endemicity_full_males, endemicity_full_females)

# ##need to subset to only age groups needed so the data tables will be the same length
draws_endemic <- draws_endemic[!(draws_endemic$age_group_id %in% c(164,27,33)),]
endemicity_full_both <- endemicity_full_both[!(endemicity_full_both$age_group_id %in% c(164,27,33)),]

# #clear up space
rm(endemicity_full_males, endemicity_full_females, endemicity_full)

# # 3. Run checks to make sure endemicity file equal draw file ---------------------------------------------------------------------------
# ## check to make sure the zero-ing out endemicity file matches the draws by a) nrows and b) order
stopifnot(nrow(endemicity_full_both) == nrow(draws_endemic))

endemicity_full_both <- endemicity_full_both[with(endemicity_full_both, order(location_id, year_id, age_group_id, sex_id)),]
draws_endemic <- draws_endemic[with(draws_endemic, order(location_id, year_id, age_group_id, sex_id)),]

stopifnot(sum(draws_endemic$location_id - endemicity_full_both$location_id) == 0 | sum(draws_endemic$age_group_id - endemicity_full_both$age_group_id) == 0 | sum(draws_endemic$year_id - endemicity_full_both$year_id) == 0)

draws_endemic_zeroed <- as.data.frame(draws_endemic)
draws_endemic_zeroed[, grep("draw", names(draws_endemic_zeroed))] <- draws_endemic_zeroed[,grep("draw", names(draws_endemic_zeroed))]*endemicity_full_both$pf_endemic

# # 4. Transform draws to correct format ---------------------------------------------------------------------------
# # Remove unnecessary columns and age_groups
draws_endemic_output <- draws_endemic_zeroed[, c("location_id", "year_id", "age_group_id", "sex_id", grep("draw", names(draws_endemic_zeroed), value = TRUE))]
draws_endemic_output <- filter(draws_endemic_output, age_group_id %in% demographics$age_group_id)

# ## *** IMPORTANT: ensure that the maximum for prevalence is set at 1
# ## Check that all demographic requirements (age groups, year, sex) are met
draws_endemic_output <- as.data.table(draws_endemic_output)

stopifnot(all(between(draws_endemic_output[,grep("draw", names(draws_endemic_output))], 0, 1)))

stopifnot(all(demographics$age_group_id %in% unique(draws_endemic_output$age_group_id)),
         all(demographics$sex_id %in% unique(draws_endemic_output$sex_id)),
         all(demographics$year_id %in% unique(draws_endemic_output$year_id)))


sub <- draws_endemic_output[draws_endemic_output$age_group_id ==2,]
sub[,grep('draw',names(sub))] <- 0
draws_endemic_output <- draws_endemic_output[!(draws_endemic_output$age_group_id ==2),]

draws_endemic_output <- rbind(draws_endemic_output, sub)

rm(sub)

# # 5. Save out data ---------------------------------------------------------------------------
for (i in unique(draws_endemic_output$location_id)){
 print(paste(which(unique(draws_endemic_output$location_id)==i), "of", length(unique(draws_endemic_output$location_id))))
 write.csv(format(draws_endemic_output[draws_endemic_output$location_id==i,], nsmall = 2),
           file = file.path(path_finalme,paste(i,".csv", sep = "")),
           row.names = FALSE)
}

non_endemic_locs <- loc_metadata$location_id[!loc_metadata$location_id %in% ever_endemic_only$location_id | !loc_metadata$location_id %in% draws_endemic$location_id]

# #check for duplicate locs 
dup_locs <- draws_endemic_output[draws_endemic_output$location_id %in% non_endemic_locs,]
stopifnot(nrow(dup_locs) == 0)

template_file <- read.csv(file.path(path_finalme,"/7.csv"))
template_file[,grep('draw',names(template_file))] <- 0

for(i in non_endemic_locs){
 print(paste(which(non_endemic_locs==i), "of", length(non_endemic_locs)))

 template_file_i <- template_file
 template_file_i$location_id <- i

 write.csv(format(template_file_i, nsmall = 2),
           file = file.path(path_finalme,paste(i,".csv", sep = "")),
           row.names = FALSE)

}

# # 6. Run some data quality checks ---------------------------------------------------------------------------
# ## tests to determine if a) all the required files have been created and
# ## b) that all of the printed files have the right pieces of information that we need
#
# ## a) making sure the right number of files have been written. 
# ## locations so total number of files = length(demographics$location_id)*2 (to account for incidence files already written out)
if(length(list.files(path_finalme)) != length(loc_metadata$location_id)*2 & length(list.files(path_finalme)) < length(loc_metadata$location_id)*2){
 print("Missing file for a required location")
} else{
 print("All files have been written")
}
prev <- list.files(path_finalme, pattern= "ADDRESS")
inc <- list.files(path_finalme, pattern= "ADDRESS")
inc <- sub('.*_', '', inc)
prev <- sub('.*_', '', prev)

# #change this depending on which has fewer files
missing <- prev[!(prev %in% inc)]

missing <- sub(".csv.*", "", missing)
missing <- as.integer(missing)

# #will need to write these out using another non-endemic file
missing_nonend <- missing[missing %in% non_endemic_locs]

missing_end <- missing[!(missing %in% missing_nonend)]
missing_end <- missing_end[!(missing_end %in% c(11,130,135,142,16,163,165,179,180,196,214,6))]
missing_nonend <- missing_nonend[!(missing_nonend %in% c(11,130,135,142,16,163,165,179,180,196,214,6))]
# #after removing end locs that are fine to be missing, check if there are any remaining
stopifnot(nrow(missing_end) == 0)

# #now write out missing non-endemic files
template_file <- read.csv(file.path(path_finalme,"/7.csv"))
template_file[,grep('draw',names(template_file))] <- 0

for(i in missing_nonend){
 print(paste(which(missing_nonend==i), "of", length(missing_nonend)))

 template_file_i <- template_file
 template_file_i$location_id <- i

 write.csv(format(template_file_i, nsmall = 2),
           file = file.path(path_finalme,paste(i,".csv", sep = "")),
           row.names = FALSE)

}

# ## b) to make sure each file has all the required entries. Simplistically, this can be done by checking the number of rows and columns
# ## the command nrow(fread()) also will return a warning if there is an incomplete row/column so these warnings can be
# ## used to determine if a file has not been written out completely

wn_list <- list()
for (i in 1:length(list.files(path_finalme))){
 print(paste(i, "of", length(list.files(path_finalme))))

#   ## when the file has the right number of entries, wrn == TRUE
#   ## when the file doesn't have the right number of entries wrn == FALSE
#   ## when a row is incomplete, wrn is a list with two objects: a messages and a summary of the call
 wrn <- tryCatch(nrow(fread(paste(path_finalme, list.files(path_finalme)[i], sep = "/"))) ==  length(demographics$sex_id)*length(demographics$age_group_id)*length(demographics$year_id) &
                   ncol(fread(paste(path_finalme, list.files(path_finalme)[i], sep = "/"))) == 1004, warning=function(w) w)

 wn_list[[i]] <- wrn

}

# # we anticipate 3 different outcomes for wn_list[[i]]. If TRUE (length(wn_list[[i]]) == 1), then the file contains everything it needs.
# # If FALSE (length(wn_list[[i]]) == 1), it is a rectangular file but is missing some rows/columns.
# # If length(wn_list[[i]]) >1, it is not a rectagular file and an error has been returned so the file should be checked
for (i in 1:length(list.files(path_finalme))){
 if(length(wn_list[[i]]) != 1){
   print(paste(list.files(path_finalme)[i], "is missing information. Please check file."))
 }  else if(length(wn_list[[i]]) == 1 & !wn_list[[i]]){
   print(paste(list.files(path_finalme)[i], "is missing information. Please check file."))
 }
}
length(wn_list[wn_list!=1])


file.copy(from = paste0(incidence_path, list.files(incidence_path)), to = paste(file.path(path_finalme), list.files(incidence_path), sep = "/"))

# 7. Upload results ---------------------------------------------------------------------
if (save_results == TRUE) {

  message("Uploading results")

  index_df  <- save_results_epi(input_dir=path_finalme,
                                input_file_pattern="{measure_id}_{location_id}.csv",
                                modelable_entity_id=final_me_id,
                                description=paste0(username, " - ", model_description),
                                release_id = release_id,
                                mark_best=mark_as_best,
                                bundle_id = ADDRESS,
                                crosswalk_version_id = xwalk_ver)

}
