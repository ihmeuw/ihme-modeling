
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

library(readstata13)
library(tidyverse)
library(haven)
library(data.table)
library(dplyr)
library(openxlsx)
library(readxl)
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_population.R")

# 0. Settings ---------------------------------------------------------------------------
# Change the options in the block below as needed
gbd_year <- ADDRESS
release_id <- ADDRESS
################################################################################
map_data_date <- "ADDRESS" 
int_files <- "ADDRESS"
map_version <- "ADDRESS"
development <- FALSE 
date <- Sys.Date()
old_bundle_ver <- ADDRESS 
notes <- "ADDRESS"

path <- paste0("FILEPATH")
map_input_data_dir <- paste0(FILEPATH)

outpath <- paste0(FILEPATH)
interm_outpath <- paste0(FILEPATH)

dir.create(outpath, recursive = T, showWarnings = F) 
dir.create(interm_outpath, recursive = T, showWarnings = F) 

# 1. Load data ---------------------------------------------------------------------------
#load in required file(s) for reformatting 
age_group_metadata <- get_age_metadata(release_id = release_id)
age_group_M_config <- fread(paste0("FILEPATH/M_age-group-defs.csv"))
setnames(age_group_M_config, "M_age-def", "M_age.def")

demographics <- get_demographics(gbd_team = "epi", release_id = release_id)
dem <- get_demographics(gbd_team = "epi", release_id = release_id)

# location hierarchy
loc_metadata <- get_location_metadata(location_set_id=ADDRESS, release_id = release_id) 
loc_metadata <- loc_metadata[loc_metadata$level >=3,]

# load in Pf Incidence Table
################################################################################
pf_inc <- get(load(paste(map_input_data_dir, 'complete_inc_rate_age_split.Pf.RData', sep = "")))
rm(combined.table)

neonatal_enceph <- get_bundle_data(ADDRESS)
neonatal_enceph <- dplyr::filter(neonatal_enceph, measure == "mtexcess")
rm(bundle_df)

# filter to only those locations that we need
neonatal_enceph <- filter(neonatal_enceph, 
                          location_id %in% dem$location_id)

# 2. Format data ---------------------------------------------------------------------------
# begin formatting
pf_inc <- as.data.table(pf_inc)
nrow(pf_inc)

# specify locations
pf_inc_locs<-demographics$location_id

full_pf_inc <- mutate(pf_inc, 
                      ihme_age_group_id = age_group_M_config$age_group_id[match(pf_inc$ihme_age_group_id, age_group_M_config$M_age.def)]) %>%
  dplyr::select(location_id = ihme_location_id, 
         year_id = year, 
         age_group_id = ihme_age_group_id, 
         grep("draw",names(pf_inc))) %>% 
  dplyr::filter(age_group_id %in% c(3:8, 34, 238,388,389), 
         year_id %in% demographics$year_id)

# check you have the right age groups,
unique(full_pf_inc$age_group_id)


longterm_ratio <- abs(unlist(read.dta13(paste0(params_dir, "/longterm_ratio.dta"))))
pf_inc_subset <- full_pf_inc
pf_inc_subset <- as.data.frame(pf_inc_subset)
pf_inc_subset[, 4:1003] <- mapply("*", pf_inc_subset[, 4:1003], longterm_ratio)
stopifnot(min(pf_inc_subset[, 4:1003]) >= 0)


pf_inc_subset_males <- pf_inc_subset
pf_inc_subset_males$sex_id <- 1

pf_inc_subset_females <- pf_inc_subset
pf_inc_subset_females$sex_id <- 2

pf_inc_subset_both <- rbind(pf_inc_subset_males, pf_inc_subset_females) %>%
  select(location_id, year_id, age_group_id, sex_id, everything())


extra_age_groups <- demographics$age_group_id[!demographics$age_group_id %in% unique(pf_inc_subset_both$age_group_id)]
template_extra_agegroup <- dplyr::filter(pf_inc_subset_both, age_group_id == 3)
template_extra_agegroup[, grep('draw', names(template_extra_agegroup), value = TRUE)] <- 0

for(i in extra_age_groups){
  print(paste(which(extra_age_groups==i), "of", length(extra_age_groups)))
  extra_i <- template_extra_agegroup %>% 
    mutate(age_group_id = i)
  
  pf_inc_subset_both <- rbind(pf_inc_subset_both, extra_i)
}

# 3. Check for missing data ---------------------------------------------------------------------------
######### double-check that all the required rows are present
stopifnot(all(demographics$age_group_id %in% unique(pf_inc_subset_both$age_group_id)), 
          all(demographics$sex_id %in% unique(pf_inc_subset_both$sex_id)), 
          all(demographics$year_id %in% unique(pf_inc_subset_both$year_id)))

# 4. Save incidence ---------------------------------------------------------------------------
for (i in unique(pf_inc_subset_both$location_id)){
  print(paste(which(unique(pf_inc_subset_both$location_id) == i), "of", length(unique(pf_inc_subset_both$location_id))))
  write.csv(format(pf_inc_subset_both[pf_inc_subset_both$location_id == i,], nsmall = 2), 
            file = file.path(outpath,paste(i,".csv", sep = "")),
            row.names = FALSE)
}
files <- list.files(paste0(FILEPATH), full.names = TRUE, pattern = "6_*")
pf_inc_subset_both <- NULL
for (file in files){
  print(paste(which(files == file), "of", length(files)))
  temp_df <- fread(file)
  pf_inc_subset_both <- rbind(pf_inc_subset_both, temp_df)
}

missing_locs <- loc_metadata$location_id[!loc_metadata$location_id %in% pf_inc_subset_both$location_id]

template_file <- read.csv(paste(outpath, "/25318.csv", sep = ""))
template_file[, grep('draw',names(template_file))] <- 0

for(i in missing_locs){
  print(paste(which(missing_locs==i), "of", length(missing_locs)))
  
  template_file_i <- template_file
  template_file_i$location_id <- i
  
  write.csv(format(template_file_i, nsmall = 2), 
            file = file.path(outpath,paste(i,".csv", sep = "")),
            row.names = FALSE)
  
}

# 5. More data checks ---------------------------------------------------------------------------
## tests to determine if a) all the required files have been created and 
## b) that all of the printed files have the right pieces of information that we need

## a) making sure the right number of files have been written. 
if(length(list.files(outpath)) != length(demographics$location_id)){
  print("Missing file for a required location")
} else{
  print("All files have been written")
}

## b) to make sure each file has all the required entries. Simplistically, this can be done by checking the number of rows and columns
## the command nrow(fread()) also will return a warning if there is an incomplete row/column so these warnings can be
## used to determine if a file has not been written out completely

wn_list <- list()
for (i in 1:length(list.files(outpath))){
  print(paste(i, "of", length(list.files(outpath))))
  
  ## when the file has the right number of entries, wrn == TRUE
  ## when the file doesn't have the right number of entries wrn == FALSE
  ## when a row is incomplete, wrn is a list with two objects: a messages and a summary of the call
  wrn <- tryCatch(nrow(fread(paste(outpath, list.files(outpath)[i], sep = "/"))) ==  length(demographics$sex_id)*length(demographics$age_group_id)*length(demographics$year_id) & 
                    ncol(fread(paste(outpath, list.files(outpath)[i], sep = "/"))) == 1004, warning=function(w) w)  
  
  wn_list[[i]] <- wrn
  
}

# we anticipate 3 different outcomes for wn_list[[i]]. If TRUE (length(wn_list[[i]]) == 1), then the file contains everything it needs. 
# If FALSE (length(wn_list[[i]]) == 1), it is a rectangular file but is missing some rows/columns. 
# If length(wn_list[[i]]) >1, it is not a rectagular file and an error has been returned so the file should be checked
for (i in 1:length(list.files(outpath))){
  if(length(wn_list[[i]]) != 1){
    print(paste(list.files(outpath)[i], "is missing information. Please check file."))
  }  else if(length(wn_list[[i]]) == 1 & !wn_list[[i]]){
    print(paste(list.files(outpath)[i], "is missing information. Please check file."))
  }
}

# 6. Calculate prevalence ---------------------------------------------------------------------------
# PREVALENCE #####
pf_prev_subset <- pf_inc_subset

## calculate the mean and confidence interval of the new prevalence draws. Ensure that the mean is not greater than the upper bound

drawMeans <- function(dt, draws_columns = c(1:1000), metadata_columns = NULL, stderr = FALSE) {
  
  if(is.null(metadata_columns)){
    metadata_columns <- which(names(dt) %in% names(dt[,-(1:1000)]))
  }
  
  if(inherits(dt, "data.table") == FALSE) {
    dt <- data.table::as.data.table(dt)
  }
  
  if(stderr == TRUE){
    dt_temp <- dt[, .(mean = rowMeans(.SD),
                      lower = apply(.SD, 1, quantile, probs = 0.025),
                      upper = apply(.SD, 1, quantile, probs = 0.975),
                      standard_error = apply(.SD, 1, function(x) sd(x)/sqrt(length(x)))),
                  .SDcols = draws_columns]
    
  }else {
    
    dt_temp <- dt[, .(mean = rowMeans(.SD),
                      lower = apply(.SD, 1, quantile, probs = 0.025),
                      upper = apply(.SD, 1, quantile, probs = 0.975)),
                  .SDcols = draws_columns]
  }
  
  dt_output <- cbind(dt[,.SD,.SDcols = metadata_columns],dt_temp)
  
  return(dt_output)
  
}

pf_prev_means <- data.frame(drawMeans(pf_prev_subset,draws_columns = c(4:1003), metadata_columns = c(1:3)))
pf_prev_means$mean[pf_prev_means$mean > pf_prev_means$upper] <- pf_prev_means$upper[pf_prev_means$mean > pf_prev_means$upper] 

# 7. Format prevalence for the bundle ---------------------------------------------------------------------------
### create the formatted bundle
pf_prev_bundle <- mutate(pf_prev_means, 
                         bundle_id = ADDRESS,
                         nid = ADDRESS, 
                         input_type = ADDRESS, 
                         source_type = "ADDRESS", 
                         smaller_site_unit = ADDRESS, 
                         sex_issue = ADDRESS, 
                         year_issue = ADDRESS, 
                         age_issue = ADDRESS, 
                         age_demographer = ADDRESS, 
                         measure = "ADDRESS", 
                         unit_type = "ADDRESS", 
                         unit_value_as_published = ADDRESS, 
                         measure_issue = ADDRESS, 
                         measure_adjustment = ADDRESS, 
                         uncertainty_type = "ADDRESS", 
                         uncertainty_type_value = ADDRESS, 
                         urbanicity_type = "ADDRESS", 
                         recall_type = "ADDRESS", 
                         extractor = "ADDRESS", 
                         is_outlier = ADDRESS, 
                         representative_name = "ADDRESS", 
                         age_start = age_group_metadata$age_group_years_start[match(pf_prev_means$age_group_id, age_group_metadata$age_group_id)], 
                         age_end = age_group_metadata$age_group_years_end[match(pf_prev_means$age_group_id, age_group_metadata$age_group_id)], 
                         year_start = year_id, 
                         year_end = year_id,
                         sex = "ADDRESS", 
                         extractor = "ADDRESS") %>% 
  select(-age_group_id, -year_id)

## match the prevalence formatting to the columns needed in the neonatal encephalopathy file
missing_cols <- names(neonatal_enceph)[which(is.na(match(names(neonatal_enceph), names(pf_prev_bundle))))]

empty_columns <- as.data.frame(matrix(NA, ncol = length(missing_cols), nrow = dim(pf_prev_bundle)[1]))
names(empty_columns) <- missing_cols

pf_prev_bundle <- cbind(pf_prev_bundle, empty_columns)

## remove bundle_id column from pf_prev
pf_prev_bundle <- pf_prev_bundle[,-5]
neonatal_enceph <- as.data.frame(neonatal_enceph)
bundle_filled <- rbind(pf_prev_bundle, neonatal_enceph[names(pf_prev_bundle)])
## different number of columns

bundle_filled$seq <- NA
bundle_filled$response_rate <- NA
bundle_filled$mean[bundle_filled$mean == 0] <- 10^(-17)
bundle_filled$upper[bundle_filled$upper == 0] <- 9^(-17)
bundle_filled$lower[bundle_filled$lower == 0] <- 10^(-18)

formatted_date <- gsub("-", "_", Sys.Date()) # changing dash to underscore

## final bundle checks to make sure didn't actually drop data
# read in old bundle version
old_bundle <- get_bundle_version(ADDRESS)

if (nrow(old_bundle) > nrow(bundle_filled)){
  print("STOP! New bundle is missing data!")
}

test <- anti_join(bundle_filled, old_bundle)
test2 <- test <- bundle_filled[bundle_filled %in% old_bundle]

# 8. Save files and params ---------------------------------------------------------------------------
write.xlsx(bundle_filled, file = paste0(FILEPATH), sheetName = 'ADDRESS')
write.csv(bundle_filled, file = paste0(FILEPATH), row.names = FALSE)
## write out the file to upload.
write.xlsx(bundle_filled,
           file = paste0(interm_outpath,'malaria_data.csv'),
           row.names = FALSE,
           na = "")

