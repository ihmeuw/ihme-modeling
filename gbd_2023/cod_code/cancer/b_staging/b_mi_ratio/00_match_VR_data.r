################################################################################
## Description: Takes downloaded VR data and matches it to our prepped CR data
##              based on the following criteria
##              - only new causes
##              - merges on full coverage registries
##              - matches on year_start, year_end, location_id
##              - aggregates pop and death for when year_span < 1
##              - saves matched VR aggregations
##
##
## Input(s): None
## Outputs(s): Saves matched formatted files in the compiled prep folder
## How to Use: Run script from cluster using Rscript path_to_script
## Contributors: REDACTED
################################################################################

################################################################################
##				Load Libraries
################################################################################
library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r')) # Loads utilities functions, eg. get_path
source(file.path(code_repo, '_database/cdb_utils.r'))
library(data.table)
library(tidyr)
library(readstata13)
library(parallel)
library(assertthat)

################################################################################
##				Main Functions
################################################################################

aggregate_VR_years <- function(year_span, VR_df){
    # Worker function that aggregates VR data based on the current 
    # year span and location
    #
    # Args:
    #   year_span: list of 3 parts, [1] location, [2] year_start, [3] year_end
    #   VR_df: dataframe of VR data to subset and match and aggregate 
    #
    # Returns:
    #   aggregated matched VR data for thast year span and loc
    
    matchVR_df <- VR_df[location_id == year_span[1]]
    # check to make sure VR data has all the years in year span then aggregate
    if(setdiff(c(year_span[2]:year_span[3]), matchVR_df$year_id %>% unique) %>% length == 0){
        matchVR_df <- matchVR_df[year_id %in% c(year_span[2]:year_span[3])]
        matchVR_df <- matchVR_df[, lapply(.SD, FUN = sum), 
                                 .SDcols = c("deaths", "population"), 
                                 by = c('acause', 'age', 'location_id', 'sex_id')]
        matchVR_df$year_start <- year_span[2]
        matchVR_df$year_end <- year_span[3]
        print(paste0("Matched location ", year_span[1], " year ", year_span[2], " to ", year_span[3]))
    } else{
        # give an empty dataframe
        matchVR_df <- matchVR_df[year_id == 100000]
        print(paste0("No matches for location ", year_span[1], " year ", year_span[2], " to ", year_span[3]))
    }
    
    return(matchVR_df)
}


match_data <- function(VR_df, compiled_inc){
    # Main matching function that garners all matches on VR data in parallel, 
    # compiles and formats dataset
    #
    # Args:
    #   VR_df: dataframe of VR data to subset and match and aggregate 
    #   compiled_inc: dataframe of our compiled prep incidence file 
    #
    # Returns:
    #   dataframe of all of our matched VR data
    
    # subset on new causes, and get distinct registry-years
    new_causes <- c("neo_bone","neo_eye","neo_eye_other","neo_eye_rb",
                    "neo_liver_hbl","neo_lymphoma_other","neo_lymphoma_burkitt",
                    "neo_neuro","neo_tissue_sarcoma")
    
    inc_vr_years <- compiled_inc[year_start != year_end]
    inc_vr_years <- inc_vr_years[acause %in% new_causes]
    inc_vr_years <- inc_vr_years[, list(registry_index, year_start, year_end)] %>% unique()
    
    # merge on full coverage of location for prepped inc
    registry <- cdb.get_table("cancer_db.registry")
    setDT(registry)
    registry <- registry[coverage_of_location_id == 1]
    inc_vr_years <- merge(inc_vr_years, registry[, list(location_id, registry_index)], by = "registry_index")

    # get list of location year_start year_end combos
    year_span_combo <- inc_vr_years[, list(location_id, year_start, year_end)] %>% unique %>% as.list() %>% transpose
    
    # subset VR data on available locations and new causes from formatted inc file
    VR_df_format <- VR_df[acause %in% new_causes & location_id %in% (inc_vr_years$location_id %>% unique)]
    
    # get all matches
    #all_matched_list <- mclapply(year_span_combo, FUN = function(x){
    #                                aggregate_VR_years(x, VR_df_format)}, mc.cores = detectCores() - 2)
    
    all_matched_list <- lapply(year_span_combo, FUN = function(x){ # if running nonparallel
                                        aggregate_VR_years(x, VR_df_format)})
    all_matched <- rbindlist(all_matched_list, fill = T)
    uid_cols <- c('acause', 'age', 'location_id', 'sex_id', 'year_start', 'year_end')
    
    # get single year matches based on same criteria, full coverage registry, and matching year, and loc
    VR_df[, year_start := year_id]
    VR_df[, year_end := year_id]
    single_yr_inc <- compiled_inc[year_start == year_end]
    single_yr_inc <- merge(single_yr_inc, registry[, list(registry_index, location_id)], by = "registry_index")
    single_yr_inc <- single_yr_inc[, list(year_start, year_end, location_id)] %>% unique
    VR_df_format_single <- merge(VR_df[acause %in% new_causes & 
                                           location_id %in% (single_yr_inc$location_id %>% unique)], 
                                           single_yr_inc)

    print("Done with single year matching")
    
    # add in single year matches to total
    all_matched <- rbindlist(list(all_matched, VR_df_format_single), fill = T)
    
    # adding in VR registry indexes from database table
    all_matched$registry_index <- NULL
    all_matched<- merge(all_matched, registry[grepl("VR", registry_name), 
                                              list(registry_index, location_id)], all.x = T, by = "location_id")
    
    assert_that(all_matched[duplicated(all_matched[, (uid_cols), with = F])] %>% nrow == 0, 
                                msg = "Duplicates exist!")
    print("Done with matching!")
    
    # add necessary columns
    all_matched$nid <- 0
    all_matched$underlying_nid <- 0
    all_matched$dataset_id <- 10
    return(all_matched)
}


main <- function(refresh_id){
    # Main function that reads in our downloaded VR data and compiled prep incidence
    # files, and then calls matching function, then saves matched aggregated VR data
    #
    # Args: refresh_id - int, refresh version from CoD team for their VR data
    #
    # Returns:None
    
    # load in VR data for new causes
    VR_df <- fread(paste0(get_path(process='mi_dataset', key='processing_storage'),
                '/CoD/CoD_VR_ICD10/new_causes_VR_mor_refresh', refresh_id, '.csv'))

    # load in compiled prep
    staged_inc_id <- cdb.get_table("cancer_db.staged_incidence_version")
    staged_inc_id <- max(staged_inc_id$staged_incidence_version_id)
    compiled_inc <- fread(paste0(get_path(process='staging', key = 'mi_dataset_aggregation'),
                        "/appended_p7_inc_version", staged_inc_id, ".csv"))
    # match and save
    save_df <- match_data(VR_df = VR_df, compiled_inc = compiled_inc)
    save_df <- save_df[, c("registry_index", "location_id", "acause", "age", "sex_id", "year_start","year_end",
                           "deaths", "population", "dataset_id", "nid", "underlying_nid"), with = F]
    save.dta13(save_df, paste0(get_path(process='staging', key = 'mi_dataset_aggregation'), 
                            "/VR_complete_with_aggregations_version", staged_inc_id, ".dta"))
}

# run matcching
if (!interactive()){
    refresh_id <- REDACTED
    main(refresh_id)
}