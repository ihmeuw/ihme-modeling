##########################################################################
### Author: USERNAME
### Project: GBD Nonfatal Estimation
### Purpose: Dementia Data Processing Main
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, plyr, parallel, dplyr, RMySQL, stringr, msm, dbplyr)
library(mortdb, lib = "FILEPATH")
library(Hmisc, lib.loc = paste0(j, "FILEPATH"))
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

dem_dir <- paste0(j, "FILEPATH")
repo_dir <- paste0(h, "FILEPATH")
upload_dir <- paste0(j, "FILEPATH")
functions_dir <- paste0(functions_dir, "FILEPATH")
mrbrt_helper_dir <- paste0(j, "FILEPATH")
mrbrt_dir <- paste0(j, "FILEPATH")
cv_drop <- c("cv_nodoctor_diagnosis_dementia")
draws <- paste0("draw_", 0:999)
dem_id <- ID ## BUNDLE ID 

# SOURCE CENTRAL FUNCTIONS ------------------------------------------------

functs <- c("get_bundle_data.R", "save_bundle_version.R", "get_bundle_version.R", "get_crosswalk_version.R",
            "save_crosswalk_version.R", "upload_bundle_data.R", "get_location_metadata.R", "get_age_metadata.R",
            "get_draws.R", "get_population.R", "get_ids.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))
mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0(mrbrt_helper_dir, x, ".R"))))

source(paste0(repo_dir, "data_processing_functions.R"))

get_con <- function(dbname, host) {
  if (.Platform$OS.type == "windows") {
    j <- "FILEPATH"
  } else {
    j <- "FILEPATH"
  }
  credentials <- fread(paste0(j, "FILEPATH"))
  con <- suppressWarnings(src_mysql(dbname = dbname, host = host, user = credentials$user, password = credentials$pw))
  return(con)
}
run_query <- function(dbname, host, query) {
  con <- get_con(dbname, host)
  return(con %>% tbl(sql(query)) %>% collect(n=Inf) %>% data.table)
  disconnect()
}
disconnect <- function() { 
  lapply( dbListConnections(MySQL()), function(x) dbDisconnect(x) )
}
get_bundle_version_ids<-function(bids){
  
  dbname <- "epi"
  host <- "ADDRESS"
  
  # Query tables
  
  ### epi.model_version : model run specs ######################################################
  string<-c()
  for (bid in bids){
    if (is.null(string)==T){
      string<-paste0(bid)
    }else{
      string<-paste0(string," OR bundle_id =",bid)
    }
  }
  query<- paste0("SELECT 
                      bundle_version.bundle_version.bundle_version_id,
                      bundle_version.bundle_version.bundle_id,
                      bundle_version.bundle_version.gbd_round_id,
                      bundle_version.bundle_version.date_inserted,
                      bundle_version.bundle_version.last_updated,
                      bundle_version.bundle_version_decomp_step.decomp_step_id,
                      shared.cause.acause
                  FROM
                      bundle_version.bundle_version
                          LEFT JOIN
                      (bundle_version.bundle_version_decomp_step) ON (bundle_version.bundle_version.bundle_version_id = bundle_version.bundle_version_decomp_step.bundle_version_id)
                          LEFT JOIN
                      (bundle.bundle) ON (bundle_version.bundle_version.bundle_id = bundle.bundle.bundle_id)
                          LEFT JOIN
                      (shared.cause) ON (bundle.bundle.cause_id = shared.cause.cause_id)
                  WHERE
                      bundle_version.bundle_version.bundle_id =",string)
  query<-gsub("\n","",query)
  bdt<-run_query(dbname,host,query)
  return(bdt)
}
get_xwalk_version_ids<-function(bid,version_dt){
  
  dbname <- "epi"
  host <- "ADDRESS"
  bvids <- version_dt$bundle_version_id
  if (length(bvids) > 1){
    bvid_list <- paste0(bvids[1], paste0(", ", bvids[-1], collapse = ""), collapse = ",")
  } else {
    bvid_list <- bvids
  }
  
  # Query tables
  
  ### epi.model_version : model run specs ######################################################
  query<- paste0("SELECT crosswalk_version_id,bundle_version_id,crosswalk_description,date_inserted,last_updated 
                 FROM crosswalk_version.crosswalk_version WHERE bundle_version_id IN (",bvid_list, ")")
  bdt<-run_query(dbname,host,query)
  bdt <- merge(bdt, version_dt[, .(bundle_id, bundle_version_id, decomp_step_id, acause)], by = "bundle_version_id")
  return(bdt)
}



# GET DATA ----------------------------------------------------------------

decomp_step <- 'iterative'
presplit <- T

dt <- get_bundle_version(bundle_version_id = ID, fetch = "all", export = FALSE)

#Remove mortality data other than csmr
dt <- dt[(measure!= "mtwith" & measure!= "relrisk"), ]

#Remove group review data
dt <- dt[input_type!= "group_review", ]

#Add crosswalk_parent_seq variable
dt[, crosswalk_parent_seq := NA]
dt[, crosswalk_parent_seq := as.numeric(crosswalk_parent_seq)]


# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 22, gbd_round_id = 7)
age_dt <- get_age_metadata(12, gbd_round_id = 7)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == 235, age_group_years_end := 99]


# SEX SPLIT ---------------------------------------------------------------

## AFTER THIS THE OBJECT (predict_sex$final) WILL HAVE THE SEX SPLIT DATA IN 
## CORRECT FORMAT 
source(paste0(repo_dir, "sex_split.R"))

## GET CSMR DATA
other_dt <- dt[(measure=="mtspecific" | measure=="mtexcess"), ] 


# CROSSWALKING ------------------------------------------------------------

## AFTER THIS THE OBJECT (adjusted$epidb) WILL HAVE NETWORK ADJUSTED DATA 
source(paste0(repo_dir, "network_crosswalk.R"))

pdf(paste0(mrbrt_dir, model_name, "/forrestplots_", date, ".pdf"), width = 12)
forrestplot_graphs
dev.off()


## COMBINE X-WALKED DATA, TAG BACK ON NON-INCIDENCE/PREVALENCE DATA
xwalked_dt <- rbind(adjusted$epidb, other_dt, fill = T)

# LOCATION SPLITTING ------------------------------------------------------

## AFTER THIS THE OBJECT (xwalk_split_dt) WILL HAVE ALL DATA INCLUDING
## LOCATION SPLIT DATA
source(paste0(repo_dir, "location_splitting.R"))

# CLEAN UP ----------------------------------------------------------------

xwalked_total_dt <- copy(xwalk_split_dt)
xwalked_total_dt <- xwalked_total_dt[group_review == 1 | is.na(group_review)] ## NO GROUP REVIEW 0 DATA
xwalked_total_dt[is.na(lower) & is.na(upper), uncertainty_type_value := NA]
xwalked_total_dt <- col_order(xwalked_total_dt)


if (presplit == T){
  xwalked_total_dt <- xwalked_total_dt[age_end-age_start <= 25]
  write.xlsx(xwalked_total_dt, paste0(upload_dir, "newagesplitME_072920.xlsx"), sheetName = "extraction")
  xwalk_description <- "GBD 2020 iterative age splitting data <= 25"
  save_crosswalk_version(bundle_version_id = versions[decomp_step_id == decomp_step, max(bundle_version_id)], description = xwalk_description,
                         data_filepath = paste0(upload_dir, "newagesplitME_072920.xlsx"))
  save_crosswalk_version(bundle_version_id = 30566, description = xwalk_description,
                         data_filepath = paste0(upload_dir, "newagesplitME_072920.xlsx"))
} else {
  preagesplit_dt <- copy(xwalked_total_dt)
  preagesplit_dt <- calculate_cases_fromse(preagesplit_dt)
  preagesplit_dt <- get_cases_sample_size(preagesplit_dt)
  source(paste0(repo_dir, "age_split.R"))
  final_split <- age_split(gbd_id = ID, df = preagesplit_dt, age = age_dt[age_group_years_start>=40, age_group_id], 
                           region_pattern = F, location_pattern_id = 1)

  final_split[standard_error>1, standard_error:= ""]
  
  write.xlsx(final_split, paste0(upload_dir, "FILEPATH"), na = "", sheetName = 'extraction')
  xwalk_description <- "Post Age Splitting GBD 2020 iterative xwalk - updated CSMR"
  save_crosswalk_version(bundle_version_id = ID, description = xwalk_description,
                         data_filepath = paste0(upload_dir, "FILEPATH"))
}


# GET CROSSWALK VERSIONS --------------------------------------------------

crosswalk_versions <- get_xwalk_version_ids(dem_id, version_dt = versions)
