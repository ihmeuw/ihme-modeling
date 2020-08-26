##########################################################################
### Author: USERNAME
### Date: 03/22/2019
### Project: GBD Nonfatal Estimation
### Purpose: Parkinson's disease Data Processing Master
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, dplyr)
library(mortdb, lib = "FILEPATH")
library(msm, lib.loc = paste0("FILEPATH"))
library(dbplyr, lib.loc = paste0("FILEPATH"))
library(Hmisc, lib.loc = paste0("FILEPATH"))
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

repo_dir <- paste0("FILEPATH")
upload_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
mrbrt_helper_dir <- paste0("FILEPATH")
mrbrt_dir <- paste0("FILEPATH")
cv_drop <- c("cv_Gelb_criteria", "cv_marketscan", "cv_clinical_records")
draws <- paste0("draw_", 0:999)
park_id <- ID

# SOURCE CENTRAL FUNCTIONS ------------------------------------------------

functs <- c("get_bundle_data.R", "save_bundle_version.R", "get_bundle_version.R", "get_crosswalk_version.R",
            "save_crosswalk_version.R", "upload_bundle_data.R", "get_location_metadata.R", "get_age_metadata.R",
            "get_draws.R", "get_population.R", "get_ids.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))
mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
                  "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0("FILEPATH"))))

source(paste0("FILEPATH"))

get_con <- function(ID, ID) {
  if (.Platform$OS.type == "windows") {
    j <- "FILEPATH"
  } else {
    j <- "FILEPATH"
  }
  credentials <- fread(paste0("FILEPATH"))
  con <- suppressWarnings(src_mysql(dbname = ADDRESS, host = ADDRESS, user = USERNAME, password = PASSWORD))
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
  
  dbname <- "NAME"
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
  
  dbname <- "NAME"
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

decomp_step <- 
presplit <- 

versions <- get_bundle_version_ids(park_id)

dt <- get_bundle_version(bundle_version_id = versions[decomp_step_id == decomp_step, max(bundle_version_id)])
dt <- dt[(!clinical_data_type == "inpatient" & !(clinical_data_type == "claims" & year_start == 2000)) | is.na(clinical_data_type)] ## GET RID OF HOSPTIAL DATA AND MARKETSCAN 2000
dt[clinical_data_type == "claims", cv_marketscan := 1]
dt[, crosswalk_parent_seq := NA]
dt[, crosswalk_parent_seq := as.numeric(crosswalk_parent_seq)]
dt[specificity == "", specificity := NA] ## NEED THIS TO BE NA NOT "" FOR UPLOADER VALIDATIONS

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = ID)
age_dt <- get_age_metadata(ID)
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == ID, age_group_years_end := 99]

# SEX SPLIT ---------------------------------------------------------------

## AFTER THIS THE OBJECT (predict_sex$final) WILL HAVE THE SEX SPLIT DATA IN 
## CORRECT FORMAT 
source(paste0("FILEPATH", "sex_split.R"))

## GET RELRISK/MORT DATA
other_dt <- rbind(predict_mt$final, splitrep, fill = T)

# CROSSWALKING ------------------------------------------------------------

## AFTER THIS THE OBJECT (adjusted$epidb) WILL HAVE THE NON-MARKETSCAN
## ADJUSTED DATA
source(paste0("FILEPATH", "network_crosswalk.R"))

pdf(paste0(mrbrt_dir, "FILEPATH"), width = 12)
forrestplot_graphs
dev.off()

## TAG BACK ON NON-INCIDENCE/PREVALENCE DATA
xwalked_dt <- rbind(adjusted$epidb, other_dt, fill = T)

# LOCATION SPLITTING ------------------------------------------------------

## AFTER THIS THE OBJECT (xwalk_split_dt) WILL HAVE ALL DATA INCLUDING
## LOCATION SPLIT DATA
source(paste0("FILEPATH", "location_splitting.R"))

# CLEAN UP ----------------------------------------------------------------

xwalked_total_dt <- copy(xwalk_split_dt)
xwalked_total_dt <- xwalked_total_dt[group_review == 1 | is.na(group_review)]
xwalked_total_dt[is.na(lower) & is.na(upper), uncertainty_type_value := NA]
xwalked_total_dt <- col_order(xwalked_total_dt)

if (presplit == T){
  xwalked_total_dt <- xwalked_total_dt[age_end-age_start <= 20]
  write.xlsx(xwalked_total_dt, paste0(upload_dir, "preagesplit_newME.xlsx"), sheetName = "extraction")
  xwalk_description <- "Pre Age Split Model with New ME"
  save_crosswalk_version(bundle_version_id = versions[decomp_step_id == decomp_step, max(bundle_version_id)], description = xwalk_description,
                         data_filepath = paste0(upload_dir, "preagesplit_newME.xlsx"))
} else {
  preagesplit_dt <- copy(xwalked_total_dt)
  preagesplit_dt <- calculate_cases_fromse(preagesplit_dt)
  preagesplit_dt <- get_cases_sample_size(preagesplit_dt)
  source(paste0(repo_dir, "age_split.R"))
  final_split <- age_split(gbd_id = ID, df = preagesplit_dt, age = age_dt[age_group_years_start>=20, age_group_id], 
                           region_pattern = F, location_pattern_id = 1)
  write.xlsx(final_split, paste0("FILEPATH"), sheetName = "extraction")
  xwalk_description <- "Post Age Splitting GBD 2019 New Age Pattern ME"
  save_crosswalk_version(bundle_version_id = versions[decomp_step_id == decomp_step, max(bundle_version_id)], description = xwalk_description,
                         data_filepath = paste0("FILEPATH"))
}
