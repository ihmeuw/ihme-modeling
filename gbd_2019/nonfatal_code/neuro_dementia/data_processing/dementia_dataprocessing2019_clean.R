##########################################################################
### Author: USERNAME
### Date: 03/22/2019
### Project: GBD Nonfatal Estimation
### Purpose: Dementia Data Processing Master
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

pacman::p_load(data.table, openxlsx, ggplot2, plyr, parallel, dplyr, RMySQL, stringr)
library(mortdb, lib = "FILEPATH")
library(dbplyr, lib.loc = paste0("FILEPATH"))
library(msm, lib.loc = paste0("FILEPATH"))
library(Hmisc, lib.loc = paste0("FILEPATH"))
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

dem_dir <- paste0("FILEPATH")
repo_dir <- paste0("FILEPATH")
upload_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
mrbrt_helper_dir <- paste0("FILEPATH")
mrbrt_dir <- paste0("FILEPATH")
cv_drop <- c("cv_nodoctor_diagnosis_dementia")
draws <- paste0("draw_", 0:999)
dem_id <- ID ## BUNDLE ID

# SOURCE CENTRAL FUNCTIONS ------------------------------------------------

functs <- c("get_bundle_data.R", "save_bundle_version.R", "get_bundle_version.R", "get_crosswalk_version.R",
            "save_crosswalk_version.R", "upload_bundle_data.R", "get_location_metadata.R", "get_age_metadata.R",
            "get_draws.R", "get_population.R", "get_ids.R")
invisible(lapply(functs, function(x) source(paste0("FILEPATH", x))))
mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function", 
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function", 
            "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source("FILEPATH")))

source(paste0("FILEPATH"))

get_con <- function(dbname, host) {
  if (.Platform$OS.type == "windows") {
    j <- "FILEPATH"
  } else {
    j <- "FILEPATH"
  }
  credentials <- fread(paste0("FILEPATH"))
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
  
  dbname <- "ADDRESS"
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
  query<-gsub("FILEPATH")
  bdt<-run_query("ADDRESS")
  return(bdt)
}
get_xwalk_version_ids<-function(bid,version_dt){
  
  dbname <- "ADDRESS"
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
  bdt<-run_query(ADDRESS)
  bdt <- merge(bdt, version_dt[, .(bundle_id, bundle_version_id, decomp_step_id, acause)], by = "bundle_version_id")
  return(bdt)
}

# GET DATA ----------------------------------------------------------------

decomp_step <- 4
presplit <- F

versions <- get_bundle_version_ids(dem_id)

dt <- get_bundle_version(bundle_version_id = versions[decomp_step_id == decomp_step, max(bundle_version_id)]) 

clin <- as.data.table(read.csv('FILEPATH'))
clin <- clin[estimate_id==21, ]
clin$measure <- "prevalence"
clin$representative_name <- "Nationally and subnationally representative"
clin[,sex := ifelse(sex_id==1, "Male", "Female")]
age_info <- read.csv("FILEPATH")
clin <- merge(x=clin, y=age_info[, c("age_group_id", "age_start", "age_end")], by = "age_group_id")
clin[age_end==99, age_end==124]
clin$uncertainty_type <- "Sample size"
clin[, c("representative_id", "sex_id", "age_group_id", "uncertainty_type_id", "estimate_id", "id", "source_type_id", "measure_id")] <- NULL
clin$clinical_data_type <- "claims"
clin <- clin[clin$year_start!=2000, ]
clin <- clin[clin$location_id!=8, ]

dt$clinical_data_type <- ""
dt <- rbind.fill(dt, clin)
dt <- as.data.table(dt)

dt$cv_marketscan <- ifelse((dt$nid==ID | dt$nid==ID),1,0)

dt <- dt[(!clinical_data_type == "inpatient" & !(clinical_data_type == "claims" & year_start == 2000)) | is.na(clinical_data_type)] ## GET RID OF HOSPTIAL DATA AND MARKETSCAN 2000
dt[clinical_data_type == "claims", cv_marketscan := 1] 
dt[, crosswalk_parent_seq := NA]
dt[, crosswalk_parent_seq := as.numeric(crosswalk_parent_seq)]
dt[specificity == "", specificity := NA] ## NEED THIS TO BE NA NOT "" 

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
#other_dt <- rbind(predict_rr$final, dt[measure == "mtwith"], fill = T)
other_dt <- dt[(measure=="mtspecific"), ] 

poland_dt <- dt[dt$nid == ID | dt$nid == ID | dt$nid == ID, ]

# CROSSWALKING ------------------------------------------------------------

## AFTER THIS THE OBJECT (adjusted$epidb) WILL HAVE THE NON-MARKETSCAN 
## ADJUSTED DATA
source(paste0("FILEPATH", "network_crosswalk.R"))

pdf(paste0("FILEPATH"), width = 12)
forrestplot_graphs
dev.off()

## AFTER THIS THE OBJECT (mkscn_adjusted$adjusted) WILL HAVE THE ADJUSTED
## DATA
source(paste0(repo_dir, "mkscn_spline_crosswalk.R"))

pdf(paste0("FILEPATH"), width = 12)
pred_plots_mkscn
dev.off()

## COMBINE X-WALKED DATA, TAG BACK ON NON-INCIDENCE/PREVALENCE DATA
xwalked_dt <- rbind(adjusted$epidb, mkscn_adjusted$adjusted, poland_dt, fill = T)

# LOCATION SPLITTING ------------------------------------------------------

## AFTER THIS THE OBJECT (xwalk_split_dt) WILL HAVE ALL DATA INCLUDING
## LOCATION SPLIT DATA
source(paste0("FILEPATH", "location_splitting.R"))

# CLEAN UP ----------------------------------------------------------------

xwalked_total_dt <- copy(xwalk_split_dt)
xwalked_total_dt <- xwalked_total_dt[group_review == 1 | is.na(group_review)] ## NO GROUP REVIEW 0 DATA
xwalked_total_dt[is.na(lower) & is.na(upper), uncertainty_type_value := NA]
xwalked_total_dt <- col_order(xwalked_total_dt)


if (presplit == T){
  xwalked_total_dt <- xwalked_total_dt[age_end-age_start <= 20]
  write.xlsx(xwalked_total_dt, paste0(upload_dir, "newagesplitME.xlsx"), sheetName = "extraction")
  xwalk_description <- "Age Split on New ME"
  save_crosswalk_version(bundle_version_id = versions[decomp_step_id == decomp_step, max(bundle_version_id)], description = xwalk_description,
                         data_filepath = paste0(upload_dir, "newagesplitME.xlsx"))
} else {
  preagesplit_dt <- copy(xwalked_total_dt)
  preagesplit_dt <- calculate_cases_fromse(preagesplit_dt)
  preagesplit_dt <- get_cases_sample_size(preagesplit_dt)
  source(paste0(repo_dir, "age_split.R"))
  final_split <- age_split(gbd_id = 23889, df = preagesplit_dt, age = age_dt[age_group_years_start>=40, age_group_id], 
                           region_pattern = F, location_pattern_id = 1)
  #Only because clinical data was added as flat file
  final_split[clinical_data_type=="claims", unit_type := "Person"]
  final_split[clinical_data_type=="claims", urbanicity_type := "Unknown"]
  final_split[clinical_data_type=="claims", source_type := "Facility - other/unknown"]
  final_split[clinical_data_type=="claims", recall_type := "Not Set"]
  final_split[clinical_data_type=="claims", unit_value_as_published := 1]
  final_split[clinical_data_type=="claims", is_outlier := 0]
  final_split[standard_error>1, standard_error:= ""]
  
  write.xlsx(final_split, paste0("FILEPATH"), na = "", sheetName = 'extraction')
  xwalk_description <- "Post Age Splitting GBD 2019 Step4 new claims and lit data"
  save_crosswalk_version(bundle_version_id = versions[decomp_step_id == decomp_step, max(bundle_version_id)], description = xwalk_description,
                         data_filepath = paste0("FILEPATH"))
}

