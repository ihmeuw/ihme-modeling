####################################################################
## Cocaine Dependence Data Prep Code
## Purpose: Pull data from bundle, prepare crosswalk version, upload
####################################################################

# Clean up and initialize with the packages we need
rm(list = ls())
library(data.table)
library(mortdb, lib = "FILEPATH")
pacman::p_load(data.table, openxlsx, ggplot2, plyr, parallel, dplyr, RMySQL, stringr, msm)

date <- Sys.Date()
date <- gsub("-", "_", Sys.Date())

draws <- paste0("draw_", 0:999)

# Central functions
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_bundle_data", "save_crosswalk_version", "save_bundle_version", "get_bundle_version")
invisible(lapply(functs, function(x) source(paste0("FILEPATH", x, ".R"))))

mrbrt_functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
                  "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
                  "load_mr_brt_preds_function", "plot_mr_brt_function")
invisible(lapply(mrbrt_functs, function(x) source(paste0("FILEPATH", x, ".R"))))

# Data Prep Functions
source("FILEPATH")

# Set objects
bid<-156
dstep<-"step2"

df<-as.data.table(read.xlsx("FILEPATH"))

ages <- get_age_metadata(12)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 10, age_group_id]

id <- 1977
vers_id <-399764
gbd_id<-id
vid<-vers_id

df<-df[age_end>99, age_end:=99]

final_split <- age_split(gbd_id = id, df = df, age = age, region_pattern = T, location_pattern_id = 1, vid = vers_id)
final_split<-final_split[age_end -age_start > 25 & measure == "prevalence", drop:=1]
final_split<-final_split[is.na(drop)]
final_split<-final_split[, drop:=NULL]
final_split<-final_split[!is.na(group), group_review:=1]

final_split<-final_split[input_type == "group_review", input_type:="split"]
final_split<-final_split[input_type == "parent", input_type:="split"]
final_split<-final_split[standard_error>1, standard_error:=NA]

write.xlsx(final_split, "FILEPATH", sheetName = "extraction")

bundle_version<-save_bundle_version(bundle_id = 156, decomp_step = "step2", include_clinical = F)
save_crosswalk_version(bundle_version_id = bundle_version$bundle_version_id,
                       data_filepath = "FILEPATH",
                       description = "Sex-Split, Crosswalked, and Age-Split Data using vid 399764 SR pattern")
