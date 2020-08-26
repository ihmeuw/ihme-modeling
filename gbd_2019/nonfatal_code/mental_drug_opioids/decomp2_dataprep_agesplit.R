####################################################################
## Opioid Dependence Data Prep Code
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
bid<-155
dstep<-"step2"



df<-as.data.table(read.xlsx("FILEPATH"))
df<-df[is.na(seq) & is.na(crosswalk_parent_seq), drop:=1] 
df<-df[is.na(drop)]
df<-df[, drop:=NULL]
df<-merge(df, drops, by = c("location_id", "nid", "year_start", "year_end"), all.x=T)
df<-df[is.na(drop)]
df<-df[, drop:=NULL]

df<-df[!(is.na(group)) & !(is.na(specificity)), group_review:=1]

write.xlsx(df, "FILEPATH", sheetName="extraction")

bundle_version<-save_bundle_version(bundle_id = 155, decomp_step = "step2", include_clinical = F)
save_crosswalk_version(bundle_version_id = bundle_version$bundle_version_id,
                       data_filepath = "FILEPATH",
                       description = "Sex-Split and Crosswalked Data for Age Pattern, Clean Outlier, No Range Restriction, Add non-prev")

# Now launch dismod
# Then pull in dismod age pattern for the age split

df<-as.data.table(read.xlsx("FILEPATH"))

drops<-get_bundle_data(bundle_id = 155, decomp_step = dstep, gbd_round_id = 6)
drops<-unique(drops[,.(nid, location_id, year_start, year_end, group_review)])
drops<-drops[is.na(group_review), group_review:=1]
drops<-drops[, sum_gr:=sum(group_review), by = c("location_id", "nid", "year_start", "year_end")]
drops<-drops[, drop:=ifelse(sum_gr==0, 1, 0)]
drops<-unique(drops[drop==1, .(location_id, nid, year_start, year_end, drop)])

df<-df[is.na(seq) & is.na(crosswalk_parent_seq), drop:=1] # Get rid of the data that we had added for the purposes of developing crosswalk coefficient 
df<-df[, drop:=NULL]
df<-merge(df, drops, by = c("location_id", "nid", "year_start", "year_end"), all.x=T)
df<-df[is.na(drop)]
df<-df[, drop:=NULL]

ages <- get_age_metadata(12, gbd_round_id = 6)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 10, age_group_id]

id <- 1976
vers_id <-399752
gbd_id<-id
vid<-vers_id

df<-df[age_end>99, age_end:=99]

# Run model with age range only < 25 years
# Update age pattern to pull that model's results

final_split <- age_split(gbd_id = id, df = df, age = age, region_pattern = T, location_pattern_id = 1, vid = vers_id)
final_split<-final_split[age_end -age_start > 25 & measure == "prevalence", drop:=1]
final_split<-final_split[is.na(drop)]
final_split<-final_split[, drop:=NULL]
final_split<-final_split[!is.na(group), group_review:=1]

final_split<-final_split[input_type == "group_review", input_type:="split"]
final_split<-final_split[input_type == "parent", input_type:="split"]
final_split<-final_split[standard_error>1, standard_error:=NA]

write.xlsx(final_split, "FILEPATH", sheetName="extraction")

bundle_version<-save_bundle_version(bundle_id = 155, decomp_step = "step2", include_clinical = F)
save_crosswalk_version(bundle_version_id = bundle_version$bundle_version_id,
                       data_filepath = "FILEPATH",
                       description = "Sex-Split, Crosswalked, and Age-Split Data using vid 399752 SR Pattern")







