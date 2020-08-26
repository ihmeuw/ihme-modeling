####################################################################
## Proportion HIV Data Prep Code
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
functs <- c("get_draws", "get_outputs", "get_population", "get_location_metadata", "get_model_results", "get_age_metadata", "get_ids", "get_bundle_data", "save_crosswalk_version", "save_bundle_version", "get_bundle_version")
invisible(lapply(functs, function(x) source(paste0("FILEPATH", x, ".R"))))

# Set objects
bid<-387
dstep<-"step2"

df<-get_bundle_data(bundle_id = bid, decomp_step = dstep)

df<-df[is_outlier==1, drop:=1]
df<-df[group_review==0, drop:=1]
df<-df[is.na(drop)]
df<-df[, drop:=NULL]

# There are duplicate data, identify and drop these from the bundle
df<-unique(df, by = c("location_id", "year_start", "year_end", "nid", "underlying_nid", "mean", "lower", "upper", "cases", "sample_size"))

out<-df[sex%in%c("Male", "Female")]
m<-df[sex=="Both"]
m<-m[, sex:="Male"]
m<-m[, crosswalk_parent_seq:=seq]
m<-m[, seq:=NA]
f<-df[sex=="Both"]
f<-f[, sex:="Female"]
f<-f[, crosswalk_parent_seq:=seq]
f<-f[, seq:=NA]
out<-rbind(out, m, fill = T)
out<-rbind(out, f)

out<-out[!is.na(group), group_review:=1]

write.xlsx(out, "FILEPATH", sheetName="extraction")

save_bundle_version(bundle_id = bid, decomp_step = "step2", include_clinical = F)
save_crosswalk_version(bundle_version_id = 10208, data_filepath = "FILEPATH", description = "Copy Sex")


