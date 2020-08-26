####################################################################
## Other Drugs Data Prep Code
## Purpose: Pull data from bundle, prepare crosswalk version, upload
####################################################################

# Clean up and initialize with the packages we need
rm(list = ls())
library(data.table)

date <- Sys.Date()
date <- gsub("-", "_", Sys.Date())

draws <- paste0("draw_", 0:999)

# Central functions
functs <- c("get_draws", "get_population", "get_location_metadata", "get_age_metadata", "get_ids", "get_bundle_data", "save_crosswalk_version", "save_bundle_version", "get_bundle_version")
invisible(lapply(functs, function(x) source(paste0("FILEPATH", x, ".R"))))

# Other drugs bundle id: 758
bid<-758
dstep<-"step2"

# Download data
df<-get_bundle_data(bundle_id = bid, decomp_step = dstep, gbd_round_id = 6)
df<-df[, crosswalk_parent_seq:=""]
# Due to scarce data will not age-split other drugs
# Keeping all available data, regardless of age range
#df<-df[age_end-age_start<=25]
df<-df[group_review!=0]

write.xlsx(df[group_review!=0], "FILEPATH", sheetName="extraction")
save_bundle_version(bundle_id = bid, decomp_step = "step2", include_clinical = F)
save_crosswalk_version(bundle_version_id = 3965, data_filepath = "FILEPATH", description = "Decomp 2 Final Data")

