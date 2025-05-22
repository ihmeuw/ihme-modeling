
library(dplyr)
library(openxlsx)
library(readxl)
library(reticulate)

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

'%ni%' <- Negate('%in%')

# getting data
# Bundle version data:
bv_df <- get_bundle_version(bvid)

#crosswalk version data:
cwv_df <- get_crosswalk_version(crosswalk_version_id)

# outlier Oslo:
oslo_nid <- c(97468)
oslo_nid %in% cwv_df$nid
unique(cwv_df$is_outlier[cwv_df$nid %in% oslo_nid])
unique(cwv_df$location_name[cwv_df$nid %in% oslo_nid])
cwv_df$is_outlier[cwv_df$nid %in% oslo_nid ] <- as.integer(1)

# outlier Netherlands:
netherlands_nid <- c(114854)
netherlands_nid %in% cwv_df$nid
unique(cwv_df$is_outlier[cwv_df$nid %in% netherlands_nid])
unique(cwv_df$location_name[cwv_df$nid %in% netherlands_nid])
cwv_df$is_outlier[cwv_df$nid %in% netherlands_nid ] <- as.integer(1)

# outlier Portugal:
portugal_nid <- c(114855)
portugal_nid %in% cwv_df$nid
unique(cwv_df$is_outlier[cwv_df$nid %in% portugal_nid])
unique(cwv_df$location_name[cwv_df$nid %in% portugal_nid])
cwv_df$is_outlier[cwv_df$nid %in% portugal_nid ] <- as.integer(1)

# get the list of unique seqs in that bundle version,
## in the new crosswalk set seq=NULL for any seq that is not part of that set of bundle version seqs.
bv_seqs <- unique(bv_df$seq)
cwv_df$seq[cwv_df$seq %ni% bv_seqs] <- NA

# save crosswalked data frame:
write.xlsx(cwv_df, file = paste0(modeling_dir, "outliering_", date, "/", "outliering_",date,".xlsx"), sheetName = "extraction", overwrite = TRUE)


#save new crosswalk version
data_filepath <- paste0(modeling_dir, "outliering_", date, "/", "outliering_",date,".xlsx")
description <- paste0("outliering Oslo, Netherlands, and Portugal nids from cwv", date)
result <- save_crosswalk_version(
  bundle_version_id=bvid,
  data_filepath=data_filepath,
  description=description)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
