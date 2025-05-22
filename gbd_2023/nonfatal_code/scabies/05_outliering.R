
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
cwv_df <- get_crosswalk_version(46117)


# outlier Timpor:
timor_nid <- c(115286)
timor_nid %in% cwv_df$nid
unique(cwv_df$is_outlier[cwv_df$nid %in% timor_nid])
unique(cwv_df$location_name[cwv_df$nid %in% timor_nid])
cwv_df$is_outlier[cwv_df$nid %in% timor_nid & cwv_df$mean>0.06] <- as.integer(1)

unique(cwv_df$location_name[cwv_df$nid==97501])

# get the list of unique seqs in that bundle version,
## in the new crosswalk set seq=NULL for any seq that is not part of that set of bundle version seqs.
bv_seqs <- unique(bv_df$seq)
cwv_df$seq[cwv_df$seq %ni% bv_seqs] <- NA

# save crosswalked data frame:
write.xlsx(cwv_df, file = paste0(modeling_dir, "/", "outliering_",date,".xlsx"), sheetName = "extraction", overwrite = TRUE)


#save new crosswalk version
data_filepath <- paste0(modeling_dir, "/", "outliering_",date,".xlsx")
description <- paste0("outliering Timaor>0.06", date)
result <- save_crosswalk_version(
  bundle_version_id=bvid,
  data_filepath=data_filepath,
  description=description)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
