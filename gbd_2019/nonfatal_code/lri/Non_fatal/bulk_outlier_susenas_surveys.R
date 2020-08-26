##############################################
## Bulk outlier the SUSENAS surveys for LRI ##
## in decomp steps 3 and 4 ##
##############################################
source("/filepath/get_crosswalk_version.R")
source("/filepath/save_bulk_outlier.R")

susenas_nids <- c(5376,5401,6687,6706,6724,6767,6790,6811,6842,6874,6904,6970,30235,43510,43526,85265,150884,151184)

## For step 3
  crosswalk_version_id <- 5849
  
  s3_data <- get_crosswalk_version(crosswalk_version_id = crosswalk_version_id)
  
  susenas_rows <- subset(s3_data, nid %in% susenas_nids)
  susenas_outliers <- susenas_rows[,c("seq","is_outlier")]
    susenas_outliers$is_outlier <- 1
    
  write.xlsx(susenas_outliers, "filepath", sheetName="extraction")
  
  decomp_step <- 'step3'
  filepath <- "filepath"
  description <- 'Crosswalk Version 5849 with SUSENAS Outliers'
save_bulk_outlier(
    crosswalk_version_id=crosswalk_version_id,
    decomp_step=decomp_step,
    filepath=filepath,
    description=description)

## For step 4
  s4_data <- get_crosswalk_version(crosswalk_version_id = 8771)
  
  susenas_rows <- subset(s4_data, nid %in% susenas_nids)
  susenas_outliers <- susenas_rows[,c("seq","is_outlier")]
  susenas_outliers$is_outlier <- 1
  
  write.xlsx(susenas_outliers, "filepath", sheetName="extraction")
  
  decomp_step <- 'step4'
  filepath <- 'filepath'
  description <- 'Crosswalk Version 8771 Step 4 with SUSENAS Outliers'
save_bulk_outlier(
    crosswalk_version_id=8630,
    decomp_step=decomp_step,
    filepath=filepath,
    description=description)
