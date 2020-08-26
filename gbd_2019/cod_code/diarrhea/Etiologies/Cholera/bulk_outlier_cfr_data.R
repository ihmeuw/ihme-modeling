#######################################################
## Review cholera case fatality data 
#######################################################

source() # filepaths to central functions for data upload and download

cholera <- get_crosswalk_version(crosswalk_version_id = 8564)

ggplot(cholera, aes(x=age_start, y=mean)) + geom_point()
ggplot(cholera, aes(x=mean)) + geom_histogram(binwidth=0.05)
sd(cholera$mean)

quantile(cholera$mean, 0.95)

cholera$upper5 <- ifelse(cholera$mean > quantile(cholera$mean, 0.95), 1, 0)
ggplot(cholera, aes(x=age_start, y=mean, col=factor(upper5))) + geom_point()

## Bulk outlier above 95%ile
cholera$is_outlier <- ifelse(cholera$is_outlier==1, 1, ifelse(cholera$upper5==1,1,0))
  
  outliers <- subset(cholera, upper5 == 1)
  
  outliers$is_outlier <- 1
  outliers <- outliers[,c("seq","is_outlier")]
  
  write.xlsx(outliers, "filepath", sheetName="extraction")
  
  save_bulk_outlier(crosswalk_version_id = 8564, decomp_step = "step2", filepath = "filepath",
                    description = "Bulk outlier case fatality greater than 95th percentile")
  
# Try to upload as a step 4 version
  save_bundle_version(bundle_id=3083, decomp_step="step3")
  bun <- get_bundle_version(bundle_version_id = 3500)
  
  cholera_save <- data.frame(cholera)
  cholera_save$seq <- ""
  write.xlsx(cholera_save, "filepath", sheetName="extraction")
  save_crosswalk_version(bundle_version_id=3500, data_filepath = "filepath",
                         description = "Upload a step 3 crosswalk version, has data above 95th pct outliered")
  