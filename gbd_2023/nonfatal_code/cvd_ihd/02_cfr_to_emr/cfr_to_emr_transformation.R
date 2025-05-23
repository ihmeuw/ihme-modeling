libPaths(paste0(hpath, "Rlibrary"))
library(openxlsx)
library(R.utils)
timestamp <- Sys.Date()

#Load central functions
suppressMessages(sourceDirectory(paste0(jpath, "FILEPATH")))
#First stage ischemic stroke
is.cfr <- data.frame(mi_bundle)
is.cfr <- subset(is.cfr, measure!="incidence")
mtexcess <- unlist(unique(subset(is.cfr, measure=="mtexcess", select="nid")), use.names=F)
cfr <- subset(is.cfr, measure=="cfr" & nid %ni% mtexcess)
cfr$parent_id <- cfr$seq_parent
cfr$seq <- NA
cfr$mean[which(is.na(cfr$mean))] <- cfr$cases[which(is.na(cfr$mean))]/cfr$sample_size[which(is.na(cfr$mean))]
cfr$sample_size[which(cfr$nid==420961)] <- cfr$cases[which(cfr$nid==420961)]/cfr$mean[which(cfr$nid==420961)]
cfr$sample_size[which(cfr$nid==420831)] <- cfr$cases[which(cfr$nid==420831)]/cfr$mean[which(cfr$nid==420831)]
cfr$sample_size[which(cfr$nid==420873)] <- cfr$cases[which(cfr$nid==420873)]/cfr$mean[which(cfr$nid==420873)]

cfr$new_mean <- with(cfr, (-log(1-mean)/(1/12)))
cfr$new_lower <- with(cfr, ifelse(uncertainty_type=="Confidence interval", (-log(1-lower)/(1/12)), NA))
cfr$new_upper <- with(cfr, ifelse(uncertainty_type=="Confidence interval", (-log(1-upper)/(1/12)), NA))
cfr$standard_error <- NA
cfr$cases <- NA
cfr$mean <- cfr$new_mean
cfr$lower <- cfr$new_lower
cfr$upper <- cfr$new_upper
cfr$new_mean <- NULL
cfr$new_lower <- NULL
cfr$new_upper <- NULL
cfr$measure <- "mtexcess"
cfr$uncertainty_type_value <- with(cfr, ifelse(uncertainty_type=="Sample size", NA, uncertainty_type_value))
cfr$response_rate <- NA


write.xlsx(cfr, file=paste0("/FILEPATH/7052_cfr_transform_", date, ".xlsx"), sheetName="extraction", na='')
validate_input_sheet(bundle_id=114, file=paste0("/FILEPATH/7052_cfr_transform_", date, ".xlsx"),
                     error_log_path="/FILEPATH/")

upload_bundle_data(bundle_id=7052, decomp_step='step4', filepath=paste0("FILEPATH/7052_cfr_transform_", date, ".xlsx"))




