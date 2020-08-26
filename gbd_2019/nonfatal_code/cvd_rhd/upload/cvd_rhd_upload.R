rm(list = setdiff(ls(), c(lsf.str(), "jpath", "hpath", "os")))

.libPaths("FILEPATH")
library(openxlsx)
suppressMessages(library(R.utils))
suppressMessages(library(data.table))
library(ggplot2)

date <- gsub("-", "_", Sys.Date())
logs <- "FILEPATH"

#Source central functions
suppressMessages(sourceDirectory("FILEPATH"))
demographics <- get_demographics(gbd_team="epi")
locs <- unlist(demographics$location_id, use.names=F)
ages <- unlist(demographics$age_group_id, use.names=F)
age_info <- get_age_metadata(age_group_set_id=12)
age_info <- subset(age_info, age_group_id>=5)
setnames(age_info, "age_group_years_start", "age_start")
setnames(age_info, "age_group_years_end", "age_end")
years <- unlist(demographics$year_id, use.names=F)
years <- years[years!=2017]
loc_info <- get_location_metadata(location_set_id=35)
us <- unlist(loc_info[parent_id==102, c("location_id")], use.names=F)

#Map - endemic vs. non-endemic
rhd.map <- read.csv("FILEPATH/rhd_endemic_map_2019_02_05.csv")
endemic.locs <- unlist(subset(rhd.map, endemic==1, select="location_id"), use.names=F) #Subset to non-endemic locations only
non.locs <- unlist(subset(rhd.map, endemic==0, select="location_id"), use.names=F)

#check <- get_bundle_data(bundle_id="VALUE", decomp_step="step4")
endemic <- get_bundle_data(bundle_id="VALUE", decomp_step="step2")
endemic <- endemic[measure!="mtspecific"]
endemic[,seq := NA]

filepath <- paste0("FILEPATH/value_xfer_", date, ".xlsx")
write.xlsx(endemic, filepath, sheetName="extraction")
foo <- validate_input_sheet(bundle_id="VALUE", filepath, logs)
upload_bundle_data(bundle_id="VALUE", filepath, decomp_step="step4")

#get csmr
csmr <- get_outputs("cause", cause_id="VALUE", measure_id=1, metric_id=3, sex_id=c(1,2), location_id=locs, age_group_id=ages,
			decomp_step="step4", process_version_id=PROCESS_ID, year_id=years)
csmr <- merge(csmr, age_info[, c("age_start", "age_end", "age_group_id")], by="age_group_id")
setnames(csmr, "val", "mean")
csmr[,year_start:=year_id]
csmr[,year_end:=year_id]
csmr[,sex:=ifelse(sex_id==1, "Male", "Female")]
csmr[,nid:="NID"]
csmr[,underlying_nid:=NA]
csmr[,source_type:="Mixed or estimation"]
csmr[,smaller_site_unit:=NA]
csmr[,site_memo:=NA]
csmr[,sex_issue:=0]
csmr[,year_issue:=0]
csmr[,age_issue:=0]
csmr[,age_demographer:=0]
csmr[,measure:="mtspecific"]
csmr[,standard_error:=NA]
csmr[,cases:=NA]
csmr[,sample_size:=NA]
csmr[,unit_type:="Person"]
csmr[,unit_value_as_published:=1]
csmr[,measure_issue:=0]
csmr[,measure_adjustment:=0]
csmr[,uncertainty_type_value:=95]
csmr[,representative_name:="Unknown"]
csmr[,urbanicity_type:="Unknown"]
csmr[,recall_type:="Not Set"]
csmr[,recall_type_value:=NA]             
csmr[,sampling_type:=NA]
csmr[,response_rate:=NA]
csmr[,case_name:=""]
csmr[,case_definition:="CSMR from COD model"]
csmr[,extractor:="EXTRACTOR"]
csmr[,seq:=NA]
csmr[,effective_sample_size:=NA]
csmr[,design_effect:=NA]
csmr[,is_outlier:=0]
csmr[,uncertainty_type:=NA]
csmr[,input_type:=NA]

csmr.non <- csmr[location_id %in% non.locs]
csmr.non <- csmr.non[, c("location_name", "age_start", "sex", "year_start", "year_end", "underlying_nid", "nid", "source_type",
			             "location_id", "smaller_site_unit", "site_memo", "sex_issue", "year_issue", "age_end", "age_issue",
			             "age_demographer", "measure", "mean", "lower", "upper", "standard_error", "cases", "sample_size",
			             "unit_type", "unit_value_as_published", "measure_issue", "measure_adjustment", "uncertainty_type_value",
			             "representative_name", "urbanicity_type", "recall_type", "recall_type_value", "sampling_type",
			             "response_rate", "case_name", "case_definition", "extractor", "seq", "effective_sample_size", "design_effect",
			             "is_outlier", "input_type", "uncertainty_type")]

csmr.non.filepath <- paste0("FILEPATH/csmr_", date, ".xlsx")
write.xlsx(csmr.non, csmr.non.filepath, sheetName="extraction")
foo <- validate_input_sheet(bundle_id="VALUE", csmr.non.filepath, logs)
upload_bundle_data(bundle_id="VALUE", csmr.non.filepath, decomp_step="step4")


step4 <- save_bundle_version(bundle_id="VALUE", decomp_step="step4")

bv <- get_bundle_version(bundle_version_id="VALUE")
mtexcess <- subset(bv, measure=="mtspecific")
prevalence <- read.xlsx("FILEPATH/xw_results_2019_10_04_cvd_420_prevalence_step2_parameters_for_step4.xlsx")
upload <- rbind(mtexcess, prevalence, fill=T)

upload.filepath <- paste0("FILEPATH/xwalk_", date, ".xlsx")
write.xlsx(upload, upload.filepath, sheetName="extraction")
save_crosswalk_version(bundle_version_id="VALUE", upload.filepath, "v94 CSMR; sex-split prevalence")

#Save new bundle_version for "VALUE"
rhd <- save_bundle_version(bundle_id="VALUE", decomp_step="step4", include_clinical=TRUE)
rhd.non <- get_bundle_version(bundle_version_id="VALUE")
setnames(rhd.non, "clinical_data_type", "clinical")
rhd.non <- rhd.non[location_id %in% non.locs]
rhd.non <- rhd.non[location_id %in% us] #just Marketscan

#get data from step2
rhd.step2 <- get_bundle_data(bundle_id="VALUE", decomp_step="step2")
rhd.step2 <- rhd.step2[measure!="mtspecific"]

#bind dataframes together
rhd.non.upload <- rbind(rhd.non, rhd.step2, fill=T)
rhd.non.upload[,seq:=NA]
rhd.non.upload$row_id <- rownames(rhd.non.upload)
ui.prob <- unlist(subset(rhd.non.upload, mean==0 & lower==0 & upper==1, select="row_id"), use.names=F)
rhd.non.upload <- rhd.non.upload[row_id %ni% ui.prob]
rhd.non.upload[,row_id:=NULL]
non.prev.filepath <- paste0("FILEPATH/prev_", date, ".xlsx")
write.xlsx(rhd.non.upload, non.prev.filepath, sheetName="extraction")
foo <- validate_input_sheet(bundle_id="VALUE", non.prev.filepath, logs)
upload_bundle_data(bundle_id="VALUE", non.prev.filepath, decomp_step="step4")

step4.non <- save_bundle_version(bundle_id="VALUE", decomp_step="step4")

non.to.upload <- get_bundle_version(bundle_version_id="VALUE")
non.xwalk <- non.to.upload[(measure=="prevalence" & clinical!="") | measure=="mtspecific"]
non.xwalk[,crosswalk_parent_seq:=NA]

non.xwalk.filepath <- paste0(jpath, "FILEPATH/xwalk_", date, ".xlsx")
write.xlsx(non.xwalk, non.xwalk.filepath, sheetName="extraction")
save_crosswalk_version(bundle_version_id="VALUE", non.xwalk.filepath, "v94 CSMR; clinical info")
