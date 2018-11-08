############################################################################################################
## Purpose: Compile collapsed contraception microdata with reports and separate them into input data files
###########################################################################################################

## clear memory
rm(list=ls())

## runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

pacman::p_load(data.table,magrittr,ggplot2,parallel,readxl)

## create opposite of %in%
'%ni%' <- Negate('%in%')

## in/out
input_dir <- file.path(j,"FILEPATH")
output_dir <- file.path(j,"FILEPATH")

## load locations, SDI, and ages
source(file.path(j, "FILEPATH"))
locations <- get_location_metadata(gbd_round_id = 5, location_set_id = 22)
locs <- locations[,list(location_id,ihme_loc_id,region_id,super_region_id)]
source(file.path(j, "FILEPATH"))
sdi <- get_covariate_estimates(covariate_id = 881) #sdi
sdi <- sdi[,list(location_id,year_id,mean_value)]
setnames(sdi,"mean_value","sdi")
source(file.path(j, "FILEPATH"))
age_map <- get_ids(table = "age_group")
age_map <- age_map[age_group_id %in% seq(8,14)]
age_map[,age_group := gsub(" to ","-",age_group_name)]
age_map[,age_group_name := NULL]

## PREP MICRODATA ###############################################################################

## read in collapsed contraception microdata, merge on locations, and other formatting
micro <- fread(file.path(input_dir,"FILEPATH"))
micro <- merge(micro[ihme_loc_id != "" & !is.na(ihme_loc_id)], locs, by="ihme_loc_id",all.x=T)
micro <- merge(micro,age_map,by="age_group",all.x=T)
micro[,age_start := as.numeric(substr(age_group,1,2))]
micro[,age_end := as.numeric(substr(age_group,4,5))]
micro[,year_id := floor((as.numeric(year_start)+as.numeric(year_end))/2)]
micro[,sex_id := 3]
cols <- c("nid","currmar_only","evermar_only","missing_fecund","missing_desire","missing_desire_later","no_pregppa","cv_subgeo")
micro[,(cols) := lapply(.SD,as.numeric),.SDcols = cols]

## subset microdata to modern contraceptive usage
modern <- micro[var=="mod_contra"]
modern[,var:=NULL]
setnames(modern,"mean","data")

## cut out tiny sample sizes
modern <- modern[sample_size >= 20]

## subset microdata to % of demand for contraception met with modern methods
met_need_demanded <- micro[var=="met_need_demanded"]
met_need_demanded[,var:=NULL]
setnames(met_need_demanded,"mean","data")

## cut out tiny sample sizes
met_need_demanded <- met_need_demanded[sample_size >= 20]

## merge on mod_contra as a covariate (retaining even if met_demand sample for that same loc-year-age group
## was too small and was dropped)
minimum_vars <- c("location_id","ihme_loc_id","year_id","nid","age_group","age_start","age_end","age_group_id","sex_id","region_id","super_region_id","currmar_only","evermar_only","missing_fecund","missing_desire","missing_desire_later","cv_subgeo","no_pregppa","year_start","year_end","survey_name","survey_module","file_path")
modern_cv <- modern[,c(minimum_vars,"data"),with=F]
setnames(modern_cv,"data","cv_mod")
met_need_demanded <- merge(met_need_demanded,modern_cv,by=minimum_vars,all=T)


## PREP OLD REPORT DATA ###############################################################################

## Read in tabulated report data
reports <- fread(file.path(input_dir,"FILEPATH"))
setnames(reports,"survey","survey_name")

## subset to realistic values and format
reports[met_demand < 0 | met_demand >= 1 | is.na(mod_contra) | mod_contra < 0 | mod_contra >= 1]
reports <- reports[(is.na(met_demand) | met_demand < 1) & mod_contra < 1]
reports[,nid := as.numeric(nid)]


## PREP NEW REPORT DATA ###############################################################################

## read in newer extraction_sheet and format
new_reports <- data.table(read_excel(file.path(input_dir,"FILEPATH")))[-1]
string_cols <- c("notes","survey_name","file_path","ihme_loc_id","aggregated_methods","category_mismatch")
numeric_cols <- names(new_reports)[!names(new_reports) %in% string_cols]
percentage_cols <- numeric_cols[grepl("contra|unmet_need|met_demand",numeric_cols)]
new_reports[, c(numeric_cols) := lapply(.SD,as.numeric),.SDcols = numeric_cols]
new_reports[, c(percentage_cols) := lapply(.SD,function(x) {x/100}),.SDcols = percentage_cols]
new_reports[,year_id := floor((year_start + year_end)/2)]

## fill in missingness in flagging variables
new_reports[is.na(main_analysis),main_analysis := 0]
new_reports[is.na(outlier),outlier := 0]
new_reports[is.na(demand_row_only),demand_row_only := 0]

## subset to only those surveys that have actually been extracted into new format (others are
## old extractions to-be-re-extracted) and have not been flagged to outlier or as the non-main analysis
new_reports <- new_reports[outlier == 0 & main_analysis == 1]

## sum up mod_contra, trad_contra, and any_contra assuming no miscategorization of "other" categories
methods <- names(new_reports)[grepl("contra_",names(new_reports))]
trad_methods <- methods[grepl("lactat|withdraw|rhythm|calendar|trad",methods)]
mod_methods <- methods[!methods %in% trad_methods]

## if mod_contra or trad_contra is missing and the specific methods underlying them are not also missing (so that data was
## actually collected by the survey), sum up the specific methods
new_reports[is.na(mod_contra) & is.na(category_mismatch) & rowSums(!is.na(new_reports[,mod_methods,with=F])) != 0,mod_contra := rowSums(.SD,na.rm = T),.SDcols = mod_methods]
new_reports[is.na(trad_contra) & is.na(category_mismatch) & rowSums(!is.na(new_reports[,trad_methods,with=F])) != 0,trad_contra := rowSums(.SD,na.rm = T),.SDcols = trad_methods]
new_reports[is.na(any_contra) & is.na(category_mismatch),any_contra := mod_contra + trad_contra]

## calculate met_demand
new_reports[!is.na(met_demand_mod),met_demand := met_demand_mod]
new_reports[is.na(met_demand) & !is.na(met_demand_all),met_demand := met_demand_all*(mod_contra/any_contra)]
new_reports[is.na(met_demand) & !is.na(unmet_need),demand_sample_size := round(sample_size*(any_contra + unmet_need))]
new_reports[is.na(met_demand) & !is.na(unmet_need),met_demand := mod_contra/(any_contra + unmet_need)]

## check for issues
new_reports[mod_contra > 1 | mod_contra < 0 | met_demand < 0 | met_demand > 1 | mod_contra > met_demand]


## COMBINE REPORT DATA AND IDENTIFY POINTS THAT NEED AGE-SPLITTING #################################################

reports[nid %in% new_reports[,unique(nid)]]
reports <- rbind(reports[!nid %in% new_reports[,unique(nid)]],new_reports,fill=T)
reports[,cv_report := 1]
reports[,sex_id := 3]

## note any extracted reports that have since been extracted in microdata form and remove
reports[nid %in% micro[,unique(nid)]]
reports <- reports[!nid %in% micro[,unique(nid)]]

## fill in missingness in flagging variables
reports[is.na(cv_subgeo),cv_subgeo := 0]
reports[is.na(currmar_only),currmar_only := 0]
reports[is.na(evermar_only),evermar_only := 0]
reports[is.na(missing_desire),missing_desire := 0]
reports[is.na(missing_desire_later),missing_desire_later := 0]
reports[is.na(missing_fecund),missing_fecund := 0]
reports[is.na(no_pregppa),no_pregppa := 0]
reports <- merge(reports,locs,by="ihme_loc_id",all.x=T)

## save extractions that need to be split
split <- reports[is.na(age_group_id) | age_group_id == 22]
split <- merge(split,sdi,all.x=T,by=c("location_id","year_id"))
write.csv(split,file.path(input_dir,"prepped/reports_to_be_split.csv"),row.names = F)

## subset to age-specific estimates and cut out tiny sample sizes
reports <- reports[age_group_id %in% seq(8,14) & (sample_size >= 20 | is.na(sample_size))]


## CREATE MODELABLE MOD CONTRA DATASET #################################################

## subset tabulated reports to modern contraceptive usage
reports[,data := mod_contra]

## compile reports and microdata for modern contraception and merge on sdi
mod_contra <- rbind(modern[!is.na(age_group_id)],reports,fill=T)
mod_contra <- merge(mod_contra, sdi,all.x=T)

## write modern contraceptive output
write.csv(mod_contra,file.path(output_dir,"mod_contra_merged_presplit.csv"),row.names=F)

## CREATE MODELABLE MET DEMAND DATASET #################################################

## subset tabulated reports to met demand and retain mod_contra as cv_mod
reports[,cv_mod := mod_contra]
reports[,data := met_demand]
reports[,sample_size := demand_sample_size]
reports[sample_size < 20, data := NA]

## combine met demand microdata and tabulations and mergo on sdi
met_demand <- rbind(met_need_demanded[!is.na(age_group_id)],reports,fill=T)
met_demand <- merge(met_demand, sdi,all.x=T)

## all observations must have a value for modern contraceptive use (at the very least)
met_demand[is.na(cv_mod)]

## write output for met demand
write.csv(met_demand,file.path(output_dir,"met_demand_merged_presplit.csv"),row.names=F)
