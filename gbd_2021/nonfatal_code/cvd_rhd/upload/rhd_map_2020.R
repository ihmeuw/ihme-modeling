################################
##author: USERNAME
##date: DATE
##purpose:    Create endemic/nonendemic cutoffs for RHD. This script also uploads CSMR to the appropriate bundles
##  source("/FILEPATH")
#######################################


os <- .Platform$OS.type

  j<- "/FILEPATH/"
  h <- paste0("FILEPATH/")

  date<-gsub("-", "_", Sys.Date())

library(data.table)
library(RMySQL)
library(R.utils)
library(cowplot,lib.loc="/FILEPATH/")
library(Hmisc)

################### ARGS #########################################
######################################################

base_fill<-F ##USERNAME: to fill in new subnationals and new locs (new locs filled based on SDI, new subnats filled based on parent)
upload_csmr<-F
upload_old_csmr<-F
annual_gbd_compare_version<-7273 ##USERNAME: update this with the most recent GBD compare version w/ annualized cod-corrected results
age_group <- 7 ##USERNAME: input the age group id to evaluate 

decomp_step <- "iterative"
gbd_round_id <- 7
compare_round<-6

################### PATHS #########################################
######################################################
map_folder<-paste0(j, "FILEPATH/")
#old_map_path<-paste0(j, "FILEPATH/endemic_subnational_17Jun2017.csv")
#old_map_path<-paste0(j, "FILEPATH/endemic_subnational_03Mar2017.csv")

output_path_old<-paste0(j, "FILEPATH/rhd_endemic_2019_map_", date, ".csv")
output_path_new<-paste0(j, "FILEPATH/rhd_endemic_2020_map_", date, ".csv")
plot_output_2019<-paste0(j, "FILEPATH/rhd_endemicity_maps_agegrp_2019", age_group, "_", date, ".pdf")
plot_output_2020<-paste0(j, "FILEPATH/rhd_endemicity_maps_agegrp_2020", age_group, "_", date, ".pdf")

################### SCRIPTS #########################################
######################################################

central <- '/FILEPATH/'
for (func in paste0(central, list.files(central))) source(paste0(func))
suppressMessages(sourceDirectory(paste0("/FILEPATH/")))
#source(paste0("/FILEPATH/bind_covariates.R"))
source(paste0(h, "FILEPATH/bind_covariates.R"))
#source(paste0(j, "FILEPATH/GBD_WITH_INSETS_MAPPING_FUNCTION.R"))
#source(paste0(j, "FILEPATH/gbd_inset_map_ur.R"))
#source(paste0(j, "FILEPATH/global_map.R"))
source("/FILEPATH/global_map.R")
#source(paste0(j, "FILEPATH/newglobalmap.R"))

source(paste0("/FILEPATH/rm_upload_epi.R"))
source(paste0("/FILEPATH/rm_upload_epi.R"))

################### GET DATA #########################################
######################################################

if(base_fill==T){
  
  ##USERNAME: get last year's best
  old_map<-get_recent(paste0(map_folder, "gbd_round_", compare_round, "/"))
  
  ##USERNAME: get new locs
  locs<-get_location_metadata(location_set_id = 35)
  new_locs<-locs[!location_id %in% unique(old_map$location_id)]
  
  ##USERNAME: for new subnats, fill down the parent value
  new_subnats<-new_locs[ihme_loc_id %like% "_", ]
  new_subnats<-merge(new_subnats, old_map, by.x="parent_id", by.y="location_id")
  
  ##USERNAME: new nats, use SDI cutoff of 0.6
  new_nats<-new_locs[!ihme_loc_id %like% "_", ]
  new_nats[, `:=` (sex_id=3, year_id=2020, age_group_id=22)]
  new_nats<-bind_covariates(new_nats, cov_list="Socio-demographic Index")[[1]]
  new_nats[sdi>0.6, endemic:=0]
  new_nats[sdi<=0.6, endemic:=1]
  
  endem_map_2019<-copy(old_map)
  
  endem_map_2019<-rbindlist(list(endem_map_2019, new_subnats[, .(location_id, endemic)], 
                            new_nats[, .(location_id, endemic)]), use.names=T)
  
  write.csv(endem_map_2019, file=output_path_old, row.names=F)
  stop("Back fill of map is done")
  
}else{
  locs<-get_location_metadata(location_set_id=35)

  cod <- get_outputs(topic='cause', cause_id=492, metric_id = 3, year_id=2019, location_id='all', age_group_id=7, sex_id=3, 
                     gbd_round_id = 7, decomp_step = 'step2',  compare_version_id=7273)
  

  
  ##USERNAME: get SDI
  cod_and_covs<-bind_covariates(df=cod, cov_list="Socio-demographic Index")
  cod<-cod_and_covs[[1]]
  
  ##USERNAME: find endemic locs
  cod[sdi<.6, endem:=1]
  cod[val>.15/10^5, endem:=2]
  cod[sdi<=.6 & val>.15/10^5, endem:=3]
  cod[is.na(endem), endem:=0]
  
  ##USERNAME: hardcode SA to be endemic
  sa_locs<-locs[parent_id==196 | location_id==196, location_id]
  
  cod[location_id %in% sa_locs, endem:=1]
  
  
  
  cod[endem==0, endem_label:="Death rate<.15 per 100k and SDI>.6"]
  cod[endem==1, endem_label:="SDI<.6"]
  cod[endem==2, endem_label:="Death rate>.15 per 100k"]
  cod[endem==3, endem_label:="Death rate>.15 per 100k and SDI<.6"]
  
  cod[, endemic:=ifelse(endem==0, 0, 1)]
  
  
  endem_map<-cod[, .(location_id, endem)]
  endem_map[, endemic:=ifelse(endem==0, 0, 1)]
  endem_map[, endem:=NULL]
}


write.csv(endem_map, file=output_path_new, row.names=F)

################### MAP #########################################
######################################################
pdf(plot_output, width=12)
##USERNAME: create map variable
plot_data<-copy(cod)
plot_data<-merge(plot_data, locs[, .(location_id, ihme_loc_id, level)], by="location_id")
setnames(plot_data, "endemic", "mapvar")

map <- global.map(data=plot_data, map.var="mapvar", output.path = plot_output_2020, show.subnat = TRUE,
                 scale.type = "cat", limits=c(.9, 1.1, 2.1, 3.1), labels=c("Death rate<.15 per 100k and SDI>.6", "SDI<.6", "Death rate>.15 per 100k", "Death rate>.15 per 100k and SDI<.6"),
                  plot.title=paste0("RHD endemicity classification for GBD 2020, using age group id: ", unique(plot_data$age_group_name)))

map <- global.map(data=plot_data, map.var="mapvar", output.path = plot_output_2020, show.subnat = TRUE,
                  scale.type = "cat", limits=c(0, 0.9, 1.1), labels=c("Non-endemic", "Endemic"),
                  plot.title=paste0("RHD endemicity classification for GBD 2019, using age group id: ", unique(plot_data$age_group_name)))

if(T){
  ##USERNAME: print out locs for putting in RHD writeup
  ##endemic:
  paste0(sort(plot_data[mapvar>0, location_name]), collapse=", ")
  ##non-endemic:
  paste0(sort(plot_data[mapvar>0, location_name]), collapse=", ")
}


################### MAP CHANGES #########################################
######################################################

##USERNAME: get old data
#old_map<-fread(old_map_path)
#old_map <- fread(output_path_old)
old_map <- read.csv(paste0("/FILEPATH/rhd_endemic_map_2019_02_05.csv"))
old_map<-unique(old_map[, .(location_id, endemic)])
setnames(old_map, "endemic", "old_endemic")

cod<-merge(cod, old_map, by="location_id", all.x=T)

##USERNAME: identify changes
cod[, endem_change:=ifelse(endem==0 & old_endemic==1, 1, ifelse(endem!=0 & old_endemic==0, endem, 0))]

plot_data<-cod[endem_change!=0]

plot_data<-merge(plot_data, locs[, .(location_id, ihme_loc_id)], by="location_id")
setnames(plot_data, "endem_change", "map.var")

map <- global.map(data=plot_data, map.var="map.var", output.path = plot_output_2020, show.subnat = TRUE,
                  scale.type = "cat", limits=c(0, .9, 1.1, 2.1, 3.1), labels=c("Death rate<.15 per 100k and SDI>.6", "SDI<.6", "Death rate>.15 per 100k", "Death rate>.15 per 100k and SDI<.6"),
                  plot.title=paste0("RHD endemicity classification for GBD 2020, using age group id:10 to 14"))

##endemic change:
locs_change <- paste0(sort(plot_data[endem_change>0, location_name]), collapse=", ")
locs_change <- paste0(sort(plot_data[endem_change>0, location_name]))
changes <- plot_data[endem_change!=0]


dev.off()

write.csv(cod, 
          file="/FILEPATH/changes_2020_09_04.csv",
          row.names=F)

### Get old csmr and old sdi

source("/FILEPATH/get_covariate_estimates.R")

covariates <-  get_ids("covariate")

old_sdi <- get_covariate_estimates(gbd_round_id=6, covariate_id=881, decomp_step='step2')
setnames(old_sdi, "mean_value", "old_sdi")
old_sdi <- old_sdi[year_id ==2019]
cod <- merge(cod, old_sdi[, .(location_id, old_sdi)], by="location_id", all.x=T)

old_cod <- get_outputs(topic='cause', cause_id=492, metric_id = 3, year_id=2019, location_id='all', age_group_id=7, sex_id=3, 
                       gbd_round_id = 6, decomp_step = 'step2', compare_version_id = 7244)
setnames(old_cod, "val", "csmr2019")

cod <- merge(cod, old_cod[, .(location_id, csmr2019)], by="location_id", all.x=T)
setnames(cod, "val", "csmr2020")

cod <- merge(cod, locs[, . (location_id, parent_id, level, super_region_name)], by="location_id", all.x = T)
cod <- unique(cod)
cod1 <- cod
cod1$old_endemic[is.na(cod1$old_endemic)] <-0
cod1$endem_change[is.na(cod1$endem_change)] <-0

cod1 <- select(cod1, year_id, age_group_id, age_group_name, measure_id, metric_id, sex_id, acause, cause_name, sex, parent_id, level, super_region_name, location_id, location_name, 
               csmr2019, csmr2020, old_sdi, sdi, endem, endem_label, endemic, old_endemic, endem_change)

setnames(cod1, "sdi", "sdi2020")
setnames(cod1, "old_sdi", "sdi2019")

cod_order <- cod1[with(cod1, order(endem_change, super_region_name, parent_id, location_id, )),]

endem_changes <- cod_order[endem_change!=0]

data <- cod_order

write.csv(cod_order, 
          file="/FILEPATH/full_order_2020_09_04.csv",
          row.names=F)


################### UPLOAD CSMR #########################################
######################################################

if(upload_csmr==T){
  
  ################### GET AND CLEAN DATA #########################################
  ######################################################
  message("Getting annual CSMR...")

  #old_csmr <- get_outputs(topic="cause", metric_id=3, cause_id=492, sex_id=c(1,2), age_group_id="most_detailed", location_id="all", year_id=c(1990:2019), compare_version_id=310) 
  #                   gbd_round_id = 6)
  
  csmr <- get_outputs(topic='cause', cause_id=492, metric_id = 3, year_id=c(1990:2020), sex_id=c(1,2), location_id = 'most_detailed', age_group_id="most_detailed",
                     gbd_round_id = 7, decomp_step = 'step2',  compare_version_id=7273)
  
  message("Done getting annual CSMR")
  
  csmr<-csmr[!is.na(val)]
  setnames(csmr, "val", "mean")
  
  csmr[, seq:=NA]
  
  csmr[, nid:=259602]
  csmr[, field_citation_value:="Institute for Health Metrics and Evaluation (IHME). Post-CoDCorrect estimates of cause-specific mortality rates for the relevant RHD DisMod model."]
  csmr[, source_type:="Mixed or estimation"]
  csmr[, smaller_site_unit:=0]
  csmr[, age_issue:=0]
  
  ##USERNAME: format age_start/end
  csmr[, age_start:=(age_group_id-5)*5]
  csmr[age_group_id==30, age_start:=80]
  csmr[age_group_id==31, age_start:=85]
  csmr[age_group_id==32, age_start:=90]
  csmr[age_group_id==235, age_start:=95]
  csmr[age_group_id==34, age_start:=2]
  csmr[age_group_id==238, age_start:=1]

  
  csmr[, age_end:=age_start+4]
  csmr[age_group_id==34, age_end:=4]  
  csmr[age_group_id==238, age_end:=1.99]
  
  ##USERNAME:format year and sex
  csmr[, `:=` (year_start=year_id, year_end=year_id)]
  csmr[, year_issue:=0]
  
  ##USERNAME: measure cols
  csmr[, unit_type:="Person"]
  csmr[, unit_value_as_published:=1]
  csmr[, measure_adjustment:=0]
  csmr[, measure_issue:=0]
  csmr[, measure:="mtspecific"]
  csmr[, case_definition:="CSMR from step 2 & gbdround 7 COD post codcorrect"]
  csmr[, note_modeler:=NA]
  csmr[, extractor:="USERNAME"]
  csmr[, is_outlier:=0]
  
  ##USERNAME: other cols
  csmr[, underlying_nid:=NA]
  csmr[, sampling_type:=NA]
  csmr[, representative_name:="Unknown"]
  csmr[, urbanicity_type:="Unknown"]
  csmr[, recall_type:="Not Set"]
  csmr[, uncertainty_type:=NA]
  csmr[, input_type:=NA]
  csmr[, standard_error:=NA]
  csmr[, effective_sample_size:=NA]
  csmr[, design_effect:=NA]
  csmr[, site_memo:=NA]
  csmr[, case_name:=NA]
  csmr[, case_diagnostics:=NA]
  csmr[, response_rate:=NA]
  csmr[, note_SR:=NA]
  csmr[, uncertainty_type_value:=95]
  csmr[, sample_size:=NA]
  csmr[, seq_parent:=NA]
  csmr[, recall_type_value:=NA]
  csmr[, cases:=NA]
  
  ##USERNAME: drop cols
  csmr[, c("measure_id", "cause_id", "cause_name", "year_id", "age_group_id", "age_group_name", "sex_id", "metric_id", "metric_name", "measure_name", "expected"):=NULL]
  
  ################### UPLOAD #########################################
  ######################################################
  #csmr<-merge(csmr, endem_map, by="location_id", all.x=T)
  csmr<-merge(csmr, endem_map_2019, by="location_id", all.x=T)
  csmr<-csmr[!is.na(endemic)]
  
  for(endem in c(0, 1)){
    b_id<-ifelse(endem==1, 420, 421)
    
    #if(nrow(csmr[is.na(endemic)])>0){stop("Some locations are not mapped!")}
    
    csmr.sub<-csmr[endemic==endem]
    csmr.sub[, bundle_id:=b_id]
    ##USERNAME: only upload CSMR for estimation years and for loc-years with prevalence data
    est_yrs<-c(seq(from=1990, to=2020, by=5))
    
    bun_data<-get_bundle_data(bundle_id = b_id, decomp_step = "iterative", gbd_round_id = 7, export = T)
    prev_data<-bun_data[measure=="prevalence"]
    loc_yrs<-unique(prev_data[, .(year_start, year_end, location_id)])
    loc_yrs[, year_id:=floor((year_start+year_end)/2)] ##USERNAME: 
    loc_yrs<-loc_yrs[!year_id %in% est_yrs]
    loc_yrs[, loc_yr:=paste0(location_id, "_", year_id)]
    
    csmr.sub[, loc_yr:=paste0(location_id, "_", year_start)]
    csmr.sub<-csmr.sub[loc_yr %in% unique(loc_yrs$loc_yr) | year_start %in% est_yrs]
    #csmr.sub[, c("endemic", "loc_yr"):=NULL]
    
    locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 7, decomp_step = "iterative")
    locs <- subset(locs, level==3 | level==4)
    csmr.sub <- subset(csmr.sub, location_id %in% locs$location_id)

    
    ################### FINISH UPLOAD #########################################
    ######################################################
    csmr_folder<-paste0(j, "FILEPATH/")
    suppressWarnings(dir.create(csmr_folder, recursive = T))
    ##USERNAME:save
    openxlsx::write.xlsx(csmr.sub, paste0(csmr_folder, "cleaned_csmr_locs_", date, ".xlsx"), sheetName="extraction")
  }
}


################### PULL OLD UPLOADED #########################################
######################################################

#if(upload_old_csmr==T){
   #b_id<-420
#  tab<-"dismod"
#  measure_id<-15
#  mv_id1<-176717
#  message("Getting data...")
#  con <- dbConnect(dbDriver("MySQL"), username='USERNAME', password='PASSWORD', host='HOST')
  
  
  ##USERNAME: this is a vector of commands, length2 (one for each mv_id)
  #sqlcmds <- paste0("SELECT *
  #                  FROM DATABASE", tab,"
  #                  WHERE (model_version_id = ", c(mv_id1), ") AND (measure_id =", measure_id, ")")
  

  #csmr.sub <- as.data.table(dbGetQuery(con, sqlcmds[1]))
  #dbDisconnect(con)


  ## Update CSMR

  ## Remove old csmr from the bundle
  b_id <- 421
  bun_data <- get_bundle_data(bundle_id = b_id, decomp_step = "iterative", gbd_round_id = 7, export = F)
  bun_data <- bun_data[measure=="mtspecific"]
  wipe_csmr <- select(bun_data, seq)
  wipe_csmr_file <- paste0(j, "/FILEPATH/wipe_csmr_",  date, ".xlsx")
  openxlsx::write.xlsx(wipe_csmr, paste0(wipe_csmr_file), sheetName="extraction")
  upload_bundle_data(b_id, decomp_step =  "iterative", gbd_round_id = 7, wipe_csmr_file)
  
  
  
  ## Upload updated CSMR
  upload_bundle_data(b_id, decomp_step =  "iterative", gbd_round_id = 7, 
             paste0(j,  "/FILEPATH/cleaned_csmr_locs_2020_09_02.xlsx"))
  
  
  #Save bundle version
  result <- save_bundle_version(b_id, decomp_step, gbd_round_id, include_clinical= NULL)
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Bundle version ID: %s', result$bundle_version_id))
  bundle_version_id <- result$bundle_version_id
  
  data <- data.table(get_bundle_version(bundle_version_id, fetch = 'all'))
  data <- data[, crosswalk_parent_seq := NA]
  data <-  data[group_review == 1 | is.na(group_review)]
  data <- data[measure!= "incidence"]
  
  openxlsx::write.xlsx(data, paste0(j,"/FILEPATH/bun_data_updated_csmr_",  date, ".xlsx"), sheetName="extraction")
    
    if (is.null(bundle_version_id) | missing(bundle_version_id)) {
      
      stop("You need to set bundle_version_id to upload data.")
      
    } else {
      

     
  


  #openxlsx::write.xlsx(bun_data, 
  #                     paste0(j, "/FILEPATH/iterative_BundleData_420_prevalence_incidence.xlsx"), 
  #                     sheetName="extraction")
  #upload_bundle_data(b_id, decomp_step =  "iterative", gbd_round_id = 7, 
  #                   paste0(j, "/FILEPATH/iterative_BundleData_420_prevalence_incidence.xlsx"))
                     
  #bun_data <- get_bundle_data(bundle_id = b_id, decomp_step = "iterative", gbd_round_id = 7, export = F)
  #upload_bundle_data(b_id, decomp_step =  "iterative", gbd_round_id = 7, 
  #                   paste0(csmr_folder, "cleaned_csmr_", date, ".xlsx"))
  
  #bun_version <- save_bundle_version(420, decomp_step = 'iterative', gbd_round_id = 7)
  #b_version <- get_bundle_version(bun_version$bundle_version_id, fetch = 'all')
  #unique(b_version$measure)
  ################### REFORMAT #########################################
  ######################################################
  
  csmr.sub[, c("model_version_dismod_id", "model_version_id", "request_id", "legacy_input_data_key"):=NULL]
  setnames(csmr.sub, "outlier_type_id", "is_outlier")
  setnames(csmr.sub, "recall_type_id", "recall_type")
  
  csmr.sub[, seq:=NA]
  csmr.sub[, measure:="mtspecific"]
  csmr.sub[, sex:=ifelse(sex_id==1, "Male", "Female")]
  
  ##USERNAME: measure cols
  csmr.sub[, unit_type:="Person"]
  csmr.sub[, unit_value_as_published:=1]
  csmr.sub[, measure_adjustment:=0]
  csmr.sub[, measure_issue:=0]
  csmr.sub[, measure:="mtspecific"]
  csmr.sub[, case_definition:="CSMR from COD model"]
  csmr.sub[, note_modeler:=NA]
  csmr.sub[, extractor:="USERNAME"]
  csmr.sub[, is_outlier:=0]
  
  ##USERNAME: other cols
  csmr.sub[, underlying_nid:=NA]
  csmr.sub[, sampling_type:=NA]
  csmr.sub[, representative_name:="Unknown"]
  csmr.sub[, urbanicity_type:="Unknown"]
  csmr.sub[, recall_type:=NULL]
  csmr.sub[, recall_type:="Not Set"]
  
  csmr.sub[, uncertainty_type:=NA]
  csmr.sub[, input_type:=NA]
  csmr.sub[, standard_error:=NA]
  csmr.sub[, effective_sample_size:=NA]
  csmr.sub[, design_effect:=NA]
  csmr.sub[, site_memo:=NA]
  csmr.sub[, case_name:=NA]
  csmr.sub[, case_diagnostics:=NA]
  csmr.sub[, response_rate:=NA]
  csmr.sub[, note_SR:=NA]
  csmr.sub[, uncertainty_type_value:=95]
  csmr.sub[, sample_size:=NA]
  csmr.sub[, field_citation_value:="Institute for Health Metrics and Evaluation (IHME). Post-CoDCorrect estimates of cause-specific mortality rates for the relevant RHD DisMod model."]
  csmr.sub[, source_type:=NULL]
  csmr.sub[, source_type:="Mixed or estimation"]
  csmr.sub[, seq_parent:=NA]
  csmr.sub[, recall_type_value:=NA]
  csmr.sub[, cases:=NA]
  
  
  ################### FINISH UPLOAD #########################################
  ######################################################
  csmr_folder<-paste0(j, "FILEPATH/")
  suppressWarnings(dir.create(csmr_folder, recursive = T))
  ##USERNAME:save
  openxlsx::write.xlsx(csmr.sub, paste0(csmr_folder, "reupload_mvid_", mv_id1, "_csmr_", date, ".xlsx"), sheetName="extraction")
  
  #rm_upload_epi(b_id, rm_only=T, rm_conditional="measure=='mtspecific'")
  
  #rm_upload_epi(b_id, csmr_folder, match=F)
  
}
