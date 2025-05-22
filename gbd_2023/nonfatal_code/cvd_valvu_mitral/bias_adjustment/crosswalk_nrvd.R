## Crosswalk Documentation: FILEPATH/Crosswalk Documentation.pdf
## Purpose: Launch script for the CVD team's crosswalk pipeline. Once you pull in your data and provide
##          specifications, main_mrbrt_crosswalk does the following:
##              - Sex splits the data
##              - If necessary, crosswalks between one+ alternate case definitions
##              * Almost everything is toggle-able. See documentation about what features can be edited. 
##          Afterwards, this script will make an RMarkdown with vetting information, which ideally has:
##              - Title of XW
##              - Date
##              - Alternate definitions to be XWed
##              - Counts of matches and ratios
##              - Betas for sex-split and crosswalk
##              - Plots for evaluating the crosswalk
##

rm(list=ls())

# Date and functions
date <- gsub("-", "_", Sys.Date())

"%ni%" <- Negate("%in%")

"%contain%" <- function(values,x) {
  tx <- table(x)
  tv <- table(values)
  z <- tv[names(tx)] - tx
  all(z >= 0 & !is.na(z))
}

packages <- c("rmarkdown", "data.table", "R.utils", "ggplot2", "openxlsx")
suppressMessages(invisible(lapply(packages, library, character.only = TRUE)))
library(reticulate) 
reticulate::use_python("/FILEPATH/python")

source('/FILEPATH/main_mrbrt_crosswalk_func.R')
source("/FILEPATH/aggregate_subnats.R")

source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/get_covariate_estimates.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_age_metadata.R")


### Create variables

acause <-  'cvd_valvu_aort'
bundle_id <- 2987
xw_measure <- 'prevalence'
xw_index <- 47
model_abbr <- paste0(acause, "_", xw_measure, "_", bundle_id, "_index", xw_index)

### Paths
##################################################################################

## path for settings sheets
settings_path <- paste0('/FILEPATH/', acause, '.xlsx')
##################################################################################
# 1) Set arguments and filepaths, load in crosswalk settings
##################################################################################

### Arguments

args <- commandArgs(trailingOnly = T)
if(length(args)==0){
  ######################################################
  # enter arguments here if running code interactively #
  ######################################################
  # acause <- 'cvd_valvu_mitral' #'cvd_valvu_mitral' #'cvd_valvu_aort' #'cvd_pvd'
  # bundle_id <- 2993 # 345 #2993 #2987 #
  # xw_measure <- 'prevalence'
  # xw_index <- 5
  ######################################################
} else {
  # if not running code interactively, arguments will be pulled in from job call
  acause <- as.character(args[1])
  bundle_id <- as.numeric(args[2])
  xw_measure <- as.character(args[3])
  
  xw_index_path <- as.character(args[4])
  xw_index_file <- fread(xw_index_path)
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  xw_index <- as.numeric(xw_index_file[task_id, xw_index])
  
}

## path for storage folder
storage_folder <- paste0('/FILEPATH/', acause, '/')

# create storage folder if it doesn't exist
dir.create(storage_folder, showWarnings=F)

## path for save crosswalk version
save_crosswalk_result_file_name <- paste0("xw_results_", date , "_", model_abbr, ".xlsx")
save_crosswalk_result_file <- paste0(storage_folder, save_crosswalk_result_file_name)

## path for diagnostics
plot_source_folder <- '/FILEPATH/'
plot_output_folder <- paste0('/FILEPATH/', acause, '/')

# create folder for diagnostics if it doesn't exist
dir.create(plot_output_folder, showWarnings=F)

### Extracting and formatting all settings variables

## import settings file
settings <- as.data.table(read.xlsx(settings_path, sheet=paste0(xw_measure, "_", bundle_id)))
settings <- settings[model_index==xw_index]
if(nrow(settings)!=1){
  stop(paste0('One unique row with xw_index ', xw_index, ' not found.'))
}
settings[, `:=` (Modeler.Notes = NULL, model_index = NULL)]

# store all user-specified args into a list
var_collector <- list()

for (single_var in names(settings)) {
  single_setting <- settings[, get(single_var)]
  
  if(is.null(single_setting)){
    single_setting <- NA
  }
  
  # unlist if necessary
  if(class(single_setting)=='list'){
    single_setting <- unlist(single_setting)
  }
  
  # T/F char to Boolean
  if(single_setting %in% c('T', 'F')) {
    single_setting <- ifelse(single_setting == 'T', T, F)
  }
  
  # number/float char to numeric
  if(!grepl("\\D", single_setting) | single_var %in% c('trim_pct')) {
    single_setting <- as.numeric(single_setting)
  }
  
  # Coerce necessary values to integer
  if(single_var %in% c("degree", "knot_number")){
    single_setting <- as.integer(single_setting)
  }
  
  # comma cat string to vector
  if(single_var %in% c('alternate_defs', 'addl_x_covs', 'knots', 'test_age_overlap', 
                       'test_year_overlap', 'test_age_range', 'test_year_range', 'test_subnat_to_nat', 'test_allow_wide_age_bin_match') & !is.na(single_setting)) {
    single_setting <- unlist(strsplit(single_setting, ","))
    if(single_var %in% c('knots', 'test_age_overlap', 
                         'test_year_overlap', 'test_age_range', 'test_year_range')){
      single_setting <- as.numeric(single_setting)
    }
    if(single_var=='test_subnat_to_nat' | single_var=='test_allow_wide_age_bin_match'){
      single_setting <- as.logical(single_setting)
    }
  }
  
  # mount settings to environment
  if(all(!is.na(single_setting))) {
    assign(single_var, single_setting)
  } else {
    assign(single_var, NULL)
  }
  
  # also store them into a collection for saving later
  var_collector[[single_var]] <- single_setting
  
}

##################################################################################
# 2) Get bundle data / save bundle data
##################################################################################

if (if_save_bundle_version) {
  result <- save_bundle_version(bundle_id=bundle_id, include_clinical=TRUE)
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Bundle version ID: %s', result$bundle_version_id))
  bundle_version_id <- result$bundle_version_id
  data <- data.table(get_bundle_version(bundle_version_id))
  var_collector[['bundle_version_id']] <- bundle_version_id
} else {
  data <- data.table(get_bundle_version(bundle_version_id))
}

data <- data[measure == xw_measure]

# ## inpatient only
# if(clinical_informatics == 3) {
#   data[, cv_alternate := ifelse(clinical_data_type=="inpatient", 1, 0)]
# }
# 
# ## claims only
# if(clinical_informatics == 2) {
#   data[, cv_alternate := ifelse(clinical_data_type=="claims", 1, 0)]
# }
# 
# ## all clinical informatics
# if(clinical_informatics == 1) {
#   data[, cv_alternate := ifelse(clinical_data_type!="", 1, 0)]
#   
#   ## select all US states and drop US States with HCUP data
location <- get_location_metadata(location_set_id=35, release_id=release_id)
us <- unlist(unique(location[parent_id==102,c("location_id")]), use.names=F)

# US HCUP data duplicates the Marketscan data
data <- subset(data, !(location_id %in% us & grepl("HCUP", field_citation_value)))

## drop data from non-representative source
data <- subset(data, !(location_name %in% c("Jordan", "Iran", "Meghalaya", "Karnataka", "Nepal", "Kenya", "Oxfordshire", "Botswana", "Tibet")))

# }

if (acause == "cvd_valvu_aort") {
  data[acause == "cvd_valvu_aort",
       `:=` (cv_claims = ifelse(clinical_data_type == "claims", 1, 0),
             cv_cms = ifelse(field_citation_value %like% "CMS", 1, 0),
             cv_inpatient = ifelse(clinical_data_type == "inpatient",1,0))]
  data[, cv_claims := ifelse(clinical_data_type == "claims - flagged ", 1, cv_claims)]
  data[, c("mean_age", "age_sd") := NULL]
  data[cv_cms==1, cv_claims:= 0]
 #data <- subset(data, data$cv_cms==0)
  
  ## select all outliers
  location <- get_location_metadata(location_set_id=35, release_id=release_id)
  us <- unlist(unique(location[parent_id==102,c("location_id")]), use.names=F)
  mex <- unlist(unique(location[parent_id==130,c("location_id")]), use.names=F)
  brazil <- unlist(unique(location[parent_id==135,c("location_id")]), use.names=F)
  china <- unlist(unique(location[parent_id==6,c("location_id")]), use.names=F)
  data[, is_outlier := ifelse(location_id %in% mex & clinical_version_id ==3, 1, is_outlier)]
  data[, is_outlier := ifelse(location_id %in% brazil & clinical_version_id ==3, 1, is_outlier)]
  data[, is_outlier := ifelse(clinical_data_type == "claims - flagged" & clinical_version_id ==3, 1, is_outlier)] # Mongolia and Rep of Korea
  outlier_locations <- c('Belgium', 'Cyprus', 'Malta', 'Portugal', 'Argentina', 'Chile', 'Ecuador', 'Tibet', 
                         'Armenia', 'Jordan', 'Iran (Islamic Republic of)', 'Meghalaya', 'Karnataka', 'Nepal', 'Kenya', 'Botswana', 'Fujian', 
                         'Philippines', 'Georgia', 'Croatia', 'Spain', 'Czechia', 
                         'Serbia','Türkiye')
  
  data[, is_outlier := ifelse(location_name %in% outlier_locations & clinical_version_id ==2, 1, is_outlier)]
  data[, is_outlier := ifelse(nid %in% c(397812, 397813, 397814, 431674, 466458, 466459, 509155), 1, is_outlier)] # outliering Poland claims
  # data[, is_outlier := ifelse(nid %in% c(234764, 234762, 234765, 292577), 1, is_outlier)] # outliering uk inpatient
  # data[, is_outlier := ifelse(nid %in% c(336851, 336852), 1, is_outlier)] # outliering japan inpatient
  # data[, is_outlier := ifelse(nid %in% c(334464, 334465, 439738, 474095), 1, is_outlier)] # outliering Italy inpatient
  data[, is_outlier := ifelse(nid  %in% c(234766, 234769, 234771), 1, is_outlier)] #Hospital Discharge Survey 1988-1992.
  
  unique_location_names <- unique(data[data$clinical_data_type == 'inpatient', 'location_name'])
  
  
  
  # US HCUP data duplicates the Marketscan data
  data <- subset(data, !(location_id %in% us & grepl("HCUP", field_citation_value)))
  
}

if (acause == "cvd_valvu_mitral") {
  data[acause == "cvd_valvu_mitral",
       `:=` (cv_claims = ifelse(clinical_data_type == "claims", 1, 0),
             cv_cms = ifelse(field_citation_value %like% "CMS", 1, 0),
             cv_inpatient = ifelse(clinical_data_type == "inpatient",1,0))]
  data[, cv_claims := ifelse(clinical_data_type == "claims - flagged ", 1, cv_claims)]
  data[, c("mean_age", "age_sd") := NULL]
  data[cv_cms==1, cv_claims:= 0]
  

  
  ## select all outliers
  location <- get_location_metadata(location_set_id=35, release_id=release_id)
  us <- unlist(unique(location[parent_id==102,c("location_id")]), use.names=F)
  mex <- unlist(unique(location[parent_id==130,c("location_id")]), use.names=F)
  brazil <- unlist(unique(location[parent_id==135,c("location_id")]), use.names=F)
  china <- unlist(unique(location[parent_id==6,c("location_id")]), use.names=F)
  data[, is_outlier := ifelse(location_id %in% mex & clinical_version_id ==3, 1, is_outlier)]
  data[, is_outlier := ifelse(location_id %in% brazil & clinical_version_id ==3, 1, is_outlier)]
  data[, is_outlier := ifelse(location_id %in% china & clinical_version_id ==3, 1, is_outlier)]
  data[, is_outlier := ifelse(location_name == "Belgium" & clinical_version_id ==3, 1, is_outlier)]
  data[, is_outlier := ifelse(location_name == "Finland" & clinical_version_id ==3, 1, is_outlier)]
  data[, is_outlier := ifelse(clinical_data_type == "claims - flagged" & clinical_version_id ==3, 1, is_outlier)]
  outlier_locations <- c("Philippines",  "Armenia", "Poland", "Belgium", "Iceland", "Portugal",
                         "Latvia", "Lithuania", "Cyprus", "Malta", 
                         "Jordan", "Iran (Islamic Republic of)", "Meghalaya", "Karnataka", "Nepal", "Kenya", "Botswana", "Fujian",
                         "Chile", "Argentina")
  data[, is_outlier := ifelse(nid == 244369 & clinical_version_id ==3, 1, is_outlier)] # Marketscan 2000    
  data[, is_outlier := ifelse(nid ==  331084  & clinical_version_id ==3, 1, is_outlier)] # Iran  inpatient 2001-2010.
  data[, is_outlier := ifelse(nid ==  433061  & clinical_version_id ==3, 1, is_outlier)] # Argentina  inpatient 2001-2010.
  # data[, is_outlier := ifelse(location_id == 102 & clinical_data_type == "inpatient"  & clinical_version_id ==3, 1, is_outlier)] # US inpatient all zeroes
  data[, is_outlier := ifelse(location_name %in% outlier_locations & clinical_version_id ==2, 1, is_outlier)]
  data[, is_outlier := ifelse(nid == 234766, 1, is_outlier)]
  # US HCUP data duplicates the Marketscan data
  data <- subset(data, !(location_id %in% us & grepl("HCUP", field_citation_value)))
}

#Check that outliers have been marked properly
data[, is_outlier := ifelse(nid == 244369 & clinical_version_id ==3, 1, is_outlier)] # Marketscan 2000    
data[is_outlier == 1, unique(location_name)]

data <- data[is_outlier==0,]
data <- data[is_outlier==0 & input_type != "group_review",]
data <- data[!(upper==1 & lower==0)]
data[cases > sample_size, `:=` (cases=NA, sample_size=NA)]
data[, standard_error := ifelse(standard_error >1, 0.99, standard_error)]
data[, age_end := ifelse(age_end >=100, 99, age_end)]
data <- data[mean <1, ]
data <- data[mean !=0, ]


# subset ages, prevalance clinical informatics
if(!is.null(subset_by_age)) {
  if(subset_by_age > 0) {
    data <- data[age_start >= subset_by_age]
  }
}

### Aggregate data ###
if(add_aggregate_subnats){
  data <- aggregate_subnats(data, release_id)
}

# drop values over 1
rw <- nrow(data[mean>=1])
if(rw>0&logit_transform){
  message(paste0("There are ", rw, " row(s) of data with values >= 1. These will be dropped from the data."))
  data <- data[mean<1]
}

# add any necessary covariates for xwalk (ex: sdi, haqi)
if(!is.null(add_cov_to_data)){
  data[, year_id := round((year_start+year_end)/2, 0)]
  gbd_covs <- get_covariate_estimates(release_id=release_id,
                                      covariate_id=add_cov_to_data, 
                                      location_id=unique(data$location_id),
                                      year_id=unique(data$year_id),
                                      sex_id=3,
                                      age_group_id = c(22,27))
  
  if(nrow(gbd_covs)==0){
    stop("No covariate info for both sexes, all ages/age-standardized available. Cannot use sex-specific or age-specific covariates in the crosswalk since bundle data still needs to be sex-split and age-split.")
  }
  
  added_cov_mv_id <- unique(gbd_covs$model_version_id)
  gbd_cov_name <- unique(gbd_covs$covariate_name_short)
  gbd_covs <- gbd_covs[, c('location_id', 'year_id', 'mean_value'), with=F]
  setnames(gbd_covs, 'mean_value', gbd_cov_name)
  
  rw <- nrow(data)
  data <- merge(data, gbd_covs, by=c('location_id', 'year_id'))
  if(nrow(data)!=rw){
    warning(paste0("After merging in covariates, there are now ", nrow(data), " rows of data whereas before there were ", rw, ". Please check!"))
  }
  data[, year_id := NULL]
  
} else {
  added_cov_mv_id <- NULL
}

# add other necessary variables into collector for plotting
var_collector[['added_cov_mv_id']] <- added_cov_mv_id
var_collector[['settings_path']] <- settings_path
var_collector[['storage_folder']] <- storage_folder
var_collector[['save_crosswalk_result_file_name']] <- save_crosswalk_result_file_name
var_collector[['release_id']] <- release_id
var_collector[['model_abbr']] <- model_abbr

## ----------------------------------------------------------------------------------------------------------
percentiles <- quantile(data$mean, probs = c(0.05, 0.10, 0.15, 0.20, 0.40, 0.60, 0.80))
data[mean < 0.001, .(location_name, clinical_data_type, mean, age_start, age_end, clinical_data_type)]

summary_inpatient <- data[clinical_data_type == 'inpatient', .(Min = min(mean), Q1 = quantile(mean, 0.25), Median = median(mean), 
                                                               Mean = mean(mean), Q3 = quantile(mean, 0.75), Max = max(mean))]

summary_claims <- data[clinical_data_type == 'claims', .(Min = min(mean), Q1 = quantile(mean, 0.25), Median = median(mean), 
                                                         Mean = mean(mean), Q3 = quantile(mean, 0.75), Max = max(mean))]

summary_lit <- data[clinical_data_type == '', .(Min = min(mean), Q1 = quantile(mean, 0.25), Median = median(mean), 
                                                Mean = mean(mean), Q3 = quantile(mean, 0.75), Max = max(mean))]

# data <- data[!(clinical_data_type == 'inpatient' & mean < 0.001)]
# data <- data[!(clinical_data_type == 'claims' & mean < 0.001)]

data_to_plot <- copy(data)
data_to_plot[clinical_data_type == "", unique(location_name)]
data_to_plot[clinical_data_type == "claims - flagged", unique(location_name)]
data_to_plot[clinical_data_type == "claims", unique(location_name)]
data_to_plot[field_citation_value %like% "CMS" & clinical_data_type == "claims", unique(location_name)]
data_to_plot[clinical_data_type == "inpatient", unique(location_name)]
data_to_plot[clinical_data_type == "", clinical_data_type := "literature"]
data_to_plot[field_citation_value %like% "CMS", clinical_data_type := "CMS"]
data_to_plot[field_citation_value %like% "HCUP", clinical_data_type := "HCUP"]
data_to_plot[field_citation_value %like% "Truven" | field_citation_value %like% "MarketScan" , clinical_data_type := "MarketScan"]
data_to_plot[clinical_data_type == "CMS", unique(location_name)]
table(data_to_plot$clinical_data_type)

data_to_plot$ihme_loc_id <- NULL
location <- get_location_metadata(location_set_id=35, release_id=release_id)
data_to_plot<- merge(data_to_plot, location[, .(location_id, location_name, ihme_loc_id, super_region_name, region_name, parent_id, location_name_short)], by = c("location_id", "location_name"))
data_to_plot[,age := (age_start + age_end)/2]
data_to_plot[,year := (year_start + year_end)/2]
data_to_plot[,data_year := paste0(clinical_data_type," ",year_start,"-",year_end)]
data_to_plot[,loc_name := paste0(ihme_loc_id,"-",location_name_short)]

##################################################################################
# 3) Run the crosswalk
##################################################################################

crosswalk_holder <- main_mrbrt_crosswalk(
  
  model_abbr = model_abbr,
  data = data,
  storage_folder = storage_folder,
  reference_def = reference_def,
  alternate_defs = alternate_defs,
  xw_measure = xw_measure,
  release_id = release_id,
  logit_transform = logit_transform,
  
  save_crosswalk_result_file = save_crosswalk_result_file,
  bundle_version_id = bundle_version_id,
  
  sex_split_only = sex_split_only,
  
  age_overlap = age_overlap,
  year_overlap = year_overlap,
  age_range = age_range,
  year_range = year_range,
  subnat_to_nat = subnat_to_nat,
  allow_wide_age_bin_match = allow_wide_age_bin_match,
  
  sex_age_overlap = sex_age_overlap,
  sex_year_overlap = sex_year_overlap,
  sex_age_range = sex_age_range,
  sex_year_range = sex_year_range,
  
  spline_covs = spline_covs,
  degree = degree,
  knots = knots,
  knot_placement = knot_placement,
  knot_number = knot_number,
  r_linear = r_linear,
  l_linear = l_linear,
  spline_monotonicity = spline_monotonicity,
  
  trim_pct = trim_pct,
  remove_x_intercept = remove_x_intercept,
  sex_remove_x_intercept = sex_remove_x_intercept,
  addl_x_covs = addl_x_covs,
  sex_covs = sex_covs,
  offset_method = offset_method,
  
  if_save_bundle_version = if_save_bundle_version,
  acause = acause,
  modelable_entity_id = modelable_entity_id,
  model = model,
  bundle_id = bundle_id,
  xw_index = xw_index,
  reference_def_text = reference_def_text,
  clinical_informatics = clinical_informatics,
  subset_by_age = subset_by_age,
  add_aggregate_subnats = add_aggregate_subnats,
  add_cov_to_data = add_cov_to_data,
  added_cov_mv_id = added_cov_mv_id,
  
  test_matches = test_matches,
  plot_output_folder = plot_output_folder,
  test_age_overlap = test_age_overlap,
  test_year_overlap = test_year_overlap,
  test_age_range = test_age_range,
  test_year_range = test_year_range,
  test_subnat_to_nat = test_subnat_to_nat,
  test_allow_wide_age_bin_match = test_allow_wide_age_bin_match
)

### Saving data
###################################################################################

if(class(crosswalk_holder)=='character'){
  
  # stop running the code if the crosswalk matching diagnostics were run
  paste0("Main crosswalk diagnostics will not be generated since full crosswalk was not run and matching diagnostics were generated instead\n\n", crosswalk_holder)
  
} else {
  
  ## save all potentially useful data to local Rdata, for quick recreating xw environment and generating diagnostics
  crosswalk_holder$var_collector <- data.table(t(var_collector))
  
  save(crosswalk_holder, file = paste0(storage_folder, model_abbr , "_crosswalk_holder.Rdata"))
  message(paste0("\nAll results saved to ", storage_folder, model_abbr , "_crosswalk_holder.Rdata\nPython objects that were saved during crosswalking can also be found in the folder\n"))
  
  ##################################################################################
  # 4) Run diagnostics
  ##################################################################################
  
  # if crosswalk_holder is not currently in the environment, re-load it in
  if(!exists('crosswalk_holder')){
    load(paste0(storage_folder, model_abbr , "_crosswalk_holder.Rdata"))
  }
  
  # remove .Rhistory from vetting folder to avoid error if someone else saved their own .Rhistory here when code was run
  if (file.exists(paste0(plot_source_folder, '.Rhistory'))){
    file.remove(paste0(plot_source_folder, '.Rhistory'))
  }
  
  # call rmarkdown for diagnostics
  rmarkdown::render(
    input = paste0(plot_source_folder, 'vetting_report.Rmd'),
    output_file = paste0(plot_output_folder, model_abbr, "_", date, ".html"),
    clean = TRUE,
    
    params = list(storage_folder = storage_folder, model_abbr = model_abbr, 
                  crosswalk_holder = crosswalk_holder)
  )
  
}


#age specific data cavd
cavd_xwalk_prev<- read.xlsx("/FILEPATH/FILENAME.xlsx")
data_1 <- subset(cavd_xwalk_prev, cavd_xwalk_prev$age_end-cavd_xwalk_prev$age_start<=25)
data_1$standard_error<- ifelse(data_1$standard_error>1, 0.99, data_1$standard_error)

write.xlsx(data_1, "/FILEPATH/FILENAME.xlsx", sheetName="extraction")
save_crosswalk_version(bundle_version_id= 45918,  "/FILEPATH/FILENAME.xlsx", description="DATE; all age split data for CAVD; GBD 2023.")

cavd_bundle <- get_bundle_version(45918)
cavd_xwalk_prev<- read.xlsx("/FILEPATH/FILENAME.xlsx")

xwalk_prev2<- subset(cavd_xwalk_prev, cavd_xwalk_prev$measure=='prevalence')


xwalk_prev_as_803197 <- age_split(df = xwalk_prev2,
                                  model_id = 16601,
                                  model_version_id = 814826,
                                  release_id = 16,
                                  measure = 'prevalence',
                                  global_age_pattern = TRUE,
                                  drop_under_1_case = F)
xwalk_prev_as_803197$midage <- (xwalk_prev_as_803197$age_start +xwalk_prev_as_803197$age_end)/2

ggplot(xwalk_prev_as_803197, aes(x=midage, y=mean)) + 
  geom_point(size = 5)+ 
  geom_errorbarh(aes(xmin=age_start, xmax=age_end))


allcavddata_notprev <- subset(cavd_bundle , cavd_bundle$measure !="prevalence")
allcavddata_notprev_sex_spec <- subset(allcavddata_notprev, allcavddata_notprev$sex != "Both")
allcavddata_notprev_sex_both <- subset(allcavddata_notprev, allcavddata_notprev$sex == "Both")

allcavddata_notprev_sex_male <-allcavddata_notprev_sex_both
allcavddata_notprev_sex_male$sex <- ifelse(allcavddata_notprev_sex_male$sex=="Both", "Male", allcavddata_notprev_sex_male$sex)

allcavddata_notprev_sex_female <-allcavddata_notprev_sex_both
allcavddata_notprev_sex_female$sex <- ifelse(allcavddata_notprev_sex_female$sex=="Both", "Male", allcavddata_notprev_sex_female$sex)

allcavddata_notprev_male_female <- rbind(allcavddata_notprev_sex_male,allcavddata_notprev_sex_female)
allcavddata_notprev_male_female$crosswalk_parent_seq <- allcavddata_notprev_male_female$seq
allcavddata_notprev_male_female$seq <- ''

allcavddata_notprev_2 <- rbind(allcavddata_notprev_male_female,allcavddata_notprev_sex_spec, fill=TRUE)

allcavddata_prev <- xwalk_prev_as_803197


data <- rbind(allcavddata_prev, allcavddata_notprev_2, fill=TRUE)

location <- get_location_metadata(location_set_id=35, release_id=release_id)
us <- unlist(unique(location[parent_id==102,c("location_id")]), use.names=F)
mex <- unlist(unique(location[parent_id==130,c("location_id")]), use.names=F)
brazil <- unlist(unique(location[parent_id==135,c("location_id")]), use.names=F)
china <- unlist(unique(location[parent_id==6,c("location_id")]), use.names=F)
data[, is_outlier := ifelse(location_id %in% mex & clinical_version_id == 3, 1, is_outlier)]
data[, is_outlier := ifelse(location_id %in% brazil & clinical_version_id == 3, 1, is_outlier)]
data[, is_outlier := ifelse(clinical_data_type == "claims - flagged" & clinical_version_id == 3, 1, is_outlier)] # Mongolia and Rep of Korea
outlier_locations <- c('Belgium', 'Cyprus', 'Malta', 'Portugal', 'Argentina', 'Chile', 'Ecuador', 'Tibet', 
                       'Armenia', 'Jordan', 'Iran (Islamic Republic of)', 'Meghalaya', 'Karnataka', 'Nepal', 'Kenya', 'Botswana', 'Fujian', 
                       'Philippines', 'Georgia', 'Croatia', 'Spain', 'Czechia', 
                       'Serbia','Türkiye')

data[, is_outlier := ifelse(location_name %in% outlier_locations & clinical_version_id ==3, 1, is_outlier)]
data[, is_outlier := ifelse(nid %in% c(397812, 397813, 397814, 431674, 466458, 466459, 509155), 1, is_outlier)] # outliering Poland claims
# data[, is_outlier := ifelse(nid %in% c(234764, 234762, 234765, 292577), 1, is_outlier)] # outliering uk inpatient
# data[, is_outlier := ifelse(nid %in% c(336851, 336852), 1, is_outlier)] # outliering japan inpatient
# data[, is_outlier := ifelse(nid %in% c(334464, 334465, 439738, 474095), 1, is_outlier)] # outliering Italy inpatient
data[, is_outlier := ifelse(nid  %in% c(234766, 234769, 234771), 1, is_outlier)] #Hospital Discharge Survey 1988-1992.


allcavddata_1 <- data

allcavddata_1$bundle_id <- 2987
allcavddata_1$bundle_name <-'Calcific aortic valve disease'
allcavddata_1$response_rate <-''
allcavddata_1 <- allcavddata_1[,c("age_start", "sex", "location_id", "seq", "input_type", "underlying_nid", "nid", "source_type", "bundle_id", "bundle_name", "location_name",
                                "year_start", "year_end", "age_end", "age_demographer", "measure", "standard_error", "cases", "effective_sample_size", "sample_size", 
                                "unit_type", "unit_value_as_published", "uncertainty_type", "uncertainty_type_value", "representative_name", "urbanicity_type", "crosswalk_parent_seq",
                                "recall_type", "recall_type_value", "sampling_type", "response_rate", "design_effect", "extractor", "is_outlier", "mean", "lower", "upper")]

#all421data_2$standard_error<- ifelse(all421data_2$standard_error>1, 0.99, all421data_2$standard_error)
#all421data_2$seq <- ifelse(grepl("sex",all421data_2$note_modeler),NA, all421data_2$seq)

df <- allcavddata_1[measure!='cfr']
df$standard_error<- ifelse(df$standard_error>1, 0.99, df$standard_error)
#df$is_outlier <- ifelse(df$year_start>2019,1, df$is_outlier)


write.xlsx(df,  "/FILEPATH/FILENAME.xlsx", sheetName="extraction")

save_crosswalk_version(bundle_version_id= 45918, "/FILENAME/FILEPATH.xlsx", description="DATE; all data for CAVD; Age and Sex Split")

xw42482 <- get_crosswalk_version(44028)

#age specific data dmvd
dmvd_xwalk_prev<- read.xlsx("FILEPATH/FILENAME.xlsx")
data_1 <- subset(dmvd_xwalk_prev, dmvd_xwalk_prev$age_end-dmvd_xwalk_prev$age_start<=25)
data_1$standard_error<- ifelse(data_1$standard_error>1, 0.99, data_1$standard_error)


write.xlsx(data_1, "/FILENAME/FILEPATH.xlsx", sheetName="extraction")
save_crosswalk_version(bundle_version_id= 45919,  "/FILEPATH/FILENAME.xlsx", description="DATE; all age split data for DMVD; GBD 2023.")

dmvd_bundle <- get_bundle_version(45919)
dmvd_xwalk_prev<- read.xlsx("/FILEPATH/FILENAME.xlsx")

xwalk_prev2<- subset(dmvd_xwalk_prev, dmvd_xwalk_prev$measure=='prevalence')

xwalk_prev_as_803199 <- age_split(df = xwalk_prev2,
                                  model_id = 16602,
                                  model_version_id = 814829,
                                  release_id = 16,
                                  measure = 'prevalence',
                                  global_age_pattern = TRUE,
                                  drop_under_1_case = F)
xwalk_prev_as_803199$midage <- (xwalk_prev_as_803199$age_start +xwalk_prev_as_803199$age_end)/2

ggplot(xwalk_prev_as_803199, aes(x=midage, y=mean)) + 
  geom_point(size = 5)+ 
  geom_errorbarh(aes(xmin=age_start, xmax=age_end))


alldmvddata_notprev <- subset(dmvd_bundle , dmvd_bundle$measure !="prevalence")
alldmvddata_notprev_sex_spec <- subset(alldmvddata_notprev, alldmvddata_notprev$sex != "Both")
alldmvddata_notprev_sex_both <- subset(alldmvddata_notprev, alldmvddata_notprev$sex == "Both")

alldmvddata_notprev_sex_male <-alldmvddata_notprev_sex_both
alldmvddata_notprev_sex_male$sex <- ifelse(alldmvddata_notprev_sex_male$sex=="Both", "Male", alldmvddata_notprev_sex_male$sex)

alldmvddata_notprev_sex_female <-alldmvddata_notprev_sex_both
alldmvddata_notprev_sex_female$sex <- ifelse(alldmvddata_notprev_sex_female$sex=="Both", "Male", alldmvddata_notprev_sex_female$sex)

alldmvddata_notprev_male_female <- rbind(alldmvddata_notprev_sex_male,alldmvddata_notprev_sex_female)
alldmvddata_notprev_male_female$crosswalk_parent_seq <- alldmvddata_notprev_male_female$seq
alldmvddata_notprev_male_female$seq <- ''

alldmvddata_notprev_2 <- rbind(alldmvddata_notprev_male_female,alldmvddata_notprev_sex_spec, fill=TRUE)

alldmvddata_prev <- xwalk_prev_as_803199


data <- rbind(alldmvddata_prev, alldmvddata_notprev_2, fill=TRUE)

location <- get_location_metadata(location_set_id=35, release_id=release_id)
us <- unlist(unique(location[parent_id==102,c("location_id")]), use.names=F)
mex <- unlist(unique(location[parent_id==130,c("location_id")]), use.names=F)
brazil <- unlist(unique(location[parent_id==135,c("location_id")]), use.names=F)
china <- unlist(unique(location[parent_id==6,c("location_id")]), use.names=F)
data[, is_outlier := ifelse(location_id %in% mex & clinical_version_id == 3, 1, is_outlier)]
data[, is_outlier := ifelse(location_id %in% brazil & clinical_version_id == 3, 1, is_outlier)]
data[, is_outlier := ifelse(clinical_data_type == "claims - flagged" & clinical_version_id == 3, 1, is_outlier)] # Mongolia and Rep of Korea
outlier_locations <- c('Belgium', 'Cyprus', 'Malta', 'Portugal', 'Argentina', 'Chile', 'Ecuador', 'Tibet', 
                       'Armenia', 'Jordan', 'Iran (Islamic Republic of)', 'Meghalaya', 'Karnataka', 'Nepal', 'Kenya', 'Botswana', 'Fujian', 
                       'Philippines', 'Georgia', 'Croatia', 'Spain', 'Czechia', 
                       'Serbia','Türkiye')

data[, is_outlier := ifelse(location_name %in% outlier_locations & clinical_version_id ==3, 1, is_outlier)]
data[, is_outlier := ifelse(nid %in% c(397812, 397813, 397814, 431674, 466458, 466459, 509155), 1, is_outlier)] # outliering Poland claims
# data[, is_outlier := ifelse(nid %in% c(234764, 234762, 234765, 292577), 1, is_outlier)] # outliering uk inpatient
# data[, is_outlier := ifelse(nid %in% c(336851, 336852), 1, is_outlier)] # outliering japan inpatient
# data[, is_outlier := ifelse(nid %in% c(334464, 334465, 439738, 474095), 1, is_outlier)] # outliering Italy inpatient
data[, is_outlier := ifelse(nid  %in% c(234766, 234769, 234771), 1, is_outlier)] #Hospital Discharge Survey 1988-1992.


alldmvddata_1 <- data

alldmvddata_1$bundle_id <- 2993
alldmvddata_1$bundle_name <-'Degenerative mitral valve disease'
alldmvddata_1$response_rate <-''
alldmvddata_1 <- alldmvddata_1[,c("age_start", "sex", "location_id", "seq", "input_type", "underlying_nid", "nid", "source_type", "bundle_id", "bundle_name", "location_name",
                                  "year_start", "year_end", "age_end", "age_demographer", "measure", "standard_error", "cases", "effective_sample_size", "sample_size", 
                                  "unit_type", "unit_value_as_published", "uncertainty_type", "uncertainty_type_value", "representative_name", "urbanicity_type", "crosswalk_parent_seq",
                                  "recall_type", "recall_type_value", "sampling_type", "response_rate", "design_effect", "extractor", "is_outlier", "mean", "lower", "upper")]

#all421data_2$standard_error<- ifelse(all421data_2$standard_error>1, 0.99, all421data_2$standard_error)
#all421data_2$seq <- ifelse(grepl("sex",all421data_2$note_modeler),NA, all421data_2$seq)

df <- alldmvddata_1[measure!='cfr']
df$standard_error<- ifelse(df$standard_error>1, 0.99, df$standard_error)
#df$is_outlier <- ifelse(df$year_start>2019,1, df$is_outlier)
df$is_outlier <- ifelse(is.na(df$is_outlier), 0, df$is_outlier)


write.xlsx(df,  "/FILEPATH/FILENAME.xlsx", sheetName="extraction")

save_crosswalk_version(bundle_version_id= 45919, "/FILEPATH/FILENAME.xlsx", description="DATE; all data for dmvd; Age and Sex Split")
xw42484 <- get_crosswalk_version(44029)

