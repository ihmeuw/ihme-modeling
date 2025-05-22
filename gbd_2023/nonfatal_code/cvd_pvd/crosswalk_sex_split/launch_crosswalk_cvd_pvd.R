
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

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
} else {
  j<- "FILEPATH"
}

date <- gsub("-", "_", Sys.Date())

"%ni%" <- Negate("%in%")

"%contain%" <- function(values,x) {
  tx <- table(x)
  tv <- table(values)
  z <- tv[names(tx)] - tx
  all(z >= 0 & !is.na(z))
}

pacman::p_load(rmarkdown, data.table, ggplot2, openxlsx)
library(reticulate) 
reticulate::use_python("FILEPATH/bin/python")

source('FILEPATH/main_mrbrt_crosswalk_func.R')
source("FILEPATH/aggregate_subnats.R")

source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_covariate_estimates.R")


##################################################################################
# 1) Set arguments and filepaths, load in crosswalk settings
##################################################################################

### Arguments
##################################################################################

args <- commandArgs(trailingOnly = T)
if(length(args)==0){
  ######################################################
  # enter arguments here if running code interactively #
  ######################################################
  acause <- 'cvd_pvd'
  bundle_id <- 345
  xw_measure <- 'prevalence'
  xw_index <- 58
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

### Create variables
##################################################################################

model_abbr <- paste0(acause, "_", xw_measure, "_", bundle_id, "_index", xw_index)

### Paths
##################################################################################

## path for settings sheets
settings_path <- paste0('FILEPATH/', 'cvd_pvd', '.xlsx') 

## path for storage folder
storage_folder <- paste0('FILEPATH/', acause, '/')

# create storage folder if it doesn't exist
dir.create(storage_folder, showWarnings=F)

## path for save crosswalk version
save_crosswalk_result_file_name <- paste0("xw_results_", date , "_", model_abbr, ".xlsx")
save_crosswalk_result_file <- paste0(storage_folder, save_crosswalk_result_file_name)

## path for diagnostics
plot_source_folder <- 'FILEPATH'
plot_output_folder <- paste0('FILEPATH/', acause, '/')

# create folder for diagnostics if it doesn't exist
dir.create(plot_output_folder, showWarnings=F)

### Extracting and formatting all settings variables
##################################################################################

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

## inpatient only
# if(clinical_informatics == 3) {
#   data[, cv_alternate := ifelse(clinical_data_type=="inpatient", 1, 0)]
# }
# 
# # ## claims only
# if(clinical_informatics == 2) {
#   data[, cv_alternate := ifelse(clinical_data_type=="claims", 1, 0)]
# }
# 
# ## all clinical informatics
# if(clinical_informatics == 1) {
#   data[, cv_alternate := ifelse(clinical_data_type!="", 1, 0)]
#   
## select all US states and drop US States with HCUP data
location <- get_location_metadata(location_set_id=35, release_id=release_id)
us <- unlist(unique(location[parent_id==102,c("location_id")]), use.names=F)

# US HCUP data duplicates the Marketscan data
# data <- subset(data, !(location_id %in% us & grepl("HCUP", field_citation_value)))

# crosswalk claims only and removing CMS
# data[acause == "cvd_pvd", 
#    `:=` (cv_claims = ifelse(clinical_data_type == "claims" & !field_citation_value %like% "CMS", 1, 0),
#          cv_cms = ifelse(field_citation_value %like% "CMS", 1, 0))]
#     data <- subset(data, cv_cms != 1)

# crosswalk CMS only and removing claims
# data[acause == "cvd_pvd" & clinical_informatics == 2 & xw_index == 5, 
#      `:=` (cv_claims = ifelse(clinical_data_type == "claims" & !field_citation_value %like% "CMS", 1, 0),
#            cv_cms = ifelse(field_citation_value %like% "CMS", 1, 0))]
#     data <- subset(data, cv_claims != 1)

# crosswalk network claims and CMS
data[acause == "cvd_pvd",
     `:=` (cv_claims = ifelse(clinical_data_type == "claims" & !field_citation_value %like% "CMS", 1, 0),
           cv_cms = ifelse(field_citation_value %like% "CMS", 1, 0))]
data[, cv_claims := ifelse(clinical_data_type == "claims - flagged ", 1, cv_claims)]
outlier_poland  <- c(431674,397812,397813,397814) 
data[nid %in% outlier_poland, is_outlier:=1]    

#outlier claims flagged, bad marketscan, and covid years
data$is_outlier <- ifelse(data$clinical_data_type == "claims - flagged",1, data$is_outlier)
data$is_outlier <- ifelse(data$nid == 244369 ,1, data$is_outlier)
#data$is_outlier <- ifelse(data$year_start>2019,1, data$is_outlier)

data <- data[is_outlier==0 & input_type != "group_review",]
data <- data[!(upper==1 & lower==0)]
data[cases > sample_size, `:=` (cases=NA, sample_size=NA)]
data[, standard_error := ifelse(standard_error >1, 0.99, standard_error)]
data[, age_end := ifelse(age_end >=100, 99, age_end)]
#data <- data[mean <1, ]

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
  knots = knots,
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

xwalk2023 <- openxlsx::read.xlsx("FILEPATH/xw_results_2024_05_24_cvd_pvd_prevalence_345_index56.xlsx")
xwalk2023_1 <- subset(xwalk2023, xwalk2023$age_end-xwalk2023$age_start<=25)
xwalk2023_1$standard_error<- ifelse(xwalk2023_1$standard_error>1, 0.99, xwalk2023_1$standard_error)



write.xlsx(xwalk2023_1,  "FILEPATH/xw_results_2024_03_01_cvd_pvd_prevalence_345_index56.xlsx", sheetName="extraction")
save_crosswalk_version(bundle_version_id= 43501, "FILEPATH/xw_results_2024_03_01_cvd_pvd_prevalence_345_index56.xlsx", description="03/01/24; all age split data for 345; GBD 2023.")
cwas40406 <- get_crosswalk_version(43629)


#age splitting

model_version_id <-814544
model_id <- 2532
global   <- F

source('FILEPATH/age_split.R')

xwalk_prev<- openxlsx::read.xlsx("FILEPATH/xw_results_2024_07_08_cvd_pvd_prevalence_345_index58.xlsx")
xwalk_prev2<- data.table(subset(xwalk_prev, xwalk_prev$measure=='prevalence'))

## Remove wide age bin 40-99 for Col, already have age-specific data. 
xwalk_prev2 <- xwalk_prev2[-which(nid==322188 & age_start==40 & age_end==99)]

## Sources we will not be age-splitting removed (Congo, Congo/CAR, Telegana Urban)
xwalk_prev2_no_as <- xwalk_prev2[nid %in% c(140179,553924, 553943)]

## Sources we will be age-splitting
xwalk_prev2_need_as <- xwalk_prev2[nid %ni% c(140179,553924, 553943)]

## for all 
xwalk_prev_as_803083_10 <- age_split(df = xwalk_prev2_need_as[which(nid %ni% c(140190,553881,553883))], 
                                     model_id = 2532,
                                     model_version_id = 814544,
                                     release_id = 16,
                                     measure = 'prevalence',
                                     global_age_pattern = TRUE,
                                     drop_under_1_case = F,
                                     gbd_age_groups=F,
                                     age_bin_size = 10)


## for sources where 10 won't work, use 25. 
xwalk_prev_as_803083_25 <- age_split(df = xwalk_prev2_need_as[which(nid %in% c(140190,553881,553883))], 
                                     model_id = 2532,
                                     model_version_id = 814544,
                                     release_id = 16,
                                     measure = 'prevalence',
                                     global_age_pattern = TRUE,
                                     drop_under_1_case = F,
                                     gbd_age_groups=F,
                                     age_bin_size = 25)

xwalk_prev_as_803083 <- rbind(xwalk_prev2_no_as, xwalk_prev_as_803083_10,xwalk_prev_as_803083_25)


xwalk_prev_as_803083$midage <- (xwalk_prev_as_803083$age_start +xwalk_prev_as_803083$age_end)/2

ggplot(xwalk_prev_as_803083, aes(x=midage, y=mean)) + 
  geom_point(size = 5)+ 
  geom_errorbarh(aes(xmin=age_start, xmax=age_end))


#pull non prevalence data
all121data_cw2 <- get_bundle_version(45536)


all121data_notprev <- subset(all121data_cw2 , all121data_cw2$measure !="prevalence")
all121data_notprev_sex_spec <- subset(all121data_notprev, all121data_notprev$sex != "Both")
all121data_notprev_sex_both <- subset(all121data_notprev, all121data_notprev$sex == "Both")

all121data_notprev_sex_male <-all121data_notprev_sex_both
all121data_notprev_sex_male$sex <- ifelse(all121data_notprev_sex_male$sex=="Both", "Male", all121data_notprev_sex_male$sex)

all121data_notprev_sex_female <-all121data_notprev_sex_both
all121data_notprev_sex_female$sex <- ifelse(all121data_notprev_sex_female$sex=="Both", "Male", all121data_notprev_sex_female$sex)

all121data_notprev_male_female <- rbind(all121data_notprev_sex_male,all121data_notprev_sex_female)
all121data_notprev_male_female$crosswalk_parent_seq <- all121data_notprev_male_female$seq
all121data_notprev_male_female$seq <- ''


all121data_notprev_2 <- rbind(all121data_notprev_male_female, all121data_notprev_sex_spec, fill=TRUE)

all121data_prev <- xwalk_prev_as_803083[location_name != 'Nepal'] ## age_end

#double check that marketscan 2000 gets outliered 
all121data_prev$is_outlier <- ifelse(all121data_prev$nid == 244369 ,1, all121data_prev$is_outlier)

all121data_1 <- rbind(all121data_prev, all121data_notprev_2, fill=TRUE)

#columns necessary for upload. 
all121data_1$bundle_id <- 121
all121data_1$bundle_name <-'Peripheral arterial disease'
all121data_1$response_rate <-''
all121data_1 <- all121data_1[,c("age_start", "sex", "location_id", "seq", "input_type", "underlying_nid", "nid", "source_type", "bundle_id", "bundle_name", "location_name",
                                "year_start", "year_end", "age_end", "age_demographer", "measure", "standard_error", "cases", "effective_sample_size", "sample_size", 
                                "unit_type", "unit_value_as_published", "uncertainty_type", "uncertainty_type_value", "representative_name", "urbanicity_type", "crosswalk_parent_seq",
                                "recall_type", "recall_type_value", "sampling_type", "response_rate", "design_effect", "extractor", "is_outlier", "mean", "lower", "upper")]

df <- all121data_1[measure!='cfr']
df$standard_error<- ifelse(df$standard_error>1, 0.99, df$standard_error)

## last removals
df <- df[-which(nid==466458)] #poland claims
df <- df[-which(nid==336203)] #taiwan claims
df <- df[-which(nid==553881 & age_start >= 75)] ## Poland data oldest ages issue

openxlsx::write.xlsx(df,  "FILEPATH/xw_for_121_alldata_08162024_10_and_25_fix_age_split_SSA.xlsx", sheetName="extraction")

save_crosswalk_version(bundle_version_id= 45536, "FILEPATH/xw_for_121_alldata_08162024_10_and_25_fix_age_split_SSA.xlsx", 
                       description="08/16/24; all data for PAD; Age and Sex Split; Age split with 10 or 25 when necessary; No age split for SSA; Rm Pol oldest ages; outlier marketscan2000 POL TWN")



