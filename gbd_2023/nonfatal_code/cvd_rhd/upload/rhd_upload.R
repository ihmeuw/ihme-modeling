########################################################
# Date: DATE
# Purpose: RHD bundle data splitting, uploading, crosswalking, etc. 
########################################################
date <- gsub("-", "_", Sys.Date())

###### Libraries ######
packages <- c("data.table", "R.utils", "raster", "data.table", "ggplot2", "rgdal", "sf", "stringr", "tidyverse",
              "cowplot", "scales", "ggrepel", "kableExtra", "dplyr", "gridExtra", "formattable", "extrafont", "data.table", "magrittr")
suppressMessages(invisible(lapply(packages, library, character.only = TRUE)))

# Load central libraries
suppressMessages(sourceDirectory("FILEPATH", modifiedOnly=FALSE))

#pull BV for 113
rhd.all3 <-get_bundle_data(bundle_id=113)

#check for duplicate data
duplicated_check <- rhd.all3 %>%
  dplyr::group_by(nid,location_id,location_name,sex,year_start, year_end,age_start,age_end,measure, mean, lower, upper) %>%
  dplyr::summarise(freq = n())
dup3 <- subset(duplicated_check, duplicated_check$freq>1)

#no duplicates? Move on to saving BV with clinical & inpatient data
#save new bundle version without duplicates
savedbundle_nodup <- save_bundle_version(113, include_clinical = c('claims','inpatient'))

#cwversion <- get_version_quota(bundle_id = 421)

bv <- get_bundle_version(45799)


####
#Now we need to split the data by endemic/non endemic and save them into the appropriate bundle (420= endemic, 421= non-endemic)
#we are pulling in the endemicity from the step 1 code

###### Paths for endemicity######
outdir <- "FILEPATH"
year <- 2023

##pca is raw endemicity index values
#pca <- read.csv(paste0(outdir, 'Endemic_pca_final.csv'))
end <- read.csv(paste0(outdir, "endemicity", year ,".csv"))

end2 <- end[,c(2:3)]

#merge the saved 113 BV with endemicity
bv<- merge(bv, end2, by="location_id", all.x=TRUE)


bv_end <- subset(bv, bv$endemic_2023==1)
bv_nonend <- subset(bv, bv$endemic_2023==0)

#columns necessary for upload. 
bv_end$bundle_id <- 420
bv_end$bundle_name <-'Rheumatic Heart Disease'
bv_end2 <- bv_end[,c("age_start", "sex", "location_id", "seq", "input_type", "underlying_nid", "nid", "source_type", "bundle_id", "bundle_name", "location_name",
                     "year_start", "year_end", "age_end", "age_demographer", "measure", "standard_error", "cases", "effective_sample_size", "sample_size", 
                     "unit_type", "unit_value_as_published", "uncertainty_type", "uncertainty_type_value", "representative_name", "urbanicity_type",
                     "recall_type", "recall_type_value", "sampling_type", "response_rate", "design_effect", "extractor", "is_outlier", "mean", "lower", "upper")]
bv_nonend$bundle_id <- 421
bv_nonend$bundle_name <-'Rheumatic Heart Disease'
bv_nonend2 <- bv_nonend[,c("age_start", "sex", "location_id", "seq", "input_type", "underlying_nid", "nid", "source_type", "bundle_id", "bundle_name", "location_name",
                           "year_start", "year_end", "age_end", "age_demographer", "measure", "standard_error", "cases", "effective_sample_size", "sample_size", 
                           "unit_type", "unit_value_as_published", "uncertainty_type", "uncertainty_type_value", "representative_name", "urbanicity_type",
                           "recall_type", "recall_type_value", "sampling_type", "response_rate", "design_effect", "extractor", "is_outlier", "mean", "lower", "upper")]



#save bundle versions of 420 and 421  to ensure no data loss (backup) 
#savedbundle420 <- save_bundle_version(420)
savedbundle420 <- save_bundle_version(420)
bv_420 <- get_bundle_version(45678) #previous 42293

savedbundle421 <- save_bundle_version(421)
bv_421 <- get_bundle_version(45679) #previous 42420

#Wipe 420
acause <- "cvd_rhd"
rhd420_wipe <- bv_420[,'seq']
descript <- 'rhd_420wipe'
bundle_id <- 420
acause    <- 'cvd_rhd'
filename.wipe <- paste0("FILEPATH/FILENAME", ".xlsx")

write.xlsx(rhd420_wipe, filename.wipe, sheetName="extraction", na="")	
upload_bundle_data(bundle_id=bundle_id, filepath=filename.wipe)
rhd420recheck<-get_bundle_data(bundle_id=420)

#Wipe 421
rhd421_wipe <- bv_421[,'seq']
descript <- 'rhd_421wipe'
bundle_id <- 421
acause    <- 'cvd_rhd'
filename.wipe <- paste0("FILEPATH/FILENAME", ".xlsx")

write.xlsx(rhd421_wipe, filename.wipe, sheetName="extraction", na="")	
upload_bundle_data(bundle_id=bundle_id, filepath=filename.wipe)
rhd421recheck<-get_bundle_data(bundle_id=421)

#save data from 113 for 420
acause <- "cvd_rhd"
rhd420 <- bv_end2
descript <- 'rhd_420_from_113'
bundle_id <- 420
acause    <- 'cvd_rhd'
filename.420 <- paste0("FILEPATH/FILENAME", ".xlsx")

rhd420$standard_error<- ifelse(rhd420$standard_error>1, 0.99, rhd420$standard_error)

write.xlsx(rhd420, filename.420, sheetName="extraction", na="")	
upload_bundle_data(bundle_id=bundle_id, filepath=filename.420)
rhd420recheck<-get_bundle_data(bundle_id=420)
savedbundle <- save_bundle_version(420) #45806

#save data from 113 for 421
acause <- "cvd_rhd"
rhd421 <- bv_nonend2
descript <- 'rhd_421_from_113'
bundle_id <- 421
acause    <- 'cvd_rhd'
filename.421 <- paste0("FILEPATH/FILENAME", ".xlsx")

rhd421$standard_error<- ifelse(rhd421$standard_error>1, 0.99, rhd421$standard_error)
rhd421$upper<- ifelse(rhd421$upper==0, 0.00000001, rhd421$upper)

write.xlsx(rhd421, filename.421, sheetName="extraction", na="")	
upload_bundle_data(bundle_id=bundle_id, filepath=filename.421)
rhd421recheck<-get_bundle_data(bundle_id=421)
savedbundle <- save_bundle_version(421) #45683


############NEXT IS SEX AND AGE SPLITTING #########
#we do age and sex splitting for both 420/421. We use a compartmental model for age splitting for both. 
#this means i split prev/inc/rem for 420 together, and inc/prev for 421 together
#Here are the next steps:
#1. Use the crosswalking code below to sex split integrands that need it
#2. Save out age specific (age bin <25) data to file and run compartmental RHD models (endemic and/or non-endemic)
#3. Merge the age/sex specific data all back together 
#4. Complete any outliering necessary
#5. Save crosswalk versions for final modeling.
###################################################


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

source('/FILEPATH/main_mrbrt_crosswalk_func.R')
source("/FILEPATH/aggregate_subnats.R")
source("/FILEPATH/get_crosswalk_version.R")
source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/get_covariate_estimates.R")


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
  acause <- 'cvd_rhd'
  bundle_id <- 420
  xw_measure <- 'prevalence'
  xw_index <- 6
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
settings_path <- paste0('/FILEPATH/', acause, '.xlsx')

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

data <- get_bundle_version(bundle_version_id = 45806)

data <- data[measure == 'prevalence'| measure =='incidence'| measure =='remission']

## inpatient only
if(clinical_informatics == 3) {
  data[, cv_alternate := ifelse(clinical_data_type=="inpatient", 1, 0)]
}

## claims only
if(clinical_informatics == 2) {
  data[, cv_alternate := ifelse(clinical_data_type=="claims", 1, 0)]
}

## all clinical informatics
if(clinical_informatics == 1) {
  data[, cv_alternate := ifelse(clinical_data_type!="", 1, 0)]
  
  ## select all US states and drop US States with HCUP data
  location <- get_location_metadata(location_set_id=35, release_id=9)
  us <- unlist(unique(location[parent_id==102,c("location_id")]), use.names=F)
  
  # US HCUP data duplicates the Marketscan data
  data <- subset(data, !(location_id %in% us & grepl("HCUP", field_citation_value)))
  
  ## drop data from non-representative source
  data <- subset(data, !(location_name %in% c("Jordan", "Iran", "Meghalaya", "Karnataka", "Nepal", "Kenya")))
  
}

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
#############################################################################
# Now that you've sex split its time to create age specific files 


#read in your sex split data for endemic locations (420)
xwalk_prev<- read.xlsx("/FILEPATH/FILENAME.xlsx")

#remove all clinical data ensuring there is only literature data in here 
xwalk_prev<- subset(xwalk_prev, xwalk_prev$source_type!= 'Facility - inpatient')
xwalk_prev<- subset(xwalk_prev, xwalk_prev$source_type!= 'Facility - other/unknown')
#pulling only prev, inc, and remission
data <- subset(xwalk_prev, xwalk_prev$measure == 'prevalence'|xwalk_prev$measure == 'incidence'|xwalk_prev$measure == 'remission')

#subset only age specific data, save to crosswalk for age specific compart model
data_1 <- subset(data, data$age_end-data$age_start<=25)
write.xlsx(data_1,  "/FILEPATH/FILENAME.xlsx", sheetName="extraction")
save_crosswalk_version(bundle_version_id= 43493, "FILEPATH/FILENAME.xlsx", description="DATE; all age split data for 420; GBD 2023.")

#There are no hospital records from the literature review that are in the 421 data. We keep in inpatient data 
xwalk421 <- read.xlsx("/FILEPATH/FILENAME.xlsx")
xwalk421_1 <- subset(xwalk421, xwalk421$age_end-xwalk421$age_start<=25)
write.xlsx(xwalk421_1,  "/FILEPATH/FILENAME.xlsx", sheetName="extraction")
save_crosswalk_version(bundle_version_id= 43494, "/FILEPATH/FILENAME.xlsx", description="DATE; all age split data for 421; GBD 2023.")
cwas40406 <- get_crosswalk_version(40406)


#age splitting code for endemic 
model_version_id <- 803016
model_id <- 3075
global   <- F
source('/FILEPATH/age_split.R')
xwalk_prev<- read.xlsx("/FILEPATH/FILENAME.xlsx")
xwalk_prev<- subset(xwalk_prev, xwalk_prev$source_type!= 'Facility - inpatient')
xwalk_prev<- subset(xwalk_prev, xwalk_prev$source_type!= 'Facility - other/unknown')
xwalk_prev<- subset(xwalk_prev, xwalk_prev$measure=='prevalence')

xwalk_prev_as_770947 <- age_split(df = xwalk_prev,
                                  model_id = model_id,
                                  model_version_id = model_version_id,
                                  release_id = 16,
                                  measure = 'prevalence',
                                  global_age_pattern = global,
                                  drop_under_1_case = F)
xwalk_prev_as_770947$midage <- (xwalk_prev_as_770947$age_start +xwalk_prev_as_770947$age_end)/2

ggplot(xwalk_prev_as_770947, aes(x=midage, y=mean)) + 
  geom_point(size = 5)+ 
  geom_errorbarh(aes(xmin=age_start, xmax=age_end))


##Create sex/age specific dataset for 420
all420data <- get_bundle_version(bundle_version_id = 45680 )
all420data <- subset(all420data , all420data$source_type!= 'Facility - inpatient')
all420data <- subset(all420data , all420data$source_type!= 'Facility - other/unknown')
all420data_csmr <- subset(all420data, all420data$measure =='mtspecific')

#if you ran xwing model used commented out code.
#all420data_cw <- get_crosswalk_version(42272)
#all420data_cw_noprev <- subset(all420data_cw, all420data_cw$measure !='prevalence')

all420data_cw_noprev <- read.xlsx("/FILEPATH/FILENAME.xlsx")
all420data_cw_noprev <- subset(all420data_cw_noprev, all420data_cw_noprev$measure !='prevalence')

all420data_prev <- xwalk_prev_as_770947

all420data_1 <- rbind(all420data_csmr, all420data_cw_noprev, fill=TRUE)
all420data_2 <- rbind(all420data_1, all420data_prev, fill=TRUE)

all420data_2$standard_error<- ifelse(all420data_2$standard_error>1, 0.99, all420data_2$standard_error)
all420data_2$seq <- ifelse(grepl("sex",all420data_2$note_modeler),NA, all420data_2$seq)

#outliers
#all420data_2$is_outlier <- ifelse(all420data_2$year_start>2019,1, all420data_2$is_outlier)
setDT(all420data_2)[nid==124029, is_outlier := 1]

write.xlsx(all420data_2,  "/FILEPATH/FILENAME.xlsx", sheetName="extraction")

save_crosswalk_version(bundle_version_id= 45806, "/FILEPATH/FILENAME.xlsx", description="DATE;all age/sex split data for 420; remission fix; GBD 2023 2.")

cw420_final <- get_crosswalk_version(43688)


#AGE SPLIT 421

model_version_id <-803237
model_id <- 3076
global   <- F
source('/FILEPATH/age_split.R')


xwalk_prev<- read.xlsx("/FILEPATH/FILENAME.xlsx")
xwalk_prev2<- subset(xwalk_prev, xwalk_prev$measure=='prevalence')


xwalk_prev_as_773301 <- age_split(df = xwalk_prev2,
                                  model_id = 3076,
                                  model_version_id = model_version_id,
                                  release_id = 16,
                                  measure = 'prevalence',
                                  global_age_pattern = global,
                                  drop_under_1_case = F)
xwalk_prev_as_773301$midage <- (xwalk_prev_as_773301$age_start +xwalk_prev_as_773301$age_end)/2

ggplot(xwalk_prev_as_773301, aes(x=midage, y=mean)) + 
  geom_point(size = 5)+ 
  geom_errorbarh(aes(xmin=age_start, xmax=age_end))

all421data_cw <- get_bundle_version(45683)
all421data_mtspec <- subset(all421data_cw, all421data_cw$measure =="mtspecific")

all421data_prev <- xwalk_prev_as_773301

xwalk_inc<- read.xlsx("/FILEPATH/FILENAME.xlsx")
xwalk_inc<- subset(xwalk_prev, xwalk_prev$measure=='incidence')


all421data_1 <- rbind(all421data_prev, xwalk_inc, fill=TRUE)
all421data_2 <- rbind(all421data_1, all421data_mtspec, fill=TRUE)

all421data_2$standard_error<- ifelse(all421data_2$standard_error>1, 0.99, all421data_2$standard_error)
all421data_2$seq <- ifelse(grepl("sex",all421data_2$note_modeler),NA, all421data_2$seq)

# write.xlsx(all421data_2,  "/FILEPATH/FILENAME.xlsx", sheetName="extraction")
# 
# #save_crosswalk_version(bundle_version_id= 42420, "/FILEPATH/FILENAME.xlsx", description="DATE;all age/sex split data for 421; JACC TEST;NO NHS ENGLAND SUBNAT.")
# 
# #tianjin rates too high for age 0-.999
fixtianjin <- all421data_2
setDT(fixtianjin)[nid==337619 & location_id==517 & age_start==0, is_outlier := 1]
setDT(fixtianjin)[nid==337619 & location_id==517 & age_start==0, note_modeler := "data point deemed erroneously high in JACC 2022 model vetting" ]
#outlier australian data points due to non-representatitiveness
#tianjin rates too high for age 0-.999

#australia
setDT(fixtianjin)[nid==522634, is_outlier := 1]
setDT(fixtianjin)[nid==522634, note_modeler := "source non-representative" ]

#chechen republic
setDT(fixtianjin)[nid==409159 & seq==579282, is_outlier := 1]
setDT(fixtianjin)[nid==409159 & seq==579282, note_modeler := "considerable outlier" ]
setDT(fixtianjin)[nid==409159 & seq==579283, is_outlier := 1]
setDT(fixtianjin)[nid==409159 & seq==579283, note_modeler := "considerable outlier" ]
setDT(fixtianjin)[nid==409159 & seq==579284, is_outlier := 1]
setDT(fixtianjin)[nid==409159 & seq==579284, note_modeler := "considerable outlier" ]
setDT(fixtianjin)[nid==409159 & seq==579285, is_outlier := 1]
setDT(fixtianjin)[nid==409159 & seq==579285, note_modeler := "considerable outlier" ]
setDT(fixtianjin)[nid==409159 & seq==579286, is_outlier := 1]
setDT(fixtianjin)[nid==409159 & seq==579286, note_modeler := "considerable outlier" ]
setDT(fixtianjin)[nid==409159 & seq==579287, is_outlier := 1]
setDT(fixtianjin)[nid==409159 & seq==579287, note_modeler := "considerable outlier" ]
setDT(fixtianjin)[nid==409159 & seq==579288, is_outlier := 1]
setDT(fixtianjin)[nid==409159 & seq==579288, note_modeler := "considerable outlier" ]
setDT(fixtianjin)[nid==409159 & seq==579289, is_outlier := 1]
setDT(fixtianjin)[nid==409159 & seq==579289, note_modeler := "considerable outlier" ]

#new caledonia
setDT(fixtianjin)[nid==221324, is_outlier := 1]
setDT(fixtianjin)[nid==221324, note_modeler := "source for new-caledonia" ]

#setDT(fixtianjin)[year_start>2019, is_outlier := 1]

write.xlsx(fixtianjin,  "/FILEPATH/FILENAME.xlsx", sheetName="extraction")
save_crosswalk_version(bundle_version_id= 45683, "/FILEPATH/FILENAME.xlsx", description="DATE;all age/sex split data for 421; GBD 2023 run 2.")

cw421_final <- get_crosswalk_version(43747)

