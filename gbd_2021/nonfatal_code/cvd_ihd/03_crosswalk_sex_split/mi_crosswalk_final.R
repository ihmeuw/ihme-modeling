## Final MI crosswalk
##
##
## How to test code manually?


#rm(list=ls())

os <- .Platform$OS.typ
j <- "/FILEPATH/"

date <- gsub("-", "_", Sys.Date())

"%ni%" <- Negate("%in%")

"%contain%" <- function(values,x) {
  tx <- table(x)
  tv <- table(values)
  z <- tv[names(tx)] - tx
  all(z >= 0 & !is.na(z))
}

pacman::p_load(rmarkdown, data.table, ggplot2, doBy, openxlsx, tidyverse, readxl, gtools)

# Central comp shared functions
central <- "/FILEPATH/"
for (func in paste0(central, list.files(central))) source(paste0(func))

source('/FILEPATH/master_mrbrt_crosswalk_func_new_sex_split.R')
source('/FILEPATH/MI_xw.R')
source("/FILEPATH/aggregate_subnats.R")

source("/FILEPATH/save_bundle_version.R")
source("/FILEPATH/get_bundle_version.R")

### Paths, arguments
##################################################################################

## receive initialize settings from [command args]
#args <- commandArgs(trailingOnly = TRUE)
#model_abbr <- as.character(args[1])
#gbd_round_id <- as.integer(args[2])
#decomp_step <- as.character(args[3])
#settings_file_path <- as.character((args[4]))

#cat(paste(args))
ids_mi <- get_elmo_ids(gbd_round_id = 7, decomp_step = "iterative", bundle_id = 7052)
ids_mi[, .(bundle_version_id, crosswalk_version_date, crosswalk_version_id, crosswalk_description, model_version_id, model_version_description)]
## [dummy initialization vars]
model_abbr <-   "mi_7052" #"angina_115" 
gbd_round_id <- 7
decomp_step <- "iterative"

settings_file_path <- "/FILEPATH/xw_settings_2020_07_20.xlsx"
## require folders

plot_output_folder <- 'FILEPATH'
storage_folder <- 'FILEPATH'

plot_source_folder <- 'FILEPATH'
plot_source_folder <- 'FILEPATH'

### Extracting and formatting all settings variables
##################################################################################

## import settings file
settings <- data.table(read.xlsx(settings_file_path))

# store all settings into a list (for passing into plotting file)
var_collector <- list()

for (single_var in settings$variables) {
  single_setting <- settings[variables == single_var, get(model_abbr)]
  
  # T/F char to Boolean
  if(single_setting %in% c('T', 'F')) {
    single_setting <- ifelse(single_setting == 'T', T, F)
  }
  
  # number/float char to numeric
  if(!grepl("\\D", single_setting) | single_var %in% c('trim_pct')) {
    single_setting <- as.numeric(single_setting)
  }
  
  # comma cat string to vector
  if(single_var %in% c('alternate_defs', 'addl_x_covs') && !is.na(single_setting)) {
    single_setting <- unlist(strsplit(single_setting, ","))
  }
  
  # mount settings to environment
  if(!is.na(single_setting)) {
    assign(single_var, single_setting)
  } else {
    assign(single_var, NULL)
  }
  
  # also store them into a collection for saving later
  var_collector[[single_var]] <- single_setting
  
}

## path for save crosswalk version
save_crosswalk_result_file_name <- paste0("xw_results_", date , "_", acause, "_", bundle_id, "_", xw_measure, ".xlsx")
save_crosswalk_result_file <- paste0("/FILEPATH/", save_crosswalk_result_file_name)

sex_split_file <- paste0("/FILEPATH/", model_abbr, "_", date, ".xlsx")
matches_file <- paste0("/FILEPATH/", model_abbr, "_", date, ".xlsx")


###################################################################
# Get bundle data / save bundle data
##### 

bun_data <- data.table(get_bundle_version(bundle_version_id = bundle_version_id, fetch = 'all'))
data <- bun_data
data <- data[measure == xw_measure]
zeroes <- data[mean == 0]
data <- data[mean > 0]

data$age_end[(data$age_end > 95 )] <- 99


if(model_abbr == "angina_115") {
  data[, reference := ifelse(clinical_data_type=="claims", 1, 0)]
  cols <- grep("^cv_raq", colnames(data), value=TRUE)
  data[, cv_alternate := rowSums(.SD), .SDcols = cols]
  data[!is.na(cv_alternate) & cv_alternate > 1, cv_alternate := 1] #Identify alternate definition
  #data <- data[!(is.na(cv_alternate) & clinical_data_type == "claims")]
  data[is.na(cv_alternate), cv_alternate := 0]
  
  # 296602 is cv_raq=1; 109878: there are two where group_review==0; those I didn't tag because they will get dropped anyway the other two are cv_raq=1
  data <- data[nid == 296602, cv_alternate := 1]
  data <- data[nid == 109878 , cv_alternate := 1]
  data <- data[nid != 109891]
  
  data[, which(grepl("^cv_raq", colnames(data))):=NULL]
  data <- data[nid!=97455] ## Remove expenditure data
}



## Remove clinical informatics
if(clinical_informatics == 0) {
  data <- data[clinical_data_type ==""]
}

## all clinical informatics
if(clinical_informatics == 1) {
  data[, cv_alternate := ifelse(clinical_data_type!="", 1, 0)]
}

for (x in alternate_defs) data[is.na(get(x)), paste0(x) := 0]

## drop data from non-representative source
#data <- subset(data, !(location_name %in% c("Jordan", "Iran (Islamic Republic of)", "Meghalaya", "Karnataka", "Nepal", "Kenya", "Tibet", "Botswana", "Kyrgyzstan" )))

# subset ages, prevalance clinical informatics
if(!is.null(subset_by_age)) {
  if(subset_by_age > 0) {
    data <- data[age_start >= subset_by_age]
  }
}

### Aggregate data ###
data <- aggregate_subnats(data)


# add other necessary variables into collector for plotting
var_collector[['original_data']] <- data
var_collector[['storage_folder']] <- storage_folder
var_collector[['gbd_round_id']] <- gbd_round_id
var_collector[['decomp_step']] <- decomp_step


# save.image(file=paste0(storage_folder, "FILEPATH/mi_114_image.Rdata"))
# message("\n[INFO] mi_114_image before master_mrbrt_crosswalk saved to ", paste0(storage_folder, "FILEPATH/mi_114_image.Rdata"), "\n")

##### 
# If you don't have your case defs marked appropriately, do it now..!
##### 

crosswalk_holder <- master_mrbrt_crosswalk(
  
  model_abbr = model_abbr,
  data = data,
  reference_def = reference_def,
  alternate_defs = alternate_defs,
  age_overlap = age_overlap,
  year_overlap = year_overlap,
  age_range = age_range,
  year_range = year_range,
  xw_measure = xw_measure,
  addl_x_covs = addl_x_covs,
  storage_folder = storage_folder,
  sex_split_only = sex_split_only,
  subnat_to_nat = subnat_to_nat,
  
  sex_age_overlap = sex_age_overlap,
  sex_year_overlap = sex_year_overlap,
  sex_age_range = sex_age_range,
  sex_year_range = sex_year_range,
  sex_fix_zeros = sex_fix_zeros,
  use_lasso = use_lasso,
  id_vars = id_vars,
  opt_method = opt_method,
  trim_pct = trim_pct,
  remove_x_intercept = remove_x_intercept,
  sex_remove_x_intercept = sex_remove_x_intercept,
  sex_covs = sex_covs,
  spline_covs = spline_covs,
  
  gbd_round_id = gbd_round_id,
  decomp_step = decomp_step,
  logit_transform = logit_transform,
  
  upload_crosswalk_version = upload_crosswalk_version,
  bundle_version_id = bundle_version_id,
  save_crosswalk_result_file = save_crosswalk_result_file
)

### Saving data
###################################################################################

###
#
# Rmarkdown render plots for vetting
#
# Plot file for each causes will be generated in an individual folder 
#
# See docs for details of each plotting function
#
#

## save all potentially useful data to local Rdata, for quick recreating xw environment and render two rounds xw comparison plot

crosswalk_holder$var_collector = data.table(t(var_collector))

if(save_rdata_local) {
  save(crosswalk_holder, file = paste0(storage_folder, model_abbr , "_crosswalk_holder.Rdata"))
  message(paste0("\n [INFO] All results saved to ", storage_folder, model_abbr , "_crosswalk_holder.Rdata\nPython objects that were saved during crosswalking can also be found in the folder\n"))
}


### Plotting
###################################################################################


# NOTE: If your crosswalk_holder is no longer in your memory but you want to run diagnostics on it, uncomment the following line to load it in:
#load(paste0(storage_folder, model_abbr , "_crosswalk_holder.Rdata"))

# create folder for plot if it doesn't exist
dir.create(file.path(plot_output_folder, model_abbr), showWarnings = FALSE)

# remove .Rhistory from vetting folder to avoid error if someone else saved their own .Rhistory here when code was run
if (file.exists(paste0(plot_source_folder, '.Rhistory'))){
  file.remove(paste0(plot_source_folder, '.Rhistory'))
}

# call rmarkdown
rmarkdown::render(
  input = paste0(plot_source_folder, 'vetting_report.Rmd'),
  output_file = paste0(plot_output_folder, model_abbr, "/", model_abbr, "_", date, ".html"),
  clean = TRUE,
  
  params = list(storage_folder = storage_folder, model_abbr = model_abbr, 
                crosswalk_holder = crosswalk_holder)
)


########################################################################################
# Prepare data to upload

#Get  data
#----------------------------------------------------------------------------------------
mi_ids <- get_elmo_ids(gbd_round_id = 7, decomp_step = "iterative", bundle_id = 7052)
bun_data <- data.table(get_bundle_version(bundle_version_id = bundle_version_id, fetch = 'all')) #33944
unique(bun_data$measure)

xwalked <- data.table(read.xlsx("/FILEPATH/xw_results_2020_09_29_cvd_ihd_7052_incidence.xlsx"))
zeroes <- bun_data[mean == 0 & measure == "incidence" & clinical_data_type == ""]
clinical_data <- bun_data[clinical_data_type == "inpatient" | clinical_data_type == "claims, inpatient only"]
mtspecific <- bun_data[measure=="mtspecific"]
mtexcess <- bun_data[measure=="mtexcess"]
mtexcess_split <- as.data.table(read_excel("/FILEPATH/mi_7052_mtexcess_2020_08_24.xlsx"))
mtexcess_split <- select(mtexcess_split, -"aggregated")

cfr <-  bun_data[measure=="cfr"]

# Outlier clinical data 
## By age
clinical_data$is_outlier[(clinical_data$age_start <40)] <- 1 

## By location
locs <- get_location_metadata(location_set_id=22, gbd_round_id=7, decomp_step="iterative")
clinical_data$is_outlier[(clinical_data$location_id == 193)] <- 1 #Botswana
clinical_data$is_outlier[(clinical_data$location_id == 151)] <- 1 #Qatar
clinical_data$is_outlier[(clinical_data$location_id == 144)] <- 1 #Jordan
clinical_data$is_outlier[(clinical_data$location_id == 142)] <- 1 #Iran
clinical_data$is_outlier[(clinical_data$location_id == 4862)] <- 1 #Meghalaya
clinical_data$is_outlier[(clinical_data$location_id == 4856)] <- 1 #Kernataka
clinical_data$is_outlier[(clinical_data$location_id == 43887)] <- 1 #Kernataka
clinical_data$is_outlier[(clinical_data$location_id == 43923)] <- 1 #Kernataka
clinical_data$is_outlier[(clinical_data$location_id == 164)] <- 1 #Nepal
clinical_data$is_outlier[(clinical_data$location_id == 180)] <- 1 #Kenya
clinical_data$is_outlier[(clinical_data$location_id == 518)] <- 1 #Tibet
clinical_data$is_outlier[(clinical_data$location_id == 36)] <- 1 #Kazahastan
clinical_data$is_outlier[(clinical_data$location_id == 155)] <- 1 #Turkey
clinical_data$is_outlier[(clinical_data$location_id == 122)] <- 1# Ecuador
clinical_data$is_outlier[(clinical_data$location_id == 90)] <- 1# Norway
clinical_data$is_outlier[(clinical_data$location_id == 6)] <- 1# China
clinical_data$is_outlier[(clinical_data$location_id == 130)] <- #Mex

# China subnats
china_subnats <- locs[parent_id == 44533]
china_ids <- unique(china_subnats$location_id)
clinical_data$is_outlier[(clinical_data$location_id %in% china_ids)] <- 1# China
#Mexico
mex_subnats <- locs[parent_id == 130]
mex_ids <- unique(mex_subnats$location_id)
clinical_data$is_outlier[(clinical_data$location_id %in% mex_ids)] <- 1# Mex


#by nid
clinical_data$is_outlier[(clinical_data$nid == 284440  & clinical_data$location_id ==  44850 & clinical_data$age_start == 95 )] <- 1 #New Zeland Maori population
clinical_data$is_outlier[(clinical_data$nid ==  337619)] <- 1  #China nid

clinical_data$is_outlier[(clinical_data$nid ==  234750)] <- 1 
clinical_data$is_outlier[(clinical_data$nid ==  336851 & clinical_data$nid ==  336852)] <- 1 #Japan

clinical_data$is_outlier[(clinical_data$nid ==  234750)] <- 1  #norway subnationals clinical data

rows_to_remove <- xwalk[location_id==95]
#Fix nid location id 95 for England
all <- rows_to_remove
uk  <- all[location_id == 95]
uk$parent_id <- uk$seq_parent
uk$seq <- NA

england <- uk
england$location_id <- 4749 
england$location_name <-"England"
england$ihme_loc_id <- "GBR_4749"
england$sample_size <- england$sample_size*0.841
england$note_sr <- "UK data remapped to appropriate UTLAs: England, Scotland, Wales, N. Irelanand"
england$group_review <- 0


ni <- uk
ni$location_id <- 433
ni$location_name <-"Northern Ireland"
ni$ihme_loc_id <- "GBR_433"
ni$sample_size <- ni$sample_size*0.029
ni$note_sr <- "UK data remapped to appropriate UTLAs: England, Scotland, Wales, N. Irelanand"
ni$group_review <- 0

scotland <- uk
scotland$location_id <- 434
scotland$location_name <-"Scotland"
scotland$ihme_loc_id <- "GBR_434"
scotland$sample_size <- scotland$sample_size*0.083
scotland$note_sr <- "UK data remapped to appropriate UTLAs: England, Scotland, Wales, N. Irelanand"
scotland$group_review <- 0

wales <- uk
wales$location_id <- 4636
wales$location_name <-"Wales"
wales$ihme_loc_id <- "GBR_4636"
wales$sample_size <- wales$sample_size*0.048
wales$note_sr <- "UK data remapped to appropriate UTLAs: England, Scotland, Wales, N. Irelanand"
wales$group_review <- 0

all <- all[location_id != 95]
all <- rbind(all, england, fill = TRUE)
all <- rbind(all, ni, fill = TRUE)
all <- rbind(all, scotland, fill = TRUE)
all <- rbind(all, wales, fill = TRUE)
#all <- all[group_review>0 | is.na(group_review)]
all[, note_sr := NULL]

all$crosswalk_parent_seq[!is.na(all$seq)] <- all$seq
all$seq <- NA

#Zeroes 
zeroes <- zeroes[sex!="Both"]

all$crosswalk_parent_seq[!is.na(all$seq)] <- all$seq
all$seq <- NA

xwalk_data$crosswalk_parent_seq[!is.na(xwalk_data$seq)] <- xwalk_data$seq
xwalk_data$seq <- NA

#115783 Iran nid loc id 142
#Russia nid 145858 loc id 62
#Merge crosswalked data + zeroes that were deleted for the crosswalk
full_bundle <- do.call("rbind", list(xwalked, zeroes, clinical_data, mtspecific, fill = TRUE))
full_bundle <- select(full_bundle, -year_mid, -age_mid, -aggregated, -note_sr, -reference)
full_bundle <- rbind(full_bundle, all, fill=TRUE)

russia <- xwalk_final[nid== 145858]
Iran <- xwalk_final[nid == 115783]

full_bundle <- do.call("rbind", list(full_bundle, russia, Iran, fill=TRUE))
xwalk <- full_bundle[!is.na(lower)]

xwalk <- openxlsx::read.xlsx("/FILEPATH/ihd_7052_final_no_age_split.xlsx")


#Final outliers by age and measure
xwalk$is_outlier[(xwalk$clinical_data_type == "claims, inpatient only" & xwalk$clinical_data_type == "inpatient"  & xwalk$age_start <40 )] <- 1
xwalk$is_outlier[(xwalk$measure == "incidence" & xwalk$age_start <15 )] <- 1
xwalk$is_outlier[(xwalk$measure == "mtexcess"  & xwalk$age_start <15 )] <- 1
xwalk$is_outlier[(xwalk$measure == "mtspecific"  & xwalk$age_start <15 )] <- 1

xwalk$is_outlier[(xwalk$nid ==  234750)] <- 1  #norway subnationals clinical
xwalk$is_outlier[(xwalk$nid ==  420877)] <- 1  #norway subnationals
xwalk$is_outlier[(xwalk$clinical_data_type == "claims" & xwalk$year_start == 2000 )] <- 1

#xwalk <- xwalk[year_start > 1990]
xwalk_final$uncertainty_type_value[is.na(xwalk_final$uncertainty_type_value) ] <- 95
xwalk_final$uncertainty_type[is.na(xwalk_final$sample_size)] <- "Confidence interval" 

xwalk <- as.data.table(xwalk)
xwalk <- xwalk[location_id!=95]
xwalk_final <- xwalk[nid!=109891]
xwalk_final <- xwalk_final[!is.na(lower)]
save_crosswalk_result_file <- paste0("/FILEPATH/full_bundle_no_age_split_",date,".xlsx")
write.xlsx(xwalk_final, file = save_crosswalk_result_file, sheetName = "extraction")
#test <- read_xlsx(save_crosswalk_result_file)


##Apply max filter after
bundle_version_id <- 33944

description <- "Test, no age split"
save_crosswalk_version_result <- save_crosswalk_version(
  bundle_version_id=bundle_version_id,
  data_filepath=save_crosswalk_result_file,
  description=description)
  
#############################################################################################
####################################################################################################


  