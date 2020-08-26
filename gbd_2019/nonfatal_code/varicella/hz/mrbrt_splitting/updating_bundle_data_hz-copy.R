#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  "USERNAME"
# Purpose: Integrating GBD 2019 Tetanus CFR systematic review with existing bundle data
# Run:     
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- ""FILEPATH""
  h_root <- ""FILEPATH""
} else { 
  j_root <- ""FILEPATH""
  h_root <- ""FILEPATH""
}
#***********************************************************************************************************************


#-----LOAD FUNCTIONS----------------------------------------------------------------------------------------------------
library(metafor, lib.loc = paste0(j_root, FILEPATH))
library(msm, lib.loc = paste0(j_root, FILEPATH))
library(data.table)
library(ggplot2)
library(readxl)
library(openxlsx)

### central functions
source(paste0(j_root, "/"FILEPATH"get_age_metadata.R"))
source(paste0(j_root, "/"FILEPATH"get_population.R"))
source('"FILEPATH"save_bundle_version.R')
source('"FILEPATH"get_bundle_version.R')
source('"FILEPATH"save_crosswalk_version.R')
source('"FILEPATH"get_crosswalk_version.R')
source(""FILEPATH"get_bundle_data.R")
source(""FILEPATH"upload_bundle_data.R")
source(""FILEPATH"save_bulk_outlier.R")

### source MR-BRT
repo_dir <- paste0(j_root, "/temp/"USERNAME"/prog/projects/run_mr_brt/")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

### bundle specifications
bundle_dir  <- ""FILEPATH"12_bundle"
date        <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d")

### model specifications
model_label <- "new_2019_outliering"  
AGG         <- FALSE  
acause      <- "varicella"
bundle_id   <- 50  
old_step    <- "step2"  
decomp_step <- "step4"
path_to_bundle_data  <- file.path(bundle_dir, acause, bundle_id, ""FILEPATH"", paste0(acause, "_", bundle_id, "_", date, ".xlsx"))
path_to_bundle_data_blank  <- file.path(bundle_dir, acause, bundle_id, ""FILEPATH"", paste0(acause, "_", bundle_id, "_", date, "_blank.xlsx"))
path_to_bundle_data_i  <- file.path(bundle_dir, acause, bundle_id, ""FILEPATH"", paste0(acause, "_", bundle_id, "_", date, "_i.xlsx"))
path_to_bundle_data_ii <- file.path(bundle_dir, acause, bundle_id, ""FILEPATH"", paste0(acause, "_", bundle_id, "_", date, "_ii.xlsx"))
#***********************************************************************************************************************


#-----HELPER OBJECTS AND FUNCTIONS--------------------------------------------------------------------------------------
### objects
input_dir    <- paste0(j_root, "/"FILEPATH"/varicella/"FILEPATH"")
version_dir  <- paste0(j_root, "/"FILEPATH"/varicella/"FILEPATH"versions")
adj_data_dir <- paste0(j_root, "/"FILEPATH"/varicella/"FILEPATH"/01_download/")
save_dir     <- paste0(j_root, "/"FILEPATH"/varicella/"FILEPATH"/02_crosswalk/")
brt_out_dir  <- paste0(j_root, "/"FILEPATH"/varicella/"FILEPATH"")
ifelse(!dir.exists(input_dir), dir.create(input_dir), FALSE)
ifelse(!dir.exists(version_dir), dir.create(version_dir), FALSE)
ifelse(!dir.exists(brt_out_dir), dir.create(brt_out_dir), FALSE)

### functions
draw_summaries <- function(x, new_col, cols_sum, se = F) {
  x[, (paste0(new_col, "_lower")) := apply(.SD, 1, quantile, probs = .025, na.rm =T), .SDcols = (cols_sum)]
  x[, (paste0(new_col, "_mean"))  := rowMeans(.SD), .SDcols = (cols_sum)]
  x[, (paste0(new_col, "_upper")) := apply(.SD, 1, quantile, probs = .975, na.rm =T), .SDcols = (cols_sum)]
  if (se == T) x[, paste0(new_col, "_se") := (get(paste0(new_col, "_upper"))-get(paste0(new_col, "_lower")))/3.92]
  if (se == T) x[get(paste0(new_col, "_se")) == 0, paste0(new_col, "_se") := apply(.SD, 1, sd, na.rm=T), .SDcols = (cols_sum)]
  x <- x[, !cols_sum, with = F]
  return(x)
}

age_dummies <- function(x) {  
  x[, age := (age_start + age_end)/2]
  return(x)
}

'%!in%' <- function(x,y)!('%in%'(x,y))
#***********************************************************************************************************************


#-----PULLING AND CLEANING EXISTING BUNDLE------------------------------------------------------------------------------
### pull current Step 2 bundle
current <- get_bundle_data(bundle_id = bundle_id, decomp_step = old_step, export = TRUE)  #  

### check bundle empty  
new_test <- get_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, export = TRUE)  
if (nrow(new_test) > 0) {
  for (column in names(new_test)[names(new_test) != "seq"]) new_test[, (column) := NA]
  # reupload with everything blank but seq column to clear
  openxlsx::write.xlsx(new_test, file=path_to_bundle_data_blank, sheetName="extraction", row.names=FALSE, col.names=TRUE)
  result <- upload_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, path_to_bundle_data_blank)
  
}
#***********************************************************************************************************************


#-----APPENDING DATASETS TO UPLOAD--------------------------------------------------------------------------------------
step4 <- fread(""FILEPATH"fresh_bundle_version_9506_092819.csv")
step4[sex=="both", sex := "Both"]
step4[urbanicity_type=="mixed/both", urbanicity_type := "Mixed/both"]
step4[unit_type=="person*years" | unit_type=="Person*years", unit_type := "Person*year"]

# ### append together...
updated <- copy(step4)

### clean columns in preparation for upload  "ADDRESS"
updated[is.na(group_review), `:=` (group=1, specificity="total", group_review=1)]  # 1 is "visible", 0 is "hidden"
updated[is.na(is_outlier), is_outlier := 0]
updated[, seq := ""]
updated[, seq_parent := ""]

updated$cases <- as.numeric(updated$cases)
updated$sample_size <- as.numeric(updated$sample_size)

      # first save combined
      openxlsx::write.xlsx(updated, file=path_to_bundle_data, sheetName="extraction", row.names=FALSE, col.names=TRUE)
      
      # subset to the original extractions (i.e. mean/lower/upper) and save
      updated_i <- updated[!is.na(mean) & !is.na(lower) & !is.na(upper)]
      updated_i <- updated_i[, uncertainty_type_value := 95]
      
      # subset to new extractions (i.e. only have cases/sample_size) and save
      updated_ii <- updated[!is.na(cases) & (!is.na(sample_size) | !is.na(mean)) & nid %!in% unique(updated_i$nid)]
      updated_ii <- updated_ii[, uncertainty_type_value := NA]  
      

### save as excel sheet for uploading
openxlsx::write.xlsx(updated_i, file=path_to_bundle_data_i, sheetName="extraction", row.names=FALSE, col.names=TRUE)
openxlsx::write.xlsx(updated_ii, file=path_to_bundle_data_ii, sheetName="extraction", row.names=FALSE, col.names=TRUE)
#***********************************************************************************************************************


#-----UPLOAD BUNDLE!----------------------------------------------------------------------------------------------------

### upload updated_i bundle (i.e. previously existing) to GBD 2019 Step 4
result <- upload_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, path_to_bundle_data_i)   #  
print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
fwrite(result, file = file.path(bundle_dir, acause, bundle_id, "00_documentation", paste0("bundle_upload_id_", date, "_i.csv")), row.names = FALSE)

### upload updated_ii bundle (i.e. new or re-extracted) to GBD 2019 Step 4
result <- upload_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, path_to_bundle_data_ii)  #  
print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
fwrite(result, file = file.path(bundle_dir, acause, bundle_id, "00_documentation", paste0("bundle_upload_id_", date, "_ii.csv")), row.names = FALSE)

#***********************************************************************************************************************


#-----UPLOAD BUNDLE VERSION---------------------------------------------------------------------------------------------
### get_bundle_data
step4 <- get_bundle_data(bundle_id = bundle_id, decomp_step = decomp_step, export = TRUE) 

### save_bundle_version
bundle_metadata <- save_bundle_version(bundle_id = bundle_id, decomp_step = decomp_step)
fwrite(bundle_metadata, file = file.path(version_dir, "bv_metadata_step4.csv"), row.names = F)
#***********************************************************************************************************************


#############################################################################################
###                                 BEGIN PREDICTIONS                                     ###
#############################################################################################

orig_dt <- get_bundle_version(bundle_metadata$bundle_version_id, export=T)

# subset to non-outliers
orig_dt <- orig_dt[is_outlier != 1]

# drop rows where mean or lower or upper equal 0 (as per gbd2019 xw guidance)
orig_dt_0 <- orig_dt[mean == 0]
orig_dt <- orig_dt[mean != 0]

unadj_dt <- orig_dt[sex == "Both"]  
orig_dt[sex == "Both", sex_split := 1]  
orig_dt[is.na(sex_split), sex_split := 0]

## START PREDICTION
# Read in "fit1" object from Step 2
fit1 <- readRDS(""FILEPATH"fit1.rds")

pred1 <- predict_mr_brt(fit1, newdata = unadj_dt, write_draws = T)

## CHECK FOR PREDICTIONS
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)

## GET PREDICTION DRAWS
preds <- as.data.table(pred_object$model_summaries)
draws <- as.data.table(pred_object$model_draws)

## COMPUTE MEAN AND CI OF PREDICTION DRAWS
pred_summaries <- copy(draws)
pred_summaries <- draw_summaries(pred_summaries, "pred",  paste0("draw_", 0:999), T)

## MERGE PREDICTIONS TO DATA THAT NEEDS TO BE SPLIT
setnames(draws, paste0("draw_", 0:999), paste0("ratio_", 0:999))
draws   <- draws[, .SD, .SDcols = paste0("ratio_", 0:999)]
unadj_dt <- cbind(unadj_dt, draws)

# reminder of the subfolder that need to specify where plots are saved:
model_label

## COMPUTE PLOTS (only first four lines with no covs)
# export PATH=""FILEPATH"bin:$PATH"
# source "FILEPATH"activate mr_brt_env
# python "FILEPATH"mr_brt_visualize.py \
# --mr_dir "FILEPATH"
# --continuous_variables age \
# --dose_variable age

#############################################################################################
###                                 PREP FOR SEX SPLIT                                    ###
#############################################################################################
unadj_dt[, year_id := floor((year_end + year_start)/2)]

source(""FILEPATH"add_population_cols_fxn.R")
unadj_dt <- add_population_cols(unadj_dt, gbd_round = 6, decomp_step = decomp_step)
# remove unnecessary columns
unadj_dt[, c("year_id", "sex_id") := NULL]


#############################################################################################
###                                  BEGIN SEX SPLIT                                      ###
#############################################################################################

## EXPONENTIATE PREDICTION DRAWS
unadj_dt[, paste0("ratio_", 0:999) := lapply(0:999, function(x) exp(get(paste0("ratio_", x))))]

## GET 1,000 DRAWS OF CFR  #  
unadj_dt[, log_mean := log(mean)]

unadj_dt$log_se <- sapply(1:nrow(unadj_dt), function(i) {
  mean_i <- unadj_dt[i, mean]
  se_i   <- unadj_dt[i, standard_error]
  deltamethod(~log(x1), mean_i, se_i^2)
})


unadj_dt[, paste0("both_cfr_", 0:999) := lapply(0:999, function(x){
  rnorm(n = nrow(unadj_dt), mean = log_mean, sd = log_se)
})]

unadj_dt[, paste0("both_cfr_", 0:999) := lapply(0:999, function(x) exp(get(paste0("both_cfr_", x))) * 0.995)]
unadj_dt[, log_mean := NULL][, log_se := NULL]

## SEX SPLIT AT 1,000 DRAW LEVEL
unadj_dt[, paste0("cfr_female_", 0:999) := lapply(0:999, function(x) get(paste0("both_cfr_", x))*((both_population)/(female_population+get(paste0("ratio_", x))*male_population)) )]  
unadj_dt[, paste0("cfr_male_", 0:999)   := lapply(0:999, function(x) get(paste0("ratio_", x))*get(paste0("cfr_female_", x)))]

## COMPUTE MEAN AND UI
unadj_dt <- draw_summaries(unadj_dt, "male",   paste0("cfr_male_", 0:999), T)
unadj_dt <- draw_summaries(unadj_dt, "female", paste0("cfr_female_", 0:999), T)


#############################################################################################
###                                         FORMAT                                        ###
#############################################################################################

## CLEAN POST SEX-SPLIT TABLE
cols_rem <- names(unadj_dt)[names(unadj_dt) %like% "ratio_|cfr_|_pop"]
unadj_dt  <- unadj_dt[, .SD, .SDcols = -cols_rem]

orig_dt_0[, ":="(male_lower = lower,
                 female_lower = lower,
                 male_mean = mean,
                 female_mean = mean,
                 female_upper = upper,
                 male_upper = upper,
                 male_se = standard_error,
                 female_se = standard_error)]
unadj_dt <- rbind(unadj_dt, orig_dt_0)


## CREATE A MALE TABLE
male <- copy(unadj_dt)
male <- male[, .SD, .SDcols = -names(unadj_dt)[names(unadj_dt) %like% "female"]]

male[, `:=` (mean = NULL, standard_error = NULL, upper = NULL, lower = NULL)]
male[, sex := "Male"]
setnames(male, c("male_lower", "male_mean", "male_upper", "male_se"), c("lower", "mean", "upper", "standard_error"))

## CREATE A FEMALE TABLE
female <- copy(unadj_dt)

female[, `:=` (male_lower = NULL, male_mean = NULL, male_upper = NULL, male_se = NULL)]
female[, `:=` (mean = NULL, standard_error = NULL, upper = NULL, lower = NULL)]
female[, sex := "Female"]
setnames(female, c("female_lower", "female_mean", "female_upper", "female_se"), c("lower", "mean", "upper", "standard_error"))

## CREATE A BOTH SEX TABLE
both <- copy(unadj_dt)
both <- both[, .SD, .SDcols = -names(unadj_dt)[names(unadj_dt) %like% "male"]]

## SEX-SPEC TABLE
sex_spec <- rbind(male, female)
sex_spec[, sex_split := 1]
sex_spec[, uncertainty_type := "Standard error"]
sex_spec[, cases := mean * sample_size]


## FORMAT
sex_spec[, specificity := paste0("Sex split using ratio from MR-BRT")]

## COMPUTE UI
sex_spec[, lower := mean-1.96*standard_error][, upper := mean+1.96*standard_error]
sex_spec[lower < 0, lower := 0][upper > 1, upper := 1]

## APPEND THE SEX SPECIFC ESTIMATES
sex_adj_dt <- copy(orig_dt)
# add a 'specificity' column for sex_adj_dt
sex_adj_dt[, specificity := ""]
sex_adj_dt <- sex_adj_dt[sex_split == 0]
sex_adj_dt <- rbind(sex_adj_dt, sex_spec)
sex_adj_dt <- sex_adj_dt[order(nid, year_start, sex, age_start)]  

## SAVE
fwrite(sex_adj_dt, paste0(save_dir, "sex_split_data_step4.csv"), row.names = F)


#############################################################################################
###                    SAVE XW VERSION OF SEX-SPLIT DATA TO GET AGE-SPLIT                 ### 
#############################################################################################
# define file path for saving
path_to_crosswalk_sex_data <- file.path(bundle_dir, acause, bundle_id, ""FILEPATH"", paste0(acause, bundle_id, date, "_sex_split_no_age_data_step4.xlsx"))

# add additional columns necessary for uploading xw version
setnames(sex_adj_dt, "origin_seq", "crosswalk_parent_seq")
# need new / non-duplicate row seqs
sex_adj_dt[, seq := NA]
#label specificity appropriately
sex_adj_dt[is.na(specificity) | specificity=="", specificity := "Original data"]
sex_adj_dt <- sex_adj_dt[group_review != 0]

sex_adj_dt <- sex_adj_dt[location_id != 95]
sex_adj_dt <- sex_adj_dt[location_id != 4626]

# save as .xlsx file which will point to in 'save' call
openxlsx::write.xlsx(sex_adj_dt, file=path_to_crosswalk_sex_data, sheetName="extraction", row.names=FALSE, col.names=TRUE)


# save crosswalk version for Dismod using path_to_bundle_data since no additional processing before this
bundle_metadata_step4 <- fread(file = file.path(version_dir, paste0("bv_metadata_step4.csv")))
crosswalk_description <- "Complete step 4 sex split data to get age split"
crosswalk_metadata <- save_crosswalk_version(bundle_version_id = bundle_metadata_step4$bundle_version_id[1], data_filepath = path_to_crosswalk_sex_data,
                                             description = crosswalk_description)

fwrite(crosswalk_metadata, file = paste0(input_dir, "sex_split_no_age_crosswalk_metadata_step4.csv"), row.names = F)



#############################################################################################
###                    SAVE XW VERSION FOR DISMOD AGE PATTERN                             ###
#############################################################################################
# define file path for saving
path_to_crosswalk_age_data <- file.path(FILEPATH, "_dismod_age_pattern_data_step4.xlsx"))

# add additional columns necessary for uploading xw version
setnames(sex_adj_dt, "origin_seq", "crosswalk_parent_seq")
# need new / non-duplicate row seqs
sex_adj_dt[, seq := NA]

sex_adj_dt <- sex_adj_dt[location_id != 95]

age_spec_subset <- sex_adj_dt[, age_band := abs(age_end - age_start)]
age_spec_subset <- age_spec_subset[age_band < 20]

# save as .xlsx file which will point to in 'save' call
openxlsx::write.xlsx(age_spec_subset, file=path_to_crosswalk_age_data, sheetName="extraction", row.names=FALSE, col.names=TRUE)


# save crosswalk version for Dismod using path_to_bundle_data since no additional processing before this
crosswalk_description <- "NOTE "
crosswalk_metadata <- save_crosswalk_version(bundle_version_id = bundle_metadata$bundle_version_id[1], data_filepath = path_to_crosswalk_age_data,
                                             description = crosswalk_description)
fwrite(crosswalk_metadata, file = paste0(version_dir, "age_split_crosswalk_metadata.csv"), row.names = F)



#############################################################################################
###                 COMBINE W/IN STUDY, SEX-SPLIT, AGE-SPLIT DATA                         ###
#############################################################################################

## READ IN SEX-SPLIT DATA
ss_metadata   <- fread(paste0(""FILEPATH"sex_split_no_age_crosswalk_metadata_step4.csv"))
input_data_ss <- get_crosswalk_version(crosswalk_version_id = ss_metadata$crosswalk_version_id[1])

input_data_ws_ss <- copy(input_data_ss)

input_data_ws_ss[is.na(sample_size), sample_size := effective_sample_size]
input_data_ws_ss[is.na(cases), cases := mean * sample_size]

input_data_ws_ss[is.na(cases) & is.na(sample_size) & measure == "proportion", sample_size := (mean*(1-mean)/standard_error^2)]
input_data_ws_ss[is.na(cases) & is.na(sample_size) & measure == "cfr", sample_size := (mean*(1-mean)/standard_error^2)]
input_data_ws_ss[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
input_data_ws_ss[is.na(cases), cases := mean * sample_size]


## APPLY_AGE_PATTERN TO SEX-SPLIT DATA
meid <- 24320
mvid <- 393290
source(""FILEPATH"apply_age_crosswalk.R")
input_data_ss_ws_as <- apply_age_split(data = input_data_ws_ss,
                                       dismod_meid = meid,
                                       dismod_mvid = mvid,
                                       loc_pattern = 1,
                                       decomp_step_meid = "iterative",
                                       decomp_step_pop = "iterative",
                                       write_out = TRUE,
                                       out_file = file.path(FILEPATH, "_ss_ws_as_data.xlsx")))

## CLEANING DATA COLS FOR UPLOAD
input_data_ss_ws_as[, seq := NA]
input_data_ss_ws_as[, age_diff := age_end - age_start]

input_data_ss_ws_as[age_diff < 0, age_start := 0]  
input_data_ss_ws_as[, age_diff := age_end - age_start]


input_data_ss_ws_as <- input_data_ss_ws_as[nid==282600, is_outlier := 1]



## SAVE CROSSWALK VERSION TO DECOMP_STEP STEP2 FOR FINAL NONFATAL MODEL
# define file path for saving
path_to_final_crosswalk_data <- file.path(FILEPATH, "_final_crosswalk_data_ss_as_step4.xlsx"))

# save as .xlsx file which will point to in 'save' call
openxlsx::write.xlsx(input_data_ss_ws_as, file=path_to_final_crosswalk_data, sheetName="extraction", row.names=FALSE, col.names=TRUE)

# make the save!
bundle_metadata_step4 <- fread(file = file.path(FILEPATH, "bv_metadata_step4.csv")))
crosswalk_description <- "NOTE"
crosswalk_metadata <- save_crosswalk_version(bundle_version_id = bundle_metadata_step4$bundle_version_id[1], data_filepath = path_to_final_crosswalk_data,
                                             description = crosswalk_description)
fwrite(crosswalk_metadata, file = paste0(FILEPATH, "final_crosswalk_metadata_step4.csv"), row.names = F) 

#***********************************************************************************************************************

#-----UPLOAD CROSSWALK VERSION with NID 133385 BULK OUTLIERED-----------------------------------------------------------
### useful objects
xw_v_id <- 11330
path_to_edited_xwv <- file.path(FILEPATH, "_edited_final_crosswalk_data_ss_as_step4.xlsx"))
note <- "NOTE"

### read in current "final" crosswalk version
xwv <- get_crosswalk_version(crosswalk_version_id=xw_v_id, export=T)

### make outlier change, save excel
xw_edited <- xwv[nid==282600, is_outlier := 1]



# make 'crosswalk_parent_seq' the 'seq'
xw_edited[, crosswalk_parent_seq := seq]
openxlsx::write.xlsx(xw_edited, file=path_to_edited_xwv, sheetName="extraction", row.names=FALSE, col.names=TRUE)

### upload edited crosswalk version with new outliers
result <- save_bulk_outlier(crosswalk_version_id=xw_v_id, decomp_step=decomp_step, filepath=path_to_edited_xwv, description=note)
# #***********************************************************************************************************************



#########
######### Adjusting data
#########

# get data
xw_v_id <- 10139  
path_to_edited_xwv <- file.path(FILEPATH, "_edited_final_crosswalk_data_ss_as_step4_new_outliers.xlsx"))
note <- "NOTE"

diph <- get_crosswalk_version(crosswalk_version_id=xw_v_id, export=T)

# un-outlier
diph[nid==133384, is_outlier := 0]  
diph[nid==416122, is_outlier := 0]  
diph[nid==25325, is_outlier := 0]   

# add outliers
diph[nid==139072, is_outlier := 1]  

# make 'crosswalk_parent_seq' the 'seq'
diph[, crosswalk_parent_seq := seq]

# re-save as .xlsx file which will point to in 'save' call
openxlsx::write.xlsx(diph, file=path_to_edited_xwv, sheetName="extraction", row.names=FALSE, col.names=TRUE)

# upload edited crosswalk version with new outliers
result <- save_bulk_outlier(crosswalk_version_id=xw_v_id, decomp_step=decomp_step, filepath=path_to_edited_xwv, description=note)











