###########################################################################
### Purpose: GBD 2023 data processing for hepatitis E seroprevalence model   
###########################################################################

rm(list=ls())


# SOURCE FUNCTIONS AND LIBRARIES --------------------------------------------------
invisible(sapply(list.files("FILEPATH", full.names = T), source))
pacman::p_load(data.table, ggplot2, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, readxl, boot, gtools, msm, Hmisc, metafor)

source("FILEPATH/cw_mrbrt_helper_functions.R") 
source("FILEPATH/sex_splitting.R") 

gen_scale_labels <- function(x) {
  vector_length <- length(x)
  bin_labels <- character(vector_length-1)
  for (i in 2:vector_length) {
    if (i==2) {
      bin_labels[i-1] <- sprintf("<%s",x[i])
    } else if (i==vector_length) {
      label <- sprintf("%s to %s",x[i-1],x[i])
      #label <- sprintf("\U2265%s",x[i-1])
      bin_labels[i-1] <- label
    } else {
      bin_labels[i-1] <- sprintf("%s to <%s",x[i-1],x[i])
    }
  }
  return(bin_labels)
}


# SOURCE CROSSWALK PACKAGE --------------------------------------------------------
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")


# SET OBJECTS -------------------------------------------------------------
b_id <- "OBJECT"
a_cause <- "hepatitis_e"
date <- gsub("-", "_", Sys.Date())
release_id = "OBJECT" 
draws <- paste0("draw_", 0:999)
mrbrt_crosswalk_dir <- paste0("FILEPATH")
mrbrt_sexratio_dir <- paste0("FILEPATH")

if (!dir.exists(mrbrt_crosswalk_dir)) dir.create(mrbrt_crosswalk_dir)
if (!dir.exists(mrbrt_sexratio_dir)) dir.create(mrbrt_sexratio_dir) 

outlier_status <- c(0, 1) 
sex_split <- T
sex_covs <- "" 
id_vars <- "study_id"
keep_x_intercept <- T
logit_transform <- T
reference <- ""
reference_def <- "reference"
measures <- "prevalence"


if(logit_transform == T) {
  response <- "ldiff"
  data_se <- "ldiff_se"
  mrbrt_response <- "diff_logit"
} else {
  response <- "ratio_log"
  data_se <- "ratio_se_log"
  mrbrt_response <- "diff_log"
}


# GET METADATA ------------------------------------------------------
loc_dt <- get_location_metadata("OBJECT", release_id="OBJECT")
loc_dt1 <- loc_dt[, .(location_id, super_region_name)]

################################################################################
## STEP 1: Get seroprevalence data
################################################################################
orig_dt <- get_bundle_version(bundle_version_id = "OBJECT", fetch = "all", export = FALSE)


################################################################################
## STEP 2: Run sex split
################################################################################
#sex match
cv_drop <- ""
hep_sex_dt <- orig_dt[measure %in% c("prevalence")]
hep_sex_dt <- hep_sex_dt[is_outlier %in% outlier_status]
hep_sex_dt <- get_cases_sample_size(hep_sex_dt)
hep_sex_dt <- get_se(hep_sex_dt)
hep_sex_dt <- calculate_cases_fromse(hep_sex_dt)

message("There are ", nrow(hep_sex_dt[sample_size == 0,]), " rows of data with sample size zero")
hep_sex_dt <- hep_sex_dt[sample_size != 0, ] 
hep_sex_matches <- find_sex_match(hep_sex_dt)
unique(hep_sex_matches[, .N, by = c(sex_covs)])
mrbrt_sex_dt <- calc_sex_ratios(hep_sex_matches)

# FORMAT THE DATA FOR META-REGRESSION 
message("Formatting sex data without covariates")
formatted_data <- cw$CWData(df = mrbrt_sex_dt,
                              obs = "ydiff_log_mean",   # matched differences in log space
                              obs_se = "ydiff_log_se",  # SE of matched differences in log space
                              alt_dorms = "dorm_alt",   # var for the alternative def/method
                              ref_dorms = "dorm_ref",   # var for the reference def/method
                              covs = list(),            # list of (potential) covariate column names
                              study_id = id_vars  )     # var for random intercepts; i.e. (1|study_id)

## FOREST PLOT
mrbrt_sex_dt2 <- copy(mrbrt_sex_dt)
mrbrt_sex_dt2 <- merge(mrbrt_sex_dt2, loc_dt[, c("location_id", "location_name")], by = "location_id")
library("metafor")
ggplot(mrbrt_sex_dt2, aes(y = sort(interaction(as.factor(nid), age_start, age_end, location_name)) , x = ydiff_log_mean, xmin = (ydiff_log_mean-1.96*ydiff_log_se), xmax = (ydiff_log_mean+1.96*ydiff_log_se), color = factor(cv_outbreak))) +
  geom_point(size = 3) + # Point estimate
  geom_errorbarh(height = 0.1) + # Confidence interval
  geom_vline(xintercept = 0, linetype = "dotted") + # Line at null effect
  labs(title = "Forest Plot",
       x = "Effect Size",
       y = "Study") +
  theme_minimal()


## LAUNCH MR-BRT SEX-MODEL ------------------------------------
sex_results <- cw$CWModel(cwdata = formatted_data,           # result of CWData() function call
                          obs_type = "diff_log",             # must be "diff_logit" or "diff_log"
                          cov_models = list(                 # specify covariate details
                                            cw$CovModel("intercept")),
                          gold_dorm = "Male")                # level of 'ref_dorms' that's the gold standard


sex_results$fit()
sex_results$fixed_vars
sex_results$beta_sd  
sex_results$gamma

save <- save_model_RDS(sex_results, mrbrt_sexratio_dir)  
save_r <- save_mrbrt(sex_results, mrbrt_sexratio_dir)


# APPLY SEX-SPLIT ADJUSTMENT FACTORS TO ORIGINAL DATA (SEX-AGGREGATE DATA POINTS -----------------------
orig_dt2a <- subset(orig_dt, group_review!=0| is.na(group_review)) 
orig_dt2a$note_modeler <- ""
full_dt <- as.data.table(copy(orig_dt2a[measure %in% c("prevalence")]))
full_dt$crosswalk_parent_seq <- ""

dt_sex_split <- split_both_sex(full_dt, sex_results)
final_sex_dt <- copy(dt_sex_split$final)
graph_sex_dt <- copy(dt_sex_split$graph)

write.csv(final_sex_dt, paste0(mrbrt_sexratio_dir, "/sex_split.csv"), row.names = FALSE)

sex_graph <- graph_sex_predictions(graph_sex_dt)
sex_graph
ggsave(filename = paste0(mrbrt_sexratio_dir, "/sex_graph.pdf"), plot = sex_graph, width = 6, height = 6)

###################################################################################################
# STEP 3: CROSSWALK BLOOD DONOR DATA
##################################################################################################

# Get ref:alt pairs (general pop:blood donors) --------------------------------------------------------
gen_bd <- as.data.table(read.xlsx(paste0("FILEPATH"), sheet = "final_matches"))

z <- qnorm(0.975)
gen_bd[is.na(mean.x), mean.x := cases.x/sample_size.x]
gen_bd[is.na(standard_error.x), standard_error.x := sqrt(mean.x*(1-mean.x)/sample_size.x + z^2/(4*sample_size.x^2))]
gen_bd[is.na(mean.y), mean.y := cases.y/sample_size.y]
gen_bd[is.na(standard_error.y), standard_error.y := sqrt(mean.y*(1-mean.y)/sample_size.y + z^2/(4*sample_size.y^2))]


# Logit-transform mean and SE
gen_bd[, c("ref_mean", "ref_se")] <- cw$utils$linear_to_logit(
  mean = array(gen_bd$mean.x), 
  sd = array(gen_bd$standard_error.x))
gen_bd[, c("alt_mean", "alt_se")] <- cw$utils$linear_to_logit(
  mean = array(gen_bd$mean.y), 
  sd = array(gen_bd$standard_error.y))
gen_bd <- gen_bd %>%
  mutate(
    ldiff = alt_mean - ref_mean,
    ldiff_se = sqrt(alt_se^2 + ref_se^2))

#Create indicators
gen_bd$cv_blood_donor <- 1
gen_bd$dorm_alt <- "blood_donor"
gen_bd$dorm_ref <- "general_pop"


# MR-BRT for blood donors
matches_bd <- as.data.table(gen_bd)
matches_bd[, study_id := .GRP, by = c("nid.x", "nid.y")]

model_crosswalk_name <- paste0("blood_donor_logit_5-year_NoTrim_", date)
if (!dir.exists(paste0(mrbrt_crosswalk_dir,"/", model_crosswalk_name))) dir.create(paste0(mrbrt_crosswalk_dir,"/", model_crosswalk_name))
logit_transform <- T

formatted_data_bd <- cw$CWData(
  df = matches_bd,
  obs = "ldiff",
  obs_se = "ldiff_se",
  alt_dorms = "dorm_alt",
  ref_dorms = "dorm_ref",
  covs = list(),            # list of (potential) covariate column names
  study_id = id_vars)

# RUN MRBRT MODEL 
results_bd <- cw$CWModel(cwdata = formatted_data_bd,
                         obs_type = mrbrt_response,
                         cov_models = list(cw$CovModel("intercept")),
                         gold_dorm = "general_pop")                         

results_bd$fit()

results_bd$fixed_vars
results_bd$beta_sd
results_bd$gamma

save <- save_model_RDS(results_bd, "FILEPATH")
save_r <- save_mrbrt(results_bd, "FILEPATH")


# Create plots for vetting
graphs_modelfit <- graph_combos(model = results)
graphs_modelfit      


# ADJUST ORIGINAL DATA (POST SEX-SPLIT) OF BLOOD DONORS TOWARDS REFERENCE  -------------------------------------------------
full_dt_xw_bd <- subset(full_dt, cv_blood_donor==1 )
full_dt_noxw <- subset(full_dt,  cv_blood_donor==0 )

full_dt_xw_bd$definition[full_dt_xw_bd$cv_blood_donor==1] <- "blood_donor"
adjusted_bd <- make_adjustment(results_bd, full_dt_xw_bd)
epidb_bd <- copy(adjusted_bd$epidb)
test3 <- epidb_bd[, c("nid", "year_start", "year_end", "age_start", "age_end", "sex", "mean", "cases", "sample_size", "cv_blood_donor","cv_pregnant", "cv_outbreak", "extractor")]

full_data <- as.data.table(rbind.fill(epidb_bd,full_dt_noxw ))

full_data[(mean>1), mean:=1]

cw_file_path <- paste0("FILEPATH")
write.csv(full_data, cw_file_path, row.names = FALSE)

# Create plots for vetting
vetting <- copy(adjusted_bd$vetting_dt)
vetting_plots <- prediction_plot(vetting)
vetting_plots


######################################################################################################  
# Step 4. AGE-SPLITTING USING AGE PATTERNS OBSERVED IN AGE-SPECIFIC DATA POINTS
######################################################################################################  
# Subset data points with age range under 25 years ---------------------------------------------------------
age_pattern <- as.data.table(copy(full_data))
age_pattern <- age_pattern[, age_range := age_end - age_start ]
age_25 <- age_pattern[age_range <= 25,]
age_25_over <- age_pattern[age_range > 25,]

unique(age_25$sex)
age_25$standard_error[age_25$standard_error>1] <- 1
age_25[(group_review==""), `:=` (specificity=NA, group= NA)]

write.xlsx(age_25, "FILEPATH", sheetName = "extraction", colNames=TRUE)


#upload it as a crosswalk version
description <- "DESCRIPTION"
result <- save_crosswalk_version("OBJECT", "FILEPATH", description=description)


# GET OBJECTS -------------------------------------------------------------
id <- "OBJECT"
ver_id <- "OBJECT" 
description <- paste0("DESCRIPTION")

# RUN AGE-SPLITTING CODE---------------------------------------------------
ages <- get_age_metadata(release_id ="OBJECT")
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age <- ages[age_start >= 0, age_group_id]

df <- copy(age_25_over)
gbd_id <- id

region_pattern <- T
location_pattern_id <- 1

## GET TABLES ---------------------------------------------------------------
sex_names <- get_ids(table = "sex")
ages[, age_group_weight_value := NULL]
ages[age_start >= 1, age_end := age_end - 1]
ages[age_end == 124, age_end := 99]
super_region_dt <- get_location_metadata(location_set_id = "OBJECT", release_id ="OBJECT")
super_region_dt <- super_region_dt[, .(location_id, super_region_id)]

## SAVE ORIGINAL DATA  ---------------------------------------------------------------
original <- copy(df)
original[, id := 1:.N]

## FORMAT DATA ---------------------------------------------------------------
dt <- get_cases_sample_size(original)
dt <- get_se(dt)
dt <- calculate_cases_fromse(dt)
dt <- format_data(dt, sex_dt = sex_names)

## EXPAND AGE ---------------------------------------------------------------
split_dt <- expand_age(dt, age_dt = ages)

## GET PULL LOCATIONS ---------------------------------------------------------------
if (region_pattern == T){
  message("Splitting by region pattern")
  split_dt <- merge(split_dt, super_region_dt, by = "location_id")
  super_regions <- unique(split_dt$super_region_id) ##get super regions for dismod results
  locations <- super_regions
} else {
  locations <- location_pattern_id
  message("Splitting by global pattern")
}

##GET LOCS AND POPS ---------------------------------------------------------------
pop_locs <- unique(split_dt$location_id)
pop_years <- unique(split_dt$year_id)

## GET AGE PATTERN  ---------------------------------------------------------------
print("getting age pattern")
age_pattern <- get_age_pattern(locs = locations, id = gbd_id, age_groups = age) #set desired super region here if you want to specify

if (region_pattern == T) {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id", "super_region_id"))
} else {
  age_pattern1 <- copy(age_pattern)
  split_dt <- merge(split_dt, age_pattern1, by = c("sex_id", "age_group_id", "measure_id"))
}

## GET POPULATION INFO  ---------------------------------------------------------------
print("getting pop structure")
pop_structure <- get_pop_structure(locs = pop_locs, years = pop_years, age_group = age)
split_dt <- merge(split_dt, pop_structure, by = c("location_id", "sex_id", "year_id", "age_group_id"))

## CALCULATE AGE SPLIT POINTS ---------------------------------------------------------------
print("splitting data")
split_dt <- split_data(split_dt)


## AGE SPLIT ---------------------------------------------------------------
final_age_dt <- format_data_forfinal(split_dt, location_split_id = location_pattern_id, region = region_pattern,
                                     original_dt = original)


final_age_dt <- final_age_dt[standard_error >1, standard_error := NA]
final_age_dt <- final_age_dt[location_id != "OBJECT", ]
final_age_dt[!is.na(crosswalk_parent_seq), seq := NA]
locs <- get_location_metadata(location_set_id = "OBJECT", release_id ="OBJECT")
gbr <- locs[grepl("GBR_", ihme_loc_id) & level == 5, unique(location_id)]
final_age_dt <- final_age_dt[!(location_id %in% gbr)]
length(unique(final_age_dt$nid)) 
length(unique(age_25_over$nid))

final <- as.data.table(rbind.fill(final_age_dt, age_25))
final <- final[standard_error >1, standard_error := NA]

file_path <- paste0("FILEPATH")
write.xlsx(final , file_path, sheetName = "extraction", rowNames = FALSE)


####################################################################################################################################################
#STEP 5: UPLOAD DATA TO DATABASE FOR MODELING 
####################################################################################################################################################
description <- paste0("DESCRIPTION")
result <- save_crosswalk_version( bundle_version_id_new, file_path, description)

