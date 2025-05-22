####################################################
## Author: USER
## Date: DATE
## Description: Crosswalk Data Processing - NECK
####################################################

rm(list=ls())
user<- 'USER'

# Setting directories
if (Sys.info()[1] == "Linux"){
  root <- "FILEPATH"
  root <- "FILEPATH"
} else if (Sys.info()[1] == "Windows"){
  root <- "FILEPATH"
  root <- "FILEPATH"
}

library(readxl)
library(data.table)
library(ggplot2)
library(dplyr)
library(tidyr)
library(Hmisc)
library(mvtnorm)
library(survival, lib.loc = paste0("FILEPATH"))
library(expm)
library(msm)
library(openxlsx)
library(metafor, lib.loc = "FILEPATH")
library(mortdb, lib = "FILEPATH")

mr_brt_dir <- "FILEPATH"
source(paste0(mr_brt_dir, "cov_info_function.R"))
source(paste0(mr_brt_dir, "run_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_outputs_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_outputs_function.R"))
source(paste0(mr_brt_dir, "predict_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_preds_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_preds_function.R"))
source(paste0(mr_brt_dir, "plot_mr_brt_function.R"))

functions_dir <- "FILEPATH"
functs <- c('get_location_metadata', 'get_population','get_age_metadata', 
            'get_ids', 'get_outputs','get_draws', 'get_cod_data',
            'get_bundle_data', 'upload_bundle_data', 'get_bundle_version', 
            'save_bundle_version', 'get_crosswalk_version', 'save_crosswalk_version', 'get_elmo_ids')
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))
library(crosswalk, lib.loc = "FILEPATH")

date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)
# functions
source("FILEPATH")

# directories
plot_dir<- paste0('FILEPATH', date)
flat_file_dir<- paste0('FILEPATH', date)

dir.create(plot_dir)
dir.create(flat_file_dir)

# objects
loc_data <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 7))
loc_data <- loc_data[, c("location_id", "location_name", "ihme_loc_id", "region_name", "super_region_name")]

new<- data.table(read.xlsx("FILEPATH"))
new_nids<- unique(new$nid)

# run data prep -------------------------------------------
np <- as.data.table(get_bundle_version(34667, export = F, transform = T, fetch= 'all')) #saved DATE


np[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
np[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan := 1]
np[field_citation_value %like% "Truven Health" & year_start >= 2016, cv_marketscan_2016 := 1] # added for new xw
np[field_citation_value %like% "Taiwan National Health Insurance", cv_taiwan_claims_data := 1]
np[, "crosswalk_parent_seq" := as.numeric(NA)]

np <- np[is_outlier != 1 | is.na(is_outlier)]
np[, GBD := ifelse(((!is.na(case_definition) & (grepl("current", case_definition, ignore.case = T) | grepl("present", case_definition, ignore.case = T))) | (!is.na(specificity) & grepl("point", specificity, ignore.case = T))) & is.na(cv_broad) & is.na(cv_pop_school) & is.na(cv_recall_ever) & is.na(cv_recall_week) & is.na(cv_recall_month), "GBD_", NA)]
np[, chronic := ifelse(!is.na(case_definition) & grepl("chronic", case_definition, ignore.case = T), "chronic_", NA)] #original line
np[, students := ifelse(!is.na(cv_pop_school) & cv_pop_school == 1 |  !is.na(cv_students) & cv_students == 1, "students_", NA)] #added cv_students line
np[, recall1mless := ifelse(!is.na(cv_recall_week) & cv_recall_week == 1, "recall1mless_", NA)]
np[, recall1yrless := ifelse(!is.na(cv_recall_month) & cv_recall_month == 1, "recall1yrless_", NA)]
np[, recallLifetime := ifelse(!is.na(cv_recall_ever) & cv_recall_ever == 1, "recallLife_", NA)] #original line
np[, activityLimit := ifelse((!is.na(cv_beh_activity) & cv_beh_activity ==1) |(!is.na(cv_al) & cv_al ==1)  | (!is.na(case_definition) & grepl("activity*limit*", case_definition, ignore.case = T)), "activityLimit_", NA)] #added cv_al line
np[, anatBroad := ifelse(!is.na(cv_broad) & cv_broad == 1, "anatBroad_", NA)]
np[, taiwanClaims := ifelse(!is.na(cv_taiwan_claims_data) & cv_taiwan_claims_data == 1, "taiwanClaims", NA)]
np[, marketscan2000 := ifelse(!is.na(cv_marketscan_all_2000) & cv_marketscan_all_2000 == 1, "marketscan2000", NA)]
np[, marketscan2016 := ifelse(!is.na(cv_marketscan_2016) & cv_marketscan_2016 == 1, "marketscan2016", NA)] #added for new crosswalk
np[, marketscan2010 := ifelse(!is.na(cv_marketscan) & is.na(cv_marketscan_2016) & cv_marketscan == 1, "marketscan2010", NA)]

np <- np[is.na(recallLifetime)]
np[, case_def := paste2(GBD, chronic, students, recall1mless, recall1yrless, activityLimit, anatBroad, marketscan2016, sep = "")] 

np[, case_def_1 := paste2(GBD, recall1mless, recall1yrless, activityLimit, sep = "")] # for adjustments using first network
np[, case_def_2 := paste2(GBD, chronic, students, anatBroad, marketscan2016, sep = "")] # for adjustments using second network


unique(np[case_def== '', field_citation_value]) #all of the claims sources, going to outlier everything but Taiwan in final model


# merge location data
np[, `:=` (location_name= NULL, ihme_loc_id= NULL)]
np <- merge(np, loc_data, by = "location_id")
np$country <- substr(np$ihme_loc_id, 0, 3)

# fill out mean/cases/SS
np$cases <- as.numeric(np$cases)
np$sample_size <- as.numeric(np$sample_size)
np <- get_cases_sample_size(np)
np <- get_se(np)
orig <- as.data.table(copy(np))


# sex split ----------
test <- find_sex_match(np)
test2 <- calc_sex_ratios(test)

model_name <- paste0("FILEPATH", date)

sex_model <- run_mr_brt(
  output_dir = "FILEPATH",
  model_label = model_name,
  data = test2,
  mean_var = "log_ratio",
  se_var = "log_se",
  study_id = "id",
  method = "trim_maxL",
  trim_pct = 0.1,
  overwrite_previous = T
)

plot_mr_brt(sex_model)

# age-sex split -----
test <- age_sex_split(np)

# apply sex split ----------
predict_sex <- split_data(test, sex_model)
pdf(paste0(plot_dir, 'FILEPATH'))
graph_predictions(predict_sex$graph)
dev.off()
# write sex split files
write.xlsx(predict_sex$final, paste0(flat_file_dir, 'FILEPATH'))
write.xlsx(predict_sex$graph, paste0(flat_file_dir, 'FILEPATH'))
test <- copy(predict_sex$final)


# run crosswalks (skip if you have a model saved)   -------------------------------
test<- read.xlsx(paste0(flat_file_dir, 'FILEPATH'))
#test<- read.xlsx(paste0(flat_file_dir, 'adjusted_input_dataset.xlsx'))
np <- as.data.table(copy(test))
orig <- as.data.table(copy(np))

np<- copy(orig)


claims_data <- np[clinical_data_type == "claims"]
np <- np[clinical_data_type != "claims"]
claims_data <- aggregate_marketscan(claims_data)
claims_data <- claims_data[location_name == "United States" | location_name == "Taiwan"]
np <- rbind(np, claims_data)
np <- get_cases_sample_size(np)
np <- get_se(np)


np <- np[, c("nid", "sex", "year_start", "year_end", "age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "country", "region_name", "super_region_name", "measure", "mean", "lower", "upper", "standard_error", "case_def")]

# make matches
np[, demographics := paste2(sex, country, measure, sep = "_")]

np <- np[case_def != ""]

np_cds <- list(unique(np$case_def))

np[, year_mean := (year_start + year_end)/2]
np[, age_mean := (age_start + age_end)/2]

np$case_def <- as.factor(np$case_def)
#np$case_def <- as.character(np$case_def)

# remove the extra variables
levels(np$case_def)
np$case_def <- factor(np$case_def, levels(np$case_def)[c(6, 1:5, 7:14)])


np <- np[order(case_def)]

list2 <- lapply(unique(np$case_def), function(i) {
  subset(np, case_def == i)
})

system.time({
  for (i in 1:(length(list2)-1)) {
    for (j in (i+1):length(list2)) {
      name <- paste0("paired_", gsub(" ", "_", unique(list2[[i]]$case_def)), "_", gsub(" ", "_", unique(list2[[j]]$case_def)))
      assign(name, as.data.table(merge(list2[[i]], list2[[j]], by = c("demographics"), all.x = F, suffixes = c(".denom", ".num"), allow.cartesian = T)))
    }
  }
})

pairss <- grep("paired", names(.GlobalEnv), value = T)
rm_pairss <- c() # clear out matched cds with 0 observations
for (i in pairss) {
  if (nrow(get(i)) < 1) {
    rm_pairss <- c(rm_pairss, paste0(i))
  }
}
rm(list = rm_pairss, envir = .GlobalEnv)
pairss <- grep("paired", names(.GlobalEnv), value = T)

np_matched <- copy(paired_anatBroad__chronic_recall1yrless_)
np_matched <- np_matched[0,]

for (i in 1:length(pairss)) {
  np_matched <- rbind(np_matched, get(pairss[i]))
}


nrow(np_matched[nid.denom == nid.num])
nrow(np_matched[nid.denom != nid.num])


np_matched <- np_matched[abs(year_mean.denom - year_mean.num) < 15]
np_matched <- np_matched[abs(age_mean.denom - age_mean.num) < 15]


# CALCULATE RATIO AND STANDARD ERROR
np_matched <- np_matched %>%
  mutate(
    ratio = mean.num / mean.denom,
    ref_se = standard_error.denom, #(upper.denom - lower.denom) / 3.92,
    alt_se = standard_error.num, #(upper.num - lower.num) / 3.92,
    ratio_se =
      sqrt((mean.num^2 / mean.denom^2) * (alt_se^2/mean.num^2 + ref_se^2/mean.denom^2))
  )

np_matched <- as.data.table(np_matched)
np_matched[, id := paste0(nid.num, " (", country.num, ": ", sex.num, " ", age_start.num, "-", age_end.num, ") - ", nid.denom, " (", country.denom, ": ", sex.denom, " ", age_start.denom, "-", age_end.denom, ")")]
np_matched[, id_var := paste0(nid.num, ":", nid.denom)]

# write files
write.xlsx(np_matched, paste0(flat_file_dir, "FILEPATH"))

# CREATE DUMMY VARIABLES
matched<- copy(data.table(np_matched))
nrow(matched) 
matched[, pair:= paste0(case_def.num, " : ", case_def.denom)]
unique(matched$pair)
# no 0s to drop
matched <- matched[!mean.num==0] 
matched <- matched[!mean.num==1] 

matched <- matched[!mean.denom==0]
matched <- matched[!mean.denom==1] #no data dropped



# drop inter study comparison
matched<- matched[nid.num== nid.denom]


#get log calcs using the delta transform package (transform mean and SE into log-space)
logit_alt_means <- as.data.table(delta_transform(mean=matched$mean.num, sd=matched$standard_error.num, transformation='linear_to_logit'))
setnames(logit_alt_means, c('mean_logit', 'sd_logit'), c('logit_alt_mean', 'logit_alt_se'))
logit_ref_means <- as.data.table(delta_transform(mean=matched$mean.denom, sd=matched$standard_error.denom, transformation='linear_to_logit'))
setnames(logit_ref_means, c('mean_logit', 'sd_logit'), c('logit_ref_mean', 'logit_ref_se'))


#bind back onto main data table
matched <- cbind(matched, logit_alt_means)
matched <- cbind(matched, logit_ref_means)

# use calculate_diff() to calculate the logit difference between matched pairs
matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = matched, 
  alt_mean = "logit_alt_mean", alt_sd = "logit_alt_se",
  ref_mean = "logit_ref_mean", ref_sd = "logit_ref_se" )


setnames(matched, c('case_def.denom', 'case_def.num'), c('case_def_denom', 'case_def_num'))


matched$case_def_denom<- as.character(matched$case_def_denom)
matched$case_def_num<- as.character(matched$case_def_num)

matched$case_def_denom <- sapply(matched$case_def_denom, take_off_trailing_underscore)
matched$case_def_num <- sapply(matched$case_def_num, take_off_trailing_underscore)


# write a flat file 
write.xlsx(matched, paste0("FILEPATH"))

# RUN MODEL ------------------------------------------------------------------------------------------------
matched<-data.table(read.xlsx(paste0("FILEPATH")))
dat1 <- CWData(
  df = matched,                  # dataset for metaregression
  obs = "logit_diff",            # column name for the observation mean
  obs_se = "logit_diff_se",      # column name for the observation standard error
  alt_dorms = "case_def_num",         # column name of the variable indicating the alternative method
  ref_dorms = "case_def_denom",         # column name of the variable indicating the reference method
  dorm_separator = "_",      
  covs = list(),             # names of columns to be used as covariates later
  study_id = "id_var",           # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE
)
fit_full_logit <- CWModel(
  cwdata = dat1,
  obs_type = "diff_logit",
  cov_models = list(CovModel("intercept")),
  inlier_pct = 0.9,
  gold_dorm = "GBD"
)

fit_intra_log$fixed_vars
fit_full_log$fixed_vars
fit_intra_logit$fixed_vars
fit_full_logit$fixed_vars


# save model results
fit2$fixed_vars
df_result <- fit_full_log$create_result_df()
write.csv(df_result, paste0(flat_file_dir, 'FILEPATH'))
py_save_object(object = fit2, filename = paste0(flat_file_dir, 'FILEPATH'), pickle = "dill")
py_save_object(object = fit_intra_log, filename = paste0(flat_file_dir, 'FILEPATH'), pickle = "dill")
py_save_object(object = fit_full_log, filename = paste0(flat_file_dir, 'FILEPATH'), pickle = "dill")
py_save_object(object = fit_intra_logit, filename = paste0(flat_file_dir, 'FILEPATH'), pickle = "dill")
py_save_object(object = fit_full_logit, filename = paste0(flat_file_dir, 'FILEPATH'), pickle = "dill")


  
# adjust data ------------------------------------------------------------------------------------
flat_file_dir<- paste0('FILEPATH')
    
test <- data.table(read.xlsx(paste0(flat_file_dir, 'FILEPATH'))) #  data you will adjust


fit_1<- py_load_object(filename =  paste0('FILEPATH'), pickle = "dill") #  adjust using this model first
fit_2<- py_load_object(filename =  paste0('FILEPATH'), pickle = "dill") #  adjust using this model second

dt<- fit_1$create_result_df()    
# bring in data to adjust
test_2<- data.table(copy(test))

# adjust recall period and activity limit covs using the first network, the rest via the second
setnames(test_2, 'case_def', 'full_case_def')
setnames(test_2, 'case_def_1', 'case_def')

test_2[is.na(mean_orig), mean_orig:= mean] #  make a column for the original mean so we can keep track of it
test_2[, sd_original:= standard_error]     #make column for original standard error to track changes
post_intra<- adjust_data(test_2, fit_1)    #  adjust the first combinations of vars

setnames(post_intra, "case_def", "case_def_1")
setnames(post_intra, "case_def_2", "case_def")

post_inter<- adjust_data(post_intra, fit_2) # adjust data with the second network

adjusted_log<- copy(post_inter)

# adjustment function 
adjust_data<- function(dt, fit){
  dt$case_def<- unlist(dt$case_def)
  dt$case_def<- lapply(dt$case_def, take_off_trailing_underscore)
  no_adjust<- dt[case_def== ""]         
  adjust<- dt[case_def!= '' & mean!= 0] #data to run adjust_orig_vals on
  zero<- dt[case_def!= '' & mean==0]    #zeros are adjusted separately

  # adjust non-zero data
  preds1 <- adjust_orig_vals(
    fit_object = fit, # object returned by `CWModel()`
    df = adjust,
    orig_dorms = "case_def",
    orig_vals_mean = "mean",
    orig_vals_se = "standard_error"
    )
  # bind back onto original dataset
  adjust[, c("meanvar_adjusted", "sdvar_adjusted", "adjustment_log", "adjustment_se_log", "data_id")] <- preds1

  # adjust data where mean= 0
  case_defs<- unique(zero$case_def)
  for (def in case_defs) {
    adj_log<- unique(adjust[case_def== def, adjustment_log])
    adj_se_log<- unique(adjust[case_def== def, adjustment_se_log])
    zero[case_def== def, `:=` (sdvar_adjusted= sqrt(standard_error^2 + (exp(adj_se_log))^2),
                             note_modeler= paste0(note_modeler, ' | inflated uncertainty using prediction SE: ', " (", round(adj_se_log, 2), ")"),
                             adjustment_log= adj_log, adjustment_se_log= adj_se_log )]
    zero[, meanvar_adjusted:= 0]
    }
  
  # bind zero data back onto dataset
  adjusted<- rbind(adjust, zero, fill= TRUE)
  adjusted[is.na(mean_orig), mean_orig:= mean]
  # make the adjusted values the mean values
  adjusted[, `:=` (note_modeler= paste0(note_modeler, ' | adjusted by log difference: ', 
                                      round(adjustment_log, 2), " (", round(adjustment_se_log, 2), ")"), 
                mean= meanvar_adjusted, standard_error= sdvar_adjusted, data_id= NULL, 
                cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA,
                meanvar_adjusted= NULL, sdvar_adjusted= NULL, adjustment_log= NULL, adjustment_se_log= NULL)]
  
  # bind non-adjust data back onto dataset
  full_df<- rbind(adjusted, no_adjust, fill= TRUE)
  return(full_df)
  }
# now we have a full crosswalked dataset to submit to the age-pattern modelable entity ID
# save this as a flat file
write.xlsx(post_inter, paste0(flat_file_dir, 'FILEPATH'))


# age split the full dataset  -----
adjusted_log[, `:=`(age_group_id= NULL, age_group_name= NULL, population= NULL)]

dt <- data.table(copy(adjusted_log))
ages <- get_age_metadata(12, gbd_round_id= 6)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[, age_group_id]
age<- age_groups
id <- 2154 #neck pain modelable entity ID

df<- dt
gbd_id<- id
age<- age_groups

final_split <- age_split(gbd_id = id, df = dt, age = age_groups, region_pattern = F, location_pattern_id = 1) 
nrow(post_inter)
nrow(final_split)




# merge on outliers from last year  --------
to_merge<- copy(final_split)
last_year<- get_crosswalk_version(12041)
nrow(last_year[is_outlier== 1])
outliers<- last_year[is_outlier== 1 & !(note_modeler %like% 'MAD')] 
outliers<- outliers[!field_citation_value %like% 'Truven Health'] 
nrow(outliers) 

outliers<- last_year[, c('nid', 'age_start', 'age_end', 'sex', 'year_start',
                         'year_end', 'measure', 'location_id', 'is_outlier')]
to_merge[, is_outlier:= NULL]
merged<-left_join(to_merge, outliers) 
merged<- data.table(merged)
merged[is.na(is_outlier), is_outlier:= 0]


# drop Polish claims and marketscan data (except 2016-2017)
merged<- merged[!field_citation_value %like% '(Poland)']
merged<- merged[!(field_citation_value %like% 'Truven Health' & !year_start %in% c(2016, 2017))]


# mad outlier  ----
outlier_val<- 2
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) # new in GBD 2020 step 1

byvars <- c("location_id", "sex", "year_start", "year_end", "nid") 

# get age weights
all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19, gbd_round_id=7))
all_fine_ages[, age_start := age_group_years_start]

##make a set to be run through outlier script
to_outlier <- copy(merged)

pre_outliers<- nrow(to_outlier[is_outlier== 1])
print(pre_outliers)

not_mad <- subset(to_outlier, measure != "prevalence") 
dt_inp<- subset(to_outlier, measure == "prevalence") 

##merge age table map and merge on to dataset
dt_inp <- merge(dt_inp, all_fine_ages, by = c("age_start"), all.x = T)
unique(dt_inp$note_modeler)
##create new age-weights for each data source
dt_inp[, sum := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one
dt_inp[, new_weight := age_group_weight_value/sum, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their location-age-sex-nid group

##age standardizing per location-year by sex
#add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
dt_inp[, as_mean := mean * new_weight] #initially just the weighted mean for that AGE-location-year-sex-nid
dt_inp[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series

##mark as outlier if age standardized mean is 0 (either biased data or rare disease in small population)
dt_inp[as_mean == 0, is_outlier := 1]
dt_inp[as_mean == 0, note_modeler := paste0(note_modeler, " | outliered this location-year-sex-NID age-series because age standardized mean is 0")]

## log-transform to pick up low outliers
dt_inp[as_mean != 0, as_mean := log(as_mean)]

# calculate median absolute deviation
dt_inp[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
dt_inp[,mad:=mad(as_mean,na.rm = T),by=c("sex", "super_region_name")]
dt_inp[,median:=median(as_mean,na.rm = T),by=c("sex", "super_region_name")]

#***can change number of MAD to mark here
dt_inp[as_mean>((outlier_val*mad)+median), is_outlier := 1]
dt_inp[as_mean>((outlier_val*mad)+median), note_modeler := paste0(note_modeler, " | outliered because age-standardized mean for location-year-sex-NID is higher than ", outlier_val," MAD above median")]
dt_inp[as_mean<(median-(outlier_val*mad)), is_outlier := 1]
dt_inp[as_mean<(median-(outlier_val*mad)), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is lower than ", outlier_val," MAD below median")]
dt_inp[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id", "super_region_name") := NULL]


print(paste((nrow(dt_inp[is_outlier == 1])- pre_outliers), "points were outliered with", outlier_val, "MAD"))
percent_outliered <- round(((nrow(dt_inp[is_outlier == 1])- pre_outliers) / nrow(dt_inp))*100, digits = 1)
print(paste("outliered", percent_outliered, "% of data"))
dropped_locs <- setdiff(unique(merged$country), unique(dt_inp[is.na(is_outlier)]$country))
print(paste("Dropped ", length(dropped_locs), " countries from model:", paste(dropped_locs, collapse = " ")))

dt_inp[is.na(lower), uncertainty_type_value := NA]

dt_inp[measure== 'prevalence' & standard_error > 1, standard_error := 1]

upload<-rbind(dt_inp, not_mad, fill= TRUE)
outliered<- upload[is_outlier== 1]
print(nrow(outliered)/nrow(upload)) 
write.xlsx(upload, paste0(flat_file_dir, 'FILEPATH'), sheetName= "extraction")

# save this as a flat file
final<- copy(upload)
final<- final[is.na(group_review) | group_review== 1]
final[is.na(crosswalk_parent_seq), crosswalk_parent_seq:= seq]
final[, seq:= '']
final<-final[sex!= 'Both']
final[, cv_marketscan_2016:= NULL]
final[location_id == 4622, `:=` (location_name = "England", location_id = 4749, ihme_loc_id = "GBR_4749")]
final[measure == "prevalence" & upper > 1, `:=` (lower = NaN, upper = NaN, uncertainty_type_value = NA)]
final[, `:=` (group = NA, specificity = NA, group_review = NA)]
final[is.na(upper), `:=` (lower = NA, uncertainty_type_value = NA)]
final[!is.na(upper), uncertainty_type_value := 95]
final[upper > 1, `:=` (upper = NaN, lower = NaN, uncertainty_type_value = NA)]
final[standard_error> 1, standard_error:= 1]
final[nid== 371853 & mean> 1, mean:= 1]

# outlier new outliers 
outliers<- c(109248, 109254, 109247, 109233, 109248, 109254, 403934, 440451, 440453, 440583, 371852, 440541, 440557, 440543)
final[nid %in% outliers,`:=` (is_outlier=1, note_modeler= paste0(note_modeler, ' | outliering high data for GBD 2020'))]

# drop Polish claims and marketscan data (except 2016-2017)
final<- final[!field_citation_value %like% '(Poland)']
final<- final[!(field_citation_value %like% 'Truven Health' & !year_start %in% c(2016, 2017))]
#final[field_citation_value %like% '(Taiwan)', `:=` (is_outlier=1, note_modeler= paste0(note_modeler, " | outliered claims data for GBD 2020"))] #we did not outlier this data last round so leave in
# drop the Philipinnes data
final<- final[!location_name %like% "Philippines"]

write.xlsx(final, paste0(flat_file_dir, 'FILEPATH'), sheetName= "extraction")

save_crosswalk_version(34667, paste0(flat_file_dir, "FILEPATH") , 
                       description = "fixed duplicates outliered Brazil Solomon Islands Iran")
xw_id<- 27734



save_crosswalk_version(29996, paste0(flat_file_dir, "FILEPATH") , 
                       description = "MAD outlier +/- 2")
xw_id<- 24707 


