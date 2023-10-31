####################################################
## Author: USER
## Date: DATE
## Description: Crosswalk Data Processing - LBP
## Modified by: USER for GBD 2020
####################################################


# setup ------------------
rm(list=ls())
user<- 'USER'
if (Sys.info()[1] == "Linux"){
  root <- "FILEPATH"
  root <- "FILEPATH"
} else if (Sys.info()[1] == "Windows"){
  root <- "FILEPATH"
  root <- "FILEPATH"
}

# packages
library(readxl)
library(data.table)
library(ggplot2)
library(dplyr)
library(plyr)
library(tidyr)
library(Hmisc, lib.loc = paste0("FILEPATH"))
library(mvtnorm)
library(survival, lib.loc = paste0("FILEPATH"))
library(msm)
library(openxlsx)
library(metafor, lib.loc = "FILEPATH")
library(mortdb, lib = "FILEPATH")
library(crosswalk, lib.loc = "FILEPATH")
library(gtools)
library(arm)

# functions to source
mr_brt_dir <- "FILEPATH"
source(paste0(mr_brt_dir, "cov_info_function.R"))
source(paste0(mr_brt_dir, "run_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_outputs_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_outputs_function.R"))
source(paste0(mr_brt_dir, "predict_mr_brt_function.R"))
source(paste0(mr_brt_dir, "check_for_preds_function.R"))
source(paste0(mr_brt_dir, "load_mr_brt_preds_function.R"))
source(paste0(mr_brt_dir, "plot_mr_brt_function.R"))
source('FILEPATH')
source('FILEPATH')

# shared functions
shared_functions_dir <- "FILEPATH"
functions_dir <- "FILEPATH"
functs <- c('get_location_metadata', 'get_population','get_age_metadata', 
            'get_ids', 'get_outputs','get_draws', 'get_cod_data', 'get_elmo_ids',
            'get_bundle_data', 'upload_bundle_data', 'get_bundle_version', 
            'save_bundle_version', 'get_crosswalk_version', 'save_crosswalk_version', 'save_bulk_outlier')
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

# directories
plot_dir<- paste0('FILEPATH', date)
flat_file_dir<- paste0('FILEPATH', date)

dir.create(plot_dir)
dir.create(flat_file_dir)

# objects --------------
date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)

loc_data <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 7))
loc_data <- loc_data[, c("location_id", "location_name", "ihme_loc_id", "region_name", "super_region_name")]




# load data and tag covs  ---------------

lbp <- as.data.table(get_bundle_version(ID, export = F, transform = T, fetch= 'all')) #saved DATE

lbp[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
lbp[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan := 1]
lbp[field_citation_value %like% "Taiwan National Health Insurance", cv_taiwan_claims_data := 1]
lbp[, "crosswalk_parent_seq" := as.numeric(NA)]

lbp <- lbp[is_outlier != 1 | is.na(is_outlier)]
lbp <- lbp[is.na(drop) | drop != 1]
lbp[, ihme_loc_id:= NULL]

# read in input data NIDs to mark as new
extraction<- data.table(read.xlsx('FILEPATH'))
nids<- unique(extraction$nid)
lbp[nid %in% nids, new_2020:= 1] 
lbp[nid %in% nids & cv_al==1, cv_beh_activity:= 1]

# tag the covs  ----
lbp[, GBD := ifelse(((!is.na(case_definition) & (grepl("current", case_definition, ignore.case = T) | grepl("present", case_definition, ignore.case = T))) | (!is.na(specificity) & grepl("point", specificity, ignore.case = T))) & is.na(cv_broad) & is.na(cv_pop_school) & is.na(cv_recall_ever) & is.na(cv_recall_week) & is.na(cv_recall_month), "GBD_", NA)]
lbp[, chronic := ifelse(!is.na(case_definition) & grepl("chronic", case_definition, ignore.case = T), "chronic_", NA)] #2019 tagging
lbp[, students := ifelse(!is.na(cv_pop_school) & cv_pop_school == 1 | !is.na(cv_students) & cv_students == 1, "students_", NA)] 
lbp[, recall1mless := ifelse(!is.na(cv_recall_week) & cv_recall_week == 1, "recall1mless_", NA)]
lbp[, recall1yrless := ifelse(!is.na(cv_recall_month) & cv_recall_month == 1, "recall1yrless_", NA)]
lbp[, recallLife := ifelse(!is.na(cv_recall_ever) & cv_recall_ever == 1, "recallLife_", NA)] #original line
lbp[, activityLimit := ifelse((!is.na(cv_beh_activity) & cv_beh_activity ==1) | (!is.na(case_definition) & grepl("activity*limit*", case_definition, ignore.case = T)), "activityLimit_", NA)] #original line
lbp[, anatBroad := ifelse(!is.na(cv_broad) & cv_broad == 1 , "anatBroad_", NA)]
lbp[, marketscan2000 := ifelse(!is.na(cv_marketscan_all_2000) & cv_marketscan_all_2000 == 1, "marketscan2000", NA)]
lbp[, marketscan2010 := ifelse(!is.na(cv_marketscan) & cv_marketscan == 1, "marketscan2010", NA)]

lbp <- lbp[is.na(recallLife)]

# make a case definition column that can be used for crosswalking
lbp[, case_def := paste2(GBD, chronic, recall1mless, recall1yrless, recallLife, activityLimit, anatBroad, marketscan2000, marketscan2010, sep = "")]

# subset to only reference data
lbp<- lbp[case_def== "GBD_"]

# save a flat file
write.xlsx(lbp, "FILEPATH")
# add loc data
lbp[, location_name:= NULL]
lbp <- merge(lbp, loc_data, by = c("location_id"))
lbp$country <- substr(lbp$ihme_loc_id, 0, 3)

# fill out mean/cases/SS/SE 
lbp$cases <- as.numeric(lbp$cases)
lbp$sample_size <- as.numeric(lbp$sample_size)
lbp <- get_cases_sample_size(lbp)
lbp <- get_se(lbp)
orig <- as.data.table(copy(lbp))



# run sex-split model -----------------------------
test <- find_sex_match(lbp)
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

# age-sex split  -----------------------------------
lbp[, note_modeler_2019:= note_modeler]
lbp[, note_modeler:= '']
test <- age_sex_split(lbp)


# apply sex split ----------------------------------
predict_sex <- split_data(test, sex_model)
pdf(paste0("FILEPATH"))
graph_predictions(predict_sex$graph)
dev.off()
write.xlsx(predict_sex$final, paste0(flat_file_dir, 'FILEPATH'))
test <- copy(predict_sex$final)


# find matches for crosswalk (skip if you have a model) -----------------------------
test <- data.table(read.xlsx(paste0(flat_file_dir, 'FILEPATH')))


lbp <- as.data.table(copy(test)) 
orig <- as.data.table(copy(lbp))

lbp<- lbp[!case_def== ""]

claims_data <- lbp[clinical_data_type == "claims"]
lbp <- lbp[clinical_data_type != "claims"]
claims_data <- aggregate_marketscan(claims_data)
claims_data <- claims_data[location_name == "United States" | location_name == "Taiwan"] 
lbp <- rbind(lbp, claims_data)
lbp <- get_cases_sample_size(lbp)
lbp <- get_se(lbp)

lbp <- lbp[, c("nid", "sex", "year_start", "year_end", "age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "country", "region_name", "super_region_name", "measure", "mean", "lower", "upper", "standard_error", "case_def")]

# make matches by demos -------------------------
lbp[, demographics := paste0(sex, country, measure, sep = "_")] 
lbp2 <- copy(lbp)
lbp2 <- lbp2[!is.na(standard_error)] #only match studies with SE, did not drop any data
lbp_cds <- list(unique(lbp2$case_def)) #make a vector with case definitions
lbp2[, year_mean := (year_start + year_end)/2] #make variables for mean year and age
lbp2[, age_mean := (age_start + age_end)/2]


lbp2$case_def <- as.factor(lbp2$case_def) #32 different case definitions
levels(lbp2$case_def)
lbp2$case_def <- factor(lbp2$case_def, levels(lbp2$case_def)[c(11, 1:10, 12:19)]) #for test

lbp2 <- lbp2[order(case_def)]

list2 <- lapply(unique(lbp2$case_def), function(i) { #make a list separated by case definition
  subset(lbp2, case_def == i)
})

system.time({ #making pairs based on demographics
  for (i in 1:(length(list2)-1)) { #for every dataset grouped by case definition
    for (j in (i+1):length(list2)) { #for every row in the next dataset
      name <- paste0("paired_", gsub(" ", "_", unique(list2[[i]]$case_def)), "_", gsub(" ", "_", unique(list2[[j]]$case_def)))
      assign(name, as.data.table(merge(list2[[i]], list2[[j]], by = c("demographics"), all.x = F, suffixes = c(".denom", ".num"), allow.cartesian = T)))
    }
  }
})
pairss <- grep("paired", names(.GlobalEnv), value = T) #making a vector with all of the pair names


lbp_matched <- copy(paired_activityLimit__chronic_)

lbp_matched <- lbp_matched[0,]
for (i in 1:length(pairss)) {
  lbp_matched <- rbind(lbp_matched, get(pairss[i])) #make a total dataset with all pairs
}
orig<- copy(lbp_matched)

nrow(lbp_matched[nid.denom == nid.num]) #within study comparisons 
nrow(lbp_matched[nid.denom != nid.num]) #between study comparisons 

#test<- lbp_matched2[nid.denom == nid.num] #within study comparisons 2226 rows
lbp_matched<- copy(orig)
pair<-11 #pairfinding criteria you would like to set for crosswalks

lbp_matched <- lbp_matched[abs(year_mean.denom-year_mean.num) < pair]
lbp_matched <- lbp_matched[abs(age_start.denom - age_start.num) < pair & abs(age_end.denom - age_end.num) <pair] 


lbp_matched[, pair:= paste0(case_def.num, " : ", case_def.denom)]
nrow(lbp_matched[nid.denom == nid.num]) #within study pair
nrow(lbp_matched[nid.denom != nid.num]) #outside of study pair

# calc ratio and SE
lbp_matched <- lbp_matched %>%
  mutate(
    ratio = mean.num / mean.denom,
    ref_se =  standard_error.denom, #(upper.denom - lower.denom) / 3.92,
    alt_se = standard_error.num, #(upper.num - lower.num) / 3.92,
    ratio_se =
      sqrt((mean.num^2 / mean.denom^2) * (alt_se^2/mean.num^2 + ref_se^2/mean.denom^2))
  )
lbp_matched <- as.data.table(lbp_matched)
lbp_matched2 <- copy(lbp_matched)
lbp_matched2[, id := paste0(nid.num, " (", country.num, ": ", sex.num, " ", age_start.num, "-", age_end.num, ") - ", nid.denom, " (", country.denom, ": ", sex.denom, " ", age_start.denom, "-", age_end.denom, ")")]
lbp_matched2[, id_var := paste0(nid.num, ":", nid.denom)]

# write files
write.xlsx(lbp_matched2, paste0(flat_file_dir, "FILEPATH"))


# prep data for mrbrt --------------------------------------


matched<- copy(data.table(lbp_matched2))
nrow(matched) 

#Drop zeros and 1s because they will not logit transform 
matched <- matched[!mean.num==0] 
matched <- matched[!mean.num==1] 
matched <- matched[!mean.denom==0]
matched <- matched[!mean.denom==1] 

#get logit calcs using the delta transform package (transform mean and SE into logit-space)
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


# remove the end underscore from all case definitions
matched$case_def.denom<- as.character(matched$case_def.denom)
matched$case_def.num<- as.character(matched$case_def.num)

matched$case_def.denom <- sapply(matched$case_def.denom, take_off_trailing_underscore)
matched$case_def.num <- sapply(matched$case_def.num, take_off_trailing_underscore)


# write a flat file 
write.xlsx(matched, paste0(flat_file_dir, "FILEPATH"))


# RUN MODEL ------------------------------------------------------------------------------------------------
dat1 <- CWData(
  df = matched,                  # dataset for metaregression
  obs = "logit_diff",            # column name for the observation mean
  obs_se = "logit_diff_se",      # column name for the observation standard error
  alt_dorms = "case_def.num",         # column name of the variable indicating the alternative method
  ref_dorms = "case_def.denom",         # column name of the variable indicating the reference method
  dorm_separator = "_",      
  covs = list(),             # names of columns to be used as covariates later
  study_id = "id_var",           # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE
)

fit2 <- CWModel(
  cwdata = dat1,
  obs_type = "diff_logit",
  cov_models = list(CovModel("intercept")),
  inlier_pct = 0.9,
  gold_dorm = "GBD"
)

# check the outputs of the model
fit2$fixed_vars
fit2$gamma

# save the model results
df_result<- fit2$create_result_df()
py_save_object(object = fit2, filename = paste0(flat_file_dir, 'FILEPATH'), pickle = "dill")
write.csv(df_result, paste0(flat_file_dir, 'FILEPATH'))
fit2<- py_load_object(filename = paste0(flat_file_dir, 'FILEPATH'), pickle = "dill")


# funnel plots  -----
repl_python()
plots <- import("crosswalk.plots")
plots$funnel_plot(
  cwmodel = fit2, 
  cwdata = dat1,
  continuous_variables = list(),
  obs_method = 'marketscan2000',
  plot_note = 'Funnel plot LBP', 
  plots_dir = plot_dir, 
  file_name = "funnel_plot_marketscan2000",
  write_file = TRUE
)


# ADJUST DATA ------------------------------------------------------------------------------------

# load the data you will adjust (post sex-split) and the crosswalk model you will adjust it with
test <- data.table(read.xlsx(paste0(flat_file_dir, '/sex_split_data.xlsx')))
fit2<- py_load_object(filename = 'FILEPATH', pickle = "dill")
dt<- fit2$create_result_df()


# adjust
test_2<- data.table(copy(test))
final<- adjust_data(test_2, fit2)

# write file
write.xlsx(final, paste0('FILEPATH'))


adjust<- final
adjust<- adjust[case_def!=""]
adjust$case_def<- unlist(adjust$case_def)
adjust$case_def<- as.factor(adjust$case_def)
for (i in seq(1, length(unique(adjust$case_def)), 7)) {
  print(ggplot(data= adjust[adjust$case_def %in% levels(adjust$case_def)[i:(i+7)]], mapping= aes(x= mean_orig, y=mean))+
          geom_point() +
          theme_classic() +
          facet_wrap(~case_def) +
          geom_abline(slope = 1, intercept = 0,) +
          labs(title= paste0('comparison of data pre- and post-adjustment'),
               y= 'adjusted mean', x= 'original mean', color= 'case definition')+
          theme(strip.text.x = element_text(size = 7)))
}

dev.off()

# compare dist of data pre-post xw
ggplot(data= test_2[measure== 'prevalence'], mapping= aes(x=year_start, y=mean)) +
  labs(title= 'Distribution of all unadjusted data', color= 'clinical_data_type') +
  geom_jitter(aes(color= as.factor(clinical_data_type)), width=0.6, alpha=0.2, size=2)+
  theme_classic() +
  scale_color_hue(labels= c('Not claims', 'Claims'))
ggplot(data= test_2[measure== 'prevalence'], mapping= aes(x=year_start, y=mean)) +
  labs(title= 'Distribution of all unadjusted data', color= 'case_def') +
  geom_jitter(aes(color= as.factor(case_def)), width=0.6, alpha=0.2, size=2)+
  theme(legend.position="bottom")

ggplot(data= test_2[measure== 'prevalence'], mapping= aes(x=year_start, y=meanvar_adjusted)) +
  labs(title= 'Distribution of all unadjusted data', color= 'case_def') +
  geom_jitter(aes(color= as.factor(case_def)), width=0.6, alpha=0.2, size=2)+
  theme(legend.position="bottom")
dev.off()





# age split data  --------

dt <- copy(final)
ages <- get_age_metadata(12, gbd_round_id= 6)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[, age_group_id]
id <- 2149

#if you want to run the function line by line
df <- copy(dt)
age <- age_groups
gbd_id <- id

final_split <- age_split(gbd_id = id, df = dt, age = age_groups, region_pattern = F, location_pattern_id = 1)

write.xlsx(final_split, paste0('FILEPATH'))
final_split<- data.table(read.xlsx(paste0('FILEPATH')))
# plot age split  ----
x <- copy(final_split)
x[, age := (age_start + age_end) / 2]
gg <- ggplot(x, aes(x = age, y = mean)) +
  geom_smooth(se = F) +
  labs(x = "Age", y = "Prevalence", title = "Age Pattern") +
  theme_classic()

pdf(paste0(plot_dir, "/age_pattern.pdf", width = 12))
gg 
dev.off()



# merge on outliers from 2019 (that are not MAD outliers) ---------------------------------

final<- copy(final_split)
last_year<- get_crosswalk_version(11804)
nrow(last_year)
nrow(last_year[is_outlier== 1])
outliers<- last_year[is_outlier== 1 & !note_modeler %like% 'MAD'] 
outliers<- outliers[, c('nid', 'age_start', 'age_end', 'sex', 'year_start',
                         'year_end', 'measure', 'location_id', 'is_outlier')]
setnames(final, 'is_outlier', 'is_outlier_2020')
print(paste0('pre merge rows of data is ', nrow(final)))
merged<-left_join(final, outliers) #now I have added on outliers from last round. append new data back on.
print(paste0('post merge rows of data is ', nrow(merged))) 
final[is_outlier_2020== 1, is_outlier:= 1]
table(merged$is_outlier)
merged<- data.table(merged)
merged[is.na(is_outlier), is_outlier:= 0]
merged[, is_outlier_2020:= NULL]

# write flat file
write.xlsx(merged, paste0('FILEPATH'), sheetName= 'extraction')
merged<- data.table(read.xlsx(paste0('FILEPATH')))


# drop claims ----
merged<- merged[!field_citation_value %like% '(Poland)']



# mad outlier  ----
outlier_val<- 1.5 
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) # new in GBD 2020 step 1

byvars <- c("location_id", "sex", "year_start", "year_end", "nid") 

# get age weights
all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19, gbd_round_id=7))
all_fine_ages[, age_start := age_group_years_start]


not_mad <- subset(merged, measure != "prevalence") 
dt_inp<- subset(merged, measure == "prevalence") 

pre_outliers<- nrow(dt_inp[is_outlier== 1])
print(pre_outliers)

# merge age table map onto dataset
dt_inp <- merge(dt_inp, all_fine_ages, by = c("age_start"), all.x = T)
unique(dt_inp$note_modeler)

# create new age-weights for each data source
dt_inp[, sum := sum(age_group_weight_value), by = byvars] #sum of standard age-weights for all the ages we are using, by location-age-sex-nid, sum will be same for all age-groups and possibly less than one
dt_inp[, new_weight := age_group_weight_value/sum, by = byvars] #divide each age-group's standard weight by the sum of all the weights in their location-age-sex-nid group

# age standardizing per location-year by sex
# add a column titled "age_std_mean" with the age-standardized mean for the location-year-sex-nid
dt_inp[, as_mean := mean * new_weight] #initially just the weighted mean for that AGE-location-year-sex-nid
dt_inp[, as_mean := sum(as_mean), by = byvars] #sum across ages within the location-year-sex-nid group, you now have age-standardized mean for that series

# mark as outlier if age standardized mean is 0 (either biased data or rare disease in small population)
dt_inp[as_mean == 0, is_outlier := 1]
dt_inp[as_mean == 0, note_modeler := paste0(note_modeler, " | outliered this location-year-sex-NID age-series because age standardized mean is 0")]

# log-transform to pick up low outliers
dt_inp[as_mean != 0, as_mean := log(as_mean)]

# calculate median absolute deviation
dt_inp[as_mean == 0, as_mean := NA] ## don't count zeros in median calculations
dt_inp[,mad:=mad(as_mean,na.rm = T),by=c("sex", "super_region_name")]
dt_inp[,median:=median(as_mean,na.rm = T),by=c("sex", "super_region_name")]

#***can change number of MAD to mark here
dt_inp[as_mean>((outlier_val*mad)+median), is_outlier := 1]
dt_inp[as_mean>((outlier_val*mad)+median), note_modeler := paste0(note_modeler, " | outliered because age-standardized mean for location-year-sex-NID is higher than ", outlier_val," MAD above median")]
#dt_inp[as_mean<(median-(outlier_val*mad)), is_outlier := 1]
#dt_inp[as_mean<(median-(outlier_val*mad)), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is lower than ", outlier_val," MAD below median")]
dt_inp[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id", "super_region_name") := NULL]

print(paste((nrow(dt_inp[is_outlier == 1])- pre_outliers), "points were outliered with", outlier_val, "MAD"))
percent_outliered <- round(((nrow(dt_inp[is_outlier == 1])- pre_outliers) / nrow(dt_inp))*100, digits = 1)
print(paste("outliered", percent_outliered, "% more of data"))
dropped_locs <- setdiff(unique(final_split$country), unique(dt_inp[is_outlier==0]$country))
print(paste("Dropped ", length(dropped_locs), " countries from model:", paste(dropped_locs, collapse = " ")))

dt_inp[is.na(lower), uncertainty_type_value := NA]
dt_inp[standard_error > 1, standard_error := 1]

# now outlier all chronic data
upload<-rbind(dt_inp, not_mad, fill= TRUE)
unique(upload[case_def== "", field_citation_value])
upload<- upload[case_def %like% 'chronic', `:=` (is_outlier=1, note_modeler= paste0(note_modeler, " | outliered chronic data"))]

outliered<- upload[is_outlier== 1]
print(paste0(round(nrow(outliered)*100/nrow(upload), 3),' % of the dataset is outliered')) 
write.xlsx(upload, paste0(flat_file_dir, 'FILEPATH'), sheetName= "extraction")


# save a crosswalk version  -----
to_save_xwalk <- as.data.table(upload)
to_save_xwalk <- to_save_xwalk[sex != "Both"]
to_save_xwalk[measure == "prevalence" & upper > 1, `:=` (lower = NaN, upper = NaN, uncertainty_type_value = NA)]
to_save_xwalk <- to_save_xwalk[group_review == 1 | is.na(group_review)]
to_save_xwalk[, `:=` (group = NA, specificity = NA, group_review = NA)]
to_save_xwalk[is.na(upper), `:=` (lower = NA, uncertainty_type_value = NA)]
to_save_xwalk[!is.na(upper), uncertainty_type_value := 95]
to_save_xwalk <- to_save_xwalk[mean <= 1]
to_save_xwalk[is.na(crosswalk_parent_seq), crosswalk_parent_seq:= seq]
to_save_xwalk[, seq:= '']
to_save_xwalk[measure== 'prevalence' & standard_error> 1, standard_error:= .99]

# split UK subnats
final_full<- utla_split(to_save_xwalk)

# final checks
final_full <- final_full[group_review == 1 | is.na(group_review)]
final_full[, `:=` (group = NA, specificity = NA, group_review = NA)]
final_full[nid== 116266, is_outlier:= 1]
final_full<- final_full[!field_citation_value %like% '(Poland)']

write.xlsx(final_full, paste0(flat_file_dir, 'FILEPATH'), sheetName= 'extraction')



save_crosswalk_version(34427, paste0(flat_file_dir, "FILEPATH") , 
                       description = "all 2019 outliers MAD 1.5 21% with claims")



# save bulk outlier if needed ----
crosswalk_version_id <- 31325
xv<- get_crosswalk_version(crosswalk_version_id)

outliers<- c(66763)
#unoutliers<- c(222099, 44861)
xv[nid %in% outliers,`:=` (is_outlier=1, note_modeler= paste0(note_modeler, ' | outliering SAGE for GBD 2020 (small sample)'))]
#xv[nid %in% unoutliers,`:=` (is_outlier=0, note_modeler= paste0(note_modeler, ' | unoutliering data because source is trusted'))]
#xv<- xv[!nid %in% drop]

xv<- xv[, c("seq", "is_outlier")]
write.xlsx(xv, 'FILEPATH', sheetName = "extraction", col.names=TRUE)
filepath<- 'FILEPATH'

decomp_step <- 'iterative'
filepath <-'FILEPATH'
description3 <- 'outlier SAGE India' #description needed for each crosswalk version; this is what is going to show up in Dismod
save_bulk_outlier(
gbd_round_id = 7,
crosswalk_version_id=crosswalk_version_id,
decomp_step=decomp_step,
filepath=filepath,
description=description3
)

xv<- get_crosswalk_version(28415)
unique(xv$note_modeler)
