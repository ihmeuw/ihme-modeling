####################################################
## Author: USER
## Date: DATE
## Description: Crosswalk Data Processing - RA
## Modified by USER for GBD 2020
####################################################

rm(list=ls())
user<- 'USER'
date <- gsub("-","_",Sys.Date())

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
library(ggrepel)
library(dplyr)
library(tidyr)
library(Hmisc, lib.loc = paste0("FILEPATH"))
library(mvtnorm)
library(survival, lib.loc = paste0("FILEPATH"))
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
            'get_bundle_data', 'upload_bundle_data', 'get_bundle_version', 'save_bulk_outlier', 
            'save_bundle_version', 'get_crosswalk_version', 'save_crosswalk_version')
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))
library(crosswalk, lib.loc = "FILEPATH")
date <- gsub("-", "_", Sys.Date())
draws <- paste0("draw_", 0:999)

# functions
source("FILEPATH")
make_adjustment <- function(model, full_dt){
  reference_def<- 'reference'
  adjust_dt <- copy(full_dt[mean != 0, ])
  adjustse_dt <- copy(full_dt[mean == 0 & case_def != reference_def, ])
  noadjust_dt <- copy(full_dt[mean == 0 & case_def == reference_def])
  
  # Get prediction mean and standard error 
  preds <- model$fixed_vars
  preds <- as.data.frame(preds)
  preds <- rbind(preds, model$beta_sd)
  cvs <- unique(full_dt$case_def)
  cvs <- cvs[cvs != reference_def]
  
  # Adjust data points based on mrbrt and nonzero 
  adjust_dt[, c("mean_adj", "standard_error_adj", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
    fit_object = model,       # result of CWModel()
    df = adjust_dt,            # original data with obs to be adjusted
    orig_dorms = "case_def", # name of column with (all) definition/method levels
    orig_vals_mean = "mean",  # original mean
    orig_vals_se = "standard_error"  # standard error of original mean
  )
  adjust_dt$crosswalk_parent_seq <- adjust_dt$seq
  graph_adjust <- copy(adjust_dt)
  
  for (cv in cvs) {
    ladj <- preds[cv][[1]][1]
    ladj_se <- preds[cv][[1]][2]
    adjust_dt[case_def == cv & diff != 0, `:=` (mean = mean_adj, standard_error = standard_error_adj, 
                                                note_modeler = paste0(note_modeler, " | crosswalked with log(difference): ", 
                                                                      round(ladj, 2), " (", round(ladj_se, 2), ")"), 
                                                cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)] 
    adjust_dt[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  }
  
  ## ADJUST STANDARD ERROR FOR ZERO MEANS
  for (cv in cvs) {
    ladj <- preds[cv][[1]][1]
    ladj_se <- preds[cv][[1]][2]
    adjustse_dt[case_def == cv , `:=` (ladj = ladj, ladj_se = ladj_se)]
    adjustse_dt$adj_se <- sapply(1:nrow(adjustse_dt), function(i){
      mean_i <- as.numeric(adjustse_dt[i, "ladj"])
      se_i <- as.numeric(adjustse_dt[i, "ladj_se"])
      deltamethod(~exp(x1), mean_i, se_i^2)
    })
    adjustse_dt[case_def== cv, `:=` (standard_error = sqrt(standard_error^2 + adj_se^2), 
                        note_modeler = paste0(note_modeler, " | uncertainty from crosswalk added"), 
                        cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, effective_sample_size = NA)]
    adjustse_dt[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  } 
  
  # Combine everything, make sure there are no drops, drop extra columns 
  dt_all <- rbind(noadjust_dt, adjustse_dt, adjust_dt, fill = T, use.names = T)
  if (nrow(full_dt) == nrow(dt_all)) {
    message("All NIDs present")
  } else {
    message("Dropped NIDs. Stop!")
  }
  
  extra_cols <- setdiff(names(dt_all), names(full_dt))
  final_dt <- copy(dt_all)
  final_dt[, (extra_cols) := NULL]
  return(list(epidb = final_dt, vetting_dt = graph_adjust))
}

# objects
loc_data <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 6))
loc_data <- loc_data[, c("location_id", "location_name", "ihme_loc_id", "region_name", "super_region_name")]

new<- data.table(read.xlsx("FILEPATH"))
new_nids<- unique(new$nid)

# directories
plot_dir<- paste0('FILEPATH', date)
flat_file_dir<- paste0('FILEPATH', date)

dir.create(plot_dir)
dir.create(flat_file_dir)


# pull bundle ------
# tag covariates  ----
ra<- get_bundle_version(29648, fetch= 'all')

ra[nid %in% new_nids, new_survey_2020:= 1]

ra <- ra[is_outlier != 1 | is.na(is_outlier)] 
ra <- ra[is.na(drop) | drop != 1] 

ra[field_citation_value %like% "Truven Health" & year_start == 2000, cv_marketscan_all_2000 := 1]
ra[field_citation_value %like% "Truven Health" & year_start != 2000, cv_marketscan := 1]
ra[field_citation_value %like% "Taiwan National Health Insurance", cv_taiwan_claims_data := 1]
ra[case_definition %like% 'ACR 1987' | case_definition %like% 'American College of Rheumatology', cv_cd_acr:= 1]
ra[case_definition %like% 'self report' , cv_cd_non_acr:= 1]
ra[, "crosswalk_parent_seq" := as.numeric(NA)]

ra[, case_def := ifelse(!is.na(cv_cd_acr) & cv_cd_acr == 1, "reference", NA)]
ra[, case_def := ifelse(!is.na(cv_cd_non_acr)  & cv_cd_non_acr == 1, "Not ACR 1987", case_def)]
ra[, case_def := ifelse(!is.na(cv_cd_admin) & cv_cd_admin == 1, "reference", case_def)]
ra[, case_def := ifelse(!is.na(cv_marketscan_all_2000) & cv_marketscan_all_2000 == 1, "marketscan2000", case_def)]
ra[, case_def := ifelse(!is.na(cv_marketscan) & cv_marketscan == 1, "reference", case_def)]
ra[, case_def := ifelse(!is.na(cv_taiwan_claims_data) & cv_taiwan_claims_data == 1, "reference", case_def)]


# append location data  ----
ra[, `:=` (ihme_loc_id= NULL, location_name= NULL)]
ra <- merge(ra, loc_data, by = "location_id")
ra$country <- substr(ra$ihme_loc_id, 0, 3)


# fill out cases/mean/sample size/standard error based on uploader formulas ----
ra$cases <- as.numeric(ra$cases)
ra$sample_size <- as.numeric(ra$sample_size)
ra <- get_cases_sample_size(ra)
ra <- get_se(ra)

# remove note_modeler from 2019
ra[,note_modeler_2019 := note_modeler]
ra[, note_modeler:= '']
orig <- as.data.table(copy(ra))


# age-sex split ----
test <- age_sex_split(ra)

# sex split ----
test <- find_sex_match(ra)
test2 <- calc_sex_ratios(test)

model_name <- paste0("ra_sexsplit_", date)

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
predict_sex <- split_data(ra, sex_model)

# plot sex split
pdf(paste0(plot_dir, '/', model_name, "_sex_split.pdf"))
graph<- predict_sex$graph
graph<- graph[male_mean!= 0] #had to drop 0 data points for the plot
graph_predictions(graph) #can't plot the split
dev.off()

# write sex split files
write.xlsx(predict_sex$final, paste0(flat_file_dir, 'FILEPATH'))
write.xlsx(predict_sex$graph, paste0(flat_file_dir, 'FILEPATH'))


# drop extraneous columns and aggregate claims data ----
ra <- copy(predict_sex$final)
orig <- as.data.table(copy(ra))

# aggregate claims data
claims_data <- ra[clinical_data_type == "claims"]
ra <- ra[clinical_data_type != "claims"]
claims_data <- aggregate_marketscan(claims_data)
claims_data <- claims_data[location_name == "United States" | location_name == "Taiwan"] 
ra <- rbind(ra, claims_data)
ra <- get_cases_sample_size(ra)
ra <- get_se(ra)
ra <- ra[, c("nid", "sex", "year_start", "year_end", "age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "country", "region_name", "super_region_name", "measure", "mean", "lower", "upper", "standard_error", "case_def")]

# make matched dataset  ----
ra[, demographics := paste2(sex, country, measure, sep = "_")]

ra2 <- copy(ra)

ra2 <- ra2[!is.na(case_def)]
ra2 <- ra2[!is.na(standard_error)]

ra_cds <- list(unique(ra2$case_def))

ra2[, year_mean := (year_start + year_end)/2]
ra2[, age_mean := (age_start + age_end)/2]

ra2$case_def <- as.factor(ra2$case_def)
levels(ra2$case_def)
ra2$case_def <- factor(ra2$case_def, levels(ra2$case_def)[c(3, 1:2)]) #making reference first
ra2 <- ra2[order(case_def)]

list2 <- lapply(unique(ra2$case_def), function(i) {
  subset(ra2, case_def == i)
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

ra_matched <- copy(paired_reference_marketscan2000) 
ra_matched <- ra_matched[0,]

for (i in 1:length(pairss)) {
  ra_matched <- rbind(ra_matched, get(pairss[i]))
}

nrow(ra_matched[nid.denom == nid.num])
nrow(ra_matched[nid.denom != nid.num]) 

ra_matched <- ra_matched[abs(year_mean.denom - year_mean.num) < 6]
ra_matched <- ra_matched[abs(age_start.denom - age_start.num) < 6 & abs(age_end.denom - age_end.num) <6]



nrow(ra_matched[nid.denom == nid.num]) # 0 rows
nrow(ra_matched[nid.denom != nid.num]) # 270 rows

unique(ra_matched$case_def.num)
unique(ra_matched$case_def.denom)
remaining_cd <- unique(c(unique(as.character(ra_matched$case_def.num)), unique(as.character(ra_matched$case_def.denom))))

for (i in pairss) { 
  dt <- as.data.table(get(paste0(i)))
  #dt <- dt[abs(year_mean.denom - year_mean.num) < 6]
  dt <- dt[abs(year_start.denom - year_start.num) < 6 & abs(year_end.denom - year_end.num) <6]
  dt <- dt[abs(age_start.denom - age_start.num) < 6 & abs(age_end.denom - age_end.num) <6]
  assign(paste0("subset_", paste0(i)), dt)
}
subset_matches <- grep("subset", names(.GlobalEnv), value = T)

rm_subset <- c() # clear out matched cds with 0 observations
for (i in subset_matches) {
  if (nrow(get(i)) < 1) {
    rm_subset <- c(rm_subset, paste0(i))
  }
}
rm(list = rm_subset, envir = .GlobalEnv)
subset_matches <- grep("subset", names(.GlobalEnv), value = T)

# calculate ratio & SE
ra_matched <- ra_matched %>%
  mutate(
    ratio = mean.num / mean.denom,
    ref_se = standard_error.denom, #(upper.denom - lower.denom) / 3.92,
    alt_se = standard_error.num, #(upper.num - lower.num) / 3.92,
    ratio_se =
      sqrt((mean.num^2 / mean.denom^2) * (alt_se^2/mean.num^2 + ref_se^2/mean.denom^2))
  )

ra_matched <- as.data.table(ra_matched)

ra_matched2 <- copy(ra_matched)
ra_matched2[, id := paste0(nid.num, " (", country.num, ": ", sex.num, " ", age_start.num, "-", age_end.num, ") - ", nid.denom, " (", country.denom, ": ", sex.denom, " ", age_start.denom, "-", age_end.denom, ")")]
ra_matched2[, id_var := paste0(nid.num, ":", nid.denom)]

# write files
write.xlsx(ra_matched2, paste0(flat_file_dir, "FILEPATH"))


# prep for MRBRT  ----
df<- copy(ra_matched2)
df$case_def.denom <- gsub(pattern = " ", replacement = "_", x = df$case_def.denom)
df$case_def.num <- gsub(pattern = " ", replacement = "_", x = df$case_def.num)
#select(df, id, id_var, ratio, ratio_se, ref = case_def.denom, alt = case_def.num) %>%
df2 <- mutate(df, ref = case_def.denom, alt = case_def.num) %>%
  filter( !is.na(ratio) & !is.na(ratio_se))
df2$ref <- as.character(df2$ref)
df2$alt <- as.character(df2$alt)

alts <- unique(df2$alt)
refs <- unique(df2$ref)
reference_def <- "reference"
refs <- refs[refs != reference_def]

matched<- copy(data.table(df2)) 

# drop pairs where reference is not the gold-standard (indirect comparisons)
matched<- matched[ref== 'reference']

# no matches have a mean of 0 at this point 
matched <- matched[!mean.num==0]
matched <- matched[!mean.denom==0]

#get log calcs using the delta transform package (transform mean and standard error into log-space)
log_alt_means <- as.data.table(delta_transform(mean=matched$mean.num, sd=matched$standard_error.num, transformation='linear_to_log'))
setnames(log_alt_means, c('mean_log', 'sd_log'), c('log_alt_mean', 'log_alt_se'))
log_ref_means <- as.data.table(delta_transform(mean=matched$mean.denom, sd=matched$standard_error.denom, transformation='linear_to_log'))
setnames(log_ref_means, c('mean_log', 'sd_log'), c('log_ref_mean', 'log_ref_se'))

# bind back onto main data table
matched <- cbind(matched, log_alt_means)
matched <- cbind(matched, log_ref_means)

# use calculate_diff() to calculate the log difference between matched pairs
matched[, c("log_diff", "log_diff_se")] <- calculate_diff(
  df = matched, 
  alt_mean = "log_alt_mean", alt_sd = "log_alt_se",
  ref_mean = "log_ref_mean", ref_sd = "log_alt_se" )


reg<- matched[, c('log_diff', 'log_diff_se', 'alt', 'ref', 'id_var')]


reg<- reg[log_diff_se> 1, log_diff_se:= 1] 

# convert id variable to integer
df_ids <- data.frame(id_var = unique(reg$id_var)) %>%
  mutate(id2 = as.integer(1:nrow(.)))
reg_2<- merge(reg, df_ids, by= 'id_var')

# write pre-XW meta-regression data to flat file
write.xlsx(reg_2, paste0("FILEPATH"))


# run model ----

df1 <- CWData(
  df = reg_2,          # dataset for metaregression
  obs = "log_diff",       # column name for the observation mean
  obs_se = "log_diff_se", # column name for the observation standard error
  alt_dorms = "alt",     # column name of the variable indicating the alternative method
  ref_dorms = "ref",     # column name of the variable indicating the reference method
  covs= list(),
  study_id = "id2",    # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
)

fit1 <- CWModel(
  cwdata = df1,            # object returned by `CWData()`
  obs_type = "diff_log",  # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  inlier_pct= 0.9, 
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel(cov_name = "intercept")),
  gold_dorm = "reference"   # the level of `alt_dorms` that indicates it's the gold standard
  )

fit1$fixed_vars
repl_python()

plots <- import("crosswalk.plots")

plots$funnel_plot(
  cwmodel = fit1, 
  cwdata = df1,
  continuous_variables = list(),
  obs_method = 'marketscan2000',
  plot_note = 'Funnel plot RA, marketscan2000', 
  plots_dir = plot_dir, 
  file_name = "funnel_plot_marketscan2000",
  write_file = TRUE
)
# check outputs
fit1$fixed_vars

# save results  ----
df_result <- fit1$create_result_df()
write.csv(df_result, paste0(flat_file_dir, 'FILEPATH'))
py_save_object(object = fit1, filename = paste0(flat_file_dir, 'FILEPATH'), pickle = "dill")



# adjust data ----
orig <- data.table(read.xlsx(paste0(flat_file_dir, 'FILEPATH')))

to_xw<- copy(orig)
to_xw[case_def== 'Not ACR 1987', case_def:= 'Not_ACR_1987'] #make case definitions align

no_adjust<- to_xw[is.na(case_def)] 
emr<- no_adjust[field_citation_value %like% '(EMR)']

adjust<- to_xw[!is.na(case_def)]
adjusted<- make_adjustment(fit1, adjust)

# bind adjusted data to EMR data
full_dt<- rbind(adjusted$epidb, emr, fill= TRUE)

# save the crosswalked data to a flat file
write.xlsx(full_dt, paste0(flat_file_dir, 'crosswalked_dataset.xlsx'))
#full_dt<- data.table(read.xlsx( paste0(flat_file_dir, 'crosswalked_dataset.xlsx')))


# age split ---
dt <- copy(full_dt)
ages <- get_age_metadata(12, gbd_round_id= 6)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_groups <- ages[, age_group_id]
id <- 24622

df <- copy(dt)
age <- age_groups
gbd_id <- id

final_split <- age_split(gbd_id = id, df = dt, age = age_groups, region_pattern = F, location_pattern_id = 1)
write.xlsx(final_split, paste0(flat_file_dir, 'age_split_data.xlsx'))



# plot age split  ----
x <- copy(final_split)
x[, age := (age_start + age_end) / 2]
gg <- ggplot(x, aes(x = age, y = mean)) +
  geom_smooth(se = F) +
  labs(x = "Age", y = "Prevalence", title = "Age Pattern") +
  theme_classic()

pdf(paste0(plot_dir, 'FILEPATH', width = 12))
gg
dev.off()

# apply outliers from GBD 2019  ----
xv<- get_crosswalk_version(crosswalk_version_id = 11564) 
outliers<- xv[is_outlier== 1 & !(note_modeler %like% 'MAD')] 
outliers<- outliers[, c('nid', 'age_start', 'age_end', 'year_start', 'year_end',
                        'sex', 'measure', 'location_id', 'is_outlier')]
#setnames(outliers, 'crosswalk_parent_seq', '2019_origin_seq')
to_merge<- copy(final_split)
to_merge[, is_outlier:= NULL]
print(nrow(to_merge))
merged<-left_join(to_merge, outliers) 
print(nrow(merged))
merged<- data.table(merged)
merged[is.na(is_outlier), is_outlier:= 0]
table(merged$is_outlier) 
# mad outlier post age split ----
outlier_val<- 2
age_using <- c(2:3,388, 389, 238, 34,6:20, 30:32, 235 ) # new in GBD 2020 step 1

byvars <- c("location_id", "sex", "year_start", "year_end", "nid") ##same as Brittney's

# get age weights
all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19, gbd_round_id=7))
all_fine_ages[, age_start := age_group_years_start]

##make a set to be run through outlier script
not_mad <- subset(merged, measure != "prevalence") 
dt_inp<- subset(merged, measure == "prevalence") 

pre_outliers<- nrow(dt_inp[is_outlier== 1])
print(pre_outliers)


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
#dt_inp[as_mean<(median-(outlier_val*mad)), is_outlier := 1]
#dt_inp[as_mean<(median-(outlier_val*mad)), note_modeler := paste0(note_modeler, " | outliered because log age-standardized mean for location-year-sex-NID is lower than ", outlier_val," MAD below median")]
dt_inp[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value", "age_group_id", "super_region_name") := NULL]

print(paste((nrow(dt_inp[is_outlier == 1])- pre_outliers), "points were outliered with", outlier_val, "MAD"))
percent_outliered <- round(((nrow(dt_inp[is_outlier == 1])- pre_outliers) / nrow(dt_inp))*100, digits = 1)
print(paste("outliered", percent_outliered, "% of data"))
dropped_locs <- setdiff(unique(final_split$country), unique(dt_inp[is_outlier==0]$country))
print(paste("Dropped ", length(dropped_locs), " countries from model:", paste(dropped_locs, collapse = " ")))

dt_inp[is.na(lower), uncertainty_type_value := NA]

dt_inp[standard_error > 1, standard_error := 1]

upload<-rbind(dt_inp, not_mad, fill= TRUE)
outliered<- upload[is_outlier== 1]
unique(outliered$field_citation_value)
upload <- upload[sex != "Both"]
upload <- upload[group_review != 0 | is.na(group_review)]
upload[is.na(upper), `:=` (lower = NA, uncertainty_type_value = NA)]
upload[!is.na(upper), uncertainty_type_value := 95]
upload[is.na(upper), upper := NaN]
upload <- upload[location_id != 95]
upload[measure == "prevalence" & !is.na(standard_error) & upper > 1, `:=` (upper = NaN, lower = NaN, uncertainty_type_value = NA)]
upload[measure == "prevalence" & standard_error > 1 & !is.na(cases) & !is.na(sample_size), standard_error := NaN]
upload[nid == 118322 & location_id == 4623, `:=` (location_name = "Norfolk", location_id = 44723, ihme_loc_id = "GBR_44723", step2_location_year = "Retag region of England")]
upload[nid == 118325 & location_id == 4623, `:=` (location_name = "Norfolk", location_id = 44723, ihme_loc_id = "GBR_44723", step2_location_year = "Retag region of England")]
upload[location_id == 4927, `:=` (location_name = "Troms og Finnmark", location_id = 60137, ihme_loc_id = "NOR_60137")]


upload[nid== 416752, is_outlier:= 1]
upload[is.na(crosswalk_parent_seq), crosswalk_parent_seq:= seq]
upload[, seq:= '']

write.xlsx(upload, paste0(flat_file_dir, 'FILEPATH'), sheetName= "extraction")

save_crosswalk_version(29648, paste0(flat_file_dir, "FILEPATH") , 
                       description = "MAD outlier +/- 2 1.6%")

View(upload[is_outlier==1])
ggplot(upload, aes(x= year_start, y=mean, color= is_outlier)) +
  geom_jitter(aes(color= as.factor(is_outlier)))



