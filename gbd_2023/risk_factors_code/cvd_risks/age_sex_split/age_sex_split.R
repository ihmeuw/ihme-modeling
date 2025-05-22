# Author: USERNAME
# Date: DATE
# Description: Age- and sex- split bundle version data with PyDisagg

# ==============================================================================

rm(list=ls())

# Packages
library(data.table)
library(dplyr)
library(reticulate)
library(tools)
library(ggplot2)
library(splines)

# Load PyDisagg
reticulate::use_python("/FILEPATH/python")
splitter <- import("pydisagg.ihme.splitter")

# Shared functions
central <- "/FILEPATH/"
source(paste0(central, "get_bundle_version.R"))
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_draws.R"))
source(paste0(central, "get_population.R"))
source(paste0(central, "get_age_metadata.R"))
source(paste0(central, "get_elmo_ids.R"))
source(paste0(central, "get_crosswalk_version.R"))

# Parameters
rei <- "metab_sbp" # "metab_sbp" or "metab_ldl"
release <- 16 # GBD release ID
meid <- ifelse(rei=='metab_sbp', 2547, 18822) # exposure ME ID from which to pull age & sex patterns
bundle_id <- ifelse(rei=='metab_sbp', 4787, 4904) # bundle ID

bundle_tracker <- merge(fread('/FILEPATH/bundle_tracker.csv'), data.table(rei), by='rei')[!is.na(bundle_version_id)]
bvid <- bundle_tracker[nrow(bundle_tracker), bundle_version_id]

# Filepaths
output_path <- '/FILEPATH/'
if(rei=='metab_sbp'){
  sd_path <- "/FILEPATH/sbp_sd_mod_release_16_2024_08_10.rds"
} else {
  sd_path <- "/FILEPATH/ldl_sd_mod_release_16_2024_08_10.rds"
}

##############################################################################################################
################################## # Load and format bundle version data #####################################
##############################################################################################################

if(rei=='metab_sbp'){
  # load bundle version data
  input_data <- get_bundle_version(bvid)
} else {
  # load results from the LDL xwalk
  input_data <- fread(paste0("/FILEPATH/ldl_to_age_split_bvid_", 
                                      bvid, ".csv"))
  setnames(input_data, 'mean', 'val')
}
# remove years of data prior to when modeling starts
input_data <- input_data[year_id >= 1980]
if(!'sex_id' %in% names(input_data)){
  # add sex_id
  input_data[, sex_id := case_when(
    sex == 'Male' ~ 1,
    sex == 'Female' ~ 2,
    sex == 'Both' ~ 3
  )]
} else {
  # add sex
  input_data[, sex := case_when(
    sex_id == 1 ~ 'Male',
    sex_id == 2 ~ 'Female',
    sex_id == 3 ~ 'Both'
  )]
}
# save original values
orig <- c("sex", "val", "standard_error", "sample_size")
input_data[, paste0(orig, '_orig') := lapply(.SD, function(x) x), .SDcols=orig]
# set age_start & age_end to the original values and align with GBD age_metadata (where age_group_years_end is not inclusive)
if('age_start_orig' %in% names(input_data) & 'age_end_orig' %in% names(input_data)){
  input_data[, age_start := age_start_orig]
  input_data[, age_end := age_end_orig + 1] 
} else {
  orig <- c("age_start", "age_end")
  input_data[, paste0(orig, '_orig') := lapply(.SD, function(x) x), .SDcols=orig]
  input_data[, age_end := age_end + 1] 
}
if('age_group_id' %in% names(input_data)){
  input_data[, age_group_id := NULL]
}
# add location metadata
loc_meta <- get_location_metadata(location_set_id=22, release_id = release)
loc_meta <- loc_meta[ , c('ihme_loc_id', 
                         setdiff(c('location_name', 'super_region_id', 'region_id'), names(input_data))), 
                      with=F]
orig_rows <- nrow(input_data)
bundle_locs <- unique(input_data$ihme_loc_id)
input_data <- merge(input_data, loc_meta, by="ihme_loc_id")
if(nrow(input_data) != orig_rows){
  warning(paste0(orig_rows - nrow(input_data), ' row(s) lost when merging location metadata! Location(s): ', 
                 paste0(setdiff(bundle_locs, unique(input_data$ihme_loc_id)), collapse=', ')))
}

# check existing standard error
hist(input_data[!is.na(standard_error), standard_error])
input_data[!is.na(standard_error), .N, by='standard_error'][order(-N)]

# check missing standard error
print(paste0("Rows missing standard error: ", nrow(input_data[standard_error == 0 | is.na(standard_error)])))
print(paste0("Percent of bundle missing standard error: ",
             round(100*nrow(input_data[standard_error == 0 | is.na(standard_error)])/nrow(input_data), 2), "%"))
print(paste0("Number of NIDs with missing standard error: ", 
             length(unique(input_data[standard_error == 0 | is.na(standard_error), nid]))))

# impute missing standard error
if(nrow(input_data[is.na(standard_error) | standard_error == 0]) > 0){
  message(paste0("Some input data points are missing standard error or have standard error == 0, will impute"))
  
  miss_ses <- nrow(input_data[is.na(standard_error) | standard_error == 0])
  miss_ss <- nrow(input_data[is.na(sample_size) | sample_size == 0])

  # if sample size is missing, give 5th percentile SS
  five_ss <- quantile(input_data$sample_size, probs=0.05, na.rm=T)
  input_data[is.na(sample_size) | sample_size == 0, sample_size := five_ss]
  
  # calculate SE for rows missing SE but not SD
  input_data[(is.na(standard_error) | standard_error == 0) & !is.na(standard_deviation) & standard_deviation != 0, 
                      standard_error := standard_deviation/sqrt(sample_size)]
  
  # bring in SD model and predict
  sd_mod <- readRDS(sd_path)
  
  # need to give an age group id and a sex id for prediction, use males and oldest age group to max se
  data <- input_data[(is.na(standard_error) | standard_error == 0) & (is.na(standard_deviation) | standard_deviation == 0),
                              .(seq, val, sample_size)]
  data[, age_start:=95]
  data[, sex_id:=1]
  data[, year_id := max(input_data$year_id)]
  data[, log_mean := log(val)]
  
  # predict SD and calculate SE
  data[, sds := exp(predict.lm(sd_mod, newdata=data))]
  data[, ses := sds/sqrt(sample_size)]
  input_data <- merge(input_data, data[,.(seq, ses)], by='seq', all.x=T)
  input_data[!is.na(ses), standard_error := ses]
  input_data[, ses := NULL]
  
  neg_sds <- nrow(input_data[standard_error<0])
  if(neg_sds>0){stop("There are ", neg_sds, " rows of data with negative standard error, please check these!")}
  
  missing_vals <- nrow(input_data[is.na(standard_error) | standard_error == 0 | is.na(sample_size) | sample_size == 0])
  if(missing_vals>0){stop("There are ", missing_vals, " rows of data still with NA or 0 values for standard error or sample size")}
  
  message(paste0(miss_ss, " had imputed sample size"))
  message(paste0(miss_ses, " had predicted standard errors"))
  message(paste0("Predicted standard errors had mean: ", mean(data$ses)))

  # limit extreme standard_errors
  maxvar <- quantile(input_data$standard_error, probs=.999)
  minvar <- quantile(input_data$standard_error, probs=.01)
  message(paste0("Setting ", nrow(input_data[standard_error > maxvar]), " rows with standard error > ", maxvar, " to ", maxvar))
  input_data[standard_error > maxvar, standard_error := maxvar]
  message(paste0("Setting ", nrow(input_data[standard_error < minvar]), " rows with standard error < ", minvar, " to ", minvar))
  input_data[standard_error < minvar, standard_error := minvar]
}

# merge on age_group_ids where age_start and age_end match standard GBD age groups
age_meta <- get_age_metadata(release_id = release)[,.(age_group_id, age_group_years_start, age_group_years_end)]
age_meta <- rbind(age_meta[age_group_years_start>=5], data.table(age_group_id = 1, age_group_years_start = 0, age_group_years_end = 5))
input_data <- merge(input_data, age_meta, 
                             by.x = c('age_start', 'age_end'), by.y = c('age_group_years_start', 'age_group_years_end'), all.x = T)

# identify data to be split
input_data[, sex_split := as.numeric(sex == 'Both')]
print(paste0(nrow(input_data[sex_split==1]), " row(s) will be sex-split"))
input_data[, age_split := as.numeric(is.na(age_group_id))]
print(paste0(nrow(input_data[age_split==1]), " row(s) will be age-split"))

##############################################################################################################
######################################## # Sex splitting #####################################################
##############################################################################################################

# pull out data to be sex-split
pre_split <- input_data[sex_split == 1,
                                 .(nid, seq, location_id, year_id, sex_id, age_start, age_end, val, standard_error)]

# create sex pattern from ST-GPR global results
sex_pattern <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = meid, source = 'stgpr', release_id = release, 
                         location_id = 1, year_id = unique(input_data$year_id), sex_id = c(1, 2))
draw_cols <- grep('draw', names(sex_pattern), value=T)
# use population to aggregate to all-ages
pop <- get_population(release_id = release, age_group_id = unique(sex_pattern$age_group_id), location_id = unique(sex_pattern$location_id), 
                      sex_id = unique(sex_pattern$sex_id), year_id = unique(sex_pattern$year_id))
sex_pattern <- merge(sex_pattern, pop, by = c('age_group_id', 'year_id', 'sex_id', 'location_id'))
sex_pattern[, total_pop := sum(population), by = c('year_id', 'sex_id', 'location_id')]
sex_pattern[, wt := population/total_pop]
sex_pattern[, paste0(draw_cols) := lapply(X = draw_cols, FUN = function(x) get(paste0(x)) * wt)]
sex_pattern <- sex_pattern[, lapply(.SD, sum), by=c('year_id', 'sex_id', 'location_id'), .SDcols=draw_cols]
# calculate sex ratio
sex_pattern <- data.table::melt(sex_pattern, id.vars = c('year_id', 'sex_id', 'location_id'), variable.name = 'draw')
sex_pattern <- data.table::dcast(sex_pattern, year_id + location_id + draw ~ sex_id, value.var = 'value')
sex_pattern[, ratio := `2`/`1`]
sex_pattern <- sex_pattern[, list(draw_mean = mean(ratio), draw_sd = sd(ratio)), by='year_id']

# pull population for sex splitting
sex_pop <- get_population(release_id = release, location_id = unique(input_data$location_id), 
                          sex_id = c(1,2), year_id = unique(input_data$year_id))
sex_pop <- sex_pop[,c('location_id', 'year_id', 'sex_id', 'age_group_id', 'population'), with = F]
if(length(unique(sex_pop$age_group_id))==1){
  sex_pop[, age_group_id := NULL]
}

# sex splitting
sex_splitter <- splitter$SexSplitter(
  data=splitter$SexDataConfig(
    index=c("nid", "seq", "location_id", "year_id", "sex_id", "age_start", "age_end"),
    val="val",
    val_sd="standard_error"
  ),
  pattern=splitter$SexPatternConfig(
    by=list('year_id'),
    val='draw_mean',
    val_sd='draw_sd'
  ),
  population=splitter$SexPopulationConfig(
    index=c('location_id', 'year_id'),
    sex="sex_id",
    sex_m=1,
    sex_f=2,
    val='population'
  )
)

result_sex_df <- sex_splitter$split(data=pre_split,
                                   pattern=sex_pattern,
                                   population=sex_pop,
                                   output_type="rate")

# add other bundle columns to the sex-split data
post_split <- as.data.table(result_sex_df)[, .(nid, seq, location_id, year_id, sex_id, age_start, age_end, sex_split_result, sex_split_result_se, m_pop, f_pop)]
setnames(post_split, c('sex_split_result', 'sex_split_result_se'), c('val', 'standard_error'))
post_split[, sex := case_when(
  sex_id == 1 ~ 'Male',
  sex_id == 2 ~ 'Female',
)]
post_split[sex=='Male', pop_population_proportion := m_pop/(m_pop + f_pop)]
post_split[sex=='Female', pop_population_proportion := f_pop/(m_pop + f_pop)]
meta_cols <- setdiff(names(input_data), c('sex', 'sex_id', 'val', 'standard_error', 
                                                   'variance', 'standard_deviation', 'lower', 'upper'))
post_split <- merge(post_split, input_data[sex_split == 1, meta_cols, with=F], by = c('nid', 'seq', 'location_id', 'year_id', 'age_start', 'age_end'))
post_split[, sample_size := sample_size * pop_population_proportion]
post_split[, c('m_pop', 'f_pop', 'pop_population_proportion') := NULL]

# combine bundle data where sex splitting was not needed with sex-split data
post_sex_split <- rbind(input_data[sex_split==0], post_split, fill=T)

##############################################################################################################
######################################## # Age splitting #####################################################
##############################################################################################################

# pull out data to be age-split
pre_split <- post_sex_split[age_split == 1,
                                 .(nid, seq, location_id, year_id, sex_id, age_start, age_end, val, standard_error)]

# create age pattern
age_pattern <- get_draws(gbd_id_type = 'modelable_entity_id', gbd_id = meid, source = 'stgpr', release_id = release, 
                         location_id = 1, year_id = unique(post_sex_split$year_id), sex_id = c(1, 2))
draw_cols <- grep('draw', names(age_pattern), value=T)
age_pattern <- age_pattern[,c('location_id', 'year_id', 'sex_id', 'age_group_id', draw_cols), with = F]
if(length(unique(age_pattern$location_id))==1){
  age_pattern[, location_id := NULL]
}
# fill in age groups that are not modeled with the age pattern from the youngest ages
missing_ages <- setdiff(age_meta$age_group_id, unique(age_pattern$age_group_id))
if(length(missing_ages) > 0){
  min_modeled_age <- age_meta[age_group_years_start==age_meta[age_group_id %in% intersect(age_meta$age_group_id, unique(age_pattern$age_group_id)), min(age_group_years_start)], age_group_years_start]
  youngest_age_pattern <- age_pattern[age_group_id == age_meta[age_group_years_start==min_modeled_age, age_group_id]]
  youngest_age_pattern[, age_group_id := NULL]
  for(age in missing_ages){
    youngest_age_pattern[, age_group_id := age]
    age_pattern <- rbind(age_pattern, youngest_age_pattern)
  }
}
age_pattern <- merge(age_pattern, age_meta, by='age_group_id')

# pull population for age splitting
age_pop <- get_population(release_id = release, location_id = unique(post_sex_split$location_id), 
                          sex_id = c(1,2), year_id = unique(post_sex_split$year_id),
                          age_group_id = unique(age_pattern$age_group_id))
age_pop <- age_pop[,c('location_id', 'year_id', 'sex_id', 'age_group_id', 'population'), with = F]

# age splitting
age_splitter <- splitter$AgeSplitter(
  data=splitter$AgeDataConfig(
    index=c("nid", "seq", "location_id", "year_id", "sex_id"),
    age_lwr="age_start",
    age_upr="age_end",
    val="val",
    val_sd="standard_error"
  ), 
  pattern=splitter$AgePatternConfig(
    by=list("sex_id", "year_id"),
    age_key="age_group_id",
    age_lwr="age_group_years_start",
    age_upr="age_group_years_end",
    draws=draw_cols
  ), 
  population=splitter$AgePopulationConfig(
    index=c("age_group_id", "location_id", "year_id", "sex_id"),
    val="population"
  )
)

result_age_sex_df <- age_splitter$split(data=pre_split,
                                       pattern=age_pattern,
                                       population=age_pop,
                                       output_type="rate")

# add other bundle columns to the sex-split data
post_split <- as.data.table(result_age_sex_df)[, .(nid, seq, location_id, year_id, sex_id, age_group_id, 
                                                   pat_age_group_years_start, pat_age_group_years_end, age_split_result, age_split_result_se, pop_population_proportion)]
setnames(post_split, c('pat_age_group_years_start', 'pat_age_group_years_end', 'age_split_result', 'age_split_result_se'), 
         c('age_start', 'age_end', 'val', 'standard_error'))
meta_cols <- setdiff(names(post_sex_split), c('age_start', 'age_end', 'age_group_id', 'val', 'standard_error', 
                                                   'variance', 'standard_deviation', 'lower', 'upper'))
post_split <- merge(post_split, post_sex_split[age_split == 1, meta_cols, with=F], by = c('nid', 'seq', 'location_id', 'year_id', 'sex_id'))
post_split[, sample_size := sample_size * pop_population_proportion]
post_split[, pop_population_proportion := NULL]

# combine bundle data where sex splitting not needed with sex-split data
post_age_sex_split <- rbind(post_sex_split[age_split==0], post_split, fill=T)
# drop rows from ages we do not model
if(exists('min_modeled_age')){
  post_age_sex_split <- post_age_sex_split[age_start >= min_modeled_age]
}

##############################################################################################################
######################################## # Diagnostics #######################################################
##############################################################################################################

# compare pre-split data to post-split data
pre_split <- copy(input_data)
if(exists('min_modeled_age')){
  pre_split <- pre_split[age_end > min_modeled_age]
}
pre_split[, nid_split := any(age_split == 1 | sex_split == 1), by = 'nid']
pre_split <- pre_split[nid_split==T, .(nid, seq, location_name, year_id, sex, age_start, age_end, val, standard_error)]
pre_split[, type := 'Pre-splitting']

post_split <- copy(post_age_sex_split)
post_split[, nid_split := any(age_split == 1 | sex_split == 1), by = 'nid']
post_split <- post_split[nid_split==T, .(nid, seq, location_name, year_id, sex, age_start, age_end, val, standard_error)]
post_split[, type := 'Post-splitting']

plot_dt <- rbind(pre_split, post_split)
plot_dt[age_end > 99, age_end := 99]
plot_dt[, age_mid := (age_start + age_end)/2]

pdf(paste0(output_path, rei, '_pre_vs_post_splits_bvid_', bvid, '.pdf'), width = 12, height = 9)
for(study in unique(plot_dt$nid)){
  print(paste0(which(unique(plot_dt$nid)==study), ' out of ', length(unique(plot_dt$nid))))
  p <- ggplot(plot_dt[nid == study], aes(x = age_mid, y = val, linetype = sex, shape = sex, color = type, group = interaction(sex, type))) + geom_point() + geom_line() +
    theme_bw() + geom_errorbarh(aes(xmin = age_start, xmax = age_end)) + geom_errorbar(aes(ymin = (val - (qnorm(0.975)*standard_error)), ymax = (val + (qnorm(0.975)*standard_error)))) +
    facet_wrap(location_name ~ year_id, scales='free_y') + labs(title = paste0('NID ', study), y = rei, x = 'Age', color = NULL, linetype = NULL, shape = NULL) +
    scale_color_manual(values = c('Post-splitting' = "#F8B25E", 'Pre-splitting' = "#2E5662")) + scale_shape_manual(values = c('Both' = 4, 'Male' = 2, 'Female' = 16))
  print(p)
}
dev.off()

# compare new data to old data in current best ST-GPR model
xwalk_id <- get_elmo_ids(bundle_id = bundle_id)[release_id == release & is_best == 1 & modelable_entity_id == meid, crosswalk_version_id]
best_xwalk <- get_crosswalk_version(xwalk_id)
best_xwalk[!is.na(crosswalk_parent_seq), seq := crosswalk_parent_seq]

best_xwalk_demo <- unique(best_xwalk[,.(nid, location_id, location_name, ihme_loc_id, year_id, sex, age_start, age_end)])
best_xwalk_demo[, type := 'Best xwalk']
new_data_demo <- unique(post_age_sex_split[,.(nid, location_id, location_name, ihme_loc_id, year_id, sex, age_start, age_end, age_start_orig, age_end_orig, sex_orig)])
new_data_demo[, type := 'New data']
new_data_demo[location_id==44533, location_id := 6]
comp_demo <- merge(best_xwalk_demo, new_data_demo, by=c('nid', 'location_id', 'year_id', 'sex', 'age_start'), all=T)
added_data <- comp_demo[is.na(type.x)]
table(added_data$location_name.y)
dropped_data <- comp_demo[is.na(type.y)]
table(dropped_data$location_name.x)

added_seqs <- post_age_sex_split[!seq %in% best_xwalk$seq]
table(added_seqs$ihme_loc_id)
dropped_seqs <- best_xwalk[!seq %in% post_age_sex_split$seq]
table(dropped_seqs$ihme_loc_id)

merge_vars <- c('nid', 'location_id', 'ihme_loc_id', 'site_memo', 'specificity', 'year_id', 'sex', 'age_start')
merge_vars <- intersect(names(post_age_sex_split), merge_vars)
merge_vars <- intersect(names(best_xwalk), merge_vars)

pdf(paste0(output_path, rei, '_splits_bvid_', bvid, '_vs_xwid_', xwalk_id, '.pdf'), width = 12, height = 9)
comp <- merge(post_age_sex_split, best_xwalk, by=merge_vars, suffixes = c('_new', '_old'))
ggplot(comp, aes(x=val_old, y=val_new, color=as.factor(age_start), label=ihme_loc_id, shape=age_split_new==1|sex_split_new==1)) + 
  facet_wrap(~sex) + geom_text(size=3) + theme_bw() + geom_abline(slope=1, intercept=0) + 
  labs(color = 'Age start', x = paste0('xwalk version associated with current best model'), y = paste0('newly processed ', rei, ' data'),
       title = 'Mean values', subtitle = 'Merging by NID, location, site memo, specificity, year, sex, and age',
       shape = 'Data split?')
ggplot(comp, aes(x=standard_error_old, y=standard_error_new, color=as.factor(age_start), label=ihme_loc_id, shape=age_split_new==1|sex_split_new==1)) + 
  facet_wrap(~sex) + geom_text(size=3) + theme_bw() + geom_abline(slope=1, intercept=0) + 
  labs(color = 'Age start', x = paste0('xwalk version associated with current best model'), y = paste0('newly processed ', rei, ' data'),
       title = 'Standard error values', subtitle = 'Merging by NID, location, site memo, specificity, year, sex, and age',
       shape = 'Data split?')

comp <- merge(post_age_sex_split, best_xwalk, by=c('seq', merge_vars), suffixes = c('_new', '_old'))
ggplot(comp, aes(x=val_old, y=val_new, color=as.factor(age_start), label=ihme_loc_id, shape=age_split_new==1|sex_split_new==1)) + 
  facet_wrap(~sex) + geom_text(size=3) + theme_bw() + geom_abline(slope=1, intercept=0) + 
  labs(color = 'Age start', x = paste0('xwalk version associated with current best model'), y = paste0('newly processed ', rei, ' data'),
       title = 'Mean values', subtitle = 'Merging by seqs',
       shape = 'Data split?')
ggplot(comp, aes(x=standard_error_old, y=standard_error_new, color=as.factor(age_start), label=ihme_loc_id, shape=age_split_new==1|sex_split_new==1)) + 
  facet_wrap(~sex) + geom_text(size=3) + theme_bw() + geom_abline(slope=1, intercept=0) + 
  labs(color = 'Age start', x = paste0('xwalk version associated with current best model'), y = paste0('newly processed ', rei, ' data'),
       title = 'Standard error values', subtitle = 'Merging by seqs',
       shape = 'Data split?')
dev.off()

##############################################################################################################
######################################## # Save data #########################################################
##############################################################################################################

# reset age_end to be less than the age_start of the next age_group_id, as required by uploader
post_age_sex_split[, age_end := age_end - 1]
# recalculate variance, upper, and lower from standard_error to be consistent
post_age_sex_split[, `:=` (variance = standard_error^2, 
                              upper = val + (qnorm(0.975)*standard_error), 
                              lower = val - (qnorm(0.975)*standard_error))]
post_age_sex_split[, standard_deviation := NULL]
# set crosswalk_parent_seqs for the rows that were split
if(rei=='metab_sbp'){
  post_age_sex_split[(sex_split == 1 | age_split == 1), crosswalk_parent_seq := seq]
  post_age_sex_split[(sex_split == 1 | age_split == 1), seq := NA]
} else {
  post_age_sex_split[(sex_split == 1 | age_split == 1 | cv_cw == 1), crosswalk_parent_seq := seq]
  post_age_sex_split[(sex_split == 1 | age_split == 1 | cv_cw == 1), seq := NA]
}

# check final values
summary(post_age_sex_split$val)
summary(post_age_sex_split$variance)
summary(post_age_sex_split$sample_size)
table(post_age_sex_split$age_group_id, paste0(post_age_sex_split$age_start, '-', post_age_sex_split$age_end), useNA='ifany')
table(post_age_sex_split$sex, useNA='ifany')

# save file
write.csv(post_age_sex_split, file=paste0(output_path, rei, "_age_sex_split_bvid_", bvid, ".csv"), row.names=F)
