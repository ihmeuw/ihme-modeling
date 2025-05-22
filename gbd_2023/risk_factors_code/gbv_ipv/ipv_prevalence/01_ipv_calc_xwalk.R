####################################################################################################################################################################
# 
# Purpose: Run crosswalk model - network meta-analysis for IPV alternate definitions
#
####################################################################################################################################################################

rm(list=ls())

#bundle version
bvid <- 'BV ID'

#crosswalk date
xw_date <- 'DATE'

#root filepath
root <- 'FILEPATH'


### 0. SET UP ######################################################################################################################################################

#libraries + central functions
pacman::p_load(data.table, dplyr, openxlsx, ggplot2, stringr, reticulate)
invisible(sapply(list.files("FILEPATH", full.names = T), source))

#crosswalk package
reticulate::use_python("FILEPATH")
cw <- import("crosswalk")
library(crosswalk002)

#custom function to fill out mean, cases, and sample sizes
get_cases_sample_size <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

#custom function to calculate standard error
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure %in% c("prevalence","proportion"), standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}


### 1. READ IN BUNDLE DATA ################################################################################################################################

#get bundle data
bv <- get_bundle_version(bvid)

#make a copy and preserve original variable name
data <- copy(bv)
data[, og_var := var]


### 2. GENERAL FORMATTING AND SET UP ######################################################################################################################

#fill in missing data
setnames(data, 'val', 'mean')
data[, c('mean', 'standard_error', 'year_start', 'year_end', 'cases', 'sample_size', 'age_start', 'age_end')] <- lapply(data[, c('mean', 'standard_error', 'year_start', 'year_end', 'cases', 'sample_size', 'age_start', 'age_end')], as.numeric)
data[is.na(cases) & !is.na(sample_size) & !is.na(mean), cases := sample_size * mean]
data <- get_cases_sample_size(data)
data <- get_se(data)

#save copy just in case
data_raw <- copy(data)

#list of alt defs we will look for this network (the unique best available definitions in the data set)
xwalk_defs <- c('any_sexpv_lifetime', 'any_physpv_lifetime', 'any_sexpv_pastyr', 'any_physpv_pastyr', 
                'anysex_anyphys_ipv_lifetime', 'anysex_anyphys_ipv_pastyr',
                'severe_physpv_lifetime', 'severe_physpv_pastyr')



### 3. EDIT VARS TO MATCH LOGIC ###########################################################################################################################

#remove force types [not currently modeled]
data[grepl('_any_force_incl_attempts', var), var := str_replace(var, '_any_force_incl_attempts', '')]
data[grepl('_physical_force_incl_attempts', var), var := str_replace(var, '_physical_force_incl_attempts', '')]
data[grepl('_coerce_force', var), var := str_replace(var, '_coerce_force', '')]
data[grepl('_physical_force', var), var := str_replace(var, '_physical_force', '')]
data[grepl('_any_force', var), var := str_replace(var, '_any_force', '')]
data[grepl('_attempts_only', var), var := str_replace(var, '_attempts_only', '')]

#remove perp ids [not currently modeled]
data[grepl('_anypart', var), var := str_replace(var, '_anypart', '')]
data[grepl('_currpart', var), var := str_replace(var, '_currpart', '')]
data[grepl('_prevpart', var), var := str_replace(var, '_prevpart', '')]
data[grepl('_expart', var), var := str_replace(var, '_expart', '')]

#remove granularity of sv extractions
data[grepl('lifetime', var) & !grepl('phys', var) & (grepl('int_sexpv_', var) | grepl('acts_sexpv_', var) | grepl('nonintsex_', var) | grepl('noncontactsex_', var) | grepl('intsex_', var) | grepl('anysex_', var)), var := 'any_sexpv_lifetime']
data[grepl('pastyr', var) & !grepl('phys', var) & (grepl('int_sexpv_', var) | grepl('acts_sexpv_', var) | grepl('nonintsex_', var) | grepl('noncontactsex_', var) | grepl('intsex_', var) | grepl('anysex_', var)), var := 'any_sexpv_pastyr']
data[var %in% c('anysex_lifetime','contactsex_lifetime'), var := 'any_sexpv_lifetime']
data[var %in% c('anysex_pastyr', 'contactsex_pastyr'), var := 'any_sexpv_pastyr']

#aggregate physical violence [any | nonsev == any ; sev = sev]
data[grepl('lifetime', var) & !grepl('sex', var) & (grepl('nonseverephys_', var) | grepl('anyphys_', var) | grepl('nonsevere_physpv', var) | grepl('all_pv_', var)), var := 'any_physpv_lifetime']
data[grepl('pastyr', var) & !grepl('sex', var) & (grepl('nonseverephys_', var) | grepl('anyphys_', var) | grepl('nonsevere_physpv', var) | grepl('all_pv_', var)), var := 'any_physpv_pastyr']
data[var == 'severephys_lifetime', var := 'severe_physpv_lifetime']
data[var == 'severephys_pastyr', var := 'severe_physpv_pastyr']

#aggregate nonsev physical violence when it's with sexual violence
data[var %in% c('anysex_nonsevphys_ipv_lifetime', 'anysex_nonsevphys_ipv_pastyr'), var := str_replace(var, 'anysex_nonsevphys_', 'anysex_anyphys_')]
data[var %in% c('anysex_nonseverephys_ipv_lifetime', 'anysex_nonseverephys_ipv_pastyr'), var := str_replace(var, 'anysex_nonseverephys_', 'anysex_anyphys_')]
data[var %in% c('actsex_nonseverephys_ipv_lifetime', 'actsex_nonseverephys_ipv_pastyr'), var := str_replace(var, 'actsex_nonseverephys_', 'anysex_anyphys_')]
data[var %in% c('intsex_nonseverephys_ipv_lifetime', 'intsex_nonseverephys_ipv_pastyr'), var := str_replace(var, 'intsex_nonseverephys_', 'anysex_anyphys_')]

#correct any sex any phys
data[var %in% c('actsex_anyphys_ipv_lifetime', 'actsex_nonsevphys_ipv_lifetime', 'intsex_anyphys_ipv_lifetime', 'intsex_nonsevphys_ipv_lifetime', 'anyphys_contactsex_lifetime', 'anyphys_intsex_lifetime', 'intsex_anyphys_ipv_pastyr'), var := 'anysex_anyphys_ipv_lifetime']
data[var %in% c('actsex_anyphys_ipv_pastyr', 'actsex_nonsevphys_ipv_pastyr', 'anyphys_intsex_pastyr', 'intsex_nonsevphys_ipv_pastyr'), var := 'anysex_anyphys_ipv_pastyr']

#flip
data[grepl('anyphys_anysex_', var) & !grepl('ipv', var), var := str_replace(var, 'anyphys_anysex_', 'anysex_anyphys_ipv_')]

#final fixes that didn't work
data[var == 'sex_phys_pv_pastyr', var := 'anysex_anyphys_ipv_pastyr']
data[og_var == 'any_phys_anypart_lifetime', var := 'any_physpv_lifetime']
data[og_var == 'any_phys_anypart_pastyr', var := 'any_physpv_pastyr']
data[og_var == 'any_sex_anypart_lifetime', var := 'any_sexpv_lifetime']
data[og_var == 'any_sex_anypart_pastyr', var := 'any_sexpv_pastyr']
data[nid == 142941 & og_var == 'ipv_phys', `:=` (group_review = 1, xwalk = 0, var = 'any_physpv_lifetime')]



### 4. FORMAT + CLEAN DATA ######################################################################################################################

#clean up extractions since we extrac more detail that we can currently model or use
data[, var := trimws(var)]
data <- data[var %in% xwalk_defs]

#map and filter sex_id
data[sex=='Female', sex_id:=2]
data <- data[sex_id==2]
data[age_start != 999, age_midpt:=(as.numeric(age_start)+as.numeric(age_end))/2]
data[age_start == 999, `:=` (age_midpt =(as.numeric(age_start_orig)+as.numeric(age_end_orig))/2, age_start = age_start_orig, age_end = age_end_orig)]

#create year_id variable
data[, year_id:=floor((year_start+year_end)/2)]

#set zero mean adjustment value 
mean_adj_val <- quantile(data[mean!=0]$mean, probs=0.025)
setnames(data, 'standard_error', 'se')

#remove unnecessary columns
data <- data[, c('age_start', 'age_end', 'age_midpt', 'ihme_loc_id', 'nid', 'year_id', 'survey_name', 'var', 'mean', 'se')]
data_orig <- copy(data) #save an unmatched version of the data to apply adjustments to 

#drop subnational locations
data <- data[!grepl('_', ihme_loc_id)]

#get subsets of gs and alternate defs
ref_subset <- copy(data) 
alts_subset <- data[!var=='anysex_anyphys_ipv_lifetime']

#set names accordingly
setnames(ref_subset, c('mean', 'se', 'var'), c('ref_mean', 'ref_se', 'refvar'))
setnames(alts_subset, c('mean', 'se', 'var'), c('alt_mean', 'alt_se', 'altvar'))

#merge back onto each other
matched <- merge(ref_subset, alts_subset, by=c('age_start', 'age_end', 'ihme_loc_id', 'nid', 'year_id', 'survey_name', 'age_midpt'), allow.cartesian = T)

#get rid of rows that match the same def
onepoint_data <- matched[altvar==refvar]
matched <- matched[!altvar==refvar]

#remove duplicate indirect comparisons (B:C == C:B)
data <- copy(matched)
alt_defs <- unique(alts_subset$altvar)
for (i in 1:length(alt_defs)){
  for (j in 1:length(alt_defs)){
    data[refvar==alt_defs[i] & altvar==alt_defs[j], comparison_pair:=paste0(alt_defs[i], ' to ', alt_defs[j])]
    data[refvar==alt_defs[j] & altvar==alt_defs[i], comparison_pair:=paste0(alt_defs[i], ' to ', alt_defs[j])]
  }
}
data[!is.na(comparison_pair), duplicate_pair := duplicated(comparison_pair), by=c('nid', 'age_start', 'age_end')]
data <- data[is.na(duplicate_pair) | duplicate_pair==FALSE]
data[, comparison_pair:=NULL]
data[, duplicate_pair:=NULL]

#save matched data
matched <- copy(data)

#change zeros to very small numbers bc 0 will not mathematically logit transform
matched <- matched[ref_mean<=mean_adj_val, ref_mean:=mean_adj_val]
matched <- matched[alt_mean<=mean_adj_val, alt_mean:=mean_adj_val]

#get logit calcs using the delta transform package
logit_alt_means <- as.data.table(delta_transform(mean=matched$alt_mean, sd=matched$alt_se, transformation='linear_to_logit'))
setnames(logit_alt_means, c('mean_logit', 'sd_logit'), c('logit_alt_mean', 'logit_alt_se'))
logit_ref_means <- as.data.table(delta_transform(mean=matched$ref_mean, sd=matched$ref_se, transformation='linear_to_logit'))
setnames(logit_ref_means, c('mean_logit', 'sd_logit'), c('logit_ref_mean', 'logit_ref_se'))

#bind back onto main data table
matched <- cbind(matched, logit_alt_means)
matched <- cbind(matched, logit_ref_means)

#logit
matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = matched, 
  alt_mean = "logit_alt_mean", alt_sd = "logit_alt_se",
  ref_mean = "logit_ref_mean", ref_sd = "logit_alt_se" )


### 5. FIT THE CW MODEL ######################################################################################################################

# specify the data for model fitting
df <- CWData(
  df = matched,             # dataset for metaregression
  obs = "logit_diff",       # column name for the observation mean
  obs_se = "logit_diff_se", # column name for the observation standard error
  alt_dorms = "altvar",     # column name of the variable indicating the alternative method
  ref_dorms = "refvar",     # column name of the variable indicating the reference method
  covs = list('age_midpt'),
  study_id = 'nid',          # name of the column indicating group membership, usually the matching groups
  add_intercept = TRUE
)

# fit the model
fit_ipv_network <- CWModel(
  cwdata = df,             # object returned by `CWData()`
  obs_type = "diff_logit", # "diff_log" or "diff_logit" depending on whether bounds are [0, Inf) or [0, 1]
  cov_models = list(       # specifying predictors in the model; see help(CovModel)
    CovModel("intercept"),
    CovModel(
      cov_name = "age_midpt",
      spline = XSpline(
        knots = c(0, 15, 20, 25, 35, 110), # outer knots must encompass the data 
        degree = 3L, # polynomial order (1=linear, 2=quadratic, 3=cubic)
        l_linear = TRUE, # linearity in the left tail
        r_linear = TRUE ))), # linearity in the right tail
  gold_dorm = "anysex_anyphys_ipv_lifetime",  # the level of `ref_dorms` that indicates it's the gold standard
  order_prior = list(c("any_sexpv_pastyr", "any_sexpv_lifetime"),
                     c('any_physpv_pastyr', 'any_physpv_lifetime'),
                     c('anysex_anyphys_ipv_pastyr', 'anysex_anyphys_ipv_lifetime'), 
                     c('any_sexpv_pastyr', 'anysex_anyphys_ipv_pastyr'),
                     c('any_sexpv_lifetime', 'anysex_anyphys_ipv_lifetime'),
                     c('any_physpv_pastyr', 'anysex_anyphys_ipv_pastyr'),
                     c('any_physpv_lifetime', 'anysex_anyphys_ipv_lifetime')),
  max_iter=100L, #default is 100
  inlier_pct=0.9 #set trimming to 10%
)


### 6. SAVE MODEL+ RESULTS ######################################################################################################################

#pickle file
py_save_object(object = fit_ipv_network, filename = paste0(root, 'FILEPATH', xw_date, '.pkl'), pickle = "dill")




