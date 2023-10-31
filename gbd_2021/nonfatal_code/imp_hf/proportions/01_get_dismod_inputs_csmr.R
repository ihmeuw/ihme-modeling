
##
## Author: USER, parts of code from USER, USER
## Date: DATE# 
##
##            1. Pull in MCOD ratios (percent etiology deaths with HF) and EMR (excess mortality for HF with an etiology)
##            2. Launch sbatch parallelized by location to get mean, upper, and lower CSMR estimates for each etiology
##            3. Use outputs from step 1 and 2 to calculate input proportions (deaths * MCOD / EMR)
##            4. Prep epi sheets and upload to the bundles
##

rm(list=ls())

date <- gsub("-", "_", Sys.Date())

pacman::p_load(plyr, data.table, parallel, RMySQL, openxlsx, ggplot2)


###### Paths, args
#################################################################################

## Central functions
central <- "FILEPATH"
for (i in list.files(central)) source(paste0(central, i))

## CVD output folder
cvd_path = "FILEPATH"

## Shell for submitting sbatch
shell <- "FILEPATH"

## Decomp, round info
gbd_round_id <- VALUE
decomp_step <- "VALUE"

# This flag will keep Sub-Saharan African locations in the prevalence estimates when FALSE and drop them when TRUE
drop_ssa <- T
# This flag will collapse csmr over all csmr years to one value when FALSE, and collpase csmr to estimation years when TRUE
multi_yr_csmr <- T
# This flag will collapse csmr per year to estimation year when FALSE, and map estimation year onto itself when TRUE
single_yr <- T


hund_attribution <- c("cvd_htn", "cvd_cmp_other", "cvd_cmp_alcoholic")

###### Functions, pulling central info
#################################################################################

## Metadata
metadata <- get_demographics(gbd_team = "VALUE", gbd_round_id = gbd_round_id)

if (multi_yr_csmr==F){
  year_ids <- unique(metadata$year_id)
} else{
  # Using estimation years
  year_ids <- c(1990, 1995, 2000, 2005, 2010, 2015, 2020)
  all_year_ids <- 1990:2022
  if (single_yr){
    # If using single_yr flag, collapse code still runs, but collapses each year onto itself
    all_year_ids <- year_ids
  }
}
all_year_ids <- 1990:2022
if (single_yr){
  all_year_ids <- year_ids
}
sex_ids  <- unique(metadata$sex_id)
age_group_ids = unique(metadata$age_group_id)

age_groups <- get_age_metadata(gbd_round_id = gbd_round_id, age_group_set_id = VALUE)[, .(age_group_years_start, age_group_id)]
ages <- get_age_metadata(VALUE, VALUE)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))

## HF etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list_2020.csv'))

# get age_groups and modelable_entity_name from MySQL db
db_con = fread(paste0(j, "FILEPATH"),
               stringsAsFactors = FALSE)

con <- dbConnect(dbDriver("MySQL"), 
                 username = db_con$username, 
                 password = db_con$pass, 
                 host = db_con$epi)

q1 = as.data.table(dbGetQuery(con, sprintf("QUERY")[modelable_entity_id %in% unique(na.omit(composite$prop_me_id))]

dbDisconnect(con)

composite <- merge(composite, q1, by.x="prop_me_id", by.y="modelable_entity_id", all=T)


###### Pull modeled quantities
#################################################################################

ids <- get_ids("cause")

## Pull in Italy EMR data and MCOD proportions
italy_final_cfr_path <- 'FILEPATH'
mcod_final_path <- 'FILEPATH'

## Read in MCOD, clean
mcod <- fread(mcod_final_path)
mcod <- merge(mcod, age_groups, by="age_group_years_start")
mcod <- merge(mcod, ids[, .(cause_id, acause, cause_name)], by.x = "etiology", by.y = "acause")
setnames(mcod, c("mcod_ratio", "pred_se"), c("estimate_mcod", "se_mcod"))
mcod[, etiology := gsub("acute_", "", etiology)]

# Read in EMR, clean
italy_cfr <- fread(italy_final_cfr_path)
italy_cfr <- merge(italy_cfr, ids[, .(cause_id, acause, cause_name)], by.x = "etiology", by.y = "acause")
italy_cfr <- merge(italy_cfr, age_groups, by="age_group_years_start")
setnames(italy_cfr, c("estimate", "pred_se"), c("estimate_emr", "se_emr"))
italy_cfr[, etiology := gsub("acute_", "", etiology)]

# Merge data sets
both <- merge(italy_cfr, mcod, by=c("cause_id", "age_group_id", "age_group_years_start", "sex_id", "cause_name", "etiology"), all=T)
both <- both[!is.na(estimate_mcod)]
both <- both[!is.na(estimate_emr)]

# certain causes have age cutoffs of 15, where all proportions below 15 will be set to 0.
AGE_RESTRICTED_CAUSES=c(VALUE, VALUE, VALUE, VALUE, VALUE, VALUE, VALUE, VALUE, VALUE, VALUE)
both[(cause_id %in% AGE_RESTRICTED_CAUSES) & (age_group_years_start<15), estimate_mcod := 0]
# nrvd causes have age cutoffs of 1, where all proportions below 1 will be set to 0.
AGE_RESTRICTED_UNDER1=c(VALUE)
both[(cause_id %in% AGE_RESTRICTED_UNDER1) & (age_group_years_start<1), estimate_mcod := 0] 

# write the cleaned dataset to be used later by post_dismod step to calculate correction factors
fwrite(both, paste0('FILEPATH', date, 'italy_mcod_adjustments.csv'))
setnames(both, "etiology", "acause")

###### Launch sbatch parallelized by locs to pull CSMR of the 21 subcauses
#################################################################################

# Was using location_set=VALUE, but dismod won't accept some of those loc_id's (ie, VALUE UK) so using pared down equivalent to help diagnostics run more smoothly
locations <- get_location_metadata(location_set_id=VALUE, gbd_round_id=gbd_round_id)
dismod_locs <- unique(metadata$location_id)
loc_choices <- locations[(level==VALUE) | (location_id %in%dismod_locs)]$location_id
n_jobs <- length(loc_choices)
slots <- 1
user <- Sys.getenv("USER")

param_map <- expand.grid(location_id = loc_choices)
write.csv(param_map, "FILEPATH", row.names=F)

# run child script in parallelization
if (multi_yr_csmr){
  
  outputDir <- paste0(cvd_path, '/pre_dismod/')
  dir.create(outputDir, showWarnings = FALSE)
  outputDir <- ifelse(single_yr, paste0(outputDir, format(Sys.time(), '%Y_%m_%d'), '/single_yr_csmr/'), paste0(outputDir, format(Sys.time(), '%Y_%m_%d'), '/multi_yr_csmr/'))
  dir.create(outputDir, showWarnings = FALSE, recursive=T)
  rscript <- "FILEPATH"

} else{
  
  outputDir <- paste0(cvd_path, '/pre_dismod/')
  dir.create(outputDir, showWarnings = FALSE)
  outputDir <- paste0(outputDir, format(Sys.time(), '%Y_%m_%d'))
  dir.create(outputDir, showWarnings = FALSE)
  rscript <- "FILEPATH"

}

code_command <- paste0(shell, " -s ", rscript, " FIELPATH ", outputDir, ' ', single_yr)
full_command <- paste0("sbatch -J hf_predismod -A proj_cvd --mem=4G -c 1 -t 00:30:00 -C archive -p all.q ",
                       "-a ", paste0("1-", n_jobs), " ",
                       "-o FILEPATH",user,"/output_log/%x.o%j ",
                       "-e FILEPATH",user,"/error_log/%x.e%j ",
                       code_command)

print(full_command)
system(full_command)


bad_locs <- NULL
for (loc in loc_choices) {
  if (!file.exists(paste0(outputDir, '/codcorrect_', loc, '.rds'))) bad_locs <- c(bad_locs, loc)
}

if (length(bad_locs) > 0) {
  param_map <- expand.grid(location_id = bad_locs)
  write.csv(param_map, "FILEPATH", row.names=F)
  n_jobs <- length(bad_locs)

  code_command <- paste0(shell, " -s ", rscript, " FIELPATH ", outputDir)
  full_command <- paste0("sbatch -J hf_predismod -A proj_cvd --mem=4G -c 1 -t 00:30:00 -C archive -p all.q ",
                         "-a ", paste0("1-", n_jobs), " ",
                         "-o FILEPATH",user,"/output_log/%x.o%j ",
                         "-e FILEPATH",user,"/error_log/%x.e%j ",
                         code_command)
  
  print(full_command)
  system(full_command)
}

## Check that all locations have finished
file_list <- NULL
missing_list <- c()
for (loc in loc_choices) file_list <- c(file_list, paste0(outputDir, '/codcorrect_', loc, '.rds'))
for (each in file_list) if (!file.exists(each)) stop("You're missing at least some locations.")

## Pull together all locations
csmr <- rbindlist(lapply(file_list, readRDS), use.names=T)
csmr <- csmr[!is.na(cause_id)]

###### Use CSMR, modeled quantities, to estimate proportions due to 6 proportion models
#################################################################################

# No longer using endo_other for endo_thyroid
both <- both[acause!='endo_other']

csmr <- merge(csmr, both, by=c("acause", "sex_id", "age_group_id"))
csmr[(acause %in% hund_attribution), estimate_mcod := 1]
csmr[estimate_emr < 1e-3, estimate_emr := 1e-3]

csmr[, prev_adj_final := (csmr_sum * estimate_mcod)/estimate_emr]
csmr[, prev_adj_int := (csmr_sum * estimate_mcod)]
csmr[, csmr := csmr_sum]

csmr[, den := sum(prev_adj_final), by = c("age_group_id", "location_id", "sex_id")]
csmr[, percent := prev_adj_final/den]

new_comp <- copy(composite)
new_comp[, acause := gsub("acute\\_|chronic\\_", "", acause)]

csmr <- merge(csmr, unique(new_comp[, .(acause, prop_bundle_id)]), by="acause")

if (multi_yr_csmr){
  csmr[,year_id := collapse_yr]
  fwrite(csmr, paste0('FILEPATH', date, 'csmr_2020_', ifelse(single_yr, 'singleyr.csv', 'multiyr.csv')))
  
  if (single_yr){
    pop_yr_ids <- year_ids
  }else{
    pop_yr_ids <- all_year_ids
  }
  
  pop <- get_population(age_group_id = unique(csmr$age_group_id), location_id = unique(csmr$location_id), sex_id = c(1, 2), 
                        year_id = pop_yr_ids, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
  pop[, collapse_yr := sapply(year_id, collapse_yrs, year_ids)]
  pop[, avg_pop := mean(population), by=c("age_group_id", "location_id", "sex_id", "collapse_yr")]
  pop[, year_id := collapse_yr]
  pop <- unique(pop[, .(age_group_id, sex_id, location_id, avg_pop, year_id)])
  csmr <- merge(csmr, pop, by=c("age_group_id", "sex_id", "location_id", "year_id"))
  
  csmr[, prev := sum(prev_adj_final), by=c("age_group_id", "sex_id", "location_id", "prop_bundle_id", "year_id")]
  csmr[, prev_den := sum(prev_adj_final), by=c("age_group_id", "sex_id", "location_id", "year_id")]
  csmr[, prop_final := prev/prev_den]
  
  csmr[, se_cod := sqrt(sum(se_mcod^2)), by=c("age_group_id", "sex_id", "location_id", "prop_bundle_id", "year_id")]
  csmr[, se_emr := sqrt(sum(se_emr^2)), by=c("age_group_id", "sex_id", "location_id", "prop_bundle_id", "year_id")]
  csmr[, se_csmr := sqrt(sum(csmr_sum_se^2)), by=c("age_group_id", "sex_id", "location_id", "prop_bundle_id", "year_id")]
  
  csmr <- unique(csmr[, .(sex_id, age_group_id, location_id, prop_final, se_cod, se_emr, se_csmr, prop_bundle_id, year_id)])
  csmr[, standard_error := sqrt(se_cod^2 + se_emr^2 + se_csmr^2)]
  
  ## Per Theo (Catherine has the emails) this shouldn't be >.25
  csmr[standard_error>.25, standard_error := .25]
  csmr[, standard_error := standard_error * prop_final]
  
  ssa_ids <- locations[super_region_id==VALUE]$location_id
  LOCS <- get_ids(table='location')[,.(location_id, location_name)]
  
  if (drop_ssa) {
    upload <- csmr[!(location_id%in%ssa_ids),] 
  } else {
    upload <- csmr
  }
  
  upload[sex_id == 1, sex:="Male"]
  upload[sex_id == 2, sex:="Female"]
  
  upload <- merge(upload, locations[, .(location_id, location_name)], by="location_id", all.x = T, all.y=F)
  upload[, c("se_cod", "se_emr", "se_csmr") := NULL]
  setnames(upload, "prop_final", "mean")
  
  fwrite(upload, file=paste0('FILEPATH', date, 'csmr_2020_', ifelse(single_yr, 'singleyr_0.csv', 'multiyr_0.csv')))
  
  upload <- fread(paste0('FILEPATH', date, 'csmr_2020_', ifelse(single_yr, 'singleyr_0.csv', 'multiyr_0.csv')))
  
} else{
  fwrite(csmr, paste0('FILEPATH', date, 'csmr_2020.csv'))
  
  ## We need to take things from prevalence space to case-space. Take average population across all years and multiply by prevalence
  pop <- get_population(age_group_id = unique(csmr$age_group_id), location_id = unique(csmr$location_id), sex_id = c(1, 2), 
                        year_id = year_ids, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
  pop[, sum_pop := sum(population), by=c("age_group_id", "location_id", "sex_id")]
  pop[, avg_pop := sum_pop/length(unique(pop$year_id))]
  pop <- unique(pop[, .(age_group_id, sex_id, location_id, avg_pop)])
  csmr <- merge(csmr, pop, by=c("age_group_id", "sex_id", "location_id"))
  
  csmr[, cases := prev_adj_final * avg_pop]
  csmr[, sum_cases := sum(cases), by=c("age_group_id", "sex_id", "location_id", "prop_bundle_id")]
  csmr[, sum_cases_den := sum(cases), by=c("age_group_id", "sex_id", "location_id")]
  csmr[, prop_final := sum_cases/sum_cases_den]
  
  csmr[, prev := sum(prev_adj_final), by=c("age_group_id", "sex_id", "location_id", "prop_bundle_id")]
  csmr[, prev_den := sum(prev_adj_final), by=c("age_group_id", "sex_id", "location_id")]
  csmr[, prop_final := prev/prev_den]
  
  csmr[, se_cod := sqrt(sum(se_mcod^2)), by=c("age_group_id", "sex_id", "location_id", "prop_bundle_id")]
  csmr[, se_emr := sqrt(sum(se_emr^2)), by=c("age_group_id", "sex_id", "location_id", "prop_bundle_id")]
  csmr[, se_csmr := sqrt(sum(csmr_sum_se^2)), by=c("age_group_id", "sex_id", "location_id", "prop_bundle_id")]
  
  csmr <- unique(csmr[, .(sex_id, age_group_id, location_id, prop_final, se_cod, se_emr, se_csmr, prop_bundle_id)])
  csmr[, standard_error := sqrt(se_cod^2 + se_emr^2 + se_csmr^2)]
  
  ## Per Theo (Catherine has the emails) this shouldn't be >.25
  csmr[standard_error>.25, standard_error := .25]
  csmr[, standard_error := standard_error * prop_final]
  
  ssa_ids <- locations[super_region_id==VALUE]$location_id
  LOCS <- get_ids(table='location')[,.(location_id, location_name)]
  
  if (drop_ssa) {
    upload <- csmr[!(location_id%in%ssa_ids),] 
  } else {
    upload <- csmr
  }
  
  upload[sex_id == 1, sex:="Male"]
  upload[sex_id == 2, sex:="Female"]
  
  upload <- merge(upload, locations[, .(location_id, location_name)], by="location_id", all.x = T, all.y=F)
  upload[, c("se_cod", "se_emr", "se_csmr") := NULL]
  setnames(upload, "prop_final", "mean")
  
  fwrite(upload, file=paste0('FILEPATH', date, 'csmr_2020_0.csv'))
  
  upload_ <- fread(paste0('FILEPATH', date, 'csmr_2020_0.csv'))
  
  
}


###### Prep epi sheets and upload to the 6 proportion bundles
#################################################################################

upload <- fread('FILEPATH')

dir.create(paste0(cvd_path, 'FILEPATH'), showWarnings = F)

prep_sheet <- function(bundle){
  
  df <- copy(upload)[prop_bundle_id==bundle,]
  df <- as.data.table(df)

  df <- merge(df, ages[, .(age_start, age_end, age_group_id)], by = "age_group_id")
    
  df[, year_start := year_id]
  df[, year_end := year_id]
  df[, nid := VALUE]
  
  df <- df[, .(age_group_id, sex, year_start, year_end, location_id, prop_bundle_id, mean, standard_error, nid, age_start, age_end)]
  
  df[,':='(unit_type='VALUE', unit_type_value=VALUE, measure_issue=VALUE, uncertainty_type='Standard error', uncertainty_type_value=NA,
           extractor='USER', representative_name="Nationally and subnationally representative", urbanicity_type="Unknown", response_rate=NA, sampling_type=NA, 
           recall_type="Point", recall_type_value=1.0, case_name=NA, case_definition=NA, case_diagnostics=NA, 
           note_modeler='VALUE', cv_hospital=0, cv_marketscan=0, cv_low_income_hosp=0, 
           cv_high_income_hosp=0, is_outlier=0, cases=NA, measure='proportion',sample_size=NA, effective_sample_size=NA, source_type="VALUE",
           underlying_nid= NA, input_type='extracted', design_effect=NA, unit_value_as_published=1, date_inserted=date(), last_updated=date(), 
           inserted_by='USER', last_updated_by='USER', upper=NA, lower=NA, seq=NA)]
  out_path <- paste0(cvd_path, 'FILEPATH', bundle, '_', format(Sys.Date(), "%Y%m%d"), '.xlsx')
  print(dim(df))
  write.xlsx(df, out_path, sheetName='extraction')
  upload_sheet <- T
  if (upload_sheet == T){
    if (bundle==VALUE){
      upload_bundle_data(bundle_id=bundle, filepath=out_path)
    } else{
      upload_bundle_data(bundle_id=bundle, filepath=out_path, decomp_step = "iterative", gbd_round_id = 7)
    }
  } else {
    message ('Bundle', bundle, ' ', unique(validate_input_sheet(bundle, out_path, "FILEPATH")$status), ' validation!') 
  }
}

# Removes current prop_bundle data
remove_bundle_data <- function(){
  for (id in unique(upload$prop_bundle_id)) {
    
    print(id)
    if (id==VALUE){
      dt_s <- get_bundle_data(bundle_id = id)
    } else{
      dt_s <- get_bundle_data(bundle_id = id, decomp_step = "VALUE", gbd_round_id = VALUE)
    }
    dt_s <- dt_s[note_modeler %like% "Proportion generated"]
    dt_s <- data.table(seq=dt_s$seq)
    write.xlsx(dt_s, "FILEPATH", sheetName="extraction")
    
    if (id==VALUE){
      upload_bundle_data(bundle_id = id, filepath = "FILEPATH")
    } else{
    upload_bundle_data(bundle_id = id, decomp_step = "VALUE", filepath = "FILEPATH", gbd_round_id = VALUE)
    }
  }
}

## Need to remove previous bundle before uploading if bundle data has changed
remove_bundle_data()

## Upload data into bundle
system.time(mclapply(X=unique(upload$prop_bundle_id), FUN=prep_sheet, mc.cores = 7)) #5 minutes on a screen w/ 50G and 10 threads. X

source(paste0(central, "save_bundle_version.R"))
source(paste0(central, "get_bundle_version.R"))
source(paste0(central, "save_crosswalk_version.R"))

xws <- data.table(bid = unique(upload$prop_bundle_id), bvid = 0, xwid = 0)
source("VALUE")
source(paste0(central, "get_crosswalk_version.R"))
source(paste0(central, "get_version_quota.R"))
source(paste0(central, "prune_version.R"))



# May need to prune bundle versions to avoid quota. Check quota first
bundles <- c(VALUE, VALUE, VALUE, VALUE, VALUE, VALUE, VALUE)
for (idx in bundles){
  dtype <- 'bundle_version'
  quota <- get_version_quota(idx, flatten = F, data_type=dtype)
  print(paste0('Bundle: ', idx))
  print(paste0('Number of active versions: ', sum(quota$status!='delete')))
  print(paste0('Largest bvid: ', max(quota$data_lookup_id)))
  print(paste0('Largest associated w/ model: ', max(quota[status=='associated with model']$data_lookup_id)))
  print('---------------------')
}

quota_flat <-  get_version_quota(VALUE, data_type = 'crosswalk_version', flatten = F)

test <- get_crosswalk_version(VALUE)

for (bundle_id in test){
  quota_flat <-  get_version_quota(bundle_id, data_type = 'bundle_version', flatten = F)
  bundle_ver_ids <- max(quota_flat$data_lookup_id)
  pruned <- prune_version(bundle_id = bundle_id, bundle_version_id = bundle_ver_ids)
  print(paste0('Bundle_id: ', bundle_id, ' had xw_id ', bundle_ver_ids, ' pruned.'))
}


bundle_id <- VLAUE
bundle_ver_ids <- c(VALUE, VALUE)
pruned <- prune_version(bundle_id = bundle_id, bundle_version_id = bundle_ver_ids)
print(paste0('Bundle_id: ', bundle_id, ' had xw_id ', bundle_ver_ids, ' pruned.'))

for (id in unique(upload$prop_bundle_id)){
  
  print(id)
  if (id==VALUE){
    hold <- save_bundle_version(id)
  } else{
    hold <- save_bundle_version(id, gbd_round_id = VALUE, decomp_step = 'VALUE')
  }

  xws[bid==id, bvid := hold$bundle_version_id]
}

write.csv(xws, paste0(cvd_path, "/pre_dismod/proportion_ids.csv"))


