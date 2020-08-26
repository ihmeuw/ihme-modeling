
## 
## Purpose: The first step of the HF custom modeling process. This script generates inputs for HF Dismod proportion models. 
##            1. Pull in MCOD ratios (percent etiology deaths with HF) and EMR (excess mortality for HF with an etiology)
##            2. Launch qsub parallelized by location to get mean, upper, and lower CSMR estimates for each etiology
##            3. Use outputs from step 1 and 2 to calculate input proportions (deaths * MCOD / EMR)
##            4. Prep epi sheets and upload to the bundles
##

date <- gsub("-", "_", Sys.Date())

pacman::p_load(plyr, data.table, parallel, RMySQL, openxlsx, ggplot2)


###### Paths, args
#################################################################################

## Central functions
central <- "FILEPATH"

source(paste0(central, "get_draws.R"))
source(paste0(central, "get_location_metadata.R"))
source(paste0(central, "get_ids.R"))
source(paste0(central, "get_demographics.R"))
source(paste0(central, "upload_bundle_data.R"))
source(paste0(central, "validate_input_sheet.R"))
source(paste0(central, "save_crosswalk_version.R"))
source(paste0(central, "get_age_metadata.R"))
source(paste0(central, "get_population.R"))
source(paste0(central, "get_bundle_data.R"))

## job_hold function and other cluster tools
source("cluster_tools.r")
source("ubcov_tools.r")

## CVD output folder
cvd_path = "FILEPATH"

## Shell for submitting qsubs
shell <- 'FILEPATH'

## Decomp, round info
gbd_round_id <- 6
decomp_step <- "step4"

# ID table
IDs <-data.table(name = c("VALUES"), 
                 bundle_id = c("VALUES"),
                 modelable_entity_id = c("VALUES"),
                 nid = c("VALUES"),
                 cause_id = c("VALUES")
)

drop_ssa <- T

###### Functions, pulling central info
#################################################################################

## Metadata
metadata <- get_demographics(gbd_team = "epi", gbd_round_id = 6)

year_ids <- unique(metadata$year_id)
sex_ids  <- unique(metadata$sex_id)
age_group_ids = unique(metadata$age_group_id)

age_groups <- get_age_metadata(gbd_round_id = gbd_round_id, age_group_set_id = 12)[, .(age_group_years_start, age_group_id)]
setnames(age_groups, "age_group_years_start", "age_start")
age_groups[age_group_id %in% c(2, 3, 4), age_start := 0]

## HF etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list.csv'))
composite[is.na(composite_id), composite_id := cause_id]
composite <- merge(composite, IDs, by.x="composite_id", by.y="cause_id", all=T)

# get age_groups and modelable_entity_name from MySQL db
db_con = fread("FILEPATH",
               stringsAsFactors = FALSE)

con <- dbConnect(dbDriver("MySQL"), 
                 username = db_con$username, 
                 password = db_con$pass, 
                 host = db_con$host)

q1 = as.data.table(dbGetQuery(con, sprintf("QUERY")))[modelable_entity_id %in% unique(na.omit(IDs$modelable_entity_id))]

dbDisconnect(con)

composite <- merge(composite, q1, by="modelable_entity_id", all=T)


###### Pull modeled quantities
#################################################################################

## Pull in Italy EMR data and MCOD proportions
italy_final_cfr_path <- 'FILEPATH'
mcod_final_path <- 'FILEPATH'

mcod <- fread(mcod_final_path)

mcod <- merge(mcod, unique(composite[!(is.na(cause_id)), .(acause, cause_id, cause_name)]), by.x="etiology", by.y="acause", all.x=T, all.y=F)
mcod <- merge(mcod, age_groups, by.x="age_group_years_start", by.y="age_start")

italy_cfr <- fread(italy_final_cfr_path)

italy_cfr <- merge(italy_cfr, unique(composite[!(is.na(cause_id)), .(acause, cause_id, cause_name)]), by.x="etiology", by.y="acause", all.x=T, all.y=F)
italy_cfr <- merge(italy_cfr, age_groups, by.x="age_group_years_start", by.y="age_start")

both <- merge(italy_cfr, mcod, by=c("cause_id", "age_group_id", "age_group_years_start", "sex_id", "cause_name", "etiology"), all=T)
both[etiology == "cvd_cmp_myocarditis", mcod_ratio := 1]

# certain causes have age cutoffs of 15, where all proportionsbelow 15 will be set to 0.
AGE_RESTRICTED_CAUSES=c(493,498,509,511,512,513,514,516,970,938) # 968, 969, 970
both[(cause_id %in% AGE_RESTRICTED_CAUSES) & (age_group_years_start<15), mcod_ratio := 0]
# nrvd causes have age cutoffs of 1, where all proportions below 1 will be set to 0.
AGE_RESTRICTED_UNDER1=c(492)
both[(cause_id %in% AGE_RESTRICTED_UNDER1) & (age_group_years_start<1), mcod_ratio := 0] 

# write the cleaned dataset to be used later by post_dismod step to calculate correction factors
fwrite(both, 'FILEPATH')


###### Launch qsub parallelized by locs to pull CSMR of the 21 subcauses
#################################################################################

## Only launch for non-composite causes
COD_CAUSE_IDS <- composite[!(is.na(cause_id)), unique(cause_id)]

locations <- get_location_metadata(location_set_id=35, gbd_round_id=gbd_round_id)
dismod_locs <- unique(metadata$location_id)
loc_choices <- locations[(level==3) | (location_id %in%dismod_locs)]$location_id
n_jobs <- length(loc_choices)
slots <- 1
user <- Sys.getenv("USER")

## Save the parameters as a csv so then you can index the rows to find the appropriate parameters
param_map <- expand.grid(location_id = loc_choices)
write.csv(param_map, "FILEPATH", row.names=F)

# run child script in parallelization
outputDir <- "FILEPATH"
dir.create(outputDir, showWarnings = FALSE)
outputDir <- paste0(outputDir, format(Sys.time(), '%Y_%m_%d'))
dir.create(outputDir, showWarnings = FALSE)

rscript <- "get_csmr.R"
code_command <- paste0(shell, " ", rscript, " FILEPATH ", outputDir)
full_command <- paste0("qsub -l m_mem_free=VALUE -l fthread=VALUE ",
                       "-N hf_predismod ",
                       "-t ", paste0("1:", n_jobs), " ",
                       "-o FILEPATH ",
                       "-e FILEPATH ",
                       code_command)

print(full_command)
system(full_command)

## Wait until all jobs are done
job_hold("hf_predismod")

bad_locs <- NULL
for (loc in loc_choices) {
  if (!file.exists(paste0(outputDir, '/codcorrect_', loc, '.rds'))) bad_locs <- c(bad_locs, loc)
}

if (length(bad_locs) > 0) {
  ## Save the parameters as a csv so then you can index the rows to find the appropriate parameters
  param_map <- expand.grid(location_id = bad_locs)
  write.csv(param_map, "FILEPATH", row.names=F)
  n_jobs <- length(bad_locs)
  
  rscript <- "FILEPATH"
  code_command <- paste0(shell, " ", rscript, " FILEPATH ", outputDir)
  full_command <- paste0("qsub -l m_mem_free=VALUE -l fthread=VALUE ",
                         "-N hf_predismod ",
                         "-t ", paste0("1:", n_jobs), " ",
                         "-o FILEPATH ",
                         "-e FILEPATH ",
                         code_command)
  
  print(full_command)
  system(full_command)
}

## Check that all locations have finished
file_list <- NULL
for (loc in loc_choices) file_list <- c(file_list, paste0(outputDir, '/codcorrect_', loc, '.rds'))
for (each in file_list) if (!file.exists(each)) stop("You're missing at least some locations.")


## Pull together all locations
csmr <- rbindlist(lapply(file_list, readRDS), use.names=T)


###### Use CSMR, modeled quantities, to estimate proportions due to 6 proportion models
#################################################################################

csmr <- merge(csmr, both, by=c("cause_id", "sex_id", "age_group_id"))
csmr[, prev_adj_final := (csmr_sum * mcod_ratio)/estimate]
csmr[, prev_adj_int := (csmr_sum * mcod_ratio)]
csmr[, csmr := csmr_sum]

csmr[etiology == "cvd_ihd", bundle_id := "VALUE"]
csmr[etiology == "cvd_htn", bundle_id := "VALUE"]
csmr[etiology %in% c("cvd_cmp_alcoholic", "cvd_cmp_myocarditis", "cvd_cmp_other"), bundle_id := "VALUE"]
csmr[etiology %in% c("resp_copd", "resp_pneum_silico", "resp_pneum_asbest", "resp_pneum_coal", "resp_pneum_other", "resp_interstitial"), bundle_id := "VALUE"]
csmr[etiology %in% c("cvd_rhd"), bundle_id := "VALUE"]
csmr[etiology %in% c("cvd_valvu_other", "cvd_endo", "cvd_other", "hemog_thalass", "hemog_g6pd", "hemog_other", "endo", "cong_heart"), bundle_id := "VALUE"]

fwrite(csmr, 'FILEPATH')

## We actually need to take things from prevalence space to case-space. Take average population across all years and multiply by prevalence
pop <- get_population(age_group_id = unique(csmr$age_group_id), location_id = unique(csmr$location_id), sex_id = c(1, 2), 
                      year_id = 1990:max(csmr$year_end), decomp_step = decomp_step, gbd_round_id = gbd_round_id)
pop[, sum_pop := sum(population), by=c("age_group_id", "location_id", "sex_id")]
pop[, avg_pop := sum_pop/length(unique(pop$year_id))]
pop <- unique(pop[, .(age_group_id, sex_id, location_id, avg_pop)])
csmr <- merge(csmr, pop, by=c("age_group_id", "sex_id", "location_id"))

csmr[, cases := prev_adj_final * avg_pop]
csmr[, sum_cases := sum(cases), by=c("age_group_id", "sex_id", "location_id", "bundle_id")]
csmr[, sum_cases_den := sum(cases), by=c("age_group_id", "sex_id", "location_id")]
csmr[, prop_final := sum_cases/sum_cases_den]

csmr[, prev := sum(prev_adj_final), by=c("age_group_id", "sex_id", "location_id", "bundle_id")]
csmr[, prev_den := sum(prev_adj_final), by=c("age_group_id", "sex_id", "location_id")]
csmr[, prop_final := prev/prev_den]

csmr[, se_cod := sqrt(sum(pred_se.x^2)), by=c("age_group_id", "sex_id", "location_id", "bundle_id")]
csmr[, se_emr := sqrt(sum(pred_se.y^2)), by=c("age_group_id", "sex_id", "location_id", "bundle_id")]
csmr[, se_csmr := sqrt(sum(csmr_sum_se^2)), by=c("age_group_id", "sex_id", "location_id", "bundle_id")]

csmr <- unique(csmr[, .(sex_id, age_group_id, location_id, prop_final, se_cod, se_emr, se_csmr, bundle_id)])
csmr[, standard_error := sqrt(se_cod^2 + se_emr^2 + se_csmr^2)]

csmr[standard_error>.25, standard_error := .25]
csmr[, standard_error := standard_error * prop_final]

ssa_ids <- locations[super_region_id==166]$location_id
LOCS <- get_ids(table='location')[,.(location_id, location_name)]

if (drop_ssa) {
  upload <- csmr[!(location_id%in%ssa_ids),] 
} else {
  upload <- csmr
}

upload[sex_id == 1, sex:="Male"]
upload[sex_id == 2, sex:="Female"]

upload <- merge(upload, locations, by="location_id", all.x = T, all.y=F)
upload[, c("se_cod", "se_emr", "se_csmr") := NULL]
setnames(upload, "prop_final", "mean")

fwrite(upload, file='FILEPATH')

upload <- upload[!(location_id %in% c(221, 141))] # Mali, Egypt


###### Prep epi sheets and upload to the 6 proportion bundles
#################################################################################

dir.create("FILEPATH", showWarnings = F)

prep_sheet <- function(bundle){
  
  df <- join(upload, IDs[bundle_id==bundle], type='inner')
  df <- as.data.table(df)
  if (T) {
    df[age_group_id==2, ':='(age_start=0.0000000, age_end=0.0191781)]
    df[age_group_id==3, ':='(age_start=0.0191781, age_end=0.0767123)]
    df[age_group_id==4, ':='(age_start=0.0767123, age_end=1)]
    df[age_group_id==5, ':='(age_start=1,age_end=5)]
    df[age_group_id==6, ':='(age_start=5, age_end=10)]
    df[age_group_id==7, ':='(age_start=10, age_end=15)]
    df[age_group_id==8, ':='(age_start=15, age_end=20)]
    df[age_group_id==9, ':='(age_start=20, age_end=25)]
    df[age_group_id==10, ':='(age_start=25, age_end=30)]
    df[age_group_id==11, ':='(age_start=30, age_end=35)]
    df[age_group_id==12, ':='(age_start=35, age_end=40)]
    df[age_group_id==13, ':='(age_start=40, age_end=45)]
    df[age_group_id==14, ':='(age_start=45, age_end=50)]
    df[age_group_id==15, ':='(age_start=50, age_end=55)]
    df[age_group_id==16, ':='(age_start=55, age_end=60)]
    df[age_group_id==17, ':='(age_start=60, age_end=65)]
    df[age_group_id==18, ':='(age_start=65, age_end=70)]
    df[age_group_id==19, ':='(age_start=70, age_end=75)]
    df[age_group_id==20, ':='(age_start=75, age_end=80)]
    df[age_group_id==30, ':='(age_start=80, age_end=85)]
    df[age_group_id==31, ':='(age_start=85, age_end=90)]
    df[age_group_id==32, ':='(age_start=90, age_end=95)]
    df[age_group_id==235, ':='(age_start=95, age_end=125)]
  }
  
  df[, year_start := 1990]
  df[, year_end := 2019]
  df <- df[, .(age_group_id, sex, year_start, year_end, location_id, cause_id, mean, standard_error, nid, age_start, age_end)]
  df[,':='(unit_type='Person', unit_type_value=2.0, measure_issue=0.0, uncertainty_type='Standard error', uncertainty_type_value=NA,
           extractor='USER', representative_name="Nationally and subnationally representative", urbanicity_type="Unknown", response_rate=NA, sampling_type=NA, 
           recall_type="Point", recall_type_value=1.0, case_name=NA, case_definition=NA, case_diagnostics=NA, 
           note_modeler='Proportion generated from CSMR, MCOD data, and Excess Mortality Rate, resubmission', cv_hospital=0, cv_marketscan=0, cv_low_income_hosp=0, 
           cv_high_income_hosp=0, is_outlier=0, cases=NA, measure='proportion',sample_size=NA, effective_sample_size=NA, source_type="Mixed or estimation",
           underlying_nid= NA, input_type='extracted', design_effect=NA, unit_value_as_published=1, date_inserted=date(), last_updated=date(), 
           inserted_by='USER', last_updated_by='USER', upper=NA, lower=NA, seq=NA)]
  out_path <- paste0("FILEPATH", '.xlsx')
  print(dim(df))
  write.xlsx(df, out_path, sheetName='extraction')
  upload_sheet <- T
  if (upload_sheet == T){
    upload_bundle_data(bundle_id=bundle, filepath=out_path, decomp_step = "step4", gbd_round_id = 6)
  } else {
    message ('Bundle', bundle, ' ', unique(validate_input_sheet(bundle, out_path, "FILEPATH")$status), ' validation!') 
  }
}

system.time(mclapply(X=c("VALUES"), FUN=prep_sheet, mc.cores = 6)) 


