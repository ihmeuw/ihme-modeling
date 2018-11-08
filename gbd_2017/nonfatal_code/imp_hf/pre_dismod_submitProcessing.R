#!FILEPATH

##########################################################################################
####### Steps the script perform:
####### 1. Clean the raw marketscan data provided by hospital team
####### 2. Use beta distribution to calculate mean, lower quartile, and upper quartile probablity from
####### the 1000 draws generated from marketscan cases_wHF and cases
####### 3. Launch qsub parallelized by locs to calculate mean, lower, & upper_death of the 21 subcauses
####### by location-sex-age from codcorrect draws
####### 4. Use outputs from step 2 and 3 to calculate input proportion for 6 Dismod models
####### 5. Prep epi sheets and upload to the 6 proportion bundles
##########################################################################################

#######################################
## PREP ENVIRONMENT & GLOBAL VAR
#######################################
rm(list = ls())
if (Sys.info()[1] == "Linux") folder_path <- "FOLDER"
if (Sys.info()[1] == "Windows") folder_path <- "FOLDER"

# add libs
library(parallel)
library(plyr)
library(haven)
library(RMySQL)
library(openxlsx)
library(data.table)

# source central functions
source(paste0(folder_path, "/FUNCTION_PATH/get_draws.R"))
source(paste0(folder_path, "/FUNCTION_PATH/get_location_metadata.R"))
source(paste0(folder_path, "/FUNCTION_PATH/get_ids.R"))
source(paste0(folder_path, "/FUNCTION_PATH/get_demographics.R"))
source(paste0(folder_path, "/FUNCTION_PATH/upload_epi_data.R"))
source(paste0(folder_path, "/FUNCTION_PATH/validate_input_sheet.R"))

# cvd output folder
cvd_path = "/CVD_FOLDER"

# delete all existing files from output folder #might need to uncomment if job fails
do.call(file.remove, list(list.files(paste0(cvd_path, '/pre_dismod/0_codcorrected_deaths/version/'), pattern='.rds', full.names = TRUE)))

# qsub diagnostic file path
qsub_output = paste0("/LOG_FOLDER/predismod/output")
qsub_errors = paste0("/LOG_FOLDER/predismod/errors")

year_ids <- c(1990, 1995, 2000, 2005, 2010, 2017)
sex_ids  <- c(1,2)
age_group_ids = c(2:20,30,31,32,235)

# hf etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list.csv'))

# get age_groups and modelable_entity_name from MySQL db
db_con = fread(paste0(folder_path, "/FILEPATH/db_con.csv"),
               stringsAsFactors = FALSE)
con <- dbConnect(dbDriver("MySQL"),
                 username = db_con$username,
                 password = db_con$pass,
                 host = db_con$host)
age_groups <-  as.data.table(dbGetQuery(con, sprintf("SELECT age_group_years_start AS age_start, age_group_id FROM shared.age_group")))[age_group_id %in% c(2:20, 30:32, 235)]
age_groups[age_group_id %in% c(2,3,4), age_start:=0]
q0 <- as.data.table(dbGetQuery(con, sprintf("SELECT age_group_years_start AS age_start, age_group_years_end AS age_end, age_group_id FROM shared.age_group")))
q1 = as.data.table(dbGetQuery(con, sprintf("SELECT modelable_entity_name, modelable_entity_id FROM epi.modelable_entity")))[modelable_entity_id %in% unique(na.omit(composite$prop_me_id))]
dbDisconnect(con)

#######################################
## 1. Cvd-ized raw marketscan data
#######################################
# read raw marketscan
marketscan <- setDT(read_dta(paste0(folder_path,"/FILEPATH/MARKETSCAN_RATIO.dta"))) # .dta file provided by hospital team
setnames(marketscan, "HFcategory", "hfcategory") # rename HFcategory to hfcateogry to match hf map
marketscan<-marketscan[!(marketscan$hfcategory==""),] # drop any nulls or blanks in the dataset
marketscan <- marketscan[!year%in%c(2000,2015)] # drop Marketscan year 2000 & 2015 due to ICD9/ICD10 coding issues

# collapse all years
mktscan_cols <- c('sex', 'hfcategory', 'age_start', 'age_end')
marketscan <- marketscan[,.(cases_wHF=sum(cases_wHF), cases=sum(cases),
                            sample_size=sum(sample_size)), by=mktscan_cols]
marketscan[cases<10]$cases_wHF <- 0 # if cases < 10, then set cases_wHF to 0 so that the proportion will be zero
marketscan[,year_end:=2017]

# read hf cod mapping
map_hfcat_acause <- fread(paste0(folder_path, '/COD_FILEPATH/map_hfcategory_acause_gbd2016.csv'))
map_hfcat_acause <- rbind(map_hfcat_acause, list('Valve, aortic', 'cvd_valvu_aort'))
map_hfcat_acause <- rbind(map_hfcat_acause, list('Valve, mitral', 'cvd_valvu_mitral'))
map_hfcat_acause <- rbind(map_hfcat_acause, list('Valve, other', 'cvd_valvu_other'))

# merge the two files
marketscan <- join(marketscan, map_hfcat_acause, by ='hfcategory', type='left')
marketscan <- join(marketscan, composite[,.(acause,cause_id)], by='acause')

# certain causes have age cutoffs of 15, where all proportionsbelow 15 will be set to 0.
AGE_RESTRICTED_CAUSES=c(493,498,509,511,512,513,514,516,970,938) 
marketscan[(cause_id %in% AGE_RESTRICTED_CAUSES) & (age_start<15)]$cases_wHF <- 0
# nrvd & rhd causes have age cutoffs of 1, where all proportions below 1 will be set to 0.
AGE_RESTRICTED_UNDER1=c(492)
marketscan[(cause_id %in% AGE_RESTRICTED_UNDER1) & (age_start<1)]$cases_wHF <- 0

# replace age_start and age_end with age group id
# according to expert opinion, 0-4 is really 0-1, 
# this fix is only for clarity, doesnt affect
# anything as age_start is right
marketscan[(age_start==0) & (age_end==4)]$age_end <- 1

# continue cleaning
marketscan <- join(marketscan, age_groups, by='age_start') # merge with age_groups to get age_group_id
setnames(marketscan, "sex", "sex_id")
# dropping 2 nrvd causes
marketscan <- marketscan[!(cause_id%in%c(968, 969)|is.na(cause_id))]
# dropping iodine deficiency and iron-deficiency anemia as HF etiologies
marketscan <- marketscan[!(cause_id%in%c(388, 390)|is.na(cause_id))]

# write the cleaned dataset to be used later by post_dismod step to calculate correction factors
saveRDS(marketscan, paste0(cvd_path, '/MarketScan_cleaned.rds'))

#######################################
## 2. Use beta distribution to calculate coefficient variation from
## the 1000 draws generated from marketscan cases_wHF and cases
#######################################

# Calculate probability that an event assigned to a cause is due to HF
# Given p=probability of success.
# p = df[success_case_col] / df[sample_size_col]
# n = df[sample_size_col]
# Calculates ndraws samples from this distribution
# for each row in the dataframe, then adds either ndraws
# columns with each sample from the distribution, or if collapse = True,
# adds the mean, 75th percentile, and 25th percentile as columns.

# aggregate to 6 causes
cause_agg <- composite[!is.na(cause_id)][,.(cause_id, composite_id)][is.na(composite_id), composite_id:=cause_id]
marketscan_6 <- as.data.table(join(marketscan, cause_agg))
group_cols <- c('age_group_id', 'sex_id', 'year_end','composite_id')
marketscan_6 <- marketscan_6[,.(success_cases_col=sum(cases_wHF), sample_size_col=sum(cases), cause_id=composite_id), by=group_cols]
marketscan_6[,composite_id:=NULL]

marketscan[,':='(success_cases_col=cases_wHF, sample_size_col=cases)]
marketscan <- as.data.table(rbind.fill(marketscan, marketscan_6))[,mget(c(names(marketscan_6)))]
marketscan <- unique(marketscan)

stopifnot(marketscan$success_cases_col <=  marketscan$sample_size_col) # make sure successes are never more than total trials
stopifnot(marketscan$success_cases_col >= 0) # make sure successes cases are not negative

set.seed(1325)

# generate 1000 draws and calculate mean/upper/lower by row.
marketscan$tempID <- seq.int(nrow(marketscan))
draws_df <- data.table()

for (i in seq_len(nrow(marketscan))){
  tempID = marketscan$tempID[[i]]
  success_cases = marketscan$success_cases_col[[i]]
  non_cases = marketscan$sample_size_col[[i]]-success_cases
  draws = rbeta(c(1:1000), success_cases, non_cases)
  draws_row = data.table(tempID= tempID, mean=mean(draws), upper=quantile(draws, .75), lower=quantile(draws, .25), std_dev=sd(draws))
  draws_df <- rbind(draws_df, draws_row)
}

# merge newly calculated stats back to marketscan dataset
marketscan <- join(marketscan, draws_df, by=c('tempID'))
marketscan[,tempID:=NULL]
setnames(marketscan, 'mean', 'prop_hf')

# calculate coefficient variation
marketscan[,coeff_var_mkt:=std_dev/prop_hf]
marketscan$coeff_var_mkt[is.nan(marketscan$coeff_var_mkt)] <- 0

#######################################
## 3. Launch qsub parallelized by locs to calculate mean, lower, & upper_death of the 21 subcauses
## by location-sex-age from codcorrect draws
#######################################
# only calculate for non-composite causes
COD_CAUSE_IDS <- c(unique(composite[!(cause_id%in%unique(composite$composite_id))&(cause_id!=346)]$cause_id), 492)

## equivalent of the qsub child script is 01_get_draws_subprocess.py
# get choices for locations
locations <- get_location_metadata(location_set_id=35, gbd_round_id=5)
dismod_locs <- unique(get_demographics('epi')$location_id)
loc_choices <- locations[level==3|parent_id==95 | location_id %in%dismod_locs]$location_id

# run child script in parallelization
file_list <- NULL

for (loc in loc_choices){
  output_path <-  paste0(cvd_path, '/pre_dismod/0_codcorrected_deaths/version/codcorrect_', loc, '.rds')
  file_list <- c(file_list, output_path)
    command <- paste("qsub -P proj_custom_models -l mem_free=4g -pe multi_slot 2 -N ",
                     paste0("hfpre_", loc),
                     paste0("-o ", qsub_output, " -e ", qsub_errors, " /CODE_REPO/pre_dismod_submitProcessing.sh /CODE_REPO/pre_dismod_processing.R"),
                     loc)
  system(command)
  #if (loc %in% seq(50,700,50)) Sys.sleep(100) # submit in batches to not overwhelm db
}
# Wait until all jobs are done
job_hold("hfpre_", file_list = file_list)

codcorrect <- rbindlist(lapply(file_list,readRDS),use.names=T)


# aggregate to 6 causes
codcorrect_6 <- as.data.table(join(codcorrect, cause_agg))
group_cols <- c('age_group_id', 'sex_id', 'year_end', 'location_id', 'composite_id')
codcorrect_6 <- codcorrect_6[,.(mean_death=sum(mean_death), lower_death=sum(lower_death), upper_death=sum(upper_death), cause_id=composite_id), by=group_cols]
codcorrect_6[,composite_id:=NULL]

codcorrect <- as.data.table(rbind.fill(codcorrect, codcorrect_6))[,mget(c(names(codcorrect_6)))]
codcorrect <- unique(codcorrect)
# calculate coefficient of variation using upper and lower bounds
codcorrect[,std_err:=(upper_death-lower_death)/(2*1.96)]
codcorrect[,coeff_var_cod:=std_err/mean_death]
codcorrect[,std_err:=NULL]


#######################################
## 4. Use outputs from step 2 and 3 to calculate input proportion for 6 Dismod models
#######################################
# combine Marketscan data with CoDCorrect
mktscan_codcorr <- join(codcorrect, marketscan, by=c('age_group_id', 'sex_id', 'cause_id', 'year_end'), type='left')

# make sure no combination is missing (if missing fill with zeros) & there are no duplicates
square <- expand.grid(age_group_id=unique(mktscan_codcorr$age_group_id), sex_id=unique(mktscan_codcorr$sex_id), cause_id=unique(mktscan_codcorr$cause_id),location_id=unique(mktscan_codcorr$location_id), year_end=unique(mktscan_codcorr$year_end))

if (nrow(mktscan_codcorr)>nrow(square)) stop('Duplicates in mktscan_codcorr table.') # no duplicates
mktscan_codcorr <- as.data.table(join(mktscan_codcorr, square, type='right')) # no missing data
for(j in seq_along(mktscan_codcorr)){
  set(mktscan_codcorr, i = which(is.na(mktscan_codcorr[[j]]) & is.numeric(mktscan_codcorr[[j]])), j = j, value = 0)
}

# calculate mean and coefficient variation of deaths attributable to hf
mktscan_codcorr[,':='(hf_deaths=prop_hf*mean_death, mkt_cc_cv=sqrt(coeff_var_mkt^2+coeff_var_cod^2))]

## equivalent in old code is group_and_summ_deaths_uncertainty
group_cols <- c('location_id', 'age_group_id', 'sex_id', 'year_end') # group by location age sex year_start, summing deaths and
summ_df <- mktscan_codcorr[,.(summ_hf_deaths=sum(hf_deaths), summ_mkt_cc_cv=sqrt(sum(mkt_cc_cv^2))), by=group_cols] #  applying sqrt/sum_squares to coeffs

mktscan_codcorr <- join(mktscan_codcorr, summ_df, by=group_cols, type='left') # merge this with ms*deaths and coefficients by location/age/sex/cause

## calculate standard error of mktscan_codcorr
mktscan_codcorr[,hf_target_prop:=hf_deaths/summ_hf_deaths] # set proportion as ms*deaths over total ms*deaths
mktscan_codcorr[,hf_target_prop_cv:=sqrt(mkt_cc_cv^2+summ_mkt_cc_cv^2)] # set coeff var as sqrt(coeff_2 + total_coeff_2)
mktscan_codcorr[,std_err:=hf_target_prop*hf_target_prop_cv] # set std. err as proportion * coeff

## equivalent in old code is adjust_std_error_with_limit
# Adjust the coefficients of variance to a bounded maximum of(0.25) to avoid extreme standard errors due to 
# large coefficients of variance. Return a new coefficient of variance
# for each proportion that is the maximum of the mkt_coeff_var and the
# cod_coeff_var, or the limit, whichever is smaller. Do this by the group
# columns.

# throw an assertion error if coeff_var_prop_adj or std_err_adj col is already defined
if ('coeff_var_prop_adj'%in%names(mktscan_codcorr)) stop('coeff_var_prop_adj already defined')
if ('std_err_adj'%in%names(mktscan_codcorr)) stop('std_err_adj already defined')

# impose limit
limit = 0.25

max_df <- mktscan_codcorr[,.(coeff_var_prop_adj=ifelse(max(coeff_var_cod, coeff_var_mkt)>=limit, limit, max(coeff_var_cod, coeff_var_mkt))), by=group_cols]
mktscan_codcorr <- join(mktscan_codcorr, max_df, by=group_cols, type='left')
if (any(is.na(mktscan_codcorr$coeff_var_prop_adj))) stop('merge failed between mktscan_codcorr & max_df')

# recalculate std_err
mktscan_codcorr[,std_err_adj:=hf_target_prop*coeff_var_prop_adj]

mktscan_codcorr <- mktscan_codcorr[sex_id!=3] # drop sex_id ==3 if those rows exist

# verify that there are no duplicates
id_cols = c('location_id', 'age_group_id', 'sex_id', 'cause_id', 'year_end')
if(nrow(unique(mktscan_codcorr[,mget(id_cols)]))!=nrow(mktscan_codcorr)) stop('there are duplicate location-age-sex-causes')

# drop all irrelevant columns
df <- mktscan_codcorr[,mget(c(id_cols, 'hf_target_prop', 'std_err_adj'))]

## Shiny input
saveRDS(df, paste0(cvd_path, '/dismod_input.rds'))

## aggregate causes to 6 main causes
heart_failure_SPLIT_AGGs_subnat <- as.data.table(df[,mget(c(id_cols, 'hf_target_prop', 'std_err_adj'))])
if(round(sum(heart_failure_SPLIT_AGGs_subnat[location_id==8 & age_group_id==235&sex_id==1&year_end==2017]$hf_target_prop), digits=10)!=1) stop('HF props do not add up to 1!!') # random group
if(round(sum(heart_failure_SPLIT_AGGs_subnat[location_id==4657 & age_group_id==19&sex_id==2&year_end==2017]$hf_target_prop), digits=10)!=1) stop('HF props do not add up to 1!!') # random group

# add year_start for epiuploader
heart_failure_SPLIT_AGGs_subnat[, year_start:=1990]

# save file to be used by post_dismod step
saveRDS(heart_failure_SPLIT_AGGs_subnat, paste0(folder_path, "/OUTPUT_PATH/heart_failure_SPLIT_AGGs_subnat.rds"))
message = ("The Heart Failure Market-Scan/CODcorrect code has finished running. The inputs will begin uploading soon.")

#######################################
## 5. Prep epi sheets and upload to the 6 proportion bundles
#######################################
# Filter out Sub-Saharan Africa - lit data will be used instead
ssa_ids <- locations[super_region_id==166]$location_id
LOCS <- get_ids(table='location')[,.(location_id, location_name)]
upload <- heart_failure_SPLIT_AGGs_subnat[!location_id%in%ssa_ids]
upload <- join(upload, LOCS, type='right')

# ID table
IDs <-data.table(bundle_id= c(285, 286, 287, 288, 289, 290),
                 cause_id=c(493, 498, 520, 492, 499, 385),
                 nid=c(250478, 250479, 250480, 250481, 250482, 250483),
                 modelable_entity_id=c(2414, 2415, 2416, 2417, 2418, 2419))
IDs <- join(IDs, q1) # add me_name

# Prep and upload sheet. Currently only validate.
get_seq <- function(bundle){
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$username,
                   password = db_con$pass,
                   host = db_con$host)
  nid <- as.numeric(IDs[bundle_id==bundle]$nid)
  seqs <- as.data.table(dbGetQuery(con, sprintf("SELECT seq, location_id, age_start, age_end, sex_id, year_start, year_end FROM epi.bundle_dismod
                                                where bundle_id = %s and nid = %s", bundle, nid)))
  dbDisconnect(con)
  rowcheck <- nrow(seqs)
  age_groups[age_group_id==2, age_start:=0.0000000]
  age_groups[age_group_id==3, age_start:=0.0191781]
  age_groups[age_group_id==4, age_start:=0.0767123]
  seqs <- as.data.table(join(seqs, age_groups, type='inner'))[,.(seq, location_id, sex_id, year_start, year_end, age_start, age_end, age_group_id)]
  if (rowcheck>nrow(seqs)) message ('Missing age groups in the existing db')
  return(seqs)
}

prep_sheet <- function(bundle){
  df <- join(upload, IDs[bundle_id==bundle], type='inner')
  #df <- join(df, q0, type='left')
  df <- join(df, get_seq(bundle), type='left')
  setnames(df, c("hf_target_prop", "std_err_adj"), c("mean", "standard_error"))
  df[sex_id == 1, sex:="Male"]
  df[sex_id == 2, sex:="Female"]
  df[, c("cause_id", "sex_id"):=NULL]
  df[,':='(unit_type='Person', unit_type_value=2.0, measure_issue=0.0, uncertainty_type='Standard error', uncertainty_type_value=NA,
           extractor='EXTRACTOR', representative_name="Nationally and subnationally representative", urbanicity_type="Unknown", response_rate=NA, sampling_type=NA,
           recall_type="Point", recall_type_value=1.0, case_name=NA, case_definition=NA, case_diagnostics=NA,
           note_modeler='Proportion generated from CODEm deaths using Marketscan data', cv_hospital=0, cv_marketscan=1, cv_low_income_hosp=0,
           cv_high_income_hosp=0, is_outlier=0, cases=NA, measure='proportion',sample_size=NA, effective_sample_size=NA, source_type="Mixed or estimation",
           underlying_nid= NA, input_type='extracted', design_effect=NA, unit_value_as_published=1, date_inserted=date(), last_updated=date(),
           inserted_by='EXTRACTOR', last_updated_by='EXTRACTOR', upper=NA, lower=NA)]
  out_path <- paste0(cvd_path, '/OUTPUT_PATH/new_inputs_', bundle, '_', format(Sys.Date(), "%Y%m"), '.xlsx')
  print(dim(df))
  write.xlsx(df, out_path, sheetName='extraction')
  upload_sheet <- T
  if (upload_sheet == T){
    upload_epi_data(bundle, out_path)
  } else {
    message ('Bundle', bundle, ' ', unique(validate_input_sheet(bundle, out_path, qsub_errors)$status), ' validation!')
  }
}

system.time(mclapply(X=unique(IDs$bundle_id), FUN=prep_sheet, mc.cores =6)) #10 min
