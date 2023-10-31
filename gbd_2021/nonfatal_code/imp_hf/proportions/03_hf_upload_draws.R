rm(list = ls())


library(data.table)

task_id <- ifelse(is.na(as.integer(Sys.getenv("'SLURM_ARRAY_TASK_ID"))), 1, as.integer(Sys.getenv("'SLURM_ARRAY_TASK_ID")))

args <- commandArgs(trailingOnly = T)

mpath <- args[1] #arg
print(mpath)
me_id <- fread(mpath)
me_id <- me_id[task_id, c(Var1)]

print(paste0('ME ID: ', me_id))

datetime <- args[2]

print(paste0('Datetime: ', datetime))
# source the central functions
source("FILEPATH")
# cvd output folder
cvd_path = "FILEPATH"

# hf etiology map
composite <- fread(paste0(cvd_path, '/composite_cause_list_2020.csv'))
'%ni%' <- Negate('%in%')
composite <- composite[prev_folder %ni% c('congenital_mild_2179',
                                          'congenital_mod_2180',
                                          'congenital_severe_2181',
                                          'congenital_asymp_20090')]

composite[acause=="cvd_afib", prev_me_id := VALUE]
composite[acause=="chronic_cvd_stroke_cerhem", prev_me_id := VALUE]
folder <- composite[prev_me_id==me_id]$prev_folder

if (me_id %in% c(VALUE,VALUE,VALUE,VALUE)) {
  bundle_id <- VALUE
  crosswalk_id <- VALUE
} else {
  bundle_id <- composite[prev_me_id==me_id,prop_bundle_id]
  xw_ids <- data.table(b_id = c(VALUE, VALUE, VALUE, VALUE, VALUE, VALUE, VALUE),
                       xw_id = c(VALUE, VALUE, VALUE, VALUE, VALUE, VALUE, VALUE))

  crosswalk_id <- xw_ids[b_id==bundle_id, xw_id]
}

print(paste0('Upoading ME_id ', me_id, ' from ', folder))


save_results_epi(input_dir=paste0('FILEPATH', datetime, '/', folder), input_file_pattern='{location_id}.csv', modelable_entity_id=me_id,
                   description="VALUE",
                   year_id=as.integer(c(1990, 1995, 2000, 2005, 2010, 2015, 2019:2022)), sex_id=as.integer(c(1, 2)), measure_id=as.integer(c(VALUE, VALUE)),
                   metric_id=VALUE, n_draws=1000, gbd_round_id=VALUE, decomp_step="VALUE", mark_best=TRUE, bundle_id = bundle_id, crosswalk_version_id = crosswalk_id)



