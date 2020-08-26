#' @author 
#' @date 2019/10/08
#' @description compute squeezed incidence and squeezed mortality proportions for 
#'              all meningitis etiologies
#'              saves them in subdirectories of out_dir for each etiology for upload
rm(list=ls())

library(pacman)
pacman::p_load(data.table, boot, ggplot2)

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--out_dir", help = "directory for writing outputs csvs", default = NULL, type = "character")
parser$add_argument("--check_dir", help = "directory for to write finished check file", default = NULL, type = "character")
parser$add_argument("--param_map_path", help = "directory for this steps intermediate draw files", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step4', type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# get location_id from parameter map
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
parameters <- fread(param_map_path)
loc <- parameters[task_id, location_id]

# SOURCE FUNCTIONS --------------------------------------------------------
k <- # filepath
source(paste0(k, "current/r/get_draws.R"))
source(paste0(k, "current/r/get_covariate_estimates.R"))
source(paste0(k, "current/r/get_demographics.R"))
source(paste0(k, "current/r/get_population.R"))
source(paste0(k, "current/r/get_covariate_estimates.R"))

# MR-BRT wrapper functions
repo_dir <- # filepath
source(paste0(repo_dir, "mr_brt_functions.R"))

# SET OBJECTS -------------------------------------------------------------
men_parent_meid <- 1296 # Parent DisMod MEID

men_cause_id <- 332 # used in get_outputs to get CodCorrect deaths

# Incidence propotion dismod MEIDs
spn_inc_meid   <- 1298
hib_inc_meid   <- 1328
nm_inc_meid    <- 1358
other_inc_meid <- 1388

demographics <- get_demographics(gbd_team="epi", gbd_round_id=6)
years <- demographics$year_id
sexes <- demographics$sex_id
age_groups <- demographics$age_group_id

meninigitis_shock_version_ids <- c(623798, 623924) # change every ds

# directories where fatal and nonfatal PAF csvs will be written
fatal_out_dir <-  # filepath
spn_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_pneumo/")
hib_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_hib/")
nm_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_meningo/")
other_fatal_out_dir <- paste0(fatal_out_dir, "meningitis_other/")

nonfatal_out_dir <- # filepath
spn_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_pneumo/")
hib_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_hib/")
nm_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_meningo/")
other_nonfatal_out_dir <- paste0(nonfatal_out_dir, "meningitis_other/")

# SQUEEZE INCIDENCE PROPORTIONS -------------------------------------------
meningitis_inc_dt <- get_draws(
                       gbd_id_type = 'modelable_entity_id',
                       source = 'epi',
                       gbd_id = men_parent_meid,
                       location_id = loc,
                       age_group_id = age_groups,
                       year_id = years,
                       sex_id = sexes,
                       decomp_step = ds,
                       gbd_round_id = 6,
                       measure_id = 6 # incidence
                     )

# get etiology incidence proportions
spn_inc_dt <- get_draws(
                gbd_id_type = 'modelable_entity_id',
                source = 'epi',
                gbd_id = spn_inc_meid,
                location_id = loc,
                age_group_id = age_groups, 
                year_id = years,
                sex_id = sexes,
                decomp_step = ds,
                gbd_round_id = 6,
                measure_id = 18 # proportion
              )

hib_inc_dt <- get_draws(
                gbd_id_type = 'modelable_entity_id',
                source = 'epi',
                gbd_id = hib_inc_meid,
                location_id = loc,
                age_group_id = age_groups, 
                year_id = years,
                sex_id = sexes,
                decomp_step = ds,
                gbd_round_id = 6,
                measure_id = 18 # proportion
              )

nm_inc_dt <- get_draws(
               gbd_id_type = 'modelable_entity_id',
               source = 'epi',
               gbd_id = nm_inc_meid,
               location_id = loc,
               age_group_id = age_groups, 
               year_id = years,
               sex_id = sexes,
               decomp_step = ds,
               gbd_round_id = 6,
               measure_id = 18 # proportion
             )

other_inc_dt <- get_draws(
                  gbd_id_type = 'modelable_entity_id',
                  source = 'epi',
                  gbd_id = other_inc_meid,
                  location_id = loc,
                  age_group_id = age_groups, 
                  year_id = years,
                  sex_id = sexes,
                  decomp_step = ds,
                  gbd_round_id = 6,
                  measure_id = 18 # proportion
                )
cols.remove <- c("model_version_id", "modelable_entity_id", "measure_id", "metric_id")
spn_inc_dt[, (cols.remove):= NULL]
hib_inc_dt[, (cols.remove):= NULL]
nm_inc_dt[, (cols.remove):= NULL]
other_inc_dt[, (cols.remove):= NULL]
# reshape from wide to long
id_vars <- c("location_id", "year_id", "sex_id", "age_group_id")
spn_inc_dt <- melt(spn_inc_dt, id.vars = id_vars, variable.name = "draw", 
                   value.name = "prop_spn")
hib_inc_dt <- melt(hib_inc_dt, id.vars = id_vars, variable.name = "draw", 
                   value.name = "prop_hib")
nm_inc_dt <- melt(nm_inc_dt, id.vars = id_vars, variable.name = "draw", 
                  value.name = "prop_nm")
other_inc_dt <- melt(other_inc_dt, id.vars = id_vars, variable.name = "draw", 
                     value.name = "prop_other")

# squeeze proportions to sum to 1
merge_cols <- c("location_id", "year_id", "age_group_id", "sex_id", "draw")
total_prop_dt <- merge(spn_inc_dt, hib_inc_dt, by = merge_cols)
total_prop_dt <- merge(total_prop_dt, nm_inc_dt, by = merge_cols)
total_prop_dt <- merge(total_prop_dt, other_inc_dt, by = merge_cols)
total_prop_dt[, prop_total := prop_spn + prop_nm + prop_hib + prop_other]
total_prop_dt[, squeeze_prop_spn   := prop_spn / prop_total]
total_prop_dt[, squeeze_prop_nm    := prop_nm / prop_total]
total_prop_dt[, squeeze_prop_hib   := prop_hib / prop_total]
total_prop_dt[, squeeze_prop_other := prop_other / prop_total]
total_prop_dt[, prop_total := NULL]
# multiply incidence by squeezed proportions
cols.remove <- c("model_version_id", "modelable_entity_id", "measure_id", "metric_id")
meningitis_inc_dt[, (cols.remove) := NULL]
id_vars <- c("location_id", "year_id", "sex_id", "age_group_id")
meningitis_inc_dt <- melt(meningitis_inc_dt, id.vars = id_vars, 
                          variable.name = "draw", value.name = "men_inc_rate")
merge_cols <- c("location_id", "year_id", "age_group_id", "sex_id", "draw")
etiology_inc_dt <- merge(meningitis_inc_dt, total_prop_dt, by = merge_cols)
# convert rates to counts with population
pop_dt <- get_population(age_group_id = age_groups, location_id = loc, 
                         year_id = years, sex_id = sexes, gbd_round_id = 6, 
                         decomp_step = ds)
pop_dt[, run_id := NULL]
setnames(pop_dt, "population", "pop")
merge_cols <- c("location_id", "year_id", "age_group_id", "sex_id")
etiology_inc_dt <- merge(etiology_inc_dt, pop_dt, by = merge_cols,
                         all.x = T)

etiology_inc_dt[, `:=` (spn_inc_cnt   = men_inc_rate * pop * squeeze_prop_spn,
                        hib_inc_cnt   = men_inc_rate * pop * squeeze_prop_hib,
                        nm_inc_cnt    = men_inc_rate * pop * squeeze_prop_nm,
                        other_inc_cnt = men_inc_rate * pop * squeeze_prop_other)]

# PREDICT CFR AS A FUNCTION OF HAQ ----------------------------------------
haqi_dt <- get_covariate_estimates(covariate_id = 1099, 
                                   location_id = loc, 
                                   year_id = 'all', 
                                   gbd_round_id = 6, 
                                   decomp_step = ds)
haqi_dt <- haqi_dt[, .(location_id, year_id, haqi = mean_value)]
etiology_inc_dt <- merge(etiology_inc_dt, haqi_dt, 
                         by = c("location_id", "year_id"), all.x = T)
# this file was created in the parent script
preds <- fread(paste0(out_dir, "/preds/", loc, "_mrbrt_preds.csv"))
message(length(unique(preds$draw)))

spn_cfr_dt   <- preds[case_name == "meningitis_pneumo", .(location_id, year_id, 
                                                          draw, spn_cfr = cfr)]
nm_cfr_dt    <- preds[case_name == "meningitis_meningo", .(location_id, year_id, 
                                                           draw, nm_cfr = cfr)]
hib_cfr_dt   <- preds[case_name == "meningitis_hib", .(location_id, year_id, 
                                                       draw, hib_cfr = cfr)]
other_cfr_dt <- preds[case_name == "meningitis_other", .(location_id, year_id, 
                                                         draw, other_cfr = cfr)]
merge_cols <- c("location_id", "year_id", "draw")
etiology_inc_dt <- merge(etiology_inc_dt, spn_cfr_dt, by = merge_cols, all.x = T)
etiology_inc_dt <- merge(etiology_inc_dt, nm_cfr_dt, by = merge_cols, all.x = T)
etiology_inc_dt <- merge(etiology_inc_dt, hib_cfr_dt, by = merge_cols, all.x = T)
etiology_inc_dt <- merge(etiology_inc_dt, other_cfr_dt, by = merge_cols, all.x = T)
# check that merge worked!
if (nrow(etiology_inc_dt[is.na(other_cfr)]) > 0) {
  file.create(paste0(h, "failed_", loc, ".txt"), overwrite=T)
  stop(paste("ERROR: CFR DID NOT MERGE CORRECTLY FOR LOCATION", loc))
}

# compute etiology deaths as incidence counts * case fatality ratio
etiology_inc_dt[, `:=` (spn_death_cnt   = spn_inc_cnt * spn_cfr,
                        hib_death_cnt   = hib_inc_cnt * hib_cfr,
                        nm_death_cnt    = nm_inc_cnt * nm_cfr,
                        other_death_cnt = other_inc_cnt * other_cfr)]

# PULL CODCORRECT DEATHS AND SHOCKS ---------------------------------------
# remove data tables to save space
rm(list=c("spn_inc_dt", "hib_inc_dt", "nm_inc_dt", "other_inc_dt"))
men_draws_dt <- get_draws(
                   "cause_id",
                   men_cause_id,
                   source = "codcorrect",
                   measure_id = 1, # deaths
                   metric_id = 1,  # counts
                   location_id = loc,
                   age_group_id = age_groups,
                   year_id = years,
                   gbd_round_id = 6,
                   decomp_step = ds,
                   version_id = 99
                )

# get shock counts
men_shock_draws_list <- lapply(meninigitis_shock_version_ids, function(mvid) {
  nm_shock_draws <- get_draws(
    gbd_id_type = 'cause_id',
    source = 'codem',
    gbd_id = men_cause_id,
    location_id = loc,
    age_group_id = age_groups, 
    year_id = years,
    version_id = mvid,
    gbd_round_id = 6,
    decomp_step = ds,
    measure_id = 1, # deaths
    metric_id = 1  # counts
  )
})
men_shock_draws_dt <- rbindlist(men_shock_draws_list)
cols.remove <- c("envelope", "measure_id", "metric_id", "cause_id", "pop", "sex_name")
men_draws_dt[, (cols.remove) := NULL]
men_shock_draws_dt[, (cols.remove) := NULL]
# melt from wide to long
id_vars <- c("location_id", "year_id", "sex_id", "age_group_id")
men_draws_dt <- melt(men_draws_dt, id.vars = id_vars, variable.name = "draw",
                     value.name = "men_mort_cnt")
men_shock_draws_dt <- melt(men_shock_draws_dt, id.vars = id_vars, variable.name = "draw",
                           value.name = "men_shock_mort_cnt")
# add shocks to parent meningitis
merge_cols <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")
men_draws_dt <- merge(men_draws_dt, men_shock_draws_dt, by = merge_cols)
etiology_mort_dt <- merge(etiology_inc_dt, men_draws_dt, by = merge_cols, 
                          all.x = T)
etiology_mort_dt[, mort_envelope := men_mort_cnt + men_shock_mort_cnt]
# squeeze death rates to sum to CODEm deaths + 
etiology_mort_dt[, total_death_cnt := spn_death_cnt + nm_death_cnt + men_shock_mort_cnt +
                   + hib_death_cnt + other_death_cnt]
etiology_mort_dt[, squeeze_spn_death_cnt := mort_envelope * (spn_death_cnt / total_death_cnt)]
etiology_mort_dt[, squeeze_nm_death_cnt := mort_envelope * ((nm_death_cnt + men_shock_mort_cnt)/ total_death_cnt)]
etiology_mort_dt[, squeeze_hib_death_cnt := mort_envelope * (hib_death_cnt / total_death_cnt)]
etiology_mort_dt[, squeeze_other_death_cnt := mort_envelope * (other_death_cnt / total_death_cnt)]
# compute mortality proportions
etiology_mort_dt[, squeeze_mort_prop_spn   := squeeze_spn_death_cnt / mort_envelope]
etiology_mort_dt[, squeeze_mort_prop_hib   := squeeze_hib_death_cnt / mort_envelope]
etiology_mort_dt[, squeeze_mort_prop_nm    := squeeze_nm_death_cnt / mort_envelope]
etiology_mort_dt[, squeeze_mort_prop_other := squeeze_other_death_cnt / mort_envelope]


# write nonfatal PAFs for upload
keep_cols <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")
inc_keep_cols <- c("location_id", "year_id", "sex_id", "age_group_id", "draw", "squeeze_prop_spn", 
                   "squeeze_prop_nm", "squeeze_prop_hib", "squeeze_prop_other")
save_results_inc_dt <- etiology_mort_dt[, inc_keep_cols, with = FALSE]
# subset for each etiology
spn_save_results_inc_dt <- save_results_inc_dt[, c(keep_cols, "squeeze_prop_spn"), with = FALSE]
hib_save_results_inc_dt <- save_results_inc_dt[, c(keep_cols, "squeeze_prop_hib"), with = FALSE]
nm_save_results_inc_dt <- save_results_inc_dt[, c(keep_cols, "squeeze_prop_nm"), with = FALSE]
other_save_results_inc_dt <- save_results_inc_dt[, c(keep_cols, "squeeze_prop_other"), with = FALSE]
# cast from long to wide
spn_save_results_inc_dt <- dcast(spn_save_results_inc_dt, location_id + year_id + sex_id + age_group_id ~ draw, 
                                 value.var = "squeeze_prop_spn")
hib_save_results_inc_dt <- dcast(hib_save_results_inc_dt, location_id + year_id + sex_id + age_group_id ~ draw, 
                                 value.var = "squeeze_prop_hib")
nm_save_results_inc_dt <- dcast(nm_save_results_inc_dt, location_id + year_id + sex_id + age_group_id ~ draw,
                                value.var = "squeeze_prop_nm")
other_save_results_inc_dt <- dcast(other_save_results_inc_dt, location_id + year_id + sex_id + age_group_id ~ draw,
                                   value.var = "squeeze_prop_other")

fwrite(spn_save_results_inc_dt, paste0(spn_nonfatal_out_dir, loc, ".csv"))
fwrite(hib_save_results_inc_dt, paste0(hib_nonfatal_out_dir, loc, ".csv"))
fwrite(nm_save_results_inc_dt, paste0(nm_nonfatal_out_dir, loc, ".csv"))
fwrite(other_save_results_inc_dt, paste0(other_nonfatal_out_dir, loc, ".csv"))

# write fatal PAFs for upload
keep_cols <- c("location_id", "year_id", "sex_id", "age_group_id", "draw")
mort_keep_cols <- c("location_id", "year_id", "sex_id", "age_group_id", "draw", "squeeze_mort_prop_spn",
                    "squeeze_mort_prop_other", "squeeze_mort_prop_hib", "squeeze_mort_prop_nm")
save_results_mort_dt <- etiology_mort_dt[, mort_keep_cols, with = FALSE]
# subset for each etiology
spn_save_results_mort_dt <- save_results_mort_dt[, c(keep_cols, "squeeze_mort_prop_spn"), with = FALSE]
hib_save_results_mort_dt <- save_results_mort_dt[, c(keep_cols, "squeeze_mort_prop_hib"), with = FALSE]
nm_save_results_mort_dt <- save_results_mort_dt[, c(keep_cols, "squeeze_mort_prop_nm"), with = FALSE]
other_save_results_mort_dt <- save_results_mort_dt[, c(keep_cols, "squeeze_mort_prop_other"), with = FALSE]
# cast from long to wide
spn_save_results_mort_dt <- data.table::dcast(spn_save_results_mort_dt, location_id + year_id + sex_id + age_group_id ~ draw, 
                                 value.var = "squeeze_mort_prop_spn")
hib_save_results_mort_dt <- data.table::dcast(hib_save_results_mort_dt, location_id + year_id + sex_id + age_group_id ~ draw, 
                                 value.var = "squeeze_mort_prop_hib")
nm_save_results_mort_dt <- data.table::dcast(nm_save_results_mort_dt, location_id + year_id + sex_id + age_group_id ~ draw,
                                value.var = "squeeze_mort_prop_nm")
other_save_results_mort_dt <- data.table::dcast(other_save_results_mort_dt, location_id + year_id + sex_id + age_group_id ~ draw,
                                   value.var = "squeeze_mort_prop_other")

fwrite(spn_save_results_mort_dt, paste0(spn_fatal_out_dir, loc, ".csv"))
fwrite(hib_save_results_mort_dt, paste0(hib_fatal_out_dir, loc, ".csv"))
fwrite(nm_save_results_mort_dt, paste0(nm_fatal_out_dir, loc, ".csv"))
fwrite(other_save_results_mort_dt, paste0(other_fatal_out_dir, loc, ".csv"))

# WRITE CHECKS ------------------------------------------------------------
if (length(grep("draw_", colnames(other_save_results_mort_dt))) < 1000) {
  file.create(paste0(h, "failed_", loc, ".txt"), overwrite=T)
  stop(paste("Error: Output files do not have 1000 draws for location", loc))
}

file.create(paste0(check_dir, "finished_", loc, ".txt"), overwrite=T)
