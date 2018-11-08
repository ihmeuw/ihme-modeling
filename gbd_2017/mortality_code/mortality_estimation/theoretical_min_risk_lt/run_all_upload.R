###############################################################################################################
## Format and upload theoretical minimum risk life table results
## Upload both the life table results with 0-1 and five-year-age-groups to 110
## As well as the interpolated ex results at the .01 age level

###############################################################################################################
## Import libraries

library(data.table)
library(readr)

library(mortdb, lib = "/r")
library(mortcore, lib = "/r")

###############################################################################################################
## Set options

GBD_YEAR <- 2017
UPLOAD <- T
MARK_BEST <- F
DB_HOST <- ""
CLUSTER_PROJECT <- ""
SEND_SLACK_UPLOAD_MSG <- T
USERNAME <- Sys.getenv("USER")
CODE_DIR <- paste0("/theoretical_min_risk_lt") 
STATA_SHELL <- paste0(CODE_DIR, "/stata_shell.sh")
run_comment <- ""

###############################################################################################################
## Generate new versions

VERSION_SHOCK_LIFE_TABLE <- get_proc_version("with shock life table", "estimate", run_id = 'recent_completed')

VERSION_ID <- gen_new_version("theoretical minimum risk life table", "estimate", 
                            comment = paste0(run_comment, ": TMRLT from with shock LT run ", VERSION_SHOCK_LIFE_TABLE), 
                            hostname = DB_HOST)

gen_parent_child(parent_runs = VERSION_SHOCK_LIFE_TABLE, 
                 child_process = "theoretical minimum risk life table estimate", 
                 child_id = VERSION_ID,
                 hostname = DB_HOST)

###############################################################################################################
## Create directories
OUTPUT_DIR <- paste0("/theoretical_min_risk_lt/", VERSION_ID)
UPLOAD_DIR <- paste0(OUTPUT_DIR, "/upload")

dir.create(paste0(OUTPUT_DIR), showWarnings = FALSE)
dir.create(UPLOAD_DIR, showWarnings = FALSE)
dir.create(paste0(OUTPUT_DIR, "/usable"), showWarnings = FALSE)
dir.create(paste0(OUTPUT_DIR, "/final"), showWarnings = FALSE)
dir.create(paste0(OUTPUT_DIR, "/data"), showWarnings = FALSE)

###############################################################################################################
## Set up supporting files

with_shock_summary_lt <- get_mort_outputs(model_name = "with shock life table", "estimate", run_id = VERSION_SHOCK_LIFE_TABLE, gbd_year = GBD_YEAR)
with_shock_summary_lt <- with_shock_summary_lt[life_table_parameter_id %in% c(1, 2, 3)]

if (nrow(with_shock_summary_lt[is.na(ihme_loc_id),]) > 0) {
  warning("Rows found where ihme_loc_id is NA")
}

with_shock_summary_lt[life_table_parameter_id == 1, life_table_parameter_name := 'mx']
with_shock_summary_lt[life_table_parameter_id == 2, life_table_parameter_name := 'ax']
with_shock_summary_lt[life_table_parameter_id == 3, life_table_parameter_name := 'qx']
with_shock_summary_lt[, c("upload_with_shock_life_table_estimate_id", "run_id", "estimate_stage_id", "estimate_stage_name", "life_table_parameter_id", "upper", "lower") := NULL]

with_shock_summary_lt <- dcast(with_shock_summary_lt, year_id + ihme_loc_id + location_id + sex_id + age_group_id ~ life_table_parameter_name, value.var = c("mean"))

# get ages for lifetable to drop ages not needed for the process
ages <- setDT(get_age_map(type = "lifetable"))[, list(age_group_id)]

with_shock_summary_lt <- merge(with_shock_summary_lt, ages, all.y = T, by = c("age_group_id"))

write_csv(with_shock_summary_lt, paste0(OUTPUT_DIR, "/data/with_shock_summary_lt.csv"))

###############################################################################################################
## Run first script

qsub(jobname = "theo_min_risk_lt_01",
     code = paste0(CODE_DIR, "/01_create_FINAL_life_table.do"),
     pass = list(OUTPUT_DIR),
     proj = CLUSTER_PROJECT,
     shell = STATA_SHELL,
     slots = 4,
     submit = T)

###############################################################################################################
## Run second script

qsub(jobname = "theo_min_risk_lt_02",
     code = paste0(CODE_DIR, "/02_create_ex_every_age_interp_ex.do"),
     hold = "theo_min_risk_lt_01",
     pass = list(OUTPUT_DIR),
     proj = CLUSTER_PROJECT,
     shell = STATA_SHELL,
     slots = 4,
     submit = T)

###############################################################################################################
## Upload process

if (UPLOAD) {

  ## Import maps
  lt_parameters <- data.table(get_mort_ids("life_table_parameter"))
  lt_parameters[, parameter_name := tolower(parameter_name)]
  lt_parameters <- lt_parameters[, list(life_table_parameter_id, parameter_name)]
  
  ## Import primary data files
  pred_ex <- fread(paste0(OUTPUT_DIR, "/usable/FINAL_min_pred_ex.csv"))
  lt <- fread(paste0(OUTPUT_DIR, "/usable/FINAL_min_lt_final_excl.csv"))
  
  ## Format and output results
  lt[, (c("mod", "sex", "nn")) := NULL]
  lt <- melt(lt, id.vars = c("age"))
  lt[variable == "Lx", variable := "nLx"]
  lt[, variable := tolower(variable)]
  
  lt <- merge(lt, lt_parameters, by.x = "variable", by.y = "parameter_name")
  lt[, variable := NULL]
  setnames(lt, "value", "mean")
  lt[, estimate_stage_id := 1]
  
  setnames(pred_ex, "Pred_ex", "mean")
  pred_ex[, life_table_parameter_id := 5] # ex
  pred_ex[, estimate_stage_id := 6]
  
  combined <- rbindlist(list(lt, pred_ex), use.names = T)
  
  setnames(combined, "age", "precise_age")
  
  ## Write and upload results
  write_csv(combined, paste0(UPLOAD_DIR, "/tmrlt_v", VERSION_ID, ".csv"))
  
  upload_results(file = paste0(UPLOAD_DIR, "/tmrlt_v", VERSION_ID, ".csv"), 
                 model_name = "theoretical minimum risk life table", 
                 model_type = "estimate", 
                 run_id = VERSION_ID,
                 hostname = DB_HOST,
                 send_slack = SEND_SLACK_UPLOAD_MSG)
}

###############################################################################################################
## Mark best
if (MARK_BEST) {
  update_status("theoretical minimum risk life table", "estimate", 
                run_id = VERSION_ID,
                new_status = "best",
                hostname = DB_HOST)
}

###############################################################################################################
## write out comparison vs best
best_upload <- get_mort_outputs("theoretical minimum risk life table", "estimate", run_id = 'best')
current_upload <- get_mort_outputs("theoretical minimum risk life table", "estimate", run_id = VERSION_ID)
setnames(best_upload, "mean", "mean_best")
setnames(current_upload, "mean", paste0("mean_", VERSION_ID))
current_upload[, c("upload_theoretical_minimum_risk_life_table_estimate_id", "run_id", "upper", "lower") := NULL]
best_upload[, c("upload_theoretical_minimum_risk_life_table_estimate_id", "run_id", "upper", "lower") := NULL]
compared <- merge(best_upload, current_upload, by = c("precise_age", "estimate_stage_id", "estimate_stage_name", "life_table_parameter_id"))

write_csv(compared, paste0(OUTPUT_DIR, "/data/mean_comparison.csv"))

## comparison vs best of average difference between 10 year age groups
diff_avg <- copy(compared)
diff_avg[, diff := mean_best - get(paste0("mean_", VERSION_ID))]
diff_avg[, age_buckets := cut(precise_age, breaks = seq(from = 0, to = 110, by = 10), right = F)]
diff_avg[precise_age == 110, age_buckets := "[100,110)"]
diff_avg[age_buckets == "[100,110)", age_buckets := "[100,110]"]
diff_avg <- diff_avg[, lapply(.SD, mean), .SDcols = "diff", by = c("estimate_stage_id", "estimate_stage_name", "life_table_parameter_id", "age_buckets")]

write_csv(diff_avg, paste0(OUTPUT_DIR, "/data/10_year_mean_diff_compare.csv"))
