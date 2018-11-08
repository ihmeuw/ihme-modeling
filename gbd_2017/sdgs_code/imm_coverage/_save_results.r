#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: save results for all vaccination formulations
#             - covariates
#             - SDG indicators
#             - risk factor exposure models
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### set up
rm(list=ls())
library(data.table); library(dplyr, lib.loc="FILEPATH")
library(parallel); library(DBI, lib.loc="FILEPATH")

username <- Sys.info()[["user"]]
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "/FILEPATH"
  h <- paste0("FILEPATH", username)
}
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------
### source save_results functions
source(paste0(j, "FILEPATH/prep_save_results.r"))

### set paths
date         <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d")
save.root    <- file.path(draws_root, "FILEPATH", paste0("gbd", year_end), date); if (!dir.exists(save.root)) dir.create(save.root, recursive=TRUE)
results.root <- file.path(data_root, "FILEPATH", paste0("gbd", year_end), date); if (!dir.exists(results.root)) dir.create(results.root, recursive=TRUE)

### database
me_db        <- fread(paste0(code_root, "FILEPATH/me_db.csv"))
#***********************************************************************************************************************


#----PREP---------------------------------------------------------------------------------------------------------------
### prep with draws?
save_draws <- TRUE

### prep and save summary versions for vetting, draws for save_results
invisible(lapply(c("vacc_dpt3", "vacc_mcv1", "vacc_bcg", "vacc_polio3"), 
       function(x) prep.draws(x, draws=save_draws)))

### prep ratios
prep.ratio("vacc_dpt3_dpt1_ratio", head="vacc_", draws=save_draws, flip=TRUE)
ratios <- c("vacc_mcv2_mcv1_ratio", 
            "vacc_hib3_dpt3_ratio", 
            "vacc_rotac_dpt3_ratio", 
            "vacc_hepb3_dpt3_ratio", 
            "vacc_pcv3_dpt3_ratio", 
            "vacc_rcv1_mcv1_ratio")
invisible(lapply(ratios, function(x) prep.ratio(x, head=("vacc_"), draws=save_draws)))

### plot!
system(paste0("qsub -N plot_gpr_output_vax -pe multi_slot 3 -P PROJECT -o /FILEPATH/", username,
              " -e FILEPATH/", username, " FILEPATH/r_shell.sh ", 
              "/FILEPATH/_plot_all_vaccines.r ", date))

### make lagged covariates
invisible(lapply(c(5, 10), function(x) make_lags(me="vacc_hepb3", lag_years=x)))

### make aged out covariates
make_cohort(me="vacc_hepb3")
#***********************************************************************************************************************


#----MARK BEST----------------------------------------------------------------------------------------------------------
best <- TRUE
if (best) {
  save.best <- file.path(draws_root, "exp", paste0("gbd", year_end), "best")
  system(paste0("unlink ", save.best, "; ln -s ", save.root, " ", save.best))
  results.best <- file.path(data_root, "FILEPATH", paste0("gbd", year_end), "best")
  system(paste0("unlink ", results.best, "; ln -s ", results.root, " ", results.best))
}
#***********************************************************************************************************************


#----UPLOAD AS COVARIATE------------------------------------------------------------------------------------------------
### prep
hepb_special <- c("vacc_hepb3_lag_5", "vacc_hepb3_lag_10", "vacc_hepb3_cohort")
dtp_ratios   <- c("vacc_hib3", "vacc_rotac", "vacc_hepb3", "vacc_pcv3")
mcv_ratios   <- c("vacc_mcv2", "vacc_rcv1")
mes          <- c("vacc_dpt3", "vacc_mcv1", "vacc_polio3", "vacc_bcg", hepb_special, dtp_ratios, mcv_ratios)

### save for covariate upload
upload.root  <- file.path(data_root, "FILEPATH", date); if (!dir.exists(upload.root)) dir.create(upload.root)
lapply(mes, function(x) {
  
  ### save to directory
  data <- readRDS(file.path(results.root, paste0(x, ".rds")))
  cov_id <- me_db[me_name==x, covariate_id]
  data[, covariate_id := cov_id]
  data[, covariate_name_short := me_db[me_name==x, covariate_name_short]]
  setnames(data, c("gpr_mean", "gpr_lower", "gpr_upper"), c("mean_value", "lower_value", "upper_value")) 
  data[, c("me_name", "measure_id") := NULL]
  write.csv(data, paste0(upload.root, "/", unique(data$covariate_name_short), ".csv"), row.names=FALSE)
  
  ### save_results
  if (x %in% dtp_ratios) { name <- paste0(x, "_dpt3_ratio")
  } else if (x %in% mcv_ratios) { name <- paste0(x, "_mcv1_ratio")
  } else if (x %in% hepb_special) { name <- "vacc_hepb3_dpt3_ratio"
  } else { name <- x }
  save_results_description <- fread(run_log)[me_name==name & is_best==1, notes]
  job <- paste0("qsub -N save_", x, " -pe multi_slot 10 -P PROJECT -o FILEPATH/", username, " -e FILEPATH/", username, 
                " FILEPATH/r_shell.sh FILEPATH/save_results_wrapper.r",
                " --args",
                " --type covariate",
                " --me_id ", cov_id, 
                " --input_directory ", upload.root,
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", TRUE,
                " --file_pattern ", unique(data$covariate_name_short))
  system(job); print(job)
  
})
#***********************************************************************************************************************


#----UPLOAD AS SUSTAINABLE DEVELOPMENT GOALS----------------------------------------------------------------------------
### run full vaccine coverage indicator, upload
save_sdgs <- TRUE
if (save_sdgs) {
  
# make draws for SDG vaccine indicators, not duplicating out age groups (just age group 22, all ages)
full_vax_mes_direct <- c("vacc_dpt3", "vacc_mcv1", "vacc_bcg", "vacc_polio3")
full_vax_mes_ratio  <- c("vacc_hepb3_dpt3_ratio", "vacc_hib3_dpt3_ratio", "vacc_rotac_dpt3_ratio", "vacc_pcv3_dpt3_ratio")
ind_vax_mes_direct  <- c("vacc_dpt3", "vacc_polio3", "vacc_mcv1")

# make full coverage indicator, upload
source("FILEPATH/sdgs.r")

}
#***********************************************************************************************************************


#----UPLOAD AS EXPOSURE-------------------------------------------------------------------------------------------------
### save duplicate draws for exposure upload
if (save_sdgs) job_hold(paste0("save_sdg_", indicators))
lapply(c("vacc_dpt3", "vacc_mcv1"), function(x) prep.draws(x, draws=TRUE, dupe=TRUE, quantiles=FALSE))
ratios_exp <- c("vacc_mcv2_mcv1_ratio", "vacc_hib3_dpt3_ratio", "vacc_rotac_dpt3_ratio", "vacc_pcv3_dpt3_ratio")
lapply(ratios_exp, function(x) prep.ratio(x, head=("vacc_"), draws=TRUE, dupe=TRUE, quantiles=FALSE))

### prep 
mes   <- c("vacc_dpt3", "vacc_hib3", "vacc_mcv1",  "vacc_rotac", "vacc_pcv3", "vacc_mcv2")

### save for covariate upload
invisible(lapply(mes, function(x) {
  
  ### save_results
  if (x %in% c("vacc_hib3", "vacc_rotac", "vacc_hepb3", "vacc_pcv3")) { name <- paste0(x, "_dpt3_ratio")
  } else if (x %in% c("vacc_mcv2", "vacc_rcv1")) { name <- paste0(x, "_mcv1_ratio")
  } else { name <- x }
  save_results_description <- fread(run_log)[me_name==name & is_best==1, notes]
  if (x=="vacc_dpt3") xx <- "vacc_dtp3" else xx <- x
  exp_me_id <- me_db[me_name==xx, exp_me_id]
  job <- paste0("qsub -N save_exp_", x, " -pe multi_slot 20 -P PROJECT -o FILEPATH/", username, " -e FILEPATH/", username, 
                " FILEPATH/save_results_wrapper.r",
                " --args",
                " --type epi",
                " --me_id ", exp_me_id, 
                " --input_directory ", file.path(save.root, x),
                " --descript ", "\"", gsub(" ", "_", save_results_description), "\"",
                " --best ", TRUE,
                " --measure_epi 18")
  system(job); print(job)
  
}))
#***********************************************************************************************************************