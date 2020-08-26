
Sys.umask(mode = 002)

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_demographics.R"  )

args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]
  
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
param_map <- fread(param_map_filepath)
  
location_id <- param_map[task_id, location_id]
airpol_shift_version <- param_map[task_id, airpol_shift_version]
make_diagnostic <- param_map[task_id, make_diagnostic]
results_dir <- param_map[task_id, results_dir]

estimation_years <- get_demographics(gbd_team = "epi", gbd_round_id = 6)$year_id

copula_dir = file.path(results_dir, "joint_distr")
copula_fp = paste0(location_id, ".rds")

shift_dir = paste0("FILEPATH/", airpol_shift_version, sep = "/")

shifted_copula_dir = file.path(results_dir, "airpol_shifted")

diagnos_dir = file.path(results_dir, "airpol_diagnostics")
warnings_dir = file.path(diagnos_dir, "warnings")

pdf_filepath = file.path(diagnos_dir, gsub(x=copula_fp, pattern = ".rds", replacement = ".pdf"))

copula <- readRDS(file.path(copula_dir, copula_fp))

shift <- lapply(estimation_years, function(e_year){

  shift_fp = paste0(location_id, "_", e_year, ".csv")
  shift <- fread(file.path(shift_dir, shift_fp ))
  shift <- shift[draw %in% 1:100, ]
  shift[draw == 100, draw := 0]
  shift[, draw := paste0('draw_', draw)]
  setnames(shift, c("bw", "ga"), c("bw_shift", "ga_shift"))
  
  return(shift)
    
}) %>% rbindlist(use.names=T, fill = T)

copula <- merge(copula, shift)

copula[, bw_orig := bw]
copula[, ga_orig := ga]

copula[, bw := bw_orig - bw_shift]
copula[, ga := ga_orig - ga_shift]

saveRDS(copula[, -c("bw_orig", "ga_orig", "ga_shift", "bw_shift")], 
        file.path(shifted_copula_dir, copula_fp))

