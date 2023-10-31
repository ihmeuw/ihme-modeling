# --------------



rm(list=ls())

os <- .Platform$OS.type

library(data.table)
library(magrittr)
library(ggplot2)
library(copula)
library(VineCopula)

source(file.path("FILEPATH/fit_copula_functions.R"))


debug = 0

if(debug == 1){
  
  data_fp = "FILEPATH"
  copula_fam_dir = "FILEPATH"
  family = 2
  allow_rotations = FALSE
  
} else{
  ## Getting normal QSub arguments
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  
  ## Retrieving array task_id
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  param_map <- fread(param_map_filepath)
  
  data_fp <- param_map[task_id, data_fp]
  copula_fam_dir <- param_map[task_id, copula_fam_dir]
  family <- param_map[task_id, family]
  allow_rotations <- param_map[task_id, allow_rotations]
  
}

unlink(copula_fam_dir)
dir.create(copula_fam_dir)

data <- readRDS(data_fp)

# Find all cols with that match "var[anychar]_pobs"
pobs_cols <- names(data)[which(regexpr(pattern = "var.+_pobs", text = names(data)) == 1)]

fit.data <- data[, pobs_cols, with = F]

if(sum(unlist(fit.data[, lapply(.SD, min)]) < 0) == TRUE){
  stop("Range of data needs to be between 0-1. Check if all cols are between 0-1 in var1:n")
}

if(sum(unlist(fit.data[, lapply(.SD, max)]) > 1) == TRUE){
  stop("Range of data needs to be transformed into pseudo-observations space (pobs), which is between 0-1. Check if all cols are between 0-1 in var1:n")
}

# Remove "_pobs" suffix before fitting 
setnames(fit.data, pobs_cols, gsub(pobs_cols, pattern = "_pobs", replacement = ""))

copula <- fit_copula(data = fit.data, 
                     family = family, allow_rotations = allow_rotations,
                     param_filepath = file.path(copula_fam_dir, paste0("copula_params.rds")))

saveRDS(copula, file.path(copula_fam_dir, paste0("copula_obj.rds")))


# Simulate draws from joint
joint_distr <- RVineSim(N = nrow(fit.data), RVM = copula) %>% data.table()
setnames(joint_distr, names(joint_distr), paste0(names(joint_distr), "_sim_pobs"))

data <- cbind(data, joint_distr)
data[, row.num := .I]

saveRDS(data, file.path(copula_fam_dir, paste0("joint_distr_draws.rds")))


