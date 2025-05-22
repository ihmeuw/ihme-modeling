
#-----------------------

rm(list=ls())

os <- .Platform$OS.type

my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)
library(copula)
library(VineCopula)


source(file.path(paste0("FILEPATH/copula_functions.R")))
source(file.path(paste0("FILEPATH/ens_variance_functions.R")))
source("FILEPATH/pdf_families.R")


dlist <- c(classA, classM)

run_interactively = 0

if(run_interactively == 1){
  
  jointdistr_dir = "FILEPATH"
  version = "VERSION"
  var_names = c("ga,bw")
  num_jointdistr_draws = 10000
  copula_fp = file.path("FILEPATH")
  ens_dir = file.path("FILEPATH")
  meanSD_fp = "meanSD_212_2_2_2019.rds"
  
  
  
} else {
  
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  
  ## Retrieving array task_id
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  param_map <- fread(param_map_filepath)
  
  jointdistr_dir = param_map[task_id, jointdistr_dir]
  version = param_map[task_id, version]
  var_names = param_map[task_id, var_names]
  num_jointdistr_draws = param_map[task_id, num_jointdistr_draws]
  copula_fp = param_map[task_id, copula_fp]
  ens_dir = param_map[task_id, ens_dir]
  meanSD_fp = param_map[task_id, meanSD_fp]
  
}

if(var_names %like% ","){var_names <- unlist(strsplit(var_names, split=","))}


# Get copula
copula <- readRDS(copula_fp)

# Get Means & SDs
meanSDs <- lapply(var_names, function(var){ 
  meanSD <- readRDS(file.path(ens_dir, var, version, "meanSD", meanSD_fp)) 
}) %>% rbindlist(fill = T, use.names = T)

meanSDs <- dcast(meanSDs, location_id + sex_id + year_id + age_group_id + draw ~ var, value.var = c("mean", "sd"))


# Model joint distributions
joint_distr <- lapply(1:nrow(meanSDs), function(i){
  
  message("Row: ", i)
  meanSD_i <- meanSDs[i, ]
  
  # STEP1: Model marginal distributions 
  allvar_density.list <- lapply(var_names, function(var){
    
    weights <- fread(file.path(ens_dir, var, version, "weights.csv"))
    
    ens_params <- fread(file.path(ens_dir, var, version, "param_map.csv"))
    XMIN = unique(ens_params$XMIN)
    XMAX = unique(ens_params$XMAX)

    
    var_density.list <- ModelMarginalDistr(mean=meanSD_i[, get(paste0("mean_", var))], 
                                           sd = meanSD_i[, get(paste0("sd_", var))], 
                                           XMIN = XMIN, XMAX = XMAX,
                                           weights = weights, 
                                           univ_fam = "ensemble")
    return(var_density.list)
    
  })
  
  names(allvar_density.list) <- var_names
  
  
  # STEP2: Generate draw from joint distribution in 0-1 uniform distribution space
  joint_distr <- RVineSim(N = num_jointdistr_draws, RVM = copula) %>% data.table()
  setnames(joint_distr, names(joint_distr), paste0(var_names, "_pobs"))
  joint_distr <- cbind(meanSD_i[, .(location_id, sex_id, year_id, age_group_id, draw)], joint_distr)
  
  
  # STEP3: Pair marginal distributions with joint distribution to transform from 0-1 space to original space
  lapply(var_names, function(var){
    var_density.list <- allvar_density.list[[var]]
    joint_distr[, (paste0(var)) := ApplyQuantileFunction(data = get(paste0(var, "_pobs")), dens.list = var_density.list)$modeled]
  })
  
  
  return(joint_distr)
  
}) 



# Save and clean-up
joint_distr <- rbindlist(joint_distr, use.names = T, fill = T)
joint_distr[, draw := gsub(x = draw, pattern = "draw_", replacement = "")]
joint_distr_fp = gsub(meanSD_fp, pattern = "meanSD_", replacement = "")

dir.create(file.path(jointdistr_dir, paste(var_names, collapse = "_"), version, "joint_distr"), recursive = T)
saveRDS(joint_distr, file.path(jointdistr_dir, paste(var_names, collapse = "_"), version, "joint_distr", joint_distr_fp))
