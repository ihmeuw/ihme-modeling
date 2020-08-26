rm(list=ls())

os <- .Platform$OS.type

repo = "FILEPATH"
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)
library(copula)
library(VineCopula)
library(actuar, lib.loc = my_libs)

source("FILEPATH/edensity.R")
source("FILEPATH/pdf_families.R")
source(file.path(repo, "FILEPATH/lbwsg_copula_functions.R"))

dlist <- c(classA, classB, classM)
  
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  
  ## Retrieving array task_id
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  param_map <- fread(param_map_filepath)
  
  location_id = param_map[task_id, location_id]
  out_dir = param_map[task_id, out_dir]
  ga_weights_fp = param_map[task_id, ga_weights_fp]
  bw_weights_fp = param_map[task_id, bw_weights_fp]
  num_jointdistr_draws = param_map[task_id, num_jointdistr_draws]  

ens_dir = file.path(out_dir, "ensemble/draws")
copula_dir = file.path(out_dir, "copula")


# Get copula
copula <- readRDS(file.path(copula_dir, "global_copula.rds"))

# Get weights
ga_weights <- fread(ga_weights_fp)
bw_weights <- fread(bw_weights_fp)

weights_ga <- ga_weights[1, -c("location_id", "age_group_id", "sex_id", "year_id")]
weights_bw <- bw_weights[1, -c("location_id", "age_group_id", "sex_id", "year_id")]

ga_meanSDs <- readRDS(file.path(ens_dir, "ga", paste0(location_id, ".rds")))
bw_meanSDs <- readRDS(file.path(ens_dir, "bw", paste0(location_id, ".rds")))

if(nrow(ga_meanSDs) != nrow(bw_meanSDs)){ stop("GA & BW have different numbers of rows")  }

meanSDs <- rbind(ga_meanSDs, bw_meanSDs, fill = T, use.names = T)
meanSDs <- dcast(meanSDs, location_id + sex_id + year_id + age_group_id + draw ~ dimension, value.var = c("prev", "mean", "sd"))

meanSDs <- meanSDs[year_id %in% c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)]
meanSDs <- meanSDs[draw %in% paste0("draw_", 0:99)]

joint_distr <- lapply(1:nrow(meanSDs), function(i){
  
                    message("Row: ", i)
  
                    meanSD_i <- meanSDs[i, ]
                    
                    var1_density.list <- ModelMarginalDistr(mean=meanSD_i$mean_ga, sd = meanSD_i$sd_ga, weights = weights_ga, univ_fam = "ensemble")
                    var2_density.list <- ModelMarginalDistr(mean=meanSD_i$mean_bw, sd = meanSD_i$sd_bw, weights = weights_bw, univ_fam = "ensemble")
                  
                    allvar_density.list <- list(var1_density.list, var2_density.list)
  
                    # Simulate joint distribution from gestational age marginal distribution and birthweight marginal distribution
                    joint_distr <- RVineSim(N = num_jointdistr_draws, RVM = copula) %>% data.table()
                    setnames(joint_distr, names(joint_distr), paste0(names(joint_distr), "_pobs"))
                    joint_distr <- cbind(meanSD_i[, .(location_id, sex_id, year_id, age_group_id, draw)], joint_distr)
                    
                    lapply(1:2, function(i){
                      
                      var_density.list <- allvar_density.list[[i]]
                      
                      joint_distr[, (paste0("var", i)) := ApplyQuantileFunction(data = get(paste0("var", i, "_pobs")), dens.list = var_density.list)$modeled]
                      
                    })
                    
                    return(joint_distr)
                    
                    
                  }) 


saveRDS(joint_distr, file.path(copula_dir, "joint_distr_unadj", paste0(location_id, "_list.rds")))

joint_distr <- rbindlist(joint_distr, use.names = T, fill = T)

joint_distr[, draw := gsub(x = draw, pattern = "draw_", replacement = "")]

joint_distr <- joint_distr[, -c("var1_pobs", "var2_pobs")]
setnames(joint_distr, c("var1", "var2"), c("ga", "bw"))

saveRDS(joint_distr, file.path(copula_dir, "joint_distr_unadj", paste0(location_id, ".rds")))
