
rm(list=ls())

Sys.umask(mode = 002)

library(data.table)
library(magrittr)
library(ggplot2)
library(argparse)

source("FILEPATH/get_demographics.R"  )
source("FILEPATH/find_non_draw_cols.R")
source("FILEPATH/get_draws.R"  )
source("FILEPATH/rake_functions.R")

run_interactively = 1

if(run_interactively == 1){
  
  jointdistr_fp = "35453_2_2_1995.rds"
  jointdistr_dir = "FILEPATH"
  ga_ens_dir = "FILEPATH"
  bw_ens_dir = "FILEPATH"
  decomp_step = "iterative"
  gbd_round_id = 7
  
} else {
  
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  
  ## Retrieving array task_id
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  param_map <- fread(param_map_filepath)
  
  jointdistr_fp = param_map[task_id, jointdistr_fp]
  jointdistr_dir = param_map[task_id, jointdistr_dir]
  ga_ens_dir = param_map[task_id, ga_ens_dir]
  bw_ens_dir = param_map[task_id, bw_ens_dir]
  decomp_step = param_map[task_id, decomp_step]
  gbd_round_id = param_map[task_id, gbd_round_id]
  
}



joint_distr <- readRDS(file.path(jointdistr_dir, "joint_distr", jointdistr_fp))

location_ids <- unique(joint_distr$location_id)
sex_ids <- unique(joint_distr$sex_id)
age_group_ids <- unique(joint_distr$age_group_id)
year_ids <- unique(joint_distr$year_id)
num_draws <- length(unique(joint_distr$draw))

ga_ens <- readRDS(file.path(ga_ens_dir, paste0("meanSD_", jointdistr_fp)))
#this is making the prev_28 column the same as the <28 weeks STGPR estimate
ga_ens[, prev_28 := meid_24448]

bw_ens <- readRDS(file.path(bw_ens_dir, paste0("meanSD_", jointdistr_fp)))

# this is just making it so we can merge by draw, need them in the same format
joint_distr[, draw := paste0("draw_", draw)]

# Should be one number (10,000)
num_jointdistr_draws <- unique(joint_distr[,.N,.(location_id,year_id,sex_id,age_group_id,draw)]$N)

ga_ens <- merge(ga_ens, joint_distr[ga < 37, .(prev = .N / num_jointdistr_draws), by = .(location_id, year_id, sex_id, age_group_id, draw)], by = c("location_id", "year_id", "sex_id", "age_group_id", "draw"))
bw_ens <- merge(bw_ens, joint_distr[bw < 2500, .(prev = .N / num_jointdistr_draws), by = .(location_id, year_id, sex_id, age_group_id, draw)], by = c("location_id", "year_id", "sex_id", "age_group_id", "draw"))


bw_ens[, prev := meid_24450]

prev <- rbind(ga_ens, bw_ens, use.names = T, fill = T)

dir.create(file.path(jointdistr_dir, "vetting", "summarize"), recursive=T, showWarnings=F)
saveRDS(prev, file.path(jointdistr_dir, "vetting", "summarize", jointdistr_fp))



# Make adjustments so that copula exactly matches STGPR models
jointdistr.list = split(joint_distr, by = c("location_id", "year_id", "sex_id", "age_group_id", "draw"))

jointdistr <- lapply(jointdistr.list, function(jd){
  
  print(unique(jd$draw))
  
  ids <- unique(jd[, .(location_id, sex_id, year_id, age_group_id, draw)])
  #this takes the rows from prev that are from that draw, one row for ga one for bw
  ids <- merge(prev, ids, by = c("location_id", "sex_id", "year_id", "age_group_id", "draw"))
  

  # Do not adjust for ga_37 in raking (already matched in ga_ens thresholds)
  jd[, old_ga := ga]
  
  # Match preterm 28 STGPR model
  tb <- try (
    ga28_par <- optim(0.5, fn = stretch_optim_28, 
                      sim = jd, 
                      ga_u_28 = ids[var == "ga", prev_28], 
                      ga_u_37 = ids[var == "ga", prev], 
                      method = "Brent", lower = 0.01, upper = 2)$par, silent = T)
  
  if(class(tb) == "try-error"){  ga28_par = 0 }
  jd <- stretch_return_28(par = ga28_par, jd)
  
  
  
  #Match BW STGPR model
  t2a <- try (
    
    bw2500_par  <- optim(1, fn = stretch_optim_2500,
                         sim = jd,
                         bw_u_2500 = ids[var == "bw", prev],
                         method = "Brent", lower = -2, upper = 2)$par, silent = T)
  
  
  if(class(t2a) == "try-error"){ bw2500_par = 0 }
  jd <- stretch_return_2500(par = bw2500_par, jd)
  jd[bw == Inf, bw := max(old_bw)]
  

  
  # Match ratio of 500 to 2500 in data
  t2 <- try (
    
    bw_par  <- optim(1, fn = stretch_optim_500, 
                     sim = jd, 
                     bw_u_2500 = ids[var == "bw", prev], 
                     method = "Brent", lower = 0.01, upper = 3)$par, silent = T)
  
  
  if(class(t2) == "try-error"){    bw_par = 0  }
  jd <- stretch_return_500(par = bw_par, jd)
  
  return(jd)
  
}) %>% rbindlist(use.names = T, fill = T)


# Save model
saveRDS(jointdistr, file.path(jointdistr_dir, "joint_distr_raked", jointdistr_fp))

