rm(list=ls())

Sys.umask(mode = 002)

os <- .Platform$OS.type
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/find_non_draw_cols.R")
source("FILEPATH/get_draws.R"  )
source("FILEPATH/lbwsg_copula_functions.R"  )

args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
param_map <- fread(param_map_filepath)
location_id = param_map[task_id, location_id]
out_dir = param_map[task_id, out_dir]
num_jointdistr_draws = param_map[task_id, num_jointdistr_draws]  

ens_dir = file.path(out_dir, "ensemble/draws")
copula_dir = file.path(out_dir, "copula")


ga_28 <- get_draws(gbd_id_type = "modelable_entity_id", 
                   source = "epi", 
                   location_id = location_id,
                   gbd_id = 24448,
                   year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019), 
                   decomp_step = "step4", gbd_round_id = 6)


ga_28 <- melt(ga_28, id.vars = find_non_draw_cols(ga_28), variable = "draw", value.name = "prev_28")

ga_ens <- readRDS(file.path(ens_dir, "ga", paste0(location_id, '.rds')))
ga_ens <- merge(ga_ens, ga_28[, .(location_id, year_id, age_group_id, sex_id, draw, prev_28)])

bw_ens <- readRDS(file.path(ens_dir, "bw", paste0(location_id, '.rds')))

cop <- readRDS(file.path(copula_dir, "joint_distr_unadj", paste0(location_id, ".rds")))
cop[, draw := paste0("draw_", draw)]

ga_ens <- merge(ga_ens, cop[ga < 37, .(cop_prev = .N / num_jointdistr_draws), by = .(location_id, year_id, sex_id, age_group_id, draw)], by = c("location_id", "year_id", "sex_id", "age_group_id", "draw"))
bw_ens <- merge(bw_ens, cop[bw < 2500, .(cop_prev = .N / num_jointdistr_draws), by = .(location_id, year_id, sex_id, age_group_id, draw)], by = c("location_id", "year_id", "sex_id", "age_group_id", "draw"))

prev <- rbind(ga_ens, bw_ens, use.names = T, fill = T)

saveRDS(prev, file.path(out_dir, "copula", "vetting", "summarize", paste0(location_id, ".rds")))

cop.list = split(cop, by = c("location_id", "year_id", "sex_id", "age_group_id", "draw"))

cop <- lapply(cop.list, function(c){
  
  ids <- unique(c[, .(location_id, sex_id, year_id, age_group_id, draw)])
  ids <- merge(prev, ids)

  ta <- try (
    ga37_par <- optim(0.1, fn = rake_optim_37, 
                      sim = c, 
                      ga_u_37 = ids[dimension == "ga", prev], 
                      method = "Brent", lower = -0.1, upper = 1)$par, silent = T)
  
  
  if(class(ta) == "try-error"){ ga37_par = 0  }
  c <- rake_return_37(par = ga37_par, c)
  c[ga == Inf, ga := max(old_ga)]
  
  tb <- try (
    ga28_par <- optim(0.5, fn = rake_optim_28, 
                      sim = c, 
                      ga_u_28 = ids[dimension == "ga", prev_28], 
                      ga_u_37 = ids[dimension == "ga", prev], 
                      method = "Brent", lower = 0.01, upper = 2)$par, silent = T)
  
  if(class(tb) == "try-error"){  ga28_par = 0 }
  c <- rake_return_28(par = ga28_par, c)
  
  t2a <- try (
    
    bw2500_par  <- optim(1, fn = rake_optim_2500, 
                     sim = c, 
                     bw_u_2500 = ids[dimension == "bw", prev], 
                     method = "Brent", lower = -2, upper = 2)$par, silent = T)
  
  
  if(class(t2a) == "try-error"){ bw2500_par = 0 }
  c <- rake_return_2500(par = bw2500_par, c)
  c[bw == Inf, bw := max(old_bw)]
  
  t2 <- try (
    
    bw_par  <- optim(1, fn = rake_optim_500, 
                     sim = c, 
                     bw_u_2500 = ids[dimension == "bw", prev], 
                     method = "Brent", lower = 0.01, upper = 3)$par, silent = T)
  
  
  if(class(t2) == "try-error"){    bw_par = 0  }
  c <- rake_return_500(par = bw_par, c)
  
  return(c)
  
}) %>% rbindlist(use.names = T, fill = T)


saveRDS(cop, file.path(copula_dir, "joint_distr", paste0(location_id, ".rds")))
