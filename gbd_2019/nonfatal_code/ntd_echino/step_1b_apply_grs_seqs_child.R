#' [Title: Leprosy Apply GRs Parent
#' [Author: Chase Gottlich
#' [Date:  07/12/19 (Decomp 2)
#' [Notes: Array job for GRs
#' [TBD TODO: Array jobs reads restricted, mvid from best zero draws from central function, when grs are same est ADDRESS1 and ADDRESS2 together, new gr meid], make years, zero draw file, model versions generalizable

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

# packages

library(data.table)
library(argparse, lib.loc= "/ihme/ntds/ntd_envs/singularity_packages/3.5.0")
library(stringr)
source("/ihme/cc_resources/libraries/current/r/get_draws.R")
source("/ihme/cc_resources/libraries/current/r/get_model_results.R")
source("/ihme/cc_resources/libraries/current/r/get_location_metadata.R")

#############################################################################################
###'                                 [Recieve Qsub]                                       ###
#############################################################################################

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path", help = "param path", default = "168", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
params  <- fread(param_path)
my_loc <- params[task_id, location_id]
draws_dir <- "/ihme/ntds/ntd_models/ntd_models/ntd_echino/runs/run3_2019-10-07/draws/"

#############################################################################################
###'                              [Estimate ADDRESS3, ADDRESS4, ADDRESS5]                             ###
#############################################################################################

grs <- fread("/ihme/ntds/ntd_models/ntd_models/ntd_echino/params/ntd_echino_lgr.csv")
r_locs <- unique(grs[value_endemicity == 0, location_id])

if (my_loc %in% r_locs){
  
  draws <- fread("/ihme/ntds/ntd_models/ntd_models/ntd_echino/params/zero_draw_file.csv")
  draws <- draws[, location_id := my_loc]
  draws <- draws[measure_id == 5]
  draws[, metric_id := 3]
  draws <- draws[!(age_group_id %in% c(164, 27))]
  
  #ADDRESS3
  draws[, modelable_entity_id := ADDRESS3]
  write.csv(draws, paste0(draws_dir, "ADDRESS3/", my_loc, ".csv"), row.names = FALSE)
  
  #ADDRESS4
  draws[, modelable_entity_id := ADDRESS4]
  write.csv(draws, paste0(draws_dir, "ADDRESS4/", my_loc, ".csv"), row.names = FALSE)
  
  #ADDRESS5
  draws[, modelable_entity_id := ADDRESS5]
  write.csv(draws, paste0(draws_dir, "ADDRESS5/", my_loc, ".csv"), row.names = FALSE)
  
}

# end locs, ADDRESS0 -> ADDRESS3 , ADDRESS4, ADDRESS5

if (!(my_loc %in% r_locs)){
  
  draws  <- get_draws(gbd_id_type = "modelable_entity_id",
                      gbd_id = ADDRESS0,
                      gbd_round_id = 6,
                      source = "epi",
                      measure_id = 5,
                      year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019),
                      location_id = my_loc,
                      decomp_step = "iterative",
                      version_id = ADDRESS,
                      sex_id = c(1,2)
  )
  
  #*Create thousand draws of proportions for abdominal, respiratory and epileptic symptoms among echinococcosis cases that
  #*add up to 1, given the observed sample sizes in Eckert & Deplazes, Clinical Microbiology Reviews 2004; 17(1) 107-135 (Table 3).
  #*Assume that the observed cases follow a multinomial distribution cat(p1,p2,p3), where (p1,p2,p3)~Dirichlet(a1,a2,a3),
  #*where the size parameters of the Dirichlet distribution are the number of observations in each category (must be non-zero).
  
  #Abdominal or pelvic cyst localization
  n_abd <- 50
  
  #thoracic cyst localization (lungs & mediastinum)
  n_thr <- 47
  
  #brain cyst localization
  n_brn <- 3
  
  #other localization (bones, muscles, and skin; currently not assigning this to a healthstate)
  #local n4 = 0
  
  prop_abd <- rgamma(n = 1000, shape = n_abd)
  prop_thr <- rgamma(n = 1000, shape = n_thr)
  prop_brn <- rgamma(n = 1000, shape = n_brn)
  
  # vectorized ie prop_abd[1] + prop_thr[1] + prop_brn[1] == scal_fct[1]  
  scal_fct <- prop_abd + prop_thr + prop_brn
  
  # vectorized again, scale so sum of proportions == 1 (internally consistent with parent)
  prop_abd <- prop_abd / scal_fct
  prop_thr <- prop_thr / scal_fct
  prop_brn <- prop_brn / scal_fct

  # append to column
  prop_draws <- data.table(uniq = 1)
  prop_draws[, paste0("abd_draw_", 0:999) := as.list(prop_abd)]
  prop_draws[, paste0("thr_draw_", 0:999) := as.list(prop_thr)]
  prop_draws[, paste0("brn_draw_", 0:999) := as.list(prop_brn)]
  
  draws <- cbind(draws, prop_draws)

  # apply proportions vectorizes
  draws[, paste0("draw_abd_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("abd_draw_", x)))]
  draws[, paste0("draw_thr_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("thr_draw_", x)))]
  draws[, paste0("draw_brn_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("brn_draw_", x)))]
  
  # clean
  
  abd_draws <- draws[, c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", paste0("draw_abd_", 0:999))]
  thr_draws <- draws[, c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", paste0("draw_thr_", 0:999))]
  brn_draws <- draws[, c("age_group_id", "location_id", "measure_id", "sex_id", "year_id", "metric_id", paste0("draw_brn_", 0:999))]
  
  setnames(abd_draws, paste0("draw_abd_", 0:999), paste0("draw_", 0:999))
  setnames(thr_draws, paste0("draw_thr_", 0:999), paste0("draw_", 0:999))
  setnames(brn_draws, paste0("draw_brn_", 0:999), paste0("draw_", 0:999))
  
  # write out
    
  abd_draws <- abd_draws[!(age_group_id %in% c(164, 27))]
  abd_draws[, modelable_entity_id := ADDRESS3]
  write.csv(abd_draws, paste0(draws_dir, "ADDRESS3/", my_loc, ".csv"), row.names = FALSE)
  
  thr_draws <- thr_draws[!(age_group_id %in% c(164, 27))]
  thr_draws[, modelable_entity_id := ADDRESS4]
  write.csv(thr_draws, paste0(draws_dir, "ADDRESS4/", my_loc, ".csv"), row.names = FALSE)
  
  brn_draws <- brn_draws[!(age_group_id %in% c(164, 27))]
  brn_draws[, modelable_entity_id := ADDRESS5]
  write.csv(brn_draws, paste0(draws_dir, "ADDRESS5/", my_loc, ".csv"), row.names = FALSE)
    
}