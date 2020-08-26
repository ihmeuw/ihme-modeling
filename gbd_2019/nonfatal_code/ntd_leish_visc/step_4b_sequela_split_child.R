# process location ids - zero, is a custom subnational, is a national
# solve for prevalence from stgpr incidence model using durations
#
### ======================= BOILERPLATE ======================= ###

rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
  loc_id <- 214
}

library(data.table)
library(argparse, lib.loc= "FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
draws_dir    <- paste0(run_dir, "FILEPATH")
interms_dir    <- paste0(run_dir, "FILEPATH")
params_dir  <- paste0(data_root, "FILEPATH")

#load in saved workspace
load(file = paste0(interms_dir, "FILEPATH"))

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

location_id <- params[task_id, location_id]
loc_id <- location_id
stgpr_run_id     <- params[task_id, stgpr_run_id]
decomp_step <- params[task_id, decomp_step]
gbd_round_id <- params[task_id, gbd_round_id]

### ======================= Main Execution ======================= ###

# duration used for each meid since GBD 2016
duration_ADDRESS1 <- 0.25
duration_ADDRESS2 <- 0.1875
duration_ADDRESS3 <- 0.25 - 0.1875

full_year_id<-c(1980:max(full_year_id))

if (loc_id %in% unique_vl_locations){
  cat("is an unique location")
  #if yes, load in its st-gpr all-age estimate
  if( loc_id %in% subnat_prop[, location_id ]){
    cat("is an unique subnat)")
    parent_id <- subnat_prop[location_id == loc_id, parent_id]
    #if a subnational, pull in the national envelope and multiply the count by proportion
    #for Indian subnationals, need to piece out India, not level 5
    
    loc_stgpr<-read.csv(paste0("FILEPATH", stgpr_run_id, "FILEPATH",parent_id,'.csv'))
    
    # stack these
    for(alpha in full_year_id){
      cat(paste0("alpha at: ", alpha))
      annual_stgpr<-subset(loc_stgpr,loc_stgpr$year_id==alpha)
      annual_stgpr_draws<-annual_stgpr[,c(-1:-4,-1005)]
      incidence_df<-empty_df
      prevalence_ADDRESS1_df<-empty_df
      prevalence_ADDRESS2_df<-empty_df
      prevalence_ADDRESS3_df<-empty_df
      population_pull_sex1<-get_population(location_id=loc_id, year_id = alpha, age_group_id = full_age_set, sex_id=1, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
      population_pull_sex2<-get_population(location_id=loc_id, year_id = alpha, age_group_id = full_age_set, sex_id=2, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
      population_pull<-rbind(population_pull_sex1, population_pull_sex2)
      full_population_pull<-get_population(location_id=location_id, year_id=alpha, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
      full_country_pull<-get_population(location_id=parent_id, year_id=alpha, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
      country_pop<-full_country_pull$population
      total_pop<-full_population_pull$population
      split_pop<-data.frame(population_pull$population)
      
      for(draw in 1:1000){
        envelope<-annual_stgpr_draws[1,draw]
        envelope_country_cases<-envelope*country_pop
        envelope_cases<-envelope_country_cases*subnat_prop[location_id == loc_id, draw_0]
        draw_ratio<-data.frame(draws_ratio[[draw]])
        names(draw_ratio)<-paste0("draw_",draw-1)
        inflated_ratio<-draw_ratio*split_pop
        rescaled_ratio<-inflated_ratio*envelope_cases/sum(inflated_ratio)
        result<-rescaled_ratio/split_pop
        result_prevalence_ADDRESS1<-result*duration_ADDRESS1
        result_prevalence_ADDRESS2<-result*duration_ADDRESS2
        result_prevalence_ADDRESS3<-result*duration_ADDRESS3
        incidence_df<-cbind(incidence_df,result)
        prevalence_ADDRESS1_df<-cbind(prevalence_ADDRESS1_df,result_prevalence_ADDRESS1)
        prevalence_ADDRESS2_df<-cbind(prevalence_ADDRESS2_df,result_prevalence_ADDRESS2)
        prevalence_ADDRESS3_df<-cbind(prevalence_ADDRESS3_df,result_prevalence_ADDRESS3)
      }

      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(loc_id,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_df$modelable_entity_id<-rep(ADDRESS1,nrow(incidence_df))
      
      prevalence_ADDRESS1_df$year_id<-rep(alpha,nrow(prevalence_ADDRESS1_df))
      prevalence_ADDRESS1_df$location_id<-rep(loc_id,nrow(prevalence_ADDRESS1_df))
      prevalence_ADDRESS1_df$measure_id<-rep(5,nrow(prevalence_ADDRESS1_df))
      prevalence_ADDRESS1_df$modelable_entity_id<-rep(ADDRESS1,nrow(prevalence_ADDRESS1_df))
      
      inc_prev_ADDRESS1_df<-rbind(incidence_df,prevalence_ADDRESS1_df)
      
      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(loc_id,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_df$modelable_entity_id<-rep(ADDRESS2,nrow(incidence_df))
      
      prevalence_ADDRESS2_df$year_id<-rep(alpha,nrow(prevalence_ADDRESS2_df))
      prevalence_ADDRESS2_df$location_id<-rep(loc_id,nrow(prevalence_ADDRESS2_df))
      prevalence_ADDRESS2_df$measure_id<-rep(5,nrow(prevalence_ADDRESS2_df))
      prevalence_ADDRESS2_df$modelable_entity_id<-rep(ADDRESS2,nrow(prevalence_ADDRESS2_df))
      
      inc_prev_ADDRESS2_df<-rbind(incidence_df,prevalence_ADDRESS2_df)

      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(loc_id,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_df$modelable_entity_id<-rep(ADDRESS3,nrow(incidence_df))
      
      prevalence_ADDRESS3_df$year_id<-rep(alpha,nrow(prevalence_ADDRESS3_df))
      prevalence_ADDRESS3_df$location_id<-rep(loc_id,nrow(prevalence_ADDRESS3_df))
      prevalence_ADDRESS3_df$measure_id<-rep(5,nrow(prevalence_ADDRESS3_df))
      prevalence_ADDRESS3_df$modelable_entity_id<-rep(ADDRESS3,nrow(prevalence_ADDRESS3_df))
      
      inc_prev_ADDRESS3_df<-rbind(incidence_df,prevalence_ADDRESS3_df)
     
       if (alpha == 1980){
        all_draws_58 <- inc_prev_ADDRESS1_df
        all_draws_59 <- inc_prev_ADDRESS2_df
        all_draws_60 <- inc_prev_ADDRESS3_df
      } else {
        all_draws_58 <- rbind(inc_prev_ADDRESS1_df, all_draws_58)
        all_draws_59 <- rbind(inc_prev_ADDRESS2_df, all_draws_59)
        all_draws_60 <- rbind(inc_prev_ADDRESS3_df, all_draws_60)
      }

    }
    
    all_draws_58 <- as.data.table(all_draws_58)
    all_draws_59 <- as.data.table(all_draws_59)
    all_draws_60 <- as.data.table(all_draws_60)
    
    all_draws_58[, metric_id := 3]
    all_draws_59[, metric_id := 3]
    all_draws_60[, metric_id := 3]
    
    write.csv(all_draws_58, file=paste0(draws_dir, 'FILEPATH',loc_id,'.csv'))
    write.csv(all_draws_59, file=paste0(draws_dir, 'FILEPATH',loc_id,'.csv'))
    write.csv(all_draws_60, file=paste0(draws_dir, 'FILEPATH',loc_id,'.csv'))
    
  }else{
    cat("is a unique location but not a subnat")
    loc_stgpr<-read.csv(paste0("FILEPATH", stgpr_run_id, "FILEPATH",loc_id,'.csv'))
    
    # stack these
    for(alpha in full_year_id){
      cat(paste0("alpha at: ", alpha))
      
      annual_stgpr<-subset(loc_stgpr,loc_stgpr$year_id==alpha)
      annual_stgpr_draws<-annual_stgpr[,c(-1:-4,-1005)]
      incidence_df<-empty_df
      prevalence_ADDRESS1_df<-empty_df
      prevalence_ADDRESS2_df<-empty_df
      prevalence_ADDRESS3_df<-empty_df
      population_pull_sex1<-get_population(location_id=loc_id, year_id = alpha, age_group_id = full_age_set, sex_id=1, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
      population_pull_sex2<-get_population(location_id=loc_id, year_id = alpha, age_group_id = full_age_set, sex_id=2, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
      population_pull<-rbind(population_pull_sex1, population_pull_sex2)
      full_population_pull<-get_population(location_id=loc_id, year_id=alpha, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
      total_pop<-full_population_pull$population
      split_pop<-data.frame(population_pull$population)

      for(draw in 1:1000){
        envelope<-annual_stgpr_draws[1,draw]
        envelope_cases<-envelope*total_pop
        draw_ratio<-data.frame(draws_ratio[[draw]])
        names(draw_ratio)<-paste0("draw_",draw-1)
        inflated_ratio<-draw_ratio*split_pop
        rescaled_ratio<-inflated_ratio*envelope_cases/sum(inflated_ratio)
        result<-rescaled_ratio/split_pop
        result_prevalence_ADDRESS1<-result*duration_ADDRESS1
        result_prevalence_ADDRESS2<-result*duration_ADDRESS2
        result_prevalence_ADDRESS3<-result*duration_ADDRESS3
        incidence_df<-cbind(incidence_df,result)
        prevalence_ADDRESS1_df<-cbind(prevalence_ADDRESS1_df,result_prevalence_ADDRESS1)
        prevalence_ADDRESS2_df<-cbind(prevalence_ADDRESS2_df,result_prevalence_ADDRESS2)
        prevalence_ADDRESS3_df<-cbind(prevalence_ADDRESS3_df,result_prevalence_ADDRESS3)
      }

      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(loc_id,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_df$modelable_entity_id<-rep(ADDRESS1,nrow(incidence_df))
      
      prevalence_ADDRESS1_df$year_id<-rep(alpha,nrow(prevalence_ADDRESS1_df))
      prevalence_ADDRESS1_df$location_id<-rep(loc_id,nrow(prevalence_ADDRESS1_df))
      prevalence_ADDRESS1_df$measure_id<-rep(5,nrow(prevalence_ADDRESS1_df))
      prevalence_ADDRESS1_df$modelable_entity_id<-rep(ADDRESS1,nrow(prevalence_ADDRESS1_df))
      
      inc_prev_ADDRESS1_df<-rbind(incidence_df,prevalence_ADDRESS1_df)

      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(loc_id,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_df$modelable_entity_id<-rep(ADDRESS2,nrow(incidence_df))
      
      prevalence_ADDRESS2_df$year_id<-rep(alpha,nrow(prevalence_ADDRESS2_df))
      prevalence_ADDRESS2_df$location_id<-rep(loc_id,nrow(prevalence_ADDRESS2_df))
      prevalence_ADDRESS2_df$measure_id<-rep(5,nrow(prevalence_ADDRESS2_df))
      prevalence_ADDRESS2_df$modelable_entity_id<-rep(ADDRESS2,nrow(prevalence_ADDRESS2_df))
      
      inc_prev_ADDRESS2_df<-rbind(incidence_df,prevalence_ADDRESS2_df)

      incidence_df$year_id<-rep(alpha,nrow(incidence_df))
      incidence_df$location_id<-rep(loc_id,nrow(incidence_df))
      incidence_df$measure_id<-rep(6,nrow(incidence_df))
      incidence_df$modelable_entity_id<-rep(ADDRESS3,nrow(incidence_df))
      
      prevalence_ADDRESS3_df$year_id<-rep(alpha,nrow(prevalence_ADDRESS3_df))
      prevalence_ADDRESS3_df$location_id<-rep(loc_id,nrow(prevalence_ADDRESS3_df))
      prevalence_ADDRESS3_df$measure_id<-rep(5,nrow(prevalence_ADDRESS3_df))
      prevalence_ADDRESS3_df$modelable_entity_id<-rep(ADDRESS3,nrow(prevalence_ADDRESS3_df))
      
      inc_prev_ADDRESS3_df<-rbind(incidence_df,prevalence_ADDRESS3_df)
      
      if (alpha == 1980){
        all_draws_58 <- inc_prev_ADDRESS1_df
        all_draws_59 <- inc_prev_ADDRESS2_df
        all_draws_60 <- inc_prev_ADDRESS3_df
      } else {
        all_draws_58 <- rbind(inc_prev_ADDRESS1_df, all_draws_58)
        all_draws_59 <- rbind(inc_prev_ADDRESS2_df, all_draws_59)
        all_draws_60 <- rbind(inc_prev_ADDRESS3_df, all_draws_60)
      }
    }
    
    all_draws_58 <- as.data.table(all_draws_58)
    all_draws_59 <- as.data.table(all_draws_59)
    all_draws_60 <- as.data.table(all_draws_60)
    
    all_draws_58[, metric_id := 3]
    all_draws_59[, metric_id := 3]
    all_draws_60[, metric_id := 3]
    write.csv(all_draws_58,file=paste0(draws_dir, 'FILEPATH',loc_id,'.csv'))
    write.csv(all_draws_59,file=paste0(draws_dir, 'FILEPATH',loc_id,'.csv'))
    write.csv(all_draws_60,file=paste0(draws_dir, 'FILEPATH',loc_id,'.csv'))
  }
}else{
  cat("is geo restricted")
  zero_draws <- as.data.table(zero_draws)
  zero_draws[, location_id := loc_id]
  zero_draws[, metric_id := 3]
  all_draws_58 <- zero_draws[, modelable_entity_id := ADDRESS]
  write.csv(all_draws_58, paste0(draws_dir, 'FILEPATH',loc_id,'.csv'), row.names = FALSE)
  all_draws_59 <- all_draws_58[, modelable_entity_id := ADDRESS]
  write.csv(all_draws_59, paste0(draws_dir, 'FILEPATH',loc_id,'.csv'))
  all_draws_60 <- all_draws_59[, modelable_entity_id := ADDRESS]
  write.csv(all_draws_60, paste0(draws_dir, 'FILEPATH',loc_id,'.csv'))
}
