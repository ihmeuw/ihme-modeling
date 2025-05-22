###########################################################################################
### Author: USERNAME
### Project: GBD Nonfatal Estimation
### Purpose: Collapse updated Norway subnationals for 2019 results to use in 2020 modeling
###########################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
  l <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

library(dplyr)
library(readr)
functions_dir <- paste0("FILEPATH")
my_dir <- "FILEPATH"
cause <- "tbi/"

source(paste0(functions_dir, "get_population.R"))
source(paste0(functions_dir, "get_age_metadata.R"))

locations <- c(4928,4927,4914,4911,4915,4912,4913,4917,4916,4919,4918,4921,4922)

age_dt <- get_age_metadata(12, gbd_round_id=7)
age_dt <- age_dt[age_group_id >= 13]
age_id <- age_dt$age_group_id

#Get 2019 populations 
pop_2019 <- get_population(gbd_round_id=6, sex_id = c(1,2), year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019), 
                           location_id = locations, age_group_id = age_id, decomp_step="step4")
pop_2019[,c("run_id")] <- NULL

# Open Norway subnationals to be aggregated
norway_4928 <- readRDS(paste0(my_dir, cause, "4928.rds"))
norway_4927 <- readRDS(paste0(my_dir, cause, "4927.rds"))
norway_4914 <- readRDS(paste0(my_dir, cause, "4914.rds"))
norway_4911 <- readRDS(paste0(my_dir, cause, "4911.rds"))
norway_4915 <- readRDS(paste0(my_dir, cause, "4915.rds"))
norway_4912 <- readRDS(paste0(my_dir, cause, "4912.rds"))
norway_4913 <- readRDS(paste0(my_dir, cause, "4913.rds"))
norway_4917 <- readRDS(paste0(my_dir, cause, "4917.rds"))
norway_4916 <- readRDS(paste0(my_dir, cause, "4916.rds"))
norway_4919 <- readRDS(paste0(my_dir, cause, "4919.rds"))
norway_4918 <- readRDS(paste0(my_dir, cause, "4918.rds"))
norway_4921 <- readRDS(paste0(my_dir, cause, "4921.rds"))
norway_4922 <- readRDS(paste0(my_dir, cause, "4922.rds"))

locations <- list(4928,4927,4914,4911,4915,4912,4913,4917,4916,4919,4918,4921,4922)

if(cause == "stroke/" | cause == "tbi/"){
# convert prevalence rates to counts so able to aggregate
print("Stroke or TBI")
  rates_to_cases <- function(loc_id){
  pop_tmp <- pop_2019[pop_2019$location_id == loc_id, ]
  dt_tmp <- get(paste0("norway_", loc_id))
  dt_tmp <- as.data.table(merge(dt_tmp, pop_tmp, by= c("sex_id", "year_id", "age_group_id"), all.x=T))
  dt_tmp[, paste0("case_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * population)]
  dt_tmp$location_id <- loc_id
  dt_tmp[, c(paste0("draw_", 0:999), "location_id.x", "location_id.y")] <- NULL
  return(dt_tmp)
  }
} else if(cause == "pd/" | cause == "ds/"){               
  print("PD or DS")
# convert prevalence rates to counts so able to aggregate
rates_to_cases <- function(loc_id){
  pop_tmp <- pop_2019[pop_2019$location_id == loc_id, ]
  dt_tmp <- get(paste0("norway_", loc_id))
  dt_tmp$metric_id <- 3
  dt_tmp$measure_id <- 5 
  dt_tmp <- as.data.table(merge(dt_tmp, pop_tmp, by= c("sex_id", "year_id", "age_group_id"), all.x=T))
  dt_tmp[, paste0("case_", 0:999) := lapply(0:999, function(x) get(paste0("prev_", x)) * population)]
  dt_tmp[, c(paste0("prev_", 0:999), "run_id")] <- NULL
  return(dt_tmp)
  }  
}

result <- lapply(locations, rates_to_cases) %>% rbindlist()


# norway 60133 - 4918 + 4919
norway_4918 <- result[result$location_id==4918, ]
norway_4919 <- result[result$location_id==4919, ]
for (i in 0:999) {
  colnames(norway_4918)[colnames(norway_4918)==paste0("case_", i)] <- paste0("V_", i)
}
norway_60133 <- as.data.table(merge(norway_4918, norway_4919, by= c("year_id", "age_group_id", "sex_id", "measure_id", "metric_id")))
norway_60133[,c("location_id.x", "location_id.y", "population.x", "population.y", "model_version_id.x", "model_version_id.y")] <- NULL
norway_60133[,location_id := 60133]
norway_60133[, paste0("agg_", 0:999) := lapply(0:999, function(x) get(paste0("case_", x))+get(paste0("V_", x)))]
norway_60133[,c(paste0("case_", 0:999), paste0("V_", 0:999))] <- NULL

# norway 60135 - 4912 + 4913
norway_4912 <- result[result$location_id==4912, ]
norway_4913 <- result[result$location_id==4913, ]
for (i in 0:999) {
  colnames(norway_4912)[colnames(norway_4912)==paste0("case_", i)] <- paste0("V_", i)
}
norway_60135 <- as.data.table(merge(norway_4912, norway_4913, by= c("year_id", "age_group_id", "sex_id", "metric_id", "measure_id")))
norway_60135[,c("location_id.x", "location_id.y", "population.x", "population.y", "model_version_id.x", "model_version_id.y")] <- NULL
norway_60135[,location_id := 60135]
norway_60135[, paste0("agg_", 0:999) := lapply(0:999, function(x) get(paste0("case_", x))+get(paste0("V_", x)))]
norway_60135[,c(paste0("case_", 0:999), paste0("V_", 0:999))] <- NULL

# norway 60137 - 4928 + 4927
norway_4928 <- result[result$location_id==4928, ]
norway_4927 <- result[result$location_id==4927, ]
for (i in 0:999) {
  colnames(norway_4928)[colnames(norway_4928)==paste0("case_", i)] <- paste0("V_", i)
}
norway_60137 <- as.data.table(merge(norway_4928, norway_4927, by= c("year_id", "age_group_id", "sex_id", "metric_id", "measure_id")))
norway_60137[,c("location_id.x", "location_id.y", "population.x", "population.y", "model_version_id.x", "model_version_id.y")] <- NULL
norway_60137[,location_id := 60137]
norway_60137[, paste0("agg_", 0:999) := lapply(0:999, function(x) get(paste0("case_", x))+get(paste0("V_", x)))]
norway_60137[,c(paste0("case_", 0:999), paste0("V_", 0:999))] <- NULL

# norway 60134 - 4917 + 4916
norway_4917 <- result[result$location_id==4917, ]
norway_4916 <- result[result$location_id==4916, ]
for (i in 0:999) {
  colnames(norway_4917)[colnames(norway_4917)==paste0("case_", i)] <- paste0("V_", i)
}
norway_60134 <- as.data.table(merge(norway_4917, norway_4916, by= c("year_id", "age_group_id", "sex_id", "measure_id", "metric_id")))
norway_60134[,c("location_id.x", "location_id.y", "population.x", "population.y", "model_version_id.x", "model_version_id.y")] <- NULL
norway_60134[,location_id := 60134]
norway_60134[, paste0("agg_", 0:999) := lapply(0:999, function(x) get(paste0("case_", x))+get(paste0("V_", x)))]
norway_60134[,c(paste0("case_", 0:999), paste0("V_", 0:999))] <- NULL

# norway 60132 - 4921 + 4922
norway_4921 <- result[result$location_id==4921, ]
norway_4922 <- result[result$location_id==4922, ]
for (i in 0:999) {
  colnames(norway_4922)[colnames(norway_4922)==paste0("case_", i)] <- paste0("V_", i)
}
norway_60132 <- as.data.table(merge(norway_4921, norway_4922, by= c("year_id", "age_group_id", "sex_id", "measure_id", "metric_id")))
norway_60132[,c("location_id.x", "location_id.y", "population.x", "population.y", "model_version_id.x", "model_version_id.y")] <- NULL
norway_60132[,location_id := 60132]
norway_60132[, paste0("agg_", 0:999) := lapply(0:999, function(x) get(paste0("case_", x))+get(paste0("V_", x)))]
norway_60132[,c(paste0("case_", 0:999), paste0("V_", 0:999))] <- NULL

# norway 60136 - 4911 + 4915 + 4914
norway_4911 <- result[result$location_id==4911, ]
norway_4915 <- result[result$location_id==4915, ]
norway_4914 <- result[result$location_id==4914, ]
for (i in 0:999) {
  colnames(norway_4911)[colnames(norway_4911)==paste0("case_", i)] <- paste0("V_", i)
}
for (i in 0:999) {
  colnames(norway_4915)[colnames(norway_4915)==paste0("case_", i)] <- paste0("H_", i)
}
norway_60136 <- as.data.table(merge(norway_4911, norway_4914, by= c("year_id", "age_group_id", "sex_id", "measure_id", "metric_id")))
norway_60136[,c("location_id.x", "location_id.y", "population.x", "population.y")] <- NULL
norway_60136 <- as.data.table(merge(norway_60136, norway_4915, by= c("year_id", "age_group_id", "sex_id", "measure_id", "metric_id")))
norway_60136[,c("population", "model_version_id.x", "model_version_id", "model_version_id.y")] <- NULL
norway_60136[,location_id := 60136]
norway_60136[, paste0("agg_", 0:999) := lapply(0:999, function(x) get(paste0("case_", x))+get(paste0("V_", x))+get(paste0("H_", x)))]
norway_60136[,c(paste0("V_", 0:999), paste0("H_", 0:999), paste0("case_", 0:999))] <- NULL


# convert to rate space and save
pop_2020 <- get_population(age_group_id=c(13:20, 30:32, 235), sex_id=c(1,2), 
                      year_id=c(1990,1995,2000,2005,2010,2015,2017,2019),
                      location_id=c(60132,60133,60134,60135,60136,60137),
                      gbd_round_id=7, decomp_step= "step2")

id <- ID
dt_tmp <- norway_60137

pop_tmp <- pop_2020[pop_2020$location_id == id, ]
dt_tmp <- as.data.table(merge(dt_tmp,pop_tmp, by=c("age_group_id", "sex_id", "location_id", "year_id"), all.x=T))
dt_tmp[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("agg_", x))/population)]
dt_tmp[,c(paste0("agg_", 0:999), "run_id", "population")] <- NULL
write_rds(dt_tmp, paste0("FILEPATH", cause, id, ".rds"))




