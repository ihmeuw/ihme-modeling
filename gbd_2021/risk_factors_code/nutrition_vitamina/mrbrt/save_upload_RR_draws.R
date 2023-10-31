################################################################################
## DESCRIPTION ##  Take draws from best mr-brt output (GBD 2020) then clean, save, and upload
## INPUTS ##
## OUTPUTS ##
## AUTHOR ##   
## DATE ##    
################################################################################

rm(list = ls())

library(ggplot2)
library(data.table)
library(openxlsx)

# source
source("FILEPATH/get_draws.R")
source("FILEPATH/save_results_risk.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/mr_brt_functions.R")
source("FILEPATH/cov_info_function.R")
# 
gbd_round_id = 7
decomp_step = "iterative"
rr_version <- "2021_05_13.01"   # best version of RR
meid_for_vita_rr <- 9025 
rei_for_vita <- 96
rr_dir <- file.path("FILEPATH/results", rr_version)
save_dir <- "FILEPATH/upload_draws"
description <- "GBD 2020 MR-BRT RRs, final version uploaded, annual"

#---------------------------------------------------------------------------------------------------------------------------

#Make sure we upload correct years and age_groups
old_rr <- get_draws("rei_id", rei_for_vita, source="rr", status="best", gbd_round_id=6, decomp_step="step4")
years <- seq(1990,2022)
ages <- c(388,389,238,34)
cause_table <- data.table(cause_id = c(302, 322, 341), outcome = c("diarrhea", "lri", "measles"))

# read in diarrhea and measles - LRTI still insignificant
diarrhea <- fread(paste0(rr_dir, "/Diarrhea_draws_gamma_fib.csv"))
diarrhea[, outcome := "diarrhea"]
measles <- fread(paste0(rr_dir, "/Measles_draws_gamma_fib.csv"))
measles[, outcome := "measles"]

all_draws <- rbind(diarrhea, measles)

# Add necessary columns
all_draws <- merge(all_draws, cause_table, by = "outcome")
all_draws$morbidity <- 1
all_draws$mortality <- 1
all_draws$parameter <- "cat1"
all_draws$location_id <- 1
all_draws$rei_id<- rei_for_vita
all_draws$metric_id <- 3
all_draws$modelable_entity_id <- meid_for_vita_rr
all_draws$merge_id   <- 1

## Duplicate for sexes
all_draws <- merge(all_draws, data.table("sex_id" = c(1,2), "merge_id" = 1), by = "merge_id")

## Duplicate for age_groups
all_draws <- merge(all_draws, data.table("age_group_id" = ages, "merge_id" = 1), by = "merge_id", allow.cartesian = T)

## Duplicate for years
all_draws <- merge(all_draws, data.table("year_id" = years, "merge_id" = 1), by = "merge_id", allow.cartesian = T)

all_draws[, merge_id := NULL]

## Duplicate out for cateogorical risk
vars <- paste0("draw_", seq(0, 999))
all_draws_new <- copy(all_draws)
all_draws_new[, parameter:="cat2"]
all_draws_new[, paste(vars):=1]
all_draws <- rbind(all_draws, all_draws_new)

file_name <- paste0("draws_to_upload_", rr_version,".csv")
write.csv(all_draws, paste0(save_dir,"/",file_name), row.names = FALSE)

result <- save_results_risk(save_dir, 
                            file_name,
                            modelable_entity_id = meid_for_vita_rr, 
                            description = description,
                            risk_type = "rr", 
                            year_id = seq(1990, 2022),
                            decomp_step = decomp_step, 
                            gbd_round_id = gbd_round_id, 
                            mark_best = T)
print(result)
