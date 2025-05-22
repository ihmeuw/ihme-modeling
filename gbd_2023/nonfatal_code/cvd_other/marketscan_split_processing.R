##
## Author: USERNAME and USERNAME
##
## Purpose: Calculate draws for other CVD (excluding HF deaths) prevalence for each specified location using the ratio of odds(cvd_other) / odds(cvd_other_hf) (generated in marketscan_cvd_other.R)
##          Adapted from code written by USERNAME and USERNAME, rewritten from Stata to R and to use MarketScan data instead of MEPS
##

# pull in parameters
args <- commandArgs(trailingOnly = TRUE)
task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))

loc_path <- args[1]
print(loc_path)
print(paste0("Loc path is ", loc_path))

library(data.table)

loc_path <- fread(loc_path)
location <- loc_path[task_id, location_id]

tmp_dir <- "/FILEPATH/cvd_other/" 
gbd_round_id <- 8
decomp_step <- "iterative"

# pull in MarketScan ratios
marketscan_long <- data.table(read.csv(paste0(tmp_dir, "/ratio_merge.csv")))

# get draws for overall heart failure due to other
source("FILEPATH/get_draws.R")
source("FILEPATH/get_age_metadata.R")

#ages <- get_age_metadata(age_group_set_id = 19)
ages <- get_age_metadata(release_id=16)

draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 9575, source="epi",
                   version_id = 876711, release_id = 16,
                   measure_id = 5, location_id = location, age_group_id = ages$age_group_id, sex_id = c(1, 2))

# reshape the data
draws_long = melt(draws, measure.vars = names(draws)[grepl("draw", names(draws))], variable.name = "draw", value.name = "draw_val")
draws_long <- data.table(draws_long)

# merge with marketscan
# merge data with age groups above 1 on age
master_other <- merge(draws_long[!(age_group_id%in%c(2, 3, 388, 389)), c("age_group_id", "location_id", "sex_id", "year_id", "draw", "draw_val")],
                      marketscan_long[age_group_id!=28,],
                      by=c("age_group_id", "sex_id", "draw"))

# for data with age groups below 1, merge ages 0-1 MarketScan data to all < 1 GBD age groups 
master_other_under_1 <- merge(draws_long[age_group_id%in%c(2, 3, 388, 389), c("age_group_id", "location_id", "sex_id", "year_id", "draw", "draw_val")],
                              marketscan_long[age_group_id==28, c("sex_id", "draw", "coef")],
                              by=c("sex_id", "draw"))
master_other <- rbind(master_other, master_other_under_1)

# Here we multiply the odds ratios of all countries for cvd_other due to HF, by the MarketScan-generated ratio of odds(cvd_other) / odds(cvd_other_hf)
master_other[ , draw_val_mod := (draw_val / (1 - draw_val)) * coef]
setnames(master_other, old="draw_val", new="draw_val_old")

# convert from the odds ratio(cvd_other) to prevalence(cvd_other)
master_other[ , draw_val := draw_val_mod / (1 + draw_val_mod)]
master_other[ , draw_val := ifelse(draw_val<0 | is.na(draw_val), 0, draw_val)]

# Save Other CVD (excluding HF deaths)
other_cvd <- copy(master_other)

# Take all-other prevalence minus other-due-to-HF prevalence
other_cvd[ , draw_val := draw_val - draw_val_old]
other_cvd[ , draw_val := ifelse(draw_val<0 | is.na(draw_val), 0, draw_val)]

other_cvd$draw_val_mod <- NULL
other_cvd$draw_val_old <- NULL
other_cvd$coef <- NULL

other_cvd <- dcast(other_cvd, location_id + age_group_id + sex_id + year_id ~ draw, value.var = "draw_val")

write.csv(other_cvd, file =
            paste0(tmp_dir, "/mkt_draws/me_2908/5_", location, ".csv"), row.names = FALSE)
