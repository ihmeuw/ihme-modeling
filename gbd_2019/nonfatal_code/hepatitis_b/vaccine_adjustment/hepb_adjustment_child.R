############################################################
# POST HOC ADJUSTMENT OF SEROPREVALENCE OF HEPATITIS B HBSAG
############################################################

rm(list=ls())


pacman::p_load(data.table, openxlsx, ggplot2)

# SOURCE SHARED FUNCTIONS -------------------------------------------------

source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/r/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_crosswalk_version.R")


# SET OBJECTS -------------------------------------------------------------
# Hepatitis B 3-dose coverage (proportion)
# Fraction of 12-23 month-old children born in a given geography-year vaccinated with at least 3 doses of the hepatitis B vaccine

# GET ARGS ----------------------------------------------------------------
args<-commandArgs(trailingOnly = TRUE)
save_dir <- args[1]
map_path <-args[2]
xw_version <- args[3]
locations <- fread(map_path)
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
loc_id <- locations[task_num == task_id, location]

age_cov_id <- 1981
gbd_round_id <- 6
decomp_step <- "step4"
years <- c(seq(1990, 2015, by = 5), 2017, 2019)
ages <- c(2:20, 30:32, 164, 235)
measure <- 5 # or 6 

draws <- paste0("draw_", c(0:999))
p_draws <- paste0("p_draw_", c(0:999))
f_draws <- paste0("f_draw_", c(0:999))

age_dt <- get_age_metadata(age_group_set_id = 12, gbd_round_id = gbd_round_id)
age_dt <- age_dt[age_group_id %in% ages, ]
age_dt[, midage := (age_group_years_start + age_group_years_end) / 2]
age_dt <- age_dt[, .(age_group_id, midage)]

loc_dt <- get_location_metadata(location_set_id = 9, gbd_round_id = gbd_round_id)
loc_dt <- loc_dt[, .(location_id, location_name)]
loc_name <- loc_dt[location_id == loc_id, location_name]

sex_dt <- get_ids("sex")

# GET COVARIATE ESTIMATES 

dt_vac <- get_covariate_estimates(
              covariate_id=age_cov_id,
              gbd_round_id=gbd_round_id,
              decomp_step=decomp_step, 
              location_id= loc_id
              )

message("Finished getting covariate estimates")


dt_vac <- dt_vac[, -c("model_version_id", "covariate_name_short", "sex", "sex_id", "lower_value", "upper_value")]
drop_ages <- c(2, 3, 4)

dt_vac_fixed <- dt_vac[!(age_group_id %in% drop_ages), ]

vac_fill <- copy(dt_vac[age_group_id == 5, ])


vac_2 <- copy(vac_fill)
vac_2[, `:=` (age_group_id = 2, age_group_name = "Early Neonatal")]

vac_3 <- copy(vac_fill)
vac_3[, `:=` (age_group_id = 3, age_group_name = "Late Neonatal")]

vac_4 <- copy(vac_fill)
vac_4[, `:=` (age_group_id = 4, age_group_name = "Post Neonatal")]

vac_164 <- copy(vac_fill)
vac_164[, `:=` (age_group_id = 164, age_group_name = "Birth")]

dt_vac <- rbind(dt_vac_fixed, vac_2, vac_3, vac_4, vac_164)

message("Dropped unneeded columns for vaccinate estimates")

# PULL MODEL ESTIMATES
dt_sero <- get_draws(gbd_id_type = "modelable_entity_id", 
                      gbd_id = 18673, 
                      source="epi", 
                      location_id=loc_id, 
                      year_id=years, 
                      measure_id = measure, 
                      status="best", 
                      gbd_round_id=gbd_round_id, 
                      decomp_step=decomp_step)

message("Finished getting DisMod draws")

# MERGE SEROPREVALENCE AND VACCINATION ESTIMATES TOGETHER 
# MULTIPLY MEAN COVARIATE ESTIMATE BY DRAWS 
# SUBTRACT SEROPREVALENCE DRAW i BY SEROPREV DRAW i * PROPORTION 
# Using 95% vaccine efficacy based on CDC article https://www.cdc.gov/vaccines/pubs/pinkbook/hepb.html and expert opinion 

dt_all <- merge(dt_sero, dt_vac, by = c("year_id", "age_group_id", "location_id"))

message("Merged draws")

# Calculate mean, upper and lower for DisMod seroprevalence draws
raw_estimates <- copy(dt_all)
raw_estimates[, dis_mean := rowMeans(.SD), .SDcols = draws]
raw_estimates[, dis_lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
raw_estimates[, dis_upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
raw_estimates[, (draws) := NULL]
raw_estimates_final <- copy(raw_estimates[, c("location_id", "location_name", "age_group_id", "sex_id", "year_id", "dis_mean", "dis_lower", "dis_upper")])

# Calculate p draws by multiplying vaccine proportion coverage * vaccine efficacy * draw value
dt_all[, (p_draws) := lapply(.SD, function(x) mean_value * 0.95* x), .SDcols = draws,
                            by = c("location_id", "year_id", "age_group_id")]

message("Calculated p_draws")

# Subset to only the seroprev draws
keep_cols <- names(dt_all)[!grepl("draw", names(dt_all))]
keep_cols1 <- append(keep_cols, draws)
sero_draws <- copy(dt_all[, (keep_cols1), with = FALSE])
sero_draws$version <- "seroprev"

# Subset to only the proportion draws
keep_cols2 <- append(keep_cols, p_draws)
prop_draws <- copy(dt_all[, (keep_cols2), with = FALSE])
setnames(prop_draws, p_draws, draws)
prop_draws$version <- "prop"

# Estimate the mean, upper, and lower of the proportion draws
p_estimates <- copy(prop_draws)
p_estimates[, p_mean := rowMeans(.SD), .SDcols = draws]
p_estimates[, p_lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
p_estimates[, p_upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
p_estimates[, (draws) := NULL]
p_estimates_final <- copy(p_estimates[, c("location_id", "location_name", "age_group_id", "sex_id", "year_id", "p_mean", "p_lower", "p_upper")])

# NEED TO RESHAPE TO BE ABLE TO SUBTRACT ESTIMATES -------------------------------------------

# Bind the full draw sets for seroprevalence and proportion draws together
all_draws <- rbind(sero_draws, prop_draws)
all_draws <- all_draws[, -c("metric_id", "model_version_id", "modelable_entity_id", "covariate_id", "mean_value")]

# Then melt
all_draws_melt <- melt.data.table(all_draws, measure.vars = draws)

# Then dcast
all_draws_wide <- dcast.data.table(all_draws_melt, location_id + age_group_id + sex_id + year_id + variable ~ version, value.var = "value")

# Then subtract!
all_draws_wide[, final := seroprev - prop]

# Dcast again to (1) so draws are wide for upload (2) so draws are in an easy format to compute upper and lower
all_draws_wide2 <- copy(all_draws_wide[, -c("prop", "seroprev")])
all_draws_wide2$version <- "final"
all_draws_wide2 <- dcast.data.table(all_draws_wide2, location_id + age_group_id + sex_id + year_id ~ variable, value.var = "final")


# Get upper and lower of final draws
f_estimates <- copy(all_draws_wide2)
f_estimates[, f_mean := rowMeans(.SD), .SDcols = draws]
f_estimates[, f_lower := apply(.SD, 1, quantile, probs= 0.025), .SDcols = draws]
f_estimates[, f_upper := apply(.SD, 1, quantile, probs=0.975), .SDcols = draws]
f_estimates[, (draws) := NULL]
f_estimates <- copy(f_estimates[, c("location_id", "age_group_id", "sex_id", "year_id", "f_mean", "f_lower", "f_upper")])
f_estimates_final <- merge(f_estimates, loc_dt, by = "location_id")

# CREATE DRAW FILE FOR SAVE_RESULTS_EPI

dt_draws_final <- copy(all_draws_wide2)
dt_draws_final$measure_id <- measure

write.csv(dt_draws_final, FILEPATH, row.names = FALSE)