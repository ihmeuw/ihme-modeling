# load libraries and functions
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)
library(fitdistrplus)
library(actuar)
library(Rcpp)
library(compiler)
library(parallel)

source("./utils/db.R")
source("FILEPATH/get_age_spans.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_population.R")

set.seed(124535)

#-- SET UP ARGS ----------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
rei_id <- as.numeric(args[1])
gbd_round_id <- as.numeric(args[2])
decomp_step <- args[3]
out_dir <- args[4]
age_specific <- as.logical(args[5])

demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)

# get risk info
rei_meta <- get_rei_meta(rei_id, gbd_round_id)
rei <- rei_meta$rei
exp_dist <- rei_meta$exposure_type
inv_exp <- rei_meta$inv_exp
mes <- get_rei_mes(rei_id, gbd_round_id, decomp_step)

age_spans <- get_age_spans(gbd_round_id = gbd_round_id)
age_min <- min(age_spans[
    age_group_id %in% c(rei_meta$yll_age_group_id_start, rei_meta$yld_age_group_id_start)
]$age_group_years_start)
age_max <- max(age_spans[
    age_group_id %in% c(rei_meta$yll_age_group_id_end, rei_meta$yld_age_group_id_end)
]$age_group_years_start)
age_range <- age_spans[age_group_years_start >= age_min & age_group_years_start <= age_max, ]$age_group_id
rm(age_min, age_max)

#-- PULL EXP MEAN, EXP SD, AND POPULATION --------------------------------------
exp_dt <- get_model_results("epi",
                            gbd_id = mes[draw_type  == "exposure"]$modelable_entity_id,
                            location_id = demo$location_id, year_id = demo$year_id,
                            age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                            gbd_round_id = gbd_round_id, decomp_step = decomp_step)
setnames(exp_dt, "mean", "exp_mean")
exp_sd_dt <- get_model_results("epi",
                               gbd_id = mes[draw_type  == "exposure_sd"]$modelable_entity_id,
                               location_id = demo$location_id, year_id = demo$year_id,
                               age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                               gbd_round_id = gbd_round_id, decomp_step = decomp_step)
setnames(exp_sd_dt, "mean", "exp_sd")
pop <- get_population(location_id = demo$location_id, year_id = demo$year_id,
                      age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                      gbd_round_id = gbd_round_id, decomp_step = decomp_step)
dt <- merge(exp_dt[, .(location_id, year_id, age_group_id, sex_id, exp_mean)],
            exp_sd_dt[, .(location_id, year_id, age_group_id, sex_id, exp_sd)],
            by = c("location_id", "year_id", "age_group_id", "sex_id"))
dt <- merge(dt,
            pop[, .(location_id, year_id, age_group_id, sex_id, population)],
            by = c("location_id", "year_id", "age_group_id", "sex_id"))
dt <- dt[age_group_id %in% age_range, ] # only keep relevant ages
if (is.na(rei_meta$female)) {
    dt <- dt[sex_id == 1, ] # only keep males
} else if (is.na(rei_meta$male)) {
    dt <- dt[sex_id == 2, ] # only keep females
}
rm(exp_dt, exp_sd_dt, pop)

#-- SET UP HELPER FUNCTIONS FOR MAX/MIN CALC -----------------------------------
log_normal <- function(dt, dist) {
    pop <- dt$population
    if (dist == "normal") {
        sample <- rnorm(dt$population, dt$exp_mean, dt$exp_sd)
    } else {
        mu <- log(dt$exp_mean/sqrt(1 + (dt$exp_sd^2/(dt$exp_mean^2))))
        sdlog <- sqrt(log(1 + (dt$exp_sd^2/dt$exp_mean^2)))
        sample <- rlnorm(dt$population, mu, sdlog)
    }
    percentiles <- quantile(sample[sample > 0], c(0.01, 0.99), na.rm = T)
    return(percentiles)
}

#-- FIND MAX/MIN AND SAVE TO CSV -----------------------------------------------
max_min <- data.table(rei_id = rei_id, age_group_id = unique(dt$age_group_id))
if (age_specific) {
    # risk uses percentiles specific to each age group
    for (age in unique(dt$age_group_id)) {
        percentiles <- log_normal(dt[age_group_id==age], exp_dist)
        max_min[age_group_id==age, `:=` (lower = percentiles[[1]], upper = percentiles[[2]])]
    }
} else {
    # risk uses the same all-ages percentiles for each age group
    percentiles <- log_normal(dt, exp_dist)
    max_min[, `:=` (lower = percentiles[[1]], upper = percentiles[[2]])]
}

file.rename(paste0(out_dir, "/exposure/exp_max_min.csv"),
            paste0(out_dir, "/exposure/exp_max_min_", format(Sys.time(), "%d%m%y_%H%M"), ".csv"))
write.csv(max_min, paste0(out_dir, "/exposure/exp_max_min.csv"), row.names = F)
