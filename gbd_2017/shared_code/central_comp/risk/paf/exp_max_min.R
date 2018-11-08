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
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_population.R")

set.seed(124535)

#-- SET UP ARGS ----------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)
rei_id <- as.numeric(args[1])
gbd_round_id <- as.numeric(args[2])
out_dir <- args[3]

demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)

# get risk info
rei_meta <- get_rei_meta(rei_id)
rei <- rei_meta$rei
exp_dist <- rei_meta$exp_dist
inv_exp <- rei_meta$inv_exp
mes <- get_rei_mes(rei_id)
age_min <- min(rei_meta$yll_age_group_id_start,rei_meta$yld_age_group_id_start)
age_max <- max(rei_meta$yll_age_group_id_end,rei_meta$yld_age_group_id_end)

#-- PULL EXP MEAN, EXP SD, AND POPULATION --------------------------------------


exp_dt <- get_model_results("epi",
                            gbd_id = ifelse(rei_id == 122, 2436, mes[draw_type  == "exposure"]$modelable_entity_id),
                            location_id = demo$location_id, year_id = demo$year_id,
                            age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                            gbd_round_id = gbd_round_id)
setnames(exp_dt, "mean", "exp_mean")
exp_sd_dt <- get_model_results("epi",
                               gbd_id = ifelse(rei_id == 95, 10488, mes[draw_type  == "exposure_sd"]$modelable_entity_id),
                               location_id = demo$location_id, year_id = demo$year_id,
                               age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                               gbd_round_id = gbd_round_id)
setnames(exp_sd_dt, "mean", "exp_sd")
pop <- get_population(location_id = demo$location_id, year_id = demo$year_id,
                      age_group_id = demo$age_group_id, sex_id = demo$sex_id,
                      gbd_round_id = gbd_round_id)
dt <- merge(exp_dt[, .(location_id, year_id, age_group_id, sex_id, exp_mean)],
            exp_sd_dt[, .(location_id, year_id, age_group_id, sex_id, exp_sd)],
            by = c("location_id", "year_id", "age_group_id", "sex_id"))
dt <- merge(dt,
            pop[, .(location_id, year_id, age_group_id, sex_id, population)],
            by = c("location_id", "year_id", "age_group_id", "sex_id"))
dt[population < 5, population := 5] # set to at least 5 so we can sample
dt <- dt[age_group_id >= age_min & age_group_id <= age_max, ] # only keep relevant ages

#-- SET UP HELPER FUNCTIONS FOR MAX/MIN CALC -----------------------------------

log_normal <- function(dt, dist, inv_exp) {
    pop <- dt$population
    if (dist == "normal") {
        sample <- rnorm(dt$population, dt$exp_mean, dt$exp_sd)
    } else {
        mu <- log(dt$exp_mean/sqrt(1 + (dt$exp_sd^2/(dt$exp_mean^2))))
        sdlog <- sqrt(log(1 + (dt$exp_sd^2/dt$exp_mean^2)))
        sample <- rlnorm(dt$population, mu, sdlog)
    }
    percentiles <- quantile(sample[sample > 0], c(0.01, 0.99), na.rm = T)
    if (inv_exp) {
        percentiles[[1]]
    } else {
        percentiles[[2]]
    }
}

#-- FIND MAX/MIN AND SAVE TO CSV -----------------------------------------------

cap <- log_normal(dt, exp_dist, inv_exp)
max_min <- data.table(rei_id = rei_id, cap = cap)
file.rename(paste0(out_dir, "/exposure/exp_max_min.csv"),
            paste0(out_dir, "/exposure/exp_max_min_", format(Sys.time(), "%d%m%y_%H%M"), ".csv"))
write.csv(max_min, paste0(out_dir, "/exposure/exp_max_min.csv"), row.names = F)
