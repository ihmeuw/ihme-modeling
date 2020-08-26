# load libraries and functions
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)

source("./utils/data.R")
source("./utils/db.R")
source("math.R")
source("save.R")
source("FILEPATH/get_draws.R")

set.seed(124535)

#-- SET UP ARGS ----------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
task_id <- Sys.getenv("SGE_TASK_ID") %>% as.numeric
params <- fread(args[1])[task_id, ]

location_id <- unique(params$location_id)
sex_id <- unique(params$sex_id)
rei_id <- as.numeric(args[2])
year_id <- eval(parse(text = args[3]))
n_draws <- as.numeric(args[4])
gbd_round_id <- as.numeric(args[5])
decomp_step <- args[6]
out_dir <- args[7]

# get risk info
rei_meta <- get_rei_meta(rei_id)
rei <- rei_meta$rei

#--PULL EXPOSURE ---------------------------------------------------------------
exp <- get_exp(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
# for global exposure, pull 2019
global_exp <- get_exp(rei_id, 1, 2019, sex_id, gbd_round_id, decomp_step, n_draws)
setnames(global_exp, "exp_mean", "global_exp_mean")
global_exp$location_id <- location_id
global_exp[, year_id := NULL]
exp <- merge(exp, global_exp, by = c("location_id", "age_group_id", "sex_id", "parameter", "draw"))
exp <- exp[parameter != "cat125"]

#--PULL RR AND MERGE------------------------------------------------------------
rr <- get_rr(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
dt <- merge(exp, rr, by = c("location_id", "year_id", "age_group_id", "sex_id", "parameter", "draw"))

#--PULL TMREL AND MERGE---------------------------------------------------------
tmrel <- fread("FILEPATH/nutrition_lbw_preterm_tmrel.csv")
tmrel <- tmrel[, .(age_group_id, sex_id, parameter, preterm, lbw)]
dt <- merge(dt, tmrel, by = c("age_group_id", "sex_id", "parameter"))

# rescale exposures to sum to 1
residual_categ <- unique(dt[preterm == 40 & lbw == 4000]$parameter)
dt <- dt[parameter != residual_categ]
dt[, `:=` (total_exp=sum(exp_mean), total_global_exp=sum(global_exp_mean)),
   by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
          "mortality", "morbidity", "draw")]
dt[total_exp > 1, exp_mean := exp_mean/total_exp][, total_exp := NULL]
dt[total_global_exp > 1, global_exp_mean := global_exp_mean/total_global_exp][, total_global_exp := NULL]
dt <- rbind(dt[, .(location_id, year_id, age_group_id, sex_id, cause_id, mortality,
                   morbidity, parameter, draw, exp_mean, global_exp_mean, rr)],
            dt[, .(exp_mean = 1 - sum(exp_mean), global_exp_mean = 1 - sum(global_exp_mean),
                   rr = 1, parameter = residual_categ),
               by = c("location_id", "year_id", "age_group_id", "sex_id",
                      "cause_id", "mortality", "morbidity", "draw")])
dt <- merge(dt, tmrel, by = c("age_group_id", "sex_id", "parameter"))

#--CALC LBW AND PRETERM INDIVIDUAL PAFS ----------------------------------------
calc_child_paf <- function(child, child_name) {
    setkeyv(child, c("location_id", "year_id", "age_group_id", "sex_id",
                     "cause_id", "mortality", "morbidity", child_name,
                     "draw"))

    # collapse exposures (sum) and rrs (weighted product by global exposure)
    # across opposite dimension
    child <- child[, .(exp_mean=sum(exp_mean),
                       rr=sum(rr*global_exp_mean/sum(global_exp_mean))),
                   by=c("location_id", "year_id", "age_group_id", "sex_id",
                        "cause_id", "mortality", "morbidity", child_name,
                        "draw")]

    # TMREL is set to two bins.
    # rescale all RR dividing by highest RR in TMREL
    child[, tmrel := 0]
    if (child_name == "preterm") {
        child[preterm == 38, tmrel := 1]
        child[preterm %in% c(38, 40), min_rr := max(rr),
              by=c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
                   "mortality", "morbidity", "draw")]
    } else if (child_name == "lbw") {
        child[lbw == 3500, tmrel := 1]
        child[lbw %in% c(3500, 4000), min_rr := max(rr),
              by=c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
                   "mortality", "morbidity", "draw")]
    }
    child[, min_rr := mean(min_rr, na.rm = T),
          by=c("location_id", "year_id", "age_group_id", "sex_id", "cause_id",
               "mortality", "morbidity", "draw")]
    child[, rr := rr / min_rr][, min_rr := NULL]
    child[rr < 1, rr := 1]

    # calc paf
    child[, parameter := paste0("cat", get(child_name))][, c(child_name) := NULL]
    child <- categ_paf(child)
    return(child)
}

preterm <- calc_child_paf(dt, "preterm")
lbw <- calc_child_paf(dt, "lbw")

#--CALC JOINT PAF --------------------------------------------------------------
# add tmrel
dt[, tmrel := ifelse(parameter == residual_categ, 1, 0)]
dt[, c("preterm", "lbw", "global_exp_mean") := NULL]

# calc paf
dt <- categ_paf(dt)

#--SAVE + VALIDATE -------------------------------------------------------------
# flip rei_ids, bc pafs calculated backwards
save_paf(preterm, 334, "nutrition_preterm", n_draws, gsub("lbw_preterm","preterm", out_dir))
save_paf(lbw, 335, "nutrition_lbw", n_draws, gsub("lbw_preterm", "lbw", out_dir))

save_paf(dt, rei_id, rei, n_draws, out_dir)

