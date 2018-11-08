# load libraries and functions
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)
library(zoo)

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
out_dir <- args[6]

# get risk info
rei_meta <- get_rei_meta(rei_id)
rei <- rei_meta$rei

#--PULL EXPOSURE ---------------------------------------------------------------

exp <- get_exp(rei_id, location_id, year_id, sex_id, gbd_round_id, n_draws)
exp <- exp[parameter != "cat124"]  # drop resid category get draws adds

#--PULL RR AND MERGE------------------------------------------------------------

rr <- get_rr(rei_id, location_id, year_id, sex_id, gbd_round_id, n_draws)
dt <- merge(exp, rr, by = c("location_id", "year_id", "age_group_id", "sex_id", "parameter", "draw"))

#--PULL TMREL AND MERGE---------------------------------------------------------

tmrel <- fread("FILEPATH/nutrition_lbw_preterm_tmrel.csv")
tmrel[, c("modelable_entity_name", "modelable_entity_id", "ga", "bw") := NULL]
dt <- merge(dt, tmrel, by = c("age_group_id", "sex_id", "parameter"))

#--CALC LBW AND PRETERM INDIVIDUAL PAFS ----------------------------------------

calc_child_paf <- function(child, child_name) {

    child <- child[order(location_id, year_id, age_group_id, sex_id, cause_id,
                         mortality, morbidity, draw, get(child_name),
                         get(paste0("tmrel_", child_name)))]

    # scale RR such that RR for TMREL is 1 for each row/column
    child[get(paste0("tmrel_", child_name)) == 1, tmrel_rr := rr]
    child[, tmrel_rr := na.locf(tmrel_rr), by = child_name]
    child[, rr := rr/tmrel_rr, by = c("location_id", "year_id", "age_group_id",
                                      "sex_id", "cause_id", "mortality", "morbidity",
                                      "draw")]

    # calc paf
    child[, paf := (exp_mean * (rr - 1))/((exp_mean * (rr - 1) + 1))]

    # multiplicative sum (1-(1-PAF)(1-PAF)(1-PAF))... across rows/columns
    child <- child[, .(paf = 1-prod(1-paf)), by = c("location_id", "year_id", "age_group_id",
                                                    "sex_id", "cause_id", "mortality", "morbidity",
                                                    "draw")]
    child <- child[, c("location_id", "year_id", "age_group_id", "sex_id",
                       "cause_id", "mortality", "morbidity", "draw", "paf"),
                   with = F] %>% setkey %>% unique
    setnames(child, "paf", child_name)
    return(child)
}

preterm <- calc_child_paf(dt, "preterm")
lbw <- calc_child_paf(dt, "lbw")

#--CALC JOINT PAF --------------------------------------------------------------

dt <- dt[is.na(tmrel_lbw_preterm)]
dt[, total_exp := sum(exp_mean), by = c("location_id", "year_id", "age_group_id",
                                        "sex_id", "cause_id", "mortality", "morbidity",
                                        "draw")]
dt[total_exp > 1, exp_mean := exp_mean/total_exp][, total_exp := NULL]
dt <- rbind(dt[, .(location_id, year_id, age_group_id, sex_id, cause_id, mortality,
                   morbidity, parameter, draw, exp_mean, rr)],
            dt[, .(exp_mean = 1 - sum(exp_mean), rr = 1, parameter = "cat56"),
               by = c("location_id", "year_id", "age_group_id", "sex_id",
                      "cause_id", "mortality", "morbidity", "draw")])

# add tmrel
dt[, tmrel := ifelse(parameter == "cat56", 1, 0)]

# calc paf
dt <- categ_paf(dt)

#--SCALE CHILD PAFS TO SUM TO JOINT PAF ----------------------------------------

dt <- merge(dt, preterm, by = c("location_id", "year_id", "age_group_id", "sex_id",
                                "cause_id", "mortality", "morbidity", "draw"))
dt <- merge(dt, lbw, by = c("location_id", "year_id", "age_group_id", "sex_id",
                            "cause_id", "mortality", "morbidity", "draw"))
dt[, scalar := paf/(lbw + preterm)]
dt[, lbw := lbw * scalar]
dt[, preterm := preterm * scalar][, scalar := NULL]

#--SAVE + VALIDATE -------------------------------------------------------------

save_paf(copy(dt), rei_id, rei, n_draws, out_dir)
save_paf(copy(dt)[, paf := preterm], # preterm
         335, "nutrition_preterm", n_draws, gsub("lbw_preterm","preterm",out_dir))
save_paf(copy(dt)[, paf := lbw], # lbw
         334, "nutrition_lbw", n_draws, gsub("lbw_preterm","lbw",out_dir))
