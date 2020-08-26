# load libraries and functions
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)

source("./utils/data.R")
source("./utils/db.R")
source("math.R")
source("mediate_rr.R")
source("save.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_cause_metadata.R")

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
mediate <- as.logical(args[8])

# get risk info
rei_meta <- get_rei_meta(rei_id)
rei <- rei_meta$rei

#--CALC PAF --------------------------------------------------------------------

# find most detailed homicide outcomes
cause_dt <- get_cause_metadata(cause_set_id=2, gbd_round_id=gbd_round_id)
cause_dt <- cause_dt[acause %like% "inj_homicide_" & level == 4, ]
cause_ids <- unique(cause_dt$cause_id)

# pull 'exp' as paf, assign outcomes
exp <- get_exp(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step,
               n_draws)
dt <- exp[, .(age_group_id, sex_id, location_id, year_id, draw, exp_mean)]
setnames(dt, "exp_mean", "paf")
dt <- dt[, .(cause_id = cause_ids), by=c("age_group_id", "sex_id", "location_id",
                                         "year_id", "draw", "paf")]

# duplicate for morbidity and mortality
dt <- rbind(copy(dt)[, mortality := 1][, morbidity := 0],
            copy(dt)[, mortality := 0][, morbidity := 1])

# age-restrict
dt <- dt[age_group_id >= 8, ]

#--SAVE + VALIDATE -------------------------------------------------------------

save_paf(dt, rei_id, rei, n_draws, out_dir)
