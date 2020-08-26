# load libraries and functions
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

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

#--PULL EXPOSURE ---------------------------------------------------------------

# pull exposure (all "Proportion HIV..." MEs), scale to 1
exp <- get_draws(gbd_id_type = "rei_id", gbd_id = rei_id, location_id = location_id,
                 year_id = year_id, sex_id = sex_id, gbd_round_id = gbd_round_id,
                 decomp_step = decomp_step, source = "exposure",
                 n_draws = n_draws, downsample = TRUE)
exp <- melt(exp, id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                             "modelable_entity_id"),
            measure.vars = paste0("draw_", 0:(n_draws - 1)),
            variable.name = "draw", value.name = "exp_mean")
exp[, draw := as.numeric(gsub("draw_", "", draw))]
exp[, exp_total := sum(exp_mean), by = c("location_id", "year_id", "age_group_id",
                                         "sex_id", "draw")]
exp[, exp_mean := exp_mean/exp_total][, exp_total := NULL]

#--CALC PAF ---------------------------------------------------------------------

# find most detailed hiv outcomes
cause_dt <- get_cause_metadata(cause_set_id=2, gbd_round_id=gbd_round_id)
cause_dt <- cause_dt[acause %like% "hiv_" & level == 4, ]
cause_ids <- unique(cause_dt$cause_id)

# keep the proportion HIV due to sex and assign outcomes
dt <- exp[modelable_entity_id == 24812, .(age_group_id, sex_id, location_id,
                                         year_id, draw, exp_mean)]
setnames(dt, "exp_mean", "paf")
dt <- dt[, .(cause_id = cause_ids), by=c("age_group_id", "sex_id", "location_id",
                                         "year_id", "draw", "paf")]
# duplicate for morbidity and mortality
dt <- rbind(copy(dt)[, mortality := 1][, morbidity := 0],
            copy(dt)[, mortality := 0][, morbidity := 1])

# age-restrict
dt <- dt[age_group_id >= 7, ]

#--SAVE + VALIDATE -------------------------------------------------------------

save_paf(dt, rei_id, rei, n_draws, "FILEPATH")