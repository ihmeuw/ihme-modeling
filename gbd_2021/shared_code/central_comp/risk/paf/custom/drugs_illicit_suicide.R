# load libraries and functions
library(data.table)
library(magrittr)
library(gtools)
library(ini)
library(RMySQL)

source("math.R")
source("mediate_rr.R")
source("save.R")
source("./utils/data.R")
source("./utils/db.R")
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
out_dir_unmed <- args[8]

mediation <- get_mediation(rei_id)
if (nrow(mediation) > 0)
    stop(paste("Mediation factors found but mediation is not implemented for rei ID", rei_id))

# get risk info
rei_meta <- get_rei_meta(rei_id, gbd_round_id)
rei <- rei_meta$rei
rr_metadata <- fread(paste0(out_dir, "/rr_metadata.csv"))
rr_by_year_id <- unique(rr_metadata$by_year_id)
rr_year_id <- if (rr_by_year_id) year_id else unique(rr_metadata$year_id)

#--CALC PAF FOR EACH DRUG ------------------------------------------------------
calc_drug_paf <- function(me_id) {

    # pull prevalence and make a dichotomous exposure
    exp <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = me_id,
                     measure_id = 5, location_id = location_id, year_id = year_id,
                     sex_id = sex_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                     source = "epi", n_draws = n_draws, downsample = TRUE)
    exp <- melt(exp, id.vars = c("age_group_id", "sex_id", "location_id", "year_id"),
                measure.vars = paste0("draw_", 0:(n_draws - 1)),
                variable.name = "draw", value.name = "cat1")
    exp[, draw := as.numeric(gsub("draw_", "", draw))]
    exp[, "cat2" := 1 - cat1]
    exp <- melt(exp,
                id.vars = c("age_group_id", "sex_id", "location_id", "year_id", "draw"),
                measure.vars = c("cat1", "cat2"),
                variable.name = "parameter", value.name = "exp_mean")

    # pull rr and merge
    rr <- get_rr(rei_id = rei_id, location_id = location_id, year_id = rr_year_id,
                 sex_id = sex_id, gbd_round_id = gbd_round_id,
                 decomp_step = decomp_step, n_draws = n_draws,
                 version_id = drug_mes[modelable_entity_id == me_id, ]$rr_model_version,
                 by_year_id=rr_by_year_id)
    rr_merge_cols <- c("age_group_id", "sex_id", "parameter", "draw")
    if (rr_by_year_id) rr_merge_cols <- c(rr_merge_cols, "year_id")
    dt <- merge(exp, rr, by = rr_merge_cols, allow.cartesian = TRUE)

    # add tmrel, 0/1 as categorical
    dt[, tmrel := ifelse(parameter == "cat2", 1, 0)]

    # calc paf
    dt <- categ_paf(dt)
    return(dt)

}

# calculate pafs for opioid, cocaine, and amphetamine
# note - cocaine and amphetamine have the same RRs
drug_mes <- fread(paste0(out_dir, "/mes.csv"))[draw_type == "exposure", ]
rr_mes <- fread(paste0(out_dir, "/mes.csv"))[draw_type == "rr", ]
drug_mes[modelable_entity_name %like% "Cocaine|Amphetamine",
         rr_model_version := rr_mes[modelable_entity_name %like% "Cocaine", ]$model_version_id]
drug_mes[modelable_entity_name %like% "Opioid",
         rr_model_version := rr_mes[modelable_entity_name %like% "Opioid", ]$model_version_id]
drug_mes <- drug_mes[, .(modelable_entity_id, rr_model_version)]
dt <- lapply(drug_mes$modelable_entity_id, calc_drug_paf) %>% rbindlist(use.names=TRUE)

#--CALCULATE JOINT PAF ---------------------------------------------------------
# multiplicative agg (1-(1- opiod PAF)(1- cocaine PAF)(1- amphetamine PAF))
dt <- dt[, .(paf = 1-prod(1-paf)), by=c("location_id", "year_id", "age_group_id", "sex_id",
                                        "cause_id", "mortality", "morbidity", "draw")]

#--SAVE + VALIDATE -------------------------------------------------------------
save_paf(dt, rei_id, rei, n_draws, out_dir)
