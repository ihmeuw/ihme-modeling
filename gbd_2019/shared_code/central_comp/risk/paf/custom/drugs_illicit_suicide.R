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

    # calculate rr and merge
    rr <- expand.grid(location_id = location_id,
                      year_id = year_id,
                      age_group_id = unique(exp$age_group_id),
                      sex_id = sex_id,
                      cause_id = c(721, 723), # self-harm child causes
                      parameter = c("cat1", "cat2"),
                      morbidity = c(0, 1),
                      mortality = c(0, 1)) %>% data.table
    # cocaine and amphetamine have the same values, opioid is the exception
    # Note these values are in log space
    rr_mean <- ifelse(me_id == 24644, 1.297694, 1.082291)
    rr_sd <- ifelse(me_id == 24644, 0.2509040, 0.3529344)
    rr[, paste0("rr_", 0:(n_draws-1)) := as.list(exp(rnorm(n=n_draws,
                                                           mean=rr_mean,
                                                           sd=rr_sd)))]
    rr[parameter == "cat2", paste0("rr_", 0:(n_draws-1)) := 1]
    rr <- melt(rr[xor(mortality == 1, morbidity == 1)],
               id.vars = c("age_group_id", "sex_id", "location_id", "year_id",
                           "cause_id", "mortality", "morbidity", "parameter"),
               measure.vars = paste0("rr_", 0:(n_draws - 1)),
               variable.name = "draw", value.name = "rr")
    rr[, draw := as.numeric(gsub("rr_", "", draw))]

    dt <- merge(exp, rr,
                by = c("location_id", "year_id", "age_group_id", "sex_id", "parameter", "draw"))

    # add tmrel, 0/1 as categorical
    max_categ <- mixedsort(unique(dt$parameter)) %>% tail(., n = 1)
    dt[, tmrel := ifelse(parameter == max_categ, 1, 0)]

    # calc paf
    dt <- categ_paf(dt)
    return(dt)

}

# for opioid, cocaine, and amphetamine
drug_mes <- c(24644, 24645, 24646)
dt <- lapply(drug_mes, calc_drug_paf) %>% rbindlist

#--CALCULATE JOINT PAF ---------------------------------------------------------

# multiplicative agg (1-(1- opiod PAF)(1- cocaine PAF)(1- amphetamine PAF))
dt <- dt[, .(paf = 1-prod(1-paf)), by=c("location_id", "year_id", "age_group_id", "sex_id",
                                        "cause_id", "mortality", "morbidity", "draw")]

# cap it
paf_cap <- data.table(draw = 0:(n_draws - 1),
                      cap = inv.logit(rnorm(n = n_draws,
                                            mean = logit(0.84488),
                                            sd = (logit(0.896145) - logit(0.785724))/(2 * qnorm(0.975)))))
dt <- merge(dt, paf_cap, by = "draw")
dt[paf > cap, paf := cap][, cap := NULL]

#--SAVE + VALIDATE -------------------------------------------------------------

save_paf(dt, rei_id, rei, n_draws, out_dir)
