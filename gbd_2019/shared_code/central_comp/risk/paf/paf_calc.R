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
library(openxlsx)
library(readstata13)

source("./utils/data.R")
source("./utils/db.R")
source("math.R")
source("mediate_rr.R")
source("./custom/add_injuries.R")
source("./custom/sequela_to_cause.R")
source("save.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/get_rei_metadata.R")
source("FILEPATH/get_location_metadata.R")

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
cont <- ifelse(rei_meta$calc_type == 2, TRUE, FALSE)
if (cont) {
    exp_dist <- rei_meta$exp_dist
    inv_exp <- rei_meta$inv_exp
    rr_scalar <- rei_meta$rr_scalar
    tmrel_dist <- rei_meta$tmrel_dist
    tmrel_lower <- rei_meta$tmrel_lower
    tmrel_upper <- rei_meta$tmrel_upper
}

#--PULL EXPOSURE (AND SD IF CONT)-----------------------------------------------

exp <- get_exp(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
if (cont) {
    exp_sd <- get_exp_sd(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
    exp <- merge(exp, exp_sd, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
} else {
    exp[, exp_tot := sum(exp_mean), by = c("location_id","year_id","age_group_id","sex_id","draw")]
    # if sum of exposure categories != 1, this is an error.
    tot_above_1 <- nrow(exp[round(exp_tot, digits=0) > 1])
    if (tot_above_1 != 0 & rei != "occ_asthmagens") {
        print(head(exp[round(exp_tot, digits=0) > 1][order(location_id, year_id, age_group_id, sex_id)]))
        stop("Exposure categories sum to > 1")
    }
    exp[, exp_tot := NULL]
}

#--PULL RR AND MERGE------------------------------------------------------------

if (rei == "envir_lead_bone") {
    # use SBP RRs for Lead (bone) - .61 mmHg shift in SBP, convert SBP unit space to 1 SBP
    rr <- get_rr(107, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
    rr[, rr := rr^(.61/10)]
} else {
    rr <- get_rr(rei_id, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
}

dt <- merge(exp, rr, by = c("location_id", "year_id", "age_group_id", "sex_id",
                            "parameter", "draw"))

# for vaccines and interventions, exposure and rr actually represents the proportion covered, so flip it
if ((rei %like% "vacc_") | (rei_id %in% c(324, 325, 326, 328, 329, 330))) {
    dt[, rr := 1/rr]
    dt[parameter == "cat1", vaccinated := 1][, parameter := "cat1"]
    dt[vaccinated == 1, parameter := "cat2"][, vaccinated := NULL]
}

if (mediate) dt <- mediate_rr(rei_id, dt)

#--ADD TMREL -------------------------------------------------------------------
if (cont) {
    # pull or generate if continuous
    ages <- unique(dt$age_group_id)
    tmrel <- get_tmrel(rei_id, location_id, year_id, sex_id, gbd_round_id,
                       decomp_step, n_draws, tmrel_lower, tmrel_upper, ages)
    dt <- merge(dt, tmrel, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
} else {
    # 0/1 if categorical
    max_categ <- mixedsort(unique(dt$parameter)) %>% tail(., n = 1)
    dt[, tmrel := ifelse(parameter == max_categ, 1, 0)]
    # replace exp with tmrel if vaccine not introduced
    if (rei %in% c("vacc_hib3","vacc_pcv3","vacc_rotac")) {
        vacc_intro <- get_tmrel(rei_id, location_id, year_id, sex_id, gbd_round_id,
                                decomp_step, n_draws)
        setnames(vacc_intro, "tmrel", "vacc_intro")
        dt <- merge(dt, vacc_intro, by = c("location_id", "year_id", "age_group_id", "sex_id", "draw"))
        dt[vacc_intro == 0, exp_mean := tmrel]
        dt[, vacc_intro := NULL]
    }
}

#--CALC PAF ---------------------------------------------------------------------
if (cont) {
    if (exp_dist == "ensemble") {
        weights <- fread(paste0("FILEPATH/", rei, ".csv"))
        dt <- cont_paf_ensemble(dt, weights, rr_scalar, inv_exp, n_draws)
        dt[exp_mean == 0 & exp_sd == 0, paf := 0]
    } else {
        if (rei == "nutrition_iron") {
            dt <- cont_paf_nocap(dt, 0, 5000, rr_scalar, exp_dist, inv_exp)
        } else if (rei == "envir_radon") {
            cap <- fread(paste0(out_dir, "/exposure/exp_max_min.csv"))$cap %>% unique
            dt <- cont_paf_cap(dt, 0, 10000, rr_scalar, exp_dist, inv_exp, cap)
        }
    }
} else {
    dt <- categ_paf(dt)
}
if (rei %in% c("metab_bmd", "occ_hearing"))
    save_for_sev(dt, rei_id, rei, n_draws, out_dir)

# convert hip/non-hip fractures to injury outcomes for bmd
if (rei %in% c("metab_bmd"))
  dt <- add_injuries(dt, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)
# convert hearing sequela to cause for occ noise
if (rei %in% c("occ_hearing"))
  dt <- convert_hearing(dt, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)

#--SAVE + VALIDATE -------------------------------------------------------------
save_paf(dt, rei_id, rei, n_draws, out_dir)
