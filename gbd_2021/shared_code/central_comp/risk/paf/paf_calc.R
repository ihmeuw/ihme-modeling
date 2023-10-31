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
library(mvtnorm)

source("math.R")
source("mediate_rr.R")
source("save.R")
source("two_stage_mediation.R")
source("./custom/add_injuries.R")
source("./custom/sequela_to_cause.R")
source("./custom/split_parent_cause_to_subcauses.R")
source("./utils/data.R")
source("./utils/db.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_rei_metadata.R")
source("FILEPATH/get_restrictions.R")

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
codcorrect_version <- as.numeric(args[9])
update_bmi_fpg_exp_sd <- as.logical(args[10])

# get risk info
rei_meta <- get_rei_meta(rei_id, gbd_round_id)
cont <- ifelse(rei_meta$rei_calculation_type == 2, TRUE, FALSE)
has_tmrel_draws <- "tmrel" %in%
    fread(paste0(out_dir, "/mes.csv"))[rei_id == rei_meta$rei_id, ]$draw_type

rr_metadata <- fread(paste0(out_dir, "/rr_metadata.csv"))
restricted_causes <- get_restrictions(
    restriction_type = "sex", cause_id = rr_metadata$cause_id, sex_id = sex_id,
    gbd_round_id = gbd_round_id, decomp_step = decomp_step, cause_set_id = 2)$cause_id
rr_metadata <- rr_metadata[!cause_id %in% restricted_causes, ]
# for activity and FPG, drop male breast cancer even though not sex-restricted
if (rei_meta$rei %in% c("activity", "metab_fpg") & sex_id == 1)
    rr_metadata <- rr_metadata[cause_id != 429, ]

# Read mediation matrix. Will be empty if mediation does not apply to risk.
mediation <- get_mediation(rei_id)
mediation <- mediation[cause_id %in% rr_metadata$cause_id]
if (!cont & nrow(mediation) > 0)
    stop("Mediation factors found but mediation not yet implemented for rei ID ", rei_id)

#--PULL EXPOSURE (AND SD IF CONT) AND TMREL ------------------------------------
dt <- get_exposure_and_tmrel(
    rei_id=rei_id, location_id=location_id, year_id=year_id, sex_id=sex_id,
    gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws,
    cont=cont, has_tmrel_draws=has_tmrel_draws,
    tmrel_lower=rei_meta$tmrel_lower, tmrel_upper=rei_meta$tmrel_upper,
    update_bmi_fpg_exp_sd=update_bmi_fpg_exp_sd
)
if (rei_meta$rei %in% c("metab_bmi_adult", "metab_fpg") & update_bmi_fpg_exp_sd)
    save_exp_sd(dt, rei_id, rei_meta$rei, n_draws, out_dir)

#--CALC PAF ---------------------------------------------------------------------
if (cont) {
    if (rei_meta$exposure_type == "ensemble") {
        wlist <- fread(paste0("FILEPATH", rei_meta$rei, ".csv"))
    } else {
        wlist <- NULL  # this case is only for radon
    }

    # exposure percentiles and age restrictions
    exp_percentiles <- fread(paste0(out_dir, "/exposure/exp_max_min.csv"))
    dt <- dt[age_group_id %in% exp_percentiles$age_group_id, ]

    # read any delta draws for this risk and its mediators
    if ("delta" %in% rr_metadata$source) delta_dt <- get_delta_draws(rei_id)

    # calculate PAFs for each cause
    # RR methods for continuous risks can vary by cause (exposure-based vs log-linear)
    out <- vector("list", length(rr_metadata$cause_id)) %>%
        setNames(., paste0(rr_metadata$med_id, "_", rr_metadata$cause_id))
    out_unmed <- vector("list", length(unique(mediation$cause_id))) %>%
        setNames(., unique(mediation$cause_id))

    for (mid in unique(rr_metadata$med_id)) {
        rr_source <- unique(rr_metadata[med_id == mid, ]$source)
        rr_rei_id <- if (rr_source == "rr") rei_id else mid

        if (rr_source == "delta") {
            med_meta <- get_rei_meta(rei_id=mid, gbd_round_id=gbd_round_id)
            med_cont <- ifelse(med_meta$rei_calculation_type == 2, TRUE, FALSE)
            med_has_tmrel_draws <- "tmrel" %in% get_rei_mes(
                rei_id=mid, gbd_round_id=gbd_round_id, decomp_step=decomp_step
            )$draw_type
            med_wlist <- fread(paste0("FILEPATH", rei_meta$rei, ".csv"))
            med_exp_percentiles <- fread(
                paste0("FILEPATH", med_meta$rei, "/exposure/exp_max_min.csv")
            )
            med_dt <- get_exposure_and_tmrel(
                rei_id=mid, location_id=location_id, year_id=year_id, sex_id=sex_id,
                gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws,
                cont=med_cont, has_tmrel_draws=med_has_tmrel_draws,
                tmrel_lower=med_meta$tmrel_lower, tmrel_upper=med_meta$tmrel_upper
            )
            value_cols <- c("exp_mean", "exp_sd", "tmrel")
            setnames(med_dt, value_cols, paste0("med_", value_cols))
            med_dt <- merge(med_dt, dt, by = c("location_id", "year_id", "age_group_id",
                                               "sex_id", "draw", "parameter"))
            setnames(med_dt, value_cols, paste0("distal_", value_cols))
            med_dt <- merge(med_dt, delta_dt[med_id == mid, .(draw, delta)], by = "draw")
            if (rei_meta$rei == "diet_salt" & med_meta$rei == "metab_sbp")
                med_dt <- adjust_salt_sbp_delta(
                    dt=med_dt, gbd_round_id=gbd_round_id, decomp_step=decomp_step,
                    n_draws=n_draws
                )
        }

        for (cid in rr_metadata[med_id == mid, ]$cause_id) {
            rr_by_year_id <- rr_metadata[med_id == mid & cause_id==cid, ]$by_year_id
            rr_year_id <- if (rr_by_year_id) year_id else
                rr_metadata[med_id == mid & cause_id==cid, ]$year_id
            rr <- get_rr(
                rei_id=rr_rei_id, location_id=location_id, year_id=rr_year_id,
                sex_id=sex_id, gbd_round_id=gbd_round_id, decomp_step=decomp_step,
                n_draws=n_draws, cause_id=cid, long=FALSE, by_year_id=rr_by_year_id
            )

            # calculate continuous exposure PAF using RRs or two stage method
            if (rr_source == "rr") {
                # cause is part of distal risk RR model. calculate PAFs,
                # then calculate unmediated PAFs, if applicable.
                out[[paste0(mid, "_", cid)]] <- cont_paf(
                    dt=dt,
                    rr=rr,
                    rr_type=rr_metadata[med_id == mid & cause_id==cid, relative_risk_type_id],
                    rr_scalar=rei_meta$rr_scalar,
                    inv_exp=rei_meta$inv_exp,
                    dist=rei_meta$exposure_type,
                    wlist=wlist,
                    exp_percentiles=exp_percentiles,
                    n_draws=n_draws,
                    mediation=NULL
                )[, med_id := mid]
                if (cid %in% mediation$cause_id) {
                    out_unmed[[paste(cid)]] <- cont_paf(
                        dt=dt,
                        rr=rr,
                        rr_type=rr_metadata[med_id == mid & cause_id==cid, relative_risk_type_id],
                        rr_scalar=rei_meta$rr_scalar,
                        inv_exp=rei_meta$inv_exp,
                        dist=rei_meta$exposure_type,
                        wlist=wlist,
                        exp_percentiles=exp_percentiles,
                        n_draws=n_draws,
                        mediation=mediation
                    )
                }
                rm(rr)
            } else {
                # for SBP-HTN, we currently have an absolute risk curve rather than
                # a relative risk curve and need to integrate the numerator and the
                # denominator separately
                absolute_risk_curve <- mid == 107 & cid == 498
                out[[paste0(mid, "_", cid)]] <- two_stage_cont_paf(
                    dt=med_dt,
                    n_draws=n_draws,
                    rr=rr,
                    rr_type=rr_metadata[med_id == mid & cause_id==cid, relative_risk_type_id],
                    rr_scalar=med_meta$rr_scalar,
                    distal_dist=rei_meta$exposure_type,
                    distal_wlist=wlist,
                    distal_exp_percentiles=exp_percentiles,
                    med_dist=med_meta$exposure_type,
                    med_wlist=med_wlist,
                    med_exp_percentiles=med_exp_percentiles,
                    med_inv_exp=med_meta$inv_exp,
                    distal_inv_exp=rei_meta$inv_exp,
                    absolute_risk_curve=absolute_risk_curve
                )[, med_id := mid]
            }
        }
    }
    dt <- rbindlist(out, fill=TRUE, use.names=TRUE)
    dt_unmed <- rbindlist(out_unmed, fill=TRUE, use.names=TRUE)

} else {

    # categorical risk: read RRs for all causes and merge
    rr_by_year_id <- unique(rr_metadata$by_year_id)
    rr_year_id <- if (rr_by_year_id) year_id else unique(rr_metadata$year_id)
    rr <- get_rr(
        rei_id=rei_id, location_id=location_id, year_id=rr_year_id,
        sex_id=sex_id, gbd_round_id=gbd_round_id, decomp_step=decomp_step,
        n_draws=n_draws, long=TRUE, by_year_id=rr_by_year_id
    )
    rr_merge_cols <- c("age_group_id", "sex_id", "parameter", "draw")
    if (rr_by_year_id) rr_merge_cols <- c(rr_merge_cols, "year_id")
    dt <- merge(dt, rr, by = rr_merge_cols, allow.cartesian = TRUE)

    # for vaccines and interventions, exposure and rr represent the proportion covered, so flip it
    if ((rei_meta$rei %like% "vacc_") | (rei_id %in% c(324, 325, 326, 328, 329, 330, 336))) {
        dt[, rr := 1/rr]
        dt[parameter == "cat1", intervened := 1]
        dt[, `:=` (parameter="cat1", tmrel=0)]
        dt[intervened == 1, `:=` (parameter="cat2", tmrel=1)]
        dt[, intervened := NULL]
    }

    # calculate PAFs
    dt <- categ_paf(dt)
}

# BMI for breast cancer in pre-menopausal women cannot be above 0
if (rei_meta$rei == "metab_bmi_adult" & sex_id == 2) {
    under_fifty <- get_age_metadata(gbd_round_id=gbd_round_id)[age_group_years_start < 50, ]
    dt[cause_id == 429 & age_group_id %in% under_fifty$age_group_id & paf > 0, paf := 0]
}

if (rei_meta$rei %in% c("metab_bmd", "occ_hearing"))
    save_for_sev(dt, rei_id, rei_meta$rei, n_draws, out_dir)

# convert hip/non-hip fractures to injury outcomes for bmd
if (rei_meta$rei %in% c("metab_bmd"))
    dt <- add_injuries(dt, location_id, year_id, sex_id, gbd_round_id, decomp_step,
                       n_draws, codcorrect_version)
# convert hearing sequela to cause for occ noise
if (rei_meta$rei %in% c("occ_hearing"))
    dt <- convert_hearing(dt, location_id, year_id, sex_id, gbd_round_id, decomp_step, n_draws)

# split parent CKD to CKD subcauses
ckd_cause_id <- 589
if (ckd_cause_id %in% rr_metadata$cause_id) {
    ckd_parent_dt <- dt[cause_id == ckd_cause_id, ]
    ckd_subcause_dt <- split_parent_cause(
        rei_id = rei_id,
        med_id = rr_metadata[cause_id == ckd_cause_id, ]$med_id,
        parent_cause_id = ckd_cause_id,
        parent_dt = ckd_parent_dt,
        gbd_round_id = gbd_round_id,
        decomp_step = decomp_step
    )
    if (length(rr_metadata[cause_id == ckd_cause_id, ]$med_id) > 1) {
        # if there are multiple mediators, both FPG and SBP, sum across mediators
        # to get the total mediated PAF
        ckd_parent_dt <- ckd_parent_dt[, .(paf=sum(paf)),
                                       by = .(location_id, year_id, age_group_id,
                                              sex_id, cause_id, draw, mortality,
                                              morbidity)]
    }
    dt <- rbindlist(list(dt[cause_id != ckd_cause_id, ], ckd_parent_dt, ckd_subcause_dt),
                    use.names = TRUE, fill = TRUE)
}

#--SAVE + VALIDATE -------------------------------------------------------------
# Ensure that we've generated all the expected PAFs
validate_paf_complete(dt, year_id)
if (nrow(mediation) > 0)
    save_paf(dt_unmed, rei_id, rei_meta$rei, n_draws, out_dir_unmed)
save_paf(dt, rei_id, rei_meta$rei, n_draws, out_dir)
