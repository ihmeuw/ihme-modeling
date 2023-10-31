library(data.table)
library(gtools)
library(magrittr)
library(parallel)
library(reticulate)

gbd_env_python <- "FILEPATH"
Sys.setenv("RETICULATE_PYTHON"=gbd_env_python)
reticulate::use_python(gbd_env_python, required=TRUE)
df_utils <- reticulate::import("ihme_dimensions.dfutils")

initial_args <- commandArgs(trailingOnly = FALSE)
file_arg <- "--file="
script_dir <- dirname(sub(file_arg, "", initial_args[grep(file_arg, initial_args)]))
setwd(script_dir)

source("FILEPATH/get_age_spans.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_cause_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_rei_metadata.R")
source("FILEPATH/db.R")
source("FILEPATH/mediate_rr.R")
source("./rr_max_lbw_preterm.R")
source("./utils/common.R")
source("./utils/exp_percentiles.R")

set.seed(124535)

#-- SET UP ARGS ----------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
rei_id <- as.numeric(args[1])
gbd_round_id <- as.numeric(args[2])
decomp_step <- args[3]
year_id <- eval(parse(text = paste0("c(",args[4],")")))
n_draws <- as.numeric(args[5])
out_dir <- args[6]

# find 1st, 99th percentiles for pre-GBD 2020 and 5th, 95th percentiles for GBD 2020+
percentile_interval <- ifelse(gbd_round_id < 7, 0.01, 0.05)
# this risk-outcome uses an absolute risk curve
sbp_rei_id <- 107
htn_cause_id <- 498
htn_threshold <- 140

rei_meta <- get_rei_meta(rei_id, gbd_round_id)
rei <- rei_meta$rei
message("starting RRmax for ", rei_meta$rei, ", rei_id ", rei_id)
modelable_entities <- get_rei_mes(
    rei_id = rei_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step
)
draw_cols <- paste0("draw_", 0:(n_draws-1))
modeler_prepped_rrmax <- c("air_hap", "air_no2", "air_ozone", "air_pm", "air_pmhap",
                           "bullying", "drugs_alcohol", "smoking_direct")

#--READ MODELER PREPPED RRMAX --------------------------------------------------
if (rei %in% modeler_prepped_rrmax) {

    # modeler provides draws of RRmax for these risks
    rrmax_file <- paste0("FILEPATH/", rei, ".csv")
    message("reading modeler prepped RRmax from ", rrmax_file)
    rr_max <- fread(rrmax_file)

    # resample to n_draws
    rr_max <- downsample(dt=rr_max, n_draws=n_draws, df_utils=df_utils)

    # add outcomes for PM HAP risks that are mediated through lbw/sg. If there is any
    # overlap, we keep the directly-modeled RRmax draws and discard the mediated values.
    if (rei %in% c("air_pmhap", "air_pm", "air_hap")) {
        rr_max <- dataframe_unique(
            rbind(
                rr_max,
                get_lbw_preterm_rr(
                    rei="nutrition_lbw_preterm", gbd_round_id=gbd_round_id,
                    decomp_step=decomp_step, n_draws=n_draws
                )
            ), by_cols=c("cause_id", "age_group_id", "sex_id")
        )
    }

#--CUSTOM CALCULATE LBW & SG RRMAX ---------------------------------------------
} else if (rei %like% "lbw|preterm") {

    rr_max <- get_lbw_preterm_rr(
        rei=rei, gbd_round_id=gbd_round_id, decomp_step=decomp_step, n_draws=n_draws
    )

#--CALCULATE RRMAX FOR RISKS WITH UPLOADED RRS----------------------------------
} else {

    cause_metadata <- get_cause_metadata(2, gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    # get causes in RRs of risk and any in mediator risk(s) if applicable
    rr_model_version_id <- modelable_entities[draw_type == "rr", ]$model_version_id
    rr_metadata <- rbindlist(lapply(
        rr_model_version_id, function(x) get_rr_metadata(x)[, model_version_id := x]
    ), use.names = TRUE)
    # for sbp, drop hypertensive heart disease since 100%
    if (rei == "metab_sbp") rr_metadata <- rr_metadata[cause_id != htn_cause_id, ]
    if (nrow(rr_metadata) > 0) {
        rr_metadata[, source := "rr"]
        setnames(rr_metadata, "rei_id", "med_id")
    }
    mediators <- get_mediator_cause_pairs(rei_id)[!cause_id %in% rr_metadata$cause_id, ]
    if (nrow(mediators) > 0) {
        med_mes <- rbindlist(lapply(unique(mediators$med_id), function(x) {
            get_rei_mes(x, gbd_round_id, decomp_step)[!draw_type %like% "paf", ]
        }), use.names = TRUE)
        med_rr_metadata <- rbindlist(lapply(med_mes$model_version_id, get_rr_metadata),
                                     use.names = TRUE) %>% setnames(., "rei_id", "med_id") %>%
            merge(., mediators[, .(med_id, cause_id)], by = c("med_id", "cause_id"))
        med_rr_metadata[, source := "delta"]
        modelable_entities <- rbind(modelable_entities, med_mes)
        rr_metadata <- rbind(rr_metadata, med_rr_metadata, fill=TRUE)
    }

    # check if relative risks vary by year. if they do, we'll pull years requested.
    # otherwise, reset year_id to a single year we'll use as reference
    for (rr_model_version_id in unique(rr_metadata$model_version_id)) {
        rr_summ <- fread(paste0("FILEPATH/", rr_model_version_id, "/summaries/summaries.csv"))
        if(!"exposure" %in% names(rr_summ)) rr_summ[, exposure := as.numeric(NA)]
        rr_summ[, year_mean := mean(mean),
                by = c("age_group_id", "sex_id", "location_id", "cause_id",
                       "mortality", "morbidity", "exposure", "parameter"), ]
        rr_by_year_id <- !all(rr_summ$mean == rr_summ$year_mean)
        rr_year_id <- rr_summ$year_id %>% unique
        rm(rr_summ)
        rr_metadata[model_version_id==rr_model_version_id, `:=` (
            by_year_id=rr_by_year_id, year_id=ifelse(rr_by_year_id, as.integer(NA), max(rr_year_id))
        )]
    }

    # for continuous risks: pull exposure max/min, TMREL, and if two-stage, deltas
    if (any(c(2, 3) %in% rr_metadata$relative_risk_type_id)) {

        # calculate exposure at the max (or min) exposure percentile. Cache the exposure
        # max so we don't have to recalculate for mediated risks
        get_exp_max <- function(rei_id, median_threshold = NULL) {
            median_text <- ifelse(is.null(median_threshold), "", paste0("_median_above_", median_threshold))
            max_min_path <- paste0(out_dir, "/rrmax/exposure_max_min/", rei_id, median_text, ".csv")
            if (file.exists(max_min_path)) {
                exposure_max_min <- fread(max_min_path)
            } else {
                meta <- get_rei_meta(rei_id = rei_id, gbd_round_id = gbd_round_id)
                exposure_max_min <- get_exp_percentiles(rei_id = rei_id, gbd_round_id = gbd_round_id,
                                                        decomp_step = decomp_step, rei_meta = meta,
                                                        pop_path = paste0(out_dir, "/population_35.csv"),
                                                        percentile_interval = percentile_interval,
                                                        median_threshold = median_threshold
                )[, .(rei_id, age_group_id, exposure_max=get(ifelse(meta$inv_exp == 1, "lower", "upper")))]
                write.csv(exposure_max_min, max_min_path, row.names = FALSE)
                message("generated and saved exposure max for rei_id ", rei_id)
            }
            return(exposure_max_min)
        }
        exp_max <- get_exp_max(rei_id)

        # cache the actual percentiles used for reference
        exposure_pct_path <- paste0(out_dir, "/exposure_percentiles.csv")
        if (!file.exists(exposure_pct_path)) {
            exp_pct <- data.table(lower = percentile_interval, upper = 1 - percentile_interval)
            write.csv(exp_pct, exposure_pct_path, row.names = FALSE)
        }

        # pull or generate tmrel for risk
        if ("tmrel" %in% modelable_entities$draw_type) {
            tmrel <- get_draws("rei_id",  rei_id, "tmrel", year_id = year_id,
                               gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                               n_draws = n_draws, downsample = TRUE)
            tmrel <- tmrel[, lapply(.SD, mean), by=c("age_group_id", "sex_id"), .SDcols=draw_cols]
        } else {
            demo <- get_demographics("epi", gbd_round_id = gbd_round_id)
            tmrel <- expand.grid(age_group_id = demo$age_group_id,
                                 sex_id = demo$sex_id) %>% data.table
            tmrel[, (draw_cols) := as.list(runif(n_draws, rei_meta$tmrel_lower, rei_meta$tmrel_upper))]
        }
        tmrel_cols <- gsub("draw", "tmrel", draw_cols)
        setnames(tmrel, draw_cols, tmrel_cols)

        # read any delta draws for this risk and its mediators
        if ("delta" %in% rr_metadata$source) {
            delta_dt <- read_deltas()[rei_id == rei_meta$rei_id]
            delta_dt <- downsample(dt=delta_dt, n_draws=n_draws, df_utils=df_utils)
            delta_cols <- paste0("delta_", 0:(n_draws-1))
            setnames(delta_dt, paste0("draw_", 0:(n_draws-1)), delta_cols)
            # if the mediator RR isn't log-linear, additionally calculate
            # delta_term = delta * (distal exposure +- distal tmrel)
            # and pull the exposure max for the mediators
            if (3 %in% rr_metadata[source == "delta", ]$relative_risk_type_id) {
                delta_term <- merge(delta_dt, exp_max, by = "rei_id", allow.cartesian = TRUE) %>%
                    merge(., tmrel, by = "age_group_id")
                if (rei_meta$inv_exp) {
                    delta_term[, (draw_cols) := lapply(1:n_draws, function(x)
                        get(delta_cols[x]) * min(exposure_max - get(tmrel_cols[x]), 0.0)
                    ), by=1:nrow(delta_term)]
                } else {
                    delta_term[, (draw_cols) := lapply(1:n_draws, function(x)
                        get(delta_cols[x]) * max(exposure_max - get(tmrel_cols[x]), 0.0)
                    ), by=1:nrow(delta_term)]
                }
                delta_term <- delta_term[, c("med_id", "age_group_id", "sex_id", draw_cols), with = FALSE]
                med_exp_max <- rbindlist(lapply(unique(rr_metadata[med_id != rei_id, ]$med_id),
                                                get_exp_max), use.names = TRUE) %>%
                    setnames(., c("rei_id", "exposure_max"), c("med_id", "med_exposure_max"))
            }

        }
    }

    message("reading RR draws by cause and finding RR at max risk exposure")
    rr_max_list <- list()
    for (i in 1:nrow(rr_metadata)) {
        relative_risk_type_id <- rr_metadata[i, ]$relative_risk_type_id
        message("cause_id ", rr_metadata[i, ]$cause_id)
        id_cols <- c("cause_id", "location_id", "year_id", "age_group_id", "sex_id")
        rr_year_id <- if (rr_metadata[i, ]$by_year_id == FALSE) { rr_metadata[i, ]$year_id } else { year_id }
        rr <- get_draws(
            c("rei_id", "cause_id"), c(rr_metadata[i, ]$med_id, rr_metadata[i, ]$cause_id), "rr",
            year_id = rr_year_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step,
            n_draws = n_draws, downsample = TRUE, version_id = rr_metadata[i, ]$model_version_id) %>%
            setnames(., "rei_id", "med_id")

        # for vaccines and interventions, rr represents the proportion covered, so flip it
        if ((rei %like% "vacc_") | (rei_id %in% c(324, 325, 326, 328, 329, 330, 336))) {
            rr[, (draw_cols) := lapply(.SD, function(x) 1/x), .SDcols=draw_cols]
            rr[parameter == "cat1", vaccinated := 1][, parameter := "cat1"]
            rr[vaccinated == 1, parameter := "cat2"][, vaccinated := NULL]
        }

        # use YLL RRs unless cause is YLD only. Occ Noise RRs are by sequela
        # and are YLD only by definition
        yld_only <- rr_metadata[i, ]$cause_id %in% cause_metadata[yld_only==1, ]$cause_id
        if(yld_only | rei == "occ_hearing") {
            rr <- rr[morbidity == 1, ]
        } else {
            rr <- rr[mortality == 1, ]
        }

        if (relative_risk_type_id == 1) {

            # categorical RRs: keep exposure category with highest RR (max exposure)
            rr[, mean_rr := rowMeans(.SD), by=c("age_group_id", "sex_id"), .SDcols=draw_cols]
            exp_max <- mixedsort(unique(rr[mean_rr == max(mean_rr), ]$parameter)) %>% head(., n = 1)
            rr <- rr[parameter == exp_max, ]

        } else if (relative_risk_type_id == 2) {

            # log-linear per-unit RRs: merge tmrel and calculate RR at exposure max
            rr <- merge(rr, tmrel, by = c("age_group_id", "sex_id"))
            rr <- merge(rr, exp_max[, .(age_group_id, exposure_max)], by="age_group_id")

            if (rei_meta$inv_exp == 1) {
                rr[, (tmrel_cols) := lapply(.SD, function(x)
                    (x - exposure_max) / rei_meta$rr_scalar), .SDcols=tmrel_cols]
            } else {
                rr[, (tmrel_cols) := lapply(.SD, function(x)
                    (exposure_max - x) / rei_meta$rr_scalar), .SDcols=tmrel_cols]
            }
            if (rr_metadata[i, ]$source == "delta") {
                # if RR is from the mediator, use delta to shift to distal exposure
                rr <- merge(rr, delta_dt, by = "med_id")
                rr[, (draw_cols) := lapply(1:n_draws, function(x)
                    get(draw_cols[x]) ^ get(delta_cols[x])
                )]
            }
            rr[, (draw_cols) := lapply(1:n_draws, function(x)
                get(draw_cols[x]) ^ ((abs(get(tmrel_cols[x])) + get(tmrel_cols[x])) / 2)
            )]

        } else if (relative_risk_type_id == 3) {

            # exposure-dependent RRs: approximate RR at the exposure max
            rr[, all_zero := all(rowSums(.SD) == 0), .SDcols = draw_cols, by=id_cols]
            if (rr_metadata[i, ]$source == "delta") {
                # if RR is from the mediator, use delta to shift to distal exposure,
                # mediator RR (mediator exposure max + delta (distal exposure max +- distal TMREL)) /
                #   mediator RR(mediator exposure max)

                # when mediator is SBP and cause is hypertension, the exposure max is the median
                # of SBP exposure above the definitional threshold of high SBP
                if (rr_metadata[i, ]$med_id == sbp_rei_id & rr_metadata[i, ]$cause_id == htn_cause_id) {
                    med_exp_max_for_cause <- get_exp_max(rr_metadata[i, ]$med_id, htn_threshold)
                    setnames(med_exp_max_for_cause, c("rei_id", "exposure_max"), c("med_id", "med_exposure_max"))
                } else {
                    med_exp_max_for_cause <- med_exp_max
                }

                rr <- merge(rr, med_exp_max_for_cause, by = c("med_id",  "age_group_id"))
                rr <- rr[order(location_id, year_id, age_group_id, sex_id, exposure)]
                rr[, (draw_cols) := mclapply(1:n_draws, function(x) {
                    risk_curve <- approxfun(x=exposure, y=get(draw_cols[x]), rule=2, ties = "ordered")
                    aid <- age_group_id
                    sid <- sex_id
                    denominator <- risk_curve(med_exposure_max)
                    if (denominator == 0) {
                        stop(paste0("Relative risk at distal risk exposure max is 0 for draw ", x,
                                    ". Invalid for approximating RR at exposure max as it would result ",
                                    "in an infinite RRmax value."))
                    }
                    return(
                        risk_curve(med_exposure_max + delta_term[
                            med_id == rr_metadata[i, ]$med_id & age_group_id==aid & sex_id==sid,
                            get(draw_cols[x])
                        ]) /
                            denominator
                    )
                }, mc.cores = 10),
                by=id_cols]
            } else {
                # otherwise, distal RR(distal exposure max)
                rr <- merge(rr, exp_max[, .(age_group_id, exposure_max)], by="age_group_id")
                rr <- rr[order(location_id, year_id, age_group_id, sex_id, exposure)]
                rr[, (draw_cols) := mclapply(1:n_draws, function(x) {
                    risk_curve <- approxfun(x=exposure, y=get(draw_cols[x]), rule=2, ties = "ordered")
                    aid <- age_group_id
                    sid <- sex_id
                    return(risk_curve(exposure_max) /
                               risk_curve(tmrel[age_group_id==aid & sex_id==sid, get(tmrel_cols[x])]))
                }, mc.cores = 10),
                by=id_cols]
            }
            rr[all_zero == 1, (draw_cols) := 1]
            rr <- dataframe_unique(rr[, c(id_cols, draw_cols), with=FALSE])
        }

        id_cols <- setdiff(id_cols, c("location_id", "year_id"))
        rr <- rr[, lapply(.SD, mean), by=id_cols, .SDcols=draw_cols]
        rr[, med_id := rr_metadata[i, med_id]]
        rr_max_list[[i]] <- rr
        rm(rr)

    }

    rr_max <- rbindlist(rr_max_list, use.names = TRUE)

    if (rei == "drugs_illicit_suicide") {
        # average across opioid, cocaine, amphetamine to the cause level
        rr_max <- rr_max[, lapply(.SD, mean), by=c(id_cols, "med_id"), .SDcols=draw_cols]
    }

    # Validate that there's no duplicate rows across cause/age group/sex/mediator
    if (any(duplicated(rr_max, by = c(id_cols, "med_id")))) {
        duplicates <- rr_max[duplicated(rr_max, by = c(id_cols, "med_id"))]
        stop(paste0(nrow(duplicates), " duplicate(s) found after finding RR at max risk exposure. ",
                    "Examples:\n", paste(capture.output(head(duplicates)), collapse = "\n")))
    }

    # Adjust RRmax when a cause has multiple mediator risks (ex: both FPG and SBP)
    # RRmax becomes: (RRmax through mediator 1 + RRmax through mediator 2) - 1.
    # If risk doesn't have multiple mediators per cause, this is a no-op
    rr_max[, (draw_cols) := lapply(.SD, function(x)
        ifelse(length(x) > 1, sum(x) - 1, x)), by=id_cols, .SDcols=draw_cols]
    rr_max[, med_id := NULL]
    rr_max <- dataframe_unique(rr_max)

    # Validate collapsing across mediators was successful (ie. indexes cause/age group/sex are unique)
    if (any(duplicated(rr_max, by = id_cols))) {
        duplicates <- rr_max[duplicated(rr_max, by = id_cols)]
        stop(paste0(nrow(duplicates), " duplicate(s) found in RR max dataframe after adjusting ",
                    "for multiple mediator risks. Examples:\n",
                    paste(capture.output(head(duplicates)), collapse = "\n")))
    }
}

#--REMOVE 100% OUTCOMES AND VALDATE DRAW VALUES --------------------------------
# drop any RRs that are for 100% attributable outcomes
one_hundred <- fread("FILEPATH/pafs_of_one.csv")[rei_id == rei_meta$rei_id, ]
rr_max <- rr_max[!cause_id %in% one_hundred$cause_id, ]

# Validate that no RRmax ends up being < 0 or NA
# (in unlikely case that RRmax 1 and RRmax 2 are less than 1 or something else goes wrong)
rr_max[, has_negatives_or_nas  := apply(.SD, 1, function(x) {any(x < 0 | is.na(x))}), .SDcols = draw_cols]
if (nrow(rr_max[(has_negatives_or_nas)]) > 0) {
    negatives_or_nas <- rr_max[(has_negatives_or_nas)]
    stop(paste0(nrow(negatives_or_nas), " row(s) have negative/NA RR max values. Examples:\n",
                paste(capture.output(head(negatives_or_nas)), collapse = "\n")))
}
rr_max[, has_negatives_or_nas := NULL]

#--SEV RRMAX DRAWS--------------------------------------------------------------
# sort and save draws
rr_max$rei_id <- rei_id
rr_max <- rr_max[, c("rei_id", "cause_id", "age_group_id", "sex_id", ..draw_cols)]
setorder(rr_max, rei_id, cause_id, age_group_id, sex_id)
out_file <- paste0(out_dir, "/rrmax/", rei_id, ".csv")
message("draws complete, saving to ", out_file)
write.csv(rr_max, out_file, na = "", row.names = F)
