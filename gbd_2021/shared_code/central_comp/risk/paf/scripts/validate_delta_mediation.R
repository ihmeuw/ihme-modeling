#' Validate the given delta file and medation matrix for use in two stage mediation
#'
#' Required args:
#' @param delta_file (str) the file containing delta values to be validated
#' @param mediation_file (str) the mediation matrix to be validated
#' Optional args:
#' @param gbd_round_id (int) Default is 7, GBD 2020
#' @param decomp_step (str) Default is "iterative"

# smoking causes taken from FILEPATH
# These are the causes expected in the mediation matrix. If additional smoking causes are added,
# the validator will detect them. We don't expect smoking mediation to change for GBD 2020.
smoking_causes <- c("inj_trans_road_pedest", "inj_trans_road_pedal", "inj_trans_road_2wheel",
                    "inj_trans_road_4wheel", "inj_trans_road_other", "inj_trans_other",
                    "inj_falls", "inj_mech_other", "inj_animal_nonven", "inj_homicide_other")

# given a risk get a vector of cause_ids for the best rr model
get_rei_causes <- function(rei_id, gbd_round_id, decomp_step, mediation) {
    if (rei_id == 99) {
        # smoking does not have RRs uploaded but uses these injury causes
        return(mediation[acause %in% smoking_causes]$cause_id)
    } else {
        rr_mvid <- get_rei_mes(
            rei_id, gbd_round_id, decomp_step
        )[draw_type=="rr"]$model_version_id
        causes <- get_rr_metadata(rr_mvid)$cause_id    
        return(causes)
    }
}

validate <- function(
    delta_file, mediation_file,
    gbd_round_id=7, decomp_step="iterative") {

    # load libraries and functions
    library(data.table)
    library(dplyr)
    source("FILEPATH/db.R")
    source("FILEPATH/validations.R")

    deltas <- fread(delta_file)
    deltas[is.na(delta), delta := shift]
    mediation <- fread(mediation_file)

    # deltas between harmful and protective risks should be netative. Otherwise positive
    message("checking that delta values are in range")
    for (i in 1:nrow(deltas)) {
        distal_protective <- get_rei_meta(deltas[i]$rei_id, gbd_round_id)$inv_exp
        mediator_protective <- get_rei_meta(deltas[i]$med_id, gbd_round_id)$inv_exp
        if (distal_protective == mediator_protective) {
            if (deltas[i]$delta <= 0) {
                message("Found deltas <= 0 where both risks are harmful or protective: (",
                        deltas[i]$rei_id, ", ", deltas[i]$med_id, ")")
            }
        } else {
            if (deltas[i]$delta >= 0) {
                message("Found deltas >= 0 for a harmful/protective risk pair (",
                        deltas[i]$rei_id, ", ", deltas[i]$med_id, ")")
            }
        }
    }

    # all mediation factors must be (0, 1]
    message("checking that mediation factors are in the range of (0, 1]")
    if (min(mediation$mean_mediation) <= 0) {
        min_rei <- mediation[mean_mediation==min(mean_mediation), rei_id][1]
        message("Found mediation factors <= 0. Example rei_id: ", min_rei)
    }
    if (max(mediation$mean_mediation) > 1) {
        max_rei <- mediation[mean_mediation==max(mean_mediation), rei_id][1]
        message("Found mediation factors > 1. Example rei_id: ", max_rei)
    }

    for (distal in unique(mediation$rei_id)) {
        message(paste0("checking distal risk ", distal))
        distal_causes <- get_rei_causes(distal, gbd_round_id, decomp_step, mediation)
        two_stage_causes <- setdiff(mediation[rei_id==distal]$cause_id, distal_causes)
        two_stage_matrix <- mediation[rei_id==distal & cause_id %in% two_stage_causes]

        # all two stage distal-mediator relationships must have a delta
        missing_mediators <- setdiff(unique(two_stage_matrix$med_id), deltas[rei_id==distal]$med_id)
        if (length(missing_mediators > 0)) {
            message("Missing delta for distal ", distal, " and mediator(s) ", missing_mediators)
        }

        # check for missing mediator-cause RRs
        for (mediator in unique(two_stage_matrix$med_id)) {
            missing_mediator_rrs <- setdiff(
                two_stage_matrix[med_id==mediator]$cause_id,
                get_rei_causes(mediator, gbd_round_id, decomp_step, mediation))
            if (length(missing_mediator_rrs) > 0) {
                message(
                    "Missing rr model for mediator ", mediator,
                    " and cause(s) ", paste(missing_mediator_rrs, collapse=", ")
                )
            }
        }
    }

    # all distal and mediator risks with two stage mediation, as well as distal risks with
    # mediation factor < 1 must be continuous and centrally calculated. If we've gotten this far,
    # we can trust that the deltas contain all risks that require two stage mediaiton.
    message("checking risk types")
    for (rei_id in unique(c(deltas$rei_id, deltas$med_id, mediation[mean_mediation < 1]$rei_id))) {
        valid_mediator_risk(rei_id, gbd_round_id)
    }
}
