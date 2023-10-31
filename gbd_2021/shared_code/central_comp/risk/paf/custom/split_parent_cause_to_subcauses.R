#' For a list of cause IDs and a location / sex, pull GBD 2020 R1 intermediate
#' DALYs (summation of YLLs and YLDs) in count space, by draw, for the year 2010
#'
#' @param subcause_id cause ID(s) to pull YLLs and YLDs for
#' @param location_id location ID(s) to pull YLLs and YLDs for
#' @param sex_id sex ID(s) to pull YLLs and YLDs for
#' @param n_draws number of draws needed of YLLs and YLDs
#'
#' @return data.table with columns cause_id, location_id, age_group_id, sex_id,
#' draw, subcause_burden
get_subcause_burden <- function(subcause_id, location_id, sex_id, n_draws) {
    source("FILEPATH/get_draws.R")
    source("FILEPATH/get_population.R")

    # pull YLLs and YLDs for 2010 from GBD 2020 CoDCorrect v257 and COMO v932
    subcause_dt <- rbind(
        get_draws(gbd_id_type = rep("cause_id", length(subcause_id)),
                  gbd_id = subcause_id, location_id = location_id, year_id = 2010,
                  sex_id = sex_id, measure_id = 4, n_draws = n_draws, downsample = TRUE,
                  gbd_round_id = 7, decomp_step = "step3", source = "codcorrect",
                  version_id = 257),
        get_draws(gbd_id_type = rep("cause_id", length(subcause_id)),
                  gbd_id = subcause_id, location_id = location_id, year_id = 2010,
                  sex_id = sex_id, measure_id = 3, n_draws = n_draws, downsample = TRUE,
                  gbd_round_id = 7, decomp_step = "iterative", source = "como",
                  version_id = 932)
    )
    # reshape long by draw
    subcause_dt <- melt(subcause_dt, id.vars = c("cause_id", "measure_id", "location_id",
                                                 "age_group_id", "sex_id"),
                        measure.vars = paste0("draw_", 0:(n_draws - 1)),
                        variable.name = "draw", value.name = "subcause_burden")
    subcause_dt[, draw := as.numeric(gsub("draw_", "", draw))]
    # convert YLDs from rate to count space using current best GBD 2020 population
    subcause_dt <- merge(
        subcause_dt,
        get_population(location_id = location_id, year_id = 2010, sex_id = sex_id,
                       age_group_id = "all", gbd_round_id = 7, decomp_step = "iterative"
        )[, .(location_id, sex_id, age_group_id, population)],
        by = c("location_id", "sex_id", "age_group_id"))
    subcause_dt[measure_id == 3, subcause_burden := subcause_burden * population]
    # collapse to DALYs (YLLs + YLDs = DALYs)
    subcause_dt <- subcause_dt[, .(subcause_burden = sum(subcause_burden)),
                               by = c("cause_id", "location_id", "sex_id",
                                      "age_group_id", "draw")]

    return(subcause_dt)
}

#' For a mediator and cause and a set list of demographics, pull PAFs, cause
#' burden, and calculate proportion of PAF for each subcause
#'
#' @param med_id rei ID of risk to pull pull PAFs for
#' @param parent_cause_id cause ID of parent cause ID
#' @param subcause_id cause IDs of subcauses under parent cause ID
#' @param location_id location ID(s) to pull PAFs for
#' @param year_id year ID(s) to pull PAFs for
#' @param sex_id sex ID(s) to pull PAFs for
#' @param n_draws number of draws
#' @param gbd_round_id GBD round ID of PAFs
#' @param decomp_step decomp step of PAFs
#' @param cause_burden data.table of cause burden (DALYs) for parent and subcause
#'
#' @return data.table with columns cause_id, location_id, year_id, age_group_id,
#' sex_id, draw, mortality, morbidity, med_prop
get_mediator_subcause_proportions <- function(med_id, parent_cause_id, subcause_id, location_id,
                                              year_id, sex_id, n_draws, gbd_round_id,
                                              decomp_step, cause_burden) {
    # Pull PAFs for mediator risk, reshape long by draw
    med_dt <- get_draws(gbd_id_type = c("rei_id", rep("cause_id", length(subcause_id) + 1)),
                        gbd_id = c(med_id, parent_cause_id, subcause_id),
                        location_id = location_id, year_id = year_id,
                        sex_id = sex_id, n_draws = n_draws, downsample = TRUE,
                        gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                        source = "paf"
    )
    med_dt <- melt(med_dt, id.vars = c("cause_id", "measure_id", "location_id",
                                       "year_id", "age_group_id", "sex_id"),
                   measure.vars = paste0("draw_", 0:(n_draws - 1)),
                   variable.name = "draw", value.name = "med_paf")
    med_dt[, draw := as.numeric(gsub("draw_", "", draw))]
    med_dt[measure_id == 3, `:=` (mortality=0, morbidity=1)]
    med_dt[measure_id == 4, `:=` (mortality=1, morbidity=0)][, measure_id := NULL]
    # merge on cause burden for parent cause
    med_burden <- merge(med_dt[cause_id == parent_cause_id, ][, cause_id := NULL],
                        cause_burden,
                        by = c("location_id", "age_group_id", "sex_id", "draw"),
                        allow.cartesian=TRUE)
    # calculate risk attributable burden for parent cause
    med_burden[, med_parent_risk_attr := med_paf * parent_burden][, med_paf := NULL]
    # merge on cause burden for subcauses
    med_burden <- merge(med_burden, med_dt,
                        by = c("cause_id", "location_id", "year_id", "age_group_id",
                               "sex_id", "draw", "mortality", "morbidity"))
    # calculate proportion of attributable burden split to each subcause
    med_burden <- med_burden[, .(cause_id, location_id, year_id, age_group_id,
                                 sex_id, draw, mortality, morbidity,
                                 med_prop=(med_paf * subcause_burden) /
                                     med_parent_risk_attr)]
    return(med_burden)
}

#' Given PAF draws for a risk and aggregate cause, split the PAFs to some subset
#' of subcauses such that they still aggregate to the parent cause and respect
#' any risk - cause relationships that may be 100% attributable.
#'
#' @param rei_id rei ID of the risk to split PAFs
#' @param med_id rei ID of the risk that CKD is mediated through. May be the same as rei_id.
#' @param parent_cause_id cause ID of the parent cause present in PAFs
#' @param parent_dt data.table of PAF draws for the rei_id and parent_cause_id
#' @param gbd_round_id gbd round ID to determine cause hierarchy and possibly mediator PAFs
#' @param decomp_step decomp step to pull mediator PAFs
#'
#' @return data.table with columns location_id, age_group_id, sex_id, cause_id,
#' draw, mortality, morbidity, subcause_burden
split_parent_cause <- function(rei_id, med_id, parent_cause_id, parent_dt, gbd_round_id,
                               decomp_step) {

    # CLEAN UP PARENT CAUSE PAFS -----------------------------------------------
    parent_dt <- parent_dt[cause_id == parent_cause_id,
                           .(med_id, location_id, year_id, age_group_id, sex_id,
                             draw, morbidity, mortality, parent_paf=paf)]

    # PULL CAUSE SPECIFIC DALYS ----------------------------------------
    # find most-detailed causes under parent using computation cause set
    subcause_id <- get_cause_metadata(
        cause_set_id = 2, gbd_round_id = gbd_round_id
    )[path_to_top_parent %like% paste0(",", parent_cause_id, ",") &
          most_detailed == 1, ]$cause_id
    # if parent cause is CKD, for any risk other than FPG exclude the DM T1 subcause
    if (parent_cause_id == 589 & rei_id != 105) subcause_id <- setdiff(subcause_id, 997)
    # pull subcause DALYs for a single year using YLDs from COMO and YLLs from CoDCorrect
    cause_burden <- get_subcause_burden(
        subcause_id = subcause_id, location_id = unique(parent_dt$location_id),
        sex_id = unique(parent_dt$sex_id), n_draws = length(unique(parent_dt$draw))
    )

    # CALCULATE TOTAL PARENT CAUSE BURDEN --------------------------------------
    # sum cause burden across included subcauses to calculate parent cause burden
    cause_burden[, parent_burden := sum(subcause_burden),
                 by = c("location_id", "sex_id", "age_group_id", "draw")]

    # if CKD is mediated through another risk, pull mediator CKD PAFs
    if (!rei_id %in% med_id)
        med_burden <- lapply(med_id, function(x) get_mediator_subcause_proportions(
            med_id = x, parent_cause_id = parent_cause_id, subcause_id = subcause_id,
            location_id = unique(parent_dt$location_id),
            year_id = unique(parent_dt$year_id), sex_id = unique(parent_dt$sex_id),
            n_draws = length(unique(parent_dt$draw)), gbd_round_id = gbd_round_id,
            decomp_step = decomp_step, cause_burden
        )[, med_id := x]) %>% rbindlist(., use.names = TRUE)

    # merge on PAF for parent cause
    cause_burden <- merge(parent_dt, cause_burden,
                          by = c("location_id", "age_group_id", "sex_id", "draw"),
                          allow.cartesian=TRUE)
    # calculate total parent cause risk attributable burden
    cause_burden[, parent_risk_attr := parent_paf * parent_burden]

    # IF MEDIATED, SPLIT PARENT PAF USING SUBCAUSE PROPORTIONS FROM MEDIATOR ---
    if (!rei_id %in% med_id) {
        # expand cause_burden mortality/morbidity columns to match med_burden
        by_cols <- setdiff(names(cause_burden), c("mortality", "morbidity"))
        cause_burden <- rbind(cause_burden[!(mortality == 1 & morbidity == 1), ],
                              cause_burden[mortality == 1 & morbidity == 1,
                              .(mortality = c(0, 1), morbidity = c(1, 0)),
                              by = by_cols])

        cause_burden <- merge(cause_burden, med_burden,
                              by = c("med_id", "cause_id", "location_id", "year_id",
                                     "age_group_id", "sex_id", "draw", "mortality",
                                     "morbidity")
        )
        cause_burden[, subcause_risk_attr := med_prop * parent_risk_attr]
        cause_burden[, subcause_paf := subcause_risk_attr / subcause_burden]
        # sum across mediators to get total mediated PAF in case there are
        # multiple mediators
        cause_burden <- cause_burden[, .(paf=sum(subcause_paf)),
                                     by = .(location_id, year_id, age_group_id,
                                            sex_id, cause_id, draw, mortality,
                                            morbidity)]
        return(cause_burden)
    }

    # CALCULATE PAF FOR SUBCAUSES WITH FULL ATTRIBUTION TO RISK EXPOSURE -------
    # if parent is CKD: FPG and CKD due to DM T1 and T2, SBP and CKD due to hypertension
    if (parent_cause_id == 589 & rei_id == 105) {
        all_of_subcause <- c(997, 998)
    } else if (parent_cause_id == 589 & rei_id == 107) {
        all_of_subcause <- c(591)
    } else {
        all_of_subcause <- c()
    }
    # keep subcauses that are directly correlated with risk exposure
    fully_attrib <- cause_burden[cause_id %in% all_of_subcause, ]
    # sum cause burden across included subcauses
    fully_attrib[, full_parent_burden := sum(subcause_burden),
                 by = c("location_id", "year_id", "sex_id", "age_group_id", "draw",
                        "mortality", "morbidity")]
    # attribute as close to 100% of the subcause burden as present. if there's not
    # enough, proportionally split across subcause(s) if multiple.
    fully_attrib[(parent_risk_attr >= 0) & (full_parent_burden > parent_risk_attr),
                 subcause_risk_attr := (subcause_burden / full_parent_burden) * parent_risk_attr]
    fully_attrib[(parent_risk_attr >= 0) & (full_parent_burden <= parent_risk_attr),
                 subcause_risk_attr := subcause_burden]
    # if parent PAF is negative, don't allow these PAFs to go below zero
    fully_attrib[parent_risk_attr < 0, subcause_risk_attr := 0]
    # back-calculate sub-cause PAF
    fully_attrib[, subcause_paf := subcause_risk_attr / subcause_burden]

    # CALCULATE PAF FOR REMAINING SUBCAUSES ------------------------------------
    # keep causes that aren't directly correlated to risk exposure
    cause_burden <- cause_burden[!cause_id %in% all_of_subcause, ]
    cause_burden <- merge(cause_burden, fully_attrib[, .(full_risk_attr = sum(subcause_risk_attr)),
                                                     by = .(location_id, year_id, age_group_id, sex_id,
                                                            draw, mortality, morbidity, full_parent_burden)],
                          by = c("location_id", "year_id", "sex_id", "age_group_id", "draw",
                                 "mortality", "morbidity"))
    # subtract portion of attributable cause burden we've already assigned
    cause_burden[, parent_risk_attr := round(parent_risk_attr, 12) - round(full_risk_attr, 12)]
    # calculate remaining subcause PAFs from remaining burden
    cause_burden[, subcause_risk_attr := (subcause_burden / (parent_burden - full_parent_burden)) * parent_risk_attr]
    cause_burden[, subcause_paf := subcause_risk_attr / subcause_burden]

    return(rbind(fully_attrib, cause_burden, use.names = TRUE, fill = TRUE)[
        , .(location_id, year_id, age_group_id, sex_id, cause_id, draw, mortality,
            morbidity, paf=subcause_paf)]
    )
}
