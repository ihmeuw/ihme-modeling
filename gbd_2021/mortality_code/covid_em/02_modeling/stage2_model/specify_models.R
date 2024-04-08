
#' Specify Model Formulas
#'
#' Generate a list of model formulas needed for different linear model.
#'
#' @param subset_list `[\character()\]` Names of models to return
#'
#' @return A named list of formulas
#' @export

specify_models <- function(subset_list = NULL) {

  formula_list <- list()

  formula_list$m1_cdr.death_rate_excess <- formula(
    log(death_rate_excess) ~
      log(cumulative_infections_lagged) +
      prop_pop_75plus +
      log(death_rate_covid) +
      log(crude_death_rate) +
      log(diabetes_death_rate) +
      idr_lagged +
      smoking_prevalence +
      gbd_inpatient_admis +
      log(hiv_death_rate) +
      HAQI +
      avg_abs_latitude
  )

  formula_list$all_covs.prep <- formula(
    log(death_rate_excess) ~
      prop_pop_75plus + mobility_lagged + idr_lagged + log(crude_death_rate) +
      log(cumulative_infections) + log(cumulative_infections_lagged) +
      crude_death_rate_sd_1990 + avg_abs_latitude +
      universal_health_coverage + gbd_diabetes +
      gbd_inpatient_admis + stars_bin_high + gbd_obesity + HAQI +
      hypertension_prevalence + smoking_prevalence +
      log(cirrhosis_death_rate) + log(ckd_death_rate) + log(cong_downs_death_rate) +
      log(cvd_death_rate) + log(cvd_pah_death_rate) + log(cvd_stroke_cerhem_death_rate) +
      log(endo_death_rate) + log(hemog_sickle_death_rate) + log(hemog_thalass_death_rate) +
      log(hiv_death_rate) + log(ncd_death_rate) + log(neo_death_rate) + log(neuro_death_rate) +
      log(resp_asthma_death_rate) + log(resp_copd_death_rate) + log(subs_death_rate) + log(diabetes_death_rate) +
      log(death_rate_covid) +
      0
  )


  if (!is.null(subset_list)) {

    formula_list <- formula_list[names(formula_list) %in% subset_list]

  }

  return(formula_list)

}

