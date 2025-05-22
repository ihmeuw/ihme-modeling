
adjust_row <- function(row) {
  # do not adjust group_review 0 rows
  row_group_review <- ifelse(is.na(row[, group_review]), 1, row[, group_review])
  if (row_group_review == 0) {
    return(row)
  }
  # do not adjust non-incidence rows
  if (row[, measure] != "incidence") {
    return(row)
  }
  # do not adjust reference rows
  if (row[, def] == "reference") {
    return(row)
  } else if (row[, def] == "_cv_marketscan_inp_2000") {
    
    # adjust the mean estimate: logit(mean_original) - (logit(alt) - logit(ref))
    logit_adj <- ms_2000_model_summary_dt[age_mid == row[, age_mid], logit_adj]
    row_logit_mean_adj <- logit(row[, mean]) - logit_adj
    row_mean_adj <- inv.logit(row_logit_mean_adj)
    row[, mean_adj := row_mean_adj]
    
    # adjust the standard error by
    # (1) Transform the original standard error into logit space using the delta method
    # (2) Let X be the distribution of logit(mean_original) and Y be the distribution of
    #     the logit difference (logit(alt) - logit(ref)).
    #     Assume that X and Y are  independent, so Var(X + Y) = Var(X) + Var(Y).
    # (3) Transform the adjusted standard error into linear space using the delta method
    row_se <- row[, standard_error]
    logit_adj_se <- ms_data_model_summary_dt[age_mid == row[, age_mid], logit_adj_se]
    row_logit_se <- sqrt((1 / (row[, mean] - row[, mean]^2))^2 * row_se^2)
    # row_logit_se_adj <- msm::deltamethod(~log(x1/(1-x1)), row_mean, row_se^2)
    row_logit_se_adj <- sqrt(row_logit_se^2 + logit_adj_se^2)
    row_se_adj <- sqrt((exp(row_logit_mean_adj) / (1 + exp(row_logit_mean_adj))^2)^2 
                       * row_logit_se_adj^2)
    # row_se_adj <- msm::deltamethod(~exp(x1)/(1+exp(x1)), row_logit_mean_adj, row_logit_se_adj^2)
    row[, se_adj := row_se_adj]
    
    # Add xwalk parent seq and note
    # row[, crosswalk_parent_seq:= seq][, seq := NA] DO NOT ADD: is added in sex split
    row[, note_modeler := paste0(note_modeler, " | xwalk to inpatient with alternative - reference logit difference of ", round(logit_adj, 2), " (", round(logit_adj_se, 2), ")")]
    return(row)
  } else if (row[, def] == "_cv_marketscan_data") {
    logit_adj <- ms_data_model_summary_dt[age_mid == row[, age_mid], logit_adj]
    # adjust the mean estimate: logit(mean_original) - (logit(alt) - logit(ref))
    row_logit_mean_adj <- logit(row[, mean]) - logit_adj
    row_mean_adj <- inv.logit(row_logit_mean_adj)
    row[, mean_adj := row_mean_adj]
    # adjust the standard error by
    # (1) Transform the original standard error into logit space using the delta method
    # (2) Let X be the distribution of logit(mean_original) and Y be the distribution of
    #     the logit difference (logit(alt) - logit(ref)).
    #     Assume that X and Y are  independent, so Var(X + Y) = Var(X) + Var(Y).
    # (3) Transform the adjusted standard error into linear space using the delta method
    row_se <- row[, standard_error]
    logit_adj_se <- ms_data_model_summary_dt[age_mid == row[, age_mid], logit_adj_se]
    row_logit_se <- sqrt((1/(row[, mean] - row[, mean]^2))^2 * row_se^2)
    # row_logit_se_adj <- deltamethod(~log(x1/(1-x1)), row_mean, row_se^2)
    row_logit_se_adj <- sqrt(row_logit_se^2 + logit_adj_se^2)
    row_se_adj <- sqrt((exp(row_logit_mean_adj)/ (1 + exp(row_logit_mean_adj))^2)^2 * row_logit_se_adj^2)
    # row_se_adj <- deltamethod(~exp(x1)/(1+exp(x1)), row_logit_mean_adj, row_logit_se_adj^2)
    row[, se_adj := row_se_adj]
    # Add xwalk parent seq and note
    # row[, crosswalk_parent_seq:= seq][, seq := NA] DO NOT ADD: is added in sex split
    row[, note_modeler := paste0(note_modeler, " | xwalk to inpatient with alternative - reference logit difference of ", round(logit_adj, 2), " (", round(logit_adj_se, 2), ")")]
    return(row)
  } else if (row[, def] == "_cv_surveillance") {
    # logit_adj <- surv_model_summary_dt[sex_id == row[, sex_id], logit_adj]
    # fix for not sex cov
    logit_adj <- as.numeric(surv_model_summary_dt[1,1])
    # adjust the mean estimate: logit(mean_original) - (logit(alt) - logit(ref))
    row_logit_mean_adj <- logit(row[, mean]) - logit_adj
    row_mean_adj <- inv.logit(row_logit_mean_adj)
    row[, mean_adj := row_mean_adj]
    # adjust the standard error by
    # (1) Transform the original standard error into logit space using the delta method
    # (2) Let X be the distribution of logit(mean_original) and Y be the distribution of
    #     the logit difference (logit(alt) - logit(ref)).
    #     Assume that X and Y are  independent, so Var(X + Y) = Var(X) + Var(Y).
    # (3) Transform the adjusted standard error into linear space using the delta method
    # logit_adj_se <- surv_model_summary_dt[sex_id == row[, sex_id], logit_adj_se]
    logit_adj_se <- as.numeric(surv_model_summary_dt[2,2])
    row_se <- row[, standard_error]
    row_logit_se <- sqrt((1/(row[, mean] - row[, mean]^2))^2 * row_se^2)
    # row_logit_se_adj <- deltamethod(~log(x1/(1-x1)), row_mean, row_se^2)
    row_logit_se_adj <- sqrt(row_logit_se^2 + logit_adj_se^2)
    row_se_adj <- sqrt((exp(row_logit_mean_adj)/ (1 + exp(row_logit_mean_adj))^2)^2 * row_logit_se_adj^2)
    # row_se_adj <- deltamethod(~exp(x1)/(1+exp(x1)), row_logit_mean_adj, row_logit_se_adj^2)
    row[, se_adj := row_se_adj]
    # Add xwalk parent seq and note
    # row[, crosswalk_parent_seq:= seq][, seq := NA] DO NOT ADD: is added in sex split
    row[, note_modeler := paste0(note_modeler, " | xwalk to inpatient with alternative - reference logit difference of ", round(logit_adj, 2), " (", round(logit_adj_se, 2), ")")]
    return(row)
  } else {
    return(row)
  }
}
