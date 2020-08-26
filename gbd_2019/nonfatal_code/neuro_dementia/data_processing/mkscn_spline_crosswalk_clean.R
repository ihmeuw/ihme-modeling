
# FUNCTIONS ---------------------------------------------------------------

graph_mrbrt_results <- function(results, predicts){
  data_dt <- as.data.table(results$train_data)
  model_dt <- as.data.table(predicts$model_summaries)
  data_dt[w == 0, excluded := 1][w > 0, excluded := 0]
  gg_subset <- ggplot() +
    geom_point(data = data_dt, aes(x = age, y = ldiff, color = as.factor(excluded))) +
    scale_fill_manual(guide = F, values = c("midnightblue", "purple")) +
    labs(x = "Age", y = "Logit Difference") +
    ggtitle(paste0("Meta-Analysis Results")) +
    theme_classic() +
    theme(text = element_text(size = 15, color = "black")) +
    geom_smooth(data = model_dt, aes(x = X_age, y = Y_mean), color = "black", se = F) +
    geom_ribbon(data = model_dt, aes(x = X_age, ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.05) +
    scale_color_manual(name = "", values = c("blue", "red"), 
                       labels = c("Included", "Trimmed"))
  return(gg_subset)
}

get_mkscn_adjustment <- function(input_data, model){
  dt <- copy(input_data)
  dt[, age := (age_start + age_end) / 2]
  pred_temp <- data.table(age = dt[, unique(age)])
  preds <- predict_mr_brt(model, newdata = pred_temp, write_draws = T)
  pred_dt <- as.data.table(preds$model_draws)
  pred_dt[, ladj := rowMeans(.SD), .SDcols = draws]
  pred_dt[, ladj_se := apply(.SD, 1, sd), .SDcols = draws]
  pred_dt[, c(draws, "Z_intercept", "X_intercept") := NULL]
  setnames(pred_dt, "X_age", "age")
  return(pred_dt)
}

adjust_mkscn <- function(data_dt, ratio_dt){
  dt <- copy(data_dt)
  dt[, age := (age_start + age_end) / 2]
  dt <- merge(dt, ratio_dt, by = "age", all.x = T)
  adjust_dt <- copy(dt)
  
  ## ADJUST MEANS
  adjust_dt[, lmean := qlogis(mean)]
  adjust_dt$l_se <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- as.numeric(adjust_dt[i, "mean"])
    se_i <- as.numeric(adjust_dt[i, "standard_error"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  adjust_dt[, lmean_adj := lmean - ladj]
  adjust_dt[, lmean_adj_se := sqrt(ladj_se^2 + l_se^2)]
  adjust_dt[, mean_adj := plogis(lmean_adj)]
  adjust_dt$standard_error_adj <- sapply(1:nrow(adjust_dt), function(i){
    mean_i <- as.numeric(adjust_dt[i, "lmean_adj"])
    se_i <- as.numeric(adjust_dt[i, "lmean_adj_se"])
    deltamethod(~exp(x1)/(1+exp(x1)), mean_i, se_i^2)
  })
  
  full_dt <- copy(adjust_dt)
  full_dt[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
  adjust_dt[, `:=` (mean = mean_adj, standard_error = standard_error_adj, upper = "", lower = "", seq = "", 
                    cases = "", sample_size = "", uncertainty_type_value = "", effective_sample_size = "", 
                    note_modeler = paste0(note_modeler, " | crosswalked with logit(difference): ", round(ladj, 2), " (", 
                                          round(ladj_se, 2), ")"))]
  adjust_dt[age_end == 124, age_end := 99]
  extra_cols <- setdiff(names(adjust_dt), names(data_dt))
  adjust_dt[, c(extra_cols) := NULL]
  return(list(adjusted = adjust_dt, vetting = full_dt))
}

mkscn_prediction_plot <- function(vetting_data){
  dt <- copy(vetting_data)
  dt <- merge(dt, loc_dt[, .(location_id, super_region_name)], by = "location_id")
  dt[, `:=` (N_adj = (mean_adj*(1-mean_adj)/standard_error_adj^2),
             N = (mean*(1-mean)/standard_error^2))]
  wilson_norm <- as.data.table(binconf(dt$mean*dt$N, dt$N, method = "wilson"))
  wilson_adj <- as.data.table(binconf(dt$mean_adj*dt$N_adj, dt$N_adj, method = "wilson"))
  dt[, `:=` (lower = wilson_norm$Lower, upper = wilson_norm$Upper,
             lower_adj = wilson_adj$Lower, upper_adj = wilson_adj$Upper)]
  dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  dt[, age_group := cut2(midage, ages)]
  gg_funct <- function(graph_dt){
    gg <- ggplot(graph_dt, aes(x = mean, y = mean_adj, color = as.factor(year_start), shape = as.factor(sex))) +
      geom_point() +
      facet_wrap(~measure+age_group, ncol = 5) +
      geom_errorbar(aes(ymin = lower_adj, ymax = upper_adj)) +
      geom_errorbarh(aes(xmin = lower, xmax = upper)) +
      scale_color_brewer(palette = "Spectral", name = "Year") + 
      labs(x = "Unadjusted means", y = "Adjusted means") +
      theme_classic()
    return(gg)
  }
  state_funct <- function(state_dt){
    gg <- gg_funct(graph_dt = state_dt) + ggtitle(state_dt[, unique(location_name)])
    return(gg)
  }
  state_plots <- lapply(dt[, unique(location_name)], function(x) state_funct(state_dt = dt[location_name == x]))
  return(state_plots)
}

# SPLINE MODEL ------------------------------------------------------------

message("running model")
mkscn_matches <- copy(matches[def == "reference" & def2 == "_cv_marketscan"])
mkscn_setup <- create_mrbrtdt(mkscn_matches, age = T)
mrbrt_mkscn_dt <- mkscn_setup[[1]]

mkscn_name <- paste0("dementia_mkscn_", date)

if (file.exists(paste0("FILEPATH"))){
  mkscn_model <- readr::read_rds(paste0("FILEPATH"))
} else {
  mkscn_model <- run_mr_brt(
    output_dir = mrbrt_dir,
    model_label = mkscn_name,
    data = mrbrt_mkscn_dt,
    mean_var = "ldiff",
    se_var = "ldiff_se",
    covs = list(cov_info("age"s, "X", degree = 3,
                         i_knots = c("70, 75, 80, 85, 90"),
                         bspline_gprior_mean = "0, 0, 0, 0, 0, 0", bspline_gprior_var = "1e-5, inf, inf, inf, inf, 1e-3")),
    study_id = "id",
    method = "remL"
  )
  readr::write_rds(mkscn_model, paste0("FILEPATH"))
}

pred_dt <- expand.grid(age = seq(42.5, 97.5, by = 5))

mkscn_preds <- predict_mr_brt(mkscn_model, newdata = pred_dt, write_draws = T)
mkscn_graph <- graph_mrbrt_results(results = mkscn_model, predicts = mkscn_preds)
ggsave(mkscn_graph, filename = paste0("FILEPATH"), width = 12)

# ADJUST MARKETSCAN -------------------------------------------------------

message("adjusting data")
mkscn_data <- predict_sex$final
mkscn_data <- mkscn_data[cv_marketscan == 1]
mkscn_adjust <- get_mkscn_adjustment(mkscn_data, mkscn_model)
  pred_dt <- as.data.table(preds$model_draws)
  mkscn_adjust <- as.data.table(read.csv(paste0("FILEPATH")))
  mkscn_adjust[, ladj := rowMeans(.SD), .SDcols = draws]
  mkscn_adjust[, ladj_se := apply(.SD, 1, sd), .SDcols = draws]
  mkscn_adjust[, c(draws, "Z_intercept", "X_intercept") := NULL]
  setnames(mkscn_adjust, "X_age", "age")
mkscn_adjusted <- adjust_mkscn(mkscn_data, mkscn_adjust)
pred_plots_mkscn <-mkscn_prediction_plot(mkscn_adjusted$vetting)

