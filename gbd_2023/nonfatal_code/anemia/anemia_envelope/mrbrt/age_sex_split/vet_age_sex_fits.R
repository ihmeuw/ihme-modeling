
# source libraries --------------------------------------------------------

library(data.table)
library(ggplot2)
library(reticulate)
library(mrbrt003)

# create a template df ----------------------------------------------------



high_age_df_temp <- mlh$split_input_location_levels(
  input_df = high_age_df,
  cascade_dir = file.path(
    "FILEPATH",
    paste0("age_sex_cascade_", current_var, "_", current_sex, "_", current_preg)
  ),
  hierarchy_col_names = hierarchy_col_names
)

high_age_df_pred_list <- lapply(high_age_df_list, \(x){
  predict_spline_cascade(
    fit = fit_super_region,
    newdata = x
  ) |> setDT()
})

high_age_df_cascade$super_region_id <- as.integer(high_age_df_cascade$super_region_id)
high_age_df_cascade$region_id <- as.integer(high_age_df_cascade$region_id)
high_age_df_cascade <- merge.data.table(
  x = high_age_df_cascade,
  y = location_df[,.(location_id, location_name)],
  by.x = 'super_region_id',
  by.y = 'location_id'
) |>
  setnames(old = 'location_name', new = 'super_region_name') |>
  merge.data.table(
    y = location_df[,.(location_id, location_name)],
    by.x = 'region_id',
    by.y = 'location_id'
  ) |>
  setnames(old = 'location_name', new = 'region_name') |>
  merge.data.table(
    y = location_df[,.(ihme_loc_id, location_name)],
    by = 'ihme_loc_id'
  ) |>
  setnames(old = 'location_name', new = 'Location')


if(interactive()){
  for(i in unique(high_age_df_cascade$region_name)){
    cascade_plot <- ggplot(high_age_df_cascade[region_name == i], aes(x = age_start, colour = Location)) +
      geom_point(aes(y = exp(log_mean_prev)), alpha = 0.3) +
      geom_line(aes(y = exp(pred)), linewidth = 1) +
      geom_line(aes(y = exp(pred_trans_mean)), colour = 'black', linewidth = 1.5, linetype = 'dashed') +
      theme_classic() +
      labs(title = paste("MR-BRT age/sex patterns with casecade for", current_var, "-", i),
           x="Age Start",
           y=if(grepl("hemog", current_var)) "Mean hemoglobin (g/L)" else "Anemia Prevalence") +
      theme(axis.line = element_line(colour = "black"))
    
    plot(cascade_plot)
  }
}

# supply prediction/fit metrics -------------------------------------------

high_age_df_cascade$point_diff_sq <- (high_age_df_cascade$pred - high_age_df_cascade$pred_trans_mean) ^ 2
rmse <- sqrt(sum(high_age_df_cascade$point_diff_sq) / nrow(high_age_df_cascade))

py$fit1 <- fit1
reticulate::py_run_string("
import numpy as np
soln_copy = np.array(fit1.lt.soln, copy=True)
print(fit1.lt.objective(x=soln_copy))
") 

high_age_df_cascade <- setDT(high_age_df_cascade)

rmse_df <- high_age_df_cascade[
  ,.(
    rmse = sqrt(sum(point_diff_sq) / .N)
  ),
  .(sex_id, age_start, super_region_id)
]

# create template of all combos and predict means and UI ------------------

prediction_df <- CJ(
  sex_id = 0:1,
  age_start = unique(high_age_df_cascade$age_start),
  super_region_id = unique(high_age_df_cascade$super_region_id)
)

prediction_df <- predict_spline_cascade(
  fit = fit_super_region,
  newdata = prediction_df
)


# format prediction df ----------------------------------------------------

prediction_df$sex_name <- ifelse(
  prediction_df$sex_id == 0,
  'Male',
  'Female'
)

prediction_df$super_region_id <- as.integer(prediction_df$super_region_id)
prediction_df <- merge.data.table(
  x = prediction_df,
  y = unique(high_age_df[,.(sex_id, age_start, pred_trans_mean)]),
  by = c('sex_id', 'age_start'), 
  all.x = TRUE
)

prediction_df <- merge.data.table(
  x = prediction_df,
  y = location_df[,.(location_id, location_name)],
  by.x = 'super_region_id',
  by.y = 'location_id'
) |>
  setnames(old = 'location_name', new = 'Super Region')

if(interactive()){
  cascade_plot <- ggplot(prediction_df, aes(x = age_start, colour = `Super Region`)) +
    geom_line(aes(y = exp(pred)), linewidth = 1) +
    geom_line(aes(y = exp(pred_trans_mean)), linewidth = 1.5, colour = 'black') +
    theme_classic() +
    labs(title = paste("MR-BRT age/sex patterns with casecade for", current_var),
         x="Age Start",
         y=if(grepl("hemog", current_var)) "Mean hemoglobin (g/L)" else "Anemia Prevalence") +
    theme(axis.line = element_line(colour = "black")) +
    facet_wrap('sex_name')
  
  plot(cascade_plot)
}
