##################################################
## Project: CVPDs
## Script purpose: Make funnel plot of mrbrt results
## Date: April 18, 2021
## Author: USERNAME
##################################################
pacman::p_load(data.table, openxlsx, ggplot2)


# ARGUMENTS -------------------------------------
diy_funnelplot <- function(model, cause){
  mod_data <- model$data$to_df()
  mod_data$preds <- model$predict(model$data)
  mod_data$resid <- mod_data$obs - mod_data$preds
  mod_data$weight <- as.factor(abs(model$w_soln))
  mod_data$row_num <- 1:length(mod_data$obs)
  f <- ggplot(mod_data, aes(x = resid, y = obs_se, col=as.factor(weight))) +
    geom_point(alpha = 0.3, aes(shape = as.factor(weight))) +
    geom_line(data = data.table(y_vals = (seq(0, max(mod_data$obs_se), 0.1)), x_vals = seq(0, max(mod_data$obs_se), 0.1)* 1.96), aes(x = x_vals, y = y_vals), col = "black") +
    geom_line(data = data.table(y_vals = (seq(0, max(mod_data$obs_se), 0.1)), x_vals = seq(0, max(mod_data$obs_se), 0.1)* -1.96), aes(x = x_vals, y = y_vals), col = "black") +
    geom_vline(xintercept = 0, linetype = "dashed") +
    scale_y_reverse("Standard error") +
    theme_bw() +
    scale_x_continuous("Log ratio") + # this is actually the residual in log space
    ggtitle(paste0(cause, " glb model trimming"))
  return(f)

}
