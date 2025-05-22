#--------------------------------------------------------------------------------------
# Fit RR by age for metabolic mediators using data 
# Save draws of attenuation factor by mediator&outcome
##  Author: 
#  Save output as <rei_id>_<cause_id>
#--------------------------------------------------------------------------------------


# Setup ---------------------------
library(openxlsx)
library(data.table)
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")
source("FILEPATH/helper_functions.R")
source("FILEPATH/get_age_metadata.R")
# set seed
np <- import("numpy")
np$random$seed(as.integer(2738))

# Generate folder with today's date 
parent_dir <- "FILEPATH"
save_dir <- paste0(parent_dir,"/results/",gsub("-", "_", Sys.Date()), ".01/")
dir.create(save_dir)
pct_save_dir <- paste0(save_dir, "/attenuation_factor_draws/")
dir.create(pct_save_dir)
output_dir <- paste0(save_dir, "/output/")
dir.create(output_dir)
# ------------------------------------



# Run MR-BRT model and save all outputs --------------------------------

pdf(paste0(save_dir, "/vetting_plots_monospline_2iknots.pdf"), width = 11, height = 8)

# load in data and add a couple columns
data <- as.data.table(read.xlsx(paste0(parent_dir, "FILEPATH/extracted_data.xlsx")))

# Do everything in normal space and excess risk
data[, `:=` (mean = mean-1, lower = lower-1, upper=upper-1)]
data[, se := (upper-lower)/3.92]
data[, age_midpoint := age_start + (age_end - age_start)/2 ]

# Make cause id map and save
rei_cause_id_map <- unique(data[, .(risk_factor, outcome, rei_id, cause_id)])
add_hem <- rei_cause_id_map[cause_id==496][,cause_id:=497]
expand_tot_stroke <- rei_cause_id_map[cause_id==494][, cause_id := NULL]
expand_tot_stroke <- merge(expand_tot_stroke, data.table(rei_id = 141, cause_id = c(496,497,495)))
rei_cause_id_map <- rei_cause_id_map[cause_id!=494]
rei_cause_id_map <- rbind(rei_cause_id_map, add_hem, expand_tot_stroke)
write.csv(rei_cause_id_map, paste0(save_dir, "/rei_cause_id_map.csv"), row.names = F)

# Load ages for adding on gbd age group ids
ages <- get_age_metadata(19)
setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
ages <- ages[,.(age_start, age_end, age_group_id)]

# Fit model and plot for each risk-outcome pair
for(risk in unique(data$risk_factor)){
  for(out in unique(data[risk_factor == risk]$outcome)){  
    
    print(paste0(risk,"-",out))
    outdata <- data[risk_factor == risk & outcome == out]
    
    unit <- unique(outdata$units)
    
    # Prep data and covariates for mrbrt
    dat1 <- MRData()
    dat1$load_df(
      data = outdata,
      col_obs = "mean",
      col_obs_se = "se",
      col_covs = list("age_start", "age_end"),
      col_study_id = "study")
    
    N_I_KNOTS <- 2
    
    cov_models1 <- list(
      LinearCovModel("intercept", use_re = T),
      LinearCovModel(alt_cov = list("age_start", "age_end"), use_re = F,
                     use_spline = T,
                     spline_degree = 2L,
                     spline_knots_type = 'domain',
                     prior_spline_monotonicity = "decreasing",
                     spline_knots = array(seq(0, 1, length.out = N_I_KNOTS + 2)))
    )
    
    # Fit mr-brt model
    mod1 <- MRBRT(
      data = dat1,
      cov_models = cov_models1,
      inlier_pct = 1
    )
    
    mod1$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)
    
    # Save model
    py_save_object(object = mod1, 
                   filename = paste0(output_dir, risk, "_", out, ".pkl"), 
                   pickle = "dill")
    
    pred_data <- data.table("intercept"=c(1), "age_start" = seq(25, 99, by = 5), "age_end" = seq(30, 100, by = 5))
    
    
    dat_pred1 <- MRData()
    
    dat_pred1$load_df(
      data = pred_data,
      col_covs = list("age_start", "age_end")
    )
    
    # Generate draws and make plots --------------------------------------
    
    # Plot without uncertainity but with data
    pred_data$Y_mean <- mod1$predict(dat_pred1, sort_by_data_id = T)
    plot_agetrend_model(mod1, paste0(risk, "-", out, " (per ", unit, ")"))
    
    # Resample uncertainity (using the mean-gamma approach)
    sampling <- import("mrtool.core.other_sampling")
    num_samples <- 1000L
    beta_samples <- sampling$sample_simple_lme_beta(num_samples, mod1)
    gamma_samples <- rep(mod1$gamma_soln, num_samples) * matrix(1, num_samples)
    
    # # Draws without gamma
    draws3 <- mod1$create_draws(
      data = dat_pred1,
      beta_samples = beta_samples,
      gamma_samples = gamma_samples,
      random_study = FALSE)

    pred_data$Y_mean_fe <- apply(draws3, 1, function(x) mean(x))
    pred_data$Y_mean_lo_fe <- apply(draws3, 1, function(x) quantile(x, 0.025))
    pred_data$Y_mean_hi_fe <- apply(draws3, 1, function(x) quantile(x, 0.975))
    
    # Draws with gamma
    draws2 <- mod1$create_draws(
      data = dat_pred1,
      beta_samples = beta_samples,
      gamma_samples = gamma_samples,
      random_study = TRUE)
    
    pred_data$Y_mean_re <- apply(draws2, 1, function(x) mean(x))
    pred_data$Y_mean_lo_re <- apply(draws2, 1, function(x) quantile(x, 0.025))
    pred_data$Y_mean_hi_re <- apply(draws2, 1, function(x) quantile(x, 0.975))
    
    plot <- ggplot(pred_data, aes(x = age_start, y = Y_mean_fe))+geom_line()+
      geom_ribbon(aes(ymin = Y_mean_lo_fe, ymax = Y_mean_hi_fe), alpha = 0.3) + 
      theme_bw() + labs(x = "Age", y = "Excess risk w/o gamma", title = paste0(risk, "-", out, " (per ", unit, ")"), subtitle = paste0("Gamma = ", mod1$gamma_soln))
    print(plot)
    
    plot <- ggplot(pred_data, aes(x = age_start, y = Y_mean_re))+geom_line()+
      geom_ribbon(aes(ymin = Y_mean_lo_re, ymax = Y_mean_hi_re), alpha = 0.3) + 
      theme_bw() + labs(x = "Age", y = "Excess risk w/ gamma", title = paste0(risk, "-", out, " (per ", unit, ")"), subtitle = paste0("Gamma = ", mod1$gamma_soln))
    print(plot)
    
    # Add on gbd age groups
    pred_data[age_end==100, age_end:=125]
    pred_data <- merge(pred_data, ages, by = c("age_start", "age_end"))
    
    save_data <- pred_data[, .(age_start, age_end, age_group_id, Y_mean_re, Y_mean_lo_re, Y_mean_hi_re)]
    write.csv(save_data, paste0(output_dir, risk, "_", out, "_rr_summary.csv"), row.names = F)
    
    # Format draws without gamma
    draws3 <- as.data.table(draws3)
    setnames(draws3, colnames(draws3), paste0("draw_",0:999))
    
    y_draws <- cbind(pred_data[,.(age_start, age_end, age_group_id)], as.data.table(draws3))
    
    # Get attenuation factor across age groups
    draws_long <- melt(y_draws, id.vars = c("age_start", "age_end", "age_group_id"))
    
    reference_age_group <- draws_long[age_start==60, .(variable, value)]
    setnames(reference_age_group, "value", "ref_age_group_val")
    draws_long <- merge(draws_long, reference_age_group, by = "variable")
    
    # Calculate attenuation factor from excess RR space
    draws_long[, atten_factor := value/ref_age_group_val]
    pct_draws <- as.data.table(dcast(draws_long, formula = age_start +age_end + age_group_id ~ variable, value.var = "atten_factor", drop = T))
    
    # Visualize pre-adjustment
    pct_draws[, pct_mean := apply(.SD, 1, mean), .SDcols = paste0("draw_", 0:999)]
    pct_draws[, pct_upper := apply(.SD, 1, quantile, 0.975), .SDcols = paste0("draw_", 0:999)]
    pct_draws[, pct_lower := apply(.SD, 1, quantile, 0.025), .SDcols = paste0("draw_", 0:999)]
    
    summary <- pct_draws[, .(age_start, age_end, age_group_id, pct_mean, pct_upper, pct_lower)]
    pct_draws[, `:=` (pct_mean = NULL, pct_upper = NULL, pct_lower = NULL)]
    
    # 
    plot <- ggplot(summary, aes(x = age_start, y = pct_mean))+geom_line()+
      geom_ribbon(aes(ymin = pct_lower, ymax = pct_upper), alpha = 0.3) + 
      theme_bw() + labs(x = "Age", y = "Attenuation factor pre-adjustment", title = paste0(risk, "-", out, " (per ", unit, ")"))
    print(plot)
    
    # Visualize post-adjustment (holding AF constant below 35 and above 85)
    draws <- paste0("draw_",0:999)
    young_ages <- cbind(pct_draws[age_start < 35, .(age_start, age_end, age_group_id)],
                        pct_draws[age_start==35, c(draws), with = F])
    old_ages <- cbind(pct_draws[age_end > 85, .(age_start, age_end, age_group_id)],
                        pct_draws[age_end==85, c(draws), with = F])
    pct_draws <- pct_draws[age_start >=35 & age_end <= 85]
    
    pct_draws <- rbind(young_ages, pct_draws, old_ages)
    
    pct_draws[, pct_mean := apply(.SD, 1, mean), .SDcols = paste0("draw_", 0:999)]
    pct_draws[, pct_upper := apply(.SD, 1, quantile, 0.975), .SDcols = paste0("draw_", 0:999)]
    pct_draws[, pct_lower := apply(.SD, 1, quantile, 0.025), .SDcols = paste0("draw_", 0:999)]

    summary <- pct_draws[, .(age_start, age_end, age_group_id, pct_mean, pct_upper, pct_lower)]
    pct_draws[, `:=` (pct_mean = NULL, pct_upper = NULL, pct_lower = NULL)]

    #
    plot <- ggplot(summary, aes(x = age_start, y = pct_mean))+geom_line()+
      geom_ribbon(aes(ymin = pct_lower, ymax = pct_upper), alpha = 0.3) +
      theme_bw() + labs(x = "Age", y = "Attenuation factor post-adjustment", title = paste0(risk, "-", out, " (per ", unit, ")"))
    print(plot)
    
    
    write.csv(summary, paste0(output_dir, risk, "_", out, "_pct_summary.csv"), row.names = F)
    
    # Save draws to subfolder for easy modeler access
    saves <- rei_cause_id_map[risk_factor==risk & outcome == out]
    for(i in 1:nrow(saves)){
      
      pct_draws[, `:=` (cause_id = saves[i, cause_id], rei_id = saves[i, rei_id])]
      setcolorder(pct_draws, c("rei_id", "cause_id", "age_start", "age_end", "age_group_id"))
      write.csv(pct_draws, paste0(pct_save_dir, "/", 
                                  saves[i, rei_id], "_",saves[i, cause_id], ".csv"), row.names = F)
      
    }
    
    
  }
}
dev.off()

message(save_dir)



