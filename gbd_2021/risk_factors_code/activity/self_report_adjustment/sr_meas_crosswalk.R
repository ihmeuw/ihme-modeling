################################################################################
## DESCRIPTION ## MRBRT crosswalk model to adjust self-reported PA to measured PA
## INPUTS ## Extractions of SR PA to measured PA
## OUTPUTS ## Funnel plot and model object
## AUTHOR 
## DATE ## 
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
data_dir <- "FILEPATH"
save_dir <- "FILEPATH"

## LOAD DEPENDENCIES -----------------------------------------------------
source(paste0(code_dir, 'FILEPATH'))
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")
library(openxlsx)
source("FILEPATH")
# Set up meta vars
age_cov <- TRUE
age_spline <- FALSE
gpaq_ipaq <- TRUE
# Data ----
df <- fread(paste0(data_dir, "FILEPATH"))
if(gpaq_ipaq) df <- df[gpaq_ipaq == 1]

run_mrbrt <- function(male, female, both_sex, intercept, sex, age, age_spline = FALSE, inlier_perc = 0.9) {
  
  # Combinations of sex
  if(both_sex == TRUE & male == FALSE & female == FALSE){
    input_df <- copy(df)
    input_df[male == 0, female := 1]
    input_df[female == 0, male := 1]
  } else if (male == TRUE & both_sex == FALSE & female == FALSE) {
    input_df <- df[male == 0]
  } else if (male == TRUE & both_sex == TRUE & female == FALSE) {
    input_df <- df[male %in% c(0, 1)]
  } else if (female == TRUE & both_sex == FALSE & male == FALSE) {
    input_df <- df[female == 0]
  } else if (female == TRUE & both_sex == TRUE & male == FALSE) {
    input_df <- df[female %in% c(0, 1)]
  } else {
    stop(message("Invalid combinations provided."))
  }
  mrdata <- MRData()

  mrdata$load_df(
    data = input_df,
    col_obs = "log_diff",
    col_obs_se = "log_diff_se",
    col_covs = list("sex_cov", "age_mean", "male", "female", "percent_male"),
    col_study_id = "study_id"
  )
  
  if(intercept == TRUE & sex == FALSE & age == FALSE) {
    # Intercept only models
    cov_models <- list(
      LinearCovModel("intercept", use_re = TRUE)
      )
  } else if (intercept == TRUE & sex == TRUE & age == FALSE) {
    # Intercept and sex
    if (both_sex == TRUE & male == FALSE & female == FALSE) {
      # All data with sex cov
      cov_models <- list(
        LinearCovModel("intercept", use_re = TRUE),
        #LinearCovModel("percent_male", use_re = FALSE)
        LinearCovModel("male", use_re = FALSE),
        LinearCovModel("female", use_re = FALSE)
      )
    } else if (both_sex == TRUE & male == TRUE & female == FALSE) {
      # Male and both sex data 
      cov_models <- list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("male", use_re = FALSE)
      )
    } else if (both_sex == TRUE & male == FALSE & female == TRUE) {
      # Female and both sex data 
      cov_models <- list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("female", use_re = FALSE)
      )
    } else {
      stop(message("Invalid combinations provided."))
    }

  } else if (intercept == TRUE & sex == TRUE & age == TRUE) {
    # Intercept, sex, and age cov
    if(both_sex == TRUE & male == FALSE & female == FALSE) {
      cov_models <- list(
        LinearCovModel("intercept", use_re = TRUE),
        #LinearCovModel("sex_cov", use_re = FALSE),
        LinearCovModel("percent_male", use_re = FALSE),
        LinearCovModel("age_mean", use_re = FALSE)
      )
    } else if (both_sex == TRUE & male == TRUE & female == FALSE) {
      # Male and both sex data
      cov_models <- list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("male", use_re = FALSE),
        LinearCovModel("age_mean", use_re = FALSE)
      )
    } else if (both_sex == TRUE & male == FALSE & female == TRUE) {
      # Female and both sex data
      cov_models <- list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("female", use_re = FALSE),
        LinearCovModel("age_mean", use_re = FALSE)
      )
    } else {
      stop(message("Invalid combinations provided."))
    }
  } else if (intercept == TRUE & sex == FALSE & age == TRUE) {
    cov_models <- list(
      LinearCovModel("intercept", use_re = TRUE),
      LinearCovModel("age_mean", use_re = FALSE)
    )
  } else {
    stop(message("Invalid combinations provided."))
  }
  
  if(age_spline == TRUE) {
    cov_models <- append(cov_models, 
                         list(
                           LinearCovModel(
                             "age_mean",
                             use_re = F,
                             use_spline = TRUE,
                             spline_knots_type = "domain",
                             spline_degree = 2L,
                             spline_knots = array(c(seq(0, 1, length.out = 4))),
                             prior_spline_monotonicity = "increasing",
                             #spline_l_linear = TRUE,
                             spline_r_linear = TRUE
                           )
                         ))
  }
  
  model <- MRBRT(
    data = mrdata,
    cov_models = cov_models,
    inlier_pct = inlier_perc
  )
  
  #return(model)
  model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

  sampling <- import("mrtool.core.other_sampling")
  num_samples <- 1000L
  beta_samples <- sampling$sample_simple_lme_beta(num_samples, model)

  coef_df <- data.table(mean = as.numeric(model$beta_soln),
                        lower = apply(beta_samples, 2, quantile, probs = 0.025, na.rm = TRUE),
                        upper = apply(beta_samples, 2, quantile, probs = 0.975, na.rm = TRUE))

  row.names(coef_df) <- model$cov_model_names
  coef_df <- cbind(cov = rownames(coef_df), coef_df)

  out <- list(mod = model,
              coef = coef_df)

  return(out)
  
}

# Run combinations of models ----
all_data_int <- run_mrbrt(male = FALSE, female = FALSE, both_sex = TRUE, intercept = TRUE, sex = FALSE, age = FALSE)
all_data_int_sex <- run_mrbrt(male = FALSE, female = FALSE, both_sex = TRUE, intercept = TRUE, sex = TRUE, age = FALSE)
all_data_int_age <- run_mrbrt(male = FALSE, female = FALSE, both_sex = TRUE, intercept = TRUE, sex = FALSE, age = TRUE)
all_data_int_sex_age <- run_mrbrt(male = FALSE, female = FALSE, both_sex = TRUE, intercept = TRUE, sex = TRUE, age = TRUE)

male_only_int <- run_mrbrt(male = TRUE, female = FALSE, both_sex = FALSE, intercept = TRUE, sex = FALSE, age = FALSE)
male_only_int_age <- run_mrbrt(male = TRUE, female = FALSE, both_sex = FALSE, intercept = TRUE, sex = FALSE, age = TRUE)

male_int <- run_mrbrt(male = TRUE, female = FALSE, both_sex = TRUE, intercept = TRUE, sex = FALSE, age = FALSE)
male_int_sex <- run_mrbrt(male = TRUE, female = FALSE, both_sex = TRUE, intercept = TRUE, sex = TRUE, age = FALSE, inlier_perc = 1)
male_int_age <- run_mrbrt(male = TRUE, female = FALSE, both_sex = TRUE, intercept = TRUE, sex = FALSE, age = TRUE)
male_int_sex_age <- run_mrbrt(male = TRUE, female = FALSE, both_sex = TRUE, intercept = TRUE, sex = TRUE, age = TRUE)

female_only_int <- run_mrbrt(male = FALSE, female = TRUE, both_sex = FALSE, intercept = TRUE, sex = FALSE, age = FALSE)
female_only_int_age <- run_mrbrt(male = FALSE, female = TRUE, both_sex = FALSE, intercept = TRUE, sex = FALSE, age = TRUE)

female_int <- run_mrbrt(male = FALSE, female = TRUE, both_sex = TRUE, intercept = TRUE, sex = FALSE, age = FALSE)
female_int_sex <- run_mrbrt(male = FALSE, female = TRUE, both_sex = TRUE, intercept = TRUE, sex = TRUE, age = FALSE, inlier_perc = 1)
female_int_age <- run_mrbrt(male = FALSE, female = TRUE, both_sex = TRUE, intercept = TRUE, sex = FALSE, age = TRUE)
female_int_sex_age <- run_mrbrt(male = FALSE, female = TRUE, both_sex = TRUE, intercept = TRUE, sex = TRUE, age = TRUE)

# Plots and draws ----
sr_meas_age_plot <- function(df, data_df, pred, lower, upper, random_effects, sex, ylim = NULL) {
  
  if(random_effects) {
    title <- paste0("Log ratio of self-reported PA to measured PA in ", sex, ", random effects")
  } else {
    title <- paste0("Log ratio of self-reported PA to measured PA in ", sex, ", fixed effects only")
  }
  
  plot <- ggplot(data = df, aes(x = age_mean, y = get(pred))) +
    geom_line(color = "red", size = 1.1) +
    geom_ribbon(aes(ymin = get(lower), ymax = get(upper)), alpha = .3, size = 1.1, fill = "grey50") +
    geom_point(data = data_df, aes(x = age_mean, y = obs, color = as.factor(weight), shape = as.factor(weight), size = 1/obs_se))+
    scale_shape_manual(values = c(4, 20)) +
    scale_color_manual(values = c("red", "grey30")) +
    xlab("Age") +
    ylab("ln(SR/ACC)") +
    ggtitle(title) +
    theme_bw() +
    theme(legend.position = "none")
  
  if(!is.null(ylim)) plot <- plot + ylim(ylim)
  return(plot)
}

plot_dich_simple_funnel <- function(dat1, mod1, pred_data, rr_lab = "Relative Risk", plot_title = ""){
  
  #assemble data from dat1
  obs_data <- data.table("val" = dat1$obs , "se"= dat1$obs_se, "study" = dat1$study_id, "included" = mod1$w_soln)
  obs_data[, lower:=val-1.96*se]
  obs_data[, upper:=val+1.96*se]
  obs_data <- obs_data[order(val)]
  obs_data[, data:= 1]
  obs_data[included > 0 & included < 1, included:=0.5]
  # add results
  results <- data.table("val" = pred_data$mean, study = c("Result", "Result w/ gamma"), lower = c(pred_data$lo_fe, pred_data$lo_re), upper = c(pred_data$hi_fe, pred_data$hi_re) )
  results[,data:= 2]
  results[, included := 1]
  
  obs_data <- rbind(results, obs_data, fill = T)
  obs_data[, row := 1:nrow(obs_data)]
  
  cov_names <- mod1$cov_names[!mod1$cov_names=="intercept"]
  header <- paste0(as.character(mod1$data),"\ncovariates: ",paste0(cov_names, collapse=", ") ,"\nbeta: ", round(mod1$beta_soln[1], digits = 3),"     gamma: ", mod1$gamma_soln)
  
  # forest plot of the data
  color_vals <- c(16, 4, 8)
  names(color_vals) <- c(1,0, 0.5)
  
  # forest plot of the data
  p <- ggplot(data=obs_data,
              aes(y = se,x = val, size = 1/(se), shape = as.factor(included)))+
    geom_vline(xintercept =0, linetype=2, color = "red")+
    geom_point()+
    
    # add rectangles for the mr-brt results
    annotate("rect", xmin=obs_data[study == "Result", lower], xmax=obs_data[study == "Result", upper], ymin=0, ymax=Inf, alpha=0.2, fill="purple")+
    annotate("rect", xmin=obs_data[study == "Result w/ gamma", lower], xmax=obs_data[study == "Result w/ gamma", upper], ymin=0, ymax=Inf, alpha=0.2, fill="blue")+ 
    
    # add funnel plot lines mean +- 1.96*se
    geom_segment(aes(x = obs_data[study == "Result", val], y = 0, xend = obs_data[study == "Result", val]+1.96*obs_data[,max(se, na.rm=T)], yend = obs_data[,max(se, na.rm=T)], size = .01))+
    geom_segment(aes(x = obs_data[study == "Result", val], y = 0, xend = obs_data[study == "Result", val]-1.96*obs_data[,max(se, na.rm=T)], yend = obs_data[,max(se, na.rm=T)], size = .01))+
    
    ylab('standard error')+ xlab(paste0(rr_lab, ""))+
    labs(subtitle = header)+
    scale_shape_manual("", values = color_vals, guide = F) + 
    scale_size_continuous(guide = F)+
    scale_y_continuous(trans = "reverse")+theme_bw()
  
  if(plot_title != ""){
    p <- p+ labs(title= plot_title)
  }
  
  print(p)
  
}

plot_mrbrt <- function(model, sex, age, random_effects, sex_cov, percent_male) {
  if(age == TRUE & sex_cov == "male") {
    pred_df <- data.table("intercept" = 1, "age_mean" = seq(25, 70, by = 0.5), "male" = 0)
    df_draws <- data.table("intercept" = 1, "age_mean" = seq(25, 70, by = 0.5), "male" = 0)
  } else if(age == TRUE & sex_cov == "female") {
    pred_df <- data.table("intercept" = 1, "age_mean" = seq(25, 70, by = 0.5), "female" = 0)
    df_draws <- data.table("intercept" = 1, "age_mean" = seq(25, 70, by = 0.5), "female" = 0)
  } else if(age == TRUE & sex_cov == "both"){
    pred_df <- data.table("intercept" = 1, "percent_male" = percent_male, "age_mean" = seq(25, 70, by = 0.5))
    df_draws <- data.table("intercept" = 1, "percent_male" = percent_male, "age_mean" = seq(25, 70, by = 0.5))
  } else if(age == FALSE & sex_cov == "both"){
    
    if(missing(percent_male)) {
      pred_df <- data.table("intercept" = 1)
      df_draws <- data.table("intercept" = 1)
    } else {
      pred_df <- data.table("intercept" = 1, "percent_male" = percent_male) # Use if percent_male covariate was used
      df_draws <- data.table("intercept" = 1, "percent_male" = percent_male)
    }

  } else if(age == FALSE & sex_cov == "male"){
    pred_df <- data.table("intercept" = 1, "male" = 0)
    df_draws <- data.table("intercept" = 1, "male" = 0)
  } else if(age == FALSE & sex_cov == "female"){
    pred_df <- data.table("intercept" = 1, "female" = 0)
    df_draws <- data.table("intercept" = 1, "female" = 0)
  } else {
    stop(message("Invalid combinations"))
  }
  
  pred_data <- MRData()
  pred_data$load_df(
    data = pred_df,
    col_covs = as.list(names(pred_df))
  )
  
  n_samples <- 1000L
  samples <- model$sample_soln(sample_size = n_samples)
  
  draws_fe <- model$create_draws(
    data = pred_data,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = FALSE)
  
  pred_df$mean <- model$predict(pred_data)
  pred_df$lo_fe <- apply(draws_fe, 1, function(x) quantile(x, 0.025, na.rm = TRUE))
  pred_df$hi_fe <- apply(draws_fe, 1, function(x) quantile(x, 0.975, na.rm = TRUE))
  
  df_draws_fe <- cbind(df_draws, draws_fe) %>%
    setnames(., names(.), gsub("V", "draw_", names(.)))
  
  draws_re <- model$create_draws(
    data = pred_data,
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study = TRUE)
  
  pred_df$lo_re <- apply(draws_re, 1, function(x) quantile(x, 0.025, na.rm = TRUE))
  pred_df$hi_re <- apply(draws_re, 1, function(x) quantile(x, 0.975, na.rm = TRUE))
  
  df_draws_re <- cbind(df_draws, draws_re) %>%
    setnames(., names(.), gsub("V", "draw_", names(.)))
  
  if(age == TRUE) {
    model_data <- as.data.table(cbind(model$data$to_df(), model$w_soln)) %>%
      setnames(., "model$w_soln", "weight")
    
    if(random_effects == TRUE) {
      p <- sr_meas_age_plot(pred_df, model_data, "mean", "lo_re", "hi_re", random_effects = TRUE, sex = sex)
    } else {
      p <- sr_meas_age_plot(pred_df, model_data, "mean", "lo_fe", "hi_fe", random_effects = FALSE, sex = sex)
    }
  } else {
    model_data <- as.data.table(model$data$to_df())
    
    p <-
      plot_dich_simple_funnel(
        model_data,
        model,
        pred_df,
        rr_lab = "ln(RR)",
        plot_title = paste0("Ratio of Self-reported PA to accelerometer measured PA, ", sex)
      )
  }

  out <- list(fixed_effect_draws = df_draws_fe,
              random_effect_draws = df_draws_re,
              plot = p)
  return(out)

}

# Using an intercept model for all-ages, both-sexes, with no indicators for sex or age, 90% trimming
if(gpaq_ipaq) {
  gpaq_ipaq_model <- plot_mrbrt(all_data_int$mod, sex = "both", age = FALSE, random_effects = FALSE, sex_cov = "both")
} else {
  cohort_model <- plot_mrbrt(all_data_int$mod, sex = "both", age = FALSE, random_effects = FALSE, sex_cov = "both")
}

# Save ----

if(gpaq_ipaq) {
  pdf(paste0(save_dir, "FILEPATH"), width = 11, height = 7)
  print(gpaq_ipaq_model$plot)
  dev.off()
  
  py_save_object(all_data_int$mod, paste0(save_dir, "FILEPATH"), pickle = "dill")
} else {
  pdf(paste0(save_dir, "FILEPATH"), width = 11, height = 7)
  print(cohort_model$plot)
  dev.off()
  
  py_save_object(all_data_int$mod, paste0(save_dir, "FILEPATH"), pickle = "dill")
  
}
