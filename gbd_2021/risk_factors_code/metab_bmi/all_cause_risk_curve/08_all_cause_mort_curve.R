# Script to make all cause mortality curves

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- "FILEPATH"

library(dplyr)
source(paste0(code_dir, "FILEPATH")) # Loads functions like add_ui and get_knots
library(ggplot2)
library(data.table)
source("FILEPATH")
source("FILEPATH")
invisible(sapply(list.files("FILEPATH", full.names = T), source))
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, "FILEPATH"))
cause_ids <- get_ids("cause")

# Set up arguments
args <- commandArgs(trailingOnly = TRUE)

random_effects <- args[1]
weights <- args[2]
lower_bound <- args[3]
fisher <- args[4]
gamma_param_draws <- as.logical(args[5])

sex.id <- 3
percentiles <- c("025","05", "10", "15", "50", "85", "90", "95", "975")
ages <- c(9:20, 30:32, 235)
ref_covs <- c("a_0", "a_1")
alt_covs <- c("b_0", "b_1")
# Make vector of cause IDS to make curve
cause_dirs <- list.dirs(paste0(work_dir, "results"), recursive = FALSE)
causes <- list.dirs(paste0(work_dir, "results"), recursive = FALSE, full.names = FALSE) %>%
  gsub("cause_id_", "", .) %>%
  as.numeric(.)
causes <- causes[!is.na(causes)]

# dropping these demo cols
drop_cols <- c("location_id", "age_group_id", "year_id", "metric_id", "measure_id", "sex_id")

if(weights == "deaths") {
  ##### Pull draws of mortality estimates from GBD 2019 -----
  # No mortality for 628 (osteoarthritis), 630 (Low back pain), 632 (gout), 671 (cataract)
  burd <- get_draws(gbd_id_type = "cause_id",
                    gbd_id = causes[!causes %in% c(494, 429, 4299, 465, 435)],
                    year_id = 2020,
                    measure_id = 1,
                    metric_id = 1,
                    location_id = 1,
                    age_group_id = 37, # ages 20+
                    sex_id = sex.id,
                    source = "codcorrect",
                    decomp_step ="iterative",
                    version_id = 244,
                    gbd_round_id = 7) %>%
    .[, c(drop_cols) := NULL] %>%
    melt(., id.vars = "cause_id") %>%
    setnames(., "value", "burden")
  
  # Pulling draws for children of hemorrhagic stroke
  # Intracerebral hemorrhage
  intr_burd <- get_draws(gbd_id_type = "cause_id",
                         gbd_id = 496,
                         year_id = 2020,
                         measure_id = 1,
                         metric_id = 1,
                         location_id = 1,
                         age_group_id = 37, # ages 20+
                         sex_id = sex.id,
                         source = "codcorrect",
                         decomp_step ="iterative",
                         version_id = 244,
                         gbd_round_id = 7) %>%
    .[, c(drop_cols) := NULL] %>%
    melt(., id.vars = "cause_id") %>%
    setnames(., "value", "intr_value")
  
  # Subarachnoid hemorrhage
  sub_burd <- get_draws(gbd_id_type = "cause_id",
                        gbd_id = 497,
                        year_id = 2020,
                        measure_id = 1,
                        metric_id = 1,
                        location_id = 1,
                        age_group_id = 37, # ages 20+
                        sex_id = sex.id,
                        source = "codcorrect",
                        decomp_step ="iterative",
                        version_id = 244,
                        gbd_round_id = 7) %>%
    .[, c(drop_cols) := NULL] %>%
    melt(., id.vars = "cause_id") %>%
    setnames(., "value", "sub_value")
  
  # Merge to get mortality for hemorrhagic stroke
  hem_burd <- merge(intr_burd, sub_burd, by = "variable") %>%
    .[, burden := intr_value + sub_value] %>%
    .[, c("sub_value", "intr_value", "cause_id.x", "cause_id.y") := NULL] %>%
    .[, cause_id := 494]
  
  # Pulling draws for female burden
  if(sex.id %in% c(2,3)) {
    bc_burd <- get_draws(gbd_id_type = "cause_id",
                         gbd_id = 429,
                         year_id = 2020,
                         measure_id = 1,
                         metric_id = 1,
                         location_id = 1,
                         #age_group_id = 329,
                         sex_id = 2, # Because PAF is only for females, only use female deaths
                         source = "codcorrect",
                         decomp_step ="iterative",
                         version_id = 244,
                         gbd_round_id = 7)
    
    # Separate deaths for premenopausal breast cancer (<50 years)
    pre_bc_burd <- bc_burd[age_group_id %in% c(9:14)] %>%
      melt(., id.vars = c(setdiff(names(bc_burd), paste0("draw_", 0:999)))) %>%
      .[, burden := sum(value), by = "variable"]
    pre_bc_burd <- unique(pre_bc_burd[,.(cause_id, variable, burden)])
    
    # Post menopausal breast cancer (>50 years)
    post_bc_burd <- bc_burd[!age_group_id %in% c(8:14)] %>%
      melt(., id.vars = c(setdiff(names(bc_burd), paste0("draw_", 0:999)))) %>%
      .[, burden := sum(value), by = "variable"]
    post_bc_burd <- unique(post_bc_burd[,.(variable, burden)]) %>%
      .[, cause_id := 4299]
    
    ovar_uter_burd <- get_draws(gbd_id_type = c("cause_id","cause_id"),
                                gbd_id = c(435, 465),
                                year_id = 2020,
                                measure_id = 1,
                                metric_id = 1,
                                location_id = 1,
                                age_group_id = 37,
                                sex_id = sex.id,
                                source = "codcorrect",
                                decomp_step ="iterative",
                                version_id = 253,
                                gbd_round_id = 7) %>%
      .[, c(drop_cols) := NULL] %>%
      melt(., id.vars = "cause_id") %>%
      setnames(., "value", "burden")
    
    burd <- rbind(burd, pre_bc_burd, post_bc_burd, ovar_uter_burd, use.names = TRUE)
  }
  
  
  # Append mortality estimates
  burden <- rbind(burd, hem_burd)
  
  # Change name of draw_0 to draw_1000
  burden[variable == "draw_0", variable := "draw_1000"]
  burden$variable <- as.character(burden$variable)
  
} else if(weights == "dalys") {
  
  burd <- get_draws(gbd_id_type = "cause_id",
                    gbd_id = causes[!causes %in% c(494, 429, 4299, 465, 435)],
                    year_id = 2019,
                    measure_id = 2,
                    metric_id = 1,
                    location_id = 1,
                    age_group_id = 37,
                    sex_id = sex.id,
                    source = "dalynator",
                    decomp_step ="step4",
                    version_id = 47,
                    gbd_round_id = 6) %>%
    .[, c(drop_cols) := NULL] %>%
    melt(., id.vars = "cause_id") %>%
    setnames(., "value", "burden")
  
  # Pulling draws for children of hemorrhagic stroke
  # Intracerebral hemorrhage
  intr_burd <- get_draws(gbd_id_type = "cause_id",
                         gbd_id = 496,
                         year_id = 2019,
                         measure_id = 2,
                         metric_id = 1,
                         location_id = 1,
                         age_group_id = 37,
                         sex_id = sex.id,
                         source = "dalynator",
                         decomp_step ="step4",
                         version_id = 47,
                         gbd_round_id = 6) %>%
    .[, c(drop_cols) := NULL] %>%
    melt(., id.vars = "cause_id") %>%
    setnames(., "value", "intr_value")
  
  # Subarachnoid hemorrhage
  sub_burd <- get_draws(gbd_id_type = "cause_id",
                        gbd_id = 497,
                        year_id = 2019,
                        measure_id = 2,
                        metric_id = 1,
                        location_id = 1,
                        age_group_id = 37,
                        sex_id = sex.id,
                        source = "dalynator",
                        decomp_step ="step4",
                        version_id = 47,
                        gbd_round_id = 6) %>%
    .[, c(drop_cols) := NULL] %>%
    melt(., id.vars = "cause_id") %>%
    setnames(., "value", "sub_value")
  
  # Merge to get mortality for hemorrhagic stroke
  hem_burd <- merge(intr_burd, sub_burd, by = "variable") %>%
    .[, burden := intr_value + sub_value] %>%
    .[, c("sub_value", "intr_value", "cause_id.x", "cause_id.y") := NULL] %>%
    .[, cause_id := 494]
  
  # Pulling draws for premenopausal and postmenopausal breast cancer
  if(sex.id == 2) {
    bc_burd <- get_draws(gbd_id_type = "cause_id",
                         gbd_id = 429,
                         year_id = 2019,
                         measure_id = 2,
                         metric_id = 1,
                         location_id = 1,
                         sex_id = sex.id,
                         source = "dalynator",
                         decomp_step ="step4",
                         version_id = 47,
                         gbd_round_id = 6)
    
    # Separate DALYs for premenopausal breast cancer (20 < age < 50 years)
    pre_bc_burd <- bc_burd[age_group_id %in% c(9:14)] %>%
      melt(., id.vars = c(setdiff(names(bc_burd), paste0("draw_", 0:999)))) %>%
      .[, burden := sum(value), by = "variable"]
    pre_bc_burd <- unique(pre_bc_burd[,.(cause_id, variable, burden)])
    
    # Post menopausal breast cancer (>50 years)
    post_bc_burd <- bc_burd[age_group_id %in% c(15:20, 30, 31, 32, 235)] %>%
      melt(., id.vars = c(setdiff(names(bc_burd), paste0("draw_", 0:999)))) %>%
      .[, burden := sum(value), by = "variable"]
    post_bc_burd <- unique(post_bc_burd[,.(variable, burden)]) %>%
      .[, cause_id := 4299]
    
    ovar_uter_burd <- get_draws(gbd_id_type = c("cause_id","cause_id"),
                                gbd_id = c(435, 465),
                                year_id = 2019,
                                measure_id = 2,
                                metric_id = 1,
                                location_id = 1,
                                age_group_id = 37,
                                sex_id = sex.id,
                                source = "dalynator",
                                decomp_step ="step4",
                                version_id = 47,
                                gbd_round_id = 6) %>%
      .[, c(drop_cols) := NULL] %>%
      melt(., id.vars = "cause_id") %>%
      setnames(., "value", "burden")
    
    burd <- rbind(burd, pre_bc_burd, post_bc_burd, ovar_uter_burd, use.names = TRUE)
  }
  
  # Append mortality estimates
  burd <- rbind(burd, hem_burd, use.names = TRUE)
  
  # Change name of draw_0 to draw_1000
  burden[variable == "draw_0", variable := "draw_1000"]
  burden$variable <- as.character(burden$variable)
} else {
  stop(message("Weights of type ", weights, " not supported."))
}

##### Pull in RR draws -----
temp_exp <- fread("FILEPATH")
exposure <- temp_exp$exposure

# Set up table for relative filepaths
if(gamma_param_draws) {
  cause_temp <- data.table(cause_id = causes)
  cause_temp <- merge(cause_temp, cause_ids[,.(cause_id, acause)], all.x = T)
  cause_temp[cause_id == 429, acause := "neo_breast_premenopause"]
  cause_temp[cause_id == 4299, acause := "neo_breast_postmenopause"]
  cause_temp[cause_id == 494, acause := "cvd_stroke_hemmorhage"]
  cause_temp[, folder := paste0("metab_bmi_adult_", acause)]
  setnames(cause_temp, "cause_id", "cause.id")
}

rr_draws <- rbindlist(lapply(causes, function(c) {
  
  print(c)
  if(gamma_param_draws == TRUE) {
    # Load in model objects
    acause_dir <- cause_temp[cause.id == c, folder]
    data_dir <- paste0("FILEPATH", acause_dir, "/")
    
    signal_model <- py_load_object(paste0(data_dir, "signal_model.pkl"), pickle = "dill")
    linear_model <- py_load_object(paste0(data_dir, "new_linear_model.pkl"), pickle = "dill")
    # Determine when exposure values below and above cut points
    trim_exp <- exposure[exposure >= lower_bound]
    low_exp <- exposure[exposure < lower_bound]
    
    # Create draws
    ln_draws <- get_ln_rr_draws(signal_model,
                                linear_model,
                                risk = trim_exp,
                                num_draws = 1000L,
                                normalize_to_tmrel = FALSE)
    
    print(paste0("cause ID ", c, " has a range of RR draws: ", range(ln_draws[, grep("draw_", names(ln_draws), value = T)])))
    df_draws_re <- as.data.table(ln_draws)
    setnames(df_draws_re, "risk", "exposure")
    
    # Outlier and normalize to tmrel
    df_draws_re <- as.data.table(df_draws_re)
    
    min_exp <- min(df_draws_re$exposure)
    draws <- copy(df_draws_re)
    
  } else {
    
  
  # Read in models
  signal_model <- py_load_object(paste0(work_dir, "FILEPATH", c, "/FILEPATH/step1_signal_model.pkl"), pickle = "dill")
  final_model <- py_load_object(paste0(work_dir, "FILEPATH", c, "/FILEPATH/step4_signal_model.pkl"), pickle = "dill")
  
  # Trim the exposure depending on the cutpoint
  trim_exp <- exposure[exposure > lower_bound]
  min_exp <- min(trim_exp)
  
  if(fisher == FALSE) {
    # This method was using the old parameterization of gamma (without fisher or new RE estimation)
    # Set seed
    np <- import("numpy")
    np$random$seed(as.integer(3197))
    set.seed(3197)
    # Sample the final model
    sampling <- import("mrtool.core.other_sampling")
    num_samples <- 1000L
    beta_samples <- sampling$sample_simple_lme_beta(num_samples, final_model)
    gamma_samples <- rep(final_model$gamma_soln, num_samples) * matrix(1, num_samples)
    
    # Predict out signal with the fixed reference
    df_signal_pred <- data.table(a_0 = min_exp,
                                 a_1 = min_exp,
                                 b_0 = trim_exp,
                                 b_1 = trim_exp)
    
    data_signal_pred <- MRData()
    data_signal_pred$load_df(
      data = df_signal_pred,
      col_covs = as.list(names(df_signal_pred))
    )
    
    signal_pred <- signal_model$predict(data_signal_pred)
    
    # Predict out the final model
    df_final_pred <- data.table(exposure = trim_exp,
                                signal = signal_pred)
    
    if(final_model$num_cov_models != 1){
      for(cov in setdiff(final_model$cov_model_names, "signal")){
        
        df_final_pred[, paste0(cov) := 0]
        
      }
    }
    
    data_final_pred <- MRData()
    data_final_pred$load_df(
      data = df_final_pred,
      col_covs = as.list(setdiff(names(df_final_pred), "exposure"))
    )
    
    draws <- final_model$create_draws(
      data = data_final_pred,
      beta_samples = beta_samples,
      gamma_samples = gamma_samples,
      random_study = random_effects
    )
    
    
  } else {
    # This method included the uncertainty from Fisher information boost
    # Set seed
    np <- import("numpy")
    np$random$seed(as.integer(3197))
    set.seed(3197)
    data_info <- extract_data_info(signal_model,
                                   final_model,
                                   ref_covs = ref_covs,
                                   alt_covs = alt_covs,
                                   exposure_vec = trim_exp,
                                   num_points = length(trim_exp))
    
    draws <- get_fisher_draws(data_info, final_model)  
    
    print(paste0("cause ID ", c, " has a range of RR draws: ", range(exp(draws[, grep("draw_", names(draws), value = T)]))))
  }
  }
  
  df_draws <- data.table(
    cause_id = c,
    b_0 = trim_exp,
    a_0 = min_exp,
    a_1 = min_exp
  ) %>%
    cbind(., draws)
  
  if(fisher) setnames(df_draws, "draw_0", "draw_1000")
  df_draws$cause_id <- as.numeric(df_draws$cause_id)
  if(sum(is.na(df_draws)) != 0) warning(message("Some NAs returned for cause ID ", c))
  return(df_draws)
  
}), use.names = TRUE)

# Function to determine all cause TMREL (bottom of curve)
calc_all_cause_tmrel <- function(rr_draws, mad_outlier_tmrel = TRUE, number_mads = 6) {
  
  rr_long <- melt(rr_draws, id.vars = setdiff(names(rr_draws), grep("draw_", names(rr_draws), value = T)), variable.factor = FALSE)
  rr_long$exposure <- NULL
  ##### Generate all-cause estimates
  # Merge on deaths
  rr_long <- merge(rr_long, burden, by = c("cause_id", "variable"), all.y = TRUE)
  rr_long[, all_cause_rr := weighted.mean(x = value, w = burden), by = c("b_0", "variable")] # Take weighted mean of RRs
  
  # Get unique all cause dataframe
  all_cause_rr <- unique(rr_long[,.(b_0, a_0, all_cause_rr, variable)])
  all_cause_rr[, min_rr := min(all_cause_rr), by = "variable"]
  # Reshape as a wide df for draws of all-cause
  rr_wide <- data.table::dcast(all_cause_rr[, -("min_rr")], b_0 ~ variable, value.var = "all_cause_rr")
  # Pull out vector of 1000 tmrel draws
  tmrel_long <- all_cause_rr[all_cause_rr == min_rr, .(a_0, b_0, variable)]
  tmrel_wide <- data.table::dcast(tmrel_long, a_0 ~ variable, value.var = "b_0")
  
  orig_rr_wide <- copy(rr_wide)
  orig_tmrel_wide <- copy(tmrel_wide)
  
  if (mad_outlier_tmrel) {
    tmrel_long[, median := median(b_0)]
    tmrel_long[, mad := mad(b_0)]
    tmrel_long[, drop := ifelse(abs(b_0 - median) / mad > number_mads, 1, 0)]
    
    drop_draws <-
      as.character(unique(tmrel_long[drop == 1]$variable))
    print(paste0(
      "Dropping ",
      length(drop_draws),
      " draws that are greater than ",
      number_mads,
      " MADs."
    ))
    
    if (length(drop_draws) != 0) {
      set.seed(3197)
      # Outlier the tmrel draws first
      tmrel_wide[, c(drop_draws) := NULL]
      draw_options <- grep("draw_", names(tmrel_wide), value = T)
      new_draws <-
        sample(draw_options, length(drop_draws), replace = T)
      add_draws <- tmrel_wide[, new_draws, with = F]
      colnames(add_draws) <- drop_draws
      tmrel_wide <- cbind(tmrel_wide, add_draws)
      
      # Outlier all-cause risk curve draws
      rr_wide[, c(drop_draws) := NULL]
      add_draws <- rr_wide[, new_draws, with = F]
      colnames(add_draws) <- drop_draws
      rr_wide <- cbind(rr_wide, add_draws)
      
      # Create new long df
      tmrel_long <- melt(tmrel_wide, id.vars = "a_0")
      
    }
  } else {
    tmrel_long <- melt(tmrel_wide, id.vars = "a_0")
  }
  

  # Get different vals of tmrel
  tmrel_df <- data.table(
    ref_exposure = min(all_cause_rr$b_0),
    random_effect_draws = random_effects,
    fisher_draws = fisher,
    new_gamma_draws = gamma_param_draws,
    mad_outlier = mad_outlier_tmrel,
    mad_outlier_cutoff = number_mads,
    weight_type = weights,
    tmrel_min = min(tmrel_long$value),
    tmrel_max = max(tmrel_long$value),
    tmrel_mean = mean(tmrel_long$value)
  )
  
  lapply(percentiles, function(p) {
    tmrel_df[, paste0("tmrel_", p) := unname(quantile(tmrel_long$value, probs = as.numeric(paste0(".", p))))]
  })
  
  # Normalize and then reshape long for plotting
  plot_data <- summarize_draws(rr_wide)

  out <- list(tmrel = tmrel_df,
              orig_tmrel_draws = orig_tmrel_wide,
              tmrel_draws = tmrel_wide,
              orig_rr_draws = orig_rr_wide,
              rr_draws = rr_wide,
              plot_df = plot_data)
  
  return(out)
}

all_cause_list <- calc_all_cause_tmrel(rr_draws, mad_outlier_tmrel = FALSE)
if(gamma_param_draws) {
  sex <- ifelse(sex.id == 1, "males", ifelse(sex.id == 2, "females", "both"))
  pdf(paste0(work_dir, "FILEPATH", lower_bound, "_", weights, "_", sex, "_no_trim.pdf"), width = 11, height = 7)
  
  all_cause_tmrel_hist(all_cause_list$tmrel_draws, 
                       all_cause_list$orig_tmrel_draws, 
                       plot_title = paste0("Distribution of BMI TMREL (", sex, ")"),
                       xlab = "BMI (kg/m^2)")
  
  all_cause_draws_plot(all_cause_list$rr_draws, 
                       all_cause_list$orig_rr_draws, 
                       xlab = "BMI (kg/m^2)", 
                       plot_title = paste0("All-cause risk curve (", sex, ")"))
  
  dev.off()
} else {
  if(fisher == FALSE) {
    
    if(random_effects){
    
      pdf(paste0(work_dir, "FILEPATH", lower_bound, "_", weights, ".pdf"), width = 13.5, height = 6)
      
      # Plot in log space
      dr_plot(all_cause_list$plot_df, mean = "ln_pred", uncertainty = F, lower = "ln_pred_lo", upper = "ln_pred_hi",
              ylabel = "ln(RR)", caption = paste0("Bottom of curve ",round(all_cause_list$tmrel$tmrel_mean, 2)),
              log_space = T, sub.title = paste0("Weighted by ", weights, " (with random effects)"))
      
      dr_plot(all_cause_list$plot_df, mean = "ln_pred", uncertainty = T, lower = "ln_pred_lo", upper = "ln_pred_hi",
              ylabel = "ln(RR)", caption = paste0("Bottom of curve ", 
                                                  round(all_cause_list$tmrel$tmrel_mean, 3),
                                                  "\n range of ", round(all_cause_list$tmrel$tmrel_min, 3),
                                                  " - ", round(all_cause_list$tmrel$tmrel_max, 3),
                                                  "\n 95% UI (", round(all_cause_list$tmrel$tmrel_025, 3),
                                                  ", ", round(all_cause_list$tmrel$tmrel_975, 3), ")"),
              log_space = T, sub.title = paste0("Weighted by ", weights, " (with random effects)"))
      
      # Plot in normal space
      dr_plot(all_cause_list$plot_df, mean = "pred", uncertainty = F, lower = "pred_lo", upper = "pred_hi",
              ylabel = "RR", caption = paste0("Bottom of curve ",round(all_cause_list$tmrel$tmrel_mean, 2)),
              log_space = F, sub.title = paste0("Weighted by ", weights, " (with random effects)"))
      
      dr_plot(all_cause_list$plot_df, mean = "pred", uncertainty = T, lower = "pred_lo", upper = "pred_hi",
              ylabel = "RR", caption = paste0("Bottom of curve ", 
                                              round(all_cause_list$tmrel$tmrel_mean, 3),
                                              "\n range of ", round(all_cause_list$tmrel$tmrel_min, 3),
                                              " - ", round(all_cause_list$tmrel$tmrel_max, 3),
                                              "\n 95% UI (", round(all_cause_list$tmrel$tmrel_025, 3),
                                              ", ", round(all_cause_list$tmrel$tmrel_975, 3), ")"),
              log_space = F, sub.title = paste0("Weighted by ", weights, " (with random effects)"))
      
      dev.off()
    
  } else {
    
    pdf(paste0(work_dir, "FILEPATH", lower_bound, "_", weights, ".pdf"), width = 13.5, height = 6)
    
    # plot in log space
    dr_plot(all_cause_list$plot_df, mean = "ln_pred", uncertainty = F, lower = "ln_pred_lo", upper = "ln_pred_hi",
            ylabel = "ln(RR)", caption = paste0("Bottom of curve ",round(all_cause_list$tmrel$tmrel_mean, 3)),
            log_space = T, sub.title = paste0("Weighted by ", weights, " (fixed effects only)"))
    
    dr_plot(all_cause_list$plot_df, mean = "ln_pred", uncertainty = T, lower = "ln_pred_lo", upper = "ln_pred_hi",
            ylabel = "ln(RR)", caption = paste0("Bottom of curve ", 
                                                round(all_cause_list$tmrel$tmrel_mean, 3),
                                                "\n range of ", round(all_cause_list$tmrel$tmrel_min, 3),
                                                " - ", round(all_cause_list$tmrel$tmrel_max, 3),
                                                "\n 95% UI (", round(all_cause_list$tmrel$tmrel_025, 3),
                                                ", ", round(all_cause_list$tmrel$tmrel_975, 3), ")"),
            log_space = T, sub.title = paste0("Weighted by ", weights, " (fixed effects only)"))
    
    # Plot in normal space
    dr_plot(all_cause_list$plot_df, mean = "pred", uncertainty = F, lower = "pred_lo", upper = "pred_hi",
            ylabel = "RR", caption = paste0("Bottom of curve ",round(all_cause_list$tmrel$tmrel_mean, 3)),
            log_space = F, sub.title = paste0("Weighted by ", weights, " (fixed effects only)"))
    
    dr_plot(all_cause_list$plot_df, mean = "pred", uncertainty = T, lower = "pred_lo", upper = "pred_hi",
            ylabel = "RR", caption = paste0("Bottom of curve ", 
                                            round(all_cause_list$tmrel$tmrel_mean, 3),
                                            "\n range of ", round(all_cause_list$tmrel$tmrel_min, 3),
                                            " - ", round(all_cause_list$tmrel$tmrel_max, 3),
                                            "\n 95% UI (", round(all_cause_list$tmrel$tmrel_025, 3),
                                            ", ", round(all_cause_list$tmrel$tmrel_975, 3), ")"),
            log_space = F, sub.title = paste0("Weighted by ", weights, " (fixed effects only)"))
    
    dev.off()
    
  }
    
  } else {
      sex <- ifelse(sex.id == 1, "males", ifelse(sex.id == 2, "females", "both"))
      pdf(paste0(work_dir, "FILEPATH", lower_bound, "_", weights, "_", sex, ".pdf"), width = 11, height = 7)
      
      all_cause_tmrel_hist(all_cause_list$tmrel_draws, 
                           all_cause_list$orig_tmrel_draws, 
                           plot_title = paste0("Distribution of BMI TMREL (", sex, ")"),
                           xlab = "BMI (kg/m^2)")
      
      all_cause_draws_plot(all_cause_list$rr_draws, 
                           all_cause_list$orig_rr_draws, 
                           xlab = "BMI (kg/m^2)", 
                           plot_title = paste0("All-cause risk curve (", sex, ")"))
      
      dev.off()
  }
  
} 

# Final apporach: using TMREL produced from risk curves with new gamma and no trimming (weighted by deaths)

#### Save tmrel summary outputs ----
all_tmrels <- fread(paste0(work_dir, "FILEPATH"))
temp <- all_cause_list$tmrel
temp$date <- as.character(Sys.Date())
temp$sex_id <- sex.id
all_tmrels <- rbind(all_tmrels, temp, use.names = TRUE, fill = TRUE)
fwrite(all_tmrels, paste0(work_dir, "tmrel/all_cause_tmrel.csv"))

#### Save tmrel draws ----
tmrel_temp <- as.data.table(expand.grid(location_id = 1,
                                        age_group_id = ages,
                                        year_id = c(1990:2022),
                                        sex_id = c(1:2)))
tmrel_draws <- all_cause_list$tmrel_draws[, -("a_0")]
tmrel_draws$location_id <- 1
setnames(tmrel_draws, "draw_1000", "draw_0")
setcolorder(tmrel_draws, neworder = paste0("draw_", 0:999))
tmrel_out <- merge(tmrel_temp, tmrel_draws, by = c("location_id"))
fwrite(tmrel_out, paste0("FILEPATH"))
