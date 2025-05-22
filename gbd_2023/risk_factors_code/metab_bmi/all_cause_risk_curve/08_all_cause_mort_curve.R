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

source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, "FILEPATH"))
source(paste0(code_dir, "FILEPATH/process_continuous_results.R"))

# Helper datasets
cause_ids <- get_ids("cause")

# Set up arguments
weights <- "deaths" # string: Either "deaths" or "dalys"
lower_bound <- 15.4 # numeric: lowest value of BMI that the all-cause mortality curve should start at
gamma_param_draws <- T # boolean: Keep as true
random_effects <- F # boolean: Keep as false
fisher <- F # boolean: keep as false

# Set arguments
sex.id <- 3
percentiles <- c("025","05", "10", "15", "50", "85", "90", "95", "975")
ages <- c(9:20, 30:32, 235)
ref_covs <- c("a_0", "a_1")
alt_covs <- c("b_0", "b_1")
cod_version <- 261
cod_decomp <- "step3"

# Make vector of cause IDS to make curve
cause_dirs <- list.dirs(paste0(work_dir, "results"), recursive = FALSE)
causes <- list.dirs(paste0(work_dir, "results"), recursive = FALSE, full.names = FALSE) %>%
  gsub("cause_id_", "", .) %>%
  as.numeric(.)
causes <- causes[!is.na(causes)]

# dropping these demo cols
drop_cols <- c("location_id", "age_group_id", "year_id", "metric_id", "measure_id", "sex_id", "version_id")
draw_cols <- paste0("draw_", 0:999)

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
    
    # Postmenopausal breast cancer (>50 years)
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
  if (gamma_param_draws == TRUE) {
    # Load in model objects
    acause_dir <- cause_temp[cause.id == c, folder]
    data_dir <- paste0("FILEPATH/", acause_dir, "/")
    signal_model <- py_load_object(paste0(data_dir, "signal_model.pkl"), pickle = "dill")
    linear_model <- py_load_object(paste0(data_dir, "new_linear_model.pkl"), pickle = "dill")
    
    # Determine when exposure values below and above cut points
    trim_exp <- exposure[exposure >= lower_bound]
    low_exp <- exposure[exposure < lower_bound]
    if(! lower_bound %in% trim_exp) trim_exp <- c(lower_bound, trim_exp)
    
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
    min_exp <- min(df_draws_re$exposure)
    
    draws <- copy(df_draws_re)
    
  }}), use.names = TRUE)

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
  pdf(paste0(work_dir, "tmrel/all_cause_rr_random_intercept_", lower_bound, "_", weights, "_", sex, ".pdf"), width = 11, height = 7)
  
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

#### Save tmrel summary outputs ----
all_tmrels <- fread(paste0(work_dir, "tmrel/all_cause_tmrel.csv"))
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
fwrite(tmrel_out, paste0("FILEPATH/tmrel_draws.csv"))
