# Script to process and upload LPA RR draws into database
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
library(rhdf5, lib.loc = "FILEPATH")
source('FILEPATH')
source('FILEPATH')
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, "FILEPATH"))
cause_ids <- get_ids("cause")
demo <- get_demographics("epi", gbd_round_id = 7)
age_ids <- get_ids("age_group")

np <- import("numpy")
np$random$seed(as.integer(3197))
set.seed(3197)

# Set up meta vars
gamma_param_draws <- TRUE
get_glb_dist <- FALSE
# New RRs ----
# Generate draws for each outcome
if(get_glb_dist) {
  exposure <- get_global_exposure_distribution(125)
  exposure <- exposure[1:999] # Take out penultimate exposure value
  exposure <- c(0, exposure) # Replace with 0
  exp_df <- data.table(exp_raw = exposure)
  exp_df[, exp_round := round(exp_raw, digits = -2)]
  exp_df[, diff := exp_raw - exp_round]
  exp_df[, min_diff := min(abs(diff)), by = "exp_round"]
  exp_df[, exp_keep := ifelse(abs(diff) == min_diff, exp_round, exp_raw)]
  exp_vec <- exp_df$exp_keep
  saveRDS(exp_vec, file = "FILEPATH")
} else {
  exposure <- readRDS(sprintf("FILEPATH", work_dir))
}


cause_dirs <- list.dirs(paste0(work_dir, "results"), recursive = FALSE)
causes <- list.dirs(paste0(work_dir, "results"), recursive = FALSE, full.names = FALSE) %>%
  gsub("cause_id_", "", .) %>%
  as.numeric(.)

if(gamma_param_draws) {
  cause_temp <- data.table(cause_id = causes)
  cause_temp <- merge(cause_temp, cause_ids[,.(cause_id, acause)], all.x = T)
  cause_temp[, folder := paste0("activity_", acause)]
  setnames(cause_temp, "cause_id", "cause.id")
}

create_draws_for_upload <- function(cause_id) {
  
  print(cause_id)
  if(gamma_param_draws == TRUE) {
    # Load in model objects
    acause_dir <- cause_temp[cause.id == cause_id, folder]
    data_dir <- paste0("FILEPATH", acause_dir, "/")
    
    signal_model <- py_load_object(paste0(data_dir, "signal_model.pkl"), pickle = "dill")
    linear_model <- py_load_object(paste0(data_dir, "new_linear_model.pkl"), pickle = "dill")
    # Determine when exposure values below and above cut points
    data <- as.data.table(signal_model$data$to_df())
    data[, b_mid := (b_0 + b_1)/2]
    upper_bound <- as.numeric(quantile(data$b_mid, probs = 0.85))
    
    trim_exp <- exposure[exposure <= upper_bound]
    upp_exp <- exposure[exposure > upper_bound]
    
    # Create draws
    df_draws_re <- get_ln_rr_draws(signal_model,
                                   linear_model,
                                   risk = trim_exp,
                                   num_draws = 1000L,
                                   normalize_to_tmrel = FALSE)
    
    # Exponentiate in normal space and format
    df_draws_re[, grep("draw_", names(df_draws_re), value = T)] <- apply(df_draws_re[, grep("draw_", names(df_draws_re), value = T)], 2, exp)
    df_draws_re$cause_id <- cause_id
    df_draws_re <- as.data.table(df_draws_re)
    setnames(df_draws_re, "risk", "exposure")
    
    # Outlier draws
    df_draws_re <- outlier_draws(df_draws_re, exposure_col = "exposure")
    df_draws_re <- as.data.table(df_draws_re)
    
    max_exp <- max(df_draws_re$exposure)

    # Replicate out the draws for high exposure values
    for(u in upp_exp){
      
      temp <- df_draws_re[exposure == max_exp]
      temp[, exposure := u]
      
      df_draws_re <- rbind(df_draws_re, temp)
      rm(temp)
    }
    
    setnames(df_draws_re, "draw_1000", "draw_0")
  }
  
  # Check number of rows
  if(nrow(df_draws_re) != length(exposure)){
    stop(message("Length of returned draws is not the same length as the exposure vector"))
  }
  
  return(df_draws_re)
}

all_rr <-
  rbindlist(
    lapply(
      causes,
      create_draws_for_upload
    ),
    use.names = TRUE,
    fill = TRUE
  )

if(length(unique(all_rr$exposure)) != 1000) {
  stop(message(paste0("1000 unique exposures not present in dataframe. There are currently ", length(unique(all_rr$exposure)), " unique exposures")))
}
# Format data ----
desired_sex <- c(1, 2)
desired_ages <- c(10:20, 30:32, 235)
desired_loc <- 1
desired_years <- demo$year_id

upload <- rbindlist(lapply(causes, function(x) {
    
  if(x != 429) {
    temp <- as.data.table(expand.grid(desired_sex, desired_ages, desired_loc)) %>% 
      setnames(., names(.), c("sex_id", "age_group_id", "location_id"))
    
    temp[, `:=` (cause_id = x, mortality = 1, morbidity = 1)]
    
    out <- merge(temp, all_rr, by = c("cause_id"), allow.cartesian = TRUE)
    
    
    rm(temp)
    return(out)
  } else {
    
    temp <- as.data.table(expand.grid(2, desired_ages, desired_loc)) %>% 
      setnames(., names(.), c("sex_id", "age_group_id", "location_id"))
    
    temp[, `:=` (cause_id = x, mortality = 1, morbidity = 1)]
    
    out <- merge(temp, all_rr, by = c("cause_id"), allow.cartesian = TRUE)
    
    males <- copy(out) %>%
      .[, sex_id := 1]
    males[, c(paste0("draw_", 0:999)) := 0]
    
    out <- rbind(out, males)
    
    rm(temp)
    return(out)
    
  }

    
}), fill = TRUE, use.names = TRUE)

if(length(unique(upload$cause_id)) != 5) stop(message("Not all cause IDs are present."))

# Apply age attenuation
print("Applying age attenuation.")
orig_rows <- nrow(upload)

apply_age_atten <- function(x) {
  
  # Only keep rows of interest
  out <- upload[cause_id == x]
  upload <<- upload[cause_id != x]
  
  # Read in attenuation draws and format
  atten_dir <- "FILEPATH"
  
  age_atten <- fread(paste0(atten_dir, x, ".csv")) %>%
    .[, `:=` (label = NULL,
              age_start = NULL,
              age_end = NULL)] %>%
    melt(id.vars = c("cause_id", "age_group_id"),
         value.name = "attenuation_factor",
         variable.factor = FALSE)
  

  
  # Merge based on age ID and draw
  temp <- melt(out, id.vars = setdiff(names(out), grep("draw_", names(out), value = T)), variable.factor = FALSE)
  out <- merge(temp, age_atten, by = c("age_group_id", "cause_id", "variable"))
  if(nrow(out) != nrow(temp)) {stop(message("Merging age attenuation draws dropped some rows of RRs."))}
  
  # Apply attenuation factor
  out[, adj_rr := 1/(attenuation_factor * ((1/value) - 1) + 1)]
  out[, c("value", "attenuation_factor") := NULL]
  
  # Reshape wide
  out <- dcast(out, ... ~ variable, value.var = "adj_rr")
  
  # Plot the age attenuated RRs
  plot_data <- melt(out, id.vars = setdiff(names(out), grep("draw_", names(out), value = T)))
  plot_data[, mean_pred := mean(value), by = c("age_group_id", "sex_id", "exposure")]
  plot_data[, lo_pred := quantile(value, probs = 0.025), by = c("age_group_id", "sex_id", "exposure")]
  plot_data[, hi_pred := quantile(value, probs = 0.975), by = c("age_group_id", "sex_id", "exposure")]
  
  plot_data <- unique(plot_data[,.(cause_id, age_group_id, sex_id, exposure, mean_pred, lo_pred, hi_pred)])
  plot_data <- merge(plot_data, age_ids, by = "age_group_id")
  for(s in c(1, 2)) {
    sex <- ifelse(s == 1, "Males", "Females")
    
    scale_atten_plot <- ggplot(data = plot_data[sex_id == s], aes(x = exposure, y = mean_pred)) +
      geom_line(color = "steelblue2") +
      geom_ribbon(aes(ymin = lo_pred, ymax = hi_pred), fill = "steelblue2", alpha = 0.2) +
      facet_wrap(~age_group_name, scale = "free_y") +
      theme_bw() +
      ggtitle(paste0(cause_ids[cause_id == unique(plot_data$cause_id), cause_name], " ", sex)) +
      ylab("RR") +
      xlab("BMI (kg/m^2)") +
      geom_abline(intercept = 1, slope = 0, linetype = "dashed")
    
    atten_plot <- ggplot(data = plot_data[sex_id == s], aes(x = exposure, y = mean_pred)) +
      geom_line(color = "steelblue2") +
      geom_ribbon(aes(ymin = lo_pred, ymax = hi_pred), fill = "steelblue2", alpha = 0.2) +
      facet_wrap(~age_group_name) +
      theme_bw() +
      ggtitle(paste0(cause_ids[cause_id == unique(plot_data$cause_id), cause_name], " ", sex)) +
      ylab("RR") +
      xlab("BMI (kg/m^2)") +
      geom_abline(intercept = 1, slope = 0, linetype = "dashed")
    
    print(scale_atten_plot)
    print(atten_plot)
  }
  
  # Append back on
  upload <<- rbind(upload, out, use.names = TRUE, fill = TRUE)
  
  rm(temp)
  rm(out)
}

if(gamma_param_draws == TRUE) plot_path <- paste0(work_dir, "FILEPATH")
pdf(plot_path, height = 11, width = 9)
sapply(c(493, 495, 976), apply_age_atten)
dev.off()



if(nrow(upload) != orig_rows) stop(message("Something went wrong with applying the age attenuation."))
print("Success")

if(length(unique(upload$exposure)) != 1000) {
  stop(message(paste0("1000 unique exposures not present in dataframe. There are currently ", length(unique(upload$exposure)), " unique exposures")))
}

# Save data files by year ----
if(gamma_param_draws == TRUE) save_dir <- "FILEPATH"

write.csv(upload, paste0(save_dir, 2020, ".csv"), row.names = FALSE)