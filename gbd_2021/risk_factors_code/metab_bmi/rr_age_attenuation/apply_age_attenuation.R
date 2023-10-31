# Script to process and upload draws into database
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
invisible(sapply(list.files("FILEPATH", full.names = T), source))
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, "FILEPATH"))
cause_ids <- get_ids("cause")
demo <- get_demographics("epi", gbd_round_id = 7)
age_ids <- get_ids("age_group")
# Set up arguments
args <- commandArgs(trailingOnly = TRUE)
lower_bound <- 15.4 # reference value when creating draws
upper_bound <- 43.9 # upper bound to cap estimation of RRs
gamma_param_draws <- T

# Set seeds
np <- import("numpy")
np$random$seed(as.integer(3197))
set.seed(3197)

# Old RRs ----
# Get GBD 2019 RRs and extract unique cause IDs
old_rr <- get_draws("rei_id", 370, source = "rr", gbd_round_id = 6, decomp_step = "step4", year_id = 2019)
setnames(old_rr, "draw_0", "draw_1000")
all_causes <- data.table(cause_id = unique(old_rr$cause_id)) %>%
  merge(., cause_ids[,.(cause_id, cause_name)], by = "cause_id")

# Drop causes not included in this round
all_causes <- all_causes[!cause_id %in% c(591:593, 998)]

# Map to parent cause
for(c in unique(all_causes$cause_id)){
  
  if(c %in% c(418:421)) {
    # Liver cancer
    all_causes[cause_id == c, parent_cause := 417]
  } else if(c %in% c(496, 497)) {
    # Hem stroke
    all_causes[cause_id == c, parent_cause := 494]
  } else if(c %in% c(845:848, 943)) {
    # Leukemia
    all_causes[cause_id == c, parent_cause := 487]
  } else if(c %in% c(1006, 1007)) {
    # NHL
    all_causes[cause_id == c, parent_cause := 485]
  } else if(c %in% c(1014, 1015)) {
    # OA
    all_causes[cause_id == c, parent_cause := 628]
  } else {
    all_causes[cause_id == c, parent_cause := c]
  }
    
  
}

# New RRs ----
# Generate draws for each outcome
temp_exp <- fread("FILEPATH")
exposure <- temp_exp$exposure

cause_dirs <- list.dirs(paste0(work_dir, "results"), recursive = FALSE)
causes <- list.dirs(paste0(work_dir, "results"), recursive = FALSE, full.names = FALSE) %>%
  gsub("cause_id_", "", .) %>%
  as.numeric(.)
causes <- causes[!is.na(causes)]

if(gamma_param_draws) {
  cause_temp <- data.table(cause_id = causes)
  cause_temp <- merge(cause_temp, cause_ids[,.(cause_id, acause)], all.x = T)
  cause_temp[cause_id == 429, acause := "neo_breast_premenopause"]
  cause_temp[cause_id == 4299, acause := "neo_breast_postmenopause"]
  cause_temp[cause_id == 494, acause := "cvd_stroke_hemmorhage"]
  cause_temp[, folder := paste0("metab_bmi_adult_", acause)]
  setnames(cause_temp, "cause_id", "cause.id")
}

create_draws_for_upload <- function(cause_id){
  print(cause_id)
  if(gamma_param_draws == TRUE) {
    # Load in model objects
    acause_dir <- cause_temp[cause.id == cause_id, folder]
    data_dir <- paste0("FILEPATH", acause_dir, "/")
    
    signal_model <- py_load_object(paste0(data_dir, "signal_model.pkl"), pickle = "dill")
    linear_model <- py_load_object(paste0(data_dir, "new_linear_model.pkl"), pickle = "dill")
    # Determine when exposure values below and above cut points
    trim_exp <- exposure[exposure >= lower_bound & exposure <= upper_bound]
    low_exp <- exposure[exposure < lower_bound]
    upp_exp <- exposure[exposure > upper_bound]
    
    # Create draws
    df_draws_re <- get_ln_rr_draws(signal_model,
                                   linear_model,
                                   risk = trim_exp,
                                   num_draws = 1000L,
                                   normalize_to_tmrel = FALSE)
    
    df_draws_re$cause_id <- cause_id
    df_draws_re <- as.data.table(df_draws_re)
    setnames(df_draws_re, "risk", "exposure")
    
    # Outlier and normalize to tmrel
    df_draws_re <- outlier_draws(df_draws_re, exposure_col = "exposure")
    df_draws_re <- normalize_draws(df_draws_re, return_log_space = FALSE, exposure_col = "exposure")
    df_draws_re <- as.data.table(df_draws_re)
    
    min_exp <- min(df_draws_re$exposure)
    max_exp <- max(df_draws_re$exposure)
    
    # Duplicate out the draws for low exposure values
    for(l in low_exp){
      
      temp <- df_draws_re[exposure == min_exp]
      temp[, exposure := l]
      
      df_draws_re <- rbind(temp, df_draws_re)
      rm(temp)
    }
    
    # Duplicate out the draws for high exposure values
    for(u in upp_exp){
      
      temp <- df_draws_re[exposure == max_exp]
      temp[, exposure := u]
      
      df_draws_re <- rbind(df_draws_re, temp)
      rm(temp)
    }
    
  }
  
  return(df_draws_re)
}

print("Compiling draws of RR.")
all_rr <- rbindlist(
  lapply(
    causes,
    create_draws_for_upload
  ),
  use.names = TRUE
)
setnames(all_rr, "cause_id", "parent_id")
print("Success")

# Format data ----
print("Formatting data to get RRs for each demographic combination.")
desired_sex <- c(1, 2)
desired_ages <- c(9:20, 30:32, 235)
desired_years <- demo$year_id
desired_loc <- 1
upload <- rbindlist(lapply(unique(all_causes$cause_id), function(x) {
  
  # Standard RO pairs
  if(!x %in% c(498, 429, 465, 435)) {
    
    temp <- as.data.table(expand.grid(desired_sex, desired_ages, desired_loc)) %>% 
      setnames(., names(.), c("sex_id", "age_group_id", "location_id"))
    temp[, `:=` (cause_id = x,
                 parent_id = all_causes[cause_id == x]$parent_cause)]
    
    # Morbidity only outcomes
    if(x %in% c(1014, 1015, 630, 632, 671)) {
      
      temp[, `:=` (mortality = 0, morbidity = 1)]
      
    } else {
      
      temp[, `:=` (mortality = 1, morbidity = 1)]
      
    }
    
    out <- merge(temp, all_rr, by = c("parent_id"), allow.cartesian = TRUE)
    out$parent_id <- NULL
    rm(temp)
    
    return(out)
    
  } else if(x == 429) {
    # Breast cancer requires differential RRs by age (premenopausal and postmenopausal status)
    pre <- as.data.table(expand.grid(2, desired_ages[desired_ages < 15], desired_loc)) %>%
      setnames(., names(.), c("sex_id", "age_group_id", "location_id"))
    pre[, `:=` (cause_id = x,
                parent_id = 429,
                mortality = 1,
                morbidity = 1)]
    pre <- merge(pre, all_rr, by = "parent_id", allow.cartesian = TRUE)
    
    # Postmenopausal breast cancer
    post <- as.data.table(expand.grid(2, desired_ages[desired_ages >= 15], desired_loc)) %>%
      setnames(., names(.), c("sex_id", "age_group_id", "location_id"))
    post[, `:=` (cause_id = x,
                 parent_id = 4299,
                 mortality = 1,
                 morbidity = 1)]
    post <- merge(post, all_rr, by = "parent_id", allow.cartesian = TRUE)
    
    temp <- rbind(pre, post, use.names = TRUE)
    males <- copy(temp) %>%
      .[, sex_id := 1]
    males[, c(paste0("draw_", 1:1000)) := 0]
    
    out <- rbind(temp, males)
    out$parent_id <- NULL
    
    rm(temp, males)
    return(out)
    
  } else if(x %in% c(465, 435)) {
    # Ovarian cancer and Uterine cancer are sex specific RRs
    temp <- as.data.table(expand.grid(2, desired_ages, desired_loc)) %>%
      setnames(., names(.), c("sex_id", "age_group_id", "location_id"))
    temp[, `:=` (cause_id = x,
                 parent_id = all_causes[cause_id == x]$parent_cause,
                 mortality = 1,
                 morbidity = 1)]
    
    temp <- merge(temp, all_rr, by = "parent_id", allow.cartesian = TRUE)
    
    males <- copy(temp) %>%
      .[, sex_id := 1]
    males[, c(paste0("draw_", 1:1000)) := 0]
    
    out <- rbind(temp, males)
    out$parent_id <- NULL
    
    rm(temp, males)
    return(out)
    
  } else if(x == 498) {
    # HHD was not updated this year - using last year's RRs
    out <- unique(old_rr[cause_id == 498, c("cause_id", "age_group_id", "sex_id", "mortality", "morbidity", "parameter",
                                            paste0("draw_", 1:1000)), with = F])
    out$location_id <- 1
    return(out)
  }
}), fill = TRUE, use.names = TRUE)

# Make sure draw 1000 is renamed to draw 0
setnames(upload, "draw_1000", "draw_0")

# Check if all cause IDs are present
if(sum(unique(all_causes$cause_id) %in% unique(upload$cause_id)) != length(unique(all_causes$cause_id))){
  stop(message("Not all cause IDs are present in the upload dataframe."))
} else {
  print("Success")
}


# Apply age attenuation
print("Applying age attenuation.")
orig_rows <- nrow(upload)
age_atten_causes <- c(493, 495:497, 976)

apply_age_atten <- function(x, plot = TRUE, trim = F) {
  
  # Only keep rows of interest
  print(x)
  cause <- upload[cause_id == x]
  
  # Read in attenuation draws and format
  atten_dir <- "FILEPATH"
  age_atten <- fread(paste0(atten_dir, "370_", x, ".csv")) %>%
    .[, `:=` (rei_id = NULL,
              age_start = NULL,
              age_end = NULL)] %>%
    melt(id.vars = c("cause_id", "age_group_id"), value.name = "attenuation_factor")
  
  # Merge based on age ID and draw
  temp <- melt(cause, id.vars = setdiff(names(cause), paste0("draw_", 0:999)))
  out <- merge(temp, age_atten, by = c("age_group_id", "cause_id", "variable"))
  if(nrow(out) != nrow(temp)) {stop(message("Merging age attenuation draws dropped some rows of RRs."))}
  
  # Apply attenuation factor in excess risk space
  out[value >= 1, adj_rr := attenuation_factor * (value - 1) + 1]
  out[value < 1, adj_rr := 1/(attenuation_factor * (1/value - 1) + 1)] # To deal with draws of RR where RR < 1
  out[, c("value", "attenuation_factor") := NULL]
  
  # Check for negative RR draws
  print(paste0("Min and max RRs are ", range(out$adj_rr)))
  if(nrow(out[adj_rr <= 0])) {
    print(paste0("Draws ", unique(out[adj_rr <= 0]$variable), " for cause ID ", x, " are less than 0."))
    stop(message(paste0("There are ", nrow(out[adj_rr <= 0]), " draws where RR is less than 0.")))
    
  }
  
  # Reshape wide
  out <- dcast(out, ... ~ variable, value.var = "adj_rr")
  if(trim) out <- outlier_draws(out, exposure_col = "exposure") # Commenting out outlier draws (6/17)
  
  # Append back on
  upload <<- rbind(upload[cause_id != x], out, use.names = TRUE, fill = TRUE)
  upload <<- as.data.table(upload)
  
  # Plot the age attenuated RRs
  if(plot) {
    plot_data <- melt(out, id.vars = setdiff(names(out), grep("draw_", names(out), value = T)))
    plot_data <- as.data.table(plot_data)
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
    
  }
  
  rm(temp)
  rm(out)
  
  
}

if(gamma_param_draws == TRUE) plot_path <- paste0(work_dir, "FILEPATH")


pdf(plot_path, height = 11, width = 9)
sapply(age_atten_causes, apply_age_atten, plot = TRUE, trim = F)
dev.off()

if(nrow(upload) != orig_rows) stop(message("Something went wrong with applying the age attenuation."))
print("Success")

# Add Asian region RRs (East Asia, Southeast Asia, Oceania, and High-income Asia pacific)
# Setting all RRs equal to 1 for breast cancer RRs in women under 50
# allowing PAF code to handle PAFs that will be > 1 for locs in those 4 regions
print("Creating separate RRs for Asian regions and modifying breast cancer")
asia_regions <- c(65, 5, 21, 9)

prep_asia_rrs <- function(reg) {
  temp <- copy(upload)
  draw_cols <- grep("draw_", names(upload), value = T)
  # Set RR draws to 1 for breast cancer and women under 50
  if(unique(temp$location_id) != reg) temp[, location_id := reg]
  temp[age_group_id %in% desired_ages[desired_ages < 15] & sex_id == 2 & cause_id == 429, (draw_cols) := 1]
  return(temp)
}


# Save data files by year ----
if(gamma_param_draws == TRUE) save_dir <- "FILEPATH"


print(paste0("Saving draws now to ", save_dir))
save_draws <- function(loc) {
  if(loc != 1) {
    final <- prep_asia_rrs(loc)
  } else {
      final <- copy(upload)
    }
  write.csv(final, paste0(save_dir, loc, "_", 2020, ".csv"), row.names = FALSE)
  print(paste0("Saved draws for location ID ", loc))
}

locs <- c(desired_loc, asia_regions)
lapply(locs, save_draws)
print("All draws saved.")

