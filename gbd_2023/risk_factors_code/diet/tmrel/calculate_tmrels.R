#----------------------------------------------------------------------------------
# Calculate TMREL for all risks
#   all-cause mortality curves for non-protective risks
# 
#----------------------------------------------------------------------------------

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

out_dir <- "FILEPATH"
WORK_DIR <- "FILEPATH"
central_model_output_folder <- "FILEPATH"

library(dplyr)
library(ggplot2)
library(data.table)
library(reticulate)
library(mrbrt001, lib.loc = "FILEPATH/")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/helper_functions.R")
ids <- get_ids("measure")
set.seed(123)

# Set up arguments---------------------------------------
random_effects <- F
meas_id <- 1 # 1: deaths, 2:DALYs
mrbrt_version <- "diet_final_upload"
risk_grep <- "calcium"
protective_data <- T
ro_addendum <- ""
mrbrt_dir <- paste0(out_dir, mrbrt_version,"/")
save_dir <- paste0(out_dir, "/diet_results/")
exclude_ro_pair <- c()
note <- ""
sex_id <- 3

#----------------------------------------------------------
# Step 1: set up directory and cause list
#----------------------------------------------------------
if(meas_id == 1){
  source = "codcorrect"
  version_id = 135
}else if(meas_id == 2){
  source = "dalynator"
  version_id = 47
}

meas_name <- gsub(" .*", "", ids[measure_id==meas_id, measure_name])

save_dir <- paste0(save_dir, "/06_tmrel/")
if(!dir.exists(save_dir)){dir.create(save_dir)}
save_dir <- paste0(save_dir, "/", risk_grep, "/")
dir.create(save_dir)

source(paste0(mrbrt_dir, "/config.R"))
pdf(paste0(save_dir, "/",risk_grep,"all_cause_mortality_curve_", ifelse(random_effects, "re_", "fe_"), meas_name,note, ".pdf"), width = 11, height = 8)

pairs <- gsub(".pkl","",list.files(paste0(mrbrt_dir, "/04_mixed_effects_pkl_files/"), pattern = ".pkl"))
diet_ro_pair_map <- fread("/FILEPATH/diet_ro_map.csv")
diet_ro_pair_map <- diet_ro_pair_map[include==1]

use_pairs <- pairs[grepl(risk_grep, pairs)]
use_pairs <- gsub(ro_addendum,"", use_pairs)
causes <-  diet_ro_pair_map[risk_cause %in% use_pairs, .(risk_cause, cause_id)]
causes[, pair:= paste0(risk_cause, ro_addendum)]
causes <- unique(causes[risk_cause %like% "hemstroke", cause_id:=494])
causes <- causes[!(risk_cause %in% exclude_ro_pair)]

print(causes)

#----------------------------------------------------------
# Step 2: Pull burden draws from GBD 2019
#----------------------------------------------------------
drop_cols <- c("location_id", "age_group_id", "year_id", "metric_id", "measure_id", "sex_id")
# First all but hem stroke since it has two subcauses
mort <- get_draws(gbd_id_type = "cause_id",
                  gbd_id = causes$cause_id[!causes$cause_id %in% c(494, 496,497)],
                  year_id = 2019,
                  measure_id = meas_id,
                  metric_id = 1,
                  location_id = 1,
                  age_group_id = 22,
                  sex_id = sex_id,
                  source = source,
                  decomp_step ="step4",
                  version_id = version_id,
                  gbd_round_id = 6) %>%
  .[, c(drop_cols) := NULL] %>%
  melt(., id.vars = "cause_id")

if(494 %in% causes$cause_id){
  
  # Pulling draws for children of hemorrhagic stroke
  # Intracerebral hemorrhage
  intr_mort <- get_draws(gbd_id_type = "cause_id",
                         gbd_id = 496,
                         year_id = 2019,
                         measure_id = meas_id,
                         metric_id = 1,
                         location_id = 1,
                         age_group_id = 22,
                         sex_id = sex_id,
                         source = source,
                         decomp_step ="step4",
                         version_id = version_id,
                         gbd_round_id = 6) %>%
    .[, c(drop_cols) := NULL] %>%
    melt(., id.vars = "cause_id") %>%
    setnames(., "value", "intr_value")
  
  # Subarachnoid hemorrhage
  sub_mort <- get_draws(gbd_id_type = "cause_id",
                        gbd_id = 497,
                        year_id = 2019,
                        measure_id = meas_id,
                        metric_id = 1,
                        location_id = 1,
                        age_group_id = 22,
                        sex_id = sex_id,
                        source = source,
                        decomp_step ="step4",
                        version_id = version_id,
                        gbd_round_id = 6) %>%
    .[, c(drop_cols) := NULL] %>%
    melt(., id.vars = "cause_id") %>%
    setnames(., "value", "sub_value")
  
  # Merge to get mortality for hemorrhagic stroke
  hem_mort <- merge(intr_mort, sub_mort, by = "variable") %>%
    .[, value := intr_value + sub_value] %>%
    .[, c("sub_value", "intr_value", "cause_id.x", "cause_id.y") := NULL] %>%
    .[, cause_id := 494]
  
}else{
  
  hem_mort <- data.table()
}
# Append mortality estimates
mort <- rbind(mort, hem_mort) %>% 
  setnames(., "value", "deaths")

mort <- mort[cause_id %in% causes$cause_id]

# mean of mortality draws
mort[, avg_deaths := mean(deaths), by = "cause_id"]
mort <- unique(mort[,.(cause_id, avg_deaths)])
mort[, weight := avg_deaths/sum(avg_deaths)]


#----------------------------------------------------------
# Step 3: Pull data and risk curves for each outcome
#----------------------------------------------------------

# calculate exposure range for the risk
rr_data <- rbindlist(lapply(causes$pair, function(p){
  
  input_data <- readRDS(paste0(mrbrt_dir, "/00_prepped_data/", p, ".RDS"))
  df <- as.data.table(input_data$df)
  df[, b_midpoint := b_0 + (b_1-b_0)/2]
  df[, a_midpoint := a_0 + (a_1-a_0)/2]
  
  df[, pair := p]
  return(df)
  
}), fill = T)

source("FILEPATH/continuous_functions.R")
source("FILEPATH/final_model_functions.R")
rr_draws <- rbindlist(lapply(causes$pair, function(p){
  
  is_j_shaped <- F 
  
  if(p == "redmeat_hemstroke"){
    t <- "redmeat_hemstroke_496"
  }else{
    t <- p
  }
  
  signal_model_path <- paste0(central_model_output_folder, "/", t, "/signal_model.pkl")
  linear_model_path <- paste0(central_model_output_folder, "/", t, "/new_linear_model.pkl")
  signal_model <- py_load_object(filename = signal_model_path, pickle = "dill")
  linear_model <- py_load_object(filename = linear_model_path, pickle = "dill")
  
  
  # get_draws
  y_draws <- get_ln_rr_draws(signal_model,
                              linear_model,
                              risk = seq(0, max(rr_data$b_midpoint), length.out = 1000),
                              num_draws = 1000L,
                              normalize_to_tmrel = is_j_shaped,
                              fe_only = !random_effects)
  y_draws <- as.data.table(y_draws)
  
  setnames(y_draws, colnames(y_draws), c("exposure", paste0("draw_",0:999)))
  
  df_final_pred <- summarize_draws(y_draws)
  
  tmrel <- df_final_pred[mean==min(mean), exposure]
  
    
    plot <- ggplot(df_final_pred, aes(x = exposure, y = exp(mean)))+geom_line()+
      geom_ribbon(aes(ymin = exp(lower), ymax = exp(upper)),alpha =0.4)+theme_bw()+
      labs(y = "Relative Risk (with gamma)", x = "Exposure", title = p, subtitle = paste0("TMREL: ", tmrel))
    print(plot)
    
    
    y_draws[, ro := p]
    y_draws[, cause_id := causes[pair == p, cause_id]]
    
    return(y_draws)
    
    
  
}), use.names = TRUE)


#----------------------------------------------------------
# Step 4: Calculate all-cause mortality curve
#----------------------------------------------------------

# Melt RR draws long
rr_long <- melt(rr_draws, id.vars = c("ro","exposure", "cause_id"))
rr_long$variable <- as.character(rr_long$variable)
##### Generate all-cause estimates

# Merge on deaths
rr_long <- merge(rr_long, mort, by = c("cause_id"))
rr_long[, rr := sum(value*weight), by = c("exposure", "variable")] # Take weighted mean of RRs

# save separate dataset for plotting purposes
rr_long[, `:=` (mean_rr = mean(rr), mean_value = mean(value)), by = c("cause_id", "exposure")]
out_rr <- unique(rr_long[,.(cause_id, ro, exposure, weight, mean_value)])
all_rr <- unique(rr_long[,.(exposure, mean_rr)])
all_rr[, `:=` (ro = "all-cause", weight = 1)]
setnames(out_rr, "mean_value", "mean_rr")
means_for_plot <- rbind(all_rr, out_rr, fill = T)
means_for_plot[, ro:=gsub("_fixed", "", ro)]

rr_long_adj <- unique(rr_long[,.(variable, exposure, rr)])
write.csv(rr_long_adj, paste0(save_dir, "/",risk_grep,"_allcausedraws_",meas_name,note,ifelse(random_effects, "_re", "_fe"),".csv"), row.names = F)

# what exposure value minimizes each draw ?
rr_long_adj[, min_rr := min(rr), by = "variable"]

min_exposure_details <- rr_long_adj[rr==min_rr, .(variable, exposure)]
setnames(min_exposure_details, "exposure", "min_exp")
min_exposure_vec <- min_exposure_details$min_exp

# summarize and move to normal space
rr_long_adj[, all_cause_rr := exp(mean(rr)), by = "exposure"] # Median behaves much better than mean in draws with random effects
rr_long_adj[, all_cause_rr_lo := exp(quantile(rr, probs = 0.025)), by = "exposure"]
rr_long_adj[, all_cause_rr_hi := exp(quantile(rr, probs = 0.975)), by = "exposure"]

all_cause_rr <- unique(rr_long_adj[,.(exposure, all_cause_rr, all_cause_rr_lo, all_cause_rr_hi)])

# now scale each draw to it's minimum
rr_long_adj[, rr_at_min := min(rr), by = "variable"]
rr_long_adj[, rr_shifted := rr - rr_at_min]

#----------------------------------------------------------
# Step 5: Plot, calculate TMREL, etc
#----------------------------------------------------------
tmrel <- mean(min_exposure_vec)
tmrel_lower <- quantile(min_exposure_vec, probs = 0.025)
tmrel_upper <- quantile(min_exposure_vec, probs = 0.975)
tmrel_lower_ <- quantile(min_exposure_vec, probs = 0.15)
tmrel_upper_ <- quantile(min_exposure_vec, probs = 0.70)


# Density plot of TMREL draws
density <- ggplot(data.table(tmrel = min_exposure_vec), aes(x = tmrel))+geom_histogram()+theme_bw()+
  labs(title = "TMREL draws", subtitle = paste0("mean: ", round(mean(min_exposure_vec)), " 2.5%: ", round(quantile(min_exposure_vec, 0.025)),
                                                        " 5%: ", round(quantile(min_exposure_vec, 0.05))," 10%: ", round(quantile(min_exposure_vec, 0.1)),
                                                        " 50%: ", round(quantile(min_exposure_vec, 0.5)), " 90%: ", round(quantile(min_exposure_vec, 0.90)),
                                                        " 95%: ", round(quantile(min_exposure_vec, 0.95)), " 97.5%: ", round(quantile(min_exposure_vec, 0.975))))
print(density)

# Boxplot of TMREL draws
boxplot <- ggplot(data.table(tmrel = min_exposure_vec), aes(y = tmrel))+geom_boxplot()+theme_bw()+labs(title = "Post MAD outliering")
print(boxplot)

# Plot of all-cause risk curve
plot <- ggplot(rr_long_adj, aes(x = exposure, y = exp(rr_shifted), group = variable))+geom_line(alpha = 0.05)+
  theme_bw()+
  labs(y = "Relative Risk", x = "Exposure", title = "All cause curve of outliered outcomes relative to minimum risk level by draw")
print(plot)


plot <- ggplot(all_cause_rr, aes(x = exposure, y = all_cause_rr))+geom_line()+
  geom_ribbon(aes(ymin = all_cause_rr_lo, ymax = all_cause_rr_hi),alpha =0.4)+
  theme_bw()+
  labs(y = "Relative Risk", x = "Exposure", title = "All cause", subtitle = paste0("95% TMREL: ", tmrel, " (", tmrel_lower, "-", tmrel_upper, ")", "\n",
                                                                                   "70% TMREL: ", tmrel, " (", tmrel_lower_, "-", tmrel_upper_, ")"))
print(plot)

# Plot of mean all-cause risk curve components
plot <- ggplot(means_for_plot, aes(x = exposure, y = mean_rr, color = ro))+geom_line()+
  theme_bw()+
  labs(y = "Relative Risk", x = "Exposure", title = "All cause", subtitle = paste0("95% TMREL: ", tmrel, " (", tmrel_lower, "-", tmrel_upper, ")", "\n",
                                                                                   "70% TMREL: ", tmrel, " (", tmrel_lower_, "-", tmrel_upper_, ")"))
print(plot)

# Plot of mean all-cause risk curve components
plot <- ggplot(means_for_plot, aes(x = exposure, y = mean_rr*weight, color = ro))+geom_line()+
  theme_bw()+
  labs(y = "Relative Risk * Weight", x = "Exposure", title = "All cause as additive of components", subtitle = paste0("95% TMREL: ", tmrel, " (", tmrel_lower, "-", tmrel_upper, ")", "\n",
                                                                                                                      "70% TMREL: ", tmrel, " (", tmrel_lower_, "-", tmrel_upper_, ")"))
print(plot)

if(!protective_data){
  
  summary_harmful <- merge(causes, mort, by = "cause_id")
  new_row <- data.table("risk_cause" = "all-cause", 
                        tmrel_range = paste0(min(min_exposure_vec), "- ", max(min_exposure_vec)),
                        "mean" = mean(min_exposure_vec),
                        "2.5%" = quantile(min_exposure_vec, probs = 0.025),
                        "97.5%" = quantile(min_exposure_vec, probs = 0.975),
                        "15%" = quantile(min_exposure_vec, probs = 0.15),
                        "85%" = quantile(min_exposure_vec, probs = 0.85), 
                        "50%" = quantile(min_exposure_vec, probs = 0.5))
  summary_harmful <- rbind(summary_harmful, new_row, fill = T)
  
  write.csv(summary_harmful, paste0(save_dir, "/",risk_grep,"_harmfultmrelsummary_",meas_name,note, ifelse(random_effects, "_re", "_fe"), ".csv"), row.names = F, na = "") 
  
  write.csv(data.table(tmrel = min_exposure_vec, draw = paste0("draw_",0:999)), paste0(save_dir, "/",risk_grep,"_harmfultmreldraws_",meas_name,note,ifelse(random_effects, "_re", "_fe"),".csv"), row.names = F)
  
}

protective_data_tmrels <- rbindlist(lapply(causes$pair, function(p){
  
  input_data <- readRDS(paste0(mrbrt_dir, "/00_prepped_data/", p, ".RDS"))
  df <- as.data.table(input_data$df)
  
  alt_exp_study <- unique(df[,.(nid, b_1,b_0)])
  alt_exp_study[, b_midpoint := b_0 + (b_1-b_0)/2]
  
  ref_exp_study <- unique(df[,.(nid, a_1,a_0)])
  ref_exp_study[, a_midpoint := a_0 + (a_1-a_0)/2]
  
  lower <- as.numeric(quantile(alt_exp_study$b_0, 0.85))
  upper <- as.numeric(quantile(alt_exp_study$b_midpoint, 0.85))
  
  # if harmful! 
  if(p %like% "prostate"){
    lower <- as.numeric(quantile(ref_exp_study$a_midpoint, 0.15))
    upper <- as.numeric(quantile(ref_exp_study$a_1, 0.15))
    
  }
  
  tmrel_draws <- data.table("tmrel" = runif(1000, min = lower, max = upper))
  tmrel_draws[, `:=`(ro = p, cause_id = causes[pair == p, cause_id], draw = paste0("draw_",0:999), lower = lower, upper = upper)]
  return(tmrel_draws)
  
}))

rr <- merge(protective_data_tmrels, mort, by = "cause_id")
rr[, weighted_tmrel := tmrel*weight]
combined_tmrel <- rr[, .(tmrel = sum(weighted_tmrel), ro = "weighted average - draw level"), by = "draw"]

rr <- rbind(rr, combined_tmrel, fill = T)
rr[, mean_tmrel := mean(tmrel), by = "ro"]
rr[, tmrel_min := min(tmrel), by = "ro"]
rr[, tmrel_max := max(tmrel), by = "ro"]
rr[, tmrel_lower_2.5 := quantile(tmrel, 0.025), by = "ro"]
rr[, tmrel_upper_97.5 := quantile(tmrel, 0.975), by = "ro"]
rr[ro!="weighted average - draw level", `:=` (mean_tmrel=NA, tmrel_min = NA, tmrel_lower_2.5 = NA, tmrel_max = NA, tmrel_upper_97.5 = NA)]


tmrel_results <- unique(rr[,.(ro, weight, lower, upper, mean_tmrel, tmrel_min, tmrel_max, tmrel_lower_2.5, tmrel_upper_97.5)])
tmrel_results <- rbind(tmrel_results, data.table(ro = "weighted average - lower/upper", lower = tmrel_results[, sum(weight*lower, na.rm = T)],
                                                 upper = tmrel_results[, sum(weight*upper, na.rm = T)]), fill = T)


if(protective_data){
  
  write.csv(tmrel_results, paste0(save_dir, "/",risk_grep,"_weighted_datatmrel_",meas_name,note,".csv"), row.names = F, na = "")  
  
}

dev.off()