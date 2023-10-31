#
# 04_mixed_effects_models.R
#
#
library(reticulate)
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")

# set seed
np <- import("numpy")
np$random$seed(as.integer(2738))
## 

if(interactive()){
  
  version <- "VERSION"
  ro_pair <- "calcium_prostate"
  note <- "full_data"
  
  out_dir <- paste0("FILEPATH", version, "/")
  WORK_DIR <- "FILEPATH"
  test <- T
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  test <- F
  note <- ""
  
}
setwd(WORK_DIR)
source(paste0(out_dir, "/config.R"))
source("FILEPATH/helper_functions.R")
source("FILEPATH/plotting_functions.R")
source("FILEPATH/continuous_functions.R")

is_j_shaped <- ro_pair %in% J_SHAPE_RISKS
monotonicity <- DIRECTION[strsplit(ro_pair, "_")[[1]][1]][[1]]
if(ro_pair %like% "redmeat_hemstroke"){
  monotonicity <- "decreasing"
}
if(monotonicity == "decreasing" & is_j_shaped){
  is_j_shaped <- FALSE
}

# Extract selected covariates
data <- readRDS(paste0(out_dir, "03_covariate_selection_models/", ro_pair, ".RDS"))
df_data <- data$df_data
df_tmp <- data$df

# Only keep rows that are not trimmed
df_tmp <- df_tmp[as.numeric(rownames(df_data)),]

cov_names <- data$selected_covs
bias_covs <- cov_names[!cov_names == "signal"]

# Add interaction
for (cov in bias_covs) df_data[, cov] <- df_data$signal * df_tmp[, cov]

# Selected bias covariates plus signal
covs <- c("signal", bias_covs)

mrdata <- MRData()
mrdata$load_df(
  df_data,
  col_obs = c('obs'),
  col_obs_se = c('obs_se'), 
  col_study_id = c('study_id'),
  col_covs=as.list(covs)
)

# Combine cov models
cov_models <- list()
for (cov in bias_covs) cov_models <- append(cov_models, 
  list(
    do.call(
      LinearCovModel, 
      list(
        alt_cov=cov,
        use_re = F,
        # important - understand the impact of this prior! 
        prior_beta_gaussian=array(c(0, BETA_PRIOR_MULTIPLIER * data$beta_std))
      )
    )
  )
)

# Mixed effects model
cov_models <- append(cov_models, 
                     LinearCovModel('signal', use_re=TRUE, 
                    prior_beta_uniform=array(c(1.0, 1.0))))

model <- MRBRT(
  data=mrdata,
  cov_models = cov_models,
  inlier_pct = 1.0
)

model$fit_model(inner_print_level=5L, inner_max_iter=200L, 
  outer_step_size=200L, outer_max_iter=100L)

# Save model
if(!test){
  py_save_object(object = model, 
                 filename = paste0(out_dir, "04_mixed_effects_pkl_files/", ro_pair, ".pkl"), 
                 pickle = "dill")
}
# --------------------------------
# Create draws and visualizations
# --------------------------------
library(data.table)
library(ggplot2)
library(parallel)
library(gridExtra)
library(grid)
library(gtable)
source('FILEPATH/get_global_exposure_distribution.R')

# Load signal model and data in Stage 1
signal_model <- py_load_object(filename=paste0(out_dir, "01_template_pkl_files/", ro_pair, ".pkl"), 
  pickle = "dill")
orig_data <- readRDS(paste0(out_dir, "01_template_models/", ro_pair, ".RDS"))
df <- orig_data$df

# Use the CC function to get the "right" exposure distribution::: if USE_GLOBAL_DIST_PREDICT = T
# Use map to get rei_id
diet_ro_pair_map <- fread("FILEPATH/diet_ro_map.csv")
diet_ro_pair_map <- diet_ro_pair_map[include==1,]
rei_id <- unique(diet_ro_pair_map[risk_cause==ro_pair, rei_id])

# use config parameter to determine which exposure values to predict curve for
if(!USE_GLOBAL_DIST_PREDICT){
  rei_id <- NULL
}

if(length(rei_id)==1){
  NUM_POINTS <- 1000L
  include_ro_pair <- 1
  # while function still has randomness, pull from saved csvs
  exposure <- get_global_exposure_distribution(rei_id=rei_id, decomp_step="iterative", gbd_round_id=7)
  #exp_df <- fread(paste0("FILEPATH", rei_id, ".csv"))
  #exposure <- exp_df$exposure
  
}else{
  NUM_POINTS <- 100L
  include_ro_pair <- 0
  exposure_lower <- min(df[,c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)])
  exposure_upper <- max(df[,c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)])
  exposure <- seq(exposure_lower, exposure_upper, length.out=NUM_POINTS)
}

# Use function to generate draws
outputs <- generate_final_draws(out_dir, ro_pair, exposure = exposure, draw = T, normalize = is_j_shaped)
y_draws_fe <- as.data.table(outputs[[1]])
y_draws <- as.data.table(outputs[[2]])
df_final_pred <- as.data.table(outputs[[3]])
tmrel <- outputs[[4]]


# p-value
#------------------------------------(did a draw cross 0 at any point in the data rich exposure range?)
dd <- as.data.table(df_data)
dd[, a_mid := a_0 + (a_1 - a_0)/2][, b_mid := b_0 + (b_1 - b_0)/2]

min_data_rich <- quantile(dd$a_mid, 0.15)
max_data_rich <- quantile(dd$b_mid, 0.85)

data_rich_draws <- y_draws_fe[exposure > min_data_rich & exposure <= max_data_rich]
if(monotonicity == "decreasing"){
  data_rich_draws_val <- colSums(data_rich_draws >= 0)
}else{
  data_rich_draws_val <- colSums(data_rich_draws <= 0)
}
# proportion of draws that crossed 0
p_val <- mean(data_rich_draws_val > 0)


# Plotting

PAIR <- gsub("_", "-", ro_pair)
PAIR <- paste0(toupper(strsplit(PAIR, "")[[1]][1]),paste0(strsplit(PAIR, "")[[1]][-1], collapse = "" ))

monospline_header <- paste0(as.character(model$data), "\ngamma: ", round(model$gamma_soln, digits = 3))#, "\nbeta: ", round(model$beta_soln[1], digits = 3),"     gamma: ", model$gamma_soln)
if(length(bias_covs) > 0 ){
  monospline_header <- paste0(monospline_header, ", bias covs: ", paste0(bias_covs, collapse = ","))
}

if(is_j_shaped){

  monospline_header <- paste0(monospline_header, "\nJ-shaped risk, TMREL = ", tmrel)
  monospline_header <- paste0(monospline_header, "\n15-85%: ", round(min_data_rich, digits = 1),"-", round(max_data_rich, digits =1), "; P-value ", p_val)
  
}else{

  monospline_header <- paste0(monospline_header, "\n15-85%: ", round(min_data_rich, digits = 1),"-", round(max_data_rich, digits =1), "; P-value ", p_val)
}

simple_risk_curve_fe <- ggplot(df_final_pred, aes(x = b_0, y = mean_fe, ymin = lo_fe, ymax = hi_fe))+
  geom_ribbon(alpha=0.5, color = "black", fill = NA)+geom_line(color = "darkred")+theme_bw()+
  labs(y = "Relative Risk (w/o gamma)", x = "Exposure", title = PAIR, subtitle = monospline_header)


# 
data_plot_final <- plot_final_model_test2(out_dir = out_dir, ro_pair = ro_pair, exposure_vec = exposure, errorbars = T)
data_plot_final2 <- plot_final_model_test2(out_dir = out_dir, ro_pair = ro_pair, exposure_vec = exposure, errorbars = F)
data_plot_orig <- plot_final_model(out_dir = out_dir, ro_pair = ro_pair, exposure_vec = exposure, normalize = is_j_shaped)

if(ro_pair %like% "redmeat_ihd"){
  
  data_plot_final <- data_plot_final+ylim(-0.69,0.69)
  data_plot_orig <- data_plot_orig+ylim(0.50,2)
  
}


#   derivative plot

# load signal pkl model
#signal_pkl <- py_load_object(filename=paste0(out_dir, "01_template_pkl_files/", ro_pair, ".pkl"), 
#                                            pickle = "dill")
#mod1_deriv <- get_deriv(model = signal_pkl, exposure = exposure, other_covs = length(bias_covs) > 0)


# Save:
if(!test){
    pdf(paste0(out_dir, "/05_all_plots/", ro_pair, "_c_finalmodel",note,".pdf"), width = 11, height = 8)

    #grid.arrange(data_plot_final2)  
    #grid.arrange(data_plot_final)
    grid.arrange(data_plot_orig)
    print(simple_risk_curve_fe)
    #plot_deriv(model = signal_pkl, mod1_deriv, plot_title = paste0(PAIR," signal model derivative plot"), caption = "", max_data_rich = max_data_rich)
    dev.off()

    write.csv(df_final_pred, paste0(out_dir, "/05_all_csvs/", ro_pair, "_finalmodel.csv"), row.names = F)
}

#------------
# save draws
#------------
num_samples <- 1000

if(include_ro_pair ==1 ){
  
  csv_folder <- paste0(out_dir, "/05_draw_csvs/")
  
  # Add exposure values to draws
  colnames(y_draws) <- paste0("draw_", 0:(num_samples-1))
  y_draws <- cbind(data.table("exposure" = exposure), as.data.table(y_draws))
  
  colnames(y_draws_fe) <- paste0("draw_", 0:(num_samples-1))
  y_draws_fe <- cbind(data.table("exposure" = exposure), as.data.table(y_draws_fe))
  
  # Exponentiate draws so they are in normal space
  # y_draws_fe <- exponentiate_draws(y_draws_fe)
  # y_draws <- exponentiate_draws(y_draws)
  
  
  write.csv(y_draws_fe, paste0(csv_folder, "/", ro_pair, "_y_draws_fe.csv"), row.names = F)
  write.csv(y_draws, paste0(csv_folder, "/", ro_pair, "_y_draws.csv"), row.names = F)
  print("draws saved!")
  
}


