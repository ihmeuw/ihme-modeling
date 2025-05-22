#########################################################################################
### Purpose: OOS testing for model selection; this is not 
# needed for pipeline - simply an option for model selection 
#########################################################################################

code_root <- "FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
}

# Load packages/central functions
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_covariate_estimates.R")
source("/FILEPATH/get_bundle_version.R")
source(paste0(code_root, '/FILEPATH/processing.R'))
library("jtools", lib.loc = '/FILEPATH/')
library(lme4)
library(AICcmodavg)
library(readxl)
pacman::p_load(data.table,dplyr,tidyr,stringr)
date <- Sys.Date()

# Set run dir
run_file <- fread(paste0(data_root, "/FILEPATH/run_file.csv"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")
draws_dir    <- paste0(run_dir, "/draws/")
interms_dir    <- paste0(run_dir, "/interms/")

## Set constants
run_oos <- TRUE
n_folds <- 5
set.seed(SEED)
release_id <- ADDRESS
years <- 1980:2024
worm <- 'WORM'

## Create directories
working_dir <- paste0(interms_dir, '/', worm, '/') 

ifelse(!dir.exists(paste0(working_dir)), dir.create(paste0(working_dir)), FALSE)
ifelse(!dir.exists(paste0(working_dir, 'rmse/')), dir.create(paste0(working_dir, 'rmse/')), FALSE)
ifelse(!dir.exists(paste0(working_dir, 'rmse/', date)), dir.create(paste0(working_dir, 'rmse/', date)), FALSE)

for (i in 0:n_folds){
  ifelse(!dir.exists(paste0(working_dir, "rmse/", date, "/fold_", i)), dir.create(paste0(working_dir, "rmse/", date, "/fold_", i)), FALSE)
  ifelse(!dir.exists(paste0(working_dir, "rmse/", date, "/fold_", i, "/model/")), dir.create(paste0(working_dir, "rmse/", date, "/fold_", i, "/model/")), FALSE)
  ifelse(!dir.exists(paste0(working_dir, "rmse/", date, "/fold_", i, "/preds/")), dir.create(paste0(working_dir, "rmse/", date, "/fold_", i, "/preds/")), FALSE)
}

#========================================
## CALCULATE IS/OOS RMSE
#========================================
models <- read_xlsx(paste0(params_dir, '/', worm, 'FILEPATH.xlsx'))
models <- subset(models, result == 'keep')
cw_id <- unique(models$cw_vsn)

models$rowid <- seq.int(nrow(models))
models$model_id <- paste0('model_', models$rowid)


if (!run_oos & n_folds != 0) {
  message("Resetting n_folds to 0 since run_oos = FALSE.")
  message("If you meant to run an out-of-sample model, please check your options.")
  n_folds <- 0
}

df_input <- fread(paste0(params_dir, '/', worm, 'FILEPATH', cw_id,'.csv'))
setnames(df_input, 'val', 'data')

if (run_oos == TRUE) {
  
  holdout_link_table <- data.table(ihme_loc_id = unique(df_input$ihme_loc_id))
  
  random_draws <- rnorm(nrow(holdout_link_table))
  quantiles <- quantile(random_draws, 0:n_folds/n_folds)
  
  which_folds <- cut(random_draws, quantiles, include.lowest = TRUE)
  levels(which_folds) <- 1:n_folds
  
  holdout_link_table[, fold_id := as.numeric(which_folds)]
  
  df_input <- merge(df_input, holdout_link_table, by = "ihme_loc_id")
  
}

model.list <- unique(models$model)

model_df <- NULL
for (i in length(models$model)){
  
  model <- model.list[[i]]
  
  model_id <- paste0('model_', i)
  df <- data.table(model_id = model_id, model = model)
  
  model_df <- rbind(model_df,df)
  
}

for (i in seq_along(model.list)){
  for (fold in 0:n_folds) {
    
    if (fold != 0) {
      
      df_input_fold <- df_input[fold_id != fold]
      
    } else {
      
      df_input_fold <- copy(df_input)
      
    }
    
    model <- lmer(model.list[[i]], data = df_input_fold)
    
    
    # Save the model output
    saveRDS(model, file = paste0(working_dir, "rmse/", date, "/fold_", fold, "/model/", "model_", i, ".rds"))
    
    
    if (fold == 0) {
      
      pred_df <- as.data.table(subset(df_input, select = c("ihme_loc_id", "location_id","location_name", "year_id", "sdi", "universal_health_coverage", "haqi", "sanitation_prop",'handwashing', "data", "region_name",'region_id','super_region_name','super_region_id')))  

      pred_df$predicted_is <- predict(model, pred_df, type = 'response', allow.new.levels = TRUE) 
      pred_df$model <- paste0('model_', i)
      
    } else if (fold != 0) {
      

      pred_df <- as.data.table(subset(df_input, fold_id == fold)) 
      pred_df$predicted_oos <- predict(model, pred_df, type = 'response', allow.new.levels = TRUE)
      pred_df <- subset(pred_df, select = c("ihme_loc_id", "location_id","location_name", "year_id", "sdi", "universal_health_coverage", "haqi", 
                                            "sanitation_prop","handwashing", "data",'predicted_oos','fold_id',"region_name",'region_id','super_region_name',
                                            'super_region_id'))
      pred_df$model <- paste0('model_', i)
      
    }
    

    saveRDS(pred_df, file = paste0(working_dir, "rmse/", date, "/fold_", fold, "/preds/", "preds_", i, ".rds"))
    
  }
}

in_sample <- lapply(1:length(model.list), function(i) {
  readRDS(paste0(working_dir, "rmse/", date, "/fold_0/preds/preds_", i, ".rds"))
}) %>% rbindlist

out_of_sample <- data.table()
for(i in 1:n_folds){
  
  input_path <- (paste0(working_dir, "rmse/", date, "/fold_", i))
  
  for (k in 1:length(model.list)) {
    
    oos_data <- readRDS(paste0(input_path, "/preds/preds_", k, ".rds"))
    out_of_sample <- rbind(oos_data, out_of_sample)
    
  }
}

data_to_bind <- subset(out_of_sample, select = c('data','location_id','year_id'))

out_of_sample[, data := NULL]
in_sample[, data := NULL]

out_of_sample <- unique(out_of_sample)
in_sample <- unique(in_sample)

nrow(df_input) == nrow(out_of_sample)

df_is_oos <- merge(in_sample, 
                   subset(out_of_sample, select = c("location_id", "year_id", "predicted_oos", "fold_id", "model")),
                   by = c("location_id", "year_id", "model"), allow.cartesian = TRUE)

df_is_oos <- merge(df_is_oos, data_to_bind, by = c('location_id','year_id'), allow.cartesian = TRUE)

rmse = function(m, o){
  sqrt(mean((m - o)^2, na.rm = TRUE))
}

df_is_oos[, is_rmse := lapply(.SD, function(x) rmse(get('data'), get('predicted_is'))), by = c('model')]
df_is_oos[, oos_rmse := lapply(.SD, function(x) rmse(get('data'), get('predicted_oos'))), by = c('model')]

df_is_oos$min_oos_rmse <- min(df_is_oos$oos_rmse)

model_select <- unique(subset(df_is_oos, select = c('model','oos_rmse')))
setnames(model_select, 'model','model_id')


model_oos <- left_join(models, model_select, by = 'model_id')
write.xlsx(model_oos, paste0(params_dir, '/', worm, 'FILEPATH.xlsx'), rowNames = FALSE)