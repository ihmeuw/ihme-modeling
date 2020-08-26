#############################################################
##Date: 3/4/2019
##Purpose: Review and plot MR-BRT models
##Notes: As of writing this, about half the models failed. Need to investigate but want to do some prelim diagnostics
##Updates: As of 3/26, use this to make plots
#
###########################################################
user <- Sys.info()[7]
library(ggplot2)
library(data.table)
library(stringr)
library(gridExtra)
library(RMySQL)
source(paste0(db_utilities.R))

####### Functions #######
loadBundles <- function(bundle_ids) {
  # Get bundles for which we have scalars
  db_con = fread(paste0(FILEPATH))
  
  # Get list of ids and names
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$username,
                   password = db_con$pass,
                   host = db_con$host)
  df <- dbGetQuery(con, sprintf("
                                SELECT
                                b.bundle_id,
                                b.bundle_name,
                                c.cause_name,
                                c.sort_order
                                FROM
                                bundle.bundle b
                                LEFT JOIN
                                shared.cause_hierarchy_history c ON b.cause_id = c.cause_id AND c.cause_set_version_id = shared.active_cause_set_version(2, 4)
                                WHERE
                                b.bundle_id IN (%s)",
                                paste0(bundle_ids, collapse = ",")))
  df <- data.table(df)
  df <- df[is.na(sort_order), sort_order := 9999]
  df <- df[order(sort_order)][, sort_order := NULL]
  dbDisconnect(con)
  return(df)
}

mround <- function(x,base){ 
  base*round(x/base) 
} 

###### Files and format #####
# pull out ones that broke to test out for CF3
pullestimates <- function(filename){
  info <- str_split(filename, 'by_cfbundle/mrbrt_')[[1]][2]
  bun <- as.numeric(str_split(info, '_')[[1]][1])
  cf <- str_split(info, '_|/')[[1]][2]
  estimate <- list('bun' = bun, 'cf' = cf)
  return(estimate)
}

# Takes folder for MR BRT output of CF model and compares outputs / step in the process that broke
cf_modelcheck <- function(data_folder){
  # Get files
  input_data_files <- Sys.glob(paste0(data_folder,'by_cfbundle/*/input_data.csv'))
  train_data_files <- Sys.glob(paste0(data_folder,'by_cfbundle/*/train_data.csv'))
  results_files <- Sys.glob(paste0(data_folder,'by_cfbundle/*/model_summaries.csv'))
  #draws_files <- Sys.glob(paste0(data_folder,'by_cfbundle/*/model_draws.csv'))
  #coef_files <- Sys.glob(paste0(data_folder,'by_cfbundle/*/model_coefs.csv'))
  #spline_files <- Sys.glob(paste0(data_folder,'by_cfbundle/*/spline_specs.csv'))
  
  input_bundles <- data.table()
  data_bundles <- data.table()
  results_bundles <- data.table()
  
  for(file in input_data_files) input_bundles <- rbind(input_bundles, pullestimates(file))
  for(file in train_data_files) data_bundles <- rbind(data_bundles, pullestimates(file))
  for(file in results_files) results_bundles <- rbind(results_bundles, pullestimates(file))
  
  b1 <- setdiff(input_bundles, data_bundles) %>%
    mutate(issue = 'input data exists but train dataset does not')
  issues <- setdiff(data_bundles, results_bundles) %>%
    mutate(issue = 'train data exists but result dataset does not') %>%
    rbind(b1)
  return(issues)
}

# Plotting -----
cf_plots <- function(bun, cf, data_folder){
  restrictions <- get_icg_restrictions()
  max_map <- max(restrictions$map_version)
  bundle_map <- bundle_icg() %>% merge(restrictions[map_version == max_map], by = c('icg_id', 'icg_name')) %>% .[, c('bundle_id', 'male', 'female', 'yld_age_start', 'yld_age_end')]
  bundle_map[, min_age := min(yld_age_start), by = 'bundle_id'][, max_age := max(yld_age_end), by = 'bundle_id']
  bundle_map <- unique(bundle_map[, c('bundle_id', 'min_age', 'max_age')])
  bundle_map[max_age == 95, max_age := 110]
  
  a <- tryCatch({
    restrictions_map <- bundle_map[bundle_id == bun]
    
    train_data <- fread(paste0(data_folder,'by_cfbundle/mrbrt_',bun,'_',cf,'/train_data.csv'))
    model_sum <- fread(paste0(data_folder,'by_cfbundle/mrbrt_',bun,'_',cf,'/model_summaries.csv'))
    
    if('X_sex_id' %in% names(model_sum)){
      setnames(model_sum, c('X_sex_id', 'X_age_start'), c('sex_id', 'age_start'))
    } else{
      setnames(model_sum, 'X_age_start', 'age_start')
    }
    
    bun_df <- loadBundles(bun)
    bundle_name <- bun_df[bundle_id == bun]$bundle_name[1]
    print(bundle_name)
    
    train_data[w < 0.5, trimmed := 'Trimmed data'][w >= 0.5, trimmed := 'Untrimmed data']
    train_data[sex_id == 1, sex := 'Males'][sex_id == 2, sex := 'Females']
    num_sex <- length(unique(train_data$sex))
    
    if(cf != 'cf1'){
      data_plot <- ggplot(train_data, aes(x = age_start, y = exp(log_mean), color = factor(trimmed)))
    } else{
      data_plot <- ggplot(train_data, aes(x = age_start, y = inv.logit(logit_mean), color = factor(trimmed)))
    }
    
    data_plot <- data_plot +
      geom_point() + theme_bw() +
      ylab('GBD 2019 CF input data') + xlab('Age') + 
      ggtitle(paste0(bun, " - ", bundle_name, ' ', cf, ' input data')) +
      scale_color_manual(name = '', values = c('#33cc33', '#D354AC')) +
      facet_wrap(~sex) +
      xlim(c(restrictions_map$min_age[1],restrictions_map$max_age[1]))
    
    # Change sex based on input data. Should only be NA if the prediction data frame added it on incorrectly
    if(nrow(model_sum[is.na(sex_id)]) > 0){
      one_sex <- unique(train_data$sex_id)
      model_sum[is.na(sex_id), sex_id := one_sex]
      model_sum <- model_sum[sex_id == one_sex]
      model_sum <- unique(model_sum)
    }
    if('sex_id' %in% names(model_sum)){
      model_sum[sex_id == 1, sex := 'Males'][sex_id == 2, sex := 'Females']
    } else{
      model_sum[, sex := 'Females'] # Arbitrary. Should be obvious from the bundle itself
    }
    model_sum[, hi_hi := max(Y_mean_hi), by = c('age_start', 'sex')]
    model_sum[, lo_lo := min(Y_mean_lo), by = c('age_start', 'sex')]
    
    # Fixes ages 
    model_sum <- model_sum[age_start > restrictions_map$min_age[1] & age_start < restrictions_map$max_age[1] + 5]
    
    
    
    # Get out of logit space for cf1, logspace for cf2 & cf3
    if(cf != 'cf1'){
      results_plot <- ggplot(unique(model_sum), aes(x = age_start, y = exp(Y_mean), ymin = exp(lo_lo), ymax = exp(hi_hi)))
    } else {
      results_plot <- ggplot(unique(model_sum), aes(x = age_start, y = inv.logit(Y_mean), ymin = inv.logit(lo_lo), ymax = inv.logit(hi_hi)))
    }
    
    results_plot <- results_plot +
      geom_point(color = 'black') + theme_bw() +
      ylab('CF model predictions') + xlab('Age') +
      ggtitle(paste0(bundle_name, ' model comparison')) +
      facet_wrap(~sex) +
      xlim(c(restrictions_map$min_age[1],restrictions_map$max_age[1]))
    
    results_plot_UI <- results_plot +
      geom_ribbon(fill = 'gray', alpha = 0.5)
    
    
    all <- grid.arrange(data_plot, results_plot, results_plot_UI, nrow = 3)
    print(all) 
  },
  error = function(cond){
    message(paste0("bundle ", bun," ", cf, " broke"))
  })
  
  if(inherits(a, "error")) next
}

#bundles <- c(125:128)
#broken_buns <- c()
bundles <- results_bundles

for(row in 1:nrow(bundles)){
  print(bundles[row])
  bun <- row$bun
  cf <- row$cf

  # Get files and assign the bundle from filepath
  cf_plots(bundle, cf, data_folder)
}

