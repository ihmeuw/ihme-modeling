## Libraries #### 
USER = Sys.info()[7]
library(dplyr)
library(ggplot2)
library(stats)
library(data.table)
library(gtools) 
library(lubridate)
library(dplyr)
library(reticulate)
library(MASS)
library(RColorBrewer)
library(writexl)

reticulate::use_python("FILEPATH")
rg <- import("regmod")
rg.p <- import("regmod.prior")
source("~/00a_prep_setup.R")
source('~/db_utilities.R')
source("~/db_queries.R")

set.seed(111)

date = str_split(Sys.time(),pattern =' ')[[1]][1]
datetime = paste0(date, '_', hour(Sys.time()), '-', minute(Sys.time()), "")

# Read in data ####
setwd(paste0('FILEPATH',PREP_VERS))
read_folder = getwd()
df = readRDS(paste0(read_folder, "/cf_inputs_prepped.rds"))
df[, parent_id := NULL] 

# DROP 2020 / 2021 data
df = df[year_id < 2020, ]

if(agg_over_year == TRUE){
  id_vars = names(df)[!names(df) %in% c('no', 'yes', 'sample_size', 'year_id')]
  df = df[, lapply(.SD, sum), by = id_vars, .SDcols = c('no', 'yes', 'sample_size')]
}
if(agg_over_subnational == TRUE){
  df[, s_loc := NULL]
  id_vars = names(df)[!names(df) %in% c('no', 'yes', 'sample_size', 'location_id')]
  df = df[, lapply(.SD, sum), by = id_vars, .SDcols = c('no', 'yes', 'sample_size')]
}

active_bundles_cfs = copy(all_cf_bundles)

active_bundles_cfs[, maternal := ifelse(estimate_id %in% c(5,7,8,9),TRUE,FALSE)]
active_bundles_cfs[, cf1 := ifelse(estimate_id %in% c(2,3,4,7,8,9),TRUE,FALSE)]
active_bundles_cfs[, cf4 := ifelse(estimate_id %in% c(3,4,8,9),TRUE,FALSE)]
active_bundles_cfs[, cf5 := ifelse(estimate_id %in% c(4,9),TRUE,FALSE)]
active_bundles_cfs[, cf6 := ifelse(estimate_id %in% c(4,9),TRUE,FALSE)]
active_bundles_cfs = melt.data.table(active_bundles_cfs, id.vars = c('bundle_id', 'estimate_id', 'maternal'),
                                     variable.name = "cf", value.name = "priority_vetting")

# Merge on cause hierarchy to order plots ####
cause_map = as.data.table(readxl::read_xlsx("FILEPATH", sheet = "Causes"))
cause_map = unique(cause_map[,.(cause_id, Order)])
bundle_metadata = as.data.table(readxl::read_xlsx("FILEPATH", sheet = "bundle metadata"))
bundle_metadata = unique(bundle_metadata[, .(bundle_id, type_id, cause_rei_id)])

active_bundles_cfs_merged = merge(active_bundles_cfs, bundle_metadata, by = 'bundle_id', all.x = TRUE)
active_bundles_cfs_merged = merge(active_bundles_cfs_merged, cause_map, 
                                  by.x = 'cause_rei_id', by.y = 'cause_id', all.x = TRUE)
order_value_set = max(active_bundles_cfs_merged$Order, na.rm=TRUE)+10
active_bundles_cfs_merged[is.na(Order), Order:=order_value_set]

setorderv(active_bundles_cfs_merged, c('Order', 'bundle_id'))

# split out data by bundle and cf
# limit to just active bundles
df1 = df[bundle_id %in% unique(active_bundles_cfs_merged$bundle_id),]
df1 = merge(df1, unique(active_bundles_cfs_merged[,.(bundle_id, Order)]), by = 'bundle_id')
setorderv(df, cols = c('bundle_id', 'cf'))
df2 = split(df1, by=c('bundle_id', 'cf'))

# Run models ####
run_models <- function(x, run_mod1_only = TRUE) {
  try({
    ## RegMod Implementation #####
    Data <- rg$data$Data
    Variable <- rg$variable$Variable
    SplineVariable <- rg$variable$SplineVariable
    BinomialModel <- rg$models$BinomialModel
    
    # run with sex covariate if the data for this bundle has both sexes
    if(length(unique(x$sex_id))>1){
      data2 <- Data(
        col_obs = list("yes", "sample_size"),
        col_covs = list("age_midpoint", "sex_id"), # intercept is added by default
        df = as.data.frame(x)
      )
    } else {
      data2 <- Data(
        col_obs = list("yes", "sample_size"),
        col_covs = list("age_midpoint"), # intercept is added by default
        df = as.data.frame(x)
      )
    }
    
    variables2 <- lapply(data2$col_covs, Variable)
    
    mod1 <- BinomialModel(
      data = data2,
      param_specs = list(
        p = list(
          variables = variables2
        )
      )
    )
    
    mod1$fit()
    
    # Output to return ####
    out <- list(
      input_data = x,
      bundle = unique(x$bundle_id),
      cf = unique(x$cf)#,
    )
    
    ## RegMod model with spline ####
    if (run_mod1_only == FALSE) {
      # addl model
      Data <- rg$data$Data
      Variable <- rg$variable$Variable
      SplineVariable <- rg$variable$SplineVariable
      BinomialModel <- rg$models$BinomialModel
      
      # run with sex covariate if the data for this bundle has both sexes
      if(length(unique(x$sex_id))>1){
        data3 <- Data(
          col_obs = list("yes", "sample_size"),
          col_covs = list("age_midpoint", "sex_id"), # intercept is added by default
          df = as.data.frame(x)
        )
        
        bound1 <- 1e-5
        
        var_spline1 <- SplineVariable(
          name = "age_midpoint",  
          spline_specs = rg$variable$SplineSpecs(
            knots = seq(from=0.0,to=1.0,length.out=4), 
            knots_type = "rel_freq", 
            include_first_basis = FALSE,
            r_linear = TRUE, 
            l_linear = TRUE,  
            degree = 2L )
        )
        
        variables3 <- list(
          Variable("intercept"),
          Variable("sex_id"),
          var_spline1)
        
      } else {
        data3 <- Data(
          col_obs = list("yes", "sample_size"),
          col_covs = list("age_midpoint"), # intercept is added by default
          df = as.data.frame(x)
        )
        
        bound1 <- 1e-5
        
        var_spline1 <- SplineVariable(
          name = "age_midpoint",  
          spline_specs = rg$variable$SplineSpecs(
            knots = seq(from=0.0,to=1.0,length.out=4), 
            knots_type = "rel_freq", 
            include_first_basis = FALSE,
            r_linear = TRUE, 
            l_linear = TRUE,  
            degree = 2L )
        )
        
        variables3 <- list(
          Variable("intercept"),
          var_spline1)
      }
      
      mod2 <- BinomialModel(
        data = data3,
        param_specs = list(
          p = list(
            variables = variables3
          )
        )
      )
      
      mod2$fit()
      
      # append to out object so other models are also returned
      out2 = list(
        fitted_model_rg_spline = mod2
      )
      out = append(out, out2)
    }
    
    return(out)
  })
}

results = lapply(df2, function(x) {run_models(x, run_mod1_only = FALSE)})

# check which models, if any, had errors:
had_error = sapply(results, function(x) class(x)=="try-error")
which(had_error == TRUE)

### Save RegMod model objects ####
write_folder = paste0('FILEPATH', datetime)
if(!file.exists(write_folder)){
  dir.create(write_folder, showWarnings = FALSE)
}

lapply(results, function(x){
  try({
    py_save_object(object = x$fitted_model_rg_spline, filename = paste0(write_folder, '/regmod_', x$bundle, "_", x$cf, ".pkl"), pickle = "dill")
  })
})

# Create pred ages table for validations ####
df_preds = as.data.table(expand.grid(age_group_id = ages$age_group_id, bundle_id = unique(active_bundles_cfs_merged$bundle_id)))
df_preds = merge(df_preds, ages[,.(age_group_id, age_group_name, age_start, age_end)])

### Apply age restrictions to this table ####
rests = get_bundle_restrictions() %>% as.data.table()
age_rests = rests[,.(bundle_id, yld_age_start, yld_age_end)]
df_preds = merge(df_preds, age_rests, by = c('bundle_id'))
df_preds[, rm := 0]
# apply the lower bound age restriction
df_preds[age_start < yld_age_start, rm := 1]
df_preds[bundle_id == 3134 & age_group_id == 389, rm :=0 ]
# apply the upper bound age restriction 
df_preds[(yld_age_end >= 1) & (age_start > yld_age_end), rm := 1]  
df_preds[(yld_age_end < 1) & age_start >= 1, rm := 1] 
# remove incorrect ages
df_preds = df_preds[rm != 1]
df_preds[, rm := NULL]
df_preds[,age_midpoint := (age_start + age_end)/2]
df_preds[age_midpoint == 110, age_midpoint := 97.5]

# Predict results from models ####
predict_from_models <- function(x) {
  try({
    # Create the prediction data frame
    if (length(unique(x$input_data$sex_id))>1) {
      pred_df1_male = unique(data.frame(intercept = rep(1), 
                                        age_midpoint = df_preds[bundle_id==x$bundle, unique(age_midpoint)],
                                        sex_id = rep(1)
      ))
      pred_df1_female = unique(data.frame(intercept = rep(1), 
                                          age_midpoint = df_preds[bundle_id==x$bundle, unique(age_midpoint)],
                                          sex_id = rep(2)
      ))
      pred_df1 = rbind(pred_df1_male, pred_df1_female)
      
    } else {
      pred_df1 = unique(data.frame(intercept = rep(1), 
                                   age_midpoint = df_preds[bundle_id==x$bundle, unique(age_midpoint)]
      ))
    }
    
    ### Get draws from a variance-covariance matrix #### 
    x$fitted_model_rg_spline$opt_coefs
    x$fitted_model_rg_spline$opt_vcov 
    
    # The parameter n are the number of draws (e.g. 1000), mu are the coefficients, and Sigma the variance-covariance matrix.
    betas_simulated2 = MASS::mvrnorm(n=1000, mu=as.numeric(x$fitted_model_rg_spline$opt_coefs), Sigma=x$fitted_model_rg_spline$opt_vcov)
    # Create the prediction data frames
    if (length(unique(x$input_data$sex_id))>1) {
      order = x$fitted_model_rg_spline$data$col_covs
      
      df_pred2_tmp_male = unique(data.frame(intercept = rep(1), 
                                 age_midpoint = df_preds[bundle_id==x$bundle, unique(age_midpoint)],
                                 sex_id = rep(1)
      ))
      df_pred2_tmp_female = unique(data.frame(intercept = rep(1), 
                                   age_midpoint = df_preds[bundle_id==x$bundle, unique(age_midpoint)],
                                   sex_id = rep(2)
      ))
      df_pred2_tmp = rbind(pred_df1_male, pred_df1_female)
      
      df_pred2_tmp = df_pred2_tmp[, order]
      pred_df2 = rg$data$Data(df = df_pred2_tmp, col_covs = order)
    } else {
      df_pred2_tmp = unique(data.frame(
        age_midpoint = df_preds[bundle_id==x$bundle, unique(age_midpoint)],
        intercept = rep(1)))
      pred_df2 = rg$data$Data(df = df_pred2_tmp, col_covs = c("intercept", "age_midpoint"))
    }

    yhat2 = qlogis(as.matrix(do.call('rbind', lapply(1:nrow(betas_simulated2), function(i){
      coefs_tmp = betas_simulated2[i, ]
      x$fitted_model_rg_spline$params[[1]]$get_param(coefs_tmp, pred_df2)
    })
    )))
    
    ### Get uncertainty interval calculated from the draws ####
    pred_df1$mean_rg = qlogis(x$fitted_model_rg_spline$params[[1]]$get_param(as.numeric(x$fitted_model_rg_spline$opt_coefs), pred_df2)) 
    pred_df1$se_rg = apply(yhat2, 2, sd)
    pred_df1$upper_rg = apply(yhat2, 2, function(x){quantile(x, 0.975)})
    pred_df1$lower_rg = apply(yhat2, 2, function(x){quantile(x, 0.025)})
    
    all_draws = t(yhat2)
    all_draws = as.data.table(all_draws)
    names = paste0("draw_", seq(1, 1000, 1))
    setnames(all_draws, names(all_draws), names)
    all_draws = cbind(df_pred2_tmp, all_draws)
    
    if (length(unique(x$input_data$sex_id))>1) { 
      knots = x$fitted_model_rg_spline$params[[1]]$variables[[3]]$spline$knots
    } else {
      knots = x$fitted_model_rg_spline$params[[1]]$variables[[2]]$spline$knots
    }
    
    ## Output to return ####
    out <- list(
      bundle = x$bundle,
      cf = x$cf,
      predictions = pred_df1,
      input_data = x$input_data,
      draws_rg = all_draws,
      knots_rg = knots
    )
    
    return(out)
    
  })
}

### Run and save predictions ####
predictions = lapply(results, function(x) {predict_from_models(x)})
# check which preds, if any, had errors:
had_error = sapply(predictions, function(x) class(x)=="try-error")
which(had_error == TRUE)

write_folder = paste0('FILEPATH', datetime)
if(!file.exists(write_folder)){
  dir.create(write_folder, showWarnings = FALSE)
}

lapply(predictions, function(x){
  try({
  saveRDS(object = x, file = paste0(write_folder,"/preds_", x$bundle, "_", x$cf, "_correctedAges.rds"))
  })
})

# Model plots ####
myColors <- brewer.pal(8,"Set1")
names(myColors) <- c("Marketscan", "POL", "NZL", "HCUP", "TWN", "", "Medicare", "PHL")
myColors["Medicare"]="#03002e"
myColors["HCUP"]="#800080"
myColors["Marketscan"] = "#C70039"

plot_preds <- function(x) {
  try({
  df = copy(x$input_data)
  df[, ratio_to_plot := yes/(yes+no)]
  
  # modify where cell size is less than 11 for HCUP/Medicare
  df[, modeled_data := ifelse(yes<11, TRUE, FALSE)]
  df[!source %in% c('HCUP', 'Medicare'), modeled_data := FALSE]
  for(i in 1:nrow(df)) {
    if (df[i, modeled_data] == TRUE) {
      df[i, numerator_with_noise := rbinom(n = 1, size = sample_size, prob = ratio_to_plot)]
    }
  }
  df[modeled_data == TRUE, ratio_to_plot := numerator_with_noise/(numerator_with_noise+no)]
  
  # modify where ratio is 0 or 1 because you can't take the logit of those values
  df[ratio_to_plot == 1, modified_1_or_0 := TRUE]
  df[ratio_to_plot == 0, modified_1_or_0 := TRUE]
  df[is.na(modified_1_or_0), modified_1_or_0 := FALSE]
  max_value = max(df[ratio_to_plot<1, ratio_to_plot])
  df[ratio_to_plot == 1, ratio_to_plot := max_value + ((1-max_value)/2)]
  min_value = max(df[ratio_to_plot>0, ratio_to_plot])
  df[ratio_to_plot == 0, ratio_to_plot := min_value + ((0+min_value)/2)]
  
  subsetColors = myColors[names(myColors) %in% unique(df$source)]
  
  knots = x$knots_rg
  
  df[modified_1_or_0 == TRUE, modified_data := 'changed_1_or_0']
  df[modeled_data == TRUE, modified_data := 'modeled_estimate'] 
  df[is.na(modified_data), modified_data := 'no_change']
  
  setorderv(df, 'modified_data')
  
  shapeValues = c(1, 8, 16)
  names(shapeValues) <- c("changed_1_or_0", "modeled_estimate", "no_change")
  subsetShapes = shapeValues[names(shapeValues) %in% unique(df$modified_data)]
  
  plot = ggplot(df,
                 aes(x = age_midpoint,
                     y = logit(ratio_to_plot),
                     color = source)) +
            geom_point(alpha = 0.5, aes(size = (yes + no), shape = modified_data)) +
            scale_size(range = c(1, 7), name = "Sample size") +
            scale_shape_manual(name = "Data changes", values = subsetShapes) +
            theme_bw() +
            labs(x = 'Age midpoint', y = 'CF ratio, logit transformed') +
            ggtitle(paste0("Bundle ", unique(df$bundle_id), " - ",  unique(df$bundle_name), " - ", unique(df$cf))) + 
            geom_line(data = x$predictions, aes(x= age_midpoint, y= mean_rg), inherit.aes = FALSE, alpha = 0.5)+
            geom_ribbon(data = x$predictions, aes(x= age_midpoint, ymin= lower_rg, ymax= upper_rg), fill = "steelblue", alpha = 0.2, inherit.aes = FALSE) +
            scale_colour_manual(name = "Source",values = subsetColors)
  
  if( length(unique(df$sex_id))>1 ) plot = plot + facet_wrap(~ as.factor(sex_id), labeller = as_labeller(c(`1` = "male", `2` = "female")))
  
  for(i in 2:(length(knots)-1)){
    plot = plot + geom_vline(xintercept = knots[i],
                             color = "blue", alpha = 0.3, size=0.3) +
                  labs(caption = "Vertical blue lines show knot placements")
  }
  
  ## Output to return ####
  out <- list(
    bundle = x$bundle,
    cf = x$cf,
    plot = plot,
    input_data = x$input_data
  )
  return(out)
 })
}

### Save plots ####
plots = lapply(predictions, function(x) {plot_preds(x)})
# check which plots, if any, had errors:
had_error = sapply(plots, function(x) class(x)=="try-error")
which(had_error == TRUE)

write_folder = paste0('FILEPATH', datetime)
if(!file.exists(write_folder)){
  dir.create(write_folder, showWarnings = FALSE)
}
lapply(plots, function(x) {
  try({
    pdf(file = paste0(write_folder,"/modelplot_", x$bundle, "_", x$cf, ".pdf"))
    print(x$plot)
    dev.off()
  })
})

# Linear space model plots ####
plot_preds_linear <- function(x) {
  try({
    df = copy(x$input_data)
    df[, ratio_to_plot := yes/(yes+no)]
    subsetColors = myColors[names(myColors) %in% unique(df$source)]
    
    # modify where cell size is less than 11 for HCUP/Medicare
    df[, modeled_data := ifelse(yes<11, TRUE, FALSE)]
    df[!source %in% c('HCUP', 'Medicare'), modeled_data := FALSE]
    for(i in 1:nrow(df)) {
      if (df[i, modeled_data] == TRUE) {
        df[i, numerator_with_noise := rbinom(n = 1, size = sample_size, prob = ratio_to_plot)]
      }
    }
    df[modeled_data == TRUE, ratio_to_plot := numerator_with_noise/(numerator_with_noise+no)]

    df[modeled_data == TRUE, modified_data := 'modeled_estimate'] # note, since these are in linear space we won't have the modified 0s and 1s problem
    df[is.na(modified_data), modified_data := 'no_change']
    
    setorderv(df, 'modified_data')
    
    shapeValues = c(1, 8, 16)
    names(shapeValues) <- c("changed_1_or_0", "modeled_estimate", "no_change")
    subsetShapes = shapeValues[names(shapeValues) %in% unique(df$modified_data)]
    
    if (x$cf == 'cf1') {
      plot = ggplot(df,
                    aes(x = age_midpoint,
                        y = ratio_to_plot,
                        color = source)) +
        geom_point(alpha = 0.5, aes(size = (yes + no), shape = modified_data)) +
        scale_size(range = c(1, 7), name = "Sample size") +
        scale_shape_manual(name = "Data changes", values = subsetShapes) +
        theme_bw() +
        labs(x = 'Age midpoint', y = 'CF ratio', caption = 'Note: dashed grey line shows y=1, or no correction') +
        ggtitle(paste0("Bundle ", unique(df$bundle_id), " - ",  unique(df$bundle_name), " - ", unique(df$cf))) + 
        geom_hline(yintercept=1, linetype="dashed", color = "grey", alpha = 0.7) +
        geom_line(data = x$predictions, aes(x= age_midpoint, y= plogis(mean_rg)), inherit.aes = FALSE, alpha = 0.5)+
        geom_ribbon(data = x$predictions, aes(x= age_midpoint, ymin= plogis(lower_rg), ymax= plogis(upper_rg)), fill = "steelblue", alpha = 0.2, inherit.aes = FALSE) +
        scale_colour_manual(name = "Source",values = subsetColors) 
      
      if( length(unique(df$sex_id))>1 ) plot = plot + facet_wrap(~ as.factor(sex_id), labeller = as_labeller(c(`1` = "male", `2` = "female")))
      
    } else {
      
      plot = ggplot(df,
                    aes(x = age_midpoint,
                        y = 1/ratio_to_plot,
                        color = source)) +
        geom_point(alpha = 0.5, aes(size = (yes + no), shape = modified_data)) +
        scale_size(range = c(1, 7), name = "Sample size") +
        scale_shape_manual(name = "Data changes", values = subsetShapes) +
        theme_bw() +
        labs(x = 'Age midpoint', y = 'CF ratio', caption = 'Note: dashed grey line shows y=1, or no correction') +
        ggtitle(paste0("Bundle ", unique(df$bundle_id), " - ", unique(df$bundle_name), " - 1/", unique(df$cf))) + 
        geom_hline(yintercept=1, linetype="dashed", color = "grey", alpha = 0.7) +
        geom_line(data = x$predictions, aes(x= age_midpoint, y= 1/(plogis(mean_rg))), inherit.aes = FALSE, alpha = 0.5)+
        geom_ribbon(data = x$predictions, aes(x= age_midpoint, ymin= 1/(plogis(lower_rg)), ymax= 1/(plogis(upper_rg))), fill = "steelblue", alpha = 0.2, inherit.aes = FALSE) +
        expand_limits(y=0) +
        scale_colour_manual(name = "Source",values = subsetColors)
    
      if( length(unique(df$sex_id))>1 ) plot = plot + facet_wrap(~ as.factor(sex_id), labeller = as_labeller(c(`1` = "male", `2` = "female")))
    }
    ## Output to return ####
    out <- list(
      bundle = x$bundle,
      cf = x$cf,
      plot = plot,
      input_data = x$input_data
    )
    return(out)
  })
}

### Save plots ####
plots_linear = lapply(predictions, function(x) {plot_preds_linear(x)})
# check which plots, if any, had errors:
had_error = sapply(plots, function(x) class(x)=="try-error")
which(had_error == TRUE)

write_folder = paste0('FILEPATH', datetime)
if(!file.exists(write_folder)){
  dir.create(write_folder, showWarnings = FALSE)
}
lapply(plots_linear, function(x) {
  try({
    pdf(file = paste0(write_folder,"/modelplot_linear_", x$bundle, "_", x$cf, ".pdf"))
    print(x$plot)
    dev.off()
  })
})
