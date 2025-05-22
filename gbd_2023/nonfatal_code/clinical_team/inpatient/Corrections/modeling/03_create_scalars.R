## Libraries #### 
USER = Sys.info()[7]
library(ggplot2)
library(stats)
library(data.table)

source('~/db_utilities.R')
source("~/db_queries.R")
source("~/00a_prep_setup.R")

setwd(paste0('FILEPATH', PREP_VERS))
read_folder = getwd()
df = readRDS(paste0(read_folder, "/cf_inputs_prepped.rds"))
df[,parent_id:=NULL]  

if(agg_over_year == TRUE){
  id_vars = names(df)[!names(df) %in% c('no', 'yes', 'sample_size', 'year_id')]
  df = df[, lapply(.SD, sum), by = id_vars, .SDcols = c('no', 'yes', 'sample_size')]
}
if(agg_over_subnational == TRUE){
  df[, s_loc := NULL]
  id_vars = names(df)[!names(df) %in% c('no', 'yes', 'sample_size', 'location_id')]
  df = df[, lapply(.SD, sum), by = id_vars, .SDcols = c('no', 'yes', 'sample_size')]
}

# model version to pull in
datetime = "MODEL_ID"
read_folder = paste0('FILEPATH', datetime)

active_bundles_cfs = copy(all_cf_bundles)
active_bundles_cfs[, maternal := ifelse(estimate_id %in% c(5,7,8,9),TRUE,FALSE)]
active_bundles_cfs[, cf1 := ifelse(estimate_id %in% c(2,3,4,7,8,9),TRUE,FALSE)]
active_bundles_cfs[, cf4 := ifelse(estimate_id %in% c(3,4,8,9),TRUE,FALSE)]
active_bundles_cfs[, cf5 := ifelse(estimate_id %in% c(4,9),TRUE,FALSE)]
active_bundles_cfs[, cf6 := ifelse(estimate_id %in% c(4,9),TRUE,FALSE)]
active_bundles_cfs = melt.data.table(active_bundles_cfs, id.vars = c('bundle_id', 'estimate_id', 'maternal'),
                                     variable.name = "cf", value.name = "present")

# merge bundle names
bun_names = loadBundles(active_bundles_cfs$bundle_id)
active_bundles_cfs = merge(active_bundles_cfs, bun_names[,.(bundle_id, bundle_name)], by = 'bundle_id')

# Calculate scalars#####
### Read in predictions #####
files = list.files(paste0(read_folder,'/'))
files = files[grepl("correctedAges", files)]
predictions = list()
for(x in files) {
  list1 = readRDS(paste0(read_folder, '/', x))
  cf = list1$cf
  bun_id = list1$bundle
  name = paste0(bun_id, '.', cf)
  predictions[[name]] = list1
}

### Function to create scalars ####
create_cf_scalars <- function(bun_id) {
  try({
    # A list of names of bun.cf to subset predictions
    bun_cfs = c(paste0(bun_id, '.cf1'), paste0(bun_id, '.cf4'), paste0(bun_id, '.cf5'), paste0(bun_id, '.cf6'))
    preds_subset = predictions[bun_cfs]
    
    df_preds_both = lapply(preds_subset, function(x) {
      try({
      data1 = as.data.table(x$draws_rg)
      data1[, cf := unique(x$cf)]
      data1[, bundle_id := unique(x$bundle)]
      # Use this mean from the model predictions to calculate the mean for the CF scalar 
      data2 = x$predictions
      data2$cf = unique(x$cf)
      data2$bundle_id = unique(x$bundle)
      out = list(
        df_preds = data1, 
        df_mean = data2
      )
      return(out)
      })
    })

    # Subset to preds without errors
    had_error = sapply(df_preds_both, function(x) class(x)=="try-error")
    which(had_error == TRUE)
    df_preds_both = df_preds_both[which(had_error == FALSE)]

    # Data table of draws by cf for the bundle
    df_preds = rbindlist(lapply(df_preds_both, function(x) {return(x$df_preds)}))
    # Data table of point estimates by cf for the bundle
    df_means = rbindlist(lapply(df_preds_both, function(x) {return(x$df_mean)}))
    try({df_preds[, intercept := NULL]})
    
    # vars to apply functions by
    id.vars = c('bundle_id', 'cf', 'age_midpoint')
    # if sex_id was a covariate, include it here:
    if(length(unique(df_preds$sex_id))>1) id.vars = c(id.vars, 'sex_id')
    # columns to apply functions to all the other columns that aren't id vars
    sdcols1 = names(df_preds)[!names(df_preds) %in% id.vars]
    
    # get inverse logit of each draw for all CFs - put results into linear/scalar space
    df_preds2 = df_preds[, lapply(.SD, function(x) plogis(x) ), 
                         .SDcols = sdcols1, 
                         by = id.vars]
    
    # split df_preds2 out by cf
    df = split(df_preds2, df_preds2$cf)  # these are all in linear/scalar space
    
    # get reciprocal of cf4 and cf5
    inverse_df = list()
    inverse_df$cf4 = df$cf4[, lapply(.SD, function(x) {1/x} ), 
                            .SDcols = sdcols1, 
                            by = id.vars]
    
    inverse_df$cf5 = df$cf5[, lapply(.SD, function(x) {1/x} ), 
                            .SDcols = sdcols1, 
                            by = id.vars]
    
    # NOTE: at this point, we have draw level cf results in linear/scalar space -
    # cf1 is within df$cf1
    # cf4 is within inverse_df$cf4
    # cf5 is within inverse_df$cf5
    
    # order by id vars for combining cf results into scalars
    setorderv(df$cf1, id.vars)
    setorderv(inverse_df$cf4, id.vars)
    if (bun_id != 10105) setorderv(inverse_df$cf5, id.vars)
    
    # non-primary scalar is CF1 * the inverse of CF4 (both are in linear space here after plogis() to take inv.logit above)
    dn_ages_cf4 = unique(inverse_df$cf4$age_midpoint)
    dn_ages_cf1 = unique(df$cf1$age_midpoint)
    
    cf1_dn = as.matrix(df$cf1[age_midpoint %in% dn_ages_cf4, sdcols1, with = FALSE])
    cf4_dn = as.matrix(inverse_df$cf4[age_midpoint %in% dn_ages_cf1, sdcols1, with = FALSE])
    dn_scalar = cf1_dn * cf4_dn

    # non-primary + outpatient scalar is CF1 * the inverse of CF5 (both are in linear space here after plogis() to take inv. logit above)
    dno_ages_cf5 = unique(inverse_df$cf5$age_midpoint)
    dno_ages_cf1 = unique(df$cf1$age_midpoint)
    cf1_dno = as.matrix(df$cf1[age_midpoint %in% dno_ages_cf5, sdcols1, with = FALSE])
    if (bun_id != 10105) {
      cf5_dno = as.matrix(inverse_df$cf5[age_midpoint %in% dno_ages_cf1, sdcols1, with = FALSE])
      dno_scalar = cf1_dno * cf5_dno
    }
    
    # create draws data frame for scalars:
    scalar_draws = data.table()
    scalar_draws = rbind(scalar_draws, df$cf1)
    scalar_draws[cf == 'cf1', scalar := "d_scalar"]
    scalar_draws[cf == 'cf1', target_estimate_id := "2"]
    scalar_draws[, cf := NULL]
    
    dn_scalar_idvars = inverse_df$cf4[age_midpoint %in% dn_ages_cf1, id.vars, with = FALSE]
    dn_scalar_draws = cbind(dn_scalar_idvars, dn_scalar)
    dn_scalar_draws[cf == 'cf4', scalar := "dn_scalar"]
    dn_scalar_draws[cf == 'cf4', target_estimate_id := "3"]
    dn_scalar_draws[, cf := NULL]
    scalar_draws = rbind(scalar_draws, dn_scalar_draws)
    
    if (bun_id != 10105) {
      dno_scalar_idvars = inverse_df$cf5[age_midpoint %in% dno_ages_cf1, id.vars, with = FALSE]
      dno_scalar_draws = cbind(dno_scalar_idvars, dno_scalar)
      dno_scalar_draws[cf == 'cf5', scalar := "dno_scalar"]
      dno_scalar_draws[cf == 'cf5', target_estimate_id := "4"]
      dno_scalar_draws[, cf := NULL]
      scalar_draws = rbind(scalar_draws, dno_scalar_draws)
    }
    
    # create final prediction data frame
    # first, get id cols from df
    pred_d_scalar = df$cf1[,id.vars, with = FALSE]
    # calculate mean from the equivalent of the point estimates:
    # use the mean_rg from model results
    pred_d_scalar = merge(pred_d_scalar, df_means[, c(id.vars, "mean_rg"), with = FALSE], by = id.vars, all.x = TRUE)
    # take inv logit of CF1 mean to get deduplication (CF1) scalar
    pred_d_scalar$scalar_estimate = plogis(pred_d_scalar$mean_rg)
    # get uncertainty interval from the draws
    cf1 = as.matrix(df$cf1[, sdcols1, with = FALSE])
    pred_d_scalar$scalar_upper = apply(cf1, 1, function(x){quantile(x, 0.975)})
    pred_d_scalar$scalar_lower = apply(cf1, 1, function(x){quantile(x, 0.025)})
    
    id.vars.no.cf = id.vars[!id.vars %in% 'cf']
    d_scalar_estimate = pred_d_scalar[,c(id.vars.no.cf, 'scalar_estimate'), with =FALSE]
    setnames(d_scalar_estimate, 'scalar_estimate', 'd_scalar_estimate')
    
    # do the same first few steps as above for CF4
    pred_dn_scalar = df$cf4[,id.vars, with = FALSE]
    pred_dn_scalar = merge(pred_dn_scalar, df_means[, c(id.vars, "mean_rg"), with = FALSE], by = id.vars, all.x = TRUE)
    # take 1 / inv logit of CF4 mean to get CF4 scalar component in linear space
    pred_dn_scalar$mean_linear = (1/(plogis(pred_dn_scalar$mean_rg)))
    # to get the mean for the non-primary scalar we need to divide CF1/CF4 or (CF1 * (1/CF4))
    pred_dn_scalar = merge(pred_dn_scalar, d_scalar_estimate, by = id.vars.no.cf, all.x = TRUE)
    pred_dn_scalar[, scalar_estimate := mean_linear * d_scalar_estimate]
    pred_dn_scalar[, c('mean_linear', 'd_scalar_estimate'):=NULL]
    pred_dn_scalar = pred_dn_scalar[!is.na(scalar_estimate)]
    # get uncertainty interval from the combinations of draws from above
    setorderv(pred_dn_scalar, id.vars)
    pred_dn_scalar$scalar_upper = apply(dn_scalar, 1, function(x){quantile(x, 0.975)})
    pred_dn_scalar$scalar_lower = apply(dn_scalar, 1, function(x){quantile(x, 0.025)})
    
    if (bun_id != 10105) {
      # do for CF5 as was done for CF4 above
      pred_dno_scalar = df$cf5[,id.vars, with = FALSE]
      pred_dno_scalar = merge(pred_dno_scalar, df_means[, c(id.vars, "mean_rg"), with = FALSE], by = id.vars, all.x = TRUE)
      # take 1 / inv logit of CF4 mean to get CF4 scalar component in linear space
      pred_dno_scalar$mean_linear = (1/(plogis(pred_dno_scalar$mean_rg)))
      # to get the mean for the non-primary + outpatient scalar we need to divide CF1/CF5 (CF1 * (1/CF5))
      pred_dno_scalar = merge(pred_dno_scalar, d_scalar_estimate, by = id.vars.no.cf, all.x = TRUE)
      pred_dno_scalar[, scalar_estimate := mean_linear * d_scalar_estimate]
      pred_dno_scalar[, c('mean_linear', 'd_scalar_estimate'):=NULL]
      pred_dno_scalar = pred_dno_scalar[!is.na(scalar_estimate)]
      # get uncertainty interval from the combinations of draws from above
      pred_dno_scalar$scalar_upper = apply(dno_scalar, 1, function(x){quantile(x, 0.975)})
      pred_dno_scalar$scalar_lower = apply(dno_scalar, 1, function(x){quantile(x, 0.025)})
    }
    
    # combine the data to return
    list_to_combine = list(pred_d_scalar)
    if(nrow(pred_dn_scalar)>0) list_to_combine = c(list_to_combine, list(pred_dn_scalar))
    if (bun_id != 10105) {
      if(nrow(pred_dno_scalar)>0) list_to_combine = c(list_to_combine, list(pred_dno_scalar))
    }
    scalar_preds = rbindlist(list_to_combine, use.names = TRUE)
    scalar_preds[, mean_rg := NULL]
    scalar_preds[cf == 'cf1', scalar := 'd_scalar']
    scalar_preds[cf == 'cf4', scalar := 'dn_scalar']
    scalar_preds[cf == 'cf5', scalar := 'dno_scalar']
    scalar_preds[, cf := NULL]
    
    # Output to return 
    out <- list(
      bundle = bun_id,
      scalar_data = scalar_preds,
      scalar_draws = scalar_draws
    )
    return(out)
  })
}

### Run function to create scalars ####
all_buns = unique(active_bundles_cfs$bundle_id)
cf_scalars = lapply(all_buns, create_cf_scalars)

had_error = sapply(cf_scalars, function(x) class(x)=="try-error")
which(had_error == TRUE)

write_folder = paste0('FILEPATH', datetime)
if(!file.exists(write_folder)){
  dir.create(write_folder, showWarnings = FALSE, recursive = TRUE)
}

# draws results
df_scalar_results_draws = rbindlist(lapply(cf_scalars, function(x) {
  return(x$scalar_draws)
}), use.names = TRUE, fill = TRUE)

# point estimate results
df_scalar_results = rbindlist(lapply(cf_scalars, function(x) {
  return(x$scalar_data)
}), use.names = TRUE, fill = TRUE)

# bundle names
bun_names = loadBundles(unique(df_scalar_results$bundle_id))
df_scalar_results = merge(df_scalar_results, bun_names)

# merge on age group id 
ages[, age_midpoint := (age_start + age_end)/2]
df_scalar_results_draws = merge(df_scalar_results_draws, ages[,.(age_group_id, age_midpoint, age_start)], all.x = TRUE)
df_scalar_results_draws[ age_midpoint == 97.5, age_group_id := 235]
df_scalar_results_draws[ age_midpoint == 97.5, age_start := 95]

# need to fill in sex id where it is missing
bun_sex = unique(df[,.(bundle_id, sex_id)])
missing_sex = unique(df_scalar_results_draws[is.na(sex_id), bundle_id])
bun_sex = bun_sex[bundle_id %in% missing_sex, ]
setnames(bun_sex, 'sex_id', 'missing_sex')
df_scalar_results_draws = merge(df_scalar_results_draws, bun_sex, all.x = TRUE, by = 'bundle_id')
df_scalar_results_draws[is.na(sex_id), sex_id := missing_sex]
df_scalar_results_draws[, missing_sex := NULL]

## VALIDATIONS ####
#### Create a validation_df with all available bundle_ids, age_group_ids, and sex_ids ####
ages = get_age_metadata(release_id = GBD_RELEASE_ID) %>%
  setnames(c('age_group_years_start', 'age_group_years_end'), c('age_start', 'age_end'))
age_1 = data.table(age_group_id = 28, age_start = 0, age_end = 1)
ages = ages %>% rbind(age_1, fill = TRUE)

validation_df = as.data.table(expand.grid(age_group_id = ages$age_group_id, bundle_id = unique(active_bundles_cfs$bundle_id), sex_id = c(1,2)))
validation_df = merge(validation_df, ages[,.(age_group_id, age_start, age_end)], by = 'age_group_id')

### Get age sex restrictions ####
rests = get_bundle_restrictions() %>% as.data.table()
rests = melt(rests, id.vars = c('bundle_id','yld_age_start','yld_age_end','map_version'), variable.name = 'sex', value.name = 'sex_present')
rests[sex == 'male', sex_id := 1][sex == 'female', sex_id := 2]

### Apply age restrictions to this table ####
validation_df = merge(validation_df, rests, by = c('bundle_id','sex_id'))
validation_df = validation_df[sex_present != 0]
validation_df = validation_df[age_start >= yld_age_start,]

validation_df[, rm := 0]
validation_df[(yld_age_end >= 1) & (age_start > yld_age_end), rm := 1]  # this appears to be applying the upper bound age restriction 
validation_df = validation_df[rm != 1]
validation_df[(yld_age_end < 1) & age_start >= 1, rm := 1] 
validation_df = validation_df[rm != 1]
validation_df[, rm := NULL]
validation_df[, present := 1]

all_buns_ages_sexes_in_data = unique(df_scalar_results_draws[,.(bundle_id, age_group_id, sex_id)])
check = merge(all_buns_ages_sexes_in_data, validation_df[, .(bundle_id, age_group_id, sex_id, present)], all = TRUE, by = c('bundle_id', 'age_group_id', 'sex_id'))
if(nrow(check[present == 1,]) != nrow(all_buns_ages_sexes_in_data)) stop ('Missing bundles, ages, or sexes!')

# Save scalar results ####
# all together in version folder
saveRDS(df_scalar_results_draws, paste0(write_folder, '/scalar_results_draws.rds'))
saveRDS(df_scalar_results, paste0(write_folder, '/scalar_results.rds'))

# sex restrictions to be able to fix missing sex in sex-restricted bundles 
rests = get_bundle_restrictions() %>% as.data.table()
rests = melt(rests, id.vars = c('bundle_id','yld_age_start','yld_age_end','map_version'), variable.name = 'sex', value.name = 'sex_present')
rests[sex == 'male', sex_id := 1][sex == 'female', sex_id := 2]

# Plot Scalars ####
### Read in scalars #####
datetime = 'MODEL_ID'
write_folder = paste0('FILEPATH')
df_scalar_results = readRDS(paste0(write_folder, datetime, '/scalar_results.rds'))
cf_scalars = split(df_scalar_results, by = 'bundle_id')

### Read in previous refresh scalars ####
last_refresh = "PREV_MODEL_ID"
comp = readRDS(paste0(write_folder, last_refresh, "/scalar_results.rds"))

plot_scalars <- function(x, compare_with_last_GBD_round = TRUE, compare_with_last_refresh = TRUE) {
  try({
    bun = unique(x$bundle_id)
    if (compare_with_last_GBD_round == TRUE & bun %in% unique(cf_bundle_versions$bundle_id)){
      # if the model had a previous CF, add line from previous "best" model to compare to
      old_cf_version = cf_bundle_versions[bundle_id == bun, cf_version_id]
      
      # read in model results for each cf for each version
      old_cf_data = data.table()
      for (vers in old_cf_version ) {
        data_folder = 'FILEPATH'
        for (cf in c('cf1', 'cf2', 'cf3')) { 
          if (file.exists(paste0(data_folder,'FILEPATH'))){
            data = as.data.table(read.csv(paste0(data_folder,'FILEPATH')))
            data[, cf_vers := vers]
            old_cf_data = rbindlist(list(old_cf_data, data))
          }
        }
      }
      
      old_cf_data[cf_vers == 59, legend_label := "POL only, manual knots"]
      old_cf_data[cf_vers == 60, legend_label := "POL only, freq. knots "]
      old_cf_data[cf_vers == 61, legend_label := "MS only, manual knots"]
      old_cf_data[cf_vers == 62, legend_label := "MS only, freq. knots "]
      old_cf_data[cf_vers == 63, legend_label := "all sources, manual knots"]
      old_cf_data[cf_vers == 64, legend_label := "all sources, freq. knots "]
      
      # apply sex restrictions
      if ( bun %in% rests[sex == 'male' & sex_present == 0, unique(bundle_id)] ) {
        old_cf_data = old_cf_data[sex_id != 1, ]
      }
      if ( bun %in% rests[sex == 'female' & sex_present == 0, unique(bundle_id)] ) {
        old_cf_data = old_cf_data[sex_id != 2, ]
      }
    }
    
    bun_name = loadBundles(bun)$bundle_name
    scalars_for_bundle = unique(x$scalar)
    
    trunc_y = x[, diff := scalar_upper/scalar_estimate]
    if( length(unique(trunc_y$sex_id)) == 1 ) {
      if ( is.na(unique(trunc_y$sex_id)) ){
        trunc_y[, sex_id := rests[bundle_id == bun & sex_present == 1, unique(sex_id)]]
      }
    }
    # create scalar plots
    bundle_plots = lapply(scalars_for_bundle, function (scalar_tmp) {
      
      trunc_y_copy = copy(trunc_y[scalar == scalar_tmp,])
      trunc_y_copy[diff > 7, truncate := TRUE]
      trunc_y_copy[diff > 7, scalar_upper := 7*scalar_estimate]
      
      plot = ggplot() +
        geom_line(data = trunc_y_copy, aes(x= age_midpoint, y=scalar_estimate), color = 'darkblue', alpha = 0.6) +
        geom_ribbon(data = trunc_y_copy, aes(x= age_midpoint, ymin=scalar_lower, ymax=scalar_upper), fill = "steelblue", alpha = 0.2) +
        theme_bw() +
        labs(x = 'Age midpoint', y = 'CF scalar', caption = "The blue line represents the current scalar") +
        ylim(c(0, NA)) 
      
      if (compare_with_last_refresh == TRUE) {
        compare_data = comp[bundle_id == bun & scalar == scalar_tmp, ]
        
        if( length(unique(compare_data$sex_id)) == 1 ) {
          if ( is.na(unique(compare_data$sex_id)) ){
            compare_data[, sex_id := rests[bundle_id == bun & sex_present == 1, unique(sex_id)]]
          }
        }
        
        plot = plot +
          geom_line(data = compare_data, aes(x= age_midpoint, y=scalar_estimate, linetype = last_refresh), color = 'salmon', alpha = 0.6, inherit.aes = FALSE) +
          geom_ribbon(data = compare_data, aes(x= age_midpoint, ymin=scalar_lower, ymax=scalar_upper), fill = 'red', alpha = 0.1, inherit.aes = FALSE) +
          labs(linetype = 'Prev clinical refresh')
      }
      
      if(length(trunc_y_copy$sex_id)>1){
        plot = plot + 
          facet_wrap( ~ sex_id) 
      }
      
      if (scalar_tmp=='dno_scalar') plot = plot + ggtitle(paste0("Bundle ", unique(bun), " - ", bun_name, " - DNO scalar (CF1 / CF5)"))
      if (scalar_tmp=='dn_scalar') plot = plot + ggtitle(paste0("Bundle ", unique(bun), " - ", bun_name, " - DN scalar (CF1 / CF4)"))
      if (scalar_tmp=='d_scalar') plot = plot + ggtitle(paste0("Bundle ", unique(bun), " - ", bun_name, " - D scalar (CF1)"))

      if(compare_with_last_GBD_round == TRUE) { 
        if(scalar_tmp=='d_scalar' & bun %in% cf_bundle_versions$bundle_id) {
          plot = plot +
            geom_line(data = old_cf_data[cf_type == 'cf1'], aes(x= age_midpoint, y=cf_median, color = legend_label), linetype = 'dashed', alpha = 0.5, inherit.aes = FALSE) +
            labs(color = 'Prev GBD Results')
        }      
        if(scalar_tmp=='dn_scalar' & bun %in% cf_bundle_versions$bundle_id) {
          plot = plot +
            geom_line(data = old_cf_data[cf_type == 'cf2'], aes(x= age_midpoint, y=cf_median, color = legend_label), linetype = 'dashed', alpha = 0.5, inherit.aes = FALSE) +
            labs(color = 'Prev GBD Results')
        }
        if(scalar_tmp=='dno_scalar' & bun %in% cf_bundle_versions$bundle_id) {
          plot = plot +
            geom_line(data = old_cf_data[cf_type == 'cf3'], aes(x= age_midpoint, y=cf_median, color = legend_label), linetype = 'dashed', alpha = 0.5, inherit.aes = FALSE) +
            labs(color = 'Prev GBD Results')
        }
      }
      
      if (nrow(trunc_y_copy[truncate == TRUE ]) >= 1 & scalar_tmp != 'd_scalar') {
        plot = plot +
          geom_point(data = trunc_y_copy[truncate == TRUE ], aes(x= age_midpoint, y=scalar_upper * 1.05), alpha = 0.5, color = 'blue', shape = 4) +
          labs(caption = "blue x's = truncated UI for clarity of modeled estimates") 
      }
      out = list(
        plot = plot,
        scalar = scalar_tmp
      )
      return(out)
    })
    out = list(
      bundle_plot_list = bundle_plots,
      bundle_id = bun
    )
  })
}

scalar_plots_to_save = lapply(cf_scalars, function(x) {plot_scalars(x)})
had_error = sapply(scalar_plots_to_save, function(x) class(x)=="try-error")
which(had_error == TRUE)
scalar_plots_to_print = scalar_plots_to_save[sapply(scalar_plots_to_save, function(x) class(x)!="try-error")]

### Save Scalar Plots ####
write_folder = paste0('FILEPATH', datetime)
if(!file.exists(write_folder)){
  dir.create(write_folder, showWarnings = FALSE, recursive = TRUE)
}

lapply(scalar_plots_to_print, function(x) {
  try({
    lapply(x$bundle_plot_list, function(plot_tmp) {
      try({
        pdf(file = paste0(write_folder,"/scalarplot_", x$bundle_id, "_", plot_tmp$scalar, ".pdf"))
        print(plot_tmp$plot)
      })
      dev.off()
    })
  })
})

