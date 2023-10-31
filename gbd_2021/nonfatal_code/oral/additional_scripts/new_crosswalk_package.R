######## Crosswalks using new packages


library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')
#install.packages('msm', lib = 'FILEPATH')
library('msm')
library("LaplacesDemon")
library("car")
library(crosswalk, lib.loc = "FILEPATH")
library(dplyr)


###Setting up arguments
outdir <- "FILEPATH"
bundle <- 262
measure <- 'prevalence'
trim <- 'trim' # trim or no_trim
trim_percent <- 0.15
mod_lab <- "07_30_V1"

#Specifying covariate to run crosswalking for, and match-type
cv <- "cv_atchloss4ormore"
match_type <- "within_study"


set.seed(42)


#Reading in data
all_split <- read.xlsx(paste0("FILEPATH", bundle, "_", measure, "_final.xlsx"))

age_map <- fread("FILEPATH")
age_map$order <- NULL
#all_split <- all_split[,unique(names(all_split)), ]
#age_map$age_end[2] <- 0.077

#changes to which locations are being queried
locs <- get_location_metadata(22, decomp_step = 'step3')
#locs_super <- c('location_id', 'super_region_name')
locs <- locs[, c('location_id', 'super_region_id')]
#sdi_map <- fread('FILEPATH')

all_split <- merge(all_split, locs, by = 'location_id', all.x = TRUE)
#all_split <- merge(all_split, sdi_map, by = 'super_region_id', all.x = TRUE)


if(cv %like% "cv_livestill"){
  all_split <- all_split[is.na(cv_excludes_chromos) | cv_excludes_chromos == 0]
  all_split <- all_split[is.na(cv_worldatlas_to_total) | cv_worldatlas_to_total == 0]
  all_split <- all_split[is.na(cv_icbdms_to_total) | cv_icbdms_to_total == 0]
  all_split <- all_split[is.na(cv_ndbpn_to_total ) | cv_ndbpn_to_total == 0]
  all_split <- all_split[is.na(cv_congmalf_to_total) | cv_congmalf_to_total == 0]
}

#drop any rows where cv_livestill == 1 if were running chromo
if(cv %like% "cv_excludes_chromos"){
  all_split <- all_split[is.na(cv_livestill) | cv_livestill == 0]
  all_split <- all_split[is.na(cv_worldatlas_to_total) | cv_worldatlas_to_total == 0]
  all_split <- all_split[is.na(cv_icbdms_to_total) | cv_icbdms_to_total == 0]
  all_split <- all_split[is.na(cv_ndbpn_to_total ) | cv_ndbpn_to_total == 0]
  all_split <- all_split[is.na(cv_congmalf_to_total) | cv_congmalf_to_total == 0]
}

# We will need to add logic in for the cv_{registry}_to_total once we get to those! 

sex_names <- fread("FILEPATH")
all_split <- merge(all_split, sex_names, by = c('sex'), all.x = TRUE)

#all_split <- merge(all_split, age_map, by = c('age_start', 'age_end'), all.x = TRUE)

all_split <- data.table(all_split)


#Start of crosswalking

##Narrow age variable calculated from previous step, if age range falls within GBD age demographic,
##retains original age range
all_split <- all_split[all_split$narrow_age == 1, age_start := orig_age_start]
all_split <- all_split[all_split$narrow_age == 1, age_end := orig_age_end]

## Calculate age-midpoint, used to generate spline
all_split <- all_split[, age_mid := round((age_start + age_end)/2)]


all_split <- all_split[, year_id := round((year_start + year_end) / 2)]

all_split <- all_split[!is.na(standard_error), ]

#Following 3 lines to remove non-reference or alternate definition points for a given covariate (Currently only used for bundle 262)
if (bundle == 262){
  #all_split <- all_split[!all_split$cv_dmf_units_surfaces == 1,]
  
  all_split <- all_split[!all_split$cv_atchloss5ormore == 1,] 
  
  all_split <- all_split[!all_split$cv_cpiclass3 == 1, ]
  
}


##########


all_split <- all_split[, c('nid', 'location_id', 'sex_id', 'age_mid', 'age_start', 'age_end', 'year_id', 'mean', 'standard_error', 'cases', 'sample_size', 'field_citation_value', 'location_name', cv), with = F]

template_split <- copy(all_split)
print(paste0('# rows with mean > 0.01: ',nrow(all_split[mean > 0.01])))

####################################################################################
# match data
##########################################

match_split <- split(all_split, all_split[, c(cv), with = FALSE])

if(length(match_split) == 2){
  
  if(match_type == 'within_study'){
    matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('nid', 'location_id', 'age_start', 'age_mid', 'age_end', 'sex_id', 'year_id'), all.x = TRUE, allow.cartesian=TRUE)
  } 
  if(match_type == 'between_study'){
    matched_merge <- merge(match_split[['1']], match_split[['0']],  by = c('location_id', 'age_start', 'age_mid', 'age_end', 'sex_id', 'year_id'), all.x = TRUE, allow.cartesian=TRUE)
  }
  
  #drop rows where that loc/age/sex/year was only in the first dataset
  matched_merge <- matched_merge[!is.na(mean.y)]
  
  #drop rows where both means are 0
  matched_merge <- matched_merge[!(mean.x == 0 & mean.y == 0)]
  
  #add offset where both values are zero in order to be able to calculate std error
  offset <- 0.5 * median(all_split[mean != 0, mean])
  matched_merge[, `:=` (mean.x = mean.x + offset,
                        mean.y = mean.y + offset)]
  
  print(paste0('# rows where livestill is nonzero but live is zero: ',nrow(matched_merge[mean.y > 0 & mean.x == 0])))
  
  #matched_merge <- matched_merge[!mean.x==0] # uncomment me if you want to get rid of ALTERNATE means that are zero
  #matched_merge <- matched_merge[!mean.y==0] # uncomment me if you want to get rid of REFERENCE means that are zero
  
  matched_merge <- matched_merge[, mean:= mean.x / mean.y]
  matched_merge <- matched_merge[, standard_error:= sqrt( (mean.x^2 / mean.y^2) * ( (standard_error.x^2 / mean.x^2) + (standard_error.y^2 / mean.y^2) ) ) ]
  
  #output csv if the ratio is less than zero when looking at cv_livestill
  #output csv if the ratio is greater than zero if looking at cv_excludes_chromos
  #then drop from crosswalk
  #add here
  
  if(nrow(matched_merge) == 0){
    print(paste0('no ', match_type, ' matches between demographics for ', cv))
  } else{
    write.csv(matched_merge, paste0(outdir, mod_lab, "_inputdata.csv"), row.names = FALSE)
  }
  
} else{
  print('only one value for given covariate')
}

#The following lines are where the model is fit. 
#For prevalence, the data is logit transformed before the model is fit


if (measure == "prevalence"){
  ### Logit crosswalking - for prevalence
  
  logit_metareg <- as.data.frame(matched_merge) %>%
    select(nid, sex_id, year_id, age_mid, cv_atchloss4ormore.x, mean.x, standard_error.x, cv_atchloss4ormore.y, mean.y, standard_error.y, mean, standard_error)
  
  logit_metareg <- logit_metareg[!logit_metareg$mean.y >= 1, ]
  logit_metareg <- logit_metareg[!logit_metareg$mean.x >= 1, ]
  logit_metareg <- logit_metareg[!logit_metareg$mean.y <= 0, ]
  logit_metareg <- logit_metareg[!logit_metareg$mean.x <= 0, ]
  
  
  
  
  logit_metareg <- (logit_metareg) %>%
    mutate (logit_mean_alt = logit(mean.x)) %>%
    mutate (logit_mean_ref = logit(mean.y)) %>%
    mutate(logit_diff = logit_mean_alt - logit_mean_ref)
  
  
  logit_metareg$logit_se_alt<- sapply(1:nrow(logit_metareg), function(a) {
    ratio_a <- logit_metareg[a, "mean.x"]
    ratio_se_a <- logit_metareg[a, "standard_error.x"]
    deltamethod(~log(x1), ratio_a, ratio_se_a^2)
  })
  
  
  # Logit transform the SE of the reference mean
  logit_metareg$logit_se_ref<- sapply(1:nrow(logit_metareg), function(r) {
    ratio_r <- logit_metareg[r, "mean.y"]
    ratio_se_r <- logit_metareg[r, "standard_error.y"]
    deltamethod(~log(x1), ratio_r, ratio_se_r^2)
  })
  
  
  # Calculate the standard error of the ratio = logit  difference
  logit_metareg$logit_se_diff <- sqrt(logit_metareg$logit_se_alt^2 + logit_metareg$logit_se_ref^2)
  
  #logit_metareg$study_id <- "1"
  
  print(typeof(logit_metareg$study_id))
  
  new_metareg <- logit_metareg %>%
    select(sex_id, age_mid, logit_diff, logit_se_diff, nid)
  
  new_metareg$altvar <- cv
  new_metareg$refvar <- "measured"
  
  new_metareg$sex_id <- ifelse(new_metareg$sex_id == 2, 0, 1)
  
  
  new_metareg <- new_metareg[!new_metareg$logit_diff == 0, ]
  
  
  
  df_cw <- CWData(
    df = new_metareg,
    obs = "logit_diff",
    obs_se = "logit_se_diff",
    alt_dorms = "altvar",
    ref_dorms = "refvar",
    covs = list("age_mid", "sex_id"),
    add_intercept = TRUE,
    study_id = "nid"
  )
  
  
  
  
  fit1 <- CWModel(
    cwdata = df_cw,
    obs_type = 'diff_logit',
    cov_models = list(
      CovModel(cov_name = 'intercept'),
      CovModel(cov_name = 'sex_id'),
      CovModel(cov_name = 'age_mid', 
               spline = XSpline(knots = list(12, 22, 42, 110),
                                degree = 2L,
                                l_linear = TRUE,
                                r_linear = TRUE),
               spline_monotonicity = "increasing")
    ),
    gold_dorm = 'measured',
    inlier_pct = 0.9,
    use_random_intercept = TRUE
  )
  
} else{
  dat_metareg <- as.data.frame(matched_merge)
  dat_metareg$sex_id <- ifelse(dat_metareg$sex_id == 2, 0, 1)
  
  ratio_var <- "mean"
  ratio_se_var <- "standard_error"
  age_var <- "age_mid"
  sex_var <- "sex_id"
  sdi_var <- "sdi_var"
  cov_names <- c('sex_id')
  metareg_vars <- c(ratio_var, ratio_se_var, sex_var, age_var)
  
  tmp_metareg <- as.data.frame(dat_metareg) %>%
    .[, metareg_vars] %>%
    setnames(metareg_vars, c("ratio", "ratio_se", 'sex_id', "age_mid"))
  
  tmp_metareg$log_diff <- log(tmp_metareg$ratio)
  tmp_metareg$log_se_diff <- sapply(1:nrow(tmp_metareg), function(i) {
    ratio_i <- tmp_metareg[i, "ratio"]
    ratio_se_i <- tmp_metareg[i, "ratio_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  
  
  
  new_metareg <- tmp_metareg %>%
    select(sex_id, age_mid, log_diff, log_se_diff)
  
  new_metareg$altvar <- cv
  new_metareg$refvar <- "measured"
  
  
  
  
  df_cw <- CWData(
    df = new_metareg,
    obs = "log_diff",
    obs_se = "log_se_diff",
    alt_dorms = "altvar",
    ref_dorms = "refvar",
    covs = list('age_mid'),
    add_intercept = TRUE
  )
  
  
  fit1 <- CWModel(
    cwdata = df_cw,
    obs_type = 'diff_log',
    cov_models = list(
      CovModel(cov_name = 'intercept'),
      CovModel(cov_name = 'age_mid', 
               spline = XSpline(knots = list(5, 7, 12, 17),
                                degree = 2L,
                                l_linear = TRUE,
                                r_linear = TRUE),
               spline_monotonicity = "increasing")
    ),
    gold_dorm = 'measured',
    inlier_pct = 0.9
  )
  
}


print(unique(new_metareg$age_mid))


help(XSpline)



######## Non-logit crosswalking - for incidence

dat_metareg <- as.data.frame(matched_merge)
dat_metareg$sex_id <- ifelse(dat_metareg$sex_id == 2, 0, 1)

ratio_var <- "mean"
ratio_se_var <- "standard_error"
age_var <- "age_mid"
sex_var <- "sex_id"
sdi_var <- "sdi_var"
cov_names <- c('sex_id')
metareg_vars <- c(ratio_var, ratio_se_var, sex_var, age_var)

tmp_metareg <- as.data.frame(dat_metareg) %>%
  .[, metareg_vars] %>%
  setnames(metareg_vars, c("ratio", "ratio_se", 'sex_id', "age_mid"))

tmp_metareg$log_diff <- log(tmp_metareg$ratio)
tmp_metareg$log_se_diff <- sapply(1:nrow(tmp_metareg), function(i) {
  ratio_i <- tmp_metareg[i, "ratio"]
  ratio_se_i <- tmp_metareg[i, "ratio_se"]
  deltamethod(~log(x1), ratio_i, ratio_se_i^2)
})



new_metareg <- tmp_metareg %>%
  select(sex_id, age_mid, log_diff, log_se_diff)

new_metareg$altvar <- cv
new_metareg$refvar <- "measured"




df_cw <- CWData(
  df = new_metareg,
  obs = "log_diff",
  obs_se = "log_se_diff",
  alt_dorms = "altvar",
  ref_dorms = "refvar",
  covs = list('age_mid'),
  add_intercept = TRUE
)


fit1 <- CWModel(
  cwdata = df_cw,
  obs_type = 'diff_log',
  cov_models = list(
    CovModel(cov_name = 'intercept'),
    CovModel(cov_name = 'age_mid', 
             spline = XSpline(knots = list(5, 7, 12, 17),
                              degree = 2L,
                              l_linear = TRUE,
                              r_linear = TRUE),
             spline_monotonicity = "increasing")
  ),
  gold_dorm = 'measured',
  inlier_pct = 0.9
)



df_result <- fit1$create_result_df()

xwalk_betas <- df_result[df_result$dorms == cv, ]

write.csv(xwalk_betas,
          file = paste0('FILEPATH', bundle, "_", measure, "_", cv, "_corrected.csv"),
          row.names = FALSE)




###Plotting




## Plot both the funnel plot and the dose response curve when the model has a spline

repl_python()

plots <- import('crosswalk.plots')

plots$funnel_plot(
  cwmodel = fit1,
  cwdata = df_cw,
  continuous_variables = list('sex_id',"age_mid"),
  obs_method = cv,
  plot_note = paste0("Chronic Periodontal Caries Crosswalk - ", cv, ", Mean Beta: ", round(xwalk_betas$beta[xwalk_betas$cov_names == "intercept"], digits = 3), ", \nGamma: ", round(xwalk_betas$gamma[xwalk_betas$cov_names == "intercept"], digits = 3), ", Sex Beta: ", round(xwalk_betas$beta[xwalk_betas$cov_names == "sex_id"], digits = 3), ", Age Beta: ", round(xwalk_betas$beta[xwalk_betas$cov_names == "age_mid_spline_0"], digits = 3)),
  plots_dir = "FILEPATH",
  file_name = "262_perio_funnel_cv_atchloss4ormore_08_26",
  write_file = TRUE
)





plots$funnel_plot(
  cwmodel = fit1,
  cwdata = df_cw,
  continuous_variables = list("sex_id"),
  obs_method = cv,
  plot_note = paste0("Bundle 261 (Permanent Caries) Crosswalk - ", cv),
  plots_dir = "FILEPATH",
  file_name = "261_perm_funnel_cv_atchloss4ormore",
  write_file = TRUE
)




plots$dose_response_curve(
  dose_variable = 'age_mid',
  obs_method = cv,
  continuous_variables = list('sex_id'),
  cwdata = df_cw,
  cwmodel = fit1,
  plot_note = paste0("Chronic Periodontal Crosswalk - ", cv, ", Mean Beta: ", round(xwalk_betas$beta[xwalk_betas$cov_names == "intercept"], digits = 3), ", \nGamma: ", round(xwalk_betas$gamma[xwalk_betas$cov_names == "intercept"], digits = 3), ", Sex Beta: ", round(xwalk_betas$beta[xwalk_betas$cov_names == "sex_id"], digits = 3), ", Age Beta: ", round(xwalk_betas$beta[xwalk_betas$cov_names == "age_mid_spline_0"], digits = 3)),
  plots_dir = "FILEPATH",
  file_name = "262_perio_spline_cv_atchloss4ormore_08_20",
  write_file = TRUE
)

print(fit1)

print(df_cw)

help("CWModel")



### Applying crosswalk



check_df <- new_metareg %>%
  select(age_mid, sex_id, logit_diff, logit_se_diff)

### The orginal dataframe, prior to matching, is copied here, and the predictions made by the model fitting are then appended onto it
## There is code to also account for outliers, defined as points where the specified covariate is NULL, or the mean is NULL or outside the range of 0-1
df_orig <- copy(template_split)


df_orig$obs_method <- ifelse(df_orig$cv_atchloss4ormore == 0, "measured", cv)

df_orig_outliers <- df_orig[df_orig$mean <= 0 | df_orig$mean >= 1 | is.na(df_orig$mean), ]

#df_orig_ones <- df_orig[df_orig$mean >= 1, ]

df_orig <- df_orig[!df_orig$mean <= 0 & !df_orig$mean >=1, ]

df_all_outliers <- rbind(df_orig_outliers, df_orig[is.na(df_orig$cv_atchloss4ormore), ])

#df_all_outliers <- rbind(df_all_outliers, df_orig[df_orig$cv_dmf_units_surfaces == 1, ])

df_na_outliers <- df_orig[is.na(df_orig$cv_atchloss4ormore), ]

df_orig_other <- df_orig[!is.na(df_orig$cv_atchloss4ormore), ]

#df_orig_other <- df_orig[!df_orig$cv_dmf_units_surfaces == 1 | is.na(df_orig$cv_dmf_units_surfaces), ]

svd(fit1$design_mat)$d

print(data.frame(beta_mean = fit1$beta,
                 beta_se = fit1$beta_sd))

df_orig_other$nid <- as.character(df_orig_other$nid)



#Here the prdictions based on model fit are created, adjust_orig_vals is a central funtion
preds1 <- adjust_orig_vals(
  fit_object = fit1,
  df = df_orig_other,
  orig_dorms = 'obs_method',
  orig_vals_mean = 'mean',
  orig_vals_se = 'standard_error',
  study_id = "nid"
)


outlier_preds <- df_all_outliers[, c("mean", "standard_error")]

outlier_preds$pred_logit <- 0
outlier_preds$pred_se_logit <- 0

#Filling in data ID for unadjusted rows

outlier_preds$data_id <- c(4494:4661)


### The predictions are appended on and a final dataset is save out

df_orig_other[, c("meanvar_adjusted", "sdvar_adjusted", 
            "pred_logit", "pred_se_logit", "data_id")] <- preds1

df_all_outliers[, c("meanvar_adjusted", "sdvar_adjusted", 
                "pred_logit", "pred_se_logit", "data_id")] <- outlier_preds



final_df_crosswalked <- rbind(df_orig_other, df_all_outliers)



write.xlsx(final_df_crosswalked,
           file = paste0("FILEPATH", bundle, "_", measure, "_", cv, "_final_crosswalk.xlsx"),
           sheetName = 'extraction', row.names = FALSE)




















