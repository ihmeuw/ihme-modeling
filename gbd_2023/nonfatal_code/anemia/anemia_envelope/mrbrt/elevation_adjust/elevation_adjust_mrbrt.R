
# setup envrionment -------------------------------------------------------

## Get user and date
Sys.umask(mode = 002)

## Source functions
library(reticulate)
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
library(data.table)
library(ggplot2)
library(stringr)

# get cluster arguments ---------------------------------------------------

if(interactive()){
  map_file_name <- file.path(getwd(), 'mrbrt/elevation_adjust/param_map.csv')
  task_id <- 1
}else{
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  command_args <- commandArgs(trailingOnly = TRUE)
  map_file_name <- command_args[1]
  output_dir <- command_args[2]
}

param_map <- fread(map_file_name)
ref_var <- param_map$ref_var[task_id]
alt_var <- param_map$alt_var[task_id]

# load in elevation data --------------------------------------------------

df <- fread("FILEPATH")
df[df==""] <- NA
df <- df[var %in% c(ref_var, alt_var) & 
            !(is.na(anemia_category)) & 
           !(is.na(cluster_elevation_id)) & 
           !(is.na(mean)) & 
           !(is.na(standard_error)) & 
           mean > 0 & 
           mean < 1 & 
           standard_error > 0.0001
         ] 

# add id column -----------------------------------------------------------

df[, id := .GRP, .(nid)]
df <- df[order(id)]
df$id <- as.character(df$id)

# log transform mean and SE -----------------------------------------------

df$logit_mean <- nch::logit(x = df$mean)
df$logit_se <- nch::logit_se(mean_vec = df$mean, se_vec = df$standard_error)

# split and match data ----------------------------------------------------

ref_df <- df[var==ref_var]
alt_df <- df[var==alt_var]

merge_df <- merge.data.table(ref_df,alt_df,
                             by = c("nid","survey_name","ihme_loc_id","year_start","year_end","survey_module","file_path","cluster_elevation_id", "anemia_category","id"),
                             suffixes = c('.ref', '.alt'), allow.cartesian = T)

merge_df[,`:=`(diff_mean = logit_mean.alt - logit_mean.ref,
               ratio_se = sqrt(logit_se.alt^2 + logit_se.ref^2))]


# prep raw prevalence categories ------------------------------------------

raw_prev_map <- fread(file.path(getwd(), "mrbrt/elevation_adjust/raw_prev_map.csv"))
for(r in 1:nrow(raw_prev_map)){
  prev_cat <- as.numeric(raw_prev_map[r,raw_prev_category])
  u_prev <- as.numeric(raw_prev_map[r,upper_prev])
  l_prev <- as.numeric(raw_prev_map[r,lower_prev])
  merge_df <- merge_df[mean.ref >= l_prev & mean.ref < u_prev, raw_prev_category := prev_cat]
}

# create dummy variables for elevation, anemia, and raw prevalence categories --------------

merge_df[,elevation_number_cat := as.numeric(str_remove(cluster_elevation_id,"elevation_cat_"))]

elevation_cov_df <- unique(merge_df[,.(cluster_elevation_id,elevation_number_cat)])
elevation_cov_df <- elevation_cov_df[order(elevation_number_cat)]

elevation_covs <- elevation_cov_df$cluster_elevation_id
elevation_covs <- elevation_covs[elevation_covs!="elevation_cat_0"]

for(x in elevation_covs){
  i_vec <- which(merge_df$cluster_elevation_id==x)
  set(x = merge_df, i = i_vec,j = x, value = 1)

  i_vec <- which(!(merge_df$cluster_elevation_id==x))
  set(x = merge_df, i = i_vec,j = x, value = 0)
}

anemia_categories <- unique(merge_df$anemia_category)
anemia_categories <- anemia_categories[anemia_categories!=5] # use non-pregnant females as the reference
anemia_cat_covs <- lapply(anemia_categories, function(x) paste("anemia_cov",x,sep = "_"))

for(x in anemia_categories){
  j_val <- paste("anemia_cov",x,sep = "_")
  
  i_vec <- which(merge_df$anemia_category==x)
  set(x = merge_df, i = i_vec,j = j_val, value = 1)
  
  i_vec <- which(!(merge_df$anemia_category==x))
  set(x = merge_df, i = i_vec,j = j_val, value = 0)
}

raw_prev_categories <- unique(merge_df$raw_prev_category)
raw_prev_categories <- raw_prev_categories[raw_prev_categories!=1] #use lowest raw anemia prevalence as reference
raw_prev_covs <- lapply(raw_prev_categories, function(x) paste("raw_prev",x,sep = "_"))

for(x in raw_prev_categories){
  j_val <- paste("raw_prev",x,sep = "_")

  i_vec <- which(merge_df$raw_prev_category==x)
  set(x = merge_df, i = i_vec,j = j_val, value = 1)

  i_vec <- which(!(merge_df$raw_prev_category==x))
  set(x = merge_df, i = i_vec,j = j_val, value = 0)
}

covariates <- as.list(append(elevation_covs, anemia_cat_covs))
# covariates <- append(covariates, 'mean.ref')
covariates <- append(covariates, raw_prev_covs)

# vet input data ----------------------------------------------------------
if(interactive()){
  diagnostic_plot1 <- ggplot(merge_df,aes(x = elevation_number_cat, y = diff_mean, colour = mean.ref)) +
    geom_jitter() +
    scale_colour_gradientn(colours = terrain.colors(10)) +
    geom_smooth(method = "loess", colour = "black")+
    theme_classic() +
    labs(title = paste("Elevation Category vs. Change in Anemia Prevalence using BRINDA - Mild to Moderate Anemia"),
         subtitle = "Elevation categories are in 400 meter bins",
         x="Elevation Category",
         y="Logit Difference of Anemia Prevalence") +
    theme(axis.line = element_line(colour = "black"))
  
  plot(diagnostic_plot1)
  
  diagnostic_plot2 <- ggplot(merge_df[elevation_number_cat == 5]) +
    geom_jitter(aes(x = mean.ref, y = diff_mean, colour = cluster_elevation_id)) +
    geom_smooth(aes(x = mean.ref, y = diff_mean),method = "loess", colour = "black")+
    theme_classic() +
    labs(title = paste0("Original Anemia Prevalence vs. Change in Anemia Prevalence using BRINDA - Mild to Moderate Anemia"),
         subtitle = "Elevation categories are in 400 meter bins",
         x="Raw Anemia Prevalence",
         y="Logit Difference of Anemia Prevalence") +
    theme(axis.line = element_line(colour = "black"))
  
  plot(diagnostic_plot2)
  
  diagnostic_plot3 <- ggplot(merge_df) +
    geom_jitter(aes(x = mean.ref, y = diff_mean, colour = cluster_elevation_id)) +
    geom_smooth(aes(x = mean.ref, y = diff_mean, colour = cluster_elevation_id),method = "loess")+
    theme_classic() +
    labs(title = paste0("Original Anemia Prevalence vs. Change in Anemia Prevalence using BRINDA - ",plot_title_vec[i]," Anemia"),
         subtitle = "Elevation categories are in 400 meter bins",
         x="Raw Anemia Prevalence",
         y="Logit Difference of Anemia Prevalence") +
    theme(axis.line = element_line(colour = "black"))
  
  plot(diagnostic_plot3)
}

# start mrbrt model -------------------------------------------------------

dat1 <- mr$MRData()

dat1$load_df(
  data = merge_df,  col_obs = "diff_mean", col_obs_se = "ratio_se",
  col_covs = covariates, col_study_id = "id" )

# set up covariates and splines
cov_list <- list()
#add intercept
cov_mods <- append(mr$LinearCovModel("intercept", use_re = T), cov_list)

# add the following covariates
# 1) all elevation categories (but not category 0, which will be used as the reference) without REs
# 2) the mean.ref anemia value with a decreasing spline
cov_mods <- append(lapply(covariates, function(x){
  if(x == "mean.ref"){
    mr$LinearCovModel(
      alt_cov = x,
      use_spline = TRUE,
      spline_knots = array(c(0.2, 0.5)),
      spline_degree = 2L,
      spline_knots_type = 'domain',
      spline_r_linear = TRUE,
      spline_l_linear = FALSE,
      prior_spline_monotonicity = NULL,
      prior_spline_convexity = NULL,
      prior_spline_maxder_gaussian = rbind(c(0,0,0), c(0.1,0.1,0.0001))
    )
  }else{
    mr$LinearCovModel(x,use_re = F)
  }
}),cov_mods)

# run mr-brt model --------------------------------------------------------

fit1 <- mr$MRBRT(
  data = dat1,
  cov_models = cov_mods,
  inlier_pct = 1.0
)

fit1$fit_model(inner_print_level = 3L, inner_max_iter = 50000L)

# assess mr-brt output ----------------------------------------------------

dat_pred <- mr$MRData()
dat_pred$load_df(data = merge_df, col_covs = covariates)

merge_df$pred_logit_mean <- fit1$predict(dat_pred,predict_for_study = T)  #predict to get point estimate
coefs_dt <- as.data.table(fit1$summary())
colnames <- names(coefs_dt)
coefs <- melt(data = coefs_dt , measure.vars = colnames)
setnames(coefs, c('variable','value'), c('Covariate', 'Beta'))
coefs$Beta <- format(round(coefs$Beta, 3))
coefs[Covariate == 'intercept', Gamma := coefs[Covariate == 'intercept.1']$Beta] # reassign the intercept.1 value to be Gamma
coefs <- coefs[!(Covariate == 'intercept.1')]
coefs$Beta <- as.numeric(coefs$Beta)

# create draws for FE only ------------------------------------------------

n_samples1 <- as.integer(100)

samples1 <- fit1$sample_soln(
  sample_size = n_samples1
)

draws1 <- fit1$create_draws(
  data = dat_pred,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )

merge_df$pred1_lo <- apply(draws1, 1, function(x) quantile(x, 0.025))
merge_df$pred1_up <- apply(draws1, 1, function(x) quantile(x, 0.975))
merge_df$draw_se_fe <- (merge_df$pred1_up - merge_df$pred1_lo)/(qnorm(0.975)*2)

# create draws for both FE and RE -----------------------------------------

n_samples2 <- 100L

# each row is the same; point estimates of gamma rather than samples
gamma_vals2 <- matrix(
  data = fit1$gamma_soln, 
  nrow = n_samples2, 
  ncol = length(fit1$gamma_soln), 
  byrow = TRUE # 'byrow = TRUE' is important to include
)

samples2 <- fit1$sample_soln(sample_size = n_samples2)

draws2 <- fit1$create_draws(
  data = dat_pred,
  beta_samples = samples2[[1]],
  gamma_samples = gamma_vals2,
  random_study = TRUE )

merge_df$pred2_lo <- apply(draws2, 1, function(x) quantile(x, 0.025))
merge_df$pred2_up <- apply(draws2, 1, function(x) quantile(x, 0.975))
merge_df$draw_se_re <- (merge_df$pred2_up - merge_df$pred2_lo)/(qnorm(0.975)*2)

# write out data ----------------------------------------------------------

file_name <- file.path(
  output_dir,
  "csv",
  paste0(
    task_id, "_",
    ref_var, "-",
    alt_var, "-",
    ".csv"
  )
)
fwrite(
  x = merge_df,
  file = file_name
)

file_name_pkl <- file.path(
  output_dir,
  "pkl",
  paste0(
    task_id, "_",
    ref_var, "-",
    alt_var,
    ".pkl"
  )
)
py_save_object(
  object = fit1, 
  filename = file_name_pkl, 
  pickle = "dill"
)