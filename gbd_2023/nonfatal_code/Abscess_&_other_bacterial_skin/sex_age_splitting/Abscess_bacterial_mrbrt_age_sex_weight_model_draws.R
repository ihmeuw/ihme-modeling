# source libraries --------------------------------------------------------

library(data.table)
library(ggplot2)
library(dplyr)
reticulate::use_condaenv("FILEPATH")
pd <- reticulate::import("pandas")
mr_brt_env <- reticulate::py_run_string(
  "from mrtool import *"
)
set.seed(123)

setwd(paste0("FILEPATH"))
source('map_location_hierarchy.R')
# box::use(
#   mlh = ./map_location_hierarchy.R
# )
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")


# load in bundle data -----------------------------------------------------
bv <- get_bundle_version(bundle_version_id = bvid)

location_df <- get_location_metadata(
  location_set_id = 35,
  release_id = 16
)

hierarchy_col_names <- c(
  'global_id',
  'super_region_id',
  'region_id',
  'country_id',
  'subnat1_id',
  'subnat2_id',
  'subnat3_id'
)
age_mt <- get_age_metadata(release_id = 16)
under_1 <- list(age_group_id = 28, age_group_name = "<1 year", age_group_alternative_name= "<1 year", age_group_name_short = "<1",
                age_group_years_start = 0, age_group_years_end = 1, age_group_days_start=0, age_group_days_end =364, most_detailed = 0 )
age_mt <- rbind(age_mt, under_1, fill=TRUE)
age_mt_col_names <- select(age_mt, age_group_years_start, age_group_years_end, age_group_id, age_group_name)


Poland_locs <- c(53660,53661, 53662, 53663, 53664, 53665, 53666, 53667, 53668, 53669,
                 53670, 53671, 53672, 53673, 53674, 53675)


ref_df <- bv %>% filter(location_id %in% Poland_locs & clinical_data_type=='claims')
ref_df_age_id <- ref_df %>% mutate(age_end = trunc(age_end) + 1)

df<- inner_join(ref_df_age_id, age_mt_col_names, by = c("age_start" = "age_group_years_start", "age_end" = "age_group_years_end"))
df <- df %>% mutate(age_group_years_start = age_start,
                              age_group_years_end = age_end)
df <- df %>% mutate(age_end=age_end-1)
condition <- ((df$age_start == 0) & (df$age_end == 0))
df$age_end[condition] <- 0.999


clinical_data <- df|>
  dplyr::filter(is_outlier == 0 &  age_group_id != 164) |>
  dplyr::mutate(
    sex_id_mrbrt = dplyr::case_when(
      sex == 'Male' ~ 0,
      .default = 1
    )
  ) |>
  dplyr::select(nid, age_group_id, age_start, age_end, sex_id_mrbrt, sex, location_id, mean, standard_error) |>
  setDT() |>
  map_location_hierarchy(
    loc_df = location_df,
    hierarchy_col_names = hierarchy_col_names
  )

# add id column -----------------------------------------------------------

clinical_data[, id := .GRP, .(nid)]
clinical_data <- clinical_data[order(id)]
clinical_data$id <- as.character(clinical_data$id)

# remove birth age group id -----------------------------------------------
clinical_data <- clinical_data[age_group_id != 164, ]
nrow(clinical_data)
# offset and transform mean prevalence ------------------------------------

# offset 0 values to include in model
offset_value <- min(clinical_data[mean > 0, mean]) / 2
clinical_data <- clinical_data[mean == 0, mean := offset_value]


clinical_data$logit_mean <- nch::logit(clinical_data$mean)
clinical_data$logit_se <- nch::logit_se(
  mean_vec = clinical_data$mean,
  se_vec = clinical_data$standard_error
)

# create dummy variables for age group ------------------------------------

ref_age_group_id <- 6 # 5-9 year olds
clinical_age_group_ids <- unique(clinical_data$age_group_id)
clinical_age_group_ids <- clinical_age_group_ids[clinical_age_group_ids != ref_age_group_id]

for(age_id in clinical_age_group_ids) {
  new_column_name <- paste0('age_group_', age_id)
  clinical_data[[new_column_name]] <- 0
  i_vec <- which(clinical_data$age_group_id == age_id)
  clinical_data[[new_column_name]][i_vec] <- 1
}

# create dummy variables for super region ---------------------------------

# global ID will act as the reference
reference_super_region <- 64
super_region_ids <- unique(clinical_data$super_region_id)
super_region_ids <- super_region_ids[super_region_ids != reference_super_region]

for(sr_id in super_region_ids) {
  new_column_name <- paste0('super_region_', sr_id)
  clinical_data[[new_column_name]] <- 0
  i_vec <- which(clinical_data$super_region_id == sr_id)
  clinical_data[[new_column_name]][i_vec] <- 1
}

# plot diagnostic ---------------------------------------------------------

if(interactive()){
  age_cov_diagnostic <- ggplot(clinical_data ,aes(x = age_start, y = logit_mean, colour = as.factor(sex))) +
    geom_point(alpha = 0.5) +
    geom_smooth(method = "loess", se = FALSE, inherit.aes = TRUE) +
    theme_classic() +
    labs(title = paste("Age/sex patterns for ",cause_name),
         x="Age Midpoint",
         y="Logit Prevalence") +
    theme(axis.line = element_line(colour = "black"))

  plot(age_cov_diagnostic)
}

# run mrbrt model ---------------------------------------------

dat1 <- mr_brt_env$MRData()

covariates <- as.list(
  c(
    paste0('age_group_', clinical_age_group_ids),
    paste0('super_region_', super_region_ids),
    'sex_id_mrbrt'
  )
)

dat1$load_df(
  data = clinical_data,  col_obs = "logit_mean", col_obs_se = "logit_se",
  col_covs = covariates, col_study_id = "id"
)

# set up covariates and splines
cov_list <- list()
#add intercept
cov_mods <- append(mr_brt_env$LinearCovModel("intercept", use_re = T), cov_list)

cov_mods <- append(lapply(covariates, function(x){
  mr_brt_env$LinearCovModel(x,use_re = F)
}), cov_mods)

# run mr-brt model --------------------------------------------------------

fit1 <- mr_brt_env$MRBRT(
  data = dat1,
  cov_models = cov_mods,
  inlier_pct = 1.0
)

fit1$fit_model(inner_print_level = 3L, inner_max_iter = 50000L, outer_tol = 0.00001)

# Create draws -------------------------------------------------------

n_samples1 = as.integer(100)
samples1 <- fit1$sample_soln(sample_size=n_samples1)
draws1 <- fit1$create_draws(
  data = dat1,
  beta_samples = samples1[[1]],
  gamma_samples = matrix(rep(0, n_samples1), ncol = 1),
  random_study = FALSE )
# draws1 is a 3360 x 100 matrix
# Convert to a dataframe
draws_df <- as.data.frame(draws1)
names(draws_df) <- paste0("draw_", 1:ncol(draws_df))
# Should we join this to the final dataframe?
write.csv(draws_df, paste0("FILEPATH"))

# save mrbrt output -------------------------------------------------------

reticulate::py_save_object(
  object = fit1,
  filename = '',
  pickle = 'dill'
)

# plot diagnostics --------------------------------------------------------

dat_pred <- mr_brt_env$MRData()
dat_pred$load_df(data = clinical_data, col_covs = covariates)

clinical_data$pred_mean <- fit1$predict(dat_pred, predict_for_study = FALSE)

global_mrbrt_plot <- ggplot(clinical_data, aes(x = age_start, colour = sex)) +
  geom_point(aes(y = logit_mean)) +
  geom_line(aes(y = pred_mean), linewidth = 1.5) +
  labs(
    title = paste0('Global MR-BRT Age Trends'),
    x = 'Age Start',
    y = 'Logit Transformed Mean'
  ) +
  theme_minimal()

output_dir <- ""
ggsave(filename = file.path("FILEPATH"), plot = global_mrbrt_plot)
plot(global_mrbrt_plot)

summary_mrbrt <- unique(clinical_data[,.(age_group_id, sex, super_region_id, pred_mean)])
fwrite(
  x = summary_mrbrt,
  file = file.path("FILEPATH")
)

