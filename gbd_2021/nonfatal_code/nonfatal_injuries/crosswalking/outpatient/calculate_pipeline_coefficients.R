# Calculate pipeline coefficients

library(crosswalk, lib.loc = "FILEPATH")
source("FILEPATH/get_age_metadata.R")

ages <- get_age_metadata(age_group_set_id=19, gbd_round_id=7)

# get unique age midpoints
ages <- ages[,c(1,3,4)]
colnames(ages) <- c('age_group_id', 'age_start', 'age_end')
ages$age_end <- ifelse(ages$age_end == 125, 99, ages$age_end)
ages$age_midpoint <- (ages$age_start + ages$age_end) / 2

# make dummy columns in order for xwalk to work
ages$mean <- 1
ages$se <- 0.1
ages$dorm <- 'outpatient'

# get bundle info
bundle_df <- read.csv('FILEPATH/bundle_versions.csv')
bundle_df <- bundle_df[!is.na(bundle_df$save),]

bundles <- bundle_df$bundle_id
names <- bundle_df$acause_name

for (i in 1:length(bundles)) {
  bundle <- bundles[i]
  name <- names[i]
  
  # read in model
  fit1 <-
    py_load_object(
      filename = paste0('FILEPATH/model_',
                        as.character(bundle),
                        '_',
                        name,
                        '.pkl'), pickle = 'dill')
  
  # use model to predict on age_midpoint
  pred <- ages
  
  pred[, c("mean2", "mean2_se", "diff", "diff_se", "data_id")] <- adjust_orig_vals(
    fit_object = fit1,       # result of CWModel()
    df = pred,            # original data with obs to be adjusted
    orig_dorms = "dorm", # name of column with (all) def/method levels
    orig_vals_mean = "mean",  # original mean
    orig_vals_se = "se"  # standard error of original mean
  )
  
  # exponentiate
  pred$out_coeff <- exp(pred$diff)
  
  # write final df
  pred <- pred[,c('age_midpoint', 'out_coeff', 'age_group_id', 'age_start')]
  write.csv(pred, paste0('FILEPATH/', name, '.csv'), row.names = F)
}
