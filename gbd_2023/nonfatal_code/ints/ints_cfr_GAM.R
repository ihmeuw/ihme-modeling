# This code estimates iNTS case fatality, by socio-demographic index (SDI), age, and HIV-status.
# It takes case-fatality data extracted from the scientific literature including %HIV, and pulls estimates of SDI 
# from the GBD covariate database. It then fits a generalized additive model (GAM) to the data, using the mgcv package.
# We apply two inflation factors to the SE of the prediction from the GAM: 1) we apply a degrees of freedom inflator to
# account for clustered data (i.e. multiple data points from a single study), and 2) we apply an inflator to account for
# uncertainty derived in the smoothing selection process that is not accounted for in the model's SE.
#
# Author: author
# Created: 10 July 2018
# Last modified: 07 March 2024
#
# The code was updated in 3/2024 as part of in-cycle cause updates, to improve robustness, readability, and efficiency.
# I made no changes in the fundamental methodological approach
#
# Libraries: boot, data.table, mgcv; ggplot2 and wesanderson for plotting only; 
#            dplyr loaded for convenience during development and diagnostic plotting, but not used in formal code
# Input data: this reads in the extraction sheet from the systematic review of scientific lit with CFR data
# Outputs: this saves a .csv file with estimates of logit-case fatality and their standard errors that are used
#          to calculate mortality from incidence in 'enteric_split.py'


rm(list = ls())
  
library(boot) # load for logit and inv.logit functions
library(data.table)
library(dplyr)
library(ggplot2)
library(mgcv) # load for GAM
library(wesanderson) # load for color palettes


SHARED_FUN_DIR <- 'FILEPATH'

source(file.path(SHARED_FUN_DIR, 'get_age_metadata.R'))
source(file.path(SHARED_FUN_DIR, 'get_covariate_estimates.R'))

source('FILEPATH/est_crit_value.R')

user <- Sys.getenv("USER")

# Set the release number
RELEASE <- 16
IN_DIR <- file.path(FILEPATH)  
OUT_DIR <- file.path(FILEPATH)



# PULL IN SDI & AGE GROUP METADATA, & CREATE PREDICTION DATA FRAME #  

# Get SDI
sdi <- get_covariate_estimates(covariate_id = 881, release_id = RELEASE)[year_id >= 1980, ]
sdi <- dcast.data.table(sdi, location_id + year_id ~ covariate_name_short, value.var = 'mean_value')

# Get age metadata and calculate age midpoint for each group
age_meta <- get_age_metadata(release_id = RELEASE)[, .(age_group_id, age_group_years_start, age_group_years_end)]
age_meta[, age_mid := (age_group_years_start + age_group_years_end) / 2]

# Create full prediction data frame for final estimates by location, year, and age 
full = merge(sdi[, merge := 1], age_meta[, merge := 1], by = 'merge', 
             allow.cartesian = TRUE, all = TRUE)

# Duplicate rows to create a 0 and 1 estimate for HIV prevalence 
full = merge(full, data.table(merge = 1, est_pr_hiv = 0:1), by = 'merge', 
             allow.cartesian = TRUE, all = TRUE)[, merge := NULL]

# Truncate SDI to 0.2 and 0.9 to avoid extrapolation far outside the range of the input data
full[sdi < 0.2, sdi := 0.2][sdi > 0.9, sdi := 0.9]


  

# PULL IN CFR DATA AND CLEAN FOR MODELLING #  
cfr <- fread(file.path(IN_DIR, 'data', 'cfr.csv'))

# Drop rows with missing data and those flagged for exclusion
cfr <- cfr[!is.na(mean) & is.na(exclude), ]

# Calculate the midpoint of the age range and the weights for each study
cfr[, age_mid := (age_start + age_end) / 2]
cfr[, weights := sample_size / mean(sample_size)]

# Set the location ID to a factor for use as a random effect
cfr[, nid := as.factor(nid)] 
  
# Convert name from camelCase to snake_case for consistency
setnames(cfr, 'estPrHiv', 'est_pr_hiv')
  
 
  

# RUN GAM MODEL AND MAKE PREDICTIONS #

# Calculate degrees of freedom adjustment to account for clustered data   
df_mdl <- gam(cbind(cases, sample_size - cases) ~ 
                s(age_mid, bs = 'ps', sp = 150) + 
                est_pr_hiv + sdi + s(nid, bs = "re"), 
               data = cfr, weights = cfr$weights, family = binomial())
  
n_sources <- length(cfr$nid)
df_adjustment <- (n_sources / (n_sources - 1)) * (n_sources - 1) / df_mdl$df.residual

# Run model for prediction
mdl <- gam(cbind(cases, sample_size - cases) ~ s(age_mid, bs = 'ps', sp = 150) + 
             est_pr_hiv + sdi, data = cfr, weights = cfr$weights, 
           family = binomial())

# Make predictions for all locations, years, and age groups
preds <- predict.gam(mdl, full, se.fit = TRUE)

full[, logit_pred := as.numeric(preds$fit)]
full[, logit_pred_se := as.numeric(preds$se.fit) *  df_adjustment]
 

# Smooth standard errors across age groups to correct discontinuities
full <- full[order(location_id, year_id, est_pr_hiv, age_mid), ]
full[, logit_pred_se_smooth := frollmean(logit_pred_se, n = 5, algo = 'exact', 
                                         align = 'center')]
full[age_group_years_start >=85 | age_group_years_end <= 1, 
     logit_pred_se_smooth := logit_pred_se]



# Compute simultaneous interval for a penalised spline to correct uncertainty for smoothing selection
# Create prediction data frame to compute a simultaneous interval for a penalised spline 
crit_pred <- expand.grid(sdi = seq(from = 0.1, to = 0.9, by = 0.1), 
                        age_mid = c(0.1, 0.5, 1, 2.5, seq(from = 5, to = 100, by = 5)), 
                        est_pr_hiv = c(0, 1))

# Get the critical value for the simultaneous interval
crit <- est_crit_val(mdl, crit_pred)


# Adjust standard errors for simultaneous interval
full[, logit_pred_se_smooth := logit_pred_se_smooth * crit / qnorm(0.975)]

# Write out the predicted CFRs for use in enteric_split.py
write.csv(full[, .(location_id, year_id, age_group_id, est_pr_hiv, logit_pred, logit_pred_se_smooth)], 
          file = file.path(OUT_DIR, 'cfr_estimates.csv'), row.names = FALSE)
 
 

## DX plotting below
# Calculate case-fatality and 95% confidence intervals for plotting 
full[, pred := inv.logit(logit_pred)]
full[, pred_lower_smooth := inv.logit(logit_pred - qnorm(0.975) * logit_pred_se_smooth)]
full[, pred_upper_smooth := inv.logit(logit_pred + qnorm(0.975) * logit_pred_se_smooth)]
full[, pred_lower := inv.logit(logit_pred - qnorm(0.975) * logit_pred_se)]
full[, pred_upper := inv.logit(logit_pred + qnorm(0.975) * logit_pred_se)]


# Find the SDI values closest to 0.2, 0.4, 0.6, and 0.8 for plotting
nearest_sdis <- sapply(seq(0.2, 0.8, 0.2), function(x) full[which.min(abs(full$sdi - x)), sdi])

colors <- wes_palette('FantasticFox1', 5)[c(3, 5)]

full %>% filter(sdi %in% nearest_sdis) %>% 
  ggplot(aes(x = age_mid, y = pred, color = factor(est_pr_hiv), fill = factor(est_pr_hiv))) +
  geom_ribbon(aes(ymin = pred_lower_smooth, ymax = pred_upper_smooth), alpha = 0.4, color = NA) + 
  geom_line() + theme_minimal() + facet_wrap(~paste('SDI =', round(sdi, 2))) + 
  scale_y_continuous(breaks = seq(0, 1, 0.2)) + xlab('Age') + ylab('Case-fatality') +
  scale_color_manual(values = colors) + # values = c('0' = 'blue', '1' = 'red')) +
  scale_fill_manual(values = colors, name = 'HIV status', labels = c('HIV-', 'HIV+')) +
  guides(color = 'none')  
