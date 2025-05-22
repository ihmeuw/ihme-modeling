rm(list = ls())
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/save_crosswalk_version.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/upload_bundle_data.R")
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_location_metadata.R")

library(DescTools)
library(epiR)

Sys.setenv("RETICULATE_PYTHON" = "FILEPATH") # this line might be necessary on some Singularity images
library(reticulate, lib.loc = "~/R")
reticulate::use_python("FILEPATH")

mr <- reticulate::import("mrtool")
cw <- reticulate::import("crosswalk")

BUNDLE <- 3785
RELEASE <- 16
CAUSE <- 'ints' #intest

out_dir <- file.path('FILEPATH', CAUSE, paste0('release_', RELEASE), 'inputs/mrbrt')
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

create_log_ratios <- function(ratio_dt){
  dt <- copy(ratio_dt)
  dt[, log_ratio := log(ratio)]
  dt$log_se <- sapply(1:nrow(dt), function(i) {
    mean_i <- dt[i, "ratio"]
    se_i <- dt[i, "ratio_se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  dt[, log_se_test := ratio_se/ratio]
  return(dt)
}


fill_standard_errors <- function(x) {
  x[is.na(standard_error)==T & is.na(upper)==F, 
    standard_error := (upper - mean) / qnorm(0.975)]
  
  miss_rows <- which(is.na(x$standard_error))
  
  if (length(miss_rows) > 0) {
    x[is.na(mean), mean := cases/sample_size]
    miss_row_cis <- BinomCI(x$cases[miss_rows], ifelse(x$cases[miss_rows]==0, 
                                                       x$sample_size[miss_rows], 
                                                       x$cases[miss_rows]/x$mean[miss_rows]), 
                            method = 'jeffreys')
    miss_row_cis <- as.data.table(miss_row_cis)
    miss_row_cis[, se := (upr.ci - est) / qnorm(0.975)]
    
    x[miss_rows, standard_error := miss_row_cis$se]
  }
  
  return(x)
}

loc_meta <- get_location_metadata(35, release = RELEASE)

raw <- get_bundle_data(bundle_id = BUNDLE)
raw <- merge(raw, 
             loc_meta[, .(location_id, super_region_id, super_region_name, 
                          region_id, region_name)], 
             by = 'location_id', all.x = T)
raw <- fill_standard_errors(raw)
raw[, super_region_name := factor(super_region_name)]

sex_split <- merge(copy(raw)[sex == 'Male' & mean>0, 
                             .(nid, source_type, location_id, super_region_id, 
                               super_region_name, year_start, year_end, 
                               age_start, age_end, mean, standard_error)],
                   copy(raw)[sex == 'Female' & mean>0, 
                             .(nid, source_type, location_id, super_region_id, 
                               super_region_name, year_start, year_end, 
                               age_start, age_end, mean, standard_error)],
                   by = c('nid', 'source_type', 'location_id', 'super_region_id', 
                          'super_region_name', 'year_start', 'year_end', 
                          'age_start', 'age_end'))

sex_split[, ratio := mean.x / mean.y]
sex_split[, ratio_se := sqrt((mean.x^2 / mean.y^2) * 
                               ((standard_error.x^2 / mean.x^2) + 
                                  (standard_error.y^2 / mean.y^2)))]
sex_split[, log_ratio := log(ratio)]
sex_split[, log_ratio_se := ratio_se/ratio]

sex_split[, paste0('sr_', unique(sex_split$super_region_id)) := 
            lapply(unique(sex_split$super_region_id), function(x) super_region_id == x)]

dat1 <- mr$MRData()
dat1$load_df(
  data = sex_split,  col_obs = "log_ratio", col_obs_se = "log_ratio_se",
  col_study_id = "nid")
  

mod1 <- mr$MRBRT(
  data = dat1,
  cov_models = list(
    mr$LinearCovModel("intercept", use_re = F)
)
)

mod1$fit_model()
mod1$summary()

n_samples <- 1000L

samples <- mod1$sample_soln(
  sample_size = n_samples
)


write.csv(samples[[1]], file = file.path(out_dir, paste0('sex_split_draws_', BUNDLE, '.csv')), row.names = F)

          