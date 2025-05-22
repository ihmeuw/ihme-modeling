#########################################################################
# Description                                                           #
# Calculate 1000 draws of incidence from logNormal                      #
# Calculate CIs from draws                                              #
#########################################################################

# Define paths
data_root <- 'FILEPATH'
params_dir <- paste0(data_root, "/FILEPATH/params")

# Load packages/central functions
library(data.table)
library(dplyr)
library(openxlsx)
library(matrixStats)
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/get_crosswalk_version.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/save_crosswalk_version.R")

mean_ui <- function(x){
  y <- select(x, starts_with("lognorm_"))
  y$mean <- apply(y, 1, mean)
  y$lower <- apply(y, 1, function(x) quantile(x, c(.025)))
  y$upper <- apply(y, 1, function(x) quantile(x, c(.975)))
  
  w <- y[, c('mean', 'lower', 'upper')]
  
  z <- select(x, -contains("lognorm_"))
  
  v <- cbind(z, w)
  v <- as.data.table(v)
  return(v)
  
}

# constants
date <- Sys.Date()
loc_id_drop <- ID  #list of national level ids where we have subnational data

### ========================= Data Management ========================= ###
## Merge incidence & underreporting data

# pull in incidence and keep only necessary columns (data processing from step0) 
inc_df <- fread(paste0(params_dir, "/FILEPATH"))

inc_df <- select(inc_df, c("location_id", "location_name", "ihme_loc_id", "iso3", "age_group_id", "age_start", "age_end", "sex_id", "sex", "year_start", "year_end", 
                           "mean", "lower", "upper", "standard_error", "cases", "sample_size", "nid", "underlying_nid", "is_outlier", "seq"))

# pull in underreporting data and sort
uf <- fread(paste0(params_dir, "/FILEPATH"))
uf_df <- merge(inc_df, uf, by = "iso3")
uf_df$year_id <- uf_df$year_start

uf_mean <- mean_uf(uf)
### ========================= Log-Normal ========================= ###
uf_df$mean_log <- log(uf_df$mean/sqrt(1 + (uf_df$standard_error^2/(uf_df$mean^2))))
uf_df$sd_log <- sqrt(log(1 + (uf_df$standard_error^2/uf_df$mean^2)))

for (j in 0:999){
  
  uf_df[, paste0("lognorm_", j):= as.numeric(lapply(mean_log, function(x) rlnorm(n=1, x, sd_log)))]
  
}


### Calculate 1000 draws of incidence & apply under-reporting (rate per 100,000)
ur_logn <- copy(uf_df)
ur_logn[, paste0("lognorm_", 0:999) := lapply(0:999, function(x) get(paste0("uf_", x)) * get(paste0('lognorm_', x)) * 100000)] #incidence rate per 100,000

inc_df <- ur_logn %>%
  select(-contains("uf_"))

setnames(inc_df, c('mean','lower','upper','standard_error','cases'), c('orig_mean','orig_lower','orig_upper', 'orig_se','orig_cases'))

# calculate uncertainty
inc_df$variance <- apply(inc_df[, paste0("lognorm_", 0:999)], 1, var)
final_df <- mean_ui(inc_df)

### ========================= Format for Upload ========================= ###

# drop national data if have subnational
final_df <- final_df[!(location_id == loc_id_drop)]
final_df <- subset(final_df, select = -c(mean_log,sd_log))
setnames(final_df, 'mean','val')

# variables needed for bundle upload
final_df[, me_name := "ntd_leish_cut"]
final_df[, measure := "continuous"]
final_df[, c('year_start','year_end') := NULL] 

write.csv(final_df, "/FILEPATH", row.names = FALSE)