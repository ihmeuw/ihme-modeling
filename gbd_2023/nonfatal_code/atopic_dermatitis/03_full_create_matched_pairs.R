## Boilerplate
library(metafor)
library(msm)
library(plyr)
library(dplyr)
library(boot)
library(ggplot2)
library(openxlsx)
library(readxl)
library(reticulate)

reticulate::use_python("FILEPATH")
cw <- import("crosswalk")

source('FILEPATH')
source("FILEPATH")

locs <- as.data.table(get_location_metadata(location_set_id = 35, release_id=16))

# Read in data
df_orig <- read.csv(paste0(modeling_dir, "FILEPATH",cause_name,".csv"))

# Claims, MarketScan and Poland data:

# All claims location ids: (Poland + MarketScan)
claims_locs <- df_orig %>% filter(clinical_data_type == "claims")
claims_locs <- c(unique(claims_locs$location_id))

# Poland Locations:
poland_locs <- c(53660,53661, 53662, 53663, 53664, 53665, 53666, 53667, 53668, 53669,
                 53670, 53671, 53672, 53673, 53674, 53675)
# Marketscan locations:
marketscan_locs <- setdiff(claims_locs, poland_locs)

## make new column cv_poland to identify Poland data
df_orig$cv_poland <- ifelse(df_orig$location_id %in% poland_locs & df_orig$clinical_data_type == "claims", 1, 0)

## set cv_marketscan to 1 for MarketScan data:
df_orig <- df_orig%>% mutate(cv_marketscan = ifelse(df_orig$location_id %in% marketscan_locs & df_orig$clinical_data_type == "claims", 1, 0))

# outlier Marketcan data prior to 2015
df_orig$is_outlier[df_orig$cv_marketscan==1 & df_orig$year_id<2015]<- 1

# Matched pairs
df_orig$cv_not_poland <- ifelse(df_orig$cv_poland!=1,1,0)
df_orig$cv_not_poland[is.na(df_orig$cv_not_poland)] <- 0
df_orig$is_reference <- ifelse(df_orig$cv_poland==1,1,0)
df_orig$cv_diag_pcr <- 0 # add dummy pcr col to work with the crosswalk matched pairs code
df_orig$rownum <- 1:nrow(df_orig)

###FIX FOR STANDARD ERRORS THAT ARE TOO LARGE###
### make sure standard error<1
df_orig$standard_error[df_orig$standard_error>1] <- 1

age_bins <- c(1,2,5,10,20,40,60,80,100)
df_matched <- bundle_crosswalk_collapse(df_orig,
                                           reference_name="is_reference",
                                           covariate_name="cv_not_poland",
                                           age_cut=age_bins,
                                           year_cut=c(seq(1972,2015,5),2020),
                                           merge_type="between",
                                           include_logit = T,
                                           location_match = "global",
                                           release_id=16)

write.csv(df_matched, paste0(modeling_dir, "FILEPATH",date,".csv"), row.names=F)
write.csv(df_orig, paste0(modeling_dir, "FILEPATH",date,".csv"), row.names=F)
