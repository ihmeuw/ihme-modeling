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

# Poland Locations:
poland_locs <- locs$location_id[locs$parent_id==51]

# Marketscan locations:
marketscan_locs <- locs$location_id[locs$parent_id==102]

## make new column cv_poland to identify Poland data
df_orig$cv_poland <- ifelse(df_orig$location_id %in% poland_locs & df_orig$clinical_data_type == "claims", 1, 0)

## set cv_marketscan to 1 for MarketScan data:
df_orig <- df_orig%>% mutate(cv_marketscan = ifelse(df_orig$location_id %in% marketscan_locs & df_orig$clinical_data_type == "claims", 1, 0))

# outlier Marketcan data prior to 2015
df_orig$is_outlier[df_orig$cv_marketscan==1 & df_orig$year_id<2010]<- 1

#Outlier India
india_nid <- c(112750, 112751 )
unique(df_orig$location_name[df_orig$nid %in% india_nid])
unique(df_orig$is_outlier[df_orig$nid %in% india_nid])
df_orig$is_outlier[df_orig$nid %in% india_nid] <- 1

#Outlier UK
uk_nid <- c(112749)
unique(df_orig$location_name[df_orig$nid %in% uk_nid])
unique(df_orig$nid[df_orig$location_name %in% c("United Kingdom", "England", "Wales")])
unique(df_orig$is_outlier[df_orig$nid %in% uk_nid])
df_orig$is_outlier[df_orig$nid %in% uk_nid] <- 1

#Outlier Canada
canada_nid <- c(322136)
unique(df_orig$location_name[df_orig$nid %in% canada_nid])
unique(df_orig$is_outlier[df_orig$nid %in% canada_nid])
unique(df_orig$sex[df_orig$nid == canada_nid])
unique(df_orig$measure[df_orig$nid == canada_nid])
df_orig$is_outlier[df_orig$nid %in% canada_nid] <- 1

#Outlier Netherlands
unique(df_orig$nid[df_orig$location_name=="Netherlands"])
netherlands_nid <- c(97466, 114851)
unique(df_orig$location_name[df_orig$nid %in% netherlands_nid])
unique(df_orig$is_outlier[df_orig$nid %in% netherlands_nid])
unique(df_orig$sex[df_orig$nid == netherlands_nid])
unique(df_orig$measure[df_orig$nid == netherlands_nid])
df_orig$is_outlier[df_orig$nid %in% netherlands_nid] <- 1

# unoutlier a few nids:
nid1<- c(112754)
nid1_df<- df_orig[df_orig$nid %in% nid1,]
nid1 %in% df_orig$nid
unique(df_orig$location_name[df_orig$nid %in% nid1])
unique(df_orig$age_start[df_orig$nid %in% nid1])
unique(df_orig$age_end[df_orig$nid %in% nid1])
df_orig$is_outlier[df_orig$nid %in% nid1]<- 0

nid2<- c(104226)
nid2 %in% df_orig$nid
unique(df_orig$location_name[df_orig$nid %in% nid2])
unique(df_orig$age_start[df_orig$nid %in% nid2])
unique(df_orig$age_end[df_orig$nid %in% nid2])
#2-5 is good, 5-10 2003-2005,( one entry from this age group is good, the other one is not)
df_orig$is_outlier[df_orig$nid %in% nid2 & df_orig$age_start==2]<- 0

nid3<- c(104228)
nid3 %in% df_orig$nid
unique(df_orig$location_name[df_orig$nid %in% nid3])
unique(df_orig$age_start[df_orig$nid %in% nid3])
unique(df_orig$age_end[df_orig$nid %in% nid3])
df_orig$is_outlier[df_orig$nid %in% nid2 & df_orig$age_start %in% c(2,5)]<- 0

#Exclude outliers from matching
df_orig<- df_orig[df_orig$is_outlier==0,]

# Matched pairs
df_orig$cv_not_poland <- ifelse(df_orig$cv_poland!=1,1,0)
df_orig$cv_not_poland[is.na(df_orig$cv_not_poland)] <- 0
df_orig$is_reference <- ifelse(df_orig$cv_poland==1,1,0)
df_orig$cv_diag_pcr <- 0 # add dummy pcr col to work with the crosswalk matched pairs code
df_orig$rownum <- 1:nrow(df_orig)


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


write.csv(df_matched, paste0(modeling_dir, "FILEPATH",bundle_id,"_",date,".csv"), row.names=F)
write.csv(df_orig, paste0(modeling_dir, "FILEPATH",bundle_id,"_",date,".csv"), row.names=F)
