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
df_orig$is_outlier[df_orig$cv_marketscan==1 & df_orig$year_id<2010]<-1

# outlier Ethiopia
unique(df_orig$nid[df_orig$location_id==38])
ethiopia_nid <- c(104226)
ethiopia_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% ethiopia_nid])
unique(df_orig$location_name[df_orig$nid %in% ethiopia_nid])
df_orig$is_outlier[df_orig$nid %in% ethiopia_nid] <- as.integer(1)

# outlier Mongolia
#unique(df_orig$nid[df_orig$location_id %in% ])
mongolia_nid <- c(469234)
mongolia_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% mongolia_nid])
unique(df_orig$location_name[df_orig$nid %in% mongolia_nid])
df_orig$is_outlier[df_orig$nid %in% mongolia_nid] <- as.integer(1)

# outlier Korea
# check Korea inputs:
unique(df_orig$nid[df_orig$location_id==68])
korea_nid <- c(514241,514242)
korea_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% korea_nid])
unique(df_orig$location_name[df_orig$nid %in% korea_nid])
df_orig$is_outlier[df_orig$nid %in% korea_nid] <- as.integer(1)


# outlier Netherlands
unique(df_orig$nid[df_orig$location_id==89])
netherlands_nid <- c(97466)
netherlands_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% netherlands_nid])
unique(df_orig$location_name[df_orig$nid %in% netherlands_nid])
df_orig$is_outlier[df_orig$nid %in% netherlands_nid] <- as.integer(1)

# outlier Egypt
unique(df_orig$nid[df_orig$location_id==141])
egypt_nid <- c(97505)
egypt_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% egypt_nid])
unique(df_orig$location_name[df_orig$nid %in% egypt_nid])
df_orig$is_outlier[df_orig$nid %in% egypt_nid] <- as.integer(1)

# outlier Iraq
unique(df_orig$nid[df_orig$location_id==143])
iraq_nid <- c(97457)
iraq_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% iraq_nid])
unique(df_orig$location_name[df_orig$nid %in% iraq_nid])
df_orig$is_outlier[df_orig$nid %in% iraq_nid] <- as.integer(1)

# outlier France
unique(df_orig$nid[df_orig$location_id==80])
france_nid <- c(112756)
france_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% france_nid])
unique(df_orig$location_name[df_orig$nid %in% france_nid])
df_orig$is_outlier[df_orig$nid %in% france_nid] <- as.integer(1)

# outlier Fiji, Taiwan 128399
fiji_nid <- c(128399)
fiji_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% fiji_nid])
unique(df_orig$location_name[df_orig$nid %in% fiji_nid])
df_orig$is_outlier[df_orig$nid %in% fiji_nid] <- as.integer(1)


# outlier  sub saharan africa
africa_nid <- c(103988)
africa_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% africa_nid])
unique(df_orig$location_name[df_orig$nid %in% africa_nid])
df_orig$is_outlier[df_orig$nid %in% africa_nid] <- as.integer(1)

# check measure type
unique(df_orig$measure)
df_orig$is_outlier[df_orig$measure=='prevalence'] <- as.integer(1)

# Select these NID just for age under 25 and are prevalence and add
unique(df_orig$age_start)
prev_nids<- c(128399, 128395, 112756,112755, 112754, 112750,
             104226, 103988, 103986, 97505, 97493, 97492, 97457)
prev_nids %in% df_orig$nid
#first outlier above nids:
df_orig$is_outlier[df_orig$nid %in% prev_nids]<-1
# bring back data that Mohsen mentioned
df_orig$is_outlier[df_orig$nid %in% prev_nids &
                     df_orig$measure=='prevalence' &
                     df_orig$age_start<25]<-0

# Select these NID just for age under 25 and are incidence and add
inc_nids<- c(112755,
             128394,
             97466)
inc_nids %in% df_orig$nid
#first outlier above nids:
df_orig$is_outlier[df_orig$nid %in% inc_nids]<-1
# bring back data that Mohsen mentioned
df_orig$is_outlier[df_orig$nid %in% inc_nids &
                     df_orig$measure=='incidence' &
                     df_orig$age_start<25]<-0

# Exclude outliers from matching
df_orig <- df_orig[df_orig$is_outlier!=1,]

# Matched pairs
df_orig$cv_not_poland <- ifelse(df_orig$cv_poland!=1,1,0)
df_orig$cv_not_poland[is.na(df_orig$cv_not_poland)] <- 0
df_orig$is_reference <- ifelse(df_orig$cv_poland==1,1,0)
df_orig$cv_diag_pcr <- 0 # add dummy pcr col to work with the crosswalk matched pairs code
df_orig$rownum <- 1:nrow(df_orig)

### TEMPORARY FIX FOR STANDARD ERRORS THAT ARE TOO LARGE###
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


write.csv(df_matched, paste0(modeling_dir, "FILEPATH",bundle_id,"_",date,".csv"), row.names=F)
write.csv(df_orig, paste0(modeling_dir, "FILEPATH",bundle_id,"_",date,".csv"), row.names=F)

