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
df_orig$is_outlier[df_orig$cv_marketscan==1 & df_orig$year_id<2010] <- 1

# outlier Tanzania prevalence
tanzania_nid <- c(97501)
unique(df_orig$measure[df_orig$nid %in% tanzania_nid])
unique(df_orig$location_name[df_orig$nid %in% tanzania_nid])
unique(df_orig$is_outlier[df_orig$nid %in% tanzania_nid])
df_orig$is_outlier[df_orig$nid %in% tanzania_nid] <- 1

# outlier Philippine incidence
philippine_nid <- c(292574)
unique(df_orig$measure[df_orig$nid %in% philippine_nid])
unique(df_orig$location_name[df_orig$nid %in% philippine_nid])
unique(df_orig$is_outlier[df_orig$nid %in% philippine_nid])
df_orig$is_outlier[df_orig$nid %in% philippine_nid] <- 1

# outlier Nepal incidence
nepal_nid <- c(536777, 292575 )
unique(df_orig$measure[df_orig$nid %in% nepal_nid])
unique(df_orig$location_name[df_orig$nid %in% nepal_nid])
unique(df_orig$is_outlier[df_orig$nid %in% nepal_nid])
df_orig$is_outlier[df_orig$nid %in% nepal_nid] <- 1

# outlier Kenya incidence
kenya_nid <- c(133665)
unique(df_orig$measure[df_orig$nid %in% kenya_nid])
unique(df_orig$location_name[df_orig$nid %in% kenya_nid])
unique(df_orig$is_outlier[df_orig$nid %in% kenya_nid])
df_orig$is_outlier[df_orig$nid %in% kenya_nid] <- 1

# outlier Botswana incidence
botsowana_nid <- c(126516)
unique(df_orig$measure[df_orig$nid %in% botsowana_nid])
unique(df_orig$location_name[df_orig$nid %in% botsowana_nid])
unique(df_orig$is_outlier[df_orig$nid %in% botsowana_nid])
df_orig$is_outlier[df_orig$nid %in% botsowana_nid] <- 1

# outlier Mongolia
mongolia_nid <- c(469234)
unique(df_orig$measure[df_orig$nid %in% mongolia_nid])
unique(df_orig$location_name[df_orig$nid %in% mongolia_nid])
unique(df_orig$is_outlier[df_orig$nid %in% mongolia_nid])
df_orig$is_outlier[df_orig$nid %in% mongolia_nid] <- 1

# outlier EHMD data NID=3822 (hospital Europe/WHO  data)
ehmd_nid <- c(3822)
unique(df_orig$measure[df_orig$nid %in% ehmd_nid])
unique(df_orig$location_name[df_orig$nid %in% ehmd_nid])
unique(df_orig$is_outlier[df_orig$nid %in% ehmd_nid])
df_orig$is_outlier[df_orig$nid %in% ehmd_nid] <- 1

# outlier Iran
iran_nid <- c(331084)
unique(df_orig$measure[df_orig$nid %in% iran_nid])
unique(df_orig$location_name[df_orig$nid %in% iran_nid])
unique(df_orig$is_outlier[df_orig$nid %in% iran_nid])
df_orig$is_outlier[df_orig$nid %in% iran_nid] <- 1

# outlier China
china_nid<- c(337619)
unique(df_orig$measure[df_orig$nid %in% china_nid])
unique(df_orig$location_name[df_orig$nid %in% china_nid])
unique(df_orig$is_outlier[df_orig$nid %in% china_nid])
df_orig$is_outlier[df_orig$nid %in% china_nid] <- 1

# outlier Philippine
philippine_nid <- c(499632)
unique(df_orig$measure[df_orig$nid %in% philippine_nid])
unique(df_orig$location_name[df_orig$nid %in% philippine_nid])
unique(df_orig$is_outlier[df_orig$nid %in% philippine_nid])
df_orig$is_outlier[df_orig$nid %in% philippine_nid] <- 1

# outlier Georgia
georgia_nid <- c(466892)
unique(df_orig$measure[df_orig$nid %in% georgia_nid])
unique(df_orig$location_name[df_orig$nid %in% georgia_nid])
unique(df_orig$is_outlier[df_orig$nid %in% georgia_nid])
df_orig$is_outlier[df_orig$nid %in% georgia_nid] <- 1

# outlier Botswana
botswana_nid2 <- c(411100)
unique(df_orig$measure[df_orig$nid %in% botswana_nid2])
unique(df_orig$location_name[df_orig$nid %in% botswana_nid2])
unique(df_orig$is_outlier[df_orig$nid %in% botswana_nid2])
df_orig$is_outlier[df_orig$nid %in% botswana_nid2 &
                     df_orig$sex=="Male" &
                     df_orig$age_start>=95] <- 1
df_orig$is_outlier[df_orig$nid %in% botswana_nid2 &
                     df_orig$sex=="Female" &
                     df_orig$age_start==85] <- 1

sum(df_orig$is_outlier)

# Drop data points in central, eastern Europe, & high income North America fora ages>95:
locs_to_drop_95_plus<- locs$location_id[locs$region_id %in% c(42,
                                                              56)
                                        |locs$super_region_id==64]
unique(df_orig$age_start)
df_orig$is_outlier[df_orig$location_id %in% locs_to_drop_95_plus & df_orig$age_start>=95]<-1

sum(df_orig$is_outlier)

# Exclude outliers from matching
df_orig <- df_orig[df_orig$is_outlier==0, ]

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
