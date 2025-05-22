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
"%ni%" <- negate("%in%")
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

# outlier Marketcan data prior to 2010 
df_orig$is_outlier[df_orig$cv_marketscan==1 & df_orig$year_id<2011]<- 1

# outlier Sri Lanka
srilanka_nid <- c(97447)
unique(df_orig$location_name[df_orig$nid %in% srilanka_nid])
unique(df_orig$is_outlier[df_orig$nid %in% srilanka_nid])
df_orig$is_outlier[df_orig$nid %in% srilanka_nid] <- as.integer(1)

# outlier Indonesia
indonesia_nid <- c(97491)
unique(df_orig$location_name[df_orig$nid %in% indonesia_nid])
unique(df_orig$is_outlier[df_orig$nid %in% indonesia_nid])
df_orig$is_outlier[df_orig$nid %in% indonesia_nid] <- as.integer(1)

# outlier India
india_nid <- c(115285)
unique(df_orig$location_name[df_orig$nid %in% india_nid])
unique(df_orig$is_outlier[df_orig$nid %in% india_nid])
df_orig$is_outlier[df_orig$nid %in% india_nid] <- as.integer(1)

# outlier China
china_nid <- c(97445)
unique(df_orig$location_name[df_orig$nid %in% china_nid])
unique(df_orig$is_outlier[df_orig$nid %in% china_nid])
df_orig$is_outlier[df_orig$nid %in% china_nid] <- as.integer(1)

# outlier Nepal
nepal_nid <- c(97449)
unique(df_orig$location_name[df_orig$nid %in% nepal_nid])
unique(df_orig$is_outlier[df_orig$nid %in% nepal_nid])
df_orig$is_outlier[df_orig$nid %in% nepal_nid] <- as.integer(1)

# outlier Turkey
turkey_nid <- c(115796,326122)
unique(df_orig$location_name[df_orig$nid %in% turkey_nid])
unique(df_orig$is_outlier[df_orig$nid %in% turkey_nid])
df_orig$is_outlier[df_orig$nid %in% turkey_nid] <- as.integer(1)

# outlier Egypt
egypt_nid <- c(97505, 269501, 115283)
unique(df_orig$location_name[df_orig$nid %in% egypt_nid])
unique(df_orig$is_outlier[df_orig$nid %in% egypt_nid])
df_orig$is_outlier[df_orig$nid %in% egypt_nid] <- as.integer(1)

# outlier Mexico
mexico_nid <- c(115102,103995)
unique(df_orig$location_name[df_orig$nid %in% mexico_nid])
unique(df_orig$is_outlier[df_orig$nid %in% mexico_nid])
df_orig$is_outlier[df_orig$nid %in% mexico_nid] <- as.integer(1)

# outlier Mongolia
mongolia_nid <- c(469234)
unique(df_orig$location_name[df_orig$nid %in% mongolia_nid])
unique(df_orig$is_outlier[df_orig$nid %in% mongolia_nid])
df_orig$is_outlier[df_orig$nid %in% mongolia_nid] <- as.integer(1)

# outlier Tanzania
tanzania_nid <- c(328091)
unique(df_orig$location_name[df_orig$nid %in% tanzania_nid])
unique(df_orig$is_outlier[df_orig$nid %in% tanzania_nid])
df_orig$is_outlier[df_orig$nid %in% tanzania_nid] <- as.integer(1)

# outlier Taiwan
taiwan_nid <- c(103990)
unique(df_orig$location_name[df_orig$nid %in% taiwan_nid])
unique(df_orig$is_outlier[df_orig$nid %in% taiwan_nid])
df_orig$is_outlier[df_orig$nid %in% taiwan_nid] <- as.integer(1)

# outlier Gabon, Ghana
gabon_nid <- c(141296)
unique(df_orig$location_name[df_orig$nid %in% gabon_nid])
unique(df_orig$is_outlier[df_orig$nid %in% gabon_nid])
df_orig$is_outlier[df_orig$nid %in% gabon_nid] <- as.integer(1)

# outlier Nigeria
nigeria_nid <- c(269483, 97498, 70502, 97502 ,115792)
nigeria_nid %in% df_orig$nid
unique(df_orig$location_name[df_orig$nid %in% nigeria_nid])
unique(df_orig$is_outlier[df_orig$nid %in% nigeria_nid])
df_orig$is_outlier[df_orig$nid %in% nigeria_nid] <- as.integer(1)

# outlier Niger
niger_nid <- c(141317)
unique(df_orig$location_name[df_orig$nid %in% niger_nid])
unique(df_orig$is_outlier[df_orig$nid %in% niger_nid])
df_orig$is_outlier[df_orig$nid %in% niger_nid] <- as.integer(1)

# outlier Mali
mali_nid <- c(328017)
unique(df_orig$location_name[df_orig$nid %in% mali_nid])
unique(df_orig$is_outlier[df_orig$nid %in% mali_nid])
df_orig$is_outlier[df_orig$nid %in% mali_nid] <- as.integer(1)

# outlier Cameroon
cameroon_nid <- c(115309)
unique(df_orig$location_name[df_orig$nid %in% cameroon_nid])
unique(df_orig$is_outlier[df_orig$nid %in% cameroon_nid])
df_orig$is_outlier[df_orig$nid %in% cameroon_nid] <- as.integer(1)

# outlier Ethiopia
ethiopia_nid <- c(97490)
unique(df_orig$location_name[df_orig$nid %in% ethiopia_nid])
unique(df_orig$is_outlier[df_orig$nid %in% ethiopia_nid])
df_orig$is_outlier[df_orig$nid %in% ethiopia_nid] <- as.integer(1)

# outlier Kenya
kenya_nid <- c(97495, 97492)
unique(df_orig$location_name[df_orig$nid %in% kenya_nid])
unique(df_orig$is_outlier[df_orig$nid %in% kenya_nid])
df_orig$is_outlier[df_orig$nid %in% kenya_nid] <- as.integer(1)

# outlier Tanzania
tanzania_nid <- c(97501)
unique(df_orig$location_name[df_orig$nid %in% tanzania_nid])
unique(df_orig$is_outlier[df_orig$nid %in% tanzania_nid])
df_orig$is_outlier[df_orig$nid %in% tanzania_nid] <- as.integer(1)

# outlier US
us_nid <- c(130077)
unique(df_orig$location_name[df_orig$nid %in% us_nid])
unique(df_orig$is_outlier[df_orig$nid %in% us_nid])
df_orig$is_outlier[df_orig$nid %in% us_nid] <- as.integer(1)

# outlier Turkiye fro some age groups
turkiye_nid2 <- 328122
unique(df_orig$age_start[df_orig$nid == turkiye_nid2])
unique(df_orig$location_name[df_orig$nid %in% turkiye_nid2])
unique(df_orig$is_outlier[df_orig$nid %in% turkiye_nid2])
df_orig$is_outlier[df_orig$age_start %in% c(2,5,10,60,80,85,90,95) &
                     df_orig$nid %in% turkiye_nid2] <- as.integer(1)

# Excluding outliers from df_orig:
df_orig <- df_orig[df_orig$is_outlier==0,]

excluded_locs <- c(marketscan_locs, poland_locs)
unique(df_orig$location_name[!df_orig$location_id %in% excluded_locs])


# Matched pairs
df_orig$cv_not_poland <- ifelse(df_orig$cv_poland!=1,1,0)
df_orig$cv_not_poland[is.na(df_orig$cv_not_poland)] <- 0
df_orig$is_reference <- ifelse(df_orig$cv_poland==1,1,0)
df_orig$cv_diag_pcr <- 0 # add dummy pcr col to work with the crosswalk matched pairs code
df_orig$rownum <- 1:nrow(df_orig)

###FIX STANDARD ERRORS THAT ARE TOO LARGE###
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
