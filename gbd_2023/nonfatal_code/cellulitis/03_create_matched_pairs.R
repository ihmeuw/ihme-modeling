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
cw <- import("FILEPATH")

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

# all incidence and all is inpatient and outpatient data no survey data
unique(df_orig$measure)
df_orig$is_outlier[df_orig$measure %in% c("prevalence", "mtexcess" )]<-1
names(df_orig)
unique(df_orig$source_type)
df_orig$is_outlier[df_orig$source_type %in% c("Survey - cross-sectional",
                                              "Unidentifiable",
                                              "Survey - other/unknown")]<-1

# outlier Marketcan data prior to 2015
df_orig$is_outlier[df_orig$cv_marketscan==1 & df_orig$year_id<2010]<-1

# outlier China:
china_nid <- c(337619)
china_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% china_nid])
unique(df_orig$location_name[df_orig$nid %in% china_nid])
df_orig$is_outlier[df_orig$nid %in% china_nid ] <- as.integer(1)

# outlier Nigeria
nigeria_nid <- c(104002)
nigeria_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% nigeria_nid])
unique(df_orig$location_name[df_orig$nid %in% nigeria_nid])
df_orig$is_outlier[df_orig$nid %in% nigeria_nid] <- as.integer(1)

# outlier India
india_nid <- c(104208)
india_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% india_nid])
unique(df_orig$location_name[df_orig$nid %in% india_nid])
df_orig$is_outlier[df_orig$nid %in% india_nid] <- as.integer(1)

# outlier Iran
iran_nid <- c(331084)
iran_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% iran_nid])
unique(df_orig$location_name[df_orig$nid %in% iran_nid])
df_orig$is_outlier[df_orig$nid %in% iran_nid] <- as.integer(1)

# outlier Turkiye
turkiye_nid <- c(287207)
turkiye_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% turkiye_nid])
unique(df_orig$location_name[df_orig$nid %in% turkiye_nid])
df_orig$is_outlier[df_orig$nid %in% turkiye_nid] <- as.integer(1)


# outlier Mongolia
mongolia_nid <- c(469234)
mongolia_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% mongolia_nid])
unique(df_orig$location_name[df_orig$nid %in% mongolia_nid])
df_orig$is_outlier[df_orig$nid %in% mongolia_nid] <- as.integer(1)

# outlier Armenia
armenia_nid <- c(432266)
armenia_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% armenia_nid])
unique(df_orig$location_name[df_orig$nid %in% armenia_nid])
df_orig$is_outlier[df_orig$nid %in% armenia_nid] <- as.integer(1)

# outlier Kenya
kenya_nid <- c(133665)
kenya_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% kenya_nid])
unique(df_orig$location_name[df_orig$nid %in% kenya_nid])
df_orig$is_outlier[df_orig$nid %in% kenya_nid] <- as.integer(1)

# outlier Sweden
unique(df_orig$nid[df_orig$location_id==93])
sweden_nid <- c(234758, 234760,234761, 408336)
sweden_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% sweden_nid])
unique(df_orig$location_name[df_orig$nid %in% sweden_nid])
df_orig$is_outlier[df_orig$nid %in% sweden_nid] <- as.integer(1)

# outlier Nepal
nepal_nid<- c(unique(df_orig$nid[df_orig$location_name=="Nepal"]))
nepal_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% nepal_nid])
unique(df_orig$location_name[df_orig$nid %in% nepal_nid])
df_orig$is_outlier[df_orig$nid %in% nepal_nid] <- as.integer(1)

# 1- drop Japan Hospital data NID = 336851 and  336852 and other NID Ministry of Health (Italy). Italy Hospital Inpatient Discharges 2005-2019 : 334464 334465 439738 474095
unique(df_orig$nid[df_orig$location_id %in% locs$location_id[locs$parent_id==67]])
japan_nid <- c(336851, 336852)
japan_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% japan_nid])
unique(df_orig$location_name[df_orig$nid %in% japan_nid])
df_orig$is_outlier[df_orig$nid %in% japan_nid] <- as.integer(1)

# 2- drop Italy Hospital data NID =334465 and other NID. Ministry of Health (Italy). Italy Hospital Inpatient Discharges 2005-2019 : 334464 334465 439738 474095
italy_nid<- unique(df_orig$nid[df_orig$location_id %in% locs$location_id[locs$parent_id==86]])
unique(df_orig$clinical_data_type[df_orig$nid %in% italy_nid])
unique(df_orig$field_citation_value[df_orig$nid %in% italy_nid])
italy_nid %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% italy_nid])
unique(df_orig$location_name[df_orig$nid %in% italy_nid])
df_orig$is_outlier[df_orig$nid %in% italy_nid] <- as.integer(1)

# 3- drop Switzerland hospital data (HMDB ALL )Spain ,Norway, Latvia Portugal, Luxembourg, Denmark, Cyprus, Belgium, Austria
# 4- drop Botswana , Argentina data  Philippine and Georgia and Ecuador
few_locs<- c("Switzerland",
             "Spain" ,
             "Norway",
             locs$location_name[locs$parent_id==90],
             "Latvia",
             "Portugal",
             "Luxembourg",
             "Denmark",
             "Cyprus",
             "Belgium",
             "Austria",
             "Botswana" ,
             "Argentina",
             "Philippines",
             "Georgia",
             "Ecuador")

# Looping through each location in few_locs
for (location in few_locs) {
  # Find the unique nids for the current location
  nids_for_location <- unique(df_orig$nid[df_orig$location_name == location])

  cat("Location:", location, "\n")
  cat("NIDs:", nids_for_location, "\n")

  # For each nid, check clinical_version_id and print accordingly
  for (nid in nids_for_location) {
    # Extract clinical_version_id for the current nid
    clinical_version_ids <- df_orig$clinical_version_id[df_orig$nid == nid]

    # Check if clinical_version_ids are NA or equal to 4 or 5
    if (all(is.na(clinical_version_ids))) {
      # If clinical_version_id is NA for all entries of the nid, print "extracted"
      cat("NID:", nid, " - Clinical Data Status: extracted\n")
    } else if (any(clinical_version_ids %in% c(4, 5))) {
      # If any clinical_version_id for the nid is 4 or 5, print "clinical"
      cat("NID:", nid, " - Clinical Data Status: clinical\n")
    } else {
      # If there are clinical_version_ids but they are not 4, 5, or NA, handle as needed
      # This part is just a placeholder, as your requirements didn't specify this scenario
      cat("NID:", nid, " - Clinical Data Status: other\n")
    }
  }
  cat("\n") # Adding an extra line for better readability between locations
}

few_locs %in% df_orig$location_name

few_nids<- c(unique(df_orig$nid[df_orig$location_name %in% few_locs ]))
few_nids %in% df_orig$nid
unique(df_orig$is_outlier[df_orig$nid %in% few_nids])
unique(df_orig$location_name[df_orig$nid %in% few_nids])
df_orig$is_outlier[df_orig$nid %in% few_nids] <- as.integer(1)

outliers<- c(nepal_nid,
             sweden_nid,
             kenya_nid,
             armenia_nid,
             mongolia_nid,
             turkiye_nid,
             iran_nid,
             india_nid,
             nigeria_nid,
             china_nid)


# Looping through each nid in outliers
for(nid in outliers) {
  # Extracting unique clinical_data_type values for the current nid
  unique_types <- unique(df_orig$clinical_data_type[df_orig$nid == nid])

  # Printing the nid and its corresponding unique clinical_data_type values
  cat("nid:", nid, "\n")
  print(unique_types)
  cat("\n") # Adding a blank line for better readability
}

#exclude outliers from matching
df_orig <- df_orig[df_orig$is_outlier==0,]

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
