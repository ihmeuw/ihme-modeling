####################################
# MAD FILTER: ENDOCARDITIS
# AUTHOR: "USERNAME"
# PURPOSE: TO LABEL EXTREME DATA POINTS ACCORDING TO THE MEDIAN ABSOLUTE DEVIATION METHOD
#          MEDIAN ABSOLUTE DEVIATION = MAD = MEDIAN(|X_i -MEDIAN(X)|), we then filter those above 2 MADS or below .5 MADS. 
###################################


source('FILEPATH/get_ids.R')
source('FILEPATH/get_outputs.R')
source('FILEPATH/get_model_results.R')
source('FILEPATH/get_draws.R')
source('FILEPATH/get_bundle_data.R')
source('FILEPATH/upload_bundle_data.R')
source('FILEPATH/get_bundle_version.R')
source('FILEPATH/save_bundle_version.R')
source('FILEPATH/get_crosswalk_version.R')
source('FILEPATH/save_crosswalk_version.R')
source('FILEPATH/get_location_metadata.R')
source('FILEPATH/get_age_metadata.R')
source('FILEPATH/get_demographics.R')
source('FILEPATH/get_population.R')

if (Sys.info()[1] == "Linux") jpath <- "FILEPATH"
if (Sys.info()[1] == "Windows") jpath <- "FILEPATH"

library(openxlsx)
library(ggplot2)
library(plyr)
library(doBy)
library(readxl)
suppressMessages(library(R.utils))

'%ni%' <- Negate('%in%')

stats <- function(x) {
  c(
    mean = mean(x,na.rm=TRUE),
    SD = sd(x,na.rm=TRUE),
    min = min(x,na.rm=TRUE),
    max = max(x,na.rm=TRUE)
  )
}

mad.stats <- function(x) {
  c(
    mad = mad(x),
    median = median(x)
  )
}

locs <- get_location_metadata(location_set_id = 35, release_id = 16)

## Import hospital data
all_endo_inpatient <-get_bundle_version(bundle_version_id=47466) ## enter your bundle version here here. 
all_endo_inpatient <-all_endo_inpatient[, -c("parent_id")]  
endo_csmr <- all_endo_inpatient[measure=='mtspecific'] ## needed when oultiering CSMR data
hosp <- all_endo_inpatient[measure=='incidence',] ## subset to clinical
all_endo_other <- all_endo_inpatient[measure=='mtexcess']


## Outlier problematic clinical data
gbd2023 <- merge(hosp[, -c('ihme_loc_id')], locs[,c("location_id", "region_id", "region_name",  "location_type", "parent_id", "ihme_loc_id")], by=c("location_id"), all.x=T) 
df<- copy(gbd2023)

table <- df %>%
  group_by(location_id, nid) %>%
  mutate(unique_id_nid = cur_group_id()) %>%
  ungroup()
table <- unique(table[, c("parent_id", "location_id", "location_name", "nid", "unique_id_nid")])

outlier_bra_mex <- locs$location_id[which(locs$parent_id==130 | locs$parent_id==135 )] # Brazil & Mexico
outlier_poland  <- c(431674,397812,397813,397814) # NHF, Poland. 
outlier_usa     <- c(244369, 284421,284422, 234766, 234769,234771,234772,234774) #MS 2000 / HCUP / NCHS

df$is_outlier[which(df$location_id %in% outlier_bra_mex)] <- 1 #set outliers in Brazil & Mexico 
df[nid %in% outlier_poland, is_outlier:=1]                     #Poland
df[nid %in% outlier_usa, is_outlier:=1]                        # United States of America MS 2000, HCUP, NCHS 
df[location_id==193, is_outlier:=1]                            #Botswana
df[location_id==43887, is_outlier:=1]                          #India - Karnataka, Urban 
df <- df[location_name %in% c("Jordan", "Iran", "Meghalaya", "Karnataka", "Nepal", "Kenya",'Botswana'),is_outlier:=1] # troublesome sources
df[nid==469234, is_outlier := 1]                                    # Mongolia - not needed as Mongolia is excluded in line above
df[nid==428286 | nid== 464884, is_outlier := 1]                     # Russia
df[nid==514241| nid== 514242, is_outlier := 1]                      # Korea

# Outlier problematic CSMR data 
endo_csmr <- endo_csmr[location_id == 8  & year_start < 2010, is_outlier := 1] # outlier ICD9 in Taiwan because of ICD break

## Generate upper/lower thresholds using data from High-Income North America, Western Europe, Central Europe, Australia, High-Income Asia Pacific by sex, age group

# Identify locations with subnationals
df_nat <- copy(df[location_type %in% c('admin0'),])  
df_subnat <- copy(df[location_type %in% c('admin1','admin2', 'ethnicity'),]) 
df_subnat <- df_subnat[,.(cases_sum = sum(cases), sample_size_sum = sum(sample_size)), by = c('parent_id', 'age_start','age_end','sex','nid','year_start','year_end')]
df_subnat$mean <- df_subnat$cases_sum / df_subnat$sample_size_sum

# Merge region
loc_region <- unique(locs[, c('parent_id', 'region_id')])
df_subnat <- merge(df_subnat, loc_region, by='parent_id') # 2182   11

df2 <- rbind.fill(df_nat, df_subnat) # 7283   68
nrow(df[is.na(mean)]) # 0
summary(df2$mean)

df2$flag <- with(df2, ifelse(region_id %in% c(100, 73, 42, 65), 1, 0)) #High-income North America, Western Europe, Central Europe, HIAP

# Built MAD filter at the national level (df2)
df.mad<- summaryBy(data=subset(df2, flag==1 & mean!=0), mean ~ age_start + sex, FUN=mad.stats)
df.mad$mad.upper <- with(df.mad, mean.median + 2*mean.mad)
df.mad$mad.lower <- with(df.mad, mean.median - 0.5*mean.mad)
df.mad <- data.table(df.mad)

# Merge df.mad with new clinical (used for filtering)
df <- data.table(merge(df, df.mad, by=c("age_start", "sex"))) 
df$drop <- with(df, ifelse(mean>mad.upper | mean<mad.lower, 1, 0)) 

# Mark as outliers any data point greater than mad.upper limit or less than the min of the age/sex grouping for data that we "trust"
df$is_outlier <- with(df, ifelse(drop==1, 1, is_outlier))
table(df$is_outlier) # 23847 40047 (previous with xwalk: 45619 :  22338 41556 )

## Rebind data including other and new CSMR

df <- data.table(rbind(df, all_endo_other, endo_csmr, fill =TRUE))
table(df$measure)
 
# Add columns necessary for upload
df$bundle_id <- 121
df$bundle_name <-'Endocarditis'
df$age_demographer <-1
df$response_rate <- NA
df$extractor <- NA 
df <- df[,c("age_start", "sex", "location_id", "seq", "input_type", "underlying_nid", "nid", "source_type", "bundle_id", "bundle_name", "location_name",
            "year_start", "year_end", "age_end", "age_demographer", "measure", "standard_error", "cases", "effective_sample_size", "sample_size", 
            "unit_type", "unit_value_as_published", "uncertainty_type", "uncertainty_type_value", "representative_name", "urbanicity_type",
            "recall_type", "recall_type_value", "sampling_type", "response_rate", "design_effect", "extractor", "is_outlier", "mean", "lower", "upper")]
df$cv_hospital <- 1
df$crosswalk_parent_seq <- NA
df$unit_value_as_published <- 1
df$recall_type <- "Not Set"
df$unit_type   <- "Person"
df$urbanicity_type <- "Unknown"

df <- subset(df, df$mean>0)

