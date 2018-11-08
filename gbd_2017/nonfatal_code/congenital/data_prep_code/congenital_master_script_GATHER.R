#########################################################
## Master script for Congenital nonfatal data cleaning ## 
## Author: Lauren Wilner                               ##
## GBD 2017 Final version                              ## 
#########################################################

rm(list = ls())
os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  # Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe")
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  jtemp<-"FILEPATH"
}

## Load all necessary packages ##
# install.packages("R.methodsS3", lib="/home/j/temp/wilnerl/R_packages/")
library(data.table) 
library(ggplot2)
library(openxlsx)
library(rmarkdown)
library(readxl)
library(openxlsx)
library(openxlsx, lib.loc="FILEPATH")
library(readxl, lib="FILEPATH")
library(R.methodsS3, lib="FILEPATH")
library(R.oo, lib="FILEPATH")
library(R.utils, lib="FILEPATH")
##

## Source all shared functions ##
sourceDirectory(paste0(j, "FILEPATH"))
##


## EUROCAT NIDs: 
nids<-list(159930, 159937, 128835 , 159941, 163937, 159924, 159938, 159926, 159927, 159928, 159929, 159931, 159932, 159933, 159934, 159935, 159936, 159939, 159940, 159942, 163938, 163939, 159925, 128835)
##

## Load congenital bundle map ##
bundle_map<-fread(file=paste0(j, "FILEPATH/Congenital_bundle_model_and_cause_map_CURRENT.csv"))
bundle_map<-bundle_map[!(is.na(fullmod_bundle))]
# bundle_map<-bundle_map[fullmod_bundle!=2978 & fullmod_bundle!=2972 & fullmod_bundle!=2975 & fullmod_bundle!=439 ]
bundle<-unique(bundle_map$fullmod_bundle)
cause<-bundle_map$cause["cause"!=""]
bundle_name<-bundle_map$description
bundle_map<-bundle_map[fullmod_bundle!=2978 & fullmod_bundle!=2972 & fullmod_bundle!=2975 & fullmod_bundle!=439 ]
bundle<-unique(bundle_map$fullmod_bundle)
cause<-bundle_map$cause["cause"!=""]
bundle_name<-bundle_map$description

#### Outline of what this script is doing ################################################### 

# 0.  Wiping all outliers to apply all correct outliers 
# 1.  Marking any early years of data as group review 0 so that they do not show nor interfere with our model 
# 2.  Applying some universal outliers based on NID or prevalence over 1, and reassigning any prevalence values over 1 to 0.9999
# 3.  MAD outliers and a floor of 0 for if -1MAD is ever negative 
# 4.  Applying source type specific outliers 
#       - For hospital data, age restrictions, any bundle-specific things like females for klinefelter, etc. 
#       - For marketscan, age restrictions, outlier specific years of MS, any bundle-specific things like females for klinefelter, etc.
#       - For registry data, outlier India and South Africa registry data as per documentation 
# 5.  Outlier non-prevalence data (mostly mtwith data) for now

################################################### ###########################################

###########################################################################################################################################################

## For the following function, we are outliering for older ages means that are more than 3MADs over the median or less than 0.5MADs below the median. 
# If +3MADs is a higher value than +3MADs for age 0, we will take the age 0 +3MAD value (except for age_start==1). This is why i am also outliering if the mean is over mad3, to catch those edge cases. 
# Calculating beta_age on only MS data since calculating it on all data is yielding negative MAD lowers 
calc_mad <- function(age,df){
  
  #Pulling mean/median/mad from age 0 calculation that comes first 
  mean_0<-mean_0
  median_0<-median
  mad_0<-mad
  #Calculating mean/median for a given age on only MS data 
  mean_age<-df[age_start==age & extractor=="USERNAME" & mean!=0, mean(mean)]
  median_age<-df[age_start==age & extractor=="USERNAME"& mean!=0, median(mean)]
  #Mean for age 0 on only MS data: 
  mean_0_ms<-mean(df[age_start<1 & extractor=="USERNAME"& mean!=0, (mean) ] )
  # Prevalence_age_5 = Prevalence_age_zero + (Beta_age_5 * Prevalence_age_zero)
  beta_age = (mean_age + mean_0_ms) / mean_0_ms
  mad_age_specific = mad_0 + (beta_age * mad_0)
  mad_age = median_age + (3*mad_age_specific)
  mad_age_lower = 0.5*median_age
  
  return(c(mad_age_lower, mad_age))
  
}


mad_table<-data.table()
df<-list()
for (i in 1:length(bundle)){
  b=bundle[i]
  c=cause[i]
  n=bundle_name[i]
  message(paste(c, b))
  #df_temp<-get_epi_data(bundle=b)
  lapply(dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)
  con <- dbConnect(dbDriver("MySQL"),
                   username="USERNAME",
                   password="PASSWORD",
                   host="ADDRESS")
  df_temp_sql<-paste0("select * FROM epi.bundle_dismod where bundle_id = ", b)
  df_temp<-dbGetQuery(con, df_temp_sql)
  lapply(dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)
  for(var in c("mad3", "mad_n1", "median")){
    if(var %in% names(df_temp)){
      df_temp<-df_temp[,-c("mad3", "mad_n1", "median")]}
  }
  

###########################################################################################################################################################
# Clear current outliers 
###########################################################################################################################################################

df_temp<-df_temp[measure=="prevalence"]
df<-as.data.table(df_temp)
df<-df[, is_outlier:=0]
df<-df[, outlier_note:=""]
if(b=638){df<-df[,group:=NA]}


## NEED TO DELETE NIDS THAT THE HOSP TEAM FORGOT ABOUT: 
df<-df[nid!=336851 & nid!=336852 & nid!=337129]



####### COMMENT OUT WHEN NOT UPLOADING NEW HOSP DATA #######
###########################################################################################################################################################
###########################################################################################################################################################
###########################################################################################################################################################
# Append new hospital data when applicable  
###########################################################################################################################################################
# 1. Read in hosp data
hosp<-read.xlsx(paste0(j, "FILEPATH/",b,"_GBD2017_v8_2018_05_15.xlsx"), sheet="extraction")
hosp<-as.data.table(hosp)

#Filling in mean3 and mean2 where it is missing using correction factor 3
#Sometimes this creates a mean>1. It is not all the time when the correction factor is big that it does this, but it occurs sometimes.
hosp[, encounter_anydx_ind_rate := as.double(encounter_anydx_ind_rate) ]
hosp[is.na(encounter_anydx_ind_rate), encounter_anydx_ind_rate:=inpt_primarydx_admit_rate*correction_factor_3]
hosp[, upper_encounter_anydx_ind_rate := as.double(upper_encounter_anydx_ind_rate) ]
hosp[is.na(upper_encounter_anydx_ind_rate), upper_encounter_anydx_ind_rate:=upper_inpt_primarydx_admit_rate*correction_factor_3]
hosp[, lower_encounter_anydx_ind_rate := as.double(lower_encounter_anydx_ind_rate) ]
hosp[is.na(lower_encounter_anydx_ind_rate), lower_encounter_anydx_ind_rate:=lower_inpt_primarydx_admit_rate*correction_factor_3]

hosp[, inpt_anydx_ind_rate := as.double(inpt_anydx_ind_rate) ]
hosp[is.na(inpt_anydx_ind_rate), inpt_anydx_ind_rate:=inpt_primarydx_admit_rate*correction_factor_2]
hosp[, upper_inpt_anydx_ind_rate := as.double(upper_inpt_anydx_ind_rate) ]
hosp[is.na(upper_inpt_anydx_ind_rate), upper_inpt_anydx_ind_rate:=upper_inpt_primarydx_admit_rate*correction_factor_2]
hosp[, lower_inpt_anydx_ind_rate := as.double(lower_inpt_anydx_ind_rate) ]
hosp[is.na(lower_inpt_anydx_ind_rate), lower_inpt_anydx_ind_rate:=lower_inpt_primarydx_admit_rate*correction_factor_2]
#Choosing mean2:
setnames(hosp, "inpt_anydx_ind_rate", "mean")
setnames(hosp, "upper_inpt_anydx_ind_rate", "upper")
setnames(hosp, "lower_inpt_anydx_ind_rate", "lower")

#2. Read in MS data
ms<-read_excel(paste0(j, "FILEPATH/ALL_", b, "_GBD2017_v8_fixed_ages_2018_05_10.xlsx"))
ms<-as.data.table(ms)

# #3. Append hospital and marketscan data to full bundle
df<-as.data.table(df)

if(b!=624){
df<- rbindlist(list(df, hosp, ms), fill = T)
}
  #for now, we are excluding MS data for abdominal wall becuase the numbers are implausibly low (decision via email may 10, 2018):
  if (b==624){
    df<- rbindlist(list(df, hosp), fill = T)
  }

#make sure that the crosswalk cv's are filled in as 0s whenever they get filled in as NAs
for(colname in colnames(df)[grepl("cv_", colnames(df))]) {
  print(colname)
  df[is.na(get(colname)), (colname) := 0]
}

# ### Adding in Taiwan claims data ### 
# ms_taiwan<-read_excel(paste0(j, "FILEPATH/ALL_", b, "_GBD2017_v8_TWN_MS2013_2018_05_24.xlsx"))
# ms_taiwan<-as.data.table(ms_taiwan)
# if(b!=624){
#   df<- rbindlist(list(df, ms_taiwan), fill = T)
# }
# #for now, we are excluding MS data for abdominal wall becuase the numbers are implausibly low (decision via email may 10, 2018): 
# df<-as.data.table(df)


#Drop old variable names from upload 
###########################################################################################################################################################
###########################################################################################################################################################
###########################################################################################################################################################
####### COMMENT OUT WHEN NOT UPLOADING NEW HOSP DATA #######

###########################################################################################################################################################
# Mark any data before 1985 as group_review==0 per discussion with Nick 
###########################################################################################################################################################
df<-df[year_start<1985, group_review:=0]

###########################################################################################################################################################
###########################################################################################################################################################
### OUTLIERS TO BE APPLIED TO ALL DATA, REGARDLESS OF SOURCE_TYPE ### 
###########################################################################################################################################################
###########################################################################################################################################################
print("here")

df[upper>=1 & df$measure=="prevalence", upper:=0.999999]
df[mean>=1 & df$measure=="prevalence", mean:=0.9999999]
df[lower>=1 & df$measure=="prevalence", lower:=0.9999999]
#this line is a catch for if MAD is negative. If MAD is negative, then the floor should be 0. 
#therefore, if mean is 0, it will be outliered, but since 0 is the floor, anything between 0 and the MAD upper can stay in. 
df<-df[mean==0 & df$measure=="prevalence", is_outlier:=1]

#Based on one of Alison's datafixes, these NIDs were outliered for cong_heart. They are all literature sources. 
lit_outlier_nids<-list(138931, 138773, 138956, 138954, 138799, 277489, 138884, 138788, 138824)
df<-df[((nid %in% lit_outlier_nids) & (c=="cong_heart")), is_outlier:=1]


print ("ready for mad")
### MAD ### 

############
#non-chromo#
############

#doing special things to total msk, limb reduction, and urogen
if  (b!=439 & b!=2975 & b!=2972 & b!=2978 & b!=3029 & b!= 436 & b!=638 & b!=437 & b!=438) {
  
  #want to calculate MAD on all under 1 if it is NTDs
  if (b==608 | b==610 | b==612){
    median<- median( df[age_start<1, (mean) ] )
    mean_0<- mean( df[age_start<1 , (mean) ] )
    mad<- mad( df[age_start<1, (mean) ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (0.5*median)
    
    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start<1 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start<1 & mean<mad_n1),outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start<1 & mean>mad3),outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start<1 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
    df[flag==1, is_outlier:=1]
    df<-df[, -c("flag")]
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)  
    
    for(i in 1:(nrow(table))){
      age=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
      
    }        
  }
  
  if (b==614){
    median<- median( df[age_start<1, (mean) ] )
    mean_0<- mean( df[age_start<1, (mean) ] )
    mad<- mad( df[age_start<1, (mean) ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (median - (0.5*mad))
    
    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start<1 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start<1 & mean<mad_n1),outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start<1 & mean>mad3),outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start<1 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
    df[flag==1, is_outlier:=1]
    df<-df[, -c("flag")]
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)  
    
    for(i in 1:(nrow(table))){
      age=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
    }        
  }
  
  
  if (b==616){
    median<- median( df[age_start<1 & (nid %in% nids | (underlying_nid %in% nids) | extractor=="USERNAME"), mean ] )
    mean_0<- mean( df[age_start<1 & (nid %in% nids | (underlying_nid %in% nids) | extractor=="USERNAME"), (mean) ] )
    mad<- mad( df[age_start<1 & ( nid %in% nids | (underlying_nid %in% nids) | extractor=="USERNAME"), mean ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (0.5*median)
    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start<1 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start<1 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start<1 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start<1 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
    df[flag==1, is_outlier:=1]
    df<-df[, -c("flag")]
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)  
    
    for(i in 1:(nrow(table))){
      age=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
    }        
    
  }
  
  
  if (b==602 | b==604){
    median<- median( df[age_start<1 & extractor=="USERNAME", mean ] )
    mean_0<- mean( df[age_start<1 & extractor=="USERNAME", (mean) ] )
    mad<- mad( df[ age_start<1 & extractor=="USERNAME", mean ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (0.5*median)
    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start<1 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start<1 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start<1 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start<1 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
    df[flag==1 & age_start>=1, is_outlier:=1]
    df<-df[, -c("flag")]
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)  
    
    for(i in 1:(nrow(table))){
      age=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
    }        
    
  }
  
  if ((b!=608 & b!=610 & b!=612 & b!=614 & b!=602 & b!=604 & b!=616)) {
    median<- median( df[ age_start<1 & (nid %in% nids | (underlying_nid %in% nids)), mean ] )
    mean_0<- mean( df[ age_start<1 & (nid %in% nids | (underlying_nid %in% nids)), mean ] )
    mad<- mad( df[age_start<1 & (nid %in% nids | (underlying_nid %in% nids)), mean ] )
    mad3<- ((3*mad)+median)
    mad_n1<- (0.5*median)
    
    df<-df[(mean>mad3), is_outlier:=1]
    df<-df[(age_start<1 & mean<mad_n1), is_outlier:=1]
    df<- df[(age_start<1 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
    df<- df[(age_start<1 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
    
    loc_yr<-df[(age_start<1 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid)]
    loc_yr[, flag:=1]
    loc_yr <- unique(loc_yr)
    df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
    df[flag==1, is_outlier:=1]
    df<-df[, -c("flag")]
    
    #For older ages: 
    ages<-unique(df[age_start!=0]$age_start)
    mads<-lapply(ages, calc_mad, df)
    mads <-do.call('rbind',mads)
    table<-cbind(ages, mads)
    print(table)  
    
    for(i in 1:(nrow(table))){
      age=table[i, 1]
      mad_age_lower=table[i, 2]
      mad_age=table[i, 3]
      mad_age<-as.numeric(mad_age)
      mad_age_lower<-as.numeric(mad_age_lower)
      df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
      df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
      df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
    }        
    
  }
  print("end of non chromo loop")
}


########
#chromo#
########

if (b==436){
  median<- median( df[age_start<1  & (nid %in% nids | (underlying_nid %in% nids) | extractor=="USERNAME"), (mean) ] )
  mean_0<- mean( df[age_start<1  & (nid %in% nids | (underlying_nid %in% nids) | extractor=="USERNAME"), (mean) ] )
  mad<- mad( df[age_start<1 & (nid %in% nids | (underlying_nid %in% nids) | extractor=="USERNAME"), (mean) ] )
  mad3<- ((3*mad)+median)
  mad_n1<- (0.5*median)
  
  
  df<-df[(mean>mad3), is_outlier:=1]
  df<-df[(age_start<1 & mean<mad_n1), is_outlier:=1]
  df<- df[(age_start<1 & mean<mad_n1),outlier_note:="outliering based on MAD restrictions"]
  df<- df[(age_start<1 & mean>mad3),outlier_note:="outliering based on MAD restrictions"]
  
  loc_yr<-df[(age_start<1 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid)]
  loc_yr[, flag:=1]
  loc_yr <- unique(loc_yr)
  df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
  df[flag==1, is_outlier:=1]
  df<-df[, -c("flag")]
  print("downs outliers applied")
  
  
  #For older ages: 
  ages<-unique(df[age_start!=0]$age_start)
  mads<-lapply(ages, calc_mad, df)
  mads <-do.call('rbind',mads)
  table<-cbind(ages, mads)
  print(table)  
  
  for(i in 1:(nrow(table))){
    age=table[i, 1]
    mad_age_lower=table[i, 2]
    mad_age=table[i, 3]
    mad_age<-as.numeric(mad_age)
    mad_age_lower<-as.numeric(mad_age_lower)
    df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
    df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
    df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
  }        
  
}


if  (b==638) {
  #Feb 1: not subsecting to hospital and registry, doing this to all data in the bundle
  
  median<- median( df[(age_start<1 & (nid %in% nids | (underlying_nid %in% nids)) & mean!=0), mean ] )
  mean_0<- mean( df[(age_start<1 & (nid %in% nids | (underlying_nid %in% nids)) & mean!=0), mean ] )
  mad<- mad( df[(age_start<1 & (nid %in% nids | (underlying_nid %in% nids)) & mean!=0), mean ] )
  mad3<- ((3*mad)+median)
  mad_n1<- (0.5*median)
  
  df<-df[(mean>mad3), is_outlier:=1]
  df<-df[(age_start<1 & mean<mad_n1), is_outlier:=1]
  df<- df[(age_start<1 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
  df<- df[(age_start<1 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
  
  loc_yr<-df[(age_start<1 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid)]
  loc_yr[, flag:=1]
  loc_yr <- unique(loc_yr)
  df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
  df[flag==1, is_outlier:=1]
  df<-df[, -c("flag")]
  
  #For older ages: 
  ages<-unique(df[age_start!=0]$age_start)
  mads<-lapply(ages, calc_mad, df)
  mads <-do.call('rbind',mads)
  table<-cbind(ages, mads)
  print(table)  
  
  for(i in 1:(nrow(table))){
    age=table[i, 1]
    mad_age_lower=table[i, 2]
    mad_age=table[i, 3]
    mad_age<-as.numeric(mad_age)
    mad_age_lower<-as.numeric(mad_age_lower)
    df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
    df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
    df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
  }        
  
}

#########################
#turner and klinefelter##
#########################
### TURNER ###
if (b==437) {
  eurocat_turner<-df[age_start<1 & ((nid %in% nids) | (underlying_nid %in% nids) | extractor=="USERNAME")]
  eurocat_turner<-eurocat_turner[, mean:=mean+0.0001]
  eurotcat_turner<-as.double(eurocat_turner$mean)
  
  #Log eurocat data and then calculate the mean absolute deviation: 
  eurocat_turner<-eurocat_turner[, log_mean:=log(mean)]
  center_turner<- mean(eurocat_turner$log_mean)
  mean_0<-center_turner
  mad_mean_turner<- mean(abs(eurocat_turner$log_mean-center_turner))
  mad3_mean_turner<- ((3*mad_mean_turner)+center_turner)
  mad_n1_mean_turner<- center_turner - mad_mean_turner
  mad<-mad_mean_turner
  mad3<-mad3_mean_turner
  mad_n1<-mad_n1_mean_turner
  
  df<-df[(mean>(exp(mad3_mean_turner))), is_outlier:=1]
  df<-df[(age_start<1 & mean<(exp(mad_n1_mean_turner))), is_outlier:=1]
  df<-df[(age_start<1 & mean<(exp(mad_n1_mean_turner))), outlier_note:="outliering based on MAD restrictions"]
  df<-df[(age_start<1 & mean>(exp(mad3_mean_turner))), outlier_note:="outliering based on MAD restrictions"]
  
  loc_yr<-df[(age_start<1 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid)]
  loc_yr[, flag:=1]
  loc_yr <- unique(loc_yr)
  df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
  df[flag==1 & age_start>=1, is_outlier:=1]
  df<-df[, -c("flag")]
  
  #For older ages: 
  ages<-unique(df[age_start!=0]$age_start)
  mads<-lapply(ages, calc_mad, df)
  mads <-do.call('rbind',mads)
  table<-cbind(ages, mads)
  print(table)  
  
  for(i in 1:(nrow(table))){
    age=table[i, 1]
    mad_age_lower=table[i, 2]
    mad_age=table[i, 3]
    mad_age<-as.numeric(mad_age)
    mad_age_lower<-as.numeric(mad_age_lower)
    df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
    df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
    df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
  }        
  
}

if (b==438) {
  ###########################################################################
  ### KLINEFELTER ###
  eurocat_klinefelter<-df[age_start<1 & ((nid %in% nids) | (underlying_nid %in% nids) | extractor=="USERNAME")]
  eurocat_klinefelter<-eurocat_klinefelter[, mean:=mean+0.0001]
  eurotcat_klinefelter<-as.double(eurocat_klinefelter$mean)
  
  eurocat_klinefelter<-eurocat_klinefelter[, log_mean:=log(mean)]
  center_klinefelter<- mean(eurocat_klinefelter$log_mean)
  mean_0<-center_klinefelter
  mad_mean_klinefelter<- mean(abs(eurocat_klinefelter$log_mean-center_klinefelter))
  mad3_mean_klinefelter<- ((3*mad_mean_klinefelter)+center_klinefelter)
  mad_n1_mean_klinefelter<- center_klinefelter - mad_mean_klinefelter
  mad<-mad_mean_klinefelter
  mad3<-mad3_mean_klinefelter
  mad_n1<-mad_n1_mean_klinefelter
  
  df<-df[(mean>(exp(mad3_mean_klinefelter))), is_outlier:=1]
  df<-df[(age_start<1 & mean<(exp(mad_n1_mean_klinefelter))), is_outlier:=1]
  df<-df[(age_start<1 & mean<(exp(mad_n1_mean_klinefelter))), outlier_note:="outliering based on MAD restrictions"]
  df<-df[(age_start<1 & mean>(exp(mad3_mean_klinefelter))), outlier_note:="outliering based on MAD restrictions"]
  
  loc_yr<-df[(age_start<1 & (mean>(exp(mad3_mean_klinefelter)) | mean<(exp(mad_n1_mean_klinefelter)))), list(location_id, year_start, year_end, sex, nid)]
  loc_yr[, flag:=1]
  loc_yr <- unique(loc_yr)
  df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
  df[flag==1 & age_start>=1, is_outlier:=1]
  df<-df[, -c("flag")]
  
  #For older ages: 
  ages<-unique(df[age_start!=0]$age_start)
  mads<-lapply(ages, calc_mad, df)
  mads <-do.call('rbind',mads)
  table<-cbind(ages, mads)
  print(table)  
  
  for(i in 1:(nrow(table))){
    age=table[i, 1]
    mad_age_lower=table[i, 2]
    mad_age=table[i, 3]
    mad_age<-as.numeric(mad_age)
    mad_age_lower<-as.numeric(mad_age_lower)
    df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
    df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
    df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
  }        
  
}

##################
## total chromo ##
##################
if (b==3029) {
  ###########################################################################
  ### TOTAL CHROMO ###
  
  ##################################  PROCESS  ################################################## 
  ###############################################################################################
  # 1. pull eurocat data from the 4 sub chromo causes 
  # 2. append all 4 eurocat datasets onto each other 
  # 3. add the proportion of other chromo to the prevalence numbers for each of these 
  #(use the prop other from michelle's calculation)
  # Find these proportions in: J:\WORK\12_bundle\cong\01_input_data\06_proportion_other
  # Only neeed the proportion for age_start=0 as this is being applied to registry data
  # 4. Calculate MAD and median and apply those values to the total chromo bundle 
  # 5. Upload the new data
  ###############################################################################################
  ###############################################################################################
  
  ###############################################################################################
  # 1. pull eurocat data from the 4 sub chromo causes 
  ###############################################################################################
  keepvars<-c("bundle_id", "nid", "extractor", "underlying_nid", "source_type", "location_id", "location_name", "sex", "year_start", "year_end", "age_start", "age_end", "measure", "mean", "upper", "lower", "is_outlier")
  
  down_temp<-get_epi_data(b=436)
  down_temp<-as.data.table(down_temp)
  down_small<-down_temp[down_temp$measure=="prevalence",]
  down_small<-as.data.table(down_small)
  down_small<-down_small[,..keepvars]
  down_eurocat<-down_small[(nid %in% nids | underlying_nid %in% nids | extractor=="USERNAME")]
  
  turner_temp<-get_epi_data(b=437)
  turner_temp<-as.data.table(turner_temp)
  turner_small<-turner_temp[turner_temp$measure=="prevalence",]
  turner_small<-turner_small[,..keepvars]
  turner_small<-as.data.table(turner_small)
  turner_eurocat<-turner_small[(nid %in% nids | underlying_nid %in% nids | extractor=="USERNAME")]
  
  
  klinefelter_temp<-get_epi_data(b=438)
  klinefelter_temp<-as.data.table(klinefelter_temp)
  klinefelter_small<-klinefelter_temp[klinefelter_temp$measure=="prevalence",]
  klinefelter_small<-klinefelter_small[,..keepvars]
  klinefelter_small<-as.data.table(klinefelter_small)
  klinefelter_eurocat<-klinefelter_small[(nid %in% nids | underlying_nid %in% nids | extractor=="USERNAME")]
  
  
  edpatau_temp<-get_epi_data(b=638)
  edpatau_temp<-as.data.table(edpatau_temp)
  edpatau_small<-edpatau_temp[edpatau_temp$measure=="prevalence",]
  edpatau_small<-edpatau_small[,..keepvars]
  edpatau_small<-as.data.table(edpatau_small)
  edpatau_eurocat<-edpatau_small[(nid %in% nids | underlying_nid %in% nids | extractor=="USERNAME")]
  
  
  ###############################################################################################
  # 2. append all 4 eurocat datasets onto each other 
  ###############################################################################################
  all_chromo_eurocat<-rbind(down_eurocat, turner_eurocat, klinefelter_eurocat, edpatau_eurocat)
  
  ###############################################################################################
  # 3. add the proportion of other chromo to the prevalence numbers for each of these 
  #(use the prop other from michelle's calculation)
  # Find these proportions in: FILEPATH
  # Only neeed the proportion for age_start=0 as this is being applied to registry data
  ###############################################################################################
  
  #Using this other proportion calculation; can update when we get new hospital data?? 
  #"FILEPATH\cong_chromo_other_props_from_subcause_ALL_439_GBD2017_v4_2018_02_06.xlsx"
  # Proportion other for females age_start=0: 
  #  0.874898823
  # Proportion other for females age_start=0: 
  #  0.900157585
  # Proportion other for both sexes as the average of the above numbers: 
  # 0.887528204
  
  all_chromo_eurocat<-as.data.table(all_chromo_eurocat)
  all_chromo_eurocat<-all_chromo_eurocat[sex == "Female", mean_total := (mean/(1-.874898823))]
  all_chromo_eurocat<-all_chromo_eurocat[sex == "Male", mean_total := (mean/(1-.900157585))]
  all_chromo_eurocat<-all_chromo_eurocat[sex == "Both", mean_total := (mean/(1-.887528204))]
  
  
  ###############################################################################################
  # 4. Calculate MAD and median and apply those values to the total chromo bundle 
  ###############################################################################################
  #Calculate MAD bounds
  all_chromo_eurocat_nonzero<-all_chromo_eurocat[mean!=0]
  median<- median( all_chromo_eurocat_nonzero[ age_start<1 & (nid %in% nids| underlying_nid %in% nids), mean_total ] )
  mean<- mean( all_chromo_eurocat_nonzero[ age_start<1 & (nid %in% nids| underlying_nid %in% nids), mean_total ] )
  mad<- mad( all_chromo_eurocat_nonzero[age_start<1 & (nid %in% nids| underlying_nid %in% nids), mean_total ] )
  mad3<- ((3*mad)+median)
  mad_n1<- (0.5*median)
  mean_0<-mean(df$mean[df$age_start==0])
  
  df<-df[(mean>mad3), is_outlier:=1]
  df<-df[(age_start<1 & mean<mad_n1), is_outlier:=1]
  df<- df[(age_start<1 & mean<mad_n1), outlier_note:="outliering based on MAD restrictions"]
  df<- df[(age_start<1 & mean>mad3), outlier_note:="outliering based on MAD restrictions"]
  
  loc_yr<-df[(age_start<1 & (mean>mad3 | mean<mad_n1)), list(location_id, year_start, year_end, sex, nid)]
  loc_yr[, flag:=1]
  loc_yr <- unique(loc_yr)
  df<-merge(df, loc_yr, by=c("location_id", "year_start", "year_end", "sex", "nid"), all.x=TRUE)
  df[flag==1, is_outlier:=1]
  df<-df[, -c("flag")]
  
  #For older ages: 
  ages<-unique(df[age_start!=0]$age_start)
  mads<-lapply(ages, calc_mad, df)
  mads <-do.call('rbind',mads)
  table<-cbind(ages, mads)
  print(table)  
  
  for(i in 1:(nrow(table))){
    age=table[i, 1]
    mad_age_lower=table[i, 2]
    mad_age=table[i, 3]
    mad_age<-as.numeric(mad_age)
    mad_age_lower<-as.numeric(mad_age_lower)
    df<-df[age_start==age & mean>mad_age & age_start!=0, is_outlier:=1]
    df<-df[age_start==age & mean>mad3 & age_start!=0 & age_start!=1, is_outlier:=1]
    df<-df[age_start==age & mean<mad_age_lower & age_start!=0, is_outlier:=1]
  }        
}


#This will catch any bundles where -1MAD is negative, and implement a floor of 0:
df[mean==0, is_outlier:=1]


###########################################################################################################################################################
###########################################################################################################################################################
### OUTLIERS FROM GBD 2017 FIRST SUBMISSION  ### 
###########################################################################################################################################################
###########################################################################################################################################################

#outliering all kenya, nepal, and india hosp data: 
df<-df[source_type=="Facility - inpatient" & location_id %in% c(4841:4875, 43872:44959, 164, 163, 180, 35617:35663), is_outlier:=1]


df<-df[nid==284091, nid:=284093]


##############################
###### CLEFT 
##############################
if (b==435) { 
  #outlier some low vietnam data
  df<-df[nid==299375 & location_name=="Vietnam" & year_start==2013, is_outlier:=1]
  
  #Beijing data issue 
  df<-df[nid==337619 & age_start==1 & year_start==2013 & location_name=="Beijing", is_outlier:=1]

}

##############################
###### DOWNS 
##############################
if(b==436){
  
  #UTLA issues - there are no data in the UTLAs that are crazy, but hoping this will help.... 
  df<-df[mean>0.0015 & location_id %in% c(44643:44792), is_outlier:=1]
  
  #one SSA outlier that is very low
  df<-df[age_start==0 & location_name=="Tanzania" & nid==274105 & year_start==2011, is_outlier:=1]
  
  #india outliers 
  df<-df[nid==274577 & age_start==0 & year_start==2003 & location_name=="Tamil Nadu, Urban", is_outlier:=1]
  df<-df[nid==284231 & age_start==0 & location_name=="West Bengal" & year_start==2011, is_outlier:=1]
  
}

##############################
###### TURNER 
##############################
if (b==437){
  
  #sweden outlier - way too high 
  df<-df[year_start==2003 & age_start==10 & nid==234760 & location_name=="Sweden", is_outlier:=1]}

##############################
###### all heart stuff  
##############################
if (c=="cong_heart"){
  
  #drop any rows with the more specific ICD codes because those are not quite double the others and are a total of total + all the other indiv heart bundles 
  df<-df[!(case_diagnostics %like% "Q20.0")]
}

if (b==616){
  df<-df[location_id %in% c(35617:35663), is_outlier:=1]
}

if(b==604){
  #tibet outliers, tibet has data very inconsistent with the other subnats
  df<-df[location_name=="Tibet", is_outlier:=1]
  #england subnats - some of them are way too high and are inconsistent with the rest of the country. outliering these. 
  df<-df[((location_id %in% c(4618:4749) | location_id %in% c(44643:44792)) & mean> 0.0005), is_outlier:=1]
  #issue for brazil too - some way too high 
  df<-df[location_id %in% c(4750:4776) & mean>0.0006, is_outlier:=1]
}

if (b==610){
  
  #outlier low china data
  df<-df[location_id %in% c(491:521, 6) & mean<0.00002, is_outlier:=1]
  #outlier thailand and phillippines data that are too low 
  df<-df[location_id %in% c(16, 18), is_outlier:=1]
  #outlier high Japan data 
  df<-df[location_id %in% c(67, 35424:35470) & mean>0.00008, is_outlier:=1]
  #outlier turkey data
  df<-df[location_name=="Turkey", is_outlier:=1]
  
}

#atresia outlier blackpool age 10 data 
if (b==626){ 
  df<-df[location_name=="Blackpool" & age_start>5, is_outlier:=1]
  
}
if(b==606) {
  
  #outlier ecuador data in 20+
  df<-df[location_id== 122 & age_start>=20, is_outlier:=1]
  #outlier kenya data 
  df<-df[location_id %in% c(180, 35617:35663), is_outlier:=1]
  
}

if (b==628) {
  
  #vietnam data too low. malaysia is too low too, but has no data. maybe thailand and vietnam data are the issue. 
  df<-df[location_name=="Vietnam" | location_name=="Thailand", is_outlier:=1]
  #SSA is too low. Maybe it is the one data point in the entire region other than kenya, which is very low and comes from E SSA or the Kenya hosp data, which will be outliered above. 
  df<-df[location_name=="Uganda", is_outlier:=1]
  
}

if(b==632) {
  
  df<-df[location_name=="Uganda", is_outlier:=1]
  df<-df[location_name=="Nigeria", is_outlier:=1]
  #outliering low puerto rico data: 
  df<-df[location_id==385, is_outlier:=1]
  #outlier turkey, saudi, and UAE data
  df<-df[location_id== 152, is_outlier:=1]
  df<-df[location_id== 156, is_outlier:=1]
  df<-df[location_id== 155, is_outlier:=1]
  
}

if (b==634){
  
  df<-df[location_id==8, is_outlier:=1]
  df<-df[location_id %in% c(4910:4928, 53432), is_outlier:=1]
  
}






###########################################################################################################################################################
###########################################################################################################################################################
### OUTLIERS TO BE APPLIED TO SPECIFIC SOURCE_TYPES OF DATA  ### 
###########################################################################################################################################################
###########################################################################################################################################################


####### HOSPITAL DATA SECTION ########

hold = copy(df)
df = df[(source_type == 'Facility - inpatient' & extractor %like% "castl"),]

#outliering all kenya, nepal, and india hosp data: 
df<-df[location_id %in% c(4841:4875, 43872:44959, 164, 163, 180, 35617:35663), is_outlier:=1]
#
#
###############################################
#Outliering based on age and mean values:##### 
###############################################
#
#
#Outliering based on age and mean values. 
## AGE ## 
#   b. Age restrictions: 
# 			i. VSD/ASD, total, and critical malformations-keep female data through age start 40 (including age start 40) but keep males through age start 15 - women get evaluated when they get pregnant so there is more detection 
# 			ii. Single ventricle - keep through age start 40 for both sexes
# 			iii. Limb reduction, total msk - keep females through age start 50 and males through age start 15
# 			iv. Cleft, total chromo, ed patau - keep the same, drop both after age start 15 for both sexes 
# 			v. Klinefelter/turner - keep through age start 25 
# 			vi. Down - redo in log space 
# 			vii. Total ntd, spina bifida, enceph - keep females through age 40 and males through age 15 
# 			viii. Anenceph - leave it as is 
# 			ix. Cong malformations of abdominal wall, atresia of digestive tract, diagragmatic hernia  - keep through age 15
# 			x. Total digestive - keep through age 40 for both sexes 
# 			xi. Genital, urinary - keep females through age start 50 and same for males 

# VSD/ASD, total heart, critical malformations 
if (b==628 | b==632 | b==634)   {
  df<-df[age_start>40 & sex=="Female", is_outlier:=1]
  df<-df[age_start>=15 & sex=="Male", is_outlier:=1]
}

#Single ventricle
if (b==630)                     {df<-df[age_start>40, is_outlier:=1]}

# limb reduction and total msk
if (b==602 | b==604)            {
  df<-df[age_start>50 & sex=="Female", is_outlier:=1]
  df<-df[age_start>=15 & sex=="Male", is_outlier:=1]
}

# klinefelter and turner
if (b==438 | b==437)            {df<-df[age_start>25, is_outlier:=1]}

# total ntd, spina bifida, enceph
if (b==614 | b==608 | b==612)   {
  df<-df[age_start>40 & sex=="Female", is_outlier:=1]
  df<-df[age_start>=15 & sex=="Male", is_outlier:=1]}

# total digestive 
if (b==620)                     {df<-df[age_start>40, is_outlier:=1]}

# cong genital/urinary
if (b==616 | b==618)            {df<-df[age_start>50, is_outlier:=1]}

# EVERYTHING ELSE: 
if ((b!=628 & b!=632 & b!=634 & b!=630 & b!=602 & b!=604 & b!=438 & b!=437 & b!=614 & b!=608 & b!=612 & b!=620 & b!=616 & b!=618)) {
  df[age_start>=15, is_outlier:=1]}



#Capping means/uppers at 0.999999 in order to get rid of any prevalence>1. When using the correction factors, we ended up with prevalence>1 for a few cases. 
#As per email correspondence with Nick/Helen, we are just capping anything >=1 at 0.999999
df[upper>=1, upper:=0.999999]
df[mean>=1, mean:=0.9999999]
df[lower>=1, lower:=0.9999999]
df<-df [!(df$age_end>69), ]

#
#
##############################################################
#Adding covariates to indicate that this is hospital data##### 
##############################################################
#
#
#Adding covariates to indicate that these are hospital data
df[, cv_hospital:= 1]
df[, cv_hosp_under1:= 0]
df[, cv_hosp_over1:= 0]
df[age_start==0, cv_hosp_under1:=1]
df[age_start>0, cv_hosp_over1:=1]

#
#
####################################
#Model specific outlier thresholds:#
####################################
#Anencephaly: 
# 1. Outlier data for age over 1
# 2. Make any data for age_start==0 age_end==99 
if (b==610)                     {df[age_start>=1, is_outlier:=1]}
if (b==610)                     {df[age_start<1, age_end:=99]}
#DROPPING:
if(b==638)                      {df<-df[!(df$age_start>=5), ]}
#if(b==614)                      {df<-df[upper<=1]}
if(b==437)                      {df<-df[sex=="Female"]}
if(b==438)                      {df<-df[sex=="Male"]}

df = rbind(hold[!(source_type == 'Facility - inpatient' & extractor %like% "castl"),], df, fill=TRUE)


####### MS DATA SECTION ########

if (b!=624){
  hold = copy(df)
  df = df[(source_type  == "Facility - other/unknown" & extractor %like% "castl"),]
  
  df<-df[mean<1]
  df<-df[mean==0, is_outlier:=1]
  df<-df[age_start>=15, is_outlier:=1]
  
  df<-df[!(cv_marketscan_all_2000==1)]
  df<-df[!(cv_marketscan_inp_2000==1)]
  
  
  #Anencephaly: 
  # 1. Outlier data for age over 1
  # 2. Make any data for age_start==0 age_end==99 
  if (b==610)                     {df[age_start>=1, is_outlier:=1]}
  if (b==610)                     {df[age_start<1, age_end:=99]}
  #DROPPING:
  if(b==638)                      {df<-df[!(df$age_start>=5), ]}
  #if(b==614)                      {df<-df[upper<=1]}
  if(b==437)                      {df<-df[sex=="Female"]}
  if(b==438)                      {df<-df[sex=="Male"]}
  
  
  if(b==614)  {df[mean<1]}
  if(b==610)  {df[cv_marketscan_all_2000==1 | cv_marketscan_inp_2000==1 | cv_marketscan_all_2010==1 | cv_marketscan_inp_2010==1 | cv_marketscan_all_2012==1 | cv_marketscan_inp_2012==1, is_outlier:=1]}
  if(b==638)  {df<-df[!(df$age_start>=5), ]}
  
  df<-df[location_name == "Hawaii", is_outlier:=1]
  
  ## AGE ## 
  
  #   b.Age restrictions:
  # 			i. VSD/ASD, total, and critical malformations-keep female data through age start 40 (including age start 40) but keep males through age start 15 - women get evaluated when they get pregnant so there is more detection 
  # 			ii. Single ventricle - keep through age start 40 for both sexes
  # 			iii. Limb reduction, total msk - keep females through age start 50 and males through age start 15
  # 			iv. Cleft, total chromo, ed patau - keep the same, drop both after age start 15 for both sexes 
  # 			v. Klinefelter/turner - keep through age start 25 
  # 			vi. Down - redo in log space 
  # 			vii. Total ntd, spina bifida, enceph - keep females through age 40 and males through age 15 
  # 			viii. Anenceph - leave it as is 
  # 			ix. Cong malformations of abdominal wall, atresia of digestive tract, diagragmatic hernia  - keep through age 15
  # 			x. Total digestive - keep through age 40 for both sexes 
  # 			xi. Genital, urinary - keep females through age start 50 and same for males 
  
  # VSD/ASD, total heart, critical malformations 
  if (b==628 | b==632 | b==634)   {
    df<-df[age_start>40 & sex=="Female", is_outlier:=1]
    df<-df[age_start>=15 & sex=="Male", is_outlier:=1]
  }
  
  #Single ventricle
  if (b==630)                     {df<-df[age_start>40, is_outlier:=1]}
  
  # limb reduction and total msk
  if (b==602 | b==604)            {
    df<-df[age_start>50 & sex=="Female", is_outlier:=1]
    df<-df[age_start>=15 & sex=="Male", is_outlier:=1]
  }
  
  # klinefelter and turner
  if (b==438 | b==437)            {df<-df[age_start>25, is_outlier:=1]}
  
  # total ntd, spina bifida, enceph
  if (b==614 | b==608 | b==612)   {
    df<-df[age_start>40 & sex=="Female", is_outlier:=1]
    df<-df[age_start>=15 & sex=="Male", is_outlier:=1]}
  
  # total digestive 
  if (b==620)                     {df<-df[age_start>40, is_outlier:=1]}
  
  # cong genital/urinary
  if (b==616 | b==618)            {df<-df[age_start>=50, is_outlier:=1]}
  
  # EVERYTHING ELSE: 
  if ((b!=628 & b!=632 & b!=634 & b!=630 & b!=602 & b!=604 & b!=438 & b!=437 & b!=614 & b!=608 & b!=612 & b!=620 & b!=616 & b!=618)) {
    df[age_start>=15, is_outlier:=1]}
  
  df = rbind(hold[!(source_type  == "Facility - other/unknown" & extractor %like% "castl"),], df)
}


####### REGISTRY/LIT DATA SECTION ########

hold = copy(df)
df = df[source_type  == "Registry - congenital" | source_type == "Surveillance - other/unknown" | source_type == "Survey - cross-sectional",]
#South africa and india data: 
df<-df[source_type == "Registry - congenital" & (location_id==196 | location_id==163), is_outlier:=1]  
df = rbind(hold[!(source_type  == "Registry - congenital" | source_type == "Surveillance - other/unknown" | source_type == "Survey - cross-sectional"),], df)

print("made it to writing excel")

#############################################
##########NON PREVALENCE DATA FIXES #########
#############################################

#Last year you marked mtwith data that was nto from china registries as outliers
#For now, nick wants to outlier any mtwith data (as of march 26, 2018)
#But, we added our custom CSMR to bundle 3029 (total chromo) so keeping that 
df<-df[measure!="prevalence" & bundle_id!=3029, is_outlier:=1]
print(i)
print(b)
print(table(df$is_outlier))

df<-df[nid==284091, nid:=284093]

# A catch in case the age stuff is still misbehaving: 
df<-df[age_start>=50, is_outlier:=1]

df<-df[, note_SR:=NULL]
#df<-df[group=="" | specificity=="", group_review:=""]
#df<-df[group=="" | group_review=="", specificity:=""]

df<-df[(is.na(group) | is.na(specificity)), group_review:=NA]

df<-df[source_type== "Facility - other/unknown" & extractor %like% "cast", cv_marketscan:=1]

openxlsx::write.xlsx(df, paste0(j, "FILEPATH/bundle_", b, "_median_lowers.xlsx"), sheetName="extraction")
print("excel made")

print(paste(n, b, mad_n1, mad3))
print(table(df$is_outlier))
nrow_unoutliered <- nrow(df[is_outlier==0])
nrow_outliered <- nrow(df[is_outlier==1])

loop_mad_table <- data.table(
  `Description` = n,
  `Bundle` = b,
  `MAD lower` = mad_n1,
  `MAD upper` = mad3,
  `Non-outliered data points in model` = nrow_unoutliered,
  `Outliered data points` = nrow_outliered
)

mad_table <- rbind(mad_table, loop_mad_table)
}

write.csv(mad_table, file = paste0(j, "FILEPATH/MAD_table.csv"))

