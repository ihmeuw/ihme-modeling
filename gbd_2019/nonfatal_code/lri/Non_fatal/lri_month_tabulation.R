#####################################################
## The purpose of this code is to calculate a seasonality
# adjustment for the LRI survey data. The code can be run
# independently but is currently sourced in the "lri_collapse_survey_data.R"
# code that is used by the LBD team to collapse survey data
# that are extracted through UbCov.
######################################################

#Read in dataset, rename columns
library(mgcv)
library(plyr)
library(survey)
library(ggplot2)
library(boot)
library(plyr)
library(data.table)
library(lme4)

# ### PREP ###
# ####################################################
# ## This code must have a data frame called "lri" ##
# ## The following lines can be commented in if not sourced
# ## from "lri_collapse_survey_data.R"
# #Set filepaths
# # root <- ifelse(Sys.info()[1]=="Windows", "ADDRESS", "ADDRESS")
# # currentdata <- "FILEPATH" #update filepath to the most current geo-merge extraction
# # 
# # load(paste0(root, currentdata))
# # lri <- as.data.table(all)
# # rm(all)
# # setnames(lri, old = c('survey_name', 'iso3', 'year_start', 'year_end'), new = c('survey_series', 'ihme_loc_id', 'start_year', 'end_year'))
# # 
# # #Merge locs & seasonality scalars onto dataset
# # lri <- join(lri, locs, by = "ihme_loc_id")
# 
# ## Add some missing values ##
# lri$child_sex <- lri$sex_id
# lri$child_sex[is.na(lri$child_sex)] <- 3
# lri$age_yr <- floor(lri$age_year)
# ## Drop ages greater than 5
# lri <- subset(lri, age_yr<=5)
# 
# lri$nid <- lri$nid_str
# 
# ## For exploration, drop rows with weight = NA
# lri$pweight[is.na(lri$pweight)] <- 1
# lri$pweight[lri$pweight==0] <- 1
# 
# ###############################################
# # ## Part 1 ##
# ## Surveys must have difficulty breathing! ##
# # Change the NA values to 9
# lri$chest_symptoms[is.na(lri$chest_symptoms)] <- 9
# lri$had_fever[is.na(lri$had_fever)] <- 9
# lri$diff_breathing[is.na(lri$diff_breathing)] <- 9
# lri$had_cough[is.na(lri$had_cough)] <- 9
# 
# lri$tabulate <- ave(lri$diff_breathing, lri$nid, FUN= function(x) min(x))
# lri <- subset(lri, tabulate!=9)
# 
# ## For seasonality, do not adjust data for specificity but do include fever (malaria)? ##
# lri$any_lri <- ifelse(lri$chest_symptoms==1,1,ifelse(lri$diff_breathing==1 & lri$chest_symptoms!=0,1,0))
################################################
## Part 2 ##
## Get monthly prevalence by survey ##
lri$nid_new <- ifelse(is.na(lri$nid_str), paste0(lri$ihme_loc_id,"_",lri$year), lri$nid)
nid <- unique(lri$nid_new)
nid <- nid[!is.na(nid)]
nid <- nid[nid!=""]
df.month <- data.frame()
num <- 1
for(n in nid){
  print(paste0("Collapsing survey ", num, " of ", length(nid)))
  temp <- subset(lri, nid_new==n)
  #temp <- subset(temp, !is.na(psu))
  #table(temp$any_lri, temp$survey_month)
   if(length(unique(temp$psu))>1){
     if(!is.na(max(temp$psu))){
       if(max(temp$int_month, na.rm=T)!="-Inf"){
        cv_diag_valid_good <- ifelse(min(temp$chest_symptoms, na.rm=T)==0,1,0)
        ## Set svydesign
        dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
        month.prev <- svyby(~any_lri, ~int_month, dclus, svymean, na.rm=T)
        month.prev$ihme_loc_id <- unique(temp$ihme_loc_id)
        month.prev$year_id <- max(temp$end_year, na.rm=T)
        month.prev$cv_diag_valid_good <- cv_diag_valid_good
        month.prev$sample_size <- svyby(~any_lri, ~ int_month, dclus, unwtd.count, na.rm=T)$count
        month.prev$region_name <- unique(temp$region_name)
        month.prev$super_region_name <- unique(temp$super_region_name)
        month.prev$nid <- unique(temp$nid_new)
        colnames(month.prev)[1] <- "survey_month"
        df.month <- rbind.data.frame(df.month, month.prev)
       }
     }
   }
  num <- num + 1
}

write.csv(df.month, "filepath")
