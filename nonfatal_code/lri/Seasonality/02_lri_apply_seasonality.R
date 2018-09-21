#######################################################
## Adjust LRI data for seasonality 
#######################################################
library(survey)
library(plyr)
library(data.table)
library(ggplot2)

load("FILEPATH/2017_03_06.Rda")
lri <- combined_data

## Surveys must have difficulty breathing! ##
# Change the NA values to 9
lri$chest_symptoms[is.na(lri$chest_symptoms)] <- 9
lri$had_fever[is.na(lri$had_fever)] <- 9
lri$diff_breathing[is.na(lri$diff_breathing)] <- 9
lri$had_cough[is.na(lri$had_cough)] <- 9

lri$tabulate <- ave(lri$diff_breathing, lri$nid, FUN= function(x) min(x))
lri <- subset(lri, tabulate!=9)

locs <- read.csv("FILEPATH/ihme_loc_metadata_2016.csv")
locs <- locs[,c("location_name","location_id","super_region_name","super_region_id","region_name","region_id","parent_id", "ihme_loc_id")]
lri <- join(lri, locs, by="ihme_loc_id")
pred.df <- read.csv("FILEPATH/seasonal_lri_prediction.csv")
months <- read.csv("FILEPATH/month_region_scalar.csv")

########################################################
# We have a month/country scalar, now apply to raw data 
########################################################
scalar <- months[,c("scalar","month","region_name")]
scalar$survey_month <- scalar$month

lri$survey_month <- lri$int_month
lri <- join(lri, scalar, by=c("region_name","survey_month"))

########################################################
## Add some missing values ##

lri$child_sex <- lri$sex_id
lri$child_sex[is.na(lri$child_sex)] <- 3
lri$age_yr <- floor(lri$age_year)

## Don't forget subnationals in Kenya, S Africa, India, Indonesia 
##############################################################
# Create dummy nid for each subnat
lri$urban_name <- ifelse(lri$urban==1,"Urban","Rural")
lri$nid.new <- ifelse(lri$ihme_loc_id=="ZAF", paste0(lri$location_name,"_",lri$nid), 
                      ifelse(lri$ihme_loc_id=="IND", paste0(lri$location_name,"_",lri$urban_name,"_",lri$nid),lri$nid))
lri$subname <- ifelse(lri$ihme_loc_id=="ZAF", lri$location_name, ifelse(lri$ihme_loc_id=="IND", paste0(lri$location_name,", ",lri$urban_name), "none"))

nid.new <- unique(lri$nid.new)

# If not survey_weight exists, set to 1?
lri <- subset(lri, !is.na(psu))

## Set up long loop for tabulations ##
df.final <- data.frame()

for(n in nid.new){
  temp <- subset(lri, nid.new==n & age_yr<=4) # some extractions include mother's ages, too. Only keep under 5s 
  if(length(temp$ihme_loc_id)>0){
    cv_diag_valid_good <- ifelse(min(temp$chest_symptoms, na.rm=T)==0,1,0)
    exist_fever <- ifelse(min(temp$had_fever, na.rm=T)==0,1,0)
    temp$recall_period <- ifelse(is.na(temp$recall_period_weeks),2,temp$recall_period_weeks)
    recall_period <- max(temp$recall_period, na.rm=T)
    ## Some logic for definition used ##
    ## Want prevalence without fever for crosswalk ##
    temp$good_no_fever <- 0
    temp$poor_no_fever <- 0
    if(exist_fever==1 & cv_diag_valid_good==1) {
      temp$overall_lri = ifelse(temp$had_fever==1 & temp$chest_symptoms==1, 1, 0)
      temp$good_no_fever = ifelse(temp$chest_symptoms==1,1,0)
    } else if (exist_fever==1 & cv_diag_valid_good==0) {
      temp$overall_lri = ifelse(temp$had_fever==1 & temp$diff_breathing==1, 1, 0)
      temp$poor_no_fever = ifelse(temp$diff_breathing==1,1,0)
    } else if (exist_fever==0 & cv_diag_valid_good==1) {
      temp$overall_lri = ifelse(temp$chest_symptoms==1, 1, 0)
      temp$good_no_fever = 0
    } else {
      temp$overall_lri = ifelse(temp$diff_breathing==1, 1, 0)
      temp$poor_no_fever = 0
    }
    
    loop.dummy <- max(temp$scalar)
    ## Want prevalence without fever for crosswalk ##
    if(is.na(loop.dummy)){
      temp$scalar_lri <- temp$overall_lri
    } else {
      temp$scalar_lri <- temp$overall_lri * temp$scalar
      temp$good_no_fever <- temp$good_no_fever * temp$scalar
      temp$poor_no_fever <- temp$poor_no_fever * temp$scalar
    }
    
    ## Set svydesign
    dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
    prev <- svyby(~scalar_lri, ~child_sex + age_yr, dclus, svymean, na.rm=T)
    prev$base_lri <- svyby(~overall_lri, ~child_sex + age_yr, dclus, svymean, na.rm=T)$overall_lri
    prev$good_no_fever <- svyby(~good_no_fever, ~child_sex + age_yr, dclus, svymean, na.rm=T)$good_no_fever
    prev$poor_no_fever <- svyby(~poor_no_fever, ~child_sex + age_yr, dclus, svymean, na.rm=T)$poor_no_fever
    prev$sample_size <- svyby(~scalar_lri, ~child_sex + age_yr, dclus, unwtd.count, na.rm=T)$count
    prev$cases <- prev$scalar_lri * prev$sample_size
    prev$ihme_loc_id <- unique(temp$ihme_loc_id)
    prev$location <- unique(temp$subname)
    prev$year_start <- unique(temp$year_start)
    prev$year_end <- unique(temp$end_year)
    prev$cv_diag_valid_good <- cv_diag_valid_good
    prev$cv_had_fever <- exist_fever
    prev$nid <- unique(temp$nid)
    prev$sex <- ifelse(prev$child_sex==2,"Female","Male")
    prev$age_start <- prev$age_yr
    prev$age_end <- prev$age_yr + 1
    prev$recall_period <- recall_period
    prev$survey <- unique(temp$survey_name)
    prev$note_modeler <- paste0("LRI prevalence adjusted for seasonality. The original value was ", round(prev$base_lri,4)," See J:/WORK/04_epi/01_database/02_data/lri/GBD_2016/lri_seasonality_adj.R for detail.")
    df.final <- rbind.data.frame(df.final, prev)
  }
}

## Export for review ##
write.csv(df.final, "FILEPATH/tabulated_survey_season_v3.csv")
