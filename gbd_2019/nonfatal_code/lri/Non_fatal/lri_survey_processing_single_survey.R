#############################################################
## Take Diarrhea UbCov Data, account for seasonality ##
## This file processes every survey individually, no need
## to import everything! ##
## REQUIRES: Monthly survey scalars calculated previously
############### Steps ##################
# 1). Get summary prev by month
# 2). Develop simple region seasonal model
# 3). Determine scalar for month and geography
# 4). Apply scalar to raw data
# 5). Tabulate month-adjusted prevalence, save collapsed data

#### Import the things you'll need ####
library(survey)
library(openxlsx)
library(ggplot2)
library(boot)
library(plyr)
library(data.table)
library(lme4)

locs <- read.csv("filepath")[,c("location_name","location_id","ihme_loc_id","region_name","super_region_name","exists_subnational","parent_id")]

months <- read.csv("filepath") #seasonality scalars
# Use GAM scalar #
  months$scalar <- months$gamscalar
  scalar <- months[,c("scalar","month","region_name")]
  setnames(scalar, "month", "survey_month")

subnat_map <- read.csv("filepath")
setnames(subnat_map, c("parent_id", "location"), c("iso3","admin_1"))

## So we might only want to collapse New surveys ##
only_new <- TRUE

if(only_new == T){
  lri_data <- read.xlsx("filepath")
  unique_nids <- unique(lri_data$nid)
  rm(lri_data)
}

###############################################################
## Commented out, can jump straight to post-matched data ##
## Append all files that were extracted ##
filenames <- list.files() # filepath
df_output <- data.frame()
i <- 1
for(f in filenames){
  print(paste0("On survey ", i, " of ", length(filenames)))
  lri <- data.frame(fread(f))

  skip_survey <- 0
  if(only_new == T){
    if(unique(lri$nid) %in% unique_nids){
      skip_survey <- 1
    }
  }
  if(!("age_year" %in% colnames(lri))){
    skip_survey <- 1
  }
  if(!("diff_breathing" %in% colnames(lri))){
    skip_survey <- 1
  }

  #if("age_year" %in% colnames(df)){
  if(skip_survey == 0){
    # Symptom columns don't always exist
    if(!("had_fever" %in% colnames(lri))){
      lri$had_fever <- 9
    }
    if(!("chest_symptoms" %in% colnames(lri))){
      lri$chest_symptoms <- 9
    }
    #Change NAs to 9s in symptom columns
      lri$had_fever[is.na(lri$had_fever)] <- 9
      lri$had_cough[is.na(lri$had_cough)] <- 9
      lri$diff_breathing[is.na(lri$diff_breathing)] <- 9
      lri$chest_symptoms[is.na(lri$chest_symptoms)] <- 9

    ## Get location name ##
    lri <- join(lri, locs, by="ihme_loc_id")
    lri$iso3 <- substr(lri$ihme_loc_id,1,3)

    ## Join with seasonality scalar
    lri$survey_month <- lri$int_month
    # If survey month doesn't exist, set scalar to 1
    if("survey_month" %in% colnames(lri)){
      lri <- join(lri, scalar, by=c("region_name","survey_month"))
    } else {
      lri$scalar <- 1
    }

    ## Recall period missing
    if(!("recall_period_weeks" %in% colnames(lri))){
      lri$recall_period_weeks <- 2
    }

    ################################################################

    if("psu" %in% colnames(lri)){
      lri$psu <- as.numeric(lri$psu)
    } else {
      lri$psu <- 1
    }
    lri <- subset(lri, !is.na(psu))

    if("pweight" %in% colnames(lri)){
      lri$pweight <- lri$pweight
    } else {
      lri$pweight <- 1
    }

    lri$pweight <- ifelse(max(lri$pweight) == 0, 1, lri$pweight)

    lri$pweight[is.na(lri$pweight)] <- 1

    if("admin1" %in% colnames(lri)){
      lri <- join(lri, subnat_map, by=c("admin_1","iso3"))
      lri$subnat[is.na(lri$subnat)] <- 0
    } else {
      lri$subnat <- 0
    }

    ## Identify known subnationals ##
    lri$subname <- ifelse(lri$subnat==1, as.character(lri$ihme_loc_id_mapped), lri$iso3)

    lri$nid.new <- ifelse(lri$subnat==1, paste0(lri$nid,"_",lri$subname), lri$nid)

    ## Round ages
    lri$age_year <- floor(lri$age_year)
    ## Set missing sex to both
    if("sex_id" %in% colnames(lri)){
      lri$sex_id[is.na(lri$sex_id)] <- 3
    } else {
      lri$sex_id <- 3
    }
    # Drop if more detailed sex exists
    if(min(lri$sex_id) != 3){
      lri <- subset(lri, sex_id != 3)
    }

    #Set indicator = 1 if survey has chest/fever, set 0 if not
    exist_chest <- ifelse(min(lri$chest_symptoms, na.rm=T) == 0, 1, 0)
    exist_fever <- ifelse(min(lri$had_fever, na.rm=T) == 0, 1, 0)

    #Assign case definitions
    #We want prevalence w/o fever for crosswalk
    lri$good_no_fever <- 0
    lri$poor_no_fever <- 0
    if(exist_fever==1 & exist_chest==1) {
      lri$overall_lri = ifelse(lri$had_fever==1 & lri$chest_symptoms==1, 1, 0)
      lri$good_no_fever = ifelse(lri$chest_symptoms==1,1,0)
    } else if (exist_fever==1 & exist_chest==0) {
      lri$overall_lri = ifelse(lri$had_fever==1 & lri$diff_breathing==1, 1, 0)
      lri$poor_no_fever = ifelse(lri$diff_breathing==1,1,0)
    } else if (exist_fever==0 & exist_chest==1) {
      lri$overall_lri = ifelse(lri$chest_symptoms==1, 1, 0)
      lri$good_no_fever = 0
    } else if (exist_fever==0 & exist_chest==0) {
      lri$overall_lri = ifelse(lri$diff_breathing==1, 1, 0)
      lri$poor_no_fever = 0
    }

    # Add in URI since the surveys should be the same
      lri$has_uri <- ifelse(lri$had_cough==1 & lri$diff_breathing!=1, 1, 0)

    ############################################################################################
    ## Loop for each GBD location in survey ##
    ############################################################################################
    nid.new <- unique(lri$nid.new)
    df.final <- data.frame()
    for(n in nid.new){
      print(paste0("On survey number ",i," of ", length(filenames)))
      temp <- subset(lri, nid.new==n)
      # Drop if had_diarrhea is missing #
      temp <- subset(temp, !is.na(overall_lri))

      # It is possible some surveys will be empty so skip those #
      if(nrow(temp)>1){

        # If all pweights are 1 (i.e. missing), set psu to 1:nrow #
        row.count <- 1:nrow(temp)
        if(min(temp$pweight)==1){
          temp$psu <- row.count
        }

        # If all psu are missing, set psu to 1:nrow and pweight to 1 #
        if(is.na(min(temp$psu))){
          temp$psu <- row.count
          temp$pweight <- 1
        }

        # If all psu are the same, set psu to 1:nrow and pweight to 1 #
        if(length(unique(temp$psu))==1){
          temp$psu <- row.count
          temp$pweight <- 1
        }

        temp$recall_period <- ifelse(is.na(temp$recall_period_weeks),2,temp$recall_period_weeks)
        recall_period <- max(temp$recall_period, na.rm=T)

        loop.dummy <- max(temp$scalar)

        #Apply seasonality scalars
        scalar.dummy <- max(temp$scalar)
        if(is.na(scalar.dummy)) {
          temp$scalar_lri <- temp$overall_lri
        } else {
          temp$scalar_lri <- temp$overall_lri*temp$scalar
          temp$good_no_fever <- temp$good_no_fever*temp$scalar
          temp$poor_no_fever <- temp$poor_no_fever*temp$scalar
        }

        #Apply survey design & collapse
        if(length(unique(temp$psu))>1){
          dclus <- svydesign(id=~psu, weights=~pweight, data=temp)
          prev <- svyby(~scalar_lri, ~sex_id + age_year, dclus, svymean, na.rm=T)
          prev$base_lri <- svyby(~overall_lri, ~sex_id + age_year, dclus, svymean, na.rm=T)$overall_lri
          prev$good_no_fever <- svyby(~good_no_fever, ~sex_id + age_year, dclus, svymean, na.rm=T)$good_no_fever
          prev$poor_no_fever <- svyby(~poor_no_fever, ~sex_id + age_year, dclus, svymean, na.rm=T)$poor_no_fever
          prev$has_uri <- svyby(~has_uri, ~sex_id + age_year, dclus, svymean, na.rm=T)$has_uri
          prev$sample_size <- svyby(~scalar_lri, ~sex_id + age_year, dclus, unwtd.count, na.rm=T)$count
        } else {
          prev <- aggregate(cbind(scalar_lri, good_no_fever, poor_no_fever, has_uri) ~ sex_id + age_year, FUN=mean, na.rm=T, data=temp)
          prev$base_lri <- aggregate(overall_lri ~ sex_id + age_year, FUN=mean, na.rm=T, data=temp)$overall_lri
          prev$se <- aggregate(scalar_lri ~ sex_id + age_year, FUN=sd, na.rm=T, data=temp)$scalar_lri
          prev$sample_size <- aggregate(obs ~ sex_id + age_year, FUN=sum, na.rm=T, data=temp)$obs
        }
        prev$cases <- prev$scalar_lri * prev$sample_size
        prev$ihme_loc_id <- unique(temp$subname)
        prev$location <- unique(temp$location_name)
        if("start_year" %in% colnames(temp)){
          prev$start_year <- unique(temp$start_year)
        } else {
          prev$start_year <- unique(temp$year_start)
        }
        if("end_year" %in% colnames(temp)){
          prev$end_year <- unique(temp$end_year)
        } else {
          prev$end_year <- unique(temp$year_end)
        }
        prev$cv_diag_valid_good <- exist_chest
        prev$cv_had_fever <- exist_fever
        prev$nid <- unique(temp$nid)
        prev$sex <- ifelse(prev$sex_id==2,"Female",ifelse(prev$sex_id==1,"Male","Both"))
        prev$age_start <- prev$age_year
        prev$age_end <- prev$age_year + 1
        prev$recall_period <- recall_period
        prev$survey <- unique(temp$file_path)
        prev$notes <- paste0("LRI prevalence adjusted for seasonality. The original value was ", round(prev$base_lri,4))

      df_output <- rbind.fill(df_output, prev)

      }
    }
  }
  i <- i + 1
}
if(only_new == T){
  write.xlsx(df_output, "filepath")
} else {
  write.xlsx(df_output, "filepath")
}


