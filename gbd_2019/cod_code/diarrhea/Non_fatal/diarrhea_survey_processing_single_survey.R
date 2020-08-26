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
library(ggplot2)
library(boot)
library(plyr)
library(data.table)
library(lme4)
library(openxlsx)

locs <- read.csv("filepath")[,c("location_name","location_id","ihme_loc_id","region_name","super_region_name","exists_subnational","parent_id")]

curves <- read.csv("filepath")
  scalar <- curves[,c("scalar","month","region_name")]
  scalar$survey_month <- scalar$month

subnat_map <- read.csv("filepath")
  setnames(subnat_map, c("parent_id", "location"), c("iso3","admin_1"))

keep.names <- c("ihme_loc_id","year_start","year_end","psu","nid","survey_name","strata","pweight","sex_id","age_year","age_month","int_month", 
                "had_diarrhea","had_diarrhea_recall_period_weeks","admin_1","admin_2","admin_3")

## So we might only want to collapse New surveys ##
only_new <- TRUE

if(only_new == T){
  diarrhea_data <- read.xlsx("filepath")
  unique_nids <- unique(diarrhea_data$nid)
  rm(diarrhea_data)
}

###############################################################
## Commented out, can jump straight to post-matched data ##
## Append all files that were extracted ##
filenames <- list.files(path="filepath", full.names=T)
skipped_surveys <- c()
df_output <- data.frame()
i <- 1
for(f in filenames[1:802]){
  print(paste0("On survey ", i, " of ", length(filenames)))
  df <- data.frame(fread(f))

# Identify surveys to skip #
  skip_survey <- 0
  if(!("had_diarrhea" %in% colnames(df))){
    skip_survey <- 1
  }
  if(unique(df$survey_name) == "CVD_GEMS"){
    skip_survey <- 1
  }
  if(!("age_year" %in% colnames(df))){
    skip_survey <- 1
  }
  if(only_new == T){
    if(unique(df$nid) %in% unique_nids){
      skip_survey <- 1
    }
  }
  survey_number <- i
  #if("age_year" %in% colnames(df)){
  if(skip_survey == 0){

    diarrhea <- df[,which(names(df) %in% keep.names)]
    #   df <- subset(df, age_year <= 4)

    ## Make sure 'had_diarrhea' exists ##
      diarrhea$had_diarrhea[is.na(diarrhea$had_diarrhea)] <- 9
      diarrhea$tabulate <- ave(diarrhea$had_diarrhea, diarrhea$nid, FUN= function(x) min(x))
      diarrhea <- subset(diarrhea, tabulate!=9)

    ## Get location name ##
      diarrhea <- join(diarrhea, locs, by="ihme_loc_id")
      diarrhea$iso3 <- substr(diarrhea$ihme_loc_id,1,3)

    ## Join with seasonality scalar
      diarrhea$survey_month <- diarrhea$int_month
      # If survey month doesn't exist, set scalar to 1
      if("survey_month" %in% colnames(diarrhea)){
        diarrhea <- join(diarrhea, scalar, by=c("region_name","survey_month"))
      } else {
        diarrhea$scalar <- 1
      }

    ## Recall period missing
      if(!("had_diarrhea_recall_period_weeks" %in% colnames(diarrhea))){
        diarrhea$had_diarrhea_recall_period_weeks <- 2
      }

  ################################################################

    if("psu" %in% colnames(diarrhea)){
      diarrhea$psu <- as.numeric(diarrhea$psu)
    } else {
      diarrhea$psu <- 1
    }
      diarrhea <- subset(diarrhea, !is.na(psu))

    if("pweight" %in% colnames(diarrhea)){
      diarrhea$pweight <- diarrhea$pweight
    } else {
      diarrhea$pweight <- 1
    }

    diarrhea$pweight <- ifelse(max(diarrhea$pweight) == 0, 1, diarrhea$pweight)

    diarrhea$pweight[is.na(diarrhea$pweight)] <- 1

    if("admin1" %in% colnames(diarrhea)){
      diarrhea <- join(diarrhea, subnat_map, by=c("admin_1","iso3"))
      diarrhea$subnat[is.na(diarrhea$subnat)] <- 0
    } else {
      diarrhea$subnat <- 0
    }

  ## Identify known subnationals ##
    diarrhea$subname <- ifelse(diarrhea$subnat==1, as.character(diarrhea$ihme_loc_id_mapped), diarrhea$iso3)

    diarrhea$nid.new <- ifelse(diarrhea$subnat==1, paste0(diarrhea$nid,"_",diarrhea$subname), diarrhea$nid)

  ## Round ages
    diarrhea$age_year <- floor(diarrhea$age_year)
  ## Set missing sex to both
    if("sex_id" %in% colnames(diarrhea)){
      diarrhea$sex_id[is.na(diarrhea$sex_id)] <- 3
    } else {
      diarrhea$sex_id <- 3
    }

  ## prep diarrhea column (missing is set to 9) ##
    diarrhea$had_diarrhea[diarrhea$had_diarrhea==9] <- NA

  ## Loop within surveys to tabulate at most detailed GBD locations ##
  nid.new <- unique(diarrhea$nid.new)
  df.final <- data.frame()
  for(n in nid.new){
    print(paste0("On survey number ",i," of ", length(filenames)))
    temp <- subset(diarrhea, nid.new==n)
    # Drop if had_diarrhea is missing #
    temp <- subset(temp, !is.na(had_diarrhea))

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

      # If all pweights are 0, set to 1
      if(max(temp$pweight)==0){
        temp$pweight <- 1
      }

      temp$recall_period <- ifelse(is.na(temp$had_diarrhea_recall_period_weeks),2,temp$had_diarrhea_recall_period_weeks)
      recall_period <- max(temp$recall_period, na.rm=T)

      loop.dummy <- max(temp$scalar)

      # Does survey month exist? #
      if(is.na(loop.dummy)){
        temp$scalar_diarrhea <- temp$had_diarrhea
      } else {
        temp$scalar_diarrhea <- temp$had_diarrhea * temp$scalar
      }

      # Set svydesign #
      dclus <- svydesign(id=~psu, weights=~pweight, data=temp)

      # Get survey prevalence #
      prev <- svyby(~scalar_diarrhea, ~sex_id + age_year, dclus, svymean, na.rm=T)
      prev$sample_size <- svyby(~scalar_diarrhea, ~sex_id + age_year, dclus, unwtd.count, na.rm=T)$count
      prev$base_mean <- svyby(~had_diarrhea, ~sex_id + age_year, dclus, svymean, na.rm=T)$had_diarrhea
      prev$ihme_loc_id <- unique(temp$ihme_loc_id)
      prev$location <- unique(temp$subname)
      prev$year_start <- min(temp$year_start)
      prev$year_end <- max(temp$year_end)
      prev$nid <- unique(temp$nid)
      prev$sex <- ifelse(prev$sex_id==2,"Female","Male")
      prev$age_start <- prev$age_year
      prev$age_end <- prev$age_year + 1
      prev$recall_period <- recall_period
      prev$survey <- unique(temp$survey_name)
      prev$note_modeler <- paste0("Diarrhea prevalence adjusted for seasonality. The original value was ", round(prev$base_mean,4),".")
      df.final <- rbind.data.frame(df.final, prev)
      }

    df_output <- rbind.fill(df_output, df.final)

    }
  } else {
    print(paste0("Skipped survey ",i))
    skipped_surveys <- c(skipped_surveys, unique(df$nid))
  }
  i <- i + 1
}

if(only_new == T){
  write.xlsx(df_output, "filepath")
} else {
  write.xlsx(df_output, "filepath")
}

