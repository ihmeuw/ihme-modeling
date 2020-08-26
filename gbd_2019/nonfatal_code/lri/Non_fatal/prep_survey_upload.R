################################################################################
## This file takes the output from 04_crosswalk_gold_standard_surveys.R and
## Preps LRI survey data for upload ##
################################################################################
## Import useful things ##
library(plyr)
library(ggplot2)
source("filepath/get_bundle_data.R")
locs <- read.csv("filepath")

data <- read.csv("filepath")

## Rename ##
  data$non_xw_mean <- data$mean
  data$mean <- data$test_mean

## add uncertainty from the crosswalk so the decision
  data$standard_error <- data$standard_error * 1.5

## Fix some things in survey data ##
  df <- join(data, locs[,c("location_id","ihme_loc_id")], by="ihme_loc_id")
  # Kosovo not a location for modeling
  df <- subset(df, !is.na(location_id))
  #df <- data

  df <- subset(df, ihme_loc_id!="")
  df$location_name <- df$location_ascii_name
  df$year_start <- df$start_year
  df$year_end <- df$end_year
  df$sampling_type <- "Cluster"
  df$gbd_round <- 2019

  survey_ids <- data.frame(nid=unique(df$nid), drop_id=1)
  ## This survey poses problems in Kenya ##
  df <- subset(df, nid!= 20109)

#####################################################
## Survey data may already exist in the epi database
## so the idea was to pull the current data, find
## where there are matching NIDs, and clear the
## rows currently in the Epi DB. Then, during the
## upload, new data are added while old data are
## deleted in one fell swoop!!
#####################################################

## Pull epi data ##
  epi <- read.csv("filepath")

## Find where there is NID overlap in survey data ##
  # epi <- join(epi, survey_ids, by="nid")
  # drop <- subset(epi, drop_id==1)
  # drop <- data.frame(seq=drop[,c("seq")])
  #

  # head(out.df)

## Don't try to upload if NIDs already exist ##
  epi_nids <- unique(epi$nid)

  out.df <- subset(df, !(nid %in% epi_nids))
  template <- epi[1,]
  out.df <- rbind.fill(template, out.df)

  out.df[is.na(out.df)] <- ""
  out.df$representative_name <- ifelse(out.df$representative_name=="Subnationally representative only", "Representative for subnational location only", "Nationally and subnationally representative")
  out.df$nid[out.df$nid==5379] <- 5401
  out.df$nid[out.df$nid==6825] <- 6842
  out.df$nid[out.df$nid==6860] <- 6874
  out.df$nid[out.df$nid==6956] <- 6970
  out.df <- subset(out.df, substr(ihme_loc_id,1,3)!="USA")
  out.df$nid[substr(out.df$ihme_loc_id,1,3)=="IND" & out.df$year_start==1998] <- 19950
  out.df$nid[out.df$nid==43518] <- 43526
  out.df$nid[out.df$nid==43549] <- 43552
  out.df$location_id[out.df$nid %in% c(43510, 43526, 43552)] <- 11
  out.df$location_name[out.df$nid %in% c(43510, 43526, 43552)] <- "Indonesia"
  out.df$ihme_loc_id[out.df$nid %in% c(43510, 43526, 43552)] <- "IDN"

  ## These locations are technically subnationals in Indonesia but aren't mapped to IHME locations ##
  out.df <- subset(out.df, nid!=43552)
  out.df <- subset(out.df, nid!=43549)

## Some things for uploading ##
  out.df <- out.df[2:length(out.df$nid),]
  out.df <- out.df[, which(names(out.df) %in% names(template))]
  out.df <- out.df[, -(which(names(out.df) %in% c("cv_diag_x.ray")))]

write.xlsx(out.df, "filepath", sheetName="extraction")
