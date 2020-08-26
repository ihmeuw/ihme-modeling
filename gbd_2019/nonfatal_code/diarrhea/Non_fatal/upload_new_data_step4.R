#####################################################################
## Prep and upload new data for GBD 2019 decomp step 4 for diarrhea
#####################################################################
library(plyr)
library(ggplot2)
library(zoo)
library(openxlsx)
source("filepath/get_bundle_data.R")

##-------------------------------------------------------------------
## Prepare survey data 

## Load data
  locs <- read.csv("filepath")
  subnat_map <- read.csv("filepath")
  df <- read.csv("filepath")

## Drop if age is greater than 100 ##
  df <- subset(df, age_end<99)

## drop NID already captured in Peru Continuous DHS
  df <- subset(df, nid != 20663)

## Fix some things in survey data ##
  df <- join(df, locs[,c("location_id","location_ascii_name","level","location_name", "ihme_loc_id")], by="ihme_loc_id")

  df$location_name <- df$location_ascii_name

  df$sampling_type <- "Cluster"
  df$gbd_round <- 2019

## Drop really small populations ##
  df <- subset(df, sample_size>=10)
## Drop if value is zero ##
  df <- subset(df, mean!=0)

## Want adults in five year bins ##
keep.names <- c("ihme_loc_id","location","year_start","year_end","nid","sex","age_start","age_end","location_id","location_ascii_name","location_name","survey")
  df.old <- df[df$age_end>5,c("cases","sample_size",keep.names)]
  df <- df[df$age_end<=5,]
  df.old$age_cut <- cut(df.old$age_end, seq(0,100,5), labels=F)
  age_match <- data.frame(age_cut=1:20, age_start=seq(0,95,5), age_end=seq(5,100,5))
  col.df <- aggregate(cbind(cases, sample_size) ~ ihme_loc_id + location + year_start + year_end + nid + sex + age_cut + location_id + location_ascii_name + location_name, data=df.old, FUN=sum)
  col.df$mean <- col.df$cases/col.df$sample_size
  col.df$note_modeler <- "Values were collapsed to five year age bins"
  col.df$measure <- "prevalence"
  col.df <- join(col.df, age_match, by="age_cut")
  col.df$age_end[col.df$age_end==100] <- 99
  col.df$source_type <- "Survey - cross-sectional"

keep.cols <- c("bundle_id","seq","nid","underlying_nid","input_type","modelable_entity_id","modelable_entity_name","field_citation_value","file_path","response_rate",
               "page_num","table_num","source_type","location_name","ihme_loc_id","smaller_site_unit","site_memo","sex","sex_issue","year_start","year_end",
               "year_issue","age_start","age_end","age_issue","age_demographer","measure","mean","lower","upper","standard_error","effective_sample_size","cases","sample_size","unit_type",
               "unit_value_as_published","measure_issue","measure_adjustment","uncertainty_type","uncertainty_type_value","representative_name","urbanicity_type","recall_type",
               "recall_type_value","sampling_type","case_name","case_definition","case_diagnostics","group","specificity","group_review","note_modeler","extractor","is_outlier",
               "note_sr","gbd_round","design_effect")
template <- read.csv("filepath")
template <- template[,keep.cols]

out.df <- rbind.fill(df, template, col.df)
out.df <- out.df[2:length(out.df$nid),]
out.df[,c("sex_issue","year_issue","age_issue","source_type","age_demographer","measure_adjustment","extractor","is_outlier")] <- na.locf(out.df[,
                                                                                                                                                 c("sex_issue","year_issue","age_issue","source_type","age_demographer","measure_adjustment","extractor","is_outlier")])
head(out.df)
tail(out.df)

out.df$representative_name <- ifelse(out.df$location==out.df$ihme_loc_id,"Nationally and subnationally representative","Representative for subnational location only")
out.df$representative_name[is.na(out.df$representative_name)] <- "Nationally and subnationally representative"

## Add information to map to GBD subnationals ##
  subnat_map <- join(subnat_map, locs[,c("location_id","ihme_loc_id")], by="ihme_loc_id")
  subnat_map <- subnat_map[,c("parent_id","location","location_name_mapped","ihme_loc_id_mapped","location_id")]
  colnames(subnat_map)[c(1,5)] <- c("ihme_loc_id","location_id_mapped")
  subnat_map$subnat <- 1

  out.df <- join(out.df, subnat_map, by=c("ihme_loc_id","location"))

  out.df$ihme_loc_id <- ifelse(!is.na(out.df$ihme_loc_id_mapped), as.character(out.df$ihme_loc_id_mapped), as.character(out.df$ihme_loc_id))

  out.df$ihme_loc_id <- ifelse(is.na(out.df$ihme_loc_id_mapped) & out.df$location!=out.df$ihme_loc_id, as.character(out.df$location), as.character(out.df$ihme_loc_id))

  out.df <- out.df[,colnames(out.df)!="location_id"]
  out.df <- out.df[,colnames(out.df)!="location_name"]

  out.df$ihme_loc_id <- ifelse(out.df$location_ascii_name=="Ukraine","UKR",out.df$ihme_loc_id)

  out.df <- join(out.df, locs[,c("ihme_loc_id","location_id","location_name")], by="ihme_loc_id")

  out.df <- subset(out.df, !is.na(location_id))

## Add some miscellaneous things ##
  out.df$urbanicity_type <- "Mixed/both"
  out.df$unit_type <- "Person"
  out.df$source_type <- "Survey - cross-sectional"
  out.df$recall_type <- "Point"
  out.df$unit_value_as_published <- 1
  out.df$nid[out.df$nid == 5360] <- 5376
  out.df$nid[out.df$nid == 6956] <- 6970
  
  out.df$nid <- ifelse(out.df$location_id==163 & out.df$year_start == 1998, 19950, out.df$nid)

  keep.cols <- c("bundle_id","seq","nid","underlying_nid","input_type","modelable_entity_id","modelable_entity_name","field_citation_value","file_path","response_rate",
                 "page_num","table_num","source_type","location_name","location_id","ihme_loc_id","smaller_site_unit","site_memo","sex","sex_issue","year_start","year_end",
                 "year_issue","age_start","age_end","age_issue","age_demographer","measure","mean","lower","upper","standard_error","effective_sample_size","cases","sample_size","unit_type",
                 "unit_value_as_published","measure_issue","measure_adjustment","uncertainty_type","uncertainty_type_value","representative_name","urbanicity_type","recall_type",
                 "recall_type_value","sampling_type","case_name","case_definition","case_diagnostics","group","specificity","group_review","note_modeler","extractor","is_outlier",
                 "note_sr","gbd_round","design_effect","survey")
  out.df <- out.df[,keep.cols]
  out.df[is.na(out.df)] <- ""
  
  write.csv(out.df, "filepath")
  
##-----------------------------------------------------------------------
## New literature data 
  source("/filepath/convert_inc_prev_function.R")
  lit_df <- read.csv("filepath")
  lit_prev <- subset(lit_df, measure == "prevalence")
  lit_inc <- subset(lit_df, measure == "incidence")
  
  lit_inc$mean <- (lit_inc$cases / lit_inc$sample_size) * 4.2 / 365
  lit_inc$cases <- lit_inc$mean * lit_inc$sample_size
  lit_inc$measure <- "prevalence"
  lit_inc$lower <- ""
  lit_inc$upper <- ""
  
  lit <- rbind(lit_prev, lit_inc)

##------------------------------------------------------------------------
## Upload as step 4 data to diarrhea bundle
  source() # filepaths for central data upload/download functions
  
  df <- rbind.fill(out.df, lit)
  df[is.na(df)] <- ""
  df$gbd_2019_new <- 1
  
# These are outliers
  df <- subset(df, !(nid %in% c(417061,417051, 417071)))

  df <- subset(df, !(nid %in% existing_nids))
  
  df <- df[, -which(names(df) %in% c("cv_inpatient_lit"))]
  df$representative_name <- ifelse(is.na(df$representative_name), "Nationally and subnationally representative", as.character(df$representative_name))
  
  write.xlsx(df, "filepath", sheetName="extraction")
  
  upload_bundle_data(bundle_id = 3, decomp_step="step4", filepath = "filepath")
  
  
  