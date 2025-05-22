################################################################################
################################################################################
library(plyr)
library(ggplot2)
source("FILEPATH")
locs <- read.csv("FILEPATH")


## Step 4 new surveys
data <- read.csv("FILEPATH")

  data$non_xw_mean <- data$mean
  data$mean <- data$test_mean

  data$standard_error <- data$standard_error * 1.5

## Fix some things in survey data ##
  df <- join(data, locs[,c("location_id","ihme_loc_id")], by="ihme_loc_id")
  # Kosovo not a location for modeling
  df <- subset(df, !is.na(location_id))

  df <- subset(df, ihme_loc_id!="")
  df$location_name <- df$location_ascii_name
  df$year_start <- df$start_year
  df$year_end <- df$end_year
  df$sampling_type <- "Cluster"
  df$gbd_round <- 2019

  survey_ids <- data.frame(nid=unique(df$nid), drop_id=1)
  df <- subset(df, nid!= 20109)


#####################################################
## Survey data may already exist in the epi database
## so the idea was to pull the current data, find
## where there are matching NIDs, and clear the
## rows currently in the Epi DB. Then, during the
## upload, new data are added while old data are
## deleted
#####################################################

## Pull epi data ##
  epi <- read.csv("FILEPATH")
  
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

  out.df <- out.df[2:length(out.df$nid),]
  out.df <- out.df[, which(names(out.df) %in% names(template))]
  out.df <- out.df[, -(which(names(out.df) %in% c("cv_diag_x.ray")))]

write.xlsx(out.df, "FILEPATH", sheetName="extraction")

invisible(sapply(list.files("/FILEPATH", full.names = T), source))

new_data <- fread("FILEPATH")

# fix columns
new_data$year_start <- new_data$start_year
new_data$year_end   <- new_data$end_year
new_data$underlying_nid <- ""
new_data$design_effect <- ""
new_data$underlying_nid <- ""
new_data$design_effect <- ""
new_data$input_type <- "extracted"
new_data$uncertainty_type <- ""
new_data$uncertainty_type_value <- ""
new_data$sampling_type <- ""
new_data$recall_type_value <- ""
new_data$effective_sample_size <- ""
new_data$seq <- ""
new_data$upper <- ""
new_data$lower <- ""

setnames(new_data, "cv_had_fever", "had_fever_survey")
# get current bundle
lri_bun       <- get_bundle_data(bundle_id = 19)
cols          <- colnames(lri_bun)
new_data_cols <- intersect(colnames(new_data), cols)

new_data <- new_data[,new_data_cols, with=FALSE]

# get location_id and location_name
locs     <- get_location_metadata(location_set_id = 35, release_id = 16)
new_data <- merge(new_data, locs[,.(location_id, location_name, ihme_loc_id)], by="ihme_loc_id")
new_data$representative_name <- ifelse(new_data$representative_name=="Subnationally representative only", "Representative for subnational location only", "Nationally and subnationally representative")



# output for upload
write.xlsx(new_data, "FILEPATH", sheetName="extraction")

# in case you need to remove nids in the bundle before uploading - if not, SKIP portion below
lri_remove <- lri_bun[nid %in% unique(new_data$nid)]
cols_bun <- colnames(lri_bun)
cols_bun <- cols_bun[cols_bun != "seq"]

lri_remove <- lri_remove[, c(cols_bun) := ""]

write.xlsx(lri_remove, "FILEPATH", sheetName="extraction")

lri_up <- upload_bundle_data(bundle_id = 19, 
                             filepath = "FILEPATH")
new_bun <- get_bundle_data(bundle_id = 19)
## upload ----

# "not in" function
`%notin%` <- Negate(`%in%`)
xwvals <- data.frame()


surv_2022 <- as.data.table(read.xlsx("FILEPATH"))

# Outlier Susenas NIDS
susenas_nids <- c(5376,5401,6687,6706,6724,6767,6790,6811,6842,6874,6904,6970,30235,43510,43526,85265,150884,151184)
surv_2022 <- surv_2022[nid %notin% susenas_nids]
table(surv_2022$nid[surv_2022$nid %in% susenas_nids])
surv_2022$representative_name <- ifelse(surv_2022$representative_name=="Subnationally representative only", "Representative for subnational location only", "Nationally and subnationally representative")

surv_2022$mean <- as.numeric(surv_2022$mean)
surv_2022$upper <- as.numeric(surv_2022$upper)
surv_2022$lower <- as.numeric(surv_2022$lower)
surv_2022$standard_error <- as.numeric(surv_2022$standard_error)
surv_2022$seq <- ""

# SAVE
write.xlsx(surv_2022, paste0(temp_save,"FILEPATH"), sheetName = "extraction")
