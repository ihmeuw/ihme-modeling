
######################################################
##                                                  ##
## Purpose: Read in and clean tabs data extractions ##
##                                                  ##
######################################################

rm(list = ls())
Sys.umask(mode = "0002")

library(assertable)
library(data.table)
library(dplyr)
library(foreign)
library(purrr)
library(readr)
library(readxl)

library(mortdb)
library(mortcore)

user <- Sys.getenv("USER")

root <- "FILEPATH"
j_dir <- paste0(root, "FILEPATH")

gbd_year <- 2021
locsdf <- mortdb::get_locations(gbd_year = gbd_year, gbd_type = 'mortality')

# Read in extractions
tabsdf <- readxl::read_xlsx(paste0(j_dir, "FILEPATH"))
tabsdf <- as.data.table(tabsdf[-1, ])
tabsdf <- tabsdf[!is.na(nid)]
tabsdf <- tabsdf[!(!is.na(cases) & cases == "S/D")]

# Read in coded vr extractions
coded_vr_files <- list.files(paste0(j_dir, "FILEPATH"), pattern = ".csv", full.names = T, recursive = T)
coded_vr <- lapply(coded_vr_files, fread) %>% rbindlist(use.names = T, fill = T)

coded_vr <- coded_vr[!(ihme_loc_id %like% "ZAF" & is.na(sb_inclusion_preference))] # not defined can't be reassigned + most values equal to 500 grams

# Combine extractions
tabsdf <- rbind(tabsdf, coded_vr, fill = T)

# Make blank cells NA
tabsdf[tabsdf == "" | tabsdf == "NA" | tabsdf == "N.A." | tabsdf == "N/A"] <- NA

# Remove blank columns
tabsdf <- as.data.frame(tabsdf)
index <- purrr::map_lgl(tabsdf, ~ all(is.na(.)))
tabsdf <- tabsdf[, !index]
tabsdf <- as.data.table(tabsdf)

# Check that only one source is assigned to each nid
probs_source <- tabsdf %>% group_by(nid) %>% summarise(unique_sources = n_distinct(title))
probs_source <- as.data.table(probs_source)
probs_source <- probs_source[unique_sources > 1] # removing hfc and dyb
if (nrow(probs_source) > 0) stop("There are nids with more than one source listed")

# Check that only one nid is assigned to each source
probs_nid <- tabsdf %>% group_by(title) %>% summarise(unique_nids = n_distinct(nid))
probs_nid <- as.data.table(probs_nid)
probs_nid <- probs_nid[unique_nids > 1]
probs_nid <- merge(probs_nid, tabsdf[, c("nid", "title")], by = "title", all.x = T)
probs_nid <- unique(probs_nid)
if (nrow(probs_nid) > 0) stop("There are sources with more than one nid listed")

# Make sure all definitions are less than 500 characters
if (nrow(tabsdf[nchar(sb_definition) >= 500]) > 0) stop("some definitions are too long for the database")

# Add new variables
tabsdf[sex == "male" | sex == "Male", sex_id := 1]
tabsdf[sex == "female" | sex == "Female", sex_id := 2]
tabsdf[sex == "both" | sex == "Both", sex_id := 3]
tabsdf[sex == "unknown" | sex == "Unknown", sex_id := 4]

probs_sex_id <- tabsdf[is.na(sex_id)]
if (nrow(probs_sex_id) > 0) warning("There are rows missing a sex id...")
tabsdf[, sex := NULL]

tabsdf[, age_group_id := 22] # all ages

tabsdf[, mean := as.numeric(mean)]
tabsdf[, mean := round(mean, 4)]

tabsdf[, year_start := as.numeric(year_start)]
tabsdf[, year_end := as.numeric(year_end)]

tabsdf[year_start == year_end, year_id := year_start]
tabsdf[year_start + 1 == year_end, year_id := year_end]
tabsdf[is.na(year_id), year_id := round(((year_start + year_end)/2), 0)]
tabsdf[, year := year_id]

setnames(tabsdf, "sb_definition", "definition")
setnames(tabsdf, "sample_size", "total_births")

probs_data_type <- tabsdf[is.na(data_type)]
if (nrow(probs_data_type) > 0) warning("There are rows missing a data type...")

# Check consistency between cases and stillbirths columns
if (nrow(tabsdf[!is.na(cases) & !is.na(stillbirths) & cases != stillbirths, ]) > 0) warning("There are rows where cases and stillbirths are not equal, cutting those rows...")
cut_rows <- tabsdf[!is.na(cases) & !is.na(stillbirths) & cases != stillbirths, ]
if (nrow(cut_rows) > 0) readr::write_csv(cut_rows, paste0(root, "FILEPATH"))
tabsdf <- tabsdf[!(!is.na(cases) & !is.na(stillbirths) & cases != stillbirths), ]

# Fill out columns
tabsdf[is.na(stillbirths) & !is.na(cases), stillbirths := cases]

tabsdf[, stillbirths := as.numeric(stillbirths)]
tabsdf[, livebirths := as.numeric(livebirths)]
tabsdf[, total_births := as.numeric(total_births)]

tabsdf[livebirths == 0, livebirths := NA]
tabsdf[total_births == 0 | stillbirths == total_births, total_births := NA]

tabsdf[is.na(total_births) & !is.na(livebirths) & !is.na(stillbirths), total_births := livebirths + stillbirths]
tabsdf[is.na(stillbirths) & !is.na(livebirths) & !is.na(total_births), stillbirths := total_births - livebirths]
tabsdf[is.na(livebirths) & !is.na(stillbirths) & !is.na(total_births), livebirths := total_births - stillbirths]

tabsdf[, sbr := mean]
tabsdf[!is.na(stillbirths) & !is.na(total_births), sbr := stillbirths/total_births]

# Remove extractions where only stillbirth is extracted (missing mean, livebirths, and total_births)
if (nrow(tabsdf[!is.na(stillbirths) & is.na(livebirths) & is.na(total_births) & is.na(mean), ]) > 0) warning("There are rows with stillbirth values but no values for mean, livebirths, or total_births.")
cut_rows2 <- tabsdf[!is.na(stillbirths) & is.na(livebirths) & is.na(total_births) & is.na(mean), ]
if (nrow(cut_rows2) > 0) {
  readr::write_csv(cut_rows2, paste0(root, "FILEPATH"))
}
tabsdf <- tabsdf[!(!is.na(stillbirths) & is.na(livebirths) & is.na(total_births) & is.na(mean))]

# Standardize data types
tabsdf[, data_type := tolower(data_type)]
tabsdf[data_type == "report", data_type := "gov_report"]
tabsdf[data_type %like% "census", data_type := "census"]
tabsdf[data_type %like% "survey", data_type := "survey"]

# Subset to variables I want (ADD underlying_nid if it gets used)
tabsdf_sub <- tabsdf[, c("nid", "data_type", "location_id", "ihme_loc_id", "year_id", "year", "sex_id", "age_group_id", "sbr",
                         "total_births", "stillbirths", "livebirths", "definition", "sb_inclusion_ga", "sb_inclusion_bw", "sb_inclusion_preference")]

tabsdf_sub[, nid := as.numeric(nid)]
tabsdf_sub[, location_id := as.numeric(location_id)]

# Aggregate sex-specific data
sex_specific <- tabsdf_sub[sex_id != 3]
both_sexes <- tabsdf_sub[sex_id == 3]

sex_specific[is.na(sex_specific)] <- ""

sex_list <- as.data.table(sex_specific %>% group_by(ihme_loc_id, nid, year_id) %>%
                          summarize(sex_ids = paste(sort(unique(sex_id)), collapse = ",")))

sex_specific <- merge(sex_specific, sex_list, by = c("ihme_loc_id", "nid", "year_id"))
sex_specific[sex_ids %like% "1", male := "x"]
sex_specific[sex_ids %like% "2", female := "x"]

# Remove if male or female is missing from nid-loc-yr
if (nrow(sex_specific[is.na(female) | is.na(male), ]) > 0) warning("There are extractions missing data for males or females.")
cut_rows3 <- sex_specific[is.na(female) | is.na(male), ]
if (nrow(cut_rows3) > 0) {
  readr::write_csv(cut_rows3, paste0(root, "FILEPATH"))
}
sex_specific <- sex_specific[!(is.na(female) | is.na(male))]

sex_specific[, stillbirths := as.numeric(stillbirths)]
sex_specific[, livebirths := as.numeric(livebirths)]
sex_specific[, total_births := as.numeric(total_births)]

sex_specific_aggr_stillbirths <- aggregate(stillbirths ~ nid + data_type + location_id + ihme_loc_id + year_id + year + age_group_id + definition + sb_inclusion_ga + sb_inclusion_bw + sb_inclusion_preference, data = sex_specific[!is.na(stillbirths)], sum)
sex_specific_aggr_livebirths <- aggregate(livebirths ~ nid + data_type + location_id + ihme_loc_id + year_id + year + age_group_id + definition + sb_inclusion_ga + sb_inclusion_bw + sb_inclusion_preference, sex_specific[!is.na(livebirths)], sum)
sex_specific_aggr_total_births <- aggregate(total_births ~ nid + data_type + location_id + ihme_loc_id + year_id + year + age_group_id + definition + sb_inclusion_ga + sb_inclusion_bw + sb_inclusion_preference, sex_specific[!is.na(total_births)], sum)

sex_specific_aggr <- merge(sex_specific_aggr_stillbirths, sex_specific_aggr_livebirths, by = c("nid", "data_type", "location_id", "ihme_loc_id", "year_id", "year", "age_group_id", "definition", "sb_inclusion_ga", "sb_inclusion_bw", "sb_inclusion_preference"))
sex_specific_aggr <- merge(sex_specific_aggr, sex_specific_aggr_total_births, by = c("nid", "data_type", "location_id", "ihme_loc_id", "year_id", "year", "age_group_id", "definition", "sb_inclusion_ga", "sb_inclusion_bw", "sb_inclusion_preference"))
sex_specific_aggr <- as.data.table(sex_specific_aggr)
sex_specific_aggr[, sex_id := 3]
sex_specific_aggr[, sbr := stillbirths/total_births]

sex_specific_aggr[sex_specific_aggr == ""] <- NA

tabsdf_sub <- rbind(both_sexes, sex_specific_aggr, fill = T)

# Final checks
assertable::assert_values(tabsdf_sub, colnames = names(tabsdf_sub[, c("nid", "data_type", "location_id", "ihme_loc_id", "year_id", "sex_id")]), test = "not_na")
assertable::assert_values(tabsdf_sub, 'location_id', test = 'in', test_val = locsdf$location_id)
assertable::assert_values(tabsdf_sub, 'year_id', test = 'in', test_val = 1950:gbd_year)

# Write a cleaned version of the file back to ADDRESS
readr::write_csv(tabsdf_sub, paste0(root, "FILEPATH"))
readr::write_csv(tabsdf_sub, paste0(root, "FILEPATH", Sys.Date(), ".csv"))
