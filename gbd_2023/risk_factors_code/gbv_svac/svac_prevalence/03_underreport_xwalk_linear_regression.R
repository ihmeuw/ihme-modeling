####################################################################################################################################################################
# 
# Purpose: Estimate relationship between interviewer- and self-adminsitered surveys
#
####################################################################################################################################################################
rm(list=ls())

#date 
ur_xw_date <- "underreport_xw_all_data"

#version name
version <- 'src_linear_reg'


### Set-up environment -----------------------------------------------------------------------------------------------------------------------------------------------

#libraries + central functions
pacman::p_load(data.table, openxlsx, dplyr, ggplot2, stringr, locfit, gridExtra)
invisible(sapply(list.files("FILEPATH", full.names = T), source))
library(logitnorm, lib.loc= paste0("FILEPATH", Sys.getenv(x='USER'), "/rlibs"))
library(dummies, lib.loc= paste0("FILEPATH", Sys.getenv(x='USER'), "/rlibs"))

#directories
xwalk_output_fp <- paste0('FILEPATH', ur_xw_date, '/')
dir.create(xwalk_output_fp, recursive = T)

#location metadata 
locs <- get_location_metadata(22, release_id = 16)


### Read in data and get into correct matched format for within-study pairs ------------------------------------------------------------------------------------------

#get cleaned data
og_data <- fread("FILEPATH") %>% setnames("prevalence", "mean")

#subset to individual studies and raw data rather than custom aggregates
data <- copy(og_data[!is.na(nid)])

#drop extra columns
data[, c("cases", "lancet_label", "lower", "upper", "age_range", "csa_age", "case_definition", "title") := NULL]

#subset to src only
xwalk_data <- data[collection_method %in% c("face", "card")]

#drop extra cols
xwalk_data[, c("super_region_name", "age_bin", "sample_size", "year_start", "year_end") := NULL]


### Extra cleaning and matching for crosswalk ------------------------------------------------------------------------------------------------------------------------

#adjust zero values
mean_adj_val_ftfi <- quantile(xwalk_data[mean!=0 & collection_method == "face"]$mean, probs=0.025, na.rm = T)
mean_adj_val_src <- quantile(xwalk_data[mean!=0 & collection_method == "card"]$mean, probs=0.025, na.rm = T)

xwalk_data[mean <= mean_adj_val_ftfi & collection_method == "face", mean := mean_adj_val_ftfi]
xwalk_data[mean <= mean_adj_val_src & collection_method == "card", mean := mean_adj_val_src]

#get subsets of gs and alternate defs
ref_subset <- copy(xwalk_data[collection_method == "card"]) #refrence (ie, gold standard): self-report card
alts_subset <- copy(xwalk_data[collection_method == "face"]) #alt: face-to-face

#set names accordingly
setnames(ref_subset, c('mean', 'se', 'collection_method'), c('ref_mean', 'ref_se', 'refvar'))
setnames(alts_subset, c('mean', 'se', 'collection_method'), c('alt_mean', 'alt_se', 'altvar'))

#merge back onto each other
matched <- merge(ref_subset, alts_subset, by=c('nid', 'ihme_loc_id', 'location_name', 'year_id', 'sex', 'age_start', 'age_end', "urbanicity"), allow.cartesian = T)

#create a dummy column for each super_region_name
dummies <- ((cbind(locs[level==1, .(super_region_id, super_region_name)], dummy.data.frame(locs[level == 1, c('super_region_name')]))))

#merge super region dummies
matched <- merge(matched, locs[, .(ihme_loc_id, super_region_name)], by = 'ihme_loc_id', all.x = T)
matched[grepl('Serbia', location_name), super_region_name := 'Central Europe, Eastern Europe, and Central Asia']
matched <- merge(matched, dummies, by = 'super_region_name', all.x = T)

#linear regression
mod1 <- lm(ref_mean ~ alt_mean, data = matched)

#save 
saveRDS(mod1, paste0(xwalk_output_fp, version, '.RDS'))




