# SET OBJECTS -------------------------------------------------------------
rm(list=ls())

# SOURCE FUNCTIONS AND LIBRARIES --------------------------------------------------
pacman::p_load(cowplot,data.table, ggplot2, RMySQL, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, stringr, readxl, foreign, maptools, RColorBrewer, grid, gridExtra, ggplot2, sp, reshape2, rgdal, timeDate, scales, lubridate, lattice, viridis, zoo, ggrepel, data.table, labeling, forcats)
invisible(sapply(list.files("FILEPATH", full.names = T), source))
functions <- c("get_age_metadata", "get_outputs", "get_location_metadata", "get_ids", "get_covariate_estimates", "get_envelope", "get_pct_change", "get_draws", "get_population", "get_elmo_ids", "get_bundle_version", "get_crosswalk_version", "get_cod_data")

b_id <- OBJECT
bundle_id <- OBJECT 
a_cause <- "cirrhosis"
acause <- "cirrhosis"
save_outdir <- paste0(FILEPATH)
date <- gsub("-", "_", Sys.Date())



#save clinical_version_id = 5 with new CF (without MS)
result <- save_bundle_version(bundle_id, include_clinical=c("claims", "inpatient"))
print(sprintf('Bundle version ID: %s', result$bundle_version_id))
bv_id <- OBJECT
cirr = get_bundle_version(bv_id, fetch='all', export = FALSE) 
cirr <- cirr %>%
  filter(!(nid %in% c(244370, 336850, 244371, 336849, 336848, 336847, 408680, 433114, 494351, 494352, 244369))) #remove marketscan

cirr_foroutlier <- cirr %>%
  filter(!((grepl("HCUP|National Hospital Discharge Survey", field_citation_value)) & age_end > 64))%>%
  filter(!(grepl("CMS", field_citation_value) & age_start <65)) ##Keep HCUP in all US states only up to 64 Only keep CMS for 65+



envelope_new<-read.xlsx("FILEPATH")
table(addNA(envelope_new$nid))
table(cirr_foroutlier$nid)
envelope_new<-envelope_new%>%mutate(nid=ifelse(merged_nid=="na",nid,merged_nid))
envelope_distinct_new <- envelope_new %>% distinct(nid, uses_env,.keep_all = TRUE)
envelope_distinct_new$nid<-as.numeric(envelope_distinct_new$nid)
df_final<-left_join(cirr_foroutlier,envelope_distinct_new,by="nid")
df_final_exclude_envelope<-df_final
df_final_exclude_envelope <- df_final_exclude_envelope %>%
  mutate(is_outlier = ifelse(is.na(uses_env), 0, ifelse(uses_env == 1, 1, 0)))

df_final_exclude_envelope <- df_final_exclude_envelope %>%
  filter(measure == "prevalence")
names(df_final_exclude_envelope)[names(df_final_exclude_envelope) == "origin_seq"] <- "crosswalk_parent_seq"

xwalk_filepath <- paste0(FILEPATH)
write.xlsx(df_final_exclude_envelope, xwalk_filepath, rowNames = FALSE, sheetName = "extraction")
description <- paste0("DESCRIPTION) result <- save_crosswalk_version(bv_id, xwalk_filepath, description)
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
