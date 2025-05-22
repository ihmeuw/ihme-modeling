##########################################################################
# Author: USERNAME
# Project: GBD2023 Nonfatal Estimation
# Purpose: Dementia Data Processing; add new sex-specific reference literature
# and COSMIC studies to GBD2021 best xwalk
##########################################################################

rm(list=ls())

# Set directories and parameters
j_root <- "FILEPATH" 
h_root <- "FILEPATH"
l_root <- "FILEPATH"
functions_dir <- "FILEPATH"


gbd2021_best_xwalk <- 39525
gbd2023_bundle_version_id <- 47678 # This version has COSMIC data
cv_drop <- c("cv_nodoctor_diagnosis_dementia")
upload_dir <- "FILEPATH"

xwalk_description <- "GBD2023 Best 2021 xwalk+sex-specific, reference lit+cosmic, corrected location"
xwalk_filename <- "2023_iterative_6689_best2021xwalk+sexspecific_ref_lit+cosmic+correct_location.xlsx"


# load packages and source functions
pacman::p_load(data.table, openxlsx, ggplot2, plyr, parallel, dplyr, RMySQL, stringr, msm, dbplyr, Hmisc, writexl)
functs <- c("save_bundle_version.R", "get_bundle_version.R", "get_crosswalk_version.R",
            "save_crosswalk_version.R", "upload_bundle_data.R", "get_location_metadata.R", "get_age_metadata.R",
            "get_draws.R", "get_population.R", "get_ids.R", "get_elmo_ids.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))

#------------------------------------------------------------------------------#
# Get GBD 2021 best crosswalk
dt_2021_best_xwalk1 <- get_crosswalk_version(gbd2021_best_xwalk)

length(unique(dt_2021_best_xwalk1$nid)) #263 NID
table(dt_2021_best_xwalk1$group_review)

#------------------------------------------------------------------------------#
# Get current bundle and isolate new extractions
dt_current <- get_bundle_version(bundle_version_id = gbd2023_bundle_version_id, fetch = "all", export = FALSE)


dt_new <- dt_current[!dt_current$nid %in% dt_2021_best_xwalk1$nid,]
dt_new1 <- dt_new %>% 
  mutate(unit_type = ifelse(nid %in% c(493732, 529548), "Person*year", unit_type)) %>% 
  filter(!(measure=="incidence" & unit_type == "Person"))

#------------------------------------------------------------------------------#
# Apply filters

# 1. group_review
dt_new2 <- dt_new1[is.na(group_review) | group_review == 1,]

# 2. Filter out claims data
dt_new3 <- dt_new2[clinical_data_type == "",] 

# 3. Filter prevalence and incidence data
dt_new4 <- dt_new3[measure %in% c("incidence", "prevalence"),]

# 4. Keep sex specific data
dt_new5 <- dt_new4[sex %in% c("Male", "Female"),]

# 5. Keep reference data
get_definitions <- function(ref_dt, cv_drop){
  dt <- copy(ref_dt)
  cvs <- names(dt)[grepl("^cv_", names(dt)) & !names(dt) %in% cv_drop]
  cvs <- cvs[!cvs %in% cv_drop]
  dt[, definition := ""]
  for (cv in cvs){
    dt[get(cv) == 1, definition := paste0(definition, "_", cv)]
  }
  dt[definition == "", definition := "reference"]
  return(list(dt=dt, cvs=cvs))
}

dt_new6 <- get_definitions(dt_new5, cv_drop=cv_drop)
dt_new6 <- dt_new6$dt[definition == "reference",]
dt_new6 <- dt_new6 %>%
  mutate(is_outlier = ifelse(nid == 534438, 1, is_outlier),
         note_modeler = ifelse(nid == 534438, paste0(note_modeler, "| outlier due to age issue and non-representativeness"), note_modeler))


#------------------------------------------------------------------------------#
# Add new extractions to GBD2021 best crosswalk and save

dt_xwalk_final <- rbind(dt_2021_best_xwalk1, dt_new6, fill=TRUE)

# Fix validation errors
dt_xwalk_final$seq <- NA

dt_xwalk_final1 <- dt_xwalk_final %>% 
  mutate(recall_type = ifelse(recall_type == "Period: years" & is.na(recall_type_value), "Not Set", recall_type),
         effective_sample_size = ifelse(effective_sample_size < cases, NA, effective_sample_size)) %>% 
  filter(is.na(group_review) | group_review ==1)

dt_xwalk_final2 <- dt_xwalk_final1 %>% 
  mutate(fl_err = ifelse(upper < mean | lower >= mean | upper < lower,1,0)) %>%
  filter(fl_err == 0 | is.na(fl_err)) %>% 
  select(-fl_err)

dt_xwalk_final2[nid %in% c(426890, 543395), `:=` (is_outlier=1, note_modeler = "Outlier COSMIC data due to large influence in estimates")]

write_xlsx(list(extraction=dt_xwalk_final2), path=paste0(upload_dir, xwalk_filename))

save_crosswalk_version(bundle_version_id = gbd2023_bundle_version_id, description = xwalk_description,
                       data_filepath = paste0(upload_dir, xwalk_filename))
