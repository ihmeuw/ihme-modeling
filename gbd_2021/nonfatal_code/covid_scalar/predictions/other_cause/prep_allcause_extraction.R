# PREP all-cause extraction

pacman::p_load(data.table, openxlsx, ggplot2, grid, gridExtra, lubridate, metafor, pbapply, plyr, dplyr)
username <- Sys.info()[["user"]]

# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("/filepath", full.names = T), source))
source(paste0(h, "filepath/get_row_population.R"))
filepath <- "/filepath.xlsx"
prep_allcause_extraction <- function(filepath){
  message("Reading Data")
  extraction_original <- read.xlsx(filepath)
  extraction <- copy(as.data.table(extraction_original))
  if(class(extraction$start_date) == "character"){
    extraction$start_date <- as.Date(extraction$start_date, tryFormats = c("%m/%d/%Y"))
    extraction$end_date <- as.Date(extraction$end_date, tryFormats = c("%m/%d/%Y"))
  } else if(class(extraction$start_date) == "numeric"){
    extraction$start_date <- convertToDate(extraction$start_date)
    extraction$end_date <- convertToDate(extraction$end_date)
  }

  # The below commented section only needed to be done once
  # hierarchy <- get_location_metadata(location_set_id = 35, gbd_round_id = 7, decomp_step = "iterative")
  # extraction$location_id <- NULL
  # extraction <- merge(extraction, hierarchy[,.(location_id, ihme_loc_id)], by = "ihme_loc_id")
  # message("Adding Population as Sample Size")
  # # Prepare for get_row_population function
  # extraction[sex == "both", sex_id := 3]
  # extraction[sex == "male", sex_id := 1]
  # extraction[sex == "female", sex_id := 2]
  # extraction[, `:=` (year_start = year_id, year_end = year_id)]
  # # Fill in age_demographer
  # extraction[age_end <= 1, age_demographer := 0]
  # extraction[age_end > 1, age_demographer := 1]
  # # Get population separately for all-age and age-specific rows, since get_row_population is slow
  # # For all age, just merge on all-age population
  # pop <- get_population(age_group_id = 22,
  #                       sex_id = c(1,2,3),
  #                       location_id = unique(extraction$location_id),
  #                       year_id = unique(extraction$year_id),
  #                       gbd_round_id = 7,
  #                       decomp_step = "iterative")
  # pop$run_id <- NULL
  # extraction <- merge(extraction, pop, by = c("location_id", "year_id", "sex_id"))
  # # Only actually fill this population field to sample_size for all-age, both-sex rows
  # extraction[age_start == 0 & age_end >= 99, sample_size := population]
  # # Now, only put the NAs into the get_row_population function!
  # extraction_ss_filled <- get_row_population(extraction[is.na(sample_size)], gbd_round = 7, decomp_step = "iterative")
  # extraction_ss_filled[, sample_size := pop_total]
  # extraction_all <- rbind(extraction[!is.na(sample_size)], extraction_ss_filled, fill = T)
  # extraction_all$pop_total <- NULL; extraction_all$population <- NULL
  # write.xlsx(extraction_all, "/filepath.xlsx")

  # Before continuing, check for duplicates in the underlying extraction
  if(any(duplicated(extraction[is_outlier == 0]))) stop ("Duplicates found, check original data")

  # Messy cause_name fixes
  # Drop bizarre or unnecessary cause_names
  extraction <- extraction[!cause_name %like% "avian" & !cause_name %like% "chlamydia"]
  # aggregate the child causes to parent for select causes
  extraction[is.na(cause_name), cause_name := parent_cause]
  extraction[parent_cause %in% c("diphtheria", "tetanus", "varicella","pertussis"), cause_name := parent_cause]

  message("Fixing Spelling")
  extraction[cause_name %like% "rota", cause_name := "Rotavirus"]
  extraction[cause_name %like% "amoeb", cause_name := "Amoebiasis"]
  extraction[cause_name %like% "shigell" | cause_name %like% "Shigell", cause_name := "Shigella"]
  extraction[cause_name %like% "cholera", cause_name := "Cholera"]
  extraction[cause_name %like% "salmonel" | cause_name %like% "Salmonel", cause_name := "Salmonella"]
  extraction[cause_name %like% "cryptospor", cause_name := "Cryptosporidium"]
  extraction[cause_name %like% "campylo" | cause_name %like% "Campylo", cause_name := "Campylobacter"]
  extraction[cause_name %like% "adeno", cause_name := "Adenovirus"]
  extraction[cause_name %like% "aero", cause_name := "Aeromonas"]
  extraction[cause_name %like% "noro", cause_name := "Norovirus"]
  extraction[cause_name %like% "e.coli" | cause_name %like% "TEC", cause_name := "E. coli"]
  extraction[cause_name %like% "clost" | cause_name %like% "difficile", cause_name := "C. diff"]
  extraction[cause_name %like% "sync" | cause_name %like% "rsv", cause_name := "RSV"]

  # naming of etiology-specific invasive infections
  extraction[grepl(paste(c("meningo", "Meningo", "IMD", "Nm", "neiss", "m. invasive"), collapse='|'),cause_name), cause_name := "invasive_meningo"]
  extraction[grepl(paste(c("h. influenza", "hi", "Hi", "haemophilus", "Haemophilus"), collapse='|'),cause_name), cause_name := "invasive_hib"]
  extraction[grepl(paste(c("S. Pneum", "IPD", "streptococcus pneumo", "Pneumococc", "pneumococc"), collapse='|'),cause_name), cause_name := "invasive_pneumo"]
  extraction[grepl(paste(c("Group B", "GBS", "group B", "gbs"), collapse='|'),cause_name), cause_name := "invasive_gbs"]
  extraction[!cause_name %like% "invasive_" & parent_cause == "meningitis", cause_name := "meningitis_other"]
  extraction[cause_name %like% "diarrhea", cause_name := "diarrhea_unspecified"]
  # this one has to come after meningitis to not capture Hib
  extraction[cause_name %like% "flu"| cause_name %like% "Flu", cause_name := "influenza"]
  # outlier any nonspecific LRI/URI due to potential COVID inclusion
  message("Outliering nonspecific LRI/URI")
  extraction[grepl(paste(c("lri", "URI"), collapse='|'),parent_cause), is_outlier := 1]
  # but keep etiology-specific cases
  extraction[grepl(paste(c("influenza", "RSV", "mycoplasma", "hib"), collapse='|'),cause_name), is_outlier := 0]

  # get date range
  extraction[,date_range := (end_date - start_date)+1]

  message("Keeping Only Incidence")
  extraction <- extraction[measure_type == "incidence"]

  message("Filtering on group_review and outlier status")
  extraction <- extraction[is_outlier != 1 | is.na(is_outlier)]
  extraction <- extraction[group_review == 1 | is.na(group_review)]
  return(extraction)
}
