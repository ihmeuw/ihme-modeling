#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Function to analyze household modules from microdata extractions to estimate: 1) mean HH composition by location and
#          household smoking status, and 2) binary SHS exposure (nonsmoker in same HH as smoker)
# Required function inputs: suvey_series
#                           Options: "MACRO_DHS", "SHARE", "GATS", "WHS", "NHANES", "SAGE", "MICS", "LSMS"
#***********************************************************************************************************************


#----FUNCTION------------------------------------------------------------------------------------------------------------

hh_composition <- function(survey_series, file_paths) {
  
  if (survey_series=="DHS") {
      
      #file_paths <- dhs_paths[1]
      # read in HH file
      all_data_hh <- read_dta(file.path(j_root, "FILEPATH", file_paths)) %>% as.data.table
      print("Success! | Data loaded in")
      
  }
  
  if (survey_series %in% c("SHARE", "GATS", "WHS")) {
      
      #file_paths <- share_paths[1]
      # read in HH files
      all_data_hh <- fread(file.path(j_root, "FILEPATH", file_paths))
      print("Success! | Data loaded in")
      
  }
  
  if (survey_series=="census") {
    
      #file_paths <- census_paths[1]
      # read in HH file
      all_data_hh <- read_dta(file_paths) %>% as.data.table
      print("Success! | Data loaded in")
    
  }
      
      ### stop if no hh_id column
      if ("hh_id" %in% colnames(all_data_hh)==FALSE) stop("STOP | Can't run HH function without hh_id")
      ### stop if no age_year column
      if ("age_year" %in% colnames(all_data_hh)==FALSE) stop("STOP | Can't run HH function without age_year")
      ### stop if no sex_id column
      if ("sex_id" %in% colnames(all_data_hh)==FALSE) stop("STOP | Can't run HH function without sex_id")
      
      ### add in psu, strata, pweight col if missing
      if (!"psu" %in% names(all_data_hh)) { all_data_hh$psu <- 1 }
      if (unique(all_data_hh$survey_name)=="PNG/CENSUS") { all_data_hh <- all_data_hh[!is.na(psu), ] }
      all_data_hh[is.na(psu), psu := 1]
      if (!"strata" %in% names(all_data_hh)) { all_data_hh$strata <- 1 }
      all_data_hh[is.na(all_data_hh$strata), strata := 1]
      # UNICEF_MICS surveys missing pweights-- use hhweight instead
      if (!"pweight" %in% names(all_data_hh) & unique(all_data_hh$survey_name=="UNICEF_MICS")) { all_data_hh[, pweight := hhweight] }
      if ("pweight" %in% names(all_data_hh)) { if (any(is.na(all_data_hh[, pweight])) & unique(all_data_hh$survey_name=="UNICEF_MICS")) { 
                                                   all_data_hh[, pweight := hhweight] } }
      if (!"pweight" %in% names(all_data_hh) & "hhweight" %in% names(all_data_hh)) { all_data_hh[, pweight := hhweight]
                                                                                     print("pweight missing from data, filling in column with hhweight")}
      if (!"pweight" %in% names(all_data_hh) & !"hhweight" %in% names(all_data_hh)) { all_data_hh[, pweight := 1] 
                                                                                      print("pweight missing from data, filling in column with 1 (SRS)") }
      
      ### make sure hh_id not in scientific notation
      all_data_hh[, hh_id := format(hh_id, scientific=FALSE)]
      all_data_hh[, hh_id := gsub(" ", "", hh_id)]
      
      ### replicate data by subnational ihme_loc_id if survey contains representativesubnational information
      if ("admin_1_id" %in% colnames(all_data_hh) | "admin_1_urban_id"  %in% colnames(all_data_hh) | "admin_2_id"  %in% colnames(all_data_hh) | "admin_3_id"  %in% colnames(all_data_hh)) {
        subnat_files <- NULL
        if ("admin_1_id" %in% colnames(all_data_hh)) {
          for (subnat in unique(all_data_hh$admin_1_id)) {
            assign(paste0(subnat, "_data_hh"), subset(all_data_hh[admin_1_id==subnat, ]))
            set(get(paste0(subnat, "_data_hh")), j="ihme_loc_id", value=get(paste0(subnat, "_data_hh"))[["admin_1_id"]])
            subnat_files <- c(subnat_files, paste0(subnat, "_data_hh"))
          }
        }
        if ("admin_1_urban_id" %in% colnames(all_data_hh)) {
          for (subnat in unique(all_data_hh$admin_1_urban_id)) {
            assign(paste0(subnat, "_data_hh"), subset(all_data_hh[admin_1_urban_id==subnat, ]))
            set(get(paste0(subnat, "_data_hh")), j="ihme_loc_id", value=get(paste0(subnat, "_data_hh"))[["admin_1_urban_id"]])
            subnat_files <- c(subnat_files, paste0(subnat, "_data_hh"))
          }
        }
        if ("admin_2_id" %in% colnames(all_data_hh)) {
          for (subnat in unique(all_data_hh$admin_2_id)) {
            assign(paste0(subnat, "_data_hh"), subset(all_data_hh[admin_2_id==subnat, ]))
            set(get(paste0(subnat, "_data_hh")), j="ihme_loc_id", value=get(paste0(subnat, "_data_hh"))[["admin_2_id"]])
            subnat_files <- c(subnat_files, paste0(subnat, "_data_hh"))
          }
        }
        if ("admin_3_id" %in% colnames(all_data_hh)) {
          for (subnat in unique(all_data_hh$admin_3_id)) {
            assign(paste0(subnat, "_data_hh"), subset(all_data_hh[admin_3_id==subnat, ]))
            set(get(paste0(subnat, "_data_hh")), j="ihme_loc_id", value=get(paste0(subnat, "_data_hh"))[["admin_3_id"]])
            subnat_files <- c(subnat_files, paste0(subnat, "_data_hh"))
          }
        }
        print("Success! | Subnational locations created")
      }
      
      ### keep survey columns and current smoking columns
      cleaned_cols <- c("nid", "file_path", "ihme_loc_id", "survey_name", "year_start", "year_end", "survey_module", "hh_id",
                        "pweight", "strata", "psu", "age_year", "sex_id")
      all_data_hh[, c("ihme_loc_id", "year_start", "strata", "psu", "hh_id") := 
                    lapply(all_data_hh[, c("ihme_loc_id", "year_start", "strata", "psu", "hh_id"), with=FALSE], as.character)]
      all_data_hh <- all_data_hh[, colnames(all_data_hh) %in% cleaned_cols, with=FALSE]

      ### create unique identifier
      all_data_hh[, new_id := do.call(paste, c(all_data_hh[, c("ihme_loc_id", "year_start", "strata", "psu", "hh_id"), with=FALSE], sep="_"))]
      
      ### remove households with missing age/sex information
      age_sex_missings <- unique(all_data_hh[is.nan(age_year) | is.nan(sex_id) | is.na(age_year) | is.na(sex_id), new_id])
      all_data_hh <- all_data_hh[!new_id %in% age_sex_missings, ]
      # fix age groups
      all_data_hh[, age_year := age_year %>% as.numeric %>% floor]
      age_map <- fread(file.path(j_root, "FILEPATH/age_map_all_ages.csv"))
      setnames(age_map, "age", "age_year")
      all_data_hh <- merge(all_data_hh, age_map, by="age_year", all.x=TRUE)
      all_data_hh <- all_data_hh[, colnames(all_data_hh) != "age_year", with=FALSE]
      all_data_hh[age_group_id %in% c(0, 2, 3, 4, 5), age_group_id := 1]
      
      # create dummy variables of all age-sex combinations
      hh <- copy(all_data_hh)
      hh$age_sex_ <- do.call(paste, c(hh[, c("age_group_id", "sex_id"), with=FALSE], sep="_"))
      hh$age_sex_ <- hh$age_sex_ %>% as.factor
      dummies <- dummy.data.frame(hh, dummy.class="factor")
      print("Success! | Household dummy variables created")
      
      # summarize HH characteristics
      dum_cols <- c("nid", "ihme_loc_id", "survey_name", "file_path", "year_start", "year_end", "survey_module", 
                    "strata", "psu", "hh_id", "new_id")
      # collapse by hh age and sex composition
      summary <- ddply(dummies, dum_cols, function(x) colSums(x[, grep("age_sex", colnames(dummies))]))
      print("Success! | Household summaries collapsed")
      
      ### bring summaries together
      hh_summary <- merge(all_data_hh, summary, by=dum_cols, all.x=TRUE)
      # make sure all columns filled out
      hh_cols <- c("age_sex_10_1", "age_sex_10_2", "age_sex_11_1", "age_sex_11_2", "age_sex_12_1",  "age_sex_12_2", 
                   "age_sex_13_1", "age_sex_13_2", "age_sex_14_1", "age_sex_14_2", "age_sex_15_1",  "age_sex_15_2", 
                   "age_sex_16_1", "age_sex_16_2", "age_sex_17_1", "age_sex_17_2", "age_sex_18_1",  "age_sex_18_2", 
                   "age_sex_19_1", "age_sex_19_2", "age_sex_20_1", "age_sex_20_2", "age_sex_235_1", "age_sex_235_2", 
                   "age_sex_30_1", "age_sex_30_2", "age_sex_31_1", "age_sex_31_2", "age_sex_32_1",  "age_sex_32_2", 
                   "age_sex_1_1",  "age_sex_1_2",  "age_sex_5_1",  "age_sex_5_2",  "age_sex_6_1",   "age_sex_6_2", 
                   "age_sex_7_1",  "age_sex_7_2",  "age_sex_8_1",  "age_sex_8_2",  "age_sex_9_1",   "age_sex_9_2")
      missing_cols <- hh_cols[!hh_cols %in% colnames(hh_summary)]
      if (length(missing_cols) > 0) {
        for (columns in missing_cols) { hh_summary[, paste0(columns)] <- 0 }
      }
      
      ### set year_id as floor of mean year_start and year_end
      year <- floor((unique(as.numeric(all_data_hh$year_start)) + unique(as.numeric(all_data_hh$year_end))) / 2)
      
      # save hh_composition
      print("Success! | Household composition created, saving file now")
      write.csv(hh_summary, file.path(j_root, "FILEPATH", 
                                      paste0(gsub("/", "_", unique(hh_summary$survey_name)), "_", unique(hh_summary$ihme_loc_id), 
                                             "_", year, "_", unique(hh_summary$nid), ".csv")), row.names=FALSE)
      
      ### replicate HH extractions for each subnational unit, if they exist in the survey
      if (exists("subnat_files")) {
        for (subnat_file in subnat_files) {
          
          all_data_hh <- copy(get(subnat_file))
          
          ### keep survey columns and current smoking columns
          cleaned_cols <- c("nid", "file_path", "ihme_loc_id", "survey_name", "year_start", "year_end", "survey_module", "hh_id",
                            "pweight", "strata", "psu", "age_year", "sex_id")
          all_data_hh[, c("ihme_loc_id", "year_start", "strata", "psu", "hh_id") := 
                        lapply(all_data_hh[, c("ihme_loc_id", "year_start", "strata", "psu", "hh_id"), with=FALSE], as.character)]
          all_data_hh <- all_data_hh[, colnames(all_data_hh) %in% cleaned_cols, with=FALSE]
          
          ### create unique identifier
          all_data_hh[, new_id := do.call(paste, c(all_data_hh[, c("ihme_loc_id", "year_start", "strata", "psu", "hh_id"), with=FALSE], sep="_"))]
          
          ### remove households with missing age/sex information
          age_sex_missings <- unique(all_data_hh[is.nan(age_year) | is.nan(sex_id) | is.na(age_year) | is.na(sex_id), new_id])
          all_data_hh <- all_data_hh[!new_id %in% age_sex_missings, ]
          
          # fix age groups
          all_data_hh[, age_year := age_year %>% as.numeric %>% floor]
          age_map <- fread(file.path(j_root, "FILEPATH/age_map_all_ages.csv"))
          setnames(age_map, "age", "age_year")
          all_data_hh <- merge(all_data_hh, age_map, by="age_year", all.x=TRUE)
          all_data_hh <- all_data_hh[, colnames(all_data_hh) != "age_year", with=FALSE]
          all_data_hh[age_group_id %in% c(0, 2, 3, 4, 5), age_group_id := 1]
          
          # create dummy variables of all age-sex combinations
          hh <- copy(all_data_hh)
          hh$age_sex_ <- do.call(paste, c(hh[, c("age_group_id", "sex_id"), with=FALSE], sep="_"))
          hh$age_sex_ <- hh$age_sex_ %>% as.factor
          dummies <- dummy.data.frame(hh, dummy.class="factor")
          print("Success! | Household dummy variables created")
          
          # summarize HH characteristics
          dum_cols <- c("nid", "ihme_loc_id", "survey_name", "file_path", "year_start", "year_end", "survey_module", 
                        "strata", "psu", "hh_id", "new_id")
          # collapse by hh age and sex composition
          summary <- ddply(dummies, dum_cols, function(x) colSums(x[, grep("age_sex", colnames(dummies))]))
          print("Success! | Subnational household summaries collapsed")
          
          ### bring summaries together
          hh_summary <- merge(all_data_hh, summary, by=dum_cols, all.x=TRUE)
          # make sure all columns filled out
          hh_cols <- c("age_sex_10_1", "age_sex_10_2", "age_sex_11_1", "age_sex_11_2", "age_sex_12_1",  "age_sex_12_2", 
                       "age_sex_13_1", "age_sex_13_2", "age_sex_14_1", "age_sex_14_2", "age_sex_15_1",  "age_sex_15_2", 
                       "age_sex_16_1", "age_sex_16_2", "age_sex_17_1", "age_sex_17_2", "age_sex_18_1",  "age_sex_18_2", 
                       "age_sex_19_1", "age_sex_19_2", "age_sex_20_1", "age_sex_20_2", "age_sex_235_1", "age_sex_235_2", 
                       "age_sex_30_1", "age_sex_30_2", "age_sex_31_1", "age_sex_31_2", "age_sex_32_1",  "age_sex_32_2", 
                       "age_sex_1_1",  "age_sex_1_2",  "age_sex_5_1",  "age_sex_5_2",  "age_sex_6_1",   "age_sex_6_2", 
                       "age_sex_7_1",  "age_sex_7_2",  "age_sex_8_1",  "age_sex_8_2",  "age_sex_9_1",   "age_sex_9_2")
          missing_cols <- hh_cols[!hh_cols %in% colnames(hh_summary)]
          if (length(missing_cols) > 0) {
            for (columns in missing_cols) { hh_summary[, paste0(columns)] <- 0 }
          }
          
          # save hh_composition
          print("Success! | Subnational household composition calculated, saving now")
          write.csv(hh_summary, file.path(j_root, "FILEPATH", 
                                          paste0(gsub("/", "_", unique(hh_summary$survey_name)), "_", unique(hh_summary$ihme_loc_id), 
                                                 "_", year, "_", unique(hh_summary$nid), ".csv")), row.names=FALSE)
          
        }
      }
  
}

#***********************************************************************************************************************