### Purpose: Prep WaSH extractions for collapse
#####################################################################

# libraries & functions #####################################################
library(data.table)
library(magrittr)

"%ni%" <- Negate("%in%")

## prep wash data and save to directory
prep_and_save <- function(file, limited_use, current_year) {

  print(file)

  ## directories
  
  if (limited_use == T) {
    in.dir<- paste0("FILEPATH",batch)
    out.dir <- ifelse(grepl("CENSUS", file),
                      paste0("FILEPATH", current_year, "FILEPATH"), # censuses need to be collapsed separately
                      file.path(paste0("FILEPATH", current_year, "FILEPATH"), Sys.Date()))
    
    sewer.out.dir <- ifelse(grepl("CENSUS", file),
                            paste0("FILEPATH", current_year, "FILEPATH"), # censuses need to be collapsed separately
                            file.path(paste0("FILEPATH", current_year, "FILEPATH"), Sys.Date()))
    
    imp.out.dir <- ifelse(grepl("CENSUS", file),
                          paste0("FILEPATH", current_year, "FILEPATH"), # censuses need to be collapsed separately
                          file.path(paste0("FILEPATH", current_year, "FILEPATH"), Sys.Date()))
  } else {
    in.dir <- paste0("FILEPATH", current_year,"FILEPATH",batch)
    out.dir <- ifelse(grepl("CENSUS", file),# censuses need to be collapsed separately
                      paste0("FILEPATH", current_year, "FILEPATH"), 
                      file.path(paste0("FILEPATH"), Sys.Date())) # 
    sewer.out.dir<-ifelse(grepl("CENSUS", file),# censuses need to be collapsed separately
                          paste0("FILEPATH", current_year, "FILEPATH"), 
                          file.path(paste0("FILEPATH"), Sys.Date()))
    imp.out.dir<-ifelse(grepl("CENSUS", file),# censuses need to be collapsed separately
                        paste0("FILEPATH", current_year, "FILEPATH"), 
                        file.path(paste0("FILEPATH"), Sys.Date())) 
    
    
  }

  if (!dir.exists(out.dir)) dir.create(out.dir, recursive = TRUE)
  if (!dir.exists(sewer.out.dir)) dir.create(sewer.out.dir, recursive = TRUE)
  if (!dir.exists(imp.out.dir)) dir.create(imp.out.dir, recursive = TRUE)

#--------------------------------------------------------------------------------------------------------
  # read in files ############################################################
  
  ## extraction sheet ######################
  ## specify encoding to Latin-1 in order to deal with special characters
  dt<-fread(file.path(in.dir, file), encoding = "Latin-1")
  dt[dt == ""] <- NA # replace empty strings with NA
  #defining same year same location mics and mics roma pairs for combination
  nid_pairs <- list(c(453328, 453334), c(413741, 413663), c(459477, 459500), c(462027, 461987))
  
  # List of NIDs to exclude
  nids_to_exclude <- c(453334, 413663, 459500,461987) # Notice these are the seconds of the pairs as we dont want to double process these
  
  # Check if NID is in the exclusion list
  if (dt$nid[1] %in% nids_to_exclude) {
    message(paste("Skipping file due to NID exclusion:", file))
    return(NULL) # Skip further processing for this file
  }
  
  print(file)
  # Function to find matching file path by grep searching the nid in the file names
  find_matching_file_path <- function(current_nid, nid_pairs, files) {
    for (pair in nid_pairs) {
      if (current_nid %in% pair) {
        # Find the matching nid
        matching_nid <- setdiff(pair, current_nid)
        # Pattern to match file name
        pattern <- paste0(".*", matching_nid, ".*")
        # Grep search for file
        matching_files <- grep(pattern, files, value = TRUE)
        if (length(matching_files) > 0) {
          return(matching_files[1]) # Return the first matching file path
        }
      }
    }
    return(NULL) # Return NULL if no match is found
  }
  
    # Extract nid from the current file's data
    current_nid <- unique(dt$nid)[1]
    
    # Check if current_nid is part of any pair
    if (any(unlist(nid_pairs) %in% current_nid)) {
      matching_file_path <- find_matching_file_path(current_nid, nid_pairs, files)
      if (!is.null(matching_file_path)) {
        dt2 <- fread(file.path(in.dir,matching_file_path), encoding = "Latin-1")
        dt2[dt2 == ""] <- NA # Replace empty strings with NA
        
        #add _2 to hhid to make sure hhid is not the same when we combine them
        dt2$hh_id <- paste(dt2$hh_id,2,sep = "_")
        
        # Prefix old columns and create new ones
        setnames(dt2, old = c("nid", "file_path", "survey_name"), new = paste0("old_", c("nid", "file_path", "survey_name")))
        dt2[, c("nid", "file_path", "survey_name") := list(current_nid, dt$file_path[1],dt$survey_name[1])]
        
        # Bind dt and dt2
        dt <- rbind(dt, dt2, fill = TRUE)
        
        # Add roma_comb_flag column
        dt[, roma_comb_flag := 1]
        
        # Assign back to dt for further processing
        # dt is now the combined dataset
      }
    }else{
      dt[,roma_comb_flag :=0]
    }
  
  ## string mapping spreadsheets (collab with LBD) ##################
  w.strings <- fread("FILEPATH/w_source_defined_by_nid.csv") # water string mapping
  w.strings[w.strings == ""] <- NA # replace empty strings with NA
  setnames(w.strings, c("sdg","iso3"), c("w.indicator","ihme_loc_id"))

  s.strings <- fread("FILEPATH/t_type_defined_by_nid.csv") # sanitation string mapping
  s.strings[s.strings == ""] <- NA # replace empty strings with NA
  setnames(s.strings, c("sdg","iso3"), c("s.indicator","ihme_loc_id"))

  ee.strings <- fread("FILEPATH/t_ever_emptied_defined_by_nid.csv") # t_ever_emptied string mapping
  ee.strings[ee.strings == ""] <- NA
  setnames(ee.strings, c("sdg", "iso3"),c("ee.indicator","ihme_loc_id"))

  ec.strings <- fread("FILEPATH/t_emptied_contents_defined_by_nid.csv") # t_emptied contents mapping
  ec.strings[ec.strings == ""] <- NA # replace empty strings with NA
  setnames(ec.strings, c("sdg", "iso3"),c("ec.indicator","ihme_loc_id"))

  ## datasets used to split ambiguous strings #######################
  water.cw <- rbindlist(lapply(c("FILEPATH/cw_water_2.csv", # LBD dataset (maintained by USERNAME) #message USERNAME to get more info
                                "FILEPATH/cw_water_new_iso3.csv"),# GBD dataset (countries not in LBD)
                               fread), fill = T)
 
  sani.cw <- rbindlist(lapply(c("FILEPATH/cw_sani_2.csv", # LBD dataset (maintained by USERNAME)
                                "FILEPATH/cw_sani_new_iso3.csv"),# GBD dataset (countries not in LBD)
                              fread), fill = T)
  print("<")
  #--------------------------------------------------------------------------------------------------------
  # settings ####################################################

  ## these are the pweights used in collapse code ##################
  pweights <- c("pweight","pweight_admin_1","pweight_admin_2","pweight_admin_3")
  ## these are the variables for which there might be missingness; if so, need to create a missingness indicator
  # if there is >15% missingness in pweight, exclude the source entirely
  # if >15% missingness in a particular variable, exclude the indicators related to it (e.g. t_type --> wash_sanitation_piped & wash_sanitation_imp_prop)
  missing_vars <- c("strata","psu","pweight","hh_size","hhweight",
                    "t_type","w_source_drink", "w_treat","hw_station" ,"t_ever_emptied","t_emptied_contents")

  ## indicators ######################
  ### latest string matching doc is at FILEPATH
  # missing observations
  to.drop <- c(""," ","other","unknown",NA,NaN)
  to.drop.ec <- c(""," ","unknown",NA,NaN)

  #### water #########
  piped <- c("piped")
  w.improved <- c("bottled","imp","piped_imp","spring_imp","well_imp","improved")
  w.cw <- c("well_cw","spring_cw","piped_cw")
  w.unimproved <- c("spring_unimp","well_unimp","surface","unimp","not_piped")

  ### sanitation #######
  sewer <- c("septic","sewer","flush_imp_septic","flush_imp_sewer","flush_imp")
  s.improved <- c("imp","latrine_imp")
  s.cw <- c("flush_cw","latrine_cw")
  s.unimproved <- c("flush_unimp","latrine_unimp","unimp","open")

  ### handwashing ##########
  hw_general <- c("hw_soap","hw_water","hw_station")
  hw_acs <- c("hw_soap_acs","hw_water","hw_station")
  hw_mics <- c("hw_soap_mics","hw_water","hw_station")
  hw_pma <- c("hw_soap_pma","hw_water_pma","hw_station")

  ### household water treatment #########
  hwt_vars <- c("w_bleach","w_boil","w_filter","w_solar")

  ### toilet ever emptied #############
  ee_yes <- c("yes")
  ee_no <- c("no")
  ee_dk <- c("dk")

  ### toilet emptied contents ###########
  ec_tplant <- c("treatment_plant")
  ec_buriedpit <- c("buried_covered_pit")
  ec_dk <- c("dk")
  ec_open <- c("empty_open")
  ec_other <- c("other")

  print("<<")
  #--------------------------------------------------------------------------------------------------------
  # Check for completeness ################################################
  ## validation checks for extraction completeness

  ## if source is a HH module, check to make sure that hhweight variable is present (sometimes hhweight gets codebooked into the pweight variable instead)
  ## if not, individual-level estimates and household-level estimates will be identical 
  if (unique(dt$survey_module) == "HH" & "hhweight" %ni% names(dt)) {
    stop(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
               "- is a HH module but does not have hhweight. Please re-extract!"))
  }

  ## if source is a HHM module, we need hh_id in order to identify unique households so that we can generate a dataset where each row is a HH
  if (unique(dt$survey_module) != "HH" & "hh_id" %ni% names(dt)){
    stop(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                "- is a HHM module but does not have hh_id. Please re-extract!"))
  }

  ## if source is a HH module, we need hh_size in order to expand households so that we can generate a datset where each row is a HHM
  if (unique(dt$survey_module) == "HH" & "hh_size" %ni% names(dt)) {
    stop(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                "- is a HH module but does not have hh_size. Please re-extract!"))
  }

  ## check to make sure that current source is already in the string mapping spreadsheets
  ## if not, need to add
  # every unique combination of NID-location-year in w.strings & s.strings
  check <- unique(c(w.strings[, paste(nid, ihme_loc_id, year_start, sep = "_")],
                    s.strings[, paste(nid, ihme_loc_id, year_start, sep = "_")]))
  # NID-location-year of the source being extracted
  current_source <- dt[, unique(paste(nid, ihme_loc_id, year_start, sep = "_"))]

  # STOP HERE WHEN RUNNING CODE BY HAND ########################################################
  if (current_source %ni% check & any(c("w_source_drink","t_type") %in% names(dt))) {
    message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
               "- has water and/or sanitation data but is not in either of the string mapping spreadsheets. Skipping it now - please run this script after it has been added."))
  } else {


    print("<<<")
    #--------------------------------------------------------------------------------------------------------
    # Prep for HH and HHM collapse ######################################################
    ### prep dataset for both household and household member collapse

    # START CODE AGAIN HERE #########################################################
    ## Missingness ######################
    # generate missingness indicators for variables specified above (missing_vars)
    collapse_vars <- intersect(names(dt), c("nid","admin_1_id","admin_1_urban_id","admin_2_id")) # these are the variables the collapse script uses, so we want missingness within each group
    for (mvar in missing_vars) {
      if (mvar %ni% names(dt)) {
        dt[, c(paste0("missing_", mvar)) := 1]
      } else {
        dt[, temp := 0]
        dt[get(mvar) %in% c(NA,NaN,Inf,-Inf,"other","unknown",""), temp := 1]
        dt[, paste0("missing_", mvar) := mean(temp), by = collapse_vars]
        dt[, temp := NULL]
      }
    }

    ## Replace missingness ###########
    # replace missing or unrealistic hh_size
    if("hh_size" %in% names(dt)) {

      # if household size is >35, replace with missing and give a warning
      if(nrow(dt[hh_size > 35]) > 0) {
        message(paste0("NID: ", unique(dt$nid), ". This survey has ", nrow(dt[hh_size > 35]), " rows with hh_size > 35 out of ", nrow(dt), " total rows. Replacing these with median."))
      }
      dt[hh_size > 35, hh_size := NA]

      # if household size is missing, replace with median
      dt[is.na(hh_size), hh_size := median(dt$hh_size, na.rm = TRUE)]

    }

    ## Dataset of HH #################################
    # Generate a dataset where each row is a HH. We can do this for HH modules or HHM modules with unique HH identifiers
    if ("hh_id" %in% names(dt)) { # if we have a household id we will subset to the first row of each household
      dt_HH <- copy(dt)

      # these are variables which should give us a unique household
      hh_collapse_vars <- intersect(c(collapse_vars,"hh_id","psu","strata"), names(dt))

      # create column identifying these rows as HHs
      dt_HH[, cv_HH := 1]

      # calculate average pweight for hh if available
      for (weight in pweights) {
        if (weight %in% names(dt_HH)) {
          dt_HH[, paste0("hh_avg_", weight) := mean(get(weight), na.rm = TRUE), by = hh_collapse_vars]
        }
      }

      # keep only the first row for each unique household
      dt_HH[, keep := 1:.N, by = hh_collapse_vars]
      dt_HH <- dt_HH[keep == 1][, keep := NULL]

    } else if (unique(dt$survey_module) == "HH") { # here we are assuming all modules labeled "HH" without hh_id are indeed HH modules

      dt_HH <- copy(dt)
      dt_HH[, cv_HH := 1]

      # rename pweight for hh if available
      for (weight in pweights) {
        if (weight %in% names(dt_HH)) {
          dt_HH[, paste0("hh_avg_", weight) := get(weight)]
        }
      }

    }

    print("<<<<")
    # for HH surveys, we should be using hhweights instead of pweights but the collapse code only takes pweights
    if (exists("dt_HH")) {

      # first choice is to use hhweight, second choice is to use average household pweight (this should be the same for each household member)
      if ("hhweight" %in% names(dt_HH)) {
        if (length(intersect(pweights, names(dt_HH))) > 0) {
          dt_HH[, intersect(names(dt), pweights) := NULL]
        }
        dt_HH[, pweight := hhweight]

      } else if (length(intersect(pweights, names(dt))) > 0) {
        for (weight in intersect(pweights, names(dt))) {
          dt_HH[, paste(weight) := get(paste0("hh_avg_", weight))]
        }
      } else {
        dt_HH[, pweight := 1]
        message(paste0("NID: ", unique(dt$nid), ". Does not have hhweight or pweight. Setting weights to 1."))
      }

    }

    ## Convert HH to HHM, if needed #########################
    # convert HH modules to HHM if they have hh_size
    # expand HH modules so that each row is an individual
    if (unique(dt$survey_module) == "HH" & "hh_size" %in% names(dt)) {

      # these are variables which should give us a unique household
      hh_collapse_vars <- intersect(c(collapse_vars,"hh_id","psu","strata"), names(dt))

      dt_HHM <- dt_HH[, cbind(1:hh_size, .SD), by = hh_collapse_vars][, -"V1"] # create a row for each household member, then drop the extra column this creates
      dt_HHM[, cv_HH := 0]

      if ("pweight" %ni% names(dt_HHM)) {
        dt_HHM[, pweight := hh_size]
        message(paste0("NID: ", unique(dt$nid), ". No pweight available; setting pweight to hh_size."))
      }
    }

    # now deal with HHM modules where we don't have hh_id
    if (unique(dt$survey_module) != "HH") {
      dt_HHM <- copy(dt)
      dt_HHM[, cv_HH := 0]
      if (length(intersect(pweights, names(dt_HHM))) < 1) {
        dt_HHM[, pweight := 1]
        message(paste0("NID: ", unique(dt$nid), ". HHM module does not have pweights. Setting to 1"))
      }
    }

    ## Combine HH and HHM #######################
    # bind together HH and HHM data tables


    if (exists("dt_HH") & exists("dt_HHM")) {
      dt <- rbind(dt_HH, dt_HHM, fill = TRUE)
    } else if (exists("dt_HH")) {
      dt <- dt_HH
    } else if (exists("dt_HHM")) {
      dt <- dt_HHM
    } else (stop(paste0("NID: ", unique(dt$nid), ". Something went terribly wrong with this extraction. UHOH!")))

    print("<<<<<")
    #--------------------------------------------------------------------------------------------------------
    # Generate indicators ##################
    ### generate indicators for modeling

    ## water #################
    if ("w_source_drink" %in% names(dt)) {

      # change to lowercase (w.strings is entirely lowercase)
      dt[, w_source_drink := tolower(w_source_drink)]

      # merge with w.strings
      dt <- merge(dt, w.strings[, .(nid, ihme_loc_id, year_start, w_source_drink, w.indicator)],
                  by = c("nid", "ihme_loc_id", "year_start", "w_source_drink"), all.x = TRUE)

      # some IPUMS sources only have "not_piped" and "piped_cw" as options - we want to exclude those
      if (dt[!is.na(w.indicator), paste(sort(unique(w.indicator)), collapse = "_")] == "not_piped_piped_cw") {
        message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                      "- only has not_piped and piped_cw as options for water source. Skipping."))
      } else {

        # mark any missing or unknown observations (to be dropped)
        dt[w.indicator %in% to.drop, water_keep := 0] # drop these
        dt[is.na(water_keep), water_keep := 1] # keep these

        # generate piped water variable
        dt[w.indicator %in% piped, wash_water_piped := 1] # all indicators mapped to piped
        dt[is.na(wash_water_piped) & water_keep == 1, wash_water_piped := 0] # all indicators not mapped to piped AND marked to be kept

        # generate improved water variable (denominator is population not using piped water)
        dt[w.indicator %in% w.improved, wash_water_imp_prop := 1] # all indicators mapped to improved
        dt[is.na(wash_water_imp_prop) & water_keep == 1 & wash_water_piped != 1, wash_water_imp_prop := 0] # all indicators not mapped to improved or piped AND marked to be kept

        # split ambiguous strings, if there are any
        # only use country-level data if there are >= 5 sources; otherwise use region-level
        if (any(unique(dt$w.indicator) %like% "cw")) {

          sources <- water.cw[iso3 == substr(unique(dt$ihme_loc_id), 1, 3), sources] # number of sources in country
          if (length(sources) == 0) stop(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                                         "- is not in the cw dataset."))

          if (sources >= 5 & unique(dt$ihme_loc_id) != "COL") { 
            water.cw <- water.cw[iso3 == substr(unique(dt$ihme_loc_id), 1, 3)] # subset to location

          } else if (sources < 5 | unique(dt$ihme_loc_id) == "COL") { # use region-level data

            region <- water.cw[iso3 == substr(unique(dt$ihme_loc_id), 1, 3), reg] # region that the country is in
            if (region == "oceania") region <- c("oceania","se_asia") # only two total sources in oceania region, so using se asia as well
            water.cw <- water.cw[reg %in% region] # subset to region

            # need to add up all the data within the region
            water.cw.cols <- c("imp","unimp","surface","spring_imp","spring_unimp","spring_cw","well_imp","well_unimp","well_cw","piped_imp","piped","piped_cw","bottled","not_piped")
            water.cw[, (water.cw.cols) := lapply(.SD, sum), .SDcols = water.cw.cols]
            water.cw <- unique(water.cw[, water.cw.cols, with = F])

          }

          # first create the ratios of improved to total within each category
          water.cw[, well_imp_prop := well_imp/(well_unimp + well_imp)] # proportion of total wells (improved + unimproved) in this location that are improved
          water.cw[, spring_imp_prop := spring_imp/(spring_unimp + spring_imp)] # ditto, for springs
          water.cw[, piped_imp_prop := piped_imp/(piped + piped_imp)] # proportion of total piped sources (improved + piped) in this location that are improved

           # then merge & replace variables with the relevant ratios
          dt <- cbind(dt, water.cw[, .(well_imp_prop, spring_imp_prop, piped_imp_prop)])
          dt[w.indicator == "well_cw", wash_water_imp_prop := well_imp_prop]
          dt[w.indicator == "spring_cw", wash_water_imp_prop := spring_imp_prop]
          dt[w.indicator == "piped_cw", wash_water_imp_prop := piped_imp_prop]
          dt[w.indicator == "piped_cw", wash_water_piped := 1 - piped_imp_prop]

        }
      }
    }
    print("<<<<<<")

    ## sanitation ############################
    if ("t_type" %in% names(dt)) {

      # change to lowercase (s.strings is entirely lowercase)
      dt[, t_type := tolower(t_type)]
      # edit problematic strings that are messing up the merges with s.strings
      if (unique(dt$nid) == 409558) dt[, t_type := gsub("â€™","’",t_type)] # this is for NID 409558

      # if source has both sewage and t_type, need to concatenate
      if ("sewage" %in% names(dt)) {
        setnames(dt, "t_type", "t_type_orig") # preserve a copy of the original t_type variable
        dt[, sewage := tolower(sewage)] # change to lowercase
        dt[!is.na(sewage) & !is.na(t_type_orig), t_type := paste(t_type_orig, sewage)] # concatenate
        dt[!is.na(sewage) & is.na(t_type_orig), t_type := sewage] # use sewage variable if t_type is missing
        dt[is.na(t_type), t_type := t_type_orig] # use original t_type if there's nothing to concatenate
      }

      # merge with s.strings
      dt <- merge(dt, s.strings[, .(nid, ihme_loc_id, year_start, t_type, s.indicator)],
                  by = c("nid", "ihme_loc_id", "year_start", "t_type"), all.x = TRUE)

      # some IPUMS sources only have "flush_cw" and "open" as options - we want to exclude those
      if (dt[!is.na(s.indicator), paste(sort(unique(s.indicator)), collapse = "_")] == "flush_cw_open") {
        message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                      "- only has flush_cw and open as options for toilet type. Skipping."))
      } else {

        # mark any missing or unknown observations (to be dropped)
        dt[s.indicator %in% to.drop, sanitation_keep := 0] # drop these
        dt[is.na(sanitation_keep), sanitation_keep := 1] # keep these

        # generate sewer/septic variable
        dt[s.indicator %in% sewer, wash_sanitation_piped := 1] # all indicators mapped to sewer/septic
        dt[is.na(wash_sanitation_piped) & sanitation_keep == 1, wash_sanitation_piped := 0] # all indicators not mapped to sewer/septic AND marked to be kept

        # generate improved sanitation variable (denominator is population not using sewer/septic sanitation)
        dt[s.indicator %in% s.improved, wash_sanitation_imp_prop := 1] # all indicators mapped to improved
        dt[is.na(wash_sanitation_imp_prop) & sanitation_keep == 1 & wash_sanitation_piped != 1, wash_sanitation_imp_prop := 0] # all indicators not mapped to improved or sewer/septic AND marked to be kept

        # split ambiguous strings, if there are any
        # only use country-level data if there are >= 5 sources; otherwise use region-level
        print("<<<<<<<")
        if (any(unique(dt$s.indicator) %like% "cw")) {

          sources <- sani.cw[iso3 == substr(unique(dt$ihme_loc_id), 1, 3), sources] # number of sources in country
          if (length(sources) == 0) stop(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                                         "- is not in the cw dataset."))

          if (sources >= 5) { # use country-specific data
            sani.cw <- sani.cw[iso3 == substr(unique(dt$ihme_loc_id), 1, 3)] # subset to country

          } else if (sources < 5) { # use region-level data

            region <- sani.cw[iso3 == substr(unique(dt$ihme_loc_id), 1, 3), reg] # region that the country is in
            if (region == "oceania") region <- c("oceania","se_asia") # only two total sources in oceania region, so using se asia as well
            sani.cw <- sani.cw[reg %in% region] # subset to region

            # need to add up all the data within the region
            sani.cw.cols <- c("imp","unimp","od","latrine_imp","latrine_unimp","latrine_cw","flush_imp","flush_unimp","flush_cw")
            sani.cw[, (sani.cw.cols) := lapply(.SD, sum), .SDcols = sani.cw.cols]
            sani.cw <- unique(sani.cw[, sani.cw.cols, with = F])

          }

          # first create the ratios of improved to total within each category
          sani.cw[, latrine_imp_prop := latrine_imp/(latrine_unimp + latrine_imp)] # proportion of total latrines (improved + unimproved) in this location that are improved
          sani.cw[, flush_imp_prop := flush_imp/(flush_unimp + flush_imp)] # ditto, for flush toilets
          
          # then merge & replace variables with the relevant ratios
          dt <- cbind(dt, sani.cw[, .(latrine_imp_prop, flush_imp_prop)])
          dt[s.indicator == "latrine_cw", wash_sanitation_imp_prop := latrine_imp_prop]
          dt[s.indicator == "flush_cw", wash_sanitation_piped := flush_imp_prop]

        }
      }
    }

    print("<<<<<<<<")
    ## handwashing ######################
    # wash_hwws == 0 means individual/household HAS access to handwashing facility
    # wash_hwws == 1 means NO ACCESS to handwashing facility (which is what we model)
    if (all(hw_general %in% names(dt))) {

      dt[is.na(hw_soap) & is.na(hw_water) & is.na(hw_station), hygiene_keep := 0] # drop if all observations missing
      dt[is.na(hygiene_keep), hygiene_keep := 1] # keep otherwise

      # generate no access to handwashing variable
      dt[hw_soap == 1 & hw_water == 1 & hw_station == 1, wash_hwws := 0]
      dt[is.na(wash_hwws) & hygiene_keep == 1, wash_hwws := 1]

    } else if (all(hw_acs %in% names(dt))) { # ACS has its own special soap variable

      dt[is.na(hw_soap_acs) & is.na(hw_water) & is.na(hw_station), hygiene_keep := 0] # drop if all observations missing
      dt[is.na(hygiene_keep), hygiene_keep := 1] # keep otherwise

      # generate no access to handwashing variable
      dt[hw_soap_acs == 1 & hw_water == 1 & hw_station == 1, wash_hwws := 0]
      dt[is.na(wash_hwws) & hygiene_keep == 1, wash_hwws := 1]

    } else if (all(hw_mics %in% names(dt))) { # MICS has its own special soap variable

      dt[is.na(hw_soap_mics) & is.na(hw_water) & is.na(hw_station), hygiene_keep := 0] # drop if all observations missing
      dt[is.na(hygiene_keep), hygiene_keep := 1] # keep otherwise

      # generate no access to handwashing variable
      dt[hw_soap_mics == 1 & hw_water == 1 & hw_station == 1, wash_hwws := 0]
      dt[is.na(wash_hwws) & hygiene_keep == 1, wash_hwws := 1]

    } else if (all(hw_pma %in% names(dt))) { # PMA has special soap & water variables

      dt[is.na(hw_soap_pma) & is.na(hw_water_pma) & is.na(hw_station), hygiene_keep := 0] # drop if all observations missing
      dt[is.na(hygiene_keep), hygiene_keep := 1] # keep otherwise

      # generate no access to handwashing variable
      dt[hw_soap_pma == 1 & hw_water_pma == 1 & hw_station == 1, wash_hwws := 0]
      dt[is.na(wash_hwws) & hygiene_keep == 1, wash_hwws := 1]

    } else {

      message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                    "- has no handwashing data. Skipping."))
    }

    print("<<<<<<<<<")
    ## household water treatment ######################
    # wash_no_treat == 1 if respondent doesn't use any of the relevant treatment methods (boil, filter, solar, bleach)
    # wash_no_treat == 0 if respondent uses one or more of the methods
    if (all(hwt_vars %in% names(dt))) { # only extract hwt if survey asks about all four methods

      # MICS is a little wonky; it has NAs for ALL responses that aren't "yes", i.e. "no" responses are coded as NA rather than 0
      # this is a problem because we want to drop all the true NAs, i.e. the missing responses
      # so, we have to manually change the false NAs ("no" responses) to 0
      if (unique(dt$survey_name) == "UNICEF_MICS") {
        # for respondents who answered "no" to "do you treat your water?"
        dt[w_treat == 0, `:=` (w_bleach = 0, w_boil = 0, w_filter = 0, w_solar = 0)]

        # for respondents who use either cloth or settling but not bleach/boil/filter/solar
        if (all(c(hwt_vars,"w_cloth","w_settle") %in% names(dt))) { # some surveys don't ask about cloth or settling, so they can be dealt with like other sources
          dt[(!is.na(w_cloth) | !is.na(w_settle)) & (is.na(w_bleach) & is.na(w_boil) & is.na(w_filter) & is.na(w_solar)),
             `:=` (w_bleach = 0, w_boil = 0, w_filter = 0, w_solar = 0)]
        }
      }

      # only non-MICS source that has NAs; needs to be edited like above
      if (unique(dt$nid) == 137351) {
        dt[!is.na(w_cloth) & (is.na(w_bleach) & is.na(w_boil) & is.na(w_filter) & is.na(w_solar)),
           `:=` (w_bleach = 0, w_boil = 0, w_filter = 0, w_solar = 0)]
      }

      # mark missing observations (to be dropped)
      dt[is.na(w_bleach) & is.na(w_boil) & is.na(w_filter) & is.na(w_solar), hwt_keep := 0] # drop these
      dt[is.na(hwt_keep), hwt_keep := 1] # keep these

      # generate water treatment variable
      dt[w_boil == 0 & w_filter == 0 & w_solar == 0 & w_bleach == 0, wash_no_treat := 1]
      dt[is.na(wash_no_treat) & hwt_keep == 1, wash_no_treat := 0]

      # generate boil/filter variable (proportion of those that treat with boil/filter/solar/bleach that use boil or filter)
      dt[w_boil == 1 | w_filter == 1, wash_filter_treat_prop := 1]
      dt[is.na(wash_filter_treat_prop) & wash_no_treat == 0, wash_filter_treat_prop := 0]

    } else {

      message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                    "- has no (or insufficient) HWT data. Skipping."))
    }
    } #DON'T RUN THIS LINE, IF RUNNING INTERACTIVELY ############################### 
    print("<<<<<<<<<<")

      ## ever emptied #######################
    if ("t_ever_emptied" %in% names(dt)) {

      # change to lowercase (ee.strings is entirely lowercase)
      dt[, t_ever_emptied := tolower(t_ever_emptied)]

      # merge with ee.strings
      dt <- merge(dt, ee.strings[, .(nid, ihme_loc_id, year_start, t_ever_emptied, ee.indicator)],
                  by = c("nid", "ihme_loc_id", "year_start", "t_ever_emptied"), all.x = TRUE)


        # mark any missing or unknown observations (to be dropped)
        dt[ee.indicator %in% to.drop, ee_keep := 0] # drop these
        dt[is.na(ee_keep), ee_keep := 1] # keep these

        # generate yes emptied variable
        dt[ee.indicator %in% ee_yes, wash_emptied_y := 1] #
        dt[is.na(wash_emptied_y) & ee_keep == 1, wash_emptied_y := 0] #

        # generate no emptied variable
        dt[ee.indicator %in% ee_no, wash_emptied_n := 1] #
        dt[is.na(wash_emptied_n) & ee_keep == 1, wash_emptied_n := 0] #

        # generate dk emptied variable
        dt[ee.indicator %in% ee_dk, wash_emptied_dk := 1] #
        dt[is.na(wash_emptied_dk) & ee_keep == 1, wash_emptied_dk := 0] #

    
        # split ambiguous strings ## this is where it would go if/when we go crosswalk route

    }else {

      message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                    "- has no (or insufficient) t_ever_emptied data. Skipping."))
    }
    print("<<<<<<<<<<<")
    ## emptied contents #####################

    if ("t_emptied_contents" %in% names(dt)) {

      # change to lowercase (ec.strings is entirely lowercase)
      dt[, t_emptied_contents := tolower(t_emptied_contents)]

      # merge with ec.strings
      dt <- merge(dt, ec.strings[, .(nid, ihme_loc_id, year_start, t_emptied_contents, ec.indicator)],
                  by = c("nid", "ihme_loc_id", "year_start", "t_emptied_contents"), all.x = TRUE)


      # mark any missing or unknown observations (to be dropped)
      dt[ec.indicator %in% to.drop.ec, ec_keep := 0] # drop these
      dt[is.na(ec_keep), ec_keep := 1] # keep these

      # generate emptied to treatment plant variable
      dt[ec.indicator %in% ec_tplant, wash_ec_tplant := 1] #
      dt[is.na(wash_ec_tplant) & ec_keep == 1, wash_ec_tplant := 0] #

      # generate emptied to buried pit
      dt[ec.indicator %in% ec_buriedpit, wash_ec_bpit := 1] #
      dt[is.na(wash_ec_bpit) & ec_keep == 1, wash_ec_bpit := 0] #

      # generate emptied to dont know
      dt[ec.indicator %in% ec_dk, wash_ec_dk := 1] #
      dt[is.na(wash_ec_dk) & ec_keep == 1, wash_ec_dk := 0] #

      # generate emptied to open area
      dt[ec.indicator %in% ec_open, wash_ec_open := 1] #
      dt[is.na(wash_ec_open ) & ec_keep == 1, wash_ec_open  := 0] #

      # generate emptied to other
      dt[ec.indicator %in% ec_other, wash_ec_oth := 1] #
      dt[is.na(wash_ec_oth ) & ec_keep == 1, wash_ec_oth  := 0] #



      # split ambiguous strings ## this is where it would go if/when we go crosswalk route



    }else {

      message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                    "- has no (or insufficient) t_emptied_contents data. Skipping."))
    }

    print("<<<<<<<<<<<<")

    ## shared sanitation ######################

    if ("shared_san" %in% names(dt)) {


        dt[is.na("shared_san"), ss_keep := 0] # drop if observations missing
        dt[is.na(ss_keep), ss_keep := 1] # keep otherwise

        # generate shared sanitation
        dt[shared_san == 1, wash_ss := 1]
        dt[shared_san == 0 & ss_keep == 1 , wash_ss := 0]


      } else {

        message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                      "- has no shared sanitation data. Skipping."))
      }

      #}
  #  }

    # Safely Managed Sanitation #######################################################################################
    # Start filtering the table to only include the responses that are safely managed
    # safely managed only counts if it is NOT shared, and either goes to a treatment plant or buried pit
    # Sewers also count if it goes to secondary treatment. We deal with this by bringing in wastewater treatment extractions that
    # already give the percentage of the wastewater that gets secondary treatment

    print (paste0("Starting safely managed section. Starting with ",nrow(dt)," rows"))
    
    if ("t_emptied_contents" %ni% names(dt) | "t_ever_emptied" %ni% names(dt) | "shared_san" %ni% names(dt)) {
      final<-dt
      message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                    "- does not have all of the following vars as needed for safely managed: 't_emptied_contents', 't_ever_emptied', 'shared_san.'  Skipping Safely Managed Calculations."))
    } else {
      
    #add an identifier column so we can use it to merge by later
    dt<-dt[,id:=1:nrow(dt)]

    ## Separate table ##################
    #Make a seperate table that is just the sanitation stuff
    #The psu column is your key column to the original table
    
    safe<-dt[,c("ihme_loc_id","year_start","t_emptied_contents","t_ever_emptied","t_type","shared_san","id","s.indicator","sanitation_keep","wash_sanitation_piped",
                "wash_sanitation_imp_prop","ee.indicator","ee_keep","wash_emptied_y","wash_emptied_n","wash_emptied_dk","ec.indicator","ec_keep","wash_ec_tplant",
                "wash_ec_bpit","wash_ec_dk","wash_ec_open","wash_ec_oth","ss_keep","wash_ss")]

    ## Shared Sanitation #################
    safe_ss<-safe[wash_ss==0,]
    print(paste0("removed all shared facilities. Only ",nrow(safe_ss)," rows left"))

    ## Toilet type #####################

    # Filter out only the ones that have an improved toilet

    #Make a variable that are the improved toilet types
    improv_t<-c("flush_imp","flush_imp_sewer","latrine_imp","flush_imp_septic", "imp", "sewer", "septic","latrine_cw","flush_cw")

    #Filter only improve toilets
    safe_tt<-safe_ss[s.indicator %in% improv_t,]
    print(paste0("Only keeping improved toilets. Only ",nrow(safe_tt)," rows left"))

    ## Empty contents ######################

    ### yes emptied subset ########
    # Make a subset that is just the ones that have emptied their toilets
    safe_ee_y<-safe_tt[ee.indicator=="yes"]

    ### Crosswalk ############
    #Now we need to crosswalk the dk and other columns

    #Make the known ones (treatment plant, buried pit and open) to be a proportion of 1
    safe_ee_y[wash_ec_tplant==1, wash_ec_tplant_prop:=1]
    safe_ee_y[wash_ec_bpit==1, wash_ec_bpit_prop:=1]
    safe_ee_y[wash_ec_open==1, wash_ec_open_prop:=1]

    #Crosswalk dk and other
    #Make variable for each proportion:

    #treatment plant proportion
    safe_ee_y[, tplant := sum(wash_ec_tplant_prop, na.rm = T) /
                (sum(wash_ec_tplant_prop, na.rm = T) + sum(wash_ec_bpit_prop, na.rm = T) + sum(wash_ec_open_prop,na.rm = T)), by = s.indicator]

    #buried pit proportion
    safe_ee_y[, bpit := sum(wash_ec_bpit_prop, na.rm = T) /
                (sum(wash_ec_tplant_prop, na.rm = T) + sum(wash_ec_bpit_prop, na.rm = T) + sum(wash_ec_open_prop,na.rm = T)), by = s.indicator]


    #open proportion
    safe_ee_y[, open := sum(wash_ec_open_prop, na.rm = T) /
                (sum(wash_ec_tplant_prop, na.rm = T) + sum(wash_ec_bpit_prop, na.rm = T) + sum(wash_ec_open_prop,na.rm = T)), by = s.indicator]

    #dk/other/NA crosswalk
    safe_ee_y[wash_ec_dk==1 | wash_ec_oth==1 | is.na(ec.indicator), wash_ec_tplant_prop := tplant]
    safe_ee_y[wash_ec_dk==1 | wash_ec_oth==1 | is.na(ec.indicator), wash_ec_bpit_prop   := bpit]
    safe_ee_y[wash_ec_dk==1 | wash_ec_oth==1 | is.na(ec.indicator), wash_ec_open_prop   := open]

    # Since a place may not have all of the treatment types, lets add in the prop column for each if they aren't already in there
    if ("wash_ec_tplant_prop" %ni% names(safe_ee_y)){
      safe_ee_y[,wash_ec_tplant_prop:=0]
      print("Having to add tplant prop column, wasn't in data")
    }

    if ("wash_ec_bpit_prop" %ni% names(safe_ee_y)){
      safe_ee_y[,wash_ec_bpit_prop:=0]
      print("Having to add bpit prop column, wasn't in data")
    }

    if ("wash_ec_open_prop" %ni% names(safe_ee_y)){
      safe_ee_y[,wash_ec_open_prop:=0]
      print("Having to add open prop column, wasn't in data")
    }

    #Make the safely managed treatments be 0, if they are NA
    safe_ee_y[is.na(wash_ec_tplant_prop)==T, wash_ec_tplant_prop := 0]
    safe_ee_y[is.na(wash_ec_bpit_prop)==T,   wash_ec_bpit_prop   := 0]
    safe_ee_y[is.na(wash_ec_open_prop)==T,   wash_ec_open_prop   := 0]
    
    # Make a table that is just the proportions based on the s.indicator so it can be used to crosswalk the no's and dk's
    proportions<-unique(safe_ee_y[wash_ec_tplant_prop!=1 | wash_ec_bpit_prop!=1 | wash_ec_open_prop!=1,.(s.indicator,tplant,bpit,open)])

    #### Merge with original ######
    #Merge the ee yes subset back into the original safely managed subset

    # Make the ee_yes subset only the columns that we need
    safe_ee_y_small <- safe_ee_y[, .(id,wash_ec_tplant_prop,wash_ec_bpit_prop,wash_ec_open_prop)]

    # Merge
    safe_ec <- merge(safe_tt, safe_ee_y_small, by = "id", all = T)

    ## Crosswalk emptied contents #######################################
    #Merge the proportions table into the safe_ec table based on s.indicator so we can use those for crosswalking
    safe_ec<-merge(safe_ec,proportions,by="s.indicator",all.x=T)

    #Crosswalk the no's/dk's/NA's for the ever emptied

      safe_ec[ee.indicator=="no" | ee.indicator=="dk" | (is.na(ec.indicator) & is.na(wash_ec_tplant_prop)), wash_ec_tplant_prop := tplant]
      safe_ec[ee.indicator=="no" | ee.indicator=="dk" | (is.na(ec.indicator) & is.na(wash_ec_bpit_prop)), wash_ec_bpit_prop := bpit]
      safe_ec[ee.indicator=="no" | ee.indicator=="dk" | (is.na(ec.indicator) & is.na(wash_ec_open_prop)), wash_ec_open_prop := open]
      
      
      #identify s.indicators that are not in the proportions table
      prop_s<-proportions$s.indicator
      safe_ec_s<-unique(safe_ec$s.indicator)
      
      safe_ec_s<-safe_ec_s[safe_ec_s %ni% prop_s]
      
    # crosswalk the s.indicators that are not in the proportions table
      
      #Make variable for the average of each type
      tplant_total<- sum(safe_ec$wash_ec_tplant_prop, na.rm = T) /
                (sum(safe_ec$wash_ec_tplant_prop, na.rm = T) + sum(safe_ec$wash_ec_bpit_prop, na.rm = T) + sum(safe_ec$wash_ec_open_prop,na.rm = T))
      
      bpit_total<- sum(safe_ec$wash_ec_bpit_prop, na.rm = T) /
        (sum(safe_ec$wash_ec_tplant_prop, na.rm = T) + sum(safe_ec$wash_ec_bpit_prop, na.rm = T) + sum(safe_ec$wash_ec_open_prop,na.rm = T))
      
      open_total<- sum(safe_ec$wash_ec_open_prop, na.rm = T) /
        (sum(safe_ec$wash_ec_tplant_prop, na.rm = T) + sum(safe_ec$wash_ec_bpit_prop, na.rm = T) + sum(safe_ec$wash_ec_open_prop,na.rm = T))
      
      #crosswalk
      safe_ec[s.indicator %in% safe_ec_s,wash_ec_tplant_prop := tplant_total]
      
      safe_ec[s.indicator %in% safe_ec_s,wash_ec_bpit_prop := bpit_total]
      
      safe_ec[s.indicator %in% safe_ec_s,wash_ec_open_prop := open_total]
    
    # Since a place may not have all of the treatment types, lets add in the prop column for each if they aren't already in there
    if ("wash_ec_tplant_prop" %ni% names(safe_ec)){
      safe_ec[,wash_ec_tplant_prop:=0]
      print("Having to add tplant prop column, wasn't in data")
    }

    if ("wash_ec_bpit_prop" %ni% names(safe_ec)){
      safe_ec[,wash_ec_bpit_prop:=0]
      print("Having to add bpit prop column, wasn't in data")
    }

    if ("wash_ec_open_prop" %ni% names(safe_ec)){
      safe_ec[,wash_ec_open_prop:=0]
      print("Having to add open prop column, wasn't in data")
    }

    #Make the safely managed treatments be 0, if they are NA
    safe_ec$wash_ec_tplant_prop[is.na(safe_ec$wash_ec_tplant_prop)]<-0

    safe_ec$wash_ec_bpit_prop[is.na(safe_ec$wash_ec_bpit_prop)]<-0

    safe_ec$wash_ec_open_prop[is.na(safe_ec$wash_ec_open_prop)]<-0

    ## Safely disposed column #####################
    # Add a column that identifies it as safely disposed
    #Since we only needed to crosswalk the emptied contents, we only need to add the proportions of the safely managed treatments to get the
    # safely disposed proportion
    safe_sm<-safe_ec[,safe_disposal:=(wash_ec_tplant_prop + wash_ec_bpit_prop)]

    print("add safely disposed column")
    
    ## Safely Managed column #####################
    #For crosswalked sanitations (s.indicator== latrine_cw and flush_cw) safely managed is:
      #latrine_cw: (shared sanitation * wash_sanitation_imp_prop * safely disposed)
      #flush_cw: (shared sanitation * wash_sanitation_piped * safely disposed)
    #for known sanitation types: safely managed is just safely disposed
    
    #for the cw calculation to work, we first need to flip the number of shared sanitation so that non-shared sanitation = 1
    safe_sm$ss_prop<-ifelse(safe_sm$shared_san==0,1,0)
    
    #now safely managed sanitation
    safe_sm[s.indicator=="latrine_cw",safely_managed:=(ss_prop*wash_sanitation_imp_prop*safe_disposal)]
    safe_sm[s.indicator=="flush_cw",safely_managed:=(ss_prop*wash_sanitation_piped*safe_disposal)]
    
    safe_sm[s.indicator %ni% c("flush_cw","latrine_cw"),safely_managed:=safe_disposal]

    # Merge with original ########################
    # Only keep the new columns and the id column (going to merge based on this column)
    safe_final<-safe_sm[,c("id","wash_ec_tplant_prop","wash_ec_bpit_prop","wash_ec_open_prop","safe_disposal","safely_managed")]
    print("remove all unnesscessary columns right before merge")

    # Merge safely managed with whole table
   final<-merge(dt,safe_final,by="id",all = T)
    print(paste0("Merging safely managed with original. Number of rows for final: ",nrow(final),". Number of rows in original: ",nrow(dt), ". They should
                 be the same."))
    #  dt<-merge(dt,safe_sm,all=T)
    
    #If safely managed is NA, make it 0
    final<-final[is.na(safely_managed),safely_managed:=0]
    }
    #--------------------------------------------------------------------------------------------------------
    # Log ##########################################
    ### write to log
    print("updating log")
    log_new <- unique(final[, .(file_path, ihme_loc_id, nid, survey_module, survey_name, year_start)])
    log_new[, prep_date := as.character(Sys.Date())]
    log_new[, file_name := file]
    log_new[, reprep := 0]
    # if this source has no usable data, make a note of it
    if (all(c("wash_water_imp_prop","wash_water_piped","wash_sanitation_imp_prop","wash_sanitation_piped",  ##didnt add ee,ec, and ss variables here because theyre not variables that we typically NEED to have but can add for this but can add for these specific extractions
              "wash_hwws","wash_no_treat","wash_filter_treat_prop") %ni% names(final))) {
      log_new[, notes := "source has no usable data, prepped dataset not saved"]
    }
    
    source_log <- fread(paste0("FILEPATH/b_prepped_log_2022.csv"))
    source_log[, prep_date := as.character(prep_date)]
    source_log <- rbind(source_log, log_new, fill = TRUE)
    write.csv(source_log, paste0("FILEPATH/b_prepped_log_2022.csv"), row.names = FALSE) 
    
    # Save prepped data ###################################

    if (all(c("wash_water_imp_prop","wash_water_piped","wash_sanitation_imp_prop","wash_sanitation_piped",
              "wash_hwws","wash_no_treat","wash_filter_treat_prop") %ni% names(final))) {
      
      message(paste("The current source - NID", unique(final$nid), "ihme_loc_id", unique(final$ihme_loc_id), "year", unique(final$year_start),
                    "- has no usable data and will not be saved."))
    } else {
      
      print("saving final CSV")
      write.csv(final, file.path(out.dir, file), row.names = FALSE)
      print("saving sewer CSV")
      write.csv(final[s.indicator %in% sewer], file.path(sewer.out.dir, file), row.names=F)
      print("saving imp CSV")
      write.csv(final[s.indicator %in% s.improved], file.path(imp.out.dir, file), row.names=F)
      
    }
    
    message(paste("hooray! NID", unique(final$nid), "finished"))
    print("<<<<<<<<<<<<<")
    }