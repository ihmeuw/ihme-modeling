### Author: [AUTHOR]
### Date: [DATE]
### Purpose: Custom code to prep WaSH extractions for collapse
#####################################################################

## libraries & functions
library(data.table)
library(magrittr)

"%ni%" <- Negate("%in%")

## prep wash data and save to directory
prep_and_save <- function(file, limited_use) {
  
  print(file)

  ## directories
  if (limited_use == T) {
    in.dir <- "FILEPATH"
    out.dir <- ifelse(grepl("CENSUS", file),
                      "FILEPATH", # censuses need to be collapsed separately
                      file.path("FILEPATH", Sys.Date()))
  } else {
    in.dir <- "FILEPATH"
    out.dir <- ifelse(grepl("CENSUS", file),
                      "FILEPATH", # censuses need to be collapsed separately
                      file.path("FILEPATH", Sys.Date()))

  }

  if (!dir.exists(out.dir)) dir.create(out.dir, recursive = TRUE)
  
  #--------------------------------------------------------------------------------------------------------
  ### read in files
  
  ## extraction sheet
  ## specify encoding to Latin-1 in order to deal with special characters
  dt <- fread(file.path(in.dir, file), encoding = "Latin-1")
  dt[dt == ""] <- NA # replace empty strings with NA
  
  ## string mapping spreadsheets (collab with LBD)
  w.strings <- fread("FILEPATH") # water string mapping
  w.strings[w.strings == ""] <- NA # replace empty strings with NA
  setnames(w.strings, c("sdg","iso3"), c("w.indicator","ihme_loc_id"))
  
  s.strings <- fread("FILEPATH") # sanitation string mapping
  s.strings[s.strings == ""] <- NA # replace empty strings with NA
  setnames(s.strings, c("sdg","iso3"), c("s.indicator","ihme_loc_id"))
  
  ## datasets used to split ambiguous strings
  water.cw <- rbindlist(lapply(c("FILEPATH", # LBD dataset
                                 "FILEPATH"), # GBD dataset (countries not in LBD)
                               fread), fill = T)
  sani.cw <- rbindlist(lapply(c("FILEPATH", # LBD dataset
                                "FILEPATH"), # GBD dataset (countries not in LBD)
                              fread), fill = T)
    
  #--------------------------------------------------------------------------------------------------------
  ### settings
  
  ## these are the pweights used in collapse code
  pweights <- c("pweight","pweight_admin_1","pweight_admin_2","pweight_admin_3")
  ## these are the variables for which there might be missingness; if so, need to create a missingness indicator
  # if there is >15% missingness in pweight, exclude the source entirely
  # if >15% missingness in a particular variable, exclude the indicators related to it (e.g. t_type --> wash_sanitation_piped & wash_sanitation_imp_prop)
  missing_vars <- c("strata","psu","pweight","hh_size","hhweight",
                    "t_type","w_source_drink","w_treat","hw_station")
  
  ## indicators
  ### latest string matching doc is at FILEPATH\WaSH String Matching Documentation_2020_01_29.docx
  # missing observations
  to.drop <- c(""," ","other","unknown",NA,NaN)
  
  # water
  piped <- c("piped")
  w.improved <- c("bottled","imp","piped_imp","spring_imp","well_imp","improved")
  w.cw <- c("well_cw","spring_cw","piped_cw")
  w.unimproved <- c("spring_unimp","well_unimp","surface","unimp","not_piped")
  
  # sanitation
  sewer <- c("septic","sewer","flush_imp_septic","flush_imp_sewer","flush_imp")
  s.improved <- c("imp","latrine_imp")
  s.cw <- c("flush_cw","latrine_cw")
  s.unimproved <- c("flush_unimp","latrine_unimp","unimp","open")
  
  # handwashing
  hw_general <- c("hw_soap","hw_water","hw_station")
  hw_acs <- c("hw_soap_acs","hw_water","hw_station")
  hw_mics <- c("hw_soap_mics","hw_water","hw_station")
  hw_pma <- c("hw_soap_pma","hw_water_pma","hw_station")
  
  # household water treatment
  hwt_vars <- c("w_bleach","w_boil","w_filter","w_solar")
  
  #--------------------------------------------------------------------------------------------------------
  ### validation checks for extraction completeness
  
  ## if source is a HH module, check to make sure that hhweight variable is present (sometimes hhweight gets codebooked into the pweight variable instead)
  ## if not, individual-level estimates and household-level estimates will be identical - unclear why this happens but we don't want that, since there are a different number of people and households
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
  
  if (current_source %ni% check & any(c("w_source_drink","t_type") %in% names(dt))) {
    message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
               "- has water and/or sanitation data but is not in either of the string mapping spreadsheets. Skipping it now - please run this script after it has been added."))
  } else {
    
    #--------------------------------------------------------------------------------------------------------
    ### prep dataset for both household and household member collapse
    
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
    
    # bind together HH and HHM data tables
    # the central ubcov collapse code will create a collapse for both households and individuals
    if (exists("dt_HH") & exists("dt_HHM")) {
      dt <- rbind(dt_HH, dt_HHM, fill = TRUE)
    } else if (exists("dt_HH")) {                             
      dt <- dt_HH
    } else if (exists("dt_HHM")) {
      dt <- dt_HHM
    } else (stop(paste0("NID: ", unique(dt$nid), ". Something went terribly wrong with this extraction. UHOH!")))
    
    #--------------------------------------------------------------------------------------------------------
    ### generate indicators for modeling
    
    ## water
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
          if (length(sources) == 0) stop("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                                         "- is not in the cw dataset.")
          
          if (sources >= 5 & unique(dt$ihme_loc_id) != "COL") { # use country-specific data unless it's Colombia... i don't trust the Colombia ratios, using region (S America) instead
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
          water.cw[, well_imp_ratio := well_imp/(well_unimp + well_imp)] # proportion of total wells (improved + unimproved) in this location that are improved
          water.cw[, spring_imp_ratio := spring_imp/(spring_unimp + spring_imp)] # ditto, for springs
          water.cw[, piped_imp_ratio := piped_imp/(piped + piped_imp)] # proportion of total piped sources (improved + piped) in this location that are improved
          # then merge & replace variables with the relevant ratios
          dt <- cbind(dt, water.cw[, .(well_imp_ratio, spring_imp_ratio, piped_imp_ratio)])
          dt[w.indicator == "well_cw", wash_water_imp_prop := well_imp_ratio]
          dt[w.indicator == "spring_cw", wash_water_imp_prop := spring_imp_ratio]
          dt[w.indicator == "piped_cw", wash_water_imp_prop := piped_imp_ratio]
          dt[w.indicator == "piped_cw", wash_water_piped := 1 - piped_imp_ratio]
          
        }
      }
    }
    
    ## sanitation
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
        if (any(unique(dt$s.indicator) %like% "cw")) {
          
          sources <- sani.cw[iso3 == substr(unique(dt$ihme_loc_id), 1, 3), sources] # number of sources in country
          if (length(sources) == 0) stop("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                                         "- is not in the cw dataset.")
          
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
          sani.cw[, latrine_imp_ratio := latrine_imp/(latrine_unimp + latrine_imp)] # proportion of total latrines (improved + unimproved) in this location that are improved
          sani.cw[, flush_imp_ratio := flush_imp/(flush_unimp + flush_imp)] # ditto, for flush toilets
          # then merge & replace variables with the relevant ratios
          dt <- cbind(dt, sani.cw[, .(latrine_imp_ratio, flush_imp_ratio)])
          dt[s.indicator == "latrine_cw", wash_sanitation_imp_prop := latrine_imp_ratio]
          dt[s.indicator == "flush_cw", wash_sanitation_piped := flush_imp_ratio]
          
        }
      }
    }
    
    ## handwashing
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
    
    ## household water treatment
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
    
    #--------------------------------------------------------------------------------------------------------
    ### write to log
    log_new <- unique(dt[, .(file_path, ihme_loc_id, nid, survey_module, survey_name, year_start)])
    log_new[, prep_date := as.character(Sys.Date())]
    log_new[, file_name := file]
    log_new[, reprep := 0]
    # if this source has no usable data, make a note of it
    if (all(c("wash_water_imp_prop","wash_water_piped","wash_sanitation_imp_prop","wash_sanitation_piped",
              "wash_hwws","wash_no_treat","wash_filter_treat_prop") %ni% names(dt))) {
      log_new[, notes := "source has no usable data, prepped dataset not saved"]
    }
    
    source_log <- fread("FILEPATH")
    source_log <- rbind(source_log, log_new, fill = TRUE)
    write.csv(source_log, "FILEPATH", row.names = FALSE)

    ### save prepped output (only if there is any usable data)
    if (all(c("wash_water_imp_prop","wash_water_piped","wash_sanitation_imp_prop","wash_sanitation_piped",
              "wash_hwws","wash_no_treat","wash_filter_treat_prop") %ni% names(dt))) {
      
      message(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
                    "- has no usable data and will not be saved."))
    } else {
      
      write.csv(dt, file.path(out.dir, file), row.names = FALSE)
      
    }
    
    message(paste("hooray! NID", unique(dt$nid), "finished"))
    
  }
}