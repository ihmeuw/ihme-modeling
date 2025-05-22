# prep for write_cw.R

# CONFIG ##################################################################################
#Clean out list
rm(list=ls())

## libraries & functions
library(data.table)
library(magrittr)
library(haven)
library(readstata13)

"%unlike%" <- Negate("%like%")
"%ni%" <- Negate("%in%")

# ADD NEW DIRECTORIES HERE #########################################
# non limited use surveys
dir_19<-"FILEPATH"
dir_20 <- "FILEPATH"
dir_21 <- "FILEPATH"
dir_22a <- "FILEPATH"
dir_22b <- "FILEPATH"

# limited use surveys
l.dir_20 <- "FILEPATHt"
l.dir_21 <- "FILEPATH"
l.dir_22a <- "FILEPATH"
l.dir_22b <- "FILEPATH"

#columns to keep
cols <- c("nid","ihme_loc_id","year_start","year_end","survey_module","survey_name","missing_t_type","missing_w_source_drink",
          "t_type","s.indicator","w_source_drink","w.indicator")

# Prep Function ###############################################################################
prep_cw <- function(file) {

  # ADD NEW DIRECTORIES HERE #########################################
  # non limited use surveys
  if (file %like% dir_19) name <- gsub(paste0(dir_19, "/"), "", file)
  if (file %like% dir_20) name <- gsub(paste0(dir_20, "/"), "", file)
  if (file %like% dir_21) name <- gsub(paste0(dir_21, "/"), "", file)
  if (file %like% dir_22a) name <- gsub(paste0(dir_22a, "/"), "", file)
  if (file %like% dir_22b) name <- gsub(paste0(dir_22b, "/"), "", file)
  
  # limited use surveys
  if (file %like% l.dir_20) name <- gsub(paste0(l.dir_20, "/"), "", file)
  if (file %like% l.dir_21) name <- gsub(paste0(l.dir_21, "/"), "", file)
  if (file %like% l.dir_22a) name <- gsub(paste0(l.dir_22a, "/"), "", file)
  if (file %like% l.dir_22b) name <- gsub(paste0(l.dir_22b, "/"), "", file)

  print(name)
  

  #--------------------------------------------------------------------------------------------------------
  ### read in files

  ## extraction sheet
  ## specify encoding to Latin-1 in order to deal with special characters
  
  
  if (grepl(".dta",file)==T) {
    dt <- read_dta(file, encoding = "latin1")
    setDT(dt)
  } else {
    dt <- fread(file, encoding = "Latin-1")
  }
  
  print("read in file")
  
 
  dt[dt == ""] <- NA # replace empty strings with NA
  print("replaced emtpty strings")

  ## string mapping spreadsheets (collab with LBD)
  w.strings <- fread("FILEPATH/w_source_defined_by_nid.csv") # water string mapping
  w.strings[w.strings == ""] <- NA # replace empty strings with NA
  setnames(w.strings, c("sdg","iso3"), c("w.indicator","ihme_loc_id"))

  s.strings <- fread("FILEPATH/t_type_defined_by_nid.csv") # sanitation string mapping
  s.strings[s.strings == ""] <- NA # replace empty strings with NA
  setnames(s.strings, c("sdg","iso3"), c("s.indicator","ihme_loc_id"))

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
  ### latest string matching doc is at FILEPATH
  # missing observations
  to.drop <- c(""," ","other","unknown","not_piped",NA,NaN)

  # water
  piped <- c("piped")
  w.improved <- c("bottled","imp","piped_imp","spring_imp","well_imp","improved")
  w.cw <- c("well_cw","spring_cw","piped_cw")
  w.unimproved <- c("spring_unimp","well_unimp","surface","unimp")

  # sanitation
  sewer <- c("septic","sewer","flush_imp_septic","flush_imp_sewer","flush_imp")
  s.improved <- c("imp","latrine_imp")
  s.cw <- c("flush_cw","latrine_cw")
  s.unimproved <- c("flush_unimp","latrine_unimp","unimp","open")

  #--------------------------------------------------------------------------------------------------------
  ### validation checks for extraction completeness

  ## if source is a HH module, check to make sure that hhweight variable is present (sometimes hhweight gets codebooked into the pweight variable instead)
  ## if not, individual-level estimates and household-level estimates will be identical
  if (unique(dt$survey_module) == "HH" & "hhweight" %ni% names(dt)) {
    stop(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
               "- is a HH module but does not have hhweight. Please re-extract!"))
  }
  print("val 1 done")

  ## if source is a HHM module, we need hh_id in order to identify unique households so that we can generate a dataset where each row is a HH
  if (unique(dt$survey_module) != "HH" & "hh_id" %ni% names(dt)){
    stop(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
               "- is a HHM module but does not have hh_id. Please re-extract!"))
  }
  print("val 2 done")

  ## if source is a HH module, we need hh_size in order to expand households so that we can generate a datset where each row is a HHM
  if (unique(dt$survey_module) == "HH" & "hh_size" %ni% names(dt)) {
    stop(paste("The current source - NID", unique(dt$nid), "ihme_loc_id", unique(dt$ihme_loc_id), "year", unique(dt$year_start),
               "- is a HH module but does not have hh_size. Please re-extract!"))
  }
  print("val 3 done")

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
print("generating missingness")
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

    print("replacing missing hh_size")
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

    # for HH surveys, we should be using hhweights instead of pweights but the collapse code only takes pweights
    if (unique(dt$survey_module) == "HH") {

      # first choice is to use hhweight, second choice is to use average household pweight (this should be the same for each household member)
      if ("hhweight" %in% names(dt)) {
        if (length(intersect(pweights, names(dt))) > 0) {
          dt[, intersect(names(dt), pweights) := NULL]
        }
        dt[, pweight := hhweight]

      } else {
        dt_HH[, pweight := 1]
        message(paste0("NID: ", unique(dt$nid), ". Does not have hhweight or pweight. Setting weights to 1."))
      }

    }
print("hh to hhm conversion")
    # convert HH modules to HHM if they have hh_size
    # expand out HH module rather than scaling pweight
    if (unique(dt$survey_module) == "HH" & "hh_size" %in% names(dt)) {

      # these are variables which should give us a unique household
      hh_collapse_vars <- intersect(c(collapse_vars,"hh_id","psu","strata"), names(dt))

      dt <- dt[, cbind(1:hh_size, .SD), by = hh_collapse_vars][, -"V1"] # create a row for each household member, then drop the extra column this creates

      if ("pweight" %ni% names(dt)) {
        dt[, pweight := hh_size]
        message(paste0("NID: ", unique(dt$nid), ". No pweight available; setting pweight to hh_size."))
      }
    }

    # now deal with HHM modules where we don't have hh_id
    if (unique(dt$survey_module) == "HHM") {
      if (length(intersect(pweights, names(dt))) < 1) {
        dt[, pweight := 1]
        message(paste0("NID: ", unique(dt$nid), ". HHM module does not have pweights. Setting to 1"))
      }
    }

    #--------------------------------------------------------------------------------------------------------
    ### merge on mapped strings
print("mapping water")
    ## water
    if ("w_source_drink" %in% names(dt)) {

      # change to lowercase (w.strings is entirely lowercase)
      dt[, w_source_drink := tolower(w_source_drink)]

      # merge with w.strings
      dt <- merge(dt, w.strings[, .(nid, ihme_loc_id, year_start, w_source_drink, w.indicator)],
                  by = c("nid", "ihme_loc_id", "year_start", "w_source_drink"), all.x = TRUE)

    }
print("mapping sanitation")
    ## sanitation
    if ("t_type" %in% names(dt)) {

      # change to lowercase (s.strings is entirely lowercase)
      dt[, t_type := tolower(t_type)]

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

    }
  }

  keep.cols <- intersect(names(dt), cols)
  dt <- dt[, keep.cols, with = F]
  nid_list<-list()

  return(dt)
  
  
  
  print("finished!")
}
