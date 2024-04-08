#################################################################
##                                                             ##
## Description: Functions to pull each type of stillbirth data ##
##                                                             ##
#################################################################

# GET VR DATA (DYB, WHO_HFA)

# returns a data table containing all output from extracted VR data
getVrOutput = function() {
  # get list of output files. They all start with 'USABLE'
  csv_files = list.files(path = "FILEPATH",
                         pattern = "usable",
                         full.names = T,
                         recursive = F)

  # open and rbind all csv files together
  vr_data = rbindlist(
    lapply(csv_files, FUN = function(filepath) {
      data = fread(filepath)
      data[, source_file := filepath]
      return(data)
    }), use.names = T, fill = T)

  return(vr_data)
}

# GET SCIENTIFIC LITERATURE DATA

getLitOutput = function(gbd_round_id = 7, decomp_step = "iterative") {
  csv_files = list.files(path = paste0(root, "FILEPATH"),
                         full.names = T,
                         recursive = F)

  # open and rbind all csv files together
  sci_lit_data = rbindlist(
    lapply(csv_files,FUN = function(filepath) {
      data = fread(filepath)
      data[, source_file := filepath]
      return(data)
    }), use.names = T, fill = T)

  bundle_data_orig <- get_bundle_data(1088)

  bundle_data <- as.data.frame(bundle_data_orig)
  index <- map_lgl(bundle_data, ~ all(is.na(.)))
  bundle_data <- bundle_data[, !index]
  bundle_data <- as.data.table(bundle_data)

  bundle_data[nid %in% c(146854, 256163), ":=" (mean = NA, lower = NA, upper = NA)] # bundle upload validations required these columns

  bundle_data[, data_type := "literature"]

  bundle_data[, age_group_id := 22]

  bundle_data[sex == "Male", sex_id := 1]
  bundle_data[sex == "Female", sex_id := 2]
  bundle_data[sex == "Both", sex_id := 3]

  bundle_data <- bundle_data[sex_id == 3]

  bundle_data[year_start == year_end, year_id := year_start]
  bundle_data[year_start + 1 == year_end, year_id := year_end]
  bundle_data[is.na(year_id), year_id := round(((year_start + year_end)/2), 0)]
  bundle_data[, year := year_id]

  setnames(bundle_data, "sb_definition", "definition")
  setnames(bundle_data, "sample_size", "total_births")

  bundle_data[is.na(stillbirths) & !is.na(cases), stillbirths := cases]

  bundle_data[is.na(total_births) & !is.na(livebirths) & !is.na(stillbirths), total_births := livebirths + stillbirths]
  bundle_data[is.na(stillbirths) & !is.na(livebirths) & !is.na(total_births), stillbirths := total_births - livebirths]
  bundle_data[is.na(livebirths) & !is.na(stillbirths) & !is.na(total_births), livebirths := total_births - stillbirths]

  bundle_data[, sbr := mean * 1000]
  bundle_data[is.na(sbr) & !is.na(stillbirths) & !is.na(total_births), sbr := stillbirths/total_births]

  bundle_data <- bundle_data[, c("nid", "data_type", "location_id", "ihme_loc_id",
                                 "year_id", "year", "sex_id", "age_group_id",
                                 "sbr", "lower", "upper", "total_births",
                                 "stillbirths", "livebirths", "definition",
                                 "sb_inclusion_ga", "sb_inclusion_bw",
                                 "sb_inclusion_preference", "representative_name")]

  # Deduplicate old extractions (but keeps nids we haven't extracted this round)
  sci_lit_data_keep <- sci_lit_data[!(nid %in% bundle_data$nid)]

  sci_lit_data_final <- rbind(bundle_data, sci_lit_data_keep, fill = T)

  return(sci_lit_data_final)
}

# GET SURVEY MICRODATA FROM UBCOV

getUbCovOutput = function() {

  sbhfiles <- list.files(paste0("FILEPATH"), pattern = '.dta', full.names = T, recursive = T)
  # sbhfiles <- list.files(paste0("FILEPATH"), pattern = '.dta', full.names = T, recursive = T)

  sbh_dhs_calculated <- data.table()

  for (file in sbhfiles) {

    sbh_file <- as.data.table(haven::read_dta(file))

    if ("stillbirths_calculated_5yr" %in% colnames(sbh_file)) {

      print(paste0("DHS Calculated Stillbirth Data, ", file))

      # If there are subnationals, aggregate by the subnational ID instead of the national one
      sbh_file$country <- sbh_file$ihme_loc_id

      # Grabs urban / rural split, as well as subnational / national split
      if ("admin_1_urban_id" %in% colnames(sbh_file)) {

        subnational <- sbh_file[admin_1_urban_id != ""]
        subnational$ihme_loc_id <- subnational$admin_1_urban_id

        sbh_file <- rbind(sbh_file, subnational)

        subnational <- sbh_file[admin_1_id != ""]
        subnational$ihme_loc_id <- subnational$admin_1_id

        sbh_file <- rbind(sbh_file, subnational)

      } else if ("admin_1_id" %in% colnames(sbh_file)) {

        subnational <- sbh_file[admin_1_id != ""]
        subnational$ihme_loc_id <- subnational$admin_1_id

        sbh_file <- rbind(sbh_file, subnational)
      }

      # Reassign year
      sbh_file[, year_id := year_start]

      # Aggregate and bind
      sbh_file2 <- aggregate(cbind(stillbirths_calculated_5yr, births_calculated_5yr) ~ nid + year_id + ihme_loc_id + country + survey_name + stillbirth_definition,
                             data = sbh_file, FUN = sum)

      sbh_dhs_calculated <- rbind(sbh_file2, sbh_dhs_calculated)

    }

    # sbh <- rbind(sbh_dhs_calculated, sbh_numeric, fill = T)
    sbh <- sbh_dhs_calculated # only keeping dhs contraceptive calendar data until
                              # we implement a method to split lifetime stillbirths

  }

  return(sbh)
}

# ADD ON MORTALITY DATA

mergeMortalityRatesOnSurveyData = function(stillbirth_data, mortality_file, datatype) {
  # stillbirth_data:
  #   type: data.table
  #   desc: object containing stillbirth data
  # mortality_file :
  #   type: character
  #   desc: full filepath of to file containing mortality estimates by source
  # datatype:
  #   type: character
  #   desc: abbreviated names of survey, i.e. "DHS", "MICS", "MIS"

  ##### functions #####

  # find the closest corresponding sbr year given the target year.
  # return the year of the closest matching sbr entry.
  smallestDiffSbr = function(mr_year, loc_id, survey_year) {
    subset_sbr = output[ihme_loc_id == loc_id & svy_year == survey_year,]
    if (nrow(subset_sbr) == 0) {
      return(NA)
    }
    subset_sbr[, diff := abs(year - mr_year)]
    year = subset_sbr[which.min(diff), year]
    return(year)
  }

  # find the closest corresponding mortality year given the target year.
  # return the year of the closest matching mortality entry.
  smallestDiffMort = function(sbr_year, loc_id, survey_year) {
    subset_mr = mr[ihme_loc_id == loc_id & year <= max(output[, year]) & svy_year == survey_year,]
    if (nrow(subset_mr) == 0) {
      return(NA)
    }
    subset_mr[, diff := abs(mort_year - sbr_year)]
    year = subset_mr[which.min(diff), mort_year]
    return(year)
  }

  ##### main #####

  output = copy(stillbirth_data)
  mr = as.data.table(read.dta(mortality_file))
  mr = mr[sex == "both", c("source", "ihme_loc_id", "year", "q_enn", "q_nn", "exclude")]
  mr = mr[grep(datatype, mr$source, ignore.case = T),] # only keep surveys that match datatype

  # Get survey year from mortality data
  mr[, svy_year := sapply(source, function(x) {
    unlist(regmatches(x, gregexpr("([0-9]{4})", x, perl=T)))[1]
  })]

  mr[, svy_year := as.numeric(svy_year)]
  mr[, source := NULL]

  setnames(mr, "year", "mort_year")
  mr[, year := mapply(FUN = smallestDiffSbr, mr_year = mort_year, loc_id = ihme_loc_id, survey_year = svy_year)]
  output[, mort_year := mapply(FUN = smallestDiffMort, loc_id = ihme_loc_id, survey_year = svy_year, sbr_year = year)]

  if (length(mr[!is.na(year),year]) == 0) {
    output[, early_neonatal_mr := NA]
    output[, neonatal_mr := NA]
  } else {
    mr[, svy_year := NULL]
    output = merge(output, mr, by.x = c("year", "mort_year", "ihme_loc_id"), by.y = c("year", "mort_year", "ihme_loc_id"), all.x = T)
    setnames(output, c("q_enn", "q_nn"), c("early_neonatal_mr", "neonatal_mr"))

  }

  output[, svy_year := NULL]
  output[, mort_year := NULL]

  output <- as.data.table(output)

  return(output)
}
