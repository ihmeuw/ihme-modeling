# This function is intended to be called before save_results_risk and after RR prediction in MR-BRT, and
# helps to format the predictions for upload by expanding demographics and adding other columns needed for upload. 
# It takes as input a data table (long by exposure, cause_id, and specific demographics estimated, 
# and wide by predicted RR draws) and gbd_round_id, performs age/sex/year expansion if these columns 
# are missing, adds location_id and mortality or morbidity columns needed for upload, and returns a 
# formatted data table with all required columns to be uploaded to save_results_risk. 
# This function does not validate the values contained in any of the columns already present in the data.
# 
# Columns required to exist: 
#    - "exposure"
#    - "cause_id"
#    - "mortality" and/or "morbidity"
# Columns to be filled in if missing: 
#   - "mortality" or "morbidity" will be filled in as 0, if the other column is present with a value of 1
#   - "location_id" will be filled in as 1 (global)
#   - "age_group_id" will be filled in as most detailed epi values for the given gbd_round_id
#   - "sex_id" will be filled in as most detailed epi values for the given gbd_round_id
#   - "year_id" will be filled in as all epi estimation years for the given gbd_round_id
# Validations performed on existing columns:
#   - none

library(data.table)
source("FILEPATH/get_demographics.R")

format_mrbrt_rr_results <- function(dt, gbd_round_id = 7) {
  # Check for required columns that do not have default values to assign
  for (col in c("exposure", "cause_id")) {
    if(!(col %in% colnames(dt))) {
      stop(paste0("Column '", col, "' is not present in data"))
    }
  }
  if(!("mortality" %in% colnames(dt)) & !("morbidity" %in% colnames(dt))) {
    stop(paste0("Columns 'mortality' and 'morbidity' are both not present in data"))
  }

  # Assign default location_id to global if missing
  if(!("location_id" %in% colnames(dt))) {
    print("'location_id' column not found, filling with global")
    dt[, location_id := 1]
  }
  
  # Assign zero to mortality or morbidity if one of them is missing
  if(!("mortality" %in% colnames(dt)) & ("morbidity" %in% colnames(dt))) {
    if(all(unique(dt$morbidity) == 1)) {
      print("'mortality' column not found, filling with 0")
      dt[, mortality := 0]
    }
  } else if(("mortality" %in% colnames(dt)) & !("morbidity" %in% colnames(dt))) {
    if(all(unique(dt$mortality) == 1)) {
      print("'morbidity' column not found, filling with 0")
      dt[, morbidity := 0]
    }
  }
  
  # Expand sexes and ages to most-detailed epi demographics if not present,
  # and expand years to all epi estimation years if not present
  demo <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
  
  if(!("sex_id" %in% colnames(dt))) {
    print("'sex_id' column not found, expanding to most-detailed sexes")
    dt <- cbind(dt[rep(seq_len(nrow(dt)), length(demo$sex_id))],
                sex_id = sort(rep((demo$sex_id), nrow(dt))))
    
  }
  if(!("age_group_id" %in% colnames(dt))) {
    print("'age_group_id' column not found, expanding to most-detailed ages")
    dt <- cbind(dt[rep(seq_len(nrow(dt)), length(demo$age_group_id))],
                age_group_id = sort(rep((demo$age_group_id), nrow(dt))))
  }
  if(!("year_id" %in% colnames(dt))) {
    print("'year_id' column not found, expanding to all estimation years")
    dt <- cbind(dt[rep(seq_len(nrow(dt)), length(demo$year_id))],
                year_id = sort(rep((demo$year_id), nrow(dt))))
  }
  
  return(dt)
}
