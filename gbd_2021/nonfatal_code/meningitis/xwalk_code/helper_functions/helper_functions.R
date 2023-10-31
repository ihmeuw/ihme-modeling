#' @author
#' @date 2019/03/01
#' @description Helper functions 

# DECLARE USER FUNCTIONS --------------------------------------------------
# REPLACE THIS FUNCTION WITH WHATEVER YOU WANT TO USE TO PULL IN DATA
get_dem_data <- function() {
  dt <- get_bundle_version(version_id, transform = T)
  return(dt)
}

# REMOVE GROUP_REVIEW == 0
remove_group_review_0 <- function(adjusted_dt) {
  dt <- copy(adjusted_dt)
  dt <- dt[group_review != 0 | is.na(group_review)]
  return(dt)
}

# SAVES SEX-SPLIT XWALK VERSION
save_sex_split_version <- function(sex_split_dt, ratio) {
  sex_split_dt <- remove_group_review_0(sex_split_dt)
  wb <- createWorkbook()
  addWorksheet(wb, "extraction")
  writeData(wb, "extraction", sex_split_dt)
  saveWorkbook(wb, paste0(dem_dir, sex_model_name, ".xlsx"), overwrite = T)
  
  my_filepath <- paste0(dem_dir, sex_model_name, ".xlsx")
  my_desc <- paste0(sex_model_name, "_female_male_ratio (", ratio, ")")
  result <- save_crosswalk_version(bundle_version_id = version_id, 
                                   data_filepath = my_filepath, 
                                   description = my_desc)
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
}

# PULLS IN ADDITIONAL DATA FOR SEX SPLITTING
get_addiontional_sexsplit_data <- function() {
  # pull Brazil SINAN data
  dt_sinan <- fread(sinan_filepath)
  dt_sinan[, cv_active_surveillance := 1]
  dt_sinan[, cv_passive_surveillance := 0]
  dt_sinan[, cv_sentinel_surveillance := 0]
  dt_sinan[, cv_population_surveillance := 1]
  # pull in additional extracted data
  dt_other <- fread(extraction_filepath)
  dt_other[, measure := "incidence"]
  dt <- rbindlist(list(dt_sinan, dt_other), fill = T)
  return(dt)
}

# PULL IN ADDITIONAL DATA FOR XWALKING
get_addiontional_xwalk_data <- function(sex_split_dt) {
  dt <- copy(sex_split_dt)
  # pull invasive meningococcal disease population and surveilance logit ratios  
  # that have already been collapsed
  dt_imd_population <- fread(imd_pop_filepath)
  dt_imd_sentinel <- fread(imd_sentinel_filepath)
  dt <- rbindlist(list(dt_imd_population, dt_imd_sentinel, dt), fill = T)
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), 
     standard_error := (upper - lower) / 3.92]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, 
     standard_error := ((5 - mean * sample_size) / sample_size 
                        + mean * sample_size * sqrt(5 / sample_size^2)) / 5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, 
     standard_error := sqrt(mean / sample_size)]
  return(dt)
}

# GET POPULATION FOR EACH ROW IN EXTRACTION SHEET
get_row_population <- function(i, ds) {
  #' get population for each row in extraction sheet
  #' 
  #' @description pulls population using get_population shared function
  #'              from GBD 2019 decomp step ds for each row's demographics
  #'              using the closest age range in GBD land
  #' 
  #' @param i integer. row index in data table
  #' @param ds string. decomp step for get_population
  #' @return pop numeric. total population for row i's demographics
  #' 
  #' @examples get_row_population(1, 'step4')
  #' 
  # get age group id range
  age_start <- need_sample_size_dt[i, age_start]
  age_end <- need_sample_size_dt[i, age_end]
  age_start_index <- which.min(abs(age_start - age_meta$age_group_years_start))
  age_end_index <- which.min(abs(age_end - age_meta$age_group_years_end))
  my_age_group_id_start <- age_meta[age_start_index, age_group_id]
  my_age_group_id_end <- age_meta[age_end_index, age_group_id]
  # subset GBD age groups to those for this row
  gbd_age_group_id <- c(2:20, 30:32, 235)
  my_age_group_id_range <- gbd_age_group_id[gbd_age_group_id >= my_age_group_id_start
                                            & gbd_age_group_id <= my_age_group_id_end]
  # get sex id
  sex <- need_sample_size_dt[i, sex]
  if (sex == "Male") {
    my_sex_id <- 1
  } else if (sex == "Female") {
    my_sex_id <- 2
  } else if (sex == "Both") {
    my_sex_id <- 3
  } else {
    stop(paste("No sex id for sex:", sex))
  }
  # get year_id range
  year_start <- need_sample_size_dt[i, year_start]
  year_end <- need_sample_size_dt[i, year_end]
  my_year_range <- seq(year_start, year_end)
  # get location_id
  my_location_id <- need_sample_size_dt[i, location_id]
  # print arguments
  cat(paste("Pulling population for location_id:", my_location_id,
            "\nsex_id:", my_sex_id,
            "\nyear_ids:", paste(my_year_range, collapse = ", "),
            "\nage_group_ids:", paste(my_age_group_id_range, collapse = ", "),
            "\n row age_start:", age_start, "row age_end:", age_end, "\n"
  ))
  # get population
  pop_dt <- get_population(location_id = my_location_id,
                           age_group_id = my_age_group_id_range,
                           sex_id = my_sex_id,
                           year_id = my_year_range,
                           decomp_step = ds,
                           gbd_round_id = 6)
  pop <- sum(pop_dt$population)
  return(pop)
}

## Get male, female, and both sex location-age specific population for each row 
get_row <- function(n, dt, pops) {
  row <- copy(dt)[n]
  # round age start and age end to closest gbd age group values
  row_age_start <- row[, age_start]
  row_age_end <- row[, age_end]
  gbd_age_start <- unique(pops$age_group_years_start)
  gbd_age_end <- unique(pops$age_group_years_end)
  row_gbd_age_start <- gbd_age_start[which.min(abs(row_age_start - gbd_age_start))]
  row_gbd_age_end <- gbd_age_end[which.min(abs(row_age_end - gbd_age_end))]
  if (row_gbd_age_start  == row_gbd_age_end) {
    pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                            age_group_years_start == row_gbd_age_start])
  } else {
    pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                            age_group_years_start >= row_gbd_age_start & age_group_years_end <= row_gbd_age_end])
  }
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex_id")]
  row[, `:=` (male_N = agg[sex_id == 1, pop_sum], female_N = agg[sex_id == 2, pop_sum],
              both_N = agg[sex_id == 3, pop_sum])]
  return(row)
}
