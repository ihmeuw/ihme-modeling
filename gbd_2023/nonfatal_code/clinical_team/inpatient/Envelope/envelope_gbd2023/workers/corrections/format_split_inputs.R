# Title: Produce cascading splines input from bundle version
# Purpose: Filter per inclusion criteria, transform the variables, add covariates
# Input: Bundle data (can specify bundle_version_id if need be)
# Output: CSV for splines input
# Author: FILEPATH

# IDs pulled from config.xlsx, access via config_list$`varname`

# Load bundle data


release_id <- 16
location_set_id <- 35
age_group_set_id <- 24 # Standard GBD 25 age groups
bundle_version_id <- 43245 # Used for GBD 2023 bested model
bundle_id <- 7919

bundle_dt <- get_bundle_version(bundle_version_id)



# if country_id and country_name isn't there, use this function, else skip
if (!all(c("country_id", "country_name") %in% colnames(bundle_dt))) {
  bundle_dt <- append_country_ids_names(bundle_dt, location_metadata)
}

# copy to dt for processing
dt <- copy(bundle_dt)

# remove all GBD 2021 surveys
dt <- dt[is_gbd_2021_survey != 1, ]

# Get modeled age groups
age_group_set <- get_age_metadata(release_id = release_id, 
                                  age_group_set_id = age_group_set_id) %>% 
  select(age_group_id, age_group_years_start, age_group_years_end, age_group_name)
age_group_ids <- unique(age_group_set$age_group_id)

# Get sex_id
get_sex_id <- function(dt) {
  sexes <- get_ids(table = "sex")
  dt <- merge(dt, sexes, by = "sex", all.x = TRUE)
  # cat unique sex_id and sex values
  cat("Unique sex_id:", unique(dt$sex_id), "\n")
  return(dt)
}

dt <- get_sex_id(dt)


# Add age group IDs 1 (<5 years) and 5 (1-4 years)
input_age_group_ids <- c(unique(age_group_set$age_group_id), 1, 5) 

# Get and process splines input per inclusion criteria
get_splines_input <- function(dt) {
  
  # Filter to GBD age group- & sex-specific data points
  dt <- dt %>% 
    # age and sex specific data points
    filter(age_group_id %in% input_age_group_ids) %>% 
    filter(sex %in% c("Male", "Female")) %>% 
    # sample size >10
    filter(sample_size > 10) %>%
    # create age midpoints
    mutate(age_end = ifelse(age_end %% 5 == 4, age_end + 1, age_end)) %>%
    mutate(age_end = ifelse(age_start == 1 & age_end == 1, 2, age_end)) %>% 
    # adjust 95 plus so that midpoint is at 97.5
    mutate(age_end = ifelse(age_start == 95, 100, age_end)) %>%
    mutate(age_mid = (age_start + age_end) / 2)
    
  
  dt <- as.data.table(dt)
  dt <- dt[, age_span := max(age_end) - min(age_start), by = .(nid, location_id, year_id, sex_id)][age_span > 80]
  
  # remove val > 0.5 for age_group_id 28
  dt <- dt %>% 
    filter(!(age_group_id == 28 & val > 0.5))
  
  dt$country_id <- as.integer(dt$country_id)
    
    return(dt)
  
}

dt_splines_input <- get_splines_input(dt)

# Get covariates: age-specific fertility rate & in-facility delivery ratio
get_covariate_values <- function(dt, asfr = TRUE, ifd = TRUE, nmx = TRUE) {
  
  # Template dt to fill
  cov_dt <- dt %>% select(country_id, year_id, age_group_id, sex_id) %>% unique()
  cov_dt$country_id <- as.integer(cov_dt$country_id)
  
  if(asfr) {
    asfr <- get_covariate_estimates(covariate_id = 13, 
                                    release_id = release_id,
                                    sex_id = unique(cov_dt$sex_id),
                                    location_id = unique(cov_dt$country_id),
                                    year_id = unique(cov_dt$year_id),
                                    age_group_id = unique(cov_dt$age_group_id))
  }
  
  asfr <- asfr %>% 
    # rename mean_value to mean_asfr
    dplyr::rename(mean_asfr = mean_value) %>%
    select(location_id, year_id, age_group_id, sex_id, mean_asfr)
  
  if(ifd) {
    ifd <- get_covariate_estimates(covariate_id = 51, 
                                   release_id = release_id,
                                   location_id = unique(cov_dt$country_id),
                                   year_id = unique(cov_dt$year_id))
  }
  ifd <- ifd %>% 
    dplyr::rename(mean_ifd = mean_value) %>% 
    select(location_id, year_id, mean_ifd)
  
  if(nmx) {
    nmx <- get_envelope(rates = 1,
                        with_shock = 0, # Do not include shocks
                        with_hiv = 1, # Include HIV/AIDS deaths
                        release_id = release_id,
                        location_id = unique(cov_dt$country_id),
                        age_group_id = unique(cov_dt$age_group_id),
                        sex_id = unique(cov_dt$sex_id),
                        year_id = unique(cov_dt$year_id))
    
    nmx <- nmx %>% 
      dplyr::rename(mean_nmx = mean_value) %>% 
      select(location_id, year_id, age_group_id, sex_id, mean_nmx)
    
  }
  
  # Merge covariates
  cov <- merge(cov_dt, asfr, 
          by.x = c("country_id", "year_id", "age_group_id", "sex_id"),
          by.y = c("location_id", "year_id", "age_group_id", "sex_id"), all.x = TRUE)
  cov <- merge(cov, ifd, 
          by.x = c('country_id', 'year_id'), 
          by.y = c("location_id", "year_id"), all.x = TRUE)
  cov <- merge(cov_dt, nmx, 
               by.x = c("country_id", "year_id", "age_group_id", "sex_id"),
               by.y = c("location_id", "year_id", "age_group_id", "sex_id"), all.x = TRUE)
  
  ## Final covariate is the product of ASFR and IFD
  cov[, asfr_x_ifd := mean_asfr * mean_ifd]
  
  cov$country_id <- as.integer(cov$country_id)
  
  # Warn if there are missing values in cov in any columns
  if (any(colSums(is.na(cov)) > 0)) {
    warning("There are missing values in cov")
  }
  
  return(cov)
}

covariates <- get_covariate_values(dt_splines_input, asfr = TRUE, ifd = TRUE, nmx = TRUE)

append_covariate_values <- function(dt, cov = covariates) {
  # Merge the main dataset with covariate data
  dt <- merge(dt, cov, 
              by.x = c("country_id", "year_id", "age_group_id", "sex_id"),
              by.y = c("country_id", "year_id", "age_group_id", "sex_id"), all.x = TRUE)
  
  # Remove mean_asfr and mean_ifd
  dt <- dt %>% select(-mean_asfr, -mean_ifd, -mean_nmx)
  
  # Check for missing values in asfr_x_ifd
  if (any(is.na(dt$asfr_x_ifd))) {
    warning("There are missing values in asfr_x_ifd")
  }
  
  return(dt)
}
               
dt_splines_input <- append_covariate_values(dt_splines_input)

str(dt_splines_input)


# Transform data for modeling ----
transform_data <- function(dt) {
  
  # setDT
  setDT(dt)
  
  ## Log transform the dependent variable
  dt[, log_mean := log(val)]
  
  ## Apply the delta method to transform the standard error
  dt$log_se <- mapply(FUN = function(mu, sigma) {
    msm::deltamethod(g = ~log(x1), mean = mu, cov = sigma^2)
    }, mu = dt$val, sigma = dt$standard_error)
  
  ## Compute inverse variance
  dt[, inverse_variance  :=  1 / log_se^2]
  
  ## Compute the random effect variable (equivalent of a study)
  dt[, ranef_id  :=  paste0(nid, "_", location_id, "_", year_id, "_", sex)]
  
  # Create levels of the cascade
  location_metadata <- get_location_metadata(release_id = release_id, location_set_id = location_set_id)
  
  dt <- merge(dt, location_metadata[, .(location_id, region_id, region_name, super_region_id, super_region_name)], 
              by = "location_id", all.x = TRUE)
  dt[, country_id_year_id  :=  paste0(country_id, "_", year_id)]
  dt[, region_id_year_id  :=  paste0(region_id, "_", year_id)]
  dt[, super_region_id_year_id  :=  paste0(super_region_id, "_", year_id)]
  dt[, super_region_id := as.character(super_region_id)]
  
  return(dt)
  
}

dt_splines_input <- transform_data(dt_splines_input)


# Outlier dt_splines_input
# Outliering sourced from spreadsheet
outl_path <- "FILEPATH"
outlier_sheet_xlsx <- openxlsx::read.xlsx(outl_path)
outldt <- as.data.table(outlier_sheet_xlsx)
str(outldt)

outldt <- outldt %>% 
  select(country_id, year_id, nid, age_group_id, sex_id)

split_and_melt <- function(dt) {
  
  dt_long <- melt(dt, measure.vars = c("age_group_id", "sex_id"),
                  variable.name = "type", value.name = "group")
  
  dt_long <- dt_long[, strsplit(as.character(group), ","), by = .(country_id, year_id, nid, type)]
  
  dt_age <- dt_long[type == "age_group_id", .(country_id, year_id, nid, age_group_id = V1)]
  dt_sex <- dt_long[type == "sex_id", .(sex_id = V1)]
  
  result <- dt_age[, c(.SD, list(sex_id = dt_sex$sex_id)), by = .(country_id, year_id, nid, age_group_id)]
  
  # make all columns numeric
  result <- result[, lapply(.SD, as.numeric)]
  
  result <- unique(result)
  
  return(result)
  
  
}

outliers <- split_and_melt(outldt)

# anti-join dt on outliers by country_id, year_id, nid, age_group_id, sex_id
dt_splines_input$nid <- as.numeric(dt_splines_input$nid)
dt_splines_input <- dt_splines_input[!outliers, on = .(country_id, year_id, nid, age_group_id, sex_id)]


# Prediction frame ----
# Generate prediction frame - GBD age groups, Male and Female, all locations and years in the bundle
age_mid_dt <- copy(age_group_set)
setnames(age_mid_dt, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
age_mid_dt[age_start == 95, age_end := 100]
age_mid_dt[, age_mid := (age_start + age_end) / 2]

loc_yr_dt <- dt[, .(location_id, country_id, country_name, year_id)] %>% distinct()

## Add age midpoints for each location_id-year_id combo
dt_list <- lapply(1:nrow(loc_yr_dt), 
                  function(i) {
                    temp_dt <- copy(age_mid_dt) # Make a copy of age_mid_dt
                    temp_dt[, location_id := loc_yr_dt$location_id[i]]
                    temp_dt[, country_id := as.integer(loc_yr_dt$country_id[i])]
                    temp_dt[, country_name := loc_yr_dt$country_name[i]]
                    temp_dt[, year_id := loc_yr_dt$year_id[i]]
                    return(temp_dt)
                  }
)

# Combine the list of data.tables into one
pred_frame <- rbindlist(dt_list)


# add sexes
# duplicate rows to assign sex id for unique combos, merge on covariate values, split into male and female p.frames
pred_frame[, row_id := seq(1, nrow(pred_frame), 1)] 
setcolorder(pred_frame, "row_id") # make it the 1st column
pred_frame <- rbindlist(list(pred_frame, pred_frame)) # duplicate all rows for both sexes
setorder(pred_frame, "row_id")
pred_frame[, sex_id := rep(c(1, 2), length.out = .N)]
pred_frame[, row_id := NULL]

# get covariate data tables
# ASFR: Age-Specific Fertility Rate
asfr <- get_covariate_estimates(covariate_id = 13, 
                                release_id = release_id, 
                                sex_id = unique(pred_frame$sex_id))
setnames(asfr, "mean_value", "mean_asfr")
setorder(asfr, "location_id", "year_id", "age_group_id", "sex_id")

# IFD: In-Facility Delivery
ifd <- get_covariate_estimates(covariate_id = 51, 
                               release_id = release_id, 
                               location_id = "all") # all age all sex
setnames(ifd, "mean_value", "mean_ifd")
setorder(ifd, "location_id", "year_id", "age_group_id", "sex_id")

# NMX: age/sex specific mortality rate
nmx <- get_envelope(rates = 1,
                    with_shock = 0, # Do not include shocks
                    with_hiv = 1, # Include HIV/AIDS deaths
                    release_id = release_id,
                    location_id = unique(pred_frame$country_id),
                    age_group_id = unique(pred_frame$age_group_id),
                    sex_id = unique(pred_frame$sex_id),
                    year_id = unique(pred_frame$year_id))

cov_dt <- asfr[, .(location_id, year_id, age_group_id, sex_id, mean_asfr)] %>%
  merge(ifd[, .(location_id, year_id, mean_ifd)], by = c('location_id', 'year_id'),
        nomatch = 0,
        all.x = TRUE) %>% 
  merge(nmx[, .(location_id, year_id, age_group_id, sex_id, mean_nmx)], by = c('location_id', 'year_id', 'age_group_id', 'sex_id'),
        nomatch = 0,
        all.x = TRUE)

# compute maternal correction covariate that is a product of ASFR and IFD
cov_dt[, asfr_x_ifd := mean_asfr * mean_ifd]

pred_frame <- merge(pred_frame, cov_dt[, .(location_id, year_id, age_group_id, sex_id, asfr_x_ifd)], 
                    by = c("location_id", "year_id", "age_group_id", "sex_id"), all.x = TRUE)
pred_frame[age_group_id == 28, asfr_x_ifd := 0]
location_metadata <- get_location_metadata(release_id = release_id, location_set_id = location_set_id)
pred_frame <- merge(pred_frame, location_metadata[level == 3, .(location_id, region_id, region_name,
                                                      super_region_id, super_region_name)],
                    by.x = "country_id", by.y = "location_id", all.x = TRUE)

pred_frame <- pred_frame[year_id >= 1980, ]

# add sex for plotting
pred_frame[sex_id == 1, sex := "Male"][sex_id == 2, sex := "Female"]

# add levels of the cascade
pred_frame[, country_id_year_id  :=  paste0(country_id, "_", year_id)]
pred_frame[, region_id_year_id  :=  paste0(region_id, "_", year_id)]
pred_frame[, super_region_id_year_id  :=  paste0(super_region_id, "_", year_id)]
pred_frame[, super_region_id := as.character(super_region_id)]



# Save input data set and prediction frame ----
timestamp <- format(Sys.time(), "%Y_%m_%d_%H_%M_%S")
input_path <- paste0(home_dir, "FILEPATH/input_for_splines_", "v", bundle_version_id, "_", timestamp, ".csv")
frame_path <- paste0(home_dir, "FILEPATH/pred_frame_", "v", bundle_version_id, "_", timestamp, ".csv")
fwrite(dt_splines_input, input_path)
fwrite(pred_frame, frame_path)