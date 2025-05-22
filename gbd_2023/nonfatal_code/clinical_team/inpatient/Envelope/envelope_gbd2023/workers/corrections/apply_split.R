# Title: Age/sex split application
# Purpose: Apply cascading splines output to produce fully age/sex specific data set
# Inputs: cascading splines predictors, bundle data
# Outputs: post_split_data
# Author: USERNAME

# Import cascading splines predictors (filled prediction frame)
import_c_splines_predictors <- function(stage2_predictors_Female_path, stage2_predictors_Male_path) {
  stage2_predictors_Female <- fread(stage2_predictors_Female_path)
  stage2_predictors_Male <- fread(stage2_predictors_Male_path)
  stage2_predictors <- rbind(stage2_predictors_Male, stage2_predictors_Female)
  return(stage2_predictors)
}

# Function to get a population column for each demographic
get_populations <- function(dt){
  functions_dir <- "FILEPATH" # source shared functions from here
  get_population_function <- "get_population"
  invisible(lapply(get_population_function, function(x) source(paste0(functions_dir, x, ".R"))))
  
  populations <- get_population(release_id = release_id, 
                                # location_id = unique(dt$location_id),
                                location_id = unique(dt$country_id),
                                year_id = unique(dt$year_id),
                                age_group_id = unique(dt$age_group_id),
                                sex_id = unique(dt$sex_id)) %>% select(-run_id)
  dt <- merge(dt, populations, 
              by.x = c("country_id", "year_id", "sex_id", "age_group_id"),
              by.y = c("location_id", "year_id", "sex_id", "age_group_id"), all.x = TRUE)
  
  # throw message if populations are missing and list location_id with missing
  if (any(is.na(dt$population))) {
    message("Missing population data for location_id: ", unique(dt$location_id[is.na(dt$population)]))
  } else {
    cat("All populations are present.")
  }
  
  return(dt)
}

# Function to compute age weights for each sex separately to split sex-specific data points
calculate_age_weights <- function(dt) {
  dt[, min_pred := min(stage2_mean), by = .(country_id, year_id, sex_id)]
  dt$min_pred <- as.numeric(dt$min_pred)
  dt[, pred0 := stage2_mean - min_pred]
  dt[, relative_risk := exp(pred0)]
  dt[, rr_x_population := relative_risk * population]
  dt[, scalar := 1 / sum(rr_x_population), by = .(country_id, year_id, sex_id)]
  dt[, age_weight := rr_x_population * scalar, by = .(country_id, year_id, sex_id)]
  
  dt <- dt %>% select(-c("min_pred", "pred0", "relative_risk", "rr_x_population", "scalar"))
  
  # Correctly calculate the sum check to include a tolerance for floating point comparison
  # and ensure it handles NA values properly
  dt[, check_sum := round(sum(age_weight), 2), by = .(country_id, year_id, sex_id)]
  if (any(dt$check_sum != 1, na.rm = TRUE)) {
    problematic_locations <- dt[check_sum != 1, .(country_id, year_id, sex_id)]
    message("Sum of age weights is not equal to one for some location(s).")
    message("Location(s) with sum of age weights not equal to one: ", paste(unique(problematic_locations$country_id), collapse=", "))
  } else {
    cat("All age weights sum to one. Proceeding.")
  }
  
  return(dt)
}

# Function to compute age-sex weights to split "Both" sexes data points
calculate_age_sex_weights <- function(dt) {
  dt[, min_pred := min(stage2_mean), by = .(country_id, year_id)]
  dt$min_pred <- as.numeric(dt$min_pred)
  dt[, pred0 := stage2_mean - min_pred] # convert minimum to zero in the log space
  dt[, relative_risk := exp(pred0)]
  dt[, rr_x_population := relative_risk * population]
  dt[, scalar := 1 / sum(rr_x_population), by = .(country_id, year_id)]
  dt[, age_sex_weight := rr_x_population * scalar, by = .(country_id, year_id)]
  
  dt <- dt %>% select(-c("min_pred", "pred0", "relative_risk", "rr_x_population", "scalar"))
  
  # if any age_sex_weight values are missing, throw a message
  if (any(is.na(dt$age_sex_weight))) {
    message("Some age-sex weights are missing, likely due to the absence of both-sex data points requiring split for given location-years. Inspect the bundle data for country_id_year_id ",
            paste0(dt[is.na(age_sex_weight) == TRUE, unique(country_id_year_id)], collapse = ", "))
  } else {
    cat("All age-sex weights are present")
  }
  
  # Calculate the sum checks outside the if-else for clarity
  sum_check_results <- dt[, .(sum_check = round(sum(age_sex_weight), 0) == 1), by = .(country_id, year_id)]
  
  # Use ifelse to directly apply vectorized condition, making it more concise and readable
  result_messages <- ifelse(sum_check_results$sum_check != TRUE, "All age-sex weights sum to one. Proceeding.", 
                            "Sum of age-sex weights is not equal to one for some location(s).")
  
  print(unique(result_messages))

  return(dt)
  
}

# Execute functions - get age and age/sex weights
stage2_predictors <- import_c_splines_predictors(stage2_predictors_Female_path, stage2_predictors_Male_path)
stage2_predictors_v1 <- copy(get_populations(stage2_predictors))
stage2_predictors_v2 <- copy(calculate_age_weights(stage2_predictors_v1))
stage2_predictors_v3 <- copy(calculate_age_sex_weights(stage2_predictors_v2))


# Step 2. Expand rows requiring split to get a/s-specific rows

# Get subset of bundle_dt where age_group_id is not in age_group_ids OR sex_id is not 1 or 2
bundle_dt <- get_bundle_version(bundle_version_id)
get_data_for_split <- function(bundle_dt, age_group_set_id) {
  
  # <1 year age group, 28 might be replaced with 391 sometimes, change back for consistency with age group set 34
  bundle_dt[age_group_id == 391, age_group_id := 28]
  # import age_metadata if it doesn't exist in the environment
  if (!exists("age_metadata")) {
    age_metadata <- get_age_metadata(release_id = release_id, age_group_set_id = age_group_set_id)
  }
  # data needing split would not have any of those age group IDs or be sex-specific
  age_group_ids <- unique(age_group_set$age_group_id)
  # age_group_ids <- c(age_group_ids, 1, 5)
  df <- bundle_dt[!(age_group_id %in% age_group_ids) | sex == "Both", ]
  # data using the envelope will be excluded if there is any
  df <- df[uses_env == 0, ]
  
  return(df)
  
}

dt_for_split <- get_data_for_split(bundle_dt, age_group_set_id)

# Add auxiliary columns for expansion
add_columns <- function(dt) {
  dt[, `:=` (orig_age_start = age_start, 
             orig_age_end = age_end,
             orig_age_group_id = age_group_id,
             orig_sex = sex)]
  
  dt[age_end > 95, age_end := 100]
  
  return(dt)
  
}

dt_to_be_split_v0 <- add_columns(dt_for_split)

# Define functions for expansion based on original age_start, age_end, and sex
# Age start = 0, age end >=2, sex %in% c("Male", "Female")
expand_0_to_over2_mf <- function(dt) {
  
  # dt <- as.data.table()
  dt <- dt[sex %in% c("Male", "Female"), ] # Filter sex-specific only
  dt[, id := seq(1, nrow(dt), by = 1)] # Compute an ID variable with 1 to nrow with increments of 1 - needed for merging and expansion
  dt <- dt[age_start == 0 & age_end >= 2, ] # Filter to age groups [0,100] (e.g, "All Ages", "Under 5")
  dt[, n_age_groups := round(((age_end / 5) + 2), 0)] # Calculate the number of age groups to split the row into (e.g., 22 for "All Ages")
  # plus 2 if not estimating for under 1 detailed 
  # this is not very robust as it doesn't work for age_start=0 age_end=2 without rounding to 0 decimals
  expanded <- rep(dt$id, dt$n_age_groups) %>% data.table("id" = .) # Generate a single variable `id` table repeating id the number of modeled age groups (22)
  split <- merge(expanded, dt, by = "id", all = TRUE) # now the table is expanded with loc-years duplicated 22 times per each age group
  split[, age_rep := 1:.N - 1, by = "id"] # Generate 
  split <- split %>% 
    mutate(
      age_start = case_when(
        age_rep == 0 ~ 0,
        age_rep == 1 ~ 1,
        age_rep == 2 ~ 2,
        TRUE         ~ age_start + (age_rep - 2) * 5
      )
    ) 
  split <- split %>% 
    mutate(
      age_end = case_when(
        age_rep == 0 ~ 1,
        age_rep == 1 ~ 2,
        age_rep == 2 ~ 4,
        TRUE         ~ age_start + 5
      )
    )
  
  # Validate the resulting expanded rows - write up!
  # View(setorder((unique(split0[,.(orig_age_group_id, orig_age_start, orig_age_end, age_start, age_end, sex)])), "orig_age_start", "orig_age_end"))
  split <- split %>% select(-c("age_group_id")) # to get it later from age_start and age_end, keeping orig_age_group_id
  
  return(split)
}

# Age start = 0, age end >=2, sex == "Both"
expand_0_to_over2_both <- function(dt) {
  dt <- dt[sex == "Both", ] # Subset to both-sex rows
  dt[, id := seq(1, nrow(dt), by = 1)] # Compute an ID variable with 1 to nrow with increments of 1 - needed for merging and expansion
  dt <- rbindlist(list(dt, dt)) # Duplicate all rows for both sexes to ...
  setorder(dt, "id") # ... ensure that the rows are ordered by id
  dt[, sex := rep(c("Male", "Female"), length.out = .N)] # Append `sex` variable to the duplicated rows
  dt[, id := seq(1, nrow(dt), by = 1)] # # Recreate id column in place of the old one
  dt <- dt[age_start == 0 & age_end >= 2, ]  # Filter to age groups [0,>=2] (e.g, "All Ages", "Under 5")
  dt[, n_age_groups := round(((age_end / 5) + 2), 0)] # Calculate the number of age groups to split the row into (e.g., 22 for "All Ages")
  expanded <- rep(dt$id, dt$n_age_groups) %>% data.table("id" = .) # Generate a single variable `id` table repeating id the number of modeled age groups (22)
  split <- merge(expanded, dt, by = "id", all = TRUE) # now the table is expanded with loc-years duplicated 25 times per each age group
  split[, age_rep := 1:.N - 1, by = "id"] # what is this for? generates numbers 0 to 24
  split <- split %>% 
    mutate(
      age_start = case_when(
        age_rep == 0 ~ 0,
        age_rep == 1 ~ 1,
        age_rep == 2 ~ 2,
        TRUE ~ age_start + (age_rep - 2) * 5
      )
    ) 
  split <- split %>% 
    mutate(
      age_end = case_when(
        age_rep == 0 ~ 1,
        age_rep == 1 ~ 2,
        age_rep == 2 ~ 4,
        TRUE         ~ age_start + 5
      )
    )
  
  # Validate the expansion
  
  # Remove extra column(s)
  split <- split %>% select(-c("age_group_id")) # to get it later from age_start and age_end, keeping orig_age_group_id
  
  return(split)
}

# Age start >4, sex %in% c("Male", "Female")
expand_over4_mf <- function(dt) {
  
  dt <- dt[sex %in% c("Male", "Female"), ] # Filter sex-specific only
  dt[, id := seq(1, nrow(dt), by = 1)] # Compute an ID variable with 1 to nrow with increments of 1 - needed for merging and expansion
  dt <- dt[age_start > 4, ]
  dt[, n_age_groups := round(((age_end - age_start) / 5), 0)] # Calculate the number of age groups to split the row into (e.g., 22 for "All Ages")
  expanded <- rep(dt$id, dt$n_age_groups) %>% data.table("id" = .)
  split <- merge(expanded, dt, by = "id", all.x = TRUE)
  split[, age_rep := 1:.N - 1, by = 'id']
  split[, age_start := age_start + age_rep * 5] 
  split[, age_end :=  age_start + 5] 

  # Validate the resulting expanded rows - write up!
  # View(setorder((unique(split0[,.(orig_age_group_id, orig_age_start, orig_age_end, age_start, age_end, sex)])), "orig_age_start", "orig_age_end"))
  split <- split %>% select(-c("age_group_id")) # to get it later from age_start and age_end, keeping orig_age_group_id
  
  return(split)
}

# Age start >4, sex == "Both"
expand_over4_both <- function(dt) {
  dt <- dt[sex == "Both", ]
  dt[, id := seq(1, nrow(dt), by = 1)] # compute an ID variable with 1 to nrow with increments of 1 - needed for merging and expansion
  dt <- rbindlist(list(dt, dt)) # duplicate all rows for both sexes
  setorder(dt, "id") # ensure that the rows are ordered
  dt[, sex := rep(c("Male", "Female"), length.out = .N)]
  dt[, id := seq(1, nrow(dt), by = 1)] # compute an ID variable with 1 to nrow with increments of 1 - needed for merging and expansion
  dt <- dt[age_start > 4, ]
  dt[, n_age_groups := round(((age_end - age_start) / 5), 0)]
  
  expanded <- rep(dt$id, dt$n_age_groups) %>% data.table("id" = .)
  split <- merge(expanded, dt, by = "id", all.x = TRUE)
  
  split[, age_rep := 1:.N - 1, by = 'id']
  split[, age_start := age_start + age_rep * 5] 
  split[, age_end :=  age_start + 5] 
  
  split <- split %>% select(-c("age_group_id")) # to get it later from age_start and age_end, keeping orig_age_group_id
  
  return(split)
}

# Age start == 1, sex %in% c("Male", "Female")
expand_over1_mf <- function(dt) {
  dt <- dt[sex %in% c("Male", "Female")]
  dt[, id := seq(1, nrow(dt), by = 1)]
  dt <- dt[age_start == 1, ]
  dt[, n_age_groups := round(((age_end / 5) + 1), 0)] # plus 2 if not esimating for under 1 detailed
  expanded <- rep(dt$id, dt$n_age_groups) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age_rep := 1:.N - 1, by =.(id)]
  split <- split %>%
    mutate(
      age_start = case_when(
        age_rep == 0 ~ 1,
        age_rep == 1 ~ 2,
        TRUE         ~ (age_rep-1)*5
      )
    )
  split <- split %>%
    mutate(
      age_end = case_when(
        age_rep == 0 ~ 2,
        age_rep == 1 ~ 4,
        TRUE         ~ age_start + 5
      )
    )
  
  split <- split %>% select(-c("age_group_id"))
  
  return(split)
  
}

# Age start == 1, sex == "Both"
expand_over1_both <- function(dt) {
  dt <- dt[sex == "Both"]
  dt[, id := seq(1, nrow(dt), by = 1)] # compute an ID variable with 1 to nrow with increments of 1 - needed for merging and expansion
  dt <- rbindlist(list(dt, dt)) # duplicate all rows for both sexes
  setorder(dt, "id")
  dt[, sex_id := rep(c(1, 2), length.out = .N)]
  dt[sex_id == 1, sex := "Male"][sex_id == 2, sex := "Female"]
  
  dt[, id := seq(1, nrow(dt), by = 1)] # compute an ID variable with 1 to nrow with increments of 1 - needed for merging and expansion
  dt <- dt[age_start == 1, ]
  
  dt[, n_age_groups := round(((age_end / 5) + 1), 0)] # plus 2 if not esimating for under 1 detailed 
  expanded <- rep(dt$id, dt$n_age_groups) %>% data.table("id" = .)
  split <- merge(expanded, dt, by="id", all=T)
  split[, age_rep := 1:.N - 1, by =.(id)]
  split <- split %>%
    mutate(
      age_start = case_when(
        age_rep == 0 ~ 1,
        age_rep == 1 ~ 2,
        TRUE         ~ (age_rep-1)*5
      )
    )
  
  split <- split %>%
    mutate(
      age_end = case_when(
        age_rep == 0 ~ 2,
        age_rep == 1 ~ 4,
        TRUE         ~ age_start + 5
      )
    )
  
  split <- split %>% select(-c("age_group_id"))
  
  return(split)
  
}

dt_expanded_0_to_over2_mf <- expand_0_to_over2_mf(dt_to_be_split_v0)
dt_expanded_0_to_over2_both <- expand_0_to_over2_both(dt_to_be_split_v0)
dt_expanded_over4_mf <- expand_over4_mf(dt_to_be_split_v0)
dt_expanded_over4_both <- expand_over4_both(dt_to_be_split_v0)
dt_expanded_over1_mf <- expand_over1_mf(dt_to_be_split_v0)
dt_expanded_over1_both <- expand_over1_both(dt_to_be_split_v0)




# Combine and prepare the expanded rows for merging
prepare_expanded_for_merge <- function(dt_list) {
  dt <- rbindlist(dt_list, fill = TRUE)

  # warn if there are any data points with !(sex %in% c("Male", "Female"))
  if (any(!(dt$sex %in% c("Male", "Female")))) {
    warning("There are both-sex data points in the expanded table, please double-check the functions")
  }
  
  dt <- dt %>% select(-c("id", "n_age_groups", "sex_id")) # Deselect extra columns
  dt[age_end %% 5 == 4, age_end := age_end + 1] # changes age_end of 19 to 20, 4 to 5
  dt[age_end == 100, age_end := 125]
  
  # import age_metadata if it doesn't exist in the environment and merge to get names and IDs
  if (!exists("age_metadata")) {
    age_metadata <- get_age_metadata(release_id = release_id, age_group_set_id = age_group_set_id)
    setnames(age_metadata, old = c("age_group_years_start", "age_group_years_end"), new = c("age_start", "age_end"))
  }
  dt <- merge(dt, age_metadata[, .(age_group_id, age_start, age_end)], by = c("age_start", "age_end"), all.x = T)
  
  return(dt)
  
}

dt_list <- list(dt_expanded_0_to_over2_mf, dt_expanded_0_to_over2_both,
                dt_expanded_over4_mf, dt_expanded_over4_both,
                dt_expanded_over1_mf, dt_expanded_over1_both)
dt_expanded_v1 <- prepare_expanded_for_merge(dt_list)

# if country_id and country_name isn't there, use this function, else skip
if (!all(c("country_id", "country_name") %in% colnames(bundle_dt))) {
  dt_expanded_v1 <- append_country_ids_names(dt_expanded_v1)
}


# Apply age and age/sex weights and finalize split data set
apply_weights <- function(dt, weights) {
  dt <- merge(dt, weights[ ,.(country_id, year_id, sex, age_group_id, age_weight, age_sex_weight, population)], 
              by.x = c("country_id", "year_id", "sex", "age_group_id"), 
              by.y = c("country_id", "year_id", "sex", "age_group_id"), 
              all.x = TRUE)
  
  # calculate sums of age-sex-weight - this will be the denominator
  dt[, sum_age_sex_weight := sum(age_sex_weight), by = c("location_id", "year_id", "orig_age_start", "orig_age_end")]
  dt[, sum_age_weight := sum(age_weight), by = c("location_id", "year_id", "orig_sex", "orig_age_start", "orig_age_end")]
  
  # calculate original-demographic-specific age- and age-sex-weights so that they sum up to 1
  dt[, age_sex_weight_spec := age_sex_weight/sum_age_sex_weight]
  dt[, age_weight_spec := age_weight/sum_age_weight]
  
  # multiply the original demographic-specific count by the specific 
  dt[orig_sex == "Both", count_split := round(age_sex_weight_spec*cases, 0)]
  dt[orig_sex %in% c("Male", "Female"), count_split := round(age_weight_spec*cases, 0)]
  
  # remove and remerge on populations to pull location_id specific populations if level > 3
  dt[, population := NULL]
  dt[sex == "Male", sex_id := 1][sex == "Female", sex_id := 2]
  populations <- get_population(release_id = release_id, 
                                location_id = unique(dt$location_id),
                                year_id = unique(dt$year_id),
                                age_group_id = unique(dt$age_group_id),
                                sex_id = unique(dt$sex_id)) %>% select(-run_id)
  dt <- merge(dt, populations, by = c("location_id", "year_id", "sex_id", "age_group_id"), all.x = TRUE)
  
  # replace sample_size with population everywhere except surveys incomplete for the population
  jpn_surveys_nids <- dt[data_type_name=="Survey" & grepl("Japan Patient Survey", field_citation_value), unique(nid)]
  usa_surveys_nids <- dt[data_type_name=="Survey" & grepl("AHA Annual Survey", field_citation_value), unique(nid)]
  survey_nids_entire_pop <- c(jpn_surveys_nids, usa_surveys_nids)
  
  # for surveys except those JPN and USA, keep the sample size value; replace with population for the rest
  dt[, sample_size := ifelse(data_type_name == "Survey" & !(nid %in% survey_nids_entire_pop), sample_size, population)]
  
  # recalculate val with updated sample size
  dt[, val := count_split / sample_size]
  
  dt <- dt %>% mutate(age_mid = (age_start + age_end) / 2)
  
  # add random effect ID for grouping (connecting the dots on the line plots)
  dt[, ranef_id := paste0(nid, "_", location_id, "_", year_id, "_", orig_sex)]
  
  return(dt)
  
}

# Apply the split
dt_pred_v0 <- apply_weights(dt_expanded_v1, stage2_predictors_v3)

# Collate split and nonsplit datasets and retain specified columns
merge_and_select <- function(dt_not_split, dt_pred, bundle_dt) {
  dt_total_raw <- rbind(dt_not_split, dt_pred, fill = TRUE)
  cols_to_retain <- names(bundle_dt)
  dt_total <- dt_total_raw %>% select(all_of(cols_to_retain))
  return(dt_total)
}

# Recalculate uncertainty and set minimum variance threshold
calculate_uncertainties_with_min_variance <- function(dt, min_variance = 1e-18) {
  dt <- calc_uncertainty(dt)
  dt[, variance := standard_error ^ 2]
  dt[abs(variance) < min_variance, variance := min_variance]
  return(dt)
}

# Function to remove "Both" sex data points and adjust age groups
adjust_data_based_on_sex_and_age <- function(dt, age_metadata) {
  if (any(dt$sex == "Both")) {
    warning("There are ", nrow(dt[sex == "Both"]), " both-sex data points. Removing those rows...")
    dt <- dt %>% filter(sex != "Both")
  }
  
  dt[age_group_id == 238, `:=`(age_start = 1, age_end = 1)]
  dt[age_end %% 5 == 0, age_end := age_end - 1]
  dt[age_start == 0 & age_end == 1, age_group_id := 28]
  dt <- dt[age_group_id %in% unique(age_metadata$age_group_id),]
  dt[, `:=`(age_start = as.integer(age_start), age_end = as.integer(age_end))]
  dt[age_group_id == 238, `:=`(age_group_id = 28, age_start = 0, age_end = 1)]
  
  return(dt)
}

# Function to prepare data for upload by setting crosswalk sequences
prepare_data_for_upload <- function(dt, split_seq_list, origin_seq) {
  dt[, crosswalk_parent_seq := NA_integer_]
  dt[seq %in% split_seq_list, `:=`(crosswalk_parent_seq = origin_seq, seq = NA_integer_)]
  return(dt)
}

# Overall function to clean and format data for outliering
clean_and_format_data <- function(bundle_dt, dt_pred_v0, age_metadata) {
  split_seq_list <- unique(dt_pred_v0$seq)
  dt_not_split <- bundle_dt[!(seq %in% split_seq_list)]
  dt_not_split <- as.data.table(dt_not_split)
  
  dt_pred_v0[, cases := count_split]
  
  # Merge and select relevant columns
  dt_total_v0 <- merge_and_select(dt_not_split, dt_pred_v0, bundle_dt)
  
  # Calculate uncertainties and adjust variance
  dt_total_v1 <- calculate_uncertainties_with_min_variance(copy(dt_total_v0))
  
  # Adjust data based on sex and age
  pre_outliering_data <- adjust_data_based_on_sex_and_age(dt_total_v1, age_metadata)
  
  # Prepare data for upload
  pre_outliering_data <- prepare_data_for_upload(dt_total_v2, split_seq_list, dt_pred_v0$origin_seq)
  
  return(pre_outliering_data)
}

# Apply the overall function
pre_outliering_data <- clean_and_format_data(bundle_dt, dt_pred_v0, age_metadata)

# Saving
dir.create(paste0(run_dir, "data/crosswalk/"), showWarnings = FALSE)
pre_outliering_data_path <- paste0(run_dir, "data/crosswalk/pre_outliering_data_BVID_", bundle_version_id, ".csv")
fwrite(pre_outliering_data, pre_outliering_data_path)
