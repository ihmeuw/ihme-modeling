# Title: Final data formatting for bundle upload
# Input: processed flat files
# Output: bundle version ID [xxxxx]
# Author: USERNAME

# appends age group IDs
process_age_groups <- function(dt) {
  
  # merge to get age_group_id fronm GBD age metadata
  age_brackets <- unique(dt[, .(age_start, age_end)])
  age_brackets <- merge(age_brackets, complete_age_metadata, by = c("age_start", "age_end"), all.x = TRUE)
  
  # approximates nonstandard age groups to those in GBD IDs (complete_age_metadata from inputs_utils.R)
  nonstandard_ages <- age_brackets[is.na(age_group_id)]
  
  # Simplified function as per your description
  find_matching_age_group <- function(nsa_start, nsa_end, metadata) {
    # Find rows in metadata that match the age_start
    matches <- metadata[age_start == nsa_start]
    
    # If no match found, return NA
    if (nrow(matches) == 0) {
      return(list(age_group_id = NA, age_group_name = NA))
    }
    
    # If exactly one match found, return its ID and name
    if (nrow(matches) == 1) {
      return(list(age_group_id = matches$age_group_id, age_group_name = matches$age_group_name))
    }
    
    # If more than one match found, proceed to select based on age_end criteria
    if (nrow(matches) > 1) {
      matches[, diff_end := abs(age_end - nsa_end)]
      # Sort by diff_end, and then by age_end to prioritize lower age_end in case of a tie
      matches <- matches[order(diff_end, age_end)]
      # Select the first row after sorting
      closest_match <- matches[1]
      return(list(age_group_id = closest_match$age_group_id, age_group_name = closest_match$age_group_name))
    }
  }
  
  # Applying the function to each row in nonstandard_ages
  nonstandard_ages[, c("age_group_id", "age_group_name") := find_matching_age_group(age_start, age_end, complete_age_metadata), by = 1:nrow(nonstandard_ages)]
  
  if (nrow(nonstandard_ages[is.na(age_group_id)])>0) {
    cat(red("Some nonstandard age groups could not be approximated to GBD age groups."))
  }
  
  age_brackets <- rbind(age_brackets, nonstandard_ages) %>% na.omit()
  
  # when age_group_id is missing, age_group_name = paste0(age_start, " to ", age_end) and age_group_id == 283
  cat(yellow("Filling in missing age_group_ids with ID 283 ('Unknown') and naming them with age_start and age_end.\n"))
  
  # merge on updated age brackets
  dt <- merge(dt, age_brackets, by = c("age_start", "age_end"), all.x = TRUE)
  
  # check for missing age_group_id and state the number of those rows
  if (any(is.na(dt$age_group_id))) {
    cat(red("Missing some age_group_ids.\n"))
    cat(paste0(red("Number of rows with missing age_group_ids: ", nrow(dt[is.na(age_group_id) == TRUE, ]), "\n")))
    cat(yellow("Removing rows with missing age_group_ids.\n"))
    dt <- dt[!is.na(age_group_id), ]
  } else {
    cat(green("All age_group_ids are filled!"))
  }
  
  # cv_ indicator whether data point needs splitting
  age_group_ids <- unique(age_metadata$age_group_id)
  dt <- dt %>% 
    mutate(needs_split = ifelse(!(age_group_id %in% age_group_ids) | sex == "Both", 1, 0))
  
  return(dt)
  
}

# compute and format columns, assign outliers
prepare_sources <- function(formatted_list, 
                            exclude_zero_cases = exclude_zero_cases_bool, 
                            include_uses_env = include_uses_env_bool, # if TRUE, include and outlier; if FALSE, exclude altogether
                            include_proportion_measure = exclude_zero_cases_bool, # if TRUE assign all sources to measure=="continuous" to pass validations; if FALSE, exclude measure=="proportion" altogether
                            max_inlier_val = max_inlier_val_int) { # maximum value for inlier mean inp utilization; greater values will be outliered. NA_integer_ if N/A
  
  dt <- rbindlist(formatted_list, fill = TRUE)
  
  # Select required vars
  vars <- c(minimum_vars, "is_gbd_2021_survey")
  dt <- dt %>% 
    select(all_of(vars)) %>% 
    unique()
  
  # Fill in 0 where is_gbd_2021_survey is NA
  dt <- dt %>% 
    mutate(is_gbd_2021_survey = ifelse(is.na(is_gbd_2021_survey), 0, is_gbd_2021_survey))
  
  # Remove rows with sample_size == 0 or cases == 0 (~90 rows)
  if (exclude_zero_cases == TRUE) {
    dt <- dt %>% filter(!(sample_size == 0 | cases == 0))
  }
  
  # Val, lower, upper, standard_error, varianve ----
  # Get val (per capita admission rate - dependent variable, standard_error, lower, upper)
  dt <- dt %>% 
    mutate(val = cases / sample_size)
  dt <- calc_uncertainty(dt)
  
  # Calculate variance
  dt[, variance := standard_error ^ 2]
  
  # Default utliering ----
  # initialize is_outlier column to be filled with 0s and 1s
  dt <- dt %>% 
    mutate(is_outlier = NA_integer_)
  
  # remove sources using the envelope if indicated so; if not, mark them as outliers.
  if (include_uses_env == FALSE) {
    dt <- dt %>% 
      filter(uses_env == 0)  
  } else {
    dt <- dt %>% 
      mutate(is_outlier = ifelse(uses_env == 1, 1, is_outlier))
  }
  
  # Per argument, outlier per capita rates over specified value; do nothing if set to default (NA)
  if (is.na(max_inlier_val) == FALSE) {
    dt <- dt %>% 
      mutate(is_outlier = ifelse(val > max_inlier_val, 1, is_outlier))  
  }
  
  # Assign 0 to missing is_outlier values
  dt <- dt %>% 
    mutate(is_outlier = ifelse(is.na(is_outlier), 0, is_outlier))
  
  # Validate no outlier values are missing
  if (any(is.na(dt$is_outlier))) {
    stop("Some is_outlier values are missing.")
  }
  
  # if set to include proportions, assign all rows to continuous to pass uploader validations; otherwise exclude these rows
  if (include_proportion_measure == TRUE) {
    dt <- dt %>% 
      mutate(measure = "continuous")
  } else {
    dt <- dt %>% 
      filter(measure != "proportion")
  }
  
  # add sex from sex_ids
  sexes <- get_ids("sex")
  dt <- merge(dt, sexes, by = "sex_id", all.x = TRUE)
  
  # year_start and year_end = year_id
  dt <- dt %>% 
    mutate(year_start = year_id, year_end = year_id)
  
  # Remove pre 1980 data
  dt <- dt %>% filter(year_id >= 1980)
  
  # Process age group IDs
  dt <- dt %>% process_age_groups()
  
  # seq for bundle upload - values will be generated by epi uploader
  dt <- dt %>% 
    mutate(seq = NA_integer_)
  
}

# plot the result
plot_inputs_before_upload <- function(dt, plot_sources_using_env = FALSE) {
  
  # point to output directory
  plot_output_dir <- paste0(run_dir, "viz/")
  if (!dir.exists(plot_output_dir)) {
    dir.create(plot_output_dir, recursive = TRUE)
    print(paste0("Created directory: ", plot_output_dir))
  } else {
    print(paste0("Directory already exists: ", plot_output_dir))
  }
  
  # add location names for titles and and age midpoints for the x-axis (pdt = plot data table)
  pdt <- dt %>% 
    # merge on location metadata
    left_join(location_metadata[, .(location_id, location_name)], by = "location_id") %>%
    mutate(age_mid = (age_start + age_end) / 2)
  
  # filter to data not using the envelope if need be
  if (plot_sources_using_env == FALSE) {
    pdt <- pdt %>% 
      filter(uses_env == 0)
  }
  
  # series time will get you something like "Brazil Hospital Information System (SIH) (Clinical data)"
  pdt[, series_type := ifelse(is.na(series_field_citation_value) == FALSE,
                             paste0(series_field_citation_value, " (", data_type_name, ")"),
                             paste0(field_citation_value, " (", data_type_name, ")"))]
  
  # country name in parentheses for subnational locations
  countries_dt <- pdt %>% select(location_id) %>% unique()
  countries_dt <- merge(countries_dt, location_metadata[, .(location_id, level, parent_id)], by = "location_id", all.x = TRUE)
  countries_dt[, country_id := ifelse(level == 3, location_id, 
                                      ifelse(level == 4, parent_id, 163))]
  countries_dt <- countries_dt %>% select(location_id, country_id) %>% unique()
  countries_dt <- merge(countries_dt, location_metadata[, .(location_id, location_name)], by.x = "country_id", by.y = "location_id", all.x = TRUE)
  setnames(countries_dt, "location_name", "country_name")
  
  pdt <- merge(pdt, countries_dt, by = "location_id", all.x = TRUE)
  
  # indicate x-axis breaks and locations to iterate over
  age_metadata <- age_metadata %>% 
    mutate(age_mid = (age_start + age_end) / 2) 
  breaks <- age_metadata %>% 
    setorder(age_mid) %>% 
    pull(age_mid)
    
  locations_vector <- sort(unique(pdt$location_id))
  
  # plot in a PDF
  pdf_name <- paste0(plot_output_dir, "Run ", run_id, " Bundle data sex facets", 
                     # format(Sys.time(), "%Y_%m_%d_%H_%M_%S"), 
                     ".pdf")
  pdf(pdf_name, width = 16, height = 9) # album orientation with 16:9 ratio for presentations
  
  
  for (plot_location_id in locations_vector) {
    
    plot_location_name <- pdt[location_id == plot_location_id, unique(location_name)]
    
    # Subset the input data to the current country
    dt_input_subset <- pdt[location_id == plot_location_id, ]
    
    if (all(is.na(dt_input_subset$val))) {
      cat("Skipping input data plots: All values are NA for", plot_location_name, "\n")
    } else {
      cat("Plotting input data for", plot_location_name, "(location ID", plot_location_id, ")\n")
      
      plot_input <- ggplot() +
        geom_point(data = dt_input_subset,
                   aes(x = age_mid, y = val, color = factor(year_id), group = interaction(nid, year_id), alpha = 0.5)) +
        geom_line(data = dt_input_subset,
                  aes(x = age_mid, y = val, color = factor(year_id), group = interaction(nid, year_id)), 
                  alpha = 0.5) +
        geom_errorbarh(data = dt_input_subset[!(age_group_id %in% age_metadata$age_group_id),],
                       aes(x = age_mid, xmin = age_start, xmax = age_end, y = val, color = factor(year_id), group = interaction(nid, year_id)),
                       alpha = 0.5) +
        ggtitle(paste0("Raw data for bundle upload ", plot_location_name, " ( location ID ", plot_location_id, ")")) +
        scale_color_viridis_d() +
        facet_wrap(~factor(sex), ncol = length(unique(dt_input_subset$sex)), scales = "fixed") +
        scale_x_continuous(breaks = age_metadata[order(age_mid), age_mid],
                           labels = age_metadata[order(age_mid), age_group_name],
                           limits = c(0,125)) +
        labs(x = "Age group", y = "Inpatient utilization rate per capita") +
        theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5, size = 8),
              plot.title = element_text(face = "bold", hjust = 0.5),
              plot.subtitle = element_text(size = 8, margin = margin(10, 0, 10, 0)),
              legend.title = element_text(face = "bold"),
              legend.position = "bottom",
              legend.direction = "horizontal",
              legend.key.size = unit(1, "cm"))
      
      print(plot_input)
      
    }
  }
  
  # Close the PDF device
  dev.off()
  
  
  # Plot facets by age group
  # plot in a PDF
  pdf_name <- paste0(plot_output_dir, "Run ", run_id, " Bundle data age facets", 
                     # format(Sys.time(), "%Y_%m_%d_%H_%M_%S"), 
                     ".pdf")
  pdf(pdf_name, width = 16, height = 12) # album orientation with 16:9 ratio for presentations
  
  # Loop through each unique location_id and plot
  for(loc_id in locations_vector) {
    # Filter the data for the current location
    dt_location <- pdt[location_id == loc_id, ]
    dt_location$location_name <- iconv(dt_location$location_name, to = "UTF-8")
    loc_name <- unique(dt_location$location_name)
    country <- unique(dt_location$country_name)
    
    cat(yellow("\nPlotting for location_id: ", loc_id, " ", loc_name))
    
    # Determine the max mean for setting y-axis limits
    max_mean <- max(dt_location$mean, na.rm = TRUE) + 0.1
    
    # Create the plot
    p <- ggplot(dt_location, aes(x = year_id, y = val, shape = sex, color = series_type, size = sample_size, group = interaction(series_type, sex))) +
      geom_point(alpha = 0.6, size = 2.5) +  # semi-translucent points
      scale_color_viridis_d(option = "H") +
      facet_wrap(~age_group_name, scales = "free") +  # Facet by age_group_name with borders
      labs(title = paste0(loc_id, " ", loc_name, " - ", country)) +
      theme_minimal(base_family = "sans") +
      theme(plot.title = element_text(size = 14, face = "bold", hjust = 0.5, margin = margin(10, 0, 10, 0)), 
            axis.title = element_text(size = 12),
            panel.spacing = unit(2, "lines"),  # Adjust spacing between facets
            axis.text.x = element_text(angle = 45, vjust = 0.5, hjust = 1, size = 8),
            legend.position = "bottom",  # Move legend to the bottom
            legend.box = "horizontal",  # Align legend items horizontally)
            legend.text = element_text(size = 8),  # Adjust legend text size
            legend.title = element_text(size = 9),
            legend.key.size = unit(0.5, "cm"),  # Adjust legend key size
            strip.background = element_blank()) +  # Remove facet label background
      ylab("Inpatient admissions per capita") +
      scale_x_continuous(breaks = seq(1990, 2020, by = 5), limits = c(1990, 2024), 
                         labels = seq(1990, 2020, by = 5)) +
      scale_y_continuous(limits = c(0, NA))  # Fix lower y-limit to 0 and upper y-limit free
    
    # Print the plot to the PDF
    print(p)
  }
  
  # Close the PDF device
  dev.off()
  
}
