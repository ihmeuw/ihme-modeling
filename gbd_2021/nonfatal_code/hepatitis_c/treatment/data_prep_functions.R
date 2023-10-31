## #############################################################################
## 
## HEPATITIS C TREATMENT DATA PREP FUNCTIONS
## #############################################################################

## load_data() -------------------------------------------------------------->
#' 
#' @title Load in data from CGHE 
#' 
#' @description Load in data from CGHE and subset based on ihme_loc_id
#' 
#' @param in_data Full input dataset, containing the field "ihme_loc_id" with ihme_loc_id or blank 
#'

load_data <- function(data_dir, ihme_locid) {
  # Load data 
  in_data <- fread(file.path(FILEPATH))
  # Clean columns
  in_data$number_treated <- str_trim(in_data$number_treated)
  in_data$number_treated <- as.numeric(in_data$number_treated)
  in_data <- in_data[, -c("information_on_future_rx_plans", "notes", "link_to_source")]
  return(in_data)
}

## get_age_meta_info() -------------------------------------------------------------->
#' 
#' @title Get GBD age_metadata 
#' 
#' @description Get GBD age metadata information and change age end to be -1 
#' 
#'

get_age_meta_info <- function() {
  age_group_meta <- get_age_metadata(
    age_group_set_id=CONF$get_value('age_group_set_id'),
    gbd_round_id=CONF$get_value('gbd_round_id')
  )
  return(age_group_meta)
}



## create_single_years() -------------------------------------------------------------->
#' 
#' @title Convert all rows to be a single year and adjust number treated to be equal 
#' 
#' @description Convert all rows to be a single year and adjust number treated to be equal 
#' 
#' @param full_data Data set subsetted by the location of interest
#' 
#' @return A data frame with the expanded years such that year_start and year_end is the same
#'         and year_range is zero 
#'

create_single_years <- function(full_data) {
  full_data[, year_range := year_end - year_start]
  setDT(full_data)
  var <- c('location_name', 'ihme_loc_id', 'location_id', 'sex', 'year_start', 'year_end', 'age_start', 'age_end', 'number_treated', 'treatment_type', 'year_range')
  full_data <- full_data[, .(year = year_start:year_end), by=var]
  full_data[, `:=` (year_start = year, year_end = year, year = NULL)]
  full_data[, `:=`( number_treated = number_treated / (year_range + 1), year_range = NULL)]
  return(full_data)
}

## adjustment_incomplete_year_data() -------------------------------------------------->
#' 
#' @title Extrapolate rows with incomplete data to the entire year
#' 
#' @description Extrapolate all rows wtih incomplete data (ex. treatment data available only for Jan - June), it uses the "incomplete_year_adjustment" variable to full year
#' 
#' @param full_data Data set subsetted by the location of interest
#' 
#' @return A data frame with the expanded years such that year_start and year_end is the same
#'         and year_range is zero and with extrapolated data
#'

adjustment_incomplete_year_data <- function(full_data) {
  dt <- copy(full_data)
  setDT(dt)
  dt[((ihme_loc_id == "RWA" | ihme_loc_id=="GBR_4749") & year_start == 2020), `:=` (incomplete_year_adjustment = NA)]
  dt[(!is.na(incomplete_year_adjustment)), `:=` (number_treated = number_treated / (incomplete_year_adjustment/12))]
  return(dt)
}
  


## add_sex_ids() -------------------------------------------------------------->
#' 
#' @title Add sex IDs to data
#' 
#' @description Given data that is split by sex, assign GBD sex IDs to the data,
#'   including an ID for both sexes / unknown / other
#' 
#' @param in_data Full input dataset, containing the field "sex" with one of
#'   "male", "female", and blank
#'
add_sex_ids <- function(in_data){
  # Clean input sex field
  in_data[, sex := tolower(as.character(sex)) ]
  in_data[sex == "both", sex_id := 3]
  in_data[sex == "male", sex_id := 1]
  in_data[sex == "female", sex_id := 2]
  return(in_data)
}

# create_prevalent_cases() -------------------------------------------------> 
#' 
#' @title Create prevalent cases 
#' 
#' @description This function pulls in prevalence rate using interpolate and 
#'             populations and then multiplies draws to get populations 
#' @param in_data Takes full data to determine what the minimum reporting year is 
#'                  Will break if this is not an estimation year 
#' 

create_cases <- function(in_data, loc_id, measure) {
  draws <- paste0("draw_", 0:999)
  # message("Getting estimation years")
  
  min_reporting_year <- min(round_any(in_data$year_start, 5, f = floor))
  estimation_years <- CONF$get_value('estimation_years')
  if(!(min_reporting_year %in% estimation_years)) stop('Minimum reporting year is not an estimation year')
  
  message("Interpolating")
  prev_rate <- interpolate(gbd_id_type='modelable_entity_id', 
                           gbd_id=CONF$get_value("input_me_id"), 
                           source='epi', 
                           age_group_id = 'all',
                           measure_id=measure, 
                           location_id=loc_id, 
                           sex_id=c(1,2), 
                           gbd_round_id=CONF$get_value("gbd_round_id"), 
                           decomp_step = CONF$get_value("gbd_decomp_step"),
                           reporting_year_start=min_reporting_year, 
                           reporting_year_end=2022)
  
  pop <- get_population(location_id=loc_id, 
                        age_group_id = 'all',
                        year_id=unique(prev_rate$year_id), 
                        sex_id="all", 
                        gbd_round_id=CONF$get_value('gbd_round_id'), 
                        decomp_step = CONF$get_value('gbd_decomp_step'))
  pop$run_id <- NULL
  cases <- merge(prev_rate, pop, by = c("age_group_id", "location_id", "year_id", "sex_id"))
  cases[, (draws) := lapply(.SD, function(x)  x * population), .SDcols = draws,
        by = c("location_id", "year_id", "age_group_id", "sex_id")]
  
  message("Done calculating cases")
  return(cases)
}


## sex_split_data() -------------------------------------------------------->
#' 
#' @title Convert both sex data into sex specific
#' 
#' @description Use estimates of chronic hepatitis C to determine the sex ratio for year and mid age of a row
#' 
#' @param data Uses data subset to give reporting_year_start, GBD populations, and 
#'           chronic hepatitis C estimates 
#'           
sex_split_data <- function(data, age_group_meta, cases) {
  full_data <- copy(data)
  adjust_both <- copy(full_data[sex_id == 3])
  if (nrow(adjust_both) > 0 ) {
    message("Both sex rows exist. Sex spliting")
    
    # Subset to the sex specific rows that do not need to be split, save these for binding on later   
    sex_specific <- copy(full_data[sex_id %in% c(1,2)])
    n <- names(sex_specific)
    
    # Figure out year values that need to be split and age mid values to determine ratio 
    adjust_both[, year := year_start]
    adjust_both[, age_mid := (age_start + age_end)/2]
    adjust_both$age_group_years_start <- round_any(adjust_both$age_mid, 5, f = round)
    
    # Add on age group
    adjust_both <- merge(adjust_both, age_group_meta[, .(age_group_id, age_group_years_start)], by = "age_group_years_start")
    
    # Melt the cases so you can add the draws across sexes and get total cases 
    cases_melt <- melt.data.table(cases, measure.vars = draws)
    cases_melt_m <- copy(cases_melt[sex_id == 1])
    setnames(cases_melt_m, "value", "m_value")
    cases_melt_f <- copy(cases_melt[sex_id == 2])
    setnames(cases_melt_f, "value", "f_value")
    
    # Add male and female draws and calculate ratios 
    ratios <- merge(cases_melt_m, cases_melt_f, by = c('age_group_id', 'location_id', 'year_id', 'variable'))
    ratios[, t_value := m_value + f_value]
    ratios[, m_ratio := m_value / t_value]
    ratios[, f_ratio := f_value / t_value]
    ratios_m <- copy(ratios[, c('age_group_id', 'location_id', 'year_id', 'variable', 'm_ratio')])
    ratios_f <- copy(ratios[, c('age_group_id', 'location_id', 'year_id', 'variable', 'f_ratio')])
    
    # Estimate the mean 
    ratios_m <- dcast.data.table(ratios_m, location_id + age_group_id + year_id ~ variable, value.var = "m_ratio")
    ratios_m[, m_ratios := rowMeans(.SD), .SDcols = draws]
    ratios_m[, (draws) := NULL]
    setnames(ratios_m, 'year_id', 'year_start')
    ratios_f <- dcast.data.table(ratios_f, location_id + age_group_id + year_id ~ variable, value.var = "f_ratio")
    ratios_f[, f_ratios := rowMeans(.SD), .SDcols = draws]
    ratios_f[, (draws) := NULL]
    setnames(ratios_f, 'year_id', 'year_start')
    
    # Apply the ratios to the adjusted data and set the sex_id
    ratios_m <- merge(adjust_both, ratios_m, by = c('location_id', 'year_start', 'age_group_id'))
    ratios_m[, `:=` (number_treated = number_treated * m_ratios, sex_id = 1, sex = 'male')]
    ratios_f <- merge(adjust_both, ratios_f, by = c('location_id', 'year_start', 'age_group_id'))
    ratios_f[, `:=` (number_treated = number_treated * f_ratios, sex_id = 2, sex = 'female')]
    
    # Clean the column names 
    ratios_m <- ratios_m[, ..n]
    ratios_f <- ratios_f[, ..n]
    
    # Bind the sex split and sex specific rows back to create full data set 
    adjusted <- rbind(sex_specific, ratios_m, ratios_f)
    message("Done sex splitting.")
    return(adjusted) 
  }
  else {
    message("No both sex rows exist. Do not need to sex split")
    return(full_data)
  }
}


## format_age_ranges() -------------------------------------------------------->
#' 
#' @title Format age ranges
#' 
#' @description Given an input mortality dataset, format age ranges into a  
#'   standard format using decimal age_years and age_years_end
#' 
#' @param full_data Full input dataset, with unformatted age ranges defined by
#'   six columns: age_years, age_months, age_days, age_years_end, age_months_end,
#'   age_days_end
#' 
#' @return data.table with only two age fields: age_years_start and age_years_end
#' 
format_age_ranges <- function(full_data){
  # All data must have the following columns:
  required_cols <- c(
    'age_start','age_end'
  )
  missing_cols <- required_cols[!required_cols %in% colnames(full_data)]
  if(length(missing_cols) > 0) stop(paste0('Missing columns: ',missing_cols, collapse=', '))
  # Convert all age columns to numeric to avoid loss of precision
  for(col in required_cols) full_data[[col]] <- as.numeric(full_data[[col]])
  # Any rows without the age_years column filled must be dropped  
  missing_age_years <- nrow(full_data[is.na(age_start) | is.na(age_end)])
  if(missing_age_years > 0){
    message(glue("Dropping {missing_age_years} rows missing required age_years field."))
    full_data <- full_data[!is.na(age_years),]
  }
  

  full_data[(age_start > 0.0767) & (age_end < 0.084),  `:=` (age_start = 0.0767, age_end = 0.0767) ]
  full_data[(age_start > 0.0191) & (age_end < 0.0195),  `:=` (age_start = 0.0191, age_end = 0.0191) ]
  return(full_data)
}

## age_split_data() -------------------------------------------------------->
#' 
#' @title Age split data, must be sex specific first 
#' 
#' @description Age split the data into gbd age groups and split based on chronic hepatitis C
#'              Dismod age ratios; sum the youngest age groups but not going to track the reduction 
#' 
#' @param data Data set with sex split information 
#' 
#' @return Age split data; if a location only has 1 age group then the original age start and end will be used 
#' 

age_split_data <- function(data, age_group_meta, cases, drop_age_groups = F) {
  
  # 1. Format the data 
  data <- as.data.table(data)
  n <- c(names(data), "age_group_id")
  data[, measure_id := 5]
  data[, year_id := year_start]
  ages <- copy(age_group_meta)
  ages[age_group_years_start >= 2 & age_group_years_start < 95, age_group_years_end := age_group_years_end - 1 ]
  setnames(ages, c("age_group_years_start", "age_group_years_end"), c("age_start", "age_end"))
  
  ## Add in a stop in case sex splitting did not go as expected 
  if (3 %in% unique(data$sex_id)) stop("Sex-split before age-splitting.")
  
  # Add on age_group_id first to see if you need to age split 
  data[age_end > 99, age_end := 99]
  data1 <- merge(data, ages[, .(age_group_id, age_start, age_end)], by = c("age_start", "age_end"), all.x = T)
  to_split <- data1[is.na(age_group_id) & age_start != 95]
  setnames(to_split, 'age_group_id', 'age_grp_id')
  to_split[, `:=` (age_start_orig = age_start, age_end_orig = age_end)]
  
  no_split <- data1[!(is.na(age_group_id)) | age_start == 95 ]
  
  if(nrow(to_split) ==0) {
    if(nrow(no_split) != nrow(data1)) stop("Missing some rows. Stop!")
    no_split[age_start == 95, age_group_id := 235]
    return(no_split)
  } else {
    
    # Split the data here 
    
    # Create id variable
    to_split[, g_id := .GRP, by = c("age_start", "age_end", "location_id", "location_name", "ihme_loc_id", "treatment_type")]
    message("There are ", max(to_split$g_id), " groups of data that need to be split. ")
    
    df_total <- data.table()
    for (gr_id in unique(to_split$g_id)) {
      
      # make this based on what needs to be split 
      # if gr_id is 0 
      to_split1 <- to_split[g_id == gr_id]
      
      to_split1[, id := .GRP, by=names(data)]
      
      # 2. Create new age rows
      message("Creating new age rows.")
      
      ## Round age groups
      to_split1[, age_start := age_start - age_start %%5]
      to_split1[, age_end := age_end - age_end %%5 + 4]
      
      ## n_age is the number of rows the data point will be split into.
      to_split1[, n_age := (age_end + 1 - age_start)/5]
      to_split1[, age_start_floor := age_start]
      
      if(unique(to_split1$n_age == 1)) {
        dt <- copy(to_split1)
        dt[, `:=` (age_start = age_start_orig, age_end = age_end_orig)]
        message("Only 1 age group. Do not need to split.")
        df_total <- rbind(df_total, dt, fill = TRUE)
        message("Done with group ", gr_id, " of", max(to_split$g_id))
      } else {
        
        ## Expand for age. Create n_age number of points for each ID (row).
        expanded <- data.table(id = rep(to_split1$id, to_split1$n_age))
        split_dt <- merge(expanded, to_split1, by="id", all=T)
        split_dt[, age_rep := 1:.N - 1, by=.(id)]
        
        ## Use age_rep to re-assign age_start and age_end
        split_dt[, age_start := age_start + age_rep * 5]
        split_dt[, age_end := age_start + 4]
        
        split_dt <- merge(split_dt, ages, by=c("age_start", "age_end"), all.x=T)
        split_dt[age_start == 0 & age_end == 4, age_group_id := 1]
        split_dt[age_start == 95 & age_end == 99, age_group_id := 235]
        if (!is.null(drop_age_groups)) split_dt <- split_dt[!(age_group_id %in% drop_age_groups),]
        
        #3. Get prevalent cases from dismod and determine age pattern 
        # cases <- create_prevalent_cases(split_dt)
        
        ## Collapse draws under 1
        cases_1 <- copy(cases)[age_group_id %in% c(2, 3, 388, 389)]
        cases_1[ , (draws) :=  lapply(.SD, function(x) sum(x)), .SDcols=draws, by=c("location_id", "year_id", "sex_id")]
        cases_1 <- unique(cases_1, by = c("location_id", "year_id", "sex_id"))
        cases_1[, age_group_id := 1]
        cases <- rbind(cases[!(age_group_id %in% c(2, 3, 388, 389))], cases_1)
        
        cases_melt <- melt.data.table(cases, measure.vars = draws)
        cases_melt[, total_cases := sum(value), by = c('location_id', 'year_id', 'sex_id', 'variable')]
        cases_melt[, age_ratio := value / total_cases]
        
        cases_melt <- cases_melt[, .(age_group_id, location_id, year_id, sex_id, variable, age_ratio)]
        
        
        age_ratios <- dcast.data.table(cases_melt, location_id + age_group_id + sex_id + year_id ~ variable , value.var = 'age_ratio')
        age_ratios[, age_ratio := rowMeans(.SD), .SDcols = draws]
        age_ratios[, (draws) := NULL]
        
        split_dt <- merge(split_dt, age_ratios, by = c('location_id', 'sex_id', 'age_group_id', 'year_id'), all.x = T)
        if(nrow(split_dt[is.na(age_ratio)]) > 0 ) stop(message('Age ratios are missing. Stop!'))
        
        message("Applying age ratios to cases treated.")
        split_dt[, number_treated := number_treated * age_ratio]
        
        split_dt <- split_dt[, ..n]
        
        df_total <- rbind(df_total,split_dt, fill = TRUE)
        message("Done with group ", gr_id, " of ", max(to_split$g_id))
      } 
      # rows that did not need to be age split 
      good_data <- copy(no_split) 
      # bind together 
      dt <- rbind(good_data, df_total, fill = TRUE)
    }
    message("Done age splitting.")
    return(dt)
  }
}

## clean_age_group_ids() -------------------------------------------------------->
#' 
#' @title Clean age group ids to make sure that 
#' 
#' @description Making sure the age split data set has age_group_ids filled in for all rows 
#' 
#' @param data year, sex and age split data 
#' 
#' @return data set with age group id 
#' 

clean_age_group_ids <- function(data) {
  if(any(is.na(data$age_group_id))) {
    data[is.na(age_group_id) & age_start < 80, age_group_id := floor(((age_start - 15) /5 ) + 8)]
    data[is.na(age_group_id) & age_start > 79,age_group_id := floor(((age_start - 15) /5 ) + 17)]
    data[is.na(age_group_id) & age_start > 95,age_group_id :=235]
  } else{
    message("No missing age group ids")
  }
  return(data)
}

## check_treated_cases() -------------------------------------------------------->
#' 
#' @title Check the percentage of the the processed/original cases to see how much was dropped 
#' 
#' @description Sums the total cases from the original extraction and from the processed data 
#'              The pipeline will stop if the percentage is not between 0.95 and 1 
#' 
#' @param original_data original extracted data 
#' @param processed_data year, sex, age split data 
#' 

check_treated_cases <- function(original_data, processed_data) {
  orig_total <- original_data[, sum(number_treated)]
  processed_total <- processed_data[, sum(number_treated)]
  percent <- orig_total/processed_total
  return(percent)
  if(percent < 0.95 | percent > 1.01 ) stop(message('Percentage of dropped falls outside reasonable range.'))
}



## apply_treatment_efficacy() -------------------------------------------------------->
#' 
#' @title Applying treatment efficacy based on different types 
#' 
#' @description Applying treatment efficacy to the number treated based on efficacy values 
#'              0.70 IFN and triple therapy 
#'              0.95 DAA
#' 
#' @param data year, sex and age split data 
#' 
#' @return data set with efficacy applied 
#' 

apply_treatment_efficacy <- function(data){
  data[grepl("DAA", treatment_type), efficacy := 0.95]
  data[grepl("IFN", treatment_type), efficacy := 0.70]
  if(nrow(data[is.na(efficacy)]) > 0 ) stop(message('Efficacy information is missing.'))
  
  data[, treated := number_treated * efficacy]
  
  return(data) 
}

## clean_final_data() -------------------------------------------------------->
#' 
#' @title Sum the data across groups and check for NAs
#' 
#' @description 
#' 
#' @param data year, sex, age split and treatment efficacy data 
#' 
#' @return cleaned data set 


clean_final_data <- function(data) {
  data[, total_treated := sum(treated), by = c("sex_id", "location_id", "location_name", "ihme_loc_id", "year_start", "year_end", "age_group_id")]
  
  if(any(data$year_start != data$year_end )) stop(message('Year start and year end do not match. Stop.'))
  data[, year_id := year_start]
  keep_cols <- c('location_name', 'ihme_loc_id', 'location_id', 'year_id', 'sex_id', 'age_group_id', 'total_treated')
  data <- data[, ..keep_cols]
  data <- unique(data)
  for (col in names(data)) {
    if (nrow(data[is.na(get(col))]) > 0) {
      stop(paste(col, "has missing data"))
    } 
  }
  return(data)
}

## append_pdf() -------------------------------------------------------------->
#'
#' @title append_pdf 
#'
#' @description binds all binds that start with a certain pattern together 
#'
#' @output creates 1 pdf will all files in it 
#' 
append_pdf <- function(dir, starts_with) {
  files <- list.files(dir, pattern = paste0("^", starts_with), full.names = T)
  files <- paste(files, collapse = " ")
  cmd <- paste0("/usr/bin/ghostscript -dBATCH -dSAFER -dNOGC -DNOPAUSE -dNumRenderingThreads=4 -q -sDEVICE=pdfwrite -sOutputFile=",
                dir, "/", starts_with, ".pdf ", files)
  system(cmd)
}