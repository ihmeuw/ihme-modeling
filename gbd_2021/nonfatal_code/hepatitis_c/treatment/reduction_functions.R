## #############################################################################
## 
## HEPATITIS C TREATMENT REDUCTIONS FUNCTIONS
## 
## #############################################################################

## apply_reduction() -------------------------------------------------------------->
#' 
#' @title Apply the treatment reduction logic 
#' 
#' @description Apply the treatment reductions to n_draws which are new draws 
#'              #' The logic is:
#'              #' Value 1 is the half year correction in year of intervention 
#'              #' Value 2: Previous years of treatment when you were in the same age group
#'              #' Value 3: Previous years of treatment when you were in the previous age_group 
#'              
#' 
#' @param in_data Full input dataset, containing the field "ihme_loc_id" with ihme_loc_id or blank 
#'
#' 

apply_reduction <- function(df) {
  # Store the min year and the min age group 
  min_year <- min(df$year_id)
  if(is.na(min_year)) stop('Minimum year is NA. Stop')
  min_age_group <- min(df$age_group_id)
  if(is.na(min_age_group)) stop('Minimum age_group_id is NA. Stop')
  
  # Sort the data so you can apply shift 
  df <- df[order(age_group_id, year_id), ]
  df <- df[order(factor(sex_id,levels = c(1,2)))]
  
  message("Working on reductions")
  
  num_shifts <- length(unique(df$year_id)) - 1
  if(num_shifts > 21) stop('Need to expand age shift logic')
  # Shift years 
  df <- setDT(df)[, paste0("treated_t_", 1:3) := dt.shift(total_treated, 1:3), by = c("age_group_id", "sex_id", "location_id")][]
  # Shift age groups by cohorts 
  df <- setDT(df)[, paste0("treated_a_", 1:5) := dt.shift(total_treated, num_shifts+2:(2*num_shifts)+2), by = c("sex_id", "location_id")][]
  df <- setDT(df)[, paste0("treated_a_", 6:10) := dt.shift(total_treated, 2*num_shifts+5:(3*num_shifts)+5), by = c("sex_id", "location_id")][]
  df <- setDT(df)[, paste0("treated_a_", 11:15) := dt.shift(total_treated, 3*num_shifts+8:(4*num_shifts)+8), by = c("sex_id", "location_id")][]
  df <- setDT(df)[, paste0("treated_a_", 16:19) := dt.shift(total_treated, 4*num_shifts+11:(5*num_shifts)+11), by = c("sex_id", "location_id")][]
  df <- setDT(df)[, paste0("treated_a_", 21:25) := dt.shift(total_treated, 5*num_shifts+14:(6*num_shifts)+14), by = c("sex_id", "location_id")][]
  df <- setDT(df)[, paste0("treated_a_", 26:30) := dt.shift(total_treated, 6*num_shifts+17:(7*num_shifts)+17), by = c("sex_id", "location_id")][]
  
  # fill NAs for after current year - 3 to min year 
  treated_a_cols <- names(df)[grepl("treated_a_", names(df))]
  df[, year_shifting := year_id - 3]
  df[, age_shifts := year_shifting - min_year + 1]
  
  for (i in -2:max(df$age_shifts)) {
    df[age_shifts == i, paste0("treated_a_",i+1:19)] <- NA 
  }
  
  df[age_shifts %in% c(-2, -1, 0), treated_a_cols] <- NA
  
  # create sum of treated_a
  df$treated_a_sum <- rowSums(df[, ..treated_a_cols], na.rm = TRUE)
  
  #' First year of treatment 
  #' Only receive 1/2 the treatment effect 
  group_cols <- c("age_group_id", "location_id", "year_id", "sex_id")
  df[year_id == min_year, (n_draws) := lapply(.SD, function(x) x - (0.5 * total_treated)), .SDcols = draws, by = group_cols]
  
  #' Second year of treatment 
  #' Receive the full treatment from the previous year and 1/2 treatment effect of current year 
  if(any(df$year_id == (min_year + 1))) {
    # message("Year id ", min_year +1, " is present")
    df[year_id == min_year + 1, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - (0.5 * total_treated)), .SDcols = draws, by = group_cols]
  }
  
  #' Third year of treatment 
  #' Receive the full treatment from the previous 2 years and 1/2 treatment effect of current year 
  if(any(df$year_id == (min_year + 2))) {
    df[year_id == min_year + 2, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - treated_t_2 - (0.5 * total_treated)), .SDcols = draws, by = group_cols]
  }
  
  #' Fourth year of treatment or greater and the first age group 
  #' Receive the full treatment from the previous 2 years and 1/2 treatment effect of current year 
  #' This will differ for the non minimum year age group because of progression of age through time 
  if(any(df$year_id == (min_year + 3))) {
    for (i in 3:num_shifts) {
      df[year_id == min_year + i & age_group_id == min_age_group, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - treated_t_2 - (0.5 * total_treated)), .SDcols = draws, by = group_cols]
    }
  }
  
  #' Fourth year of treatment or greater and not the minimum age group 
  #' Receive the full treatment from the previous 2 years and 1/2 treatment effect of current year AND full year of treatment for previous age group 
  #' This will differ for the non minimum year age group because of progression of age through time 
  if(any(df$year_id == (min_year + 3))) {
    for (i in 3:num_shifts) {
      df[year_id == min_year + i & age_group_id != min_age_group, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - treated_t_2 - treated_a_sum - (0.5 * total_treated)), .SDcols = draws, by = group_cols]
    }
  }
  
  # Drop treated_t or treated_a columns 
  drop_cols <- names(df)[grep("treated_t|treated_a", names(df))] 
  # drop_cols <- drop_cols[!drop_cols == "treated_a_sum"] # I can't remember why I put this in so commenting out for now 
  df[, (drop_cols) := NULL]
  
  # Convert any negative values to zero 
  df[df < 0] <- 0
  
  # Check for NAs 
  if(nrow(df[is.na(n_draw_0)]) > 0 ) stop("There are NAs in the dataset. Stop")
  
  return(df)
}

## predict_treatment() -------------------------------------------------------------->
#' 
#' @title Adjust draws for every year until final estimation after last year of known treatment information 
#' 
#' @description Identifies the max year of known treatment information and the max year of estimation years 
#'              Calculates the proportion reduction after treated in the last year and multiplies that times 
#'              the cases draws 
#'              Bind the adjusted and forecasted draws together 
#'              Ensure that no values are negative 
#'              
#' @param df Reduction data after logic is applied 
#' @param prev_cases Draws from DisMod 
#' @param covid_estimation T or F value depending on if we want to use covid estimation rules 
#' 
#' @return full data set of chronic number of cases for all required years 
#' 

predict_treatment <- function(df, prev_cases, covid_estimation) {
  # Figure out what years to predict 
  if(covid_estimation == T) {
    print("Estimating with COVID rules")
  }
  max_year <- max(df$year_id)
  min_year <- min(df$year_id)
  group_cols <- c("age_group_id", "location_id", "year_id", "sex_id")
  fill_years <- c((max_year+1):max(CONF$get_value('estimation_years')))
  covid_year <- CONF$get_value('covid_year')
  p_cases <- copy(prev_cases[year_id %in% fill_years])
  
  # Bind cases and adjusted data together and order
  keep_ages <- df[, unique(age_group_id)]
  loc_name <- df[!is.na(location_name), unique(location_name)]
  ihme_loc_name <- df[!is.na(ihme_loc_id), unique(ihme_loc_id)]
  df <- rbind(df, p_cases[age_group_id %in% keep_ages], fill = TRUE)
  df <- setorder(df, year_id) 
  df <- setorder(df, age_group_id)
  df <- df[order(factor(sex_id,levels = c(1,2)))]
  
  orig_cols <- names(df)
  orig_cols <- orig_cols[!(grepl("treated_", orig_cols))]
  
  for (j in 1:length(fill_years)) {
    num_shifts <- (length(unique(df$year_id)) - 1)
    if(num_shifts > 26) stop(paste0('Need to expand age shift logic. Num shifts is ', num_shifts))
    # Shift years 
    df <- setDT(df)[, paste0("treated_t_", 1:2) := dt.shift(total_treated, 1:2), by = c("age_group_id", "sex_id", "location_id")][]
    df[is.na(treated_t_1), treated_t_1 := 0]
    df[is.na(treated_t_2), treated_t_2 := 0]
    # Shift age groups by cohorts 
    df <- setDT(df)[, paste0("treated_a_", 1:5) := dt.shift(total_treated, num_shifts+2:(2*num_shifts)+2), by = c("sex_id", "location_id")][]
    df <- setDT(df)[, paste0("treated_a_", 6:10) := dt.shift(total_treated, 2*num_shifts+5:(3*num_shifts)+5), by = c("sex_id", "location_id")][]
    df <- setDT(df)[, paste0("treated_a_", 11:15) := dt.shift(total_treated, 3*num_shifts+8:(4*num_shifts)+8), by = c("sex_id", "location_id")][]
    df <- setDT(df)[, paste0("treated_a_", 16:20) := dt.shift(total_treated, 4*num_shifts+11:(5*num_shifts)+11), by = c("sex_id", "location_id")][]
    df <- setDT(df)[, paste0("treated_a_", 21:25) := dt.shift(total_treated, 5*num_shifts+14:(6*num_shifts)+14), by = c("sex_id", "location_id")][]
    df <- setDT(df)[, paste0("treated_a_", 26:30) := dt.shift(total_treated, 6*num_shifts+17:(7*num_shifts)+17), by = c("sex_id", "location_id")][]

    # fill NAs for after current year - 3 to min year 
    treated_a_cols <- names(df)[grepl("treated_a_", names(df))]
    df[, year_shifting := year_id - 3]
    df[, age_shifts := year_shifting - min_year + 1]
    
    for (i in -2:max(df$age_shifts)) {
      df[age_shifts == i, paste0("treated_a_",i+1:25)] <- NA 
    }
    df[age_shifts %in% c(-2, -1, 0), treated_a_cols] <- NA  
    
    # create sum of treated_a
    df$treated_a_sum <- rowSums(df[, ..treated_a_cols], na.rm = TRUE)
    
    # Create proportion treated in max year 
    df[, mean_value := rowMeans(.SD), .SDcols = draws]
    df[, prop_treated := (total_treated)/(mean_value - treated_t_1 - treated_t_2 - treated_a_sum)]
    df[prop_treated < 0, prop_treated := 0 ]
    df[, prop_treated_1 := dt.shift(prop_treated, 1), by = c("age_group_id", "sex_id", "location_id")]
    
    # If countries have their own 2020 and 2021 treatment data, it should be applied
    if (locs_id == 35| locs_id == 71 | locs_id ==101 | locs_id ==130 | locs_id==135 | locs_id ==185 | locs_id ==4749){
       #   if(covid_estimation == T) {
            if(max_year + j != 2020 & max_year + j != 2021 & max_year + j != 2022) {
              df[year_id == max_year + j , total_treated := as.numeric((mean_value - treated_t_1 - treated_t_2 - treated_a_sum)*prop_treated_1)]
              df[year_id == max_year + j & total_treated < 0, total_treated := 0]
              df[year_id == max_year + j, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - treated_t_2 - treated_a_sum - (0.5 * total_treated)), .SDcols = 		draws, by = group_cols]
            } 
            else {
              print(paste0(max_year + j, ": COVID year"))
              df[year_id == max_year + j , total_treated := as.numeric((mean_value - treated_t_1 - treated_t_2 - treated_a_sum)*prop_treated_1)]
              df[year_id == max_year + j & total_treated < 0, total_treated := 0]
              df[year_id == max_year + j, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - treated_t_2 - treated_a_sum - (0.5 * total_treated)), .SDcols = 		draws, by = group_cols]
            }
          } 

 else{
     # if(covid_estimation == T) {
        if(max_year + j != 2020 & max_year + j != 2021 & max_year + j != 2022) {
          df[year_id == max_year + j , total_treated := as.numeric((mean_value - treated_t_1 - treated_t_2 - treated_a_sum)*prop_treated_1)]
          df[year_id == max_year + j & total_treated < 0, total_treated := 0]
          df[year_id == max_year + j, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - treated_t_2 - treated_a_sum - (0.5 * total_treated)), .SDcols = draws, 		by = group_cols]
        } 
        if(max_year + j == 2020) { #when year 2020 and no country-specific treatment data in 2020, then apply -19.6% reduction from 2019
          print(paste0(max_year + j, ": COVID year"))
          df[year_id == max_year + j , total_treated := as.numeric((mean_value - treated_t_1 - treated_t_2 - treated_a_sum)*prop_treated_1*(1-0.196))]
          df[year_id == max_year + j & total_treated < 0, total_treated := 0]
          df[year_id == max_year + j, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - treated_t_2 - treated_a_sum - (0.5 * total_treated )), .SDcols = draws, 		by = group_cols]
        }
      if(max_year + j == 2021) { #when year 2020 and no country-specific treatment data in 2020, then apply -19.6% reduction from 2019
        print(paste0(max_year + j, ": COVID year"))
        df[year_id == max_year + j , total_treated := as.numeric((mean_value - treated_t_1 - treated_t_2 - treated_a_sum)*prop_treated_1*(1-0.201))]
        df[year_id == max_year + j & total_treated < 0, total_treated := 0]
        df[year_id == max_year + j, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - treated_t_2 - treated_a_sum - (0.5 * total_treated )), .SDcols = draws, by 		= group_cols]
      }else {
          print(paste0(max_year + j, ": COVID year")) #we don't have data on treatment trend in 2022, we will assume the rate is the same as 2021
          df[year_id == max_year + j , total_treated := as.numeric((mean_value - treated_t_1 - treated_t_2 - treated_a_sum)*prop_treated_1)]
          df[year_id == max_year + j & total_treated < 0, total_treated := 0]
          df[year_id == max_year + j, (n_draws) := lapply(.SD, function(x) x - treated_t_1 - treated_t_2 - treated_a_sum - (0.5 * total_treated)), .SDcols = draws, 		by = group_cols]
        } 
	}
    
    df <- df[, ..orig_cols]
  }


  df[, `:=` (year_shifting = NULL, age_shifts = NULL)]
  df$location_name <- loc_name
  df$ihme_loc_id <- ihme_loc_name
  
  # Convert any negative values to zero
  df[df < 0] <- 0
  
  # Check for NAs 
  if(nrow(df[is.na(draw_0)]) > 0 ) stop("There are NAs in the dataset. Stop")
  
  return(df)
}


## adjust_subnationals() -------------------------------------------------------------->

adjust_subnationals <- function(dt1, loc_id, df, prev_cases) {
  if(nrow(loc_meta[parent_id == loc_id]) > 0) {
    message("Adjusting subnationals")
    # Identify the location_ids related to the parent loc of interest 
    # If loc_id is England then make subnat_id level 6, all else should be based on parent_id 
    if(loc_id == 4749) {
      subnat_id <- loc_meta[grepl("GBR_", ihme_loc_id) & level == 6, unique(location_id)]
    } else{
        if(loc_id == 163) {
        subnat_id <- loc_meta[grepl("IND_", ihme_loc_id) & level == 5, unique(location_id)]
      }   else {
        subnat_id <- loc_meta[parent_id == loc_id, unique(location_id)]
      }
    }
    
    subnat_cases <- create_cases(dt1, subnat_id, 5) 
    subnat_cases_melt <- melt.data.table(subnat_cases, measure.vars = draws)
    
    # Create the proportion adjusted 
    adjusted_draws_melt <- melt.data.table(df, measure.vars = draws)
    setnames(adjusted_draws_melt, "value", "adjusted_value")
    
    orig_cases_melt <- melt.data.table(prev_cases, measure.vars = draws)
    setnames(orig_cases_melt, "value", "orig_value")
    
    merge_cols <- intersect(names(adjusted_draws_melt), names(orig_cases_melt))
    
    # Figure out the proportion of original cases after adjustment for treatment in max year 
    draws_melt <- merge(orig_cases_melt, adjusted_draws_melt, by = merge_cols)
    draws_melt[, proportion := adjusted_value / orig_value ]
    
    proportion_cols <- c('sex_id', 'age_group_id', 'year_id', 'variable', 'proportion')
    proportion_draws <- draws_melt[, ..proportion_cols]
    
    subnat_cases1 <- merge(subnat_cases_melt, proportion_draws, by = c('sex_id', 'age_group_id', 'year_id', 'variable'))
    subnat_cases1[, adjusted_value := value * proportion]
    subnat_cases1[, `:=` (value = NULL, proportion = NULL)]
    
    subnat_cases_wide <- dcast.data.table(subnat_cases1, location_id + age_group_id + sex_id + year_id + 
                                            measure_id + metric_id + model_version_id + modelable_entity_id + population ~ variable, value.var = "adjusted_value")
    
    loc <- unique(subnat_cases_wide$location_id)
    
    return(list(subnat_cases_wide, subnat_cases, loc))
  } else {
    loc <- unique(df$location_id)
    message("No subnationals to adjust")
    return(list(df, prev_cases, loc))
  }
}

create_proportions <- function(prev_cases, prev_reduction) {
  
  # Melt original and adjusted cases to figure out proportions adjusted
  prev_cases_melt <- melt.data.table(prev_cases, measure.vars = draws)
  prev_reduction_melt <- melt.data.table(prev_reduction, measure.vars = draws)
  
  keep_cols <- c("age_group_id", "location_id", "year_id", "sex_id", "variable", "value")
  prev_cases_melt <- prev_cases_melt[, ..keep_cols]
  all_reduction_melt <- prev_reduction_melt[, ..keep_cols]
  
  setnames(prev_cases_melt, "value", "orig_value")
  setnames(prev_reduction_melt, "value", "adj_value")
  
  dt <- merge(prev_reduction_melt, prev_cases_melt, by = c("age_group_id", "location_id", "year_id", "sex_id", "variable"), all.x = TRUE)
  dt[, proportion := adj_value / orig_value, by = c("age_group_id", "location_id", "year_id", "sex_id", "variable")]
  keep_cols <- c("age_group_id", "location_id", "year_id", "sex_id", "variable", "proportion")
  dt <- dt[, ..keep_cols]
  
  # Validations that proportion should be greater than 0 and less than 1
  if(nrow(dt[proportion > 1 | proportion < 0]) > 0) stop("There are proportions > 1 or < 0. Stop")
  
  return(dt)
}


## adjust_incidence () -------------------------------------------------------------->


adjust_incidence <- function(inc_cases, proportion) {
  # get number of incident cases 
  inc_cases_melt <- melt.data.table(inc_cases, measure.vars = draws)
  
  # merge on proportions to adjust 
  dt <- merge(inc_cases_melt, proportion, by = c("age_group_id", "location_id", "year_id", "sex_id", "variable"))
  dt[, adj_value := value * proportion, by = c("age_group_id", "location_id", "year_id", "sex_id", "variable")]
  
  # case wide to reformat 
  dt_wide <- dcast.data.table(dt, location_id + age_group_id + sex_id + year_id + 
                                measure_id + metric_id + model_version_id + modelable_entity_id + population ~ variable, value.var = "adj_value")
  return(dt_wide)
}




## create_diagnostics () -------------------------------------------------------------->
#'
#' @title create_diagnostics 
#'
#' @description takes the original and adjusted draws for incidence and prevalence and plots by location, measure, and sex 
#' 

create_diagnostics <- function(total_reduction, prev_cases, inc_cases, ihme_locid) {

  dt_orig <- rbind(prev_cases, inc_cases)
  dt_total <- copy(total_reduction)
  
  dt_orig[, orig_mean := rowMeans(.SD), .SDcols = draws]
  dt_orig[, (draws) := NULL]
  
  dt_total[, adj_mean := rowMeans(.SD), .SDcols = draws]
  dt_total[, (draws) := NULL]
  
  merge_cols <- c("location_id", "sex_id", "age_group_id", "year_id", "measure_id", "metric_id")
  dt_plot <- merge(dt_orig, dt_total, by = merge_cols)
  dt_plot <- merge(dt_plot, sex_meta, by = "sex_id")
  dt_plot <- merge(dt_plot, age_group_meta[, .(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id")
  dt_plot[, midage := (age_group_years_start + age_group_years_end) / 2]
  
  write.csv(x = dt_plot, FILEPATH, row.names = F)
  
  for (loc in unique(dt_plot$location_id)) {
    for (i in c(1,2)) {
      for (j in c(5,6)) {
        plotting_data <- copy(dt_plot[sex_id == i & measure_id == j & location_id == loc])
        loc_name <- loc_meta[location_id == loc, unique(location_name)]
        sex_name <- plotting_data[sex_id == i, tolower(unique(sex))]
        measure_name <- measure_meta[measure_id == j, unique(measure_name)]
        gg_mod_results <- ggplot() +
          geom_line(data = plotting_data, aes(x = midage, y = orig_mean, color = "Counterfactual"), size = 0.4, alpha = 0.5) +
          geom_line(data = plotting_data, aes(x = midage, y = adj_mean, color = "Treatment"), size = 0.4, alpha = 0.5)   +
          facet_wrap(~year_id) +
          scale_color_manual(name = "Estimate Type", values=c(Counterfactual="chocolate4", Treatment = "blue4")) +
          labs("Estimate Type") +
          labs(x = "Mid Age", y = "Counts", title = paste0("Chronic Hepatitis C Treatment: ", loc_name, ", ", sex_name, ", ", measure_name)) +
          theme(plot.title = element_text(size = 6)) +
          theme_bw() + theme(legend.position = "bottom")
        ggsave(filename = paste0(viz_dir, "/diagnostic_", loc, "_", i, "_", j, ".pdf"), plot = gg_mod_results, width = 6, height = 6)
        }
    }
  }
  message("Done creating graphs")
}



## counts_to rate () -------------------------------------------------------------->
#'
#' @title counts_to_rate
#'
#' @description converts draws in count space to rate based on age, sex, year,
#' location-specific population estimates.
#'
#' @param df dataframe of draws in wide format (ie. columns draw_0,
#' draw_1...draw_999). Required columns: 'age_group_id', 'location_id',
#' 'sex_id', 'year_id
#' 
counts_to_rate <- function(df){
  df[, (draws) := lapply(.SD, function(x)  x / population), .SDcols = draws,
        by = c("location_id", "year_id", "age_group_id", "sex_id")]
} 


## clean_adjusted_data() -------------------------------------------------------------->
#'
#' @title clean_adjusted_data
#'
#' @description 
#'
#' @param df 
#' 
clean_adjusted_data <- function(data) {
  
  data <- as.data.table(data)
  data$metric_id <- 3
  
  
  keep_cols <- c('location_id', 'sex_id', 'age_group_id', 'year_id', 'measure_id', 'metric_id', draws)
  data <- data[, ..keep_cols]
  
  if(nrow(data[draw_0 > 1,])) stop("There are values greater than 1 in the dataset. Stop")
  
  return(data)
  
}
