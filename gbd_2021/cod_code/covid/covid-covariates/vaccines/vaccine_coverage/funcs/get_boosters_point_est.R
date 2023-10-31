
#' @description Get point estimates for booster correction by booster course
#' 
#' Finds number of booster courses and minimum required observations per booster
#' course from model parameters. Loads observed data, finds the last day in time
#' with at least booster_x_min_obs locations with a value for boosters.
#' 
#' Update launch.R script to add additional courses, and required observations
#' per course (number of courses much match, or this will throw an error).
#' 
#' Validates results, or returns error and does not write to disk.
#' 
#' Results are picked up by booster_correction.R to time-shift modeled shots-in-arms
#' according to reported delivery by location. Results are also picked up in
#' scenario_wrapper to add to the list of locations that get additional courses.
#' 
#' REQUIRE: allowed number of booster courses == number of booster_x_min_obs values in model parameters
#' REQUIRE: columns in data c("location_id", "date", "booster_x", "value") 
#'
#' @param vaccine_output_root [character] Output directory for this vaccine run
#'
#' @return Writes file to disk. Columns = c("location_id", "date",
#'   "booster_course", "value"), long by booster_course
.get_boosters_point_est <- function(vaccine_output_root) {
  
  library(data.table)
  library(stringr)
  
  # PARAMETERS
  model_parameters <- vaccine_data$load_model_parameters(vaccine_output_root)
  # Minimum number of locations to correct per booster course
  min_obs_names <- names(model_parameters)[grep(pattern = "_min_obs$", names(model_parameters))]
  min_obs_list <- lapply(min_obs_names, function(x) model_parameters[x])
  BOOSTER_COURSES <- model_parameters$booster_courses
  
  if (BOOSTER_COURSES > length(min_obs_list)) {
    stop("get_booster_point_est.R : Number of allowed booster courses greater than number of specified point estimates - please revisit model parameters in launch.R")
  }
  
  # DATA
  observed <- fread(file.path(vaccine_output_root, "observed_data.csv"))
  observed$date <- as.Date(observed$date)
  # For scoping in 
  # observed[date < "2022-04-01", booster_1 := boosters_administered][date >= "2022-04-01", booster_2 := boosters_administered]
  observed <- melt(
    observed, 
    id.vars = c("location_id", "date", "max_date"),
    measure.vars = paste0("booster_", 1:BOOSTER_COURSES)
  )
  observed_list <- split(observed, by = "variable")
  
  # PREALLOCATE RESULT LIST 
  result_list <- vector("list", length = length(min_obs_list))
  names(result_list) <- min_obs_names
  
  # i = 1
  
  for (i in 1:BOOSTER_COURSES) {
    observed <- observed_list[[i]]
    min_obs <- min_obs_list[[i]][[1]]
    
    n <- t_lag <- 0
    while ((n < min_obs) & 
           (t_lag < (max(as.numeric(observed$date), na.rm = T) - min(as.numeric(observed$date), na.rm = T)) ) ) {
      # message(glue("t_lag = {t_lag}"))
      point_est_date <- max(observed$max_date, na.rm=T) - t_lag
      
      boosters_point_est <- observed[date == point_est_date & !is.na(value) & value > 0,]
      
      n <- length(unique(boosters_point_est$location_id))
      t_lag <- t_lag + 1
    } 
    
    if ( t_lag < (max(as.numeric(observed$date), na.rm = T) - min(as.numeric(observed$date), na.rm = T)) ) {
      
      message(glue('{unique(observed$variable)} point estimates latest date = {point_est_date}. Sample size = {n}'))
      result_list[[i]] <- boosters_point_est
      
    } else {
      
      stop(glue(" ✖ Timeout : get_booster_point_est.R : Unable to find {min_obs} rows on one day for {unique(observed$variable)}
               ℹ Can you reduce min_obs_{unique(observed$variable)}?"))
      
    }
    
  }
  
  # Ensure all booster courses have results
  result_list <- Filter(Negate(is.null), result_list)
  
  if (length(result_list) < length(min_obs_list)) { 
    
    stop("get_booster_point_est.R : Not all booster courses have a point estimate - please check required min_obs per course in launch.R")
    
  } else if (length(result_list) < BOOSTER_COURSES) {
    
    stop("get_booster_point_est.R : Number of allowed booster courses greater than number of calculated point estimates - please revisit model parameters in launch.R")
    
  } else {
    
    out <- rbindlist(result_list)
    out[, max_date := NULL]
    setnames(out, "variable", "booster_course")
    out$vaccine_course <- as.integer(str_extract(out$booster_course, "[:digit:]")) + 1L
    out_file_path <- file.path(vaccine_output_root, 'boosters_point_estimate.csv')
    fwrite(out, file = out_file_path)
    message(glue('Booster point estimates written to: {out_file_path}'))
    
  }
  
  # Scatterplots ----
  
  plot_dt <- .scatter_prep_booster_point(vaccine_output_root)
  plot_cols <- c("booster_1", "booster_2")
  
  pdf(file.path(vaccine_output_root, 
                glue("booster_point_scatter_{.model_inputs_version}_v_{.previous_best_version}.pdf")),
      height=10, width=15)
  
  for (col in plot_cols){
    .scatter_plot_compare(DATASET = plot_dt, PERCENT_DIFF = 0.5, VARIABLE = col, EXTRA_LABEL = unique(out$date))
  }
  
  dev.off()
  
}


