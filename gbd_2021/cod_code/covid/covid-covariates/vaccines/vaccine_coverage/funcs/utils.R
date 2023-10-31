# Miscellaneous utilities for the vaccine pipeline

.get_version_from_path <- function(x) {
  
  x <- unlist(strsplit(x, '/')) 
  x[length(x)]
  
}

# This function takes a vector of character dates and converts to date object with YYYY-MM-DD format. If the conversion fails, the function tries several date formats
.convert_dates <- function(x) {
  x <- as.vector(x)
  tmp <- as.Date(x, format = "%Y-%m-%d")
  if (all(is.na(tmp))) tmp <- as.Date(x, format = "%d.%m.%Y")
  if (all(is.na(tmp))) tmp <- as.Date(x, format = "%m/%d/%Y")
  if (all(is.na(tmp))) tmp <- as.Date(x, format = "%Y/%m/%d")
  if (all(is.na(tmp))) tmp <- as.Date(x, format = "%Y_%m_%d")
  if (all(is.na(tmp))) stop('Unrecognized date format')
  return(tmp)
}


# This function grabs the single unique value of a column from a data.table opr data.frame or accepts a vector
# Removes NAs and assumes its a static column (e.g. location_id)
.get_column_val <- function(x) {
  
  if (is.factor(x)) x <- as.character(x)
  
  if (is.data.table(x)) {
    
    if (ncol(x) > 1) stop('data.table has more than 1 column')
    x <- unique(x[!is.na(x)])
    
  } else if (is.data.frame(x)) {
    
    if (ncol(x) > 1) stop('data.frame has more than 1 column')
    x <- unique(as.vector(x)[!is.na(as.vector(x))])
    
  } else if (is.vector(x)) {
    
    x <- unique(x[!is.na(x)])
    
  }
  
  if (length(x) > 1) {
    
    stop(paste('Provided object has multiple unique values:', paste(x, collapse=' ')))
    
  } else if (length(x) == 0) {
    
    return(NA)
    
  } else {
    
    return(x)
    
  }
}

# This function takes a vector that is supposed to be a cumulative quantity and smooths out non-cumulative 
# elements by carrying forward the last value before the dip. Be careful using this to fix large spikes! 
.make_cumulative <- function(x) {
  
  delta <- diff(x)
  
  if (any(delta[!is.na(delta)] < 0)) {
    for (i in which(delta < 0)) {
      
      out <- x
      out[i:length(out)] <- out[i]
      x <- pmax(x, out)
      
    }
  }
  
  return(x)
}


# THis function takes a vector of cumulative integers or numeric values and makes any non-cumulative value NA
.remove_non_cumulative <- function(x) {
  
  if (!is.vector(x)) stop('Expecting a vector')
  
  for (i in 2:length(x)) {
    
    current_val <- x[i]
    past_vals <- x[1:(i-1)]
    
    if (!is.na(current_val)) {
      current_val_not_cumulative <- !all(current_val >= past_vals[!is.na(past_vals)])
      if (current_val_not_cumulative) x[i] <- NA
    }
  }
  
  return(x)
}



# This function takes a time series vector and fills NAs at the beginning and end of the time series by 
# extending the leading and trailing values out to the end of the provided vector
.extend_end_values <- function(x, 
                               lead=TRUE,   # If TRUE, the first observed value is carried backward to fill leading NAs
                               trail=TRUE   # If TRUE, the last observed value is carried forward to fill trailing NAs  
) {
  
  nas_index <- which(is.na(x))
  min_index <- min(which(!is.na(x)))
  max_index <- max(which(!is.na(x)))
  
  min_val <- x[min_index]
  max_val <- x[max_index]
  
  nas_lead <- nas_index[nas_index < min_index]
  nas_trail <- nas_index[nas_index > max_index]
  
  if (lead) x[nas_lead] <- min_val
  if (trail) x[nas_trail] <- max_val
  
  return(x)
}




# Check if the values in a numeric or integer vector are cumulative
.check_cumulative <- function(v) all(diff(zoo::na.approx(v)) > 0)

# Take a vector and do square root interpolation (transform, interp, transform back gives sqrt root increase instrad of linear)
.do_sqrt_interp <- function(v) zoo::na.approx(sqrt(v), na.rm=FALSE)^2




# This function takes a data.table or a data.frame and loops through
# columns ensuring that all values are: positive, finite, and with no NaNs
# This is pretty bare-bones so be mindful where you apply this!
.put_columns_in_bounds <- function(x,      # a data.table or data.frame
                                   ignore=NULL  # a character vector of column names to ignore
) {
  
  is.dt <- is.data.table(x)
  cn <- colnames(x)
  
  if (is.dt) x <- as.data.frame(x)
  
  for (i in seq_along(cn)) {
    
    if (cn[i] %in% ignore) next
    if (is.character(x[,i])) next
    
    x[which(x[,i] < 0), i] <- 0
    x[is.infinite(x[,i]), i] <- NA
    x[is.nan(x[,i]), i] <- NA
  }
  
  if (is.dt) return(as.data.table(x)) else return(x)
}



# A function to simply remove the rows in a data.table or data.frame for which the date column falls with a given date range.
# Note, date arguments form a closed interval, therefore given date-rows will also be removed
.knock_out_rows_by_date <- function(object, # data.table for data.frame that is long by date
                                    date1,  # start of closed interval date range; format = YYYY-MM-DD, 
                                    date2=NULL   # end of closed interval date range; format = YYYY-MM-DD
) {
  
  if (!('date' %in% colnames(object))) stop("Expecting a 'date' column")
  
  object$date <- as.Date(object$date)
  
  if (is.null(date2)) date2 <- date1
  date1 <- as.Date(date1)
  date2 <- as.Date(date2)
  
  if (date1 == date2) {
    sel <- which(object$date == date1)
  } else {
    sel <- which(object$date >= date1 & object$date <= date2)
  }
  
  return(object[-sel,])
}



# Function to make large number of distinct colors from basic brewer pal
.get_pal <- function(x=NULL) {
  if (is.null(x)) x <- 9
  colorRampPalette(brewer.pal(9, "Set1"))(x) # get_pal(50)
}



# Function to add parent names to data.frame or data.table
.add_parent_names <- function(d,          # A data.frame or data.table with a location_id column
                              hierarchy   # a location hierarchy
) { 
  
  if (!('location_id' %in% colnames(d))) stop('location_id column not found')
  
  get_name <- function(x) {
    
    tryCatch( { 
      
      x$parent_name <- hierarchy[location_id == .get_column_val(x$parent_id), location_name]
      
    }, error = function(e) {
      
      x$parent_name <- NA
      
    })
    
    return(x)
  }
  
  if (is.data.frame(d)) {
    
    out <- do.call(rbind, lapply(split(d, d$location_id), FUN=function(x) get_name(x) ))
    
  } else if (is.data.table(d)) {
    
    out <- do.call(rbind, lapply(split(d, by='location_id'), FUN=function(x) get_name(x) ))
    
  } else {
    
    stop('Expecting a data.frame OR data.table')
  }
  
  return(out)
}




# Simple function to take a vector of proportions and manually bound them to [0,1]
.do_manual_bounds_proportion <- function(x) {
  
  if (is.vector(x)) {
    
    if (sum(c(x > 1, x < 0), na.rm=TRUE) > 0) {
      message('Making manual adjustment to out of bounds proportion')
      x[which(x > 1)] <- 1
      x[which(x < 0)] <- 0
    } 
    
    return(x)
    
  } else {
    stop('Expecting a vector')
  }
  
}




# A function to split a data.table or data.frame by location and age and apply the given function (func) on each unit
# Function assumes 'location_id' and 'age_group' are columns that exist in the object
# When n.cores provided function is run in parallel, when NULL it is applied sequentially
.apply_func_location_age <- function(object, func, n_cores=NULL) {
  
  if (!is.data.frame(object)) object <- as.data.frame(object)
  
  warning_wrapper_func <- function(func) {
    
    tryCatch( { 
      
      return(func)
      
    }, warning = function(w) {
      
      message(paste('WARNING:', unique(x$location_id), unique(x$age_group), conditionMessage(w)), '\n')
      
    }, error = function(e) {
      
      message(paste('ERROR:', unique(x$location_id), unique(x$age_group), conditionMessage(e)), '\n')
      
    })
  }
  
  if (!is.null(n_cores)) {
    
    out <- do.call(
      rbind,
      pbmcapply::pbmclapply(
        X = split(object, 
                  list(object$location_id, 
                       object$age_group)), 
        mc.cores = n_cores, 
        FUN = function(x) warning_wrapper_func(func(x))
      )
    )
    
  } else {
    
    out <- do.call(
      rbind,
      pbapply::pblapply(
        X = split(object, 
                  list(object$location_id, 
                       object$age_group)), 
        FUN = function(x) warning_wrapper_func(func(x))
      )
    )
    
  }
  
  row.names(out) <- NULL
  return(out)
}



# Analogous function to the above, but only applied to locations
.apply_func_location <- function(object, func, n_cores=NULL) {
  
  if (!is.data.frame(object)) object <- as.data.frame(object)
  
  warning_wrapper_func <- function(func) {
    
    tryCatch( { 
      
      return(func)
      
    }, warning = function(w) {
      
      message(paste('WARNING:', unique(x$location_id), conditionMessage(w)), '\n')
      
    }, error = function(e) {
      
      message(paste('ERROR:', unique(x$location_id), conditionMessage(e)), '\n')
      
    })
  }
  
  if (!is.null(n_cores)) {
    
    out <- do.call(
      rbind,
      pbmcapply::pbmclapply(
        X = split(object, 
                  list(object$location_id)), 
        mc.cores = n_cores, 
        FUN = function(x) warning_wrapper_func(func(x))
      )
    )
    
  } else {
    
    out <- do.call(
      rbind,
      pbapply::pblapply(
        X = split(object, 
                  list(object$location_id)), 
        FUN = function(x) warning_wrapper_func(func(x))
      )
    )
    
  }
  
  row.names(out) <- NULL
  return(out)
}



# Analogous function to the above, but only applied to locations
.apply_func_super_region <- function(object, func, n_cores=NULL) {
  
  if (!is.data.frame(object)) object <- as.data.frame(object)
  
  warning_wrapper_func <- function(func) {
    
    tryCatch( { 
      
      return(func)
      
    }, warning = function(w) {
      
      message(paste('WARNING:', unique(x$super_region_id), conditionMessage(w)), '\n')
      
    }, error = function(e) {
      
      message(paste('ERROR:', unique(x$super_region_id), conditionMessage(e)), '\n')
      
    })
  }
  
  if (!is.null(n_cores)) {
    
    out <- do.call(
      rbind,
      pbmcapply::pbmclapply(
        X = split(object, 
                  list(object$super_region_id)), 
        mc.cores = n_cores, 
        FUN = function(x) warning_wrapper_func(func(x))
      )
    )
    
  } else {
    
    out <- do.call(
      rbind,
      pbapply::pblapply(
        X = split(object, 
                  list(object$super_region_id)), 
        FUN = function(x) warning_wrapper_func(func(x))
      )
    )
    
  }
  
  row.names(out) <- NULL
  return(out)
}




# This function takes any data.table or data.frame with a location_id column and adds associated spatial information from the provided hierarchy
# If no hierarchy COVID modeling hierarchy is used.

.add_spatial_metadata <- function(data, 
                                  hierarchy=NULL
) {
  
  is_dt <- is.data.table(data)
  if (is_dt) data <- as.data.frame(data)
  
  if (is.null(hierarchy)) {
    
    source(paste0("FILEPATH",Sys.info()['user'],"FILEPATH/paths.R"))
    source(file.path(CODE_PATHS$VACCINE_FUNCTIONS_ROOT, "vaccine_data.R"))
    hierarchy <- gbd_data$get_covid_modeling_hierarchy()
    
  }
  
  spatial_cols <- c('location_name', 'parent_id', 'parent_name', 'region_id', 'region_name', 'super_region_id', 'super_region_name')
  
  out <- merge(data[,!(colnames(data) %in% spatial_cols)], 
               hierarchy[,colnames(hierarchy) %in% c('location_id', spatial_cols), with=F], 
               by='location_id', 
               all.x=TRUE)
  
  if (is_dt) out <- as.data.table(out)
  return(out)
}

# Get character vector of age group names from a vector of age starts
.get_age_groups <- function(age_starts, include_all_ages=FALSE) {
  
  age_starts <- as.integer(age_starts)
  age_starts <- age_starts[order(age_starts)]
  
  age_groups <- character(0)
  tmp <- c(age_starts, 126)
  
  for (i in seq_along(age_starts)) age_groups <- c(age_groups, paste(tmp[i], tmp[i+1]-1, sep='-'))
  if (include_all_ages) age_groups <- c(age_groups, paste(min(age_starts), 125, sep='-'))
  
  return(age_groups)
}


.print_header_message <- function(m) {
  b <- rep('-', nchar(m))
  message(b); message(m); message(b)
}

.factor_age_groups <- function(data, age_starts=NULL) {
  
  if (is.null(age_starts)) age_starts <- get('age_starts', envir=parent.frame())

  age_groups <- .get_age_groups(age_starts)
  all_age_group <- paste0(age_starts[1], '-', 125)
  if(all_age_group %in% data) age_groups <- c(all_age_group, age_groups)
  
  factor(data, levels=age_groups, labels=age_groups)
}


# Removes columns by name from data.table or data.frame. Best for use when a table gets read in with write.csv and gets an index column added.
.strip_indices <- function(df, idx_cols = c("V1", "X")) {
  
  if (is.data.frame(df)) {
    select_cols <- setdiff(names(df), idx_cols)

    if (is.data.table(df)) {
      df <- df[, ..select_cols]
    } else {
      df <- df[, select_cols]
    }
  } else {
    stop("df is not a data.frame.")
  }


  return(df)
}


# Checks for dupes by keys.
.validate_key = function (dt, key_cols, script_name = NULL, dt_name = "data.table") {
  
  dupes_dt = data.table()
  dt <- data.table(dt)
  
  dupes_dt = dt[duplicated(dt, by = key_cols)]
  dupe_ct = nrow(dupes_dt)
  
  out_message <- paste0("Duplicate validation : " , 
                        dupe_ct, " duplicates : ", 
                        script_name, " : ",  
                        dt_name, " : ", 
                        "Key cols = ", paste(key_cols, collapse = ", "))
  
  if (dupe_ct > 0) {
    stop(out_message)
  } else {
    message(out_message)
  }
  
  
  return(dupes_dt)
}


# Drops dupes by key.
.drop_key_dupes = function (dt, key_cols, dt_name = "data.table") {

  if(
    nrow(.validate_key(dt = dt, key_cols = key_cols, dt_name = dt_name)) >
    0
  ) {
    warning("Dropping key duplicates from ", dt_name, ".")
    dt = unique(dt, by = key_cols)
  } else {
    message("No dupes dropped.")
  }


  return(dt)
}

#' Ensure corrected last_shots files contain most detailed locations
#' 
#' @param vaccine_output_root [character] Filepath to vaccine outputs (versioned)
#'
#' @description Some most detailed locations will not be present, and are not modelled (mostly Pacific Islands)
#' As of 2022-07-12, India Union Territories are missing due to a historic bug, but the infection model expects this
#'
#' @return
#' @export
#'
#' @examples
.validate_last_shots <- function(vaccine_output_root) {
  message("Validation check - last_shots_in_arms files")
  
  ref <- fread(file.path(vaccine_output_root, "last_shots_in_arm_by_brand_w_booster_reference.csv"))
  opt <- fread(file.path(vaccine_output_root, "last_shots_in_arm_by_brand_w_booster_optimal.csv"))
  hierarchy <- .get_covid_modeling_hierarchy()
  
  ref <- rbindlist(lapply(split(ref, ref$location_id), function(x) {
    x$daily <- rowSums(x[,-c(1:4)])
    x$cumul <- cumsum(x$daily)
    return(x)
  }))
  
  opt <- rbindlist(lapply(split(opt, opt$location_id), function(x) {
    x$daily <- rowSums(x[,-c(1:4)])
    x$cumul <- cumsum(x$daily)
    return(x)
  }))
  
  # get most detailed
  ref <- merge(ref, hierarchy[,.(location_id, most_detailed)], by = "location_id", all.x=T, all.y = T)
  opt <- merge(opt, hierarchy[,.(location_id, most_detailed)], by = "location_id", all.x=T, all.y = T)
  
  # which most detailed have non-zero vaccines delivered & projected
  ref_chk <- ref[(date == max(ref$date, na.rm = T) & cumul > 0 & most_detailed == 1), 
                 unique(location_id)]
  opt_chk <- opt[(date == max(opt$date, na.rm = T) & cumul > 0 & most_detailed == 1), 
                 unique(location_id)]
  
  ref_0 <- setdiff(hierarchy[most_detailed==1, location_id], ref_chk)
  opt_0 <- setdiff(hierarchy[most_detailed==1, location_id], opt_chk)
  union_0 <- union(ref_0, opt_0)

  missing_locs <- hierarchy[location_id %in% union_0, .(location_id, location_name)]
  
  expected_missing <- data.table(location_id = c(40L, 4840L, 4845L, 4858L, 4866L, 320L, 374L, 376L, 413L),
                                 location_name = c("Turkmenistan", "Andaman and Nicobar Islands", 
                                                   "Chandigarh", "Lakshadweep", "Puducherry", "Cook Islands", "Niue", 
                                                   "Northern Mariana Islands", "Tokelau"))
  
  not_expected_missing <- setdiff(missing_locs$location_id, expected_missing$location_id)
  not_expected_missing <- hierarchy[location_id %in% not_expected_missing, .(location_id, location_name)]
  
  # RETAIN BOTH condition and print statements for readability in either console or log files
  message("\n", "Last shots in arms expected missing most-detailed locations are : ", "\n\n" , paste(expected_missing, collapse =  " : "))
  print(expected_missing)
  
  if(nrow(missing_locs) > 0){
    
    message("\n", "Last shots in arms files are missing these most-detailed locations : ", "\n\n", paste(missing_locs, collapse = " : ") )
    print(missing_locs)
    
  }
  
  if (nrow(not_expected_missing) > 0){
    
    stop("\n", "Failed last_shots_in_arms valiation: NOT expected missing most-detailed locations : ", "\n\n", paste(not_expected_missing, collapse = " : "))
    print(not_expected_missing)
    
  } else {
    
    message("Congratulations! Passing last_shots_in_arms validation.")
    
  }
  
}

# Metadata related ----


#'
#' Intended to find Rstudio singularity image versions for metadata.  Finds a
#' user-defined string from the submission command you used to start your
#' Rstudio singularity image (or something else you desire).  Extracts this
#' informtion from ALL jobs you currently have active in your squeue.
#'
#' @param squeue_jobname_filter [character|regex] when you run `squeue -u
#'   <username>`, what `NAME` do you want to filter for?
#' @param max_cmd_length [integer] how many characters long is your command?
#'   Increase your default if the command is truncated.  All leading/trailing
#'   whitespace is trimmed.
#' @param string_to_extract [character|regex] what string do you want to extract
#'   after running  `sacct -j <jobid> -o submitline%xxx` using
#'   `stringr::str_extract_all`
#' @param strings_to_ignore [character|regex] if your `string_to_extract`
#'   command finds more strings than you want, this removes all strings with
#'   this pattern anywhere inside using `stringr::str_detect`
#' @param user_name [character] which user's commands to find - defaults to your
#'   own
#'
#' @return [list] all desired submission commands, and specific extracted text
#'   from string_to_extract
.extract_submission_commands <- function(
  
  squeue_jobname_filter = "^rst_ide",
  max_cmd_length        = 500L,
  string_to_extract     = "FILEPATH",
  strings_to_ignore     = "jpy",
  user_name             = Sys.info()[["user"]]
  
) {
  
  if (is.null(string_to_extract)) {
    stop("You must specify a string to find and extract from command line submissions")
  }
  
  # command to find user's cluster jobs
  
  job_finder <- function(user = user_name) {
    all_jobs <- system2(SYSTEM_COMMAND), 
      stdout = T
    )
    
    return(all_jobs)
  }
  
  # extract submission information
  
  jobs <- job_finder()
  jobs <- read.table(text = jobs, header = T, sep = "", )
  
  # format table & extract jobids
  names(jobs) <- tolower(names(jobs))
  jobs <- jobs[, c("jobid", "name")]
  rstudio_filter <- grepl(squeue_jobname_filter, jobs[["name"]])
  jobs <- jobs[rstudio_filter, ]
  
  submit_command_list <- list()
  
  # use sacct to extract original rstudio image submission commands
  # only works if rstudio was started from CLI, not from API
  # INTERNAL_COMMENT
  
  for (i in 1:nrow(jobs)) {
    
    job_id <- jobs[i, "jobid"]
    
    command_args <- paste(
      "-j", job_id,
      paste0("-o submitline%", max_cmd_length)
    )
    
    submission_command <- system2(SYSTEM_COMMAND)[[3]]
    
    submission_command <- tolower(trimws(submission_command, which = "both"))
    
    submit_command_list[[i]] <- submission_command
    
  }
  
  extract_command_string <- function (submit_command_element) {
    
    extracted_strings <- stringr::str_extract_all(submit_command_element, string_to_extract)
    
    # str_extract_all produces a list - need to deal with it
    if(!length (extracted_strings) == 1) {
      stop("submit_command_list has more than one element - investigate. 
             You likely have more than one pattern specified")
    }
    
    extracted_strings <- extracted_strings[[1]]
    ignore_filter <- !sapply(extracted_strings, stringr::str_detect, pattern = strings_to_ignore)
    
    # return only strings without jpy language
    return(extracted_strings[ignore_filter])
    
  }
  
  extracted_cmd_strings <- lapply(submit_command_list, extract_command_string)
  
  out_list <- list(
    submission_commands = submit_command_list,
    extracted_cmd_strings = extracted_cmd_strings
  )
  
  return(out_list)
  
}
