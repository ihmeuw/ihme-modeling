## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: utils.R
## Contents: %ni%, .clean_filepath, get_core_ref, .get_root, .set_roots, 
##           create_folder_shortcuts, subset_date, draw_mult_fn,  
##           check_size, .get_successful_locs, .run_diagnostics, .ensure_dir, 
##           .check_squareness, .finalize_data, save_dataset, save_epi_dataset
## Contributors: 
## --------------------------------------------------------------------- ----


## Environment Prep ---------------------------------------------------- ----
total_start <- Sys.time()
cat('Sourcing Utils...\n')

# Init NF COVID repo
if (!exists(".repo_base")) {
 .repo_base <- 'FILEPATH'
}
.nf_repo <- paste0(.repo_base, 'FILEPATH')

# Load libraries
pacman::p_load(data.table, tidyverse, jsonlite, ggplot2, tidyr, stringr, chron, yaml, 
               fuzzyjoin, gridExtra, scales, readxl, openxlsx, arrow, httr, pdftools,
               lubridate)
## --------------------------------------------------------------------- ----


## Root and Path Functions --------------------------------------------- ----

`%ni%` <- Negate(`%in%`)


.clean_filepath <- function(path) {
  #' Removes any bracketed items from path
  #' @param path [str]
  
  if (grepl('FILEPATH', fixed=T)) {
    path <- gsub('FILEPATH')
  }
  if (grepl('FILEPATH', fixed=T)) {
    path <- gsub('FILEPATH')
  }
  if (grepl('[hsp_icu_input_date]', path, fixed=T)) {
    path <- gsub('FILEPATH', get_core_ref('hsp_icu_input_date'), path)
  }
  if (grepl('[infect_death_input_date]', path, fixed=T)) {
    path <- gsub('FILEPATH', get_core_ref('infect_death_input_date'), path)
  }
  if (grepl('[age_sex_specific_input_date]', path, fixed=T)) {
    path <- gsub('FILEPATH', get_core_ref('age_sex_specific_input_date'), path)
  }
  if (grepl('[gbd_year]', path, fixed=T)) {
    path <- gsub('FILEPATH', get_core_ref('gbd_year'), path)
  }
  
  return(path)
}


get_core_ref <- function(param_name, sub_key=NULL) {
  #' Convenience function to pull static reference from refs.yaml
  #' @param param_name [str]: A string containing the reference desired
  #' @param sub_key [str]: (OPTIONAL) A string containing the sub-reference needed
  
  # Error handling
  if (is.null(param_name)) {
    stop('Supplied param_name is None. You must supply a value.')
  }
  
  f <- yaml.load_file(paste0(.nf_repo, "refs.yaml"))[[param_name]]
  if (!is.null(sub_key)) {
    f <- f[[sub_key]]
  }
  
  if (any(grepl('\\[', as.character(f)))) {
    f <- .clean_filepath(f)
  }
  
  return(f)
}


.get_root <- function(root) {
  #' Internal function to pull a specific root
  #' @param root [str] Name of root to pull
  
  os <- Sys.info()['sysname']
  
  if (root == 'ROOT') {
    if (os == 'Windows') {
      return('')
    } else {
      return ('')
    }
  } else if (root == 'ROOT') {
    if (os == 'Windows') {
      return('')
    } else {
      return (paste0('FILEPATH'))
    }
  } else if (root == 'ROOT') {
    if (os == 'Windows') {
      return('')
    } else {
      return ('FILEPATH')
    }
  } else if (root == 'ROOT') {
    if (os == 'Windows') {
      return('')
    } else {
      return('FILEPATH')
    }
  }
  else if (root == 'ROOT') {
    if (os == 'Windows') {
      return('')
    } else {
      return('FILEPATH')
    }
  }
}


.set_roots <- function() {
  #' Internal function to create root filepaths.
  
  roots <- list(
    # Base paths
    'ROOT' = .get_root('ROOT'), 
    'ROOT' = .get_root('ROOT'), 
    'ROOT' = .get_root('ROOT'),
    'ROOT' = .get_root('ROOT'), 
    'ROOT' = 'FILEPATH',
    'scratch' = 'FILEPATH',
    'nf_repo' = .nf_repo,
    # Specific paths
    'hsp_icu_input_path' = get_core_ref('hsp_icu_input_path'),
    'infect_death_input_path' = get_core_ref('infect_death_input_path'),
    'age_sex_specific_input_path' = get_core_ref(('age_sex_specific_input_path')),
    'disability_weight' = get_core_ref('disability_weight_path'),
    # GBD Stuff
    'gbd_round' = get_core_ref('gbd_round_id'),
    'gbd_year' = get_core_ref('gbd_year'),
    'decomp_step' = get_core_ref('decomp_step'),
    'age_groups' = get_core_ref('age_group_ids'),
    'estimation_years' = get_core_ref('estimation_years'),
    'all_gbd_estimation_years' = get_core_ref('all_gbd_estimation_years'),
    'draws' = paste0('draw_', c(0:999)),
    # HHSeqIDs
    hhseq_ids = list(
      'short_mild' = get_core_ref('short_mild_hhseqid'),
      'short_moderate' = get_core_ref('short_moderate_hhseqid'),
      'short_severe' = get_core_ref('short_severe_hhseqid'),
      'short_icu' = get_core_ref('short_icu_hhseqid'),
      'long_cognitive' = get_core_ref('long_cognitive_hhseqid'),
      'long_fatigue' = get_core_ref('long_fatigue_hhseqid'),
      'long_respiratory' = get_core_ref('long_respiratory_hhseqid'),
      'long_gbs' = get_core_ref('long_gbs_hhseqid')
    ),
    # ME IDs
    me_ids = list(
      # Short term
      'asymp' = get_core_ref('asymp_me_id'),
      'mild' = get_core_ref('mild_me_id'),
      'moderate' = get_core_ref('moderate_me_id'),
      'hospital' = get_core_ref('hospital_me_id'),
      'icu' = get_core_ref('icu_me_id'),
      # Long term, individuals
      'cognitive_mild' = get_core_ref('cognitive_mild_me_id'),
      'cognitive_severe' = get_core_ref('cognitive_severe_me_id'),
      'fatigue' = get_core_ref('fatigue_me_id'),
      'respiratory_mild' = get_core_ref('respiratory_mild_me_id'),
      'respiratory_moderate' = get_core_ref('respiratory_moderate_me_id'),
      'respiratory_severe' = get_core_ref('respiratory_severe_me_id'),
      # Long term, cog_fat
      'cognitive_mild_fatigue' = get_core_ref('cognitive_mild_fatigue_me_id'),
      'cognitive_severe_fatigue' = get_core_ref('cognitive_severe_fatigue_me_id'),
      # Long term, cog_resp
      'cognitive_mild_respiratory_mild' = get_core_ref('cognitive_mild_respiratory_mild_me_id'),
      'cognitive_mild_respiratory_moderate' = get_core_ref('cognitive_mild_respiratory_moderate_me_id'),
      'cognitive_mild_respiratory_severe' = get_core_ref('cognitive_mild_respiratory_severe_me_id'),
      'cognitive_severe_respiratory_mild' = get_core_ref('cognitive_severe_respiratory_mild_me_id'),
      'cognitive_severe_respiratory_moderate' = get_core_ref('cognitive_severe_respiratory_moderate_me_id'),
      'cognitive_severe_respiratory_severe' = get_core_ref('cognitive_severe_respiratory_severe_me_id'),
      # Long term, fat_resp
      'fatigue_respiratory_mild' = get_core_ref('fatigue_respiratory_mild_me_id'),
      'fatigue_respiratory_moderate' = get_core_ref('fatigue_respiratory_moderate_me_id'),
      'fatigue_respiratory_severe' = get_core_ref('fatigue_respiratory_severe_me_id'),
      # Long term, cog_fat_resp
      'cognitive_mild_fatigue_respiratory_mild' = get_core_ref('cognitive_mild_fatigue_respiratory_mild_me_id'),
      'cognitive_mild_fatigue_respiratory_moderate' = get_core_ref('cognitive_mild_fatigue_respiratory_moderate_me_id'),
      'cognitive_mild_fatigue_respiratory_severe' = get_core_ref('cognitive_mild_fatigue_respiratory_severe_me_id'),
      'cognitive_severe_fatigue_respiratory_mild' = get_core_ref('cognitive_severe_fatigue_respiratory_mild_me_id'),
      'cognitive_severe_fatigue_respiratory_moderate' = get_core_ref('cognitive_severe_fatigue_respiratory_moderate_me_id'),
      'cognitive_severe_fatigue_respiratory_severe' = get_core_ref('cognitive_severe_fatigue_respiratory_severe_me_id'),
      'gbs' = get_core_ref('gbs_me_id'),
      'any' = get_core_ref('any_me_id'),
      'midmod_any' = get_core_ref('midmod_any_me_id'),
      'hospital_any' = get_core_ref('hospital_any_me_id'),
      'icu_any' = get_core_ref('icu_any_me_id')
    ),
    # Default multipliers & date lags
    defaults = list(
      'asymp_duration' = get_core_ref('asymp_duration'),
      'incubation_period' = get_core_ref('incubation_period'),
      'midmod_duration_no_hsp' = get_core_ref('midmod_duration_no_hsp'),
      'comm_die_duration_severe' = get_core_ref('comm_die_duration_severe'),
      'infect_to_hsp_admit_duration' = get_core_ref('infect_to_hsp_admit_duration'),
      'symp_to_hsp_admit_duration' = get_core_ref('symp_to_hsp_admit_duration'),
      'prop_mild' = get_core_ref('prop_mild'),
      'prop_mod' = get_core_ref('prop_mod'),
      'icu_to_death_duration' = get_core_ref('icu_to_death_duration'),
      'hsp_death_duration' = get_core_ref('hsp_death_duration'),
      'hsp_no_icu_no_death_duration' = get_core_ref('hsp_no_icu_no_death_duration'),
      'hsp_no_icu_death_duration' = get_core_ref('hsp_no_icu_death_duration'),
      'hsp_icu_no_death_duration' = get_core_ref('hsp_icu_no_death_duration'),
      'hsp_icu_death_duration' = get_core_ref('hsp_icu_death_duration'),
      'hsp_post_icu_duration' = get_core_ref('hsp_post_icu_duration'),
      'icu_no_death_duration' = get_core_ref('icu_no_death_duration'),
      'hsp_midmod_after_discharge_duration' = get_core_ref('hsp_midmod_after_discharge_duration'),
      'icu_midmod_after_discharge_duration' = get_core_ref('icu_midmod_after_discharge_duration'),
      'prop_deaths_icu' = get_core_ref('prop_deaths_icu'),
      'prop_asymp_comm_no_die_no_hsp' = get_core_ref('prop_asymp_comm_no_die_no_hsp'),
      'gbs_dur' = get_core_ref('gbs_dur')
    )
  )
  
  roots <<- roots
  return(roots)
}
.set_roots()


create_folder_shortcuts <- function(folder_type, output_version) {
  #' Convenience function to create "latest" folder from pipeline run
  #' @param folder_type [str] Either "best" or "latest"
  #' @param output_version [str] The YYYY-MM-DD.VV of the run to mark latest
  
  # Internal Function
  .check_date <- function(d) {
    #' Convenience function to check date validity in YYYY-MM-DD format
    #' @param d [str/date] Date to parse
    
    # Parse date
    d_check <- list('y' = as.integer(unlist(str_split(d, '-'))[1]),
                    'm' = as.integer(unlist(str_split(d, '-'))[2]),
                    'd' = as.integer(unlist(str_split(d, '-'))[3]))
    
    # Check for validity
    if (d_check$y < as.integer(format(Sys.Date(), "%Y")) | d_check$m > 12 | 
        d_check$d > 31) {
      return(T)
    }
    
    # Return False
    return(F)
  }
  
  
  # Error handling
  if (folder_type %ni% c('best', 'latest')) {
    stop('Supplied folder type is invalid. Please try again with either: [best, latest]')
  }
  sub_str <- list(date = str_split(output_version, '\\.')[[1]][1],
                  version = str_split(output_version, '\\.')[[1]][2])
  if (.check_date(sub_str$date) | sub_str$version < 0) {
    stop('Supplied output_version is invalid. Please ensure it matches the format YYYY-MM-DD.VV')
  }
  
  # Intermediate output
  io <- get_core_ref('data_output', 'stage_1')
  unlink(paste0(io, folder_type))
  system(paste0('ln -s ', io, output_version, ' ', io, folder_type))
  rm(io)
  
  
  # Diagnostics
  d <- paste0(roots$'ROOT', 'FILEPATH')
  unlink(paste0(d, folder_type))
  system(paste0('ln -s ', d, output_version, ' ', d, folder_type))
  rm(d)
  
  
  # Final outputs
  f <- get_core_ref('data_output', 'final')
  unlink(paste0(f, folder_type))
  system(paste0('ln -s ', f, output_version, ' ', f, folder_type))
  rm(f)
}

## --------------------------------------------------------------------- ----


## Data Processing Functions ------------------------------------------- ----

subset_date <- function(dt, subset_year = c(2020)) {
  #' Returns data.table subset to just 2020
  #' @param df [data.table/data.frame] Must contain column called "date"
  #' @param subset_year [vector] 
  
  if ('date' %ni% colnames(dt)) {
    stop('Supplied dt missing date column.')
  }
  
  return(dt[date >= paste0(min(subset_year), '-01-01') & 
              date <= paste0(max(subset_year), '-12-31'),])
}


draw_mult_fn <- function(x, y, wherex = parent.frame(), wherey = parent.frame()) {
  #' Returns data.table where individual x in wherex has been multiplied by y in wherey.
  #' Should only be used in lapply() function calls
  #' @param x [str] Column to be multiplied on
  #' @param y [str] Column to be multiplied by
  #' @param wherex [data.table] Data structure containing [x] column
  #' @param wherey [data.table] Data structure containing [y] column
  
  return(get(x, wherex) * get(y, wherey)) 
}


check_neg <- function(data, locid, locname, output_version, nf_type, calc_cols, 
                      check_cols = NULL, add_cols = NULL, return_data = F, 
                      years_to_check = c(2020)) {
  #' Convenience function to check for negative values in a dataset
  #' @param data [data.table]
  #' @param locid [int]
  #' @param locname [str]
  #' @param output_version [str] Must resemble "2021-01-01.01"
  #' @param nf_type [str] Either 'short' or 'long'
  #' @param calc_cols [vector] List of columns to aggregate in mean - include 
  #'                           columns you want in output CSV upon error
  #' @param check_cols [vector] List of columns to check for min < 0
  #' @param add_cols [list] Named list of additional columns and values to include 
  #'                        in CSV upon error
  #' @param return_data [bool] Default False. Specify true if data should be returned 
  #'                           after negatives have been capped.
  #' @param years_to_check [vector] List of years to check. Default just to 2020
  
  dt <- setDT(copy(data))
  
  
  # Ensure required cols are present
  if ('date' %ni% names(dt)) {
    dt[, date := '2020-01-01']
    warning(paste0("Supplied data missing column for date. I'm assuming you're ", 
                   "passing in a pre-aggregated dataset so 2020-01-01 was ",
                   "filled as the date to allow passage through subset_date(). ",
                   "If you're not passing in an aggregated dataset, this is ",
                   "your warning to pass in data with a date column for proper ",
                   "subsetting and validation."))
  }
  if ('location_id' %ni% names(dt)) {
    dt[, location_id := locid]
  }
  if ('location_name' %ni% names(dt)) {
    dt[, location_name := locname]
  }
  if ('year_id' %ni% names(dt)) {
    dt[, year_id := year(date)]
  }
  if (is.null(check_cols)) {
    check_cols <- copy(calc_cols)
  }
  if (any(calc_cols %ni% names(dt))) {
    m_col <- calc_cols[calc_cols %ni% names(dt)]
    stop('Error checking for negative values. Supplied column(s) [', 
         paste0(m_col, collapse=', '), 
         '] not present in supplied data.')
  }
  
  
  # Subset to 2020
  dt <- subset_date(dt, years_to_check)
  
  
  # Sum draws by day
  dt <- dt[, lapply(.SD, sum, na.rm=T), 
           by=c('location_id', 'location_name', 'year_id', 'age_group_id', 'sex_id', 'draw_var'),
           .SDcols=calc_cols]
  
  
  # Take mean of calc_cols by location_id, age_group_id, sex_id
  dt <- dt[, lapply(.SD, mean, na.rm=T), 
           by=c('location_id', 'location_name', 'year_id', 'age_group_id', 'sex_id'),
           .SDcols=calc_cols]
  
  
  # Foreach column to check
  for (col in check_cols) {
    # See if the min is < 0
    if (min(dt[[col]], na.rm=T) < 0) {
      # Create issues dataset and output
      issues <- dt[get(col) < 0]
      issues <- issues[, c('location_id', 'location_name', 'year_id', 'age_group_id', 'sex_id',
                           calc_cols), with=F]
      if (!is.null(add_cols)) {
        for (n in names(add_cols)) {
          issues[[n]] <- add_cols[[n]]
        }
      }
      fwrite(issues, paste0(roots$'ROOT', 'FILEPATH', nf_type, '_cov_', locid,
                            '_', col, '_errors.csv'))
      stop(paste0('Negative values in ', col, '.'))
    }
    rm(col)
  }
  
  
  # If data should be returned to the user
  if (return_data) {
    # If we pass the check above, cap any negatives at 0. The mean is > 0 so we
    # have positive results, but some locations have negative lower bounds which
    # fail in the EPI uploader
    t <- copy(data)
    
    for (col in check_cols) {
      t[get(col) < 0, eval(col) := 0]
      rm(col)
    }
    
    
    # Also cap maximum prevalence rate values at 1
    for (col in check_cols[check_cols %like% '_prev_rate']) {
      t[get(col) > 1, eval(col) := 1]
    }
    
    
    # Return the data
    return(t)
  }
  
  
  rm(dt)
  
}


## --------------------------------------------------------------------- ----


## Diagnostic Tools ---------------------------------------------------- ----

check_size <- function(obj_list, env = .GlobalEnv, print=F) {
  #' Function to print out the size in megabytes of a variable in memory
  #' @param obj_list [vector:chr] A list of characters representing existing objects in the specified [env].
  #' @param env [environment] The environment where obj_list objects exist. Default the global environment.
  #' @param print [bool] If True, message printed for size of variable. Default False.
  
  # Error handling
  if (!is.environment(env)) {
    stop('Supplied env is not an existing environment.')
  }
  if (is.object(obj_list)) {
    stop('Supplied obj_list is not a character vector')
  }
  if (any(c(obj_list) %ni% ls(envir = env))) {
    stop('Supplied obj_list contains non-existing objects.')
  }
  
  # Init size and calculate
  size <- 0
  for (obj in obj_list) {
    size <- size + (as.numeric(object.size(get(obj, envir = env))) / 1e9)
  }
  
  # Print if desired
  if (print) {
    cat(paste0('  Size: ', format(size, big.mark=',', scientific = F), ' gigabytes\n'))
  } 
  
  # Return the size
  return(size)
  
}


check_job_status <- function(net_id, start_date, end_date, cluster_project = NULL, 
                             job_prefix = NULL, limit = 500, save_output = F, 
                             output_path = NULL) {
  #' Convenience function to pull job status from QPID
  #' @param net_id [str] The UW Net ID of the user to query jobs for
  #' @param cluster_project [str] The Cluster Project to query jobs for. 
  #' @param start_date [str/date] The date to begin querying jobs from. Must be in YYYY-MM-DD format
  #' @param end_date [str/date] The date to end querying jobs from. Must be in YYYY-MM-DD format
  #' @param job_prefix [str] (Optional) Prefix of job to query
  #' @param limit [int] Number of results to limit to. Greater than 0, default 500
  #' @param save_output [bool] Specify True if job logs should be saved. Default False
  #' @param output_path [str] File path to output location if [save_output] is True.
  
  # Internal function
  .check_date <- function(d) {
    #' Convenience function to check date validity in YYYY-MM-DD format
    #' @param d [str/date] Date to parse
    
    # Parse date
    d_check <- list('y' = as.integer(unlist(str_split(d, '-'))[1]),
                    'm' = as.integer(unlist(str_split(d, '-'))[2]),
                    'd' = as.integer(unlist(str_split(d, '-'))[3]))
    
    # Check for validity
    if (d_check$y < 2015 | d_check$m > 12 | d_check$d > 31) {
      return(T)
    }
    
    # Return False
    return(F)
  }
  
  
  # Error handling
  if (is.null(net_id)) {
    stop('Supplied NULL or invalid net_id. This is a required argument.')
  }
  if (nchar(net_id) == 0) {
    stop('Supplied empty net_id. This is a required argument.')
  }
  if (is.null(start_date) | is.null(end_date)) {
    stop('Supplied NULL start_date or end_date. These are requred arguments.')
  }
  if (.check_date(start_date)) {
    stop('Supplied invalid start_date. Please use YYYY-MM-DD format.')
  }
  if (.check_date(end_date)) {
    stop('Supplied invalid end_date. Please use YYYY-MM-DD format.')
  }
  if (!is.null(job_prefix)) {
    if (nchar(job_prefix) < 1) {
      stop('Supplied job_prefix is blank. If supplied, job_prefix string must have length greater than 1.')
    }
  }
  if (is.null(limit)) {
    stop('Supplied NULL limit. This is a required argument.')
  }
  if (limit < 0) {
    stop('Supplied negative limit. This argument must be greater than 0.')
  }
  start_date <- as.IDate(start_date)
  end_date <- as.IDate(end_date)
  if (start_date == end_date) {
    warning(paste0('Supplied start_date and end_date are the same. The difference ', 
                   'between them must be at least one day, so end_date was ', 
                   'shifted from ', end_date, ' to ', (end_date+1)))
    end_date <- end_date + 1
  }
  if (!save_output) {
    if (!is.null(output_path)) {
      warning('Supplied save_output as FALSE but also provided output_path. No file will be output.')
    }
  } else {
    if (is.null(output_path)) {
      stop('Supplied save_output as TRUE but also provided NULL output_path. Please provide an output_path or set save_output to FALSE.')
    }
    if (nchar(output_path) < 1) {
      stop('Supplied save_output as TRUE but also provided blank output_path. Please provide an output_path or set save_output to FALSE.')
    }
    if (substr(output_path, nchar(output_path), nchar(output_path)) != '/') {
      output_path <- paste0(output_path, '/')
    }
    if (!dir.exists(output_path)) {
      warning(paste0('Creating directory at ', output_path, '.'))
      .ensure_dir(output_path)
    }
  }
  
  
  # Setup request
  if (is.null(job_prefix)) {
    request <- 'URL'
  } else {
    request <- 'URL'
  }
  
  
  # Parse parameters
  params <- list('owner' = net_id,
                 'project' = cluster_project,
                 'ran_after' = start_date,
                 'finished_before' = end_date,
                 'job_prefix' = job_prefix,
                 'limit' = limit)
  request$query <- params
  
  
  # Submit request
  'ROOT' <- httr::content(httr::GET(request))
  
  
  # Parse request and cast as data.table
  o <- data.table(matrix(unlist(j), nrow=length(j), 
                         byrow=TRUE),
                  stringsAsFactors=FALSE)
  colnames(o) <- names(j[[1]])[names(j[[1]]) != 'cores']
  o[, `:=`(cores_requested = as.integer(cores_requested),
           exit_status = as.integer(exit_status),
           failed = as.integer(failed),
           job_number = as.integer(job_number),
           maxpss = as.integer(maxpss),
           maxrss = as.integer(maxrss),
           maxvmem = as.double(maxvmem),
           ram_gb = as.double(ram_gb),
           ram_gb_requested = as.integer(ram_gb_requested),
           runtime_min = as.double(runtime_min),
           runtime_min_requested = as.integer(runtime_min_requested))]
  
  
  # Output to CSV if requested
  if (save_output) {
    
    f_name <- paste0(output_path, net_id)
    
    if (!is.null(cluster_project)) {
      f_name <- paste0(f_name, '_', cluster_project)
    }
    if (!is.null(job_prefix)) {
      f_name <- paste0(f_name, '_', job_prefix)
    }
    
    f_name <- paste0(f_name, '_qpid_job_logs_', start_date, '_', end_date)
    
    fwrite(o, paste0(f_name, '.csv'))
    
  }
  
  
  # Return result
  return(o)
  
}


.get_successful_locs <- function(output_version, stage, full.names=F) {
  #' Convenience function to pull the locations which were successfully output from a pipeline run
  #' @param output_version [str] Must resemble "2021-01-01.01"
  #' @param stage [str] One of: ['stage_1', 'stage_2', 'final']
  #' @param full.names [bool] (OPTIONAL) Argument if full filepath to data desired
  
  # Error handling
  if (stage %ni% c('stage_1', 'stage_2', 'final')) {
    stop('Supplied stage not one of the following: [stage_1, stage_2, final]')
  }
  
  
  # Identify output directory
  out_dir <- paste0('FILEPATH')
  if (stage == 'final') {
    out_dir <- paste0('FILEPATH')
  }
  
  
  # Pull folders (excluding "_for_long_covid" if stage=="stage_1")
  full_locs <- list.dirs(out_dir, full.names = full.names, recursive = F)
  internal_locs <- list.dirs(out_dir, full.names = F, recursive = F)
  if (stage == 'stage_1') {
    full_locs <- full_locs[!(full_locs %like% '_for_long_covid')]
    internal_locs <- internal_locs[!(internal_locs %like% '_for_long_covid')]
  } else if (stage == 'final') {
    internal_locs <- NA
    for (d in full_locs) {
      internal_locs <- c(internal_locs, list.files(paste0(out_dir, d), recursive = F))
    }
  }
  
  
  # Make data.table and split directory names
  r <- data.table(output_version = output_version, stage = stage, dir = full_locs,
                  internal_dir = internal_locs)
  r[, location_ascii_name := tstrsplit(internal_dir, '_', keep=1)]
  r[, location_id := tstrsplit(internal_dir, '_', keep=2)]
  r[, location_id := as.integer(location_id)]
  r[, internal_dir := NULL]
  
  
  # Return data.table
  return(r)
}


.run_diagnostics <- function(dt, filename, out_loc, loc_id, loc_name) {
  #' Internal function to run all diagnostics on input dataset
  #' @param dt [data.table/data.frame]
  #' @param filename [str] Must be paste-able onto '_inc_rate', '_prev_rate', and '_YLD' to locate column
  #' @param stage [str] One of: ['stage_1', 'stage_2', 'final']
  #' @param out_loc [str] 
  #' @param loc_id [int]
  #' @param loc_name [str]
  
  # Error handling
  if (any(c(paste0(filename, '_inc_rate'), paste0(filename, '_prev_rate'), 
            paste0(filename, '_YLD')) %ni% names(dt))) {
    stop(paste0('Missing column for one or more of the following: [', 
                paste0(filename, '_inc_rate'), ', ',
                paste0(filename, '_prev_rate'), ', ', 
                paste0(filename, '_YLD'), ']'))
  }
  
  
  # Internal functions
  .plot <- function(dt) {
    #' Internal function to plot data
    #' @param dt [data.table]
    
    # Pull age metadata
    source(paste0(roots$'ROOT', 'FILEPATH/get_ids.R'))
    age_meta <- get_ids('age_group')
    value_cols <- list('inc' = paste0(filename, '_inc_rate'), 
                       'prev' = paste0(filename, '_prev_rate'),
                       'yld' = paste0(filename, '_YLD'))
    
    
    # Merge age metadata
    data <- merge(dt, age_meta, by='age_group_id', all.x=T)
    
    
    data[, age_group_name := factor(age_group_name, 
                                    levels=c('Early Neonatal', 'Late Neonatal', '1-5 months', 
                                             '6-11 months', '12 to 23 months', '2 to 4', '5 to 9', 
                                             '10 to 14', '15 to 19', '20 to 24', '25 to 29', 
                                             '30 to 34', '35 to 39', '40 to 44', '45 to 49', 
                                             '50 to 54', '55 to 59', '60 to 64', '65 to 69', 
                                             '70 to 74', '75 to 79', '80 to 84', '85 to 89', 
                                             '90 to 94', '95 plus'))]
    
    
    # Aggregate incidence
    plot_inc <- copy(data)[, as.list(c(mean(get(value_cols$inc)), 
                                       quantile(get(value_cols$inc), c(0.025, 0.975)))), 
                           by = c('location_id', 'year_id', 'age_group_name', 'sex_id')]
    colnames(plot_inc) <- c('location_id', 'year_id', 'age_group_name', 'sex_id', 
                            "mean", "lower", "upper")
    
    
    # Aggregate prevalence
    plot_prev <- copy(data)[, as.list(c(mean(get(value_cols$prev)), 
                                        quantile(get(value_cols$prev), c(0.025, 0.975)))), 
                            by = c('location_id', 'year_id', 'age_group_name', 'sex_id')]
    colnames(plot_prev) <- c('location_id', 'year_id', 'age_group_name', 'sex_id', 
                             "mean", "lower", "upper")
    
    
    # Aggregate YLDs
    plot_yld <- copy(data)[, as.list(c(mean(get(value_cols$yld)), 
                                       quantile(get(value_cols$yld), c(0.025, 0.975)))), 
                           by = c('location_id', 'year_id', 'age_group_name', 'sex_id')]
    colnames(plot_yld) <- c('location_id', 'year_id', 'age_group_name', 'sex_id', 
                            "mean", "lower", "upper")
    
    
    # Recode sex IDs
    plot_inc$sex_id <- ifelse(plot_inc$sex_id == 1, 'Male', 
                              ifelse(plot_inc$sex_id == 2, 'Female', 'Both'))
    plot_prev$sex_id <- ifelse(plot_prev$sex_id == 1, 'Male', 
                               ifelse(plot_prev$sex_id == 2, 'Female', 'Both'))
    plot_yld$sex_id <- ifelse(plot_yld$sex_id == 1, 'Male', 
                              ifelse(plot_yld$sex_id == 2, 'Female', 'Both'))
    
    
    # Generate plots
    inc <- ggplot(data = plot_inc) +
      geom_point(mapping = aes(x=age_group_name, y=mean, color=as.factor(year_id))) +
      geom_errorbar(mapping = aes(x=age_group_name, ymin=lower, ymax=upper, color=as.factor(year_id)), 
                    width=0.2, size=1, alpha=0.2) +
      labs(x='Age Group ID', y='', title=paste0(loc_name, ' ', toupper(filename), ' Incidence'),
           color='Year') +
      facet_wrap('sex_id') +
      theme_bw() +
      theme(axis.text.x = element_text(hjust=1, angle=45))
    prev <- ggplot(data = plot_prev) +
      geom_point(mapping = aes(x=age_group_name, y=mean, color=as.factor(year_id))) +
      geom_errorbar(mapping = aes(x=age_group_name, ymin=lower, ymax=upper, color=as.factor(year_id)), 
                    width=0.2, size=1, alpha=0.2) +
      labs(x='Age Group ID', y='', title=paste0(loc_name, ' ', toupper(filename), ' Prevalence'),
           color='Year') +
      facet_wrap('sex_id') +
      theme_bw() +
      theme(axis.text.x = element_text(hjust=1, angle=45))
    yld <- ggplot(data = plot_yld) +
      geom_point(mapping = aes(x=age_group_name, y=mean, color=as.factor(year_id))) +
      geom_errorbar(mapping = aes(x=age_group_name, ymin=lower, ymax=upper, color=as.factor(year_id)), 
                    width=0.2, size=1, alpha=0.2) +
      labs(x='Age Group ID', y='', title=paste0(loc_name, ' ', toupper(filename), ' YLDs'),
           color='Year') +
      facet_wrap('sex_id') +
      theme_bw() +
      theme(axis.text.x = element_text(hjust=1, angle=45))
    
    
    return(list('inc' = inc, 'prev' = prev, 'yld' = yld))
  }
  
  .gen_meta <- function(dt, filename, loc_name, out_loc) {
    f_sex_ids <- unique(dt$sex_id)
    f_mean <- copy(dt)[, lapply(.SD, mean), by=c('year_id', 'age_group_id', 'sex_id'), 
                       .SDcols=c(paste0(filename, '_inc_rate'), 
                                 paste0(filename, '_prev_rate'),
                                 paste0(filename, '_YLD'))]
    f_str <- '  '
    for (msre in c('_inc_rate', '_prev_rate', '_YLD')) {
      
      f_str <- paste0(f_str, 'FILEPATH', filename, msre, '\n  ')
      
      for (i in 1:nrow(f_mean)) {
        
        f_str <- paste0(f_str, f_mean[i, 'year_id'], '\t\t')
        f_str <- paste0(f_str, f_mean[i, 'age_group_id'], '\t\t\t')
        f_str <- paste0(f_str, f_mean[i, 'sex_id'], '\t\t')
        f_str <- paste0(f_str, f_mean[i, get(paste0(filename, msre))], '\n  ')
        
      }
      
      f_str <- paste0(f_str, '\n  ')
    }
    
    
    
    cat(paste0(loc_name, ' ', toupper(filename), ' notes:\n\n',
               '  Sex IDs present: ', paste0(f_sex_ids, collapse=', '), '\n',
               '  CSV file size: ', check_size(obj_list = 'dt', env = environment(), 
                                               print = F), ' GB.\n\n',
               'Mean by age and sex: \n',
               f_str
               
               
    ),
    file=paste0('FILEPATH', '_meta.txt'))
  }
  
  
  # Produce some figures
  plots <- .plot(dt)
  pdf(file=paste0(out_loc, filename, '.pdf'), height=8, width=10)
  plot(plots$inc)
  plot(plots$prev)
  plot(plots$yld)
  dev.off()
  
  
  # Produce some summary notes (including dataset size)
  .gen_meta(dt, filename, loc_name, out_loc)
  
}


.secret_launch_qsub <- function(type, queue, output_version, duration = NULL, outcome = NULL, locs = NULL) {
  #' Secret function to launch diagnostics or save_results for short or long term outcomes
  #' @param type [str] Either "diagnostics" or "save_results"
  #' @param duration [str] Either "short" or "long"
  #' @param queue [str] Cluster queue to run job on
  #' @param output_version [str] YY-MM-DD.VV
  #' @param outcome [str] One of the short/long outcome (asymp, cognitive, mild, etc.)
  
  # Internal Function
  .check_date <- function(d) {
    #' Convenience function to check date validity in YYYY-MM-DD format
    #' @param d [str/date] Date to parse
    
    # Parse date
    d_check <- list('y' = as.integer(unlist(str_split(d, '-'))[1]),
                    'm' = as.integer(unlist(str_split(d, '-'))[2]),
                    'd' = as.integer(unlist(str_split(d, '-'))[3]))
    
    # Check for validity
    if (d_check$y < 2020 | d_check$y > 2050 | d_check$m > 12 | d_check$d > 31) {
      return(T)
    }
    
    # Return False
    return(F)
  }
  
  
  # Error handling
  if (type %ni% c('diagnostics', 'save_results', 'long', 'short')) {
    stop('Supplied invalid type. Please try again with either: [diagnostics, save_results, long, short].')
  }
  if (queue %ni% c('all.q', 'long.q', 'i.q')) {
    stop('Supplied invalid queue. Please try again with either: [all.q, i.q, long.q].')
  }
  sub_str <- list(date = str_split(output_version, '\\.')[[1]][1],
                  version = str_split(output_version, '\\.')[[1]][2])
  if (.check_date(sub_str$date) | sub_str$version < 0) {
    stop('Supplied invalid output_version. Please ensure it matches the format YYYY-MM-DD.VV')
  }
  if (type == 'diagnostics' & (!is.null(duration) | !is.null(outcome))) {
    warning('Provided type as diagnostics and either duration or outcome. ', 
            'The latter two will be ignored.')
    duration <- NULL
    outcome <- NULL
  }
  if (type == 'save_results') {
    if (is.null(duration)) {
      stop('Specified type as save_results but did not provide duration. Please',
           ' retry with a specified duration.')
    }
    if (duration %ni% c('short', 'long')) {
      stop('Provided invalid duration. Please retry with either: [short, long].')
    }
  }
  
  
  # Setup qsub
  if (type == 'diagnostics') {
    
    qsub <- paste0('QSUB PARAMETERS',
                   output_version)
    
  } else if (type == 'long') {
    
    qsub <- list()
    
    for (loc in as.character(locs)) {
      
      qsub[[loc]] <- paste0('QSUB PARAMETERS', output_version)
      
    }
    
  } else if (type == 'short') {
    
    qsub <- list()
    
    for (loc in as.character(locs)) {
      
      qsub[[loc]] <- paste0('QSUB PARAMETERS', output_version)
      
    }
    
  } else {
    
    if (duration == 'short') {
      
      # If no outcome passed in, run all
      if (is.null(outcome)) {
        
        qsub <- list()
        
        for (outcome in c('asymp', 'mild', 'moderate', 'hospital', 'icu')) {
          qsub[[outcome]] <- paste0('QSUB PARAMETERS', outcome)
        }
        
      } else {
        
        qsub <- list()
        
        for (o in outcome) {
          
          qsub[[o]] <- paste0('QSUB PARAMETERS',
                              output_version, ' ', o)
          
        }
        
      }
      
    } else {
      
      # If no outcome passed in, run all
      if (is.null(outcome)) {
        
        qsub <- list()
        
        for (outcome in c('cognitive_mild', 'cognitive_severe', 'fatigue', 'respiratory_mild', 
                          'respiratory_moderate', 'respiratory_severe', 'cognitive_mild_fatigue', 
                          'cognitive_severe_fatigue', 'cognitive_mild_respiratory_mild', 
                          'cognitive_mild_respiratory_moderate', 'cognitive_mild_respiratory_severe', 
                          'cognitive_severe_respiratory_mild', 'cognitive_severe_respiratory_moderate', 
                          'cognitive_severe_respiratory_severe', 'fatigue_respiratory_mild', 
                          'fatigue_respiratory_moderate', 'fatigue_respiratory_severe', 
                          'cognitive_mild_fatigue_respiratory_mild', 'cognitive_mild_fatigue_respiratory_moderate', 
                          'cognitive_mild_fatigue_respiratory_severe', 'cognitive_severe_fatigue_respiratory_mild', 
                          'cognitive_severe_fatigue_respiratory_moderate', 'cognitive_severe_fatigue_respiratory_severe', 'gbs',
                          'any', 'midmod_any', 'hospital_any', 'icu_any')) {
          qsub[[outcome]] <- paste0('QSUB PARAMETERS',
                                    output_version, ' ', outcome)
        }
        
      } else {
        
        qsub <- list()
        
        for (o in outcome) {
          
          qsub[[o]] <- paste0('QSUB PARAMETERS',
                              output_version, ' ', o)
          
          
        }
        
      }
      
    }
    
  }
  
  
  # Launch qsub
  .ensure_dir(paste0('FILEPATH/'))
  if (is.list(qsub)) {
    
    for (i in names(qsub)) {
      
      system(qsub[[i]])
      
    }
    
  }
  
  else {
    
    system(qsub)
    
  }
  
  cat(paste0('Logs being output to: FILEPATH'))
  
}

## --------------------------------------------------------------------- ----


## Saving Datasets ----------------------------------------------------- ----

.ensure_dir <- function(path) {
  #' Internal function to make sure output directory exists
  #' @param path [str] Filepath output location
  
  # If path doesn't exist, create it (recursively) with open read/write permissions
  if (!dir.exists(path)) {
    dir.create(path, recursive = T, mode = '0777') 
  } 
  
}


.check_squareness <- function(dt, stage, filename, l) {
  #' Internal function to make sure dataset has all expected columns + rows
  #' @param dt [data.table/data.frame]
  #' @stage [str]
  #' @filename [str] Must be paste-able onto '_inc_rate', '_prev_rate', and '_YLD' to locate column
  #' @l [data.table] Data.table with location_id and most_detailed columns
  
  cat('  Checking for square-ness :: ')
  
  
  # Load valid columns and observations for this stage
  yaml <- yaml.load_file(paste0(roots$'ROOT', 'refs.yaml'))$valid_col_obs[[stage]]
  # yaml$location_id <- l[most_detailed==1]$location_id
  
  
  # Add extra obs 
  if (stage == 'stage_1') {
    yaml$draw_var <- roots$draws
    yaml$year_id <- roots$estimation_years
    yaml$age_group_id <- roots$age_groups
    yaml[[paste0(filename, '_inc_rate')]] <- yaml$`_inc_rate`
    yaml[[paste0(filename, '_prev_rate')]] <- yaml$`_prev_rate`
    yaml[[paste0(filename, '_YLD')]] <- yaml$`_YLD`
    yaml$`_inc_rate` <- NULL
    yaml$`_prev_rate` <- NULL
    yaml$`_YLD` <- NULL
  } else if (stage == 'stage_2') {
    yaml$draw_var <- roots$draws
    yaml$year_id <- roots$estimation_years
    yaml$age_group_id <- roots$age_groups
    yaml[[paste0(filename, '_inc_rate')]] <- yaml$`_inc_rate`
    yaml[[paste0(filename, '_prev_rate')]] <- yaml$`_prev_rate`
    yaml[[paste0(filename, '_YLD')]] <- yaml$`_YLD`
    yaml$`_inc_rate` <- NULL
    yaml$`_prev_rate` <- NULL
    yaml$`_YLD` <- NULL
  } else if (stage == 'final') {
    yaml$age_group_id <- roots$age_groups
    yaml$year_id <- roots$all_gbd_estimation_years
    for (draw in roots$draws) {
      yaml[[draw]] <- yaml$draw_obs
    }
    yaml$draw_obs <- NULL
    yaml$draw_cols <- NULL
  }
  
  
  # Throw error if missing any columns
  if (any(names(yaml) %ni% names(dt))) {
    missing_cols <- names(yaml)[names(yaml) %ni% names(dt)]
    stop('  Supplied dt is missing columns for [', missing_cols, '] at ', stage, '.')
  }
  
  
  # Loop through columns and check observations
  for (col in names(yaml)) {
    # Pull valid observations
    valid_obs <- yaml[[col]]
    
    
    # If we're looking at floating point data, make sure it's under threshold
    if (all(!is.integer(valid_obs)) & all(!is.character(valid_obs))) {
      
      if (any(unique(dt[[col]])[(unique(dt[[col]]) > valid_obs) | (unique(dt[[col]]) < 0)])) {
        cat('failed\n')
        culprit <- unique(dt[[col]])[(unique(dt[[col]]) > valid_obs) | (unique(dt[[col]]) < 0)]
        stop(paste0('Column [', col, '] contains invalid observation of: ', 
                    paste0(culprit, collapse=', '), '. Check your data do ensure [',
                    col, '] only has observations of: [',
                    paste0(valid_obs, collapse=", "), '].'))
      }
      
    } 
    # Otherwise, make sure listed observations are present
    else {
      
      if (any(unique(dt[[col]]) %ni% valid_obs)) {
        cat(' :: failed\n')
        culprit <- unique(dt[[col]])[unique(dt[[col]]) %ni% valid_obs]
        stop(paste0('Column [', col, '] contains invalid observation of: ',
                    paste0(culprit, collapse=', '), '. Check your data do ensure [',
                    col, '] only has observations of: [',
                    paste0(valid_obs, collapse=", "), '].'))
      }
    }
    
  }
  
  cat('passed\n')
}


.finalize_data <- function(measure_stub, i_base, loc_name, loc_id) {
  #' Convenience function to save out prepped dataset for epi uploading
  #' @param measure_stub [str] Name of desired input file (without .csv)
  #' @param i_base [str] Filepath base to pull data from
  #' @param loc_name [str]
  #' @param loc_id [int]
  
  .add_annual_dummies <- function(df, measure_colname_stub, years_to_add) {
    #' Convenience function to add dummy values for estimation years
    #' @param df [data.table]
    #' @param measure_colname_stub [str] Must be paste-able onto '_inc_rate' & '_prev_rate' to locate column
    #' @param years_to_add [vector] List of years to add within the estimation years
    
    # Prep data to append
    temp <- copy(df[year_id == min(year_id)])
    
    
    # Zero out future years
    temp[, eval(paste0(measure_colname_stub, '_inc_rate')) := 0]
    temp[, eval(paste0(measure_colname_stub, '_prev_rate')) := 0]
    
    
    # Copy to estimation years
    est <- data.table()
    for (yr in years_to_add) {
      t <- copy(temp)
      t[, year_id := yr]
      est <- rbind(est, t)
      rm(t)
    }
    
    
    return(rbind(est, df))
    
  }
  
  
  # Pull data
  dt <- read_feather(paste0('FILEPATH', '.feather'))
  
  
  # Remove unneeded observations
  dt <- dt[, c('location_id', 'year_id', 'age_group_id', 'sex_id', 'draw_var',
               paste0(measure_stub, '_inc_rate'), 
               paste0(measure_stub, '_prev_rate')), with=F]
  
  
  # Add estimation years
  dt <- .add_annual_dummies(dt, measure_stub, roots$all_gbd_estimation_years[roots$all_gbd_estimation_years %ni% unique(dt$year_id)])
  
  
  # Reshape long by measure
  setnames(dt, c(paste0(measure_stub, '_inc_rate'), 
                 paste0(measure_stub, '_prev_rate')),
           c('incidence', 'prevalence'))
  dt <- melt(dt, measure.vars = c('incidence', 'prevalence'))
  setnames(dt, 'variable', 'measure_name')
  dt$measure_id <- ifelse(dt$measure_name == 'incidence', 6, 5)
  
  
  # Reshape wide by draw
  dt <- dcast(dt, formula = 'location_id + sex_id + age_group_id + year_id + 
                                 measure_name + measure_id ~ draw_var',
              value.var = 'value')
  
  
  # Format column order
  dt <- dt[, c('location_id', 'sex_id', 'age_group_id', 'year_id', 
               'measure_id', roots$draws), with=F]
  
  
  return(dt)
  
}


save_dataset <- function(dt, filename, stage, output_version, loc_id, loc_name) {
  #' Function to save out dataset to specified location
  #' @param dt [data.table/data.frame]
  #' @param filename [str] Name of desired output file (without .csv)
  #' @param stage [str] One of: ['stage_1', 'stage_2', 'final']
  #' @param output_version [str] The version where data should be saved
  #' @param loc_id [int] The location id of the file being saved
  #' @param loc_name [str] The location name of the file being saved
  
  # Error handling
  if (filename %ni% c('asymp', 'mild', 'moderate', 'hospital', 'icu', 'cognitive_mild', 
                      'cognitive_severe', 'fatigue', 'respiratory_mild', 'respiratory_moderate', 
                      'respiratory_severe', 'cognitive_mild_fatigue', 'cognitive_severe_fatigue', 
                      'cognitive_mild_respiratory_mild', 'cognitive_mild_respiratory_moderate', 
                      'cognitive_mild_respiratory_severe', 'cognitive_severe_respiratory_mild', 
                      'cognitive_severe_respiratory_moderate', 'cognitive_severe_respiratory_severe', 
                      'fatigue_respiratory_mild', 'fatigue_respiratory_moderate', 'fatigue_respiratory_severe', 
                      'cognitive_mild_fatigue_respiratory_mild', 'cognitive_mild_fatigue_respiratory_moderate', 
                      'cognitive_mild_fatigue_respiratory_severe', 'cognitive_severe_fatigue_respiratory_mild', 
                      'cognitive_severe_fatigue_respiratory_moderate', 'cognitive_severe_fatigue_respiratory_severe', 'gbs', 
                      'any', 'midmod_any', 'hospital_any', 'icu_any')) {
    stop(paste0('Supplied filename is not one of the following: [asymp, mild, moderate, ',
                'hospital, icu, cognitive_mild, cognitive_severe, fatigue, respiratory_mild, ', 
                'respiratory_moderate, respiratory_severe, cognitive_mild_fatigue, cognitive_severe_fatigue, ', 
                'cognitive_mild_respiratory_mild, cognitive_mild_respiratory_moderate, cognitive_mild_respiratory_severe, ', 
                'cognitive_severe_respiratory_mild, cognitive_severe_respiratory_moderate, cognitive_severe_respiratory_severe, ', 
                'fatigue_respiratory_mild, fatigue_respiratory_moderate, fatigue_respiratory_severe, ', 
                'cognitive_mild_fatigue_respiratory_mild, cognitive_mild_fatigue_respiratory_moderate, ', 
                'cognitive_mild_fatigue_respiratory_severe, cognitive_severe_fatigue_respiratory_mild, ', 
                'cognitive_severe_fatigue_respiratory_moderate, cognitive_severe_fatigue_respiratory_severe, gbs, ',
                'any, midmod_any, hospital_any, icu_any]. ',
                'If you have just added a new outcome, update the save_dataset() function in utils ',
                'to accept the new outcome as a valid option.'))
  }
  if (any(c(paste0(filename, '_inc_rate'), paste0(filename, '_prev_rate'),
            paste0(filename, '_YLD')) %ni% names(dt))) {
    stop('Missing column(s) for one or more of the following: [', 
         paste0(filename, '_inc_rate'), ', ',
         paste0(filename, '_prev_rate'), ', ',
         paste0(filename, '_YLD'), ']')
  }
  if (stage %ni% c('stage_1', 'stage_2', 'final')) {
    stop('Supplied stage not one of the following: [stage_1, stage_2, final]')
  }
  
  
  
  cat(paste0(loc_name, ' ', toupper(filename), '\n'))
  
  # Check for "square-ness"
  source(paste0(roots$'ROOT', 'FILEPATH/get_location_metadata.R'))
  l <- get_location_metadata(location_set_id = 35, release_id = roots$gbd_round)
  .check_squareness(dt, stage, filename, l)
  rm(l)
  
  
  # Pull output filepath
  out_loc <- paste0(get_core_ref('data_output', stage), 'FILEPATH')
  
  
  # Ensure output location exists
  .ensure_dir(out_loc)
  .ensure_dir(paste0(out_loc, 'FILEPATH'))
  
  
  # Output csv
  cat('  Writing data :: ')
  write_feather(dt, paste0('FILEPATH', '.feather'))
  cat('done\n')
  
  
  # Run diagnostics
  cat('  Running diagnostics :: ')
  .run_diagnostics(dt, filename, paste0(out_loc, 'FILEPATH'), loc_id, loc_name)
  cat('done\n')
  
  
}


save_epi_dataset <- function(dt, stage, output_version, me_name, 
                             measure_id, loc_id, loc_name, l) {
  #' Convenience function to save out dataset before loading into epi database
  #' @param dt [data.table]
  #' @param stage [str] One of: ['stage_1', 'stage_2', 'final']
  #' @param output_version [str] 
  #' @param me_name [str] Name of desired input file (without .csv)
  #' @param measure_id [int] One of: [5, 6]
  #' @param loc_id [int]
  #' @param loc_name [str]
  #' @param l [data.table] Data.table with location_id column
  
  # Error handling
  if (stage %ni% c('stage_1', 'stage_2', 'final')) {
    stop('Supplied stage not one of the following: [stage_1, stage_2, final]')
  }
  if (me_name %ni% c('asymp', 'mild', 'moderate', 'hospital', 'icu', 'cognitive_mild', 'cognitive_severe', 
                     'fatigue', 'respiratory_mild', 'respiratory_moderate', 'respiratory_severe', 
                     'cognitive_mild_fatigue', 'cognitive_severe_fatigue', 'cognitive_mild_respiratory_mild', 
                     'cognitive_mild_respiratory_moderate', 'cognitive_mild_respiratory_severe', 
                     'cognitive_severe_respiratory_mild', 'cognitive_severe_respiratory_moderate', 
                     'cognitive_severe_respiratory_severe', 'fatigue_respiratory_mild', 
                     'fatigue_respiratory_moderate', 'fatigue_respiratory_severe', 
                     'cognitive_mild_fatigue_respiratory_mild', 'cognitive_mild_fatigue_respiratory_moderate', 
                     'cognitive_mild_fatigue_respiratory_severe', 'cognitive_severe_fatigue_respiratory_mild', 
                     'cognitive_severe_fatigue_respiratory_moderate', 'cognitive_severe_fatigue_respiratory_severe', 'gbs',
                     'any', 'midmod_any', 'hospital_any', 'icu_any')) {
    stop('Supplied filename is not one of the following: [asymp, mild, moderate, ',
         'hospital, icu, cognitive_mild, cognitive_severe, fatigue, respiratory_mild, respiratory_moderate, ', 
         'respiratory_severe, cognitive_mild_fatigue, cognitive_severe_fatigue, cognitive_mild_respiratory_mild, ', 
         'cognitive_mild_respiratory_moderate, cognitive_mild_respiratory_severe, cognitive_severe_respiratory_mild, ', 
         'cognitive_severe_respiratory_moderate, cognitive_severe_respiratory_severe, fatigue_respiratory_mild, ', 
         'fatigue_respiratory_moderate, fatigue_respiratory_severe, cognitive_mild_fatigue_respiratory_mild, ', 
         'cognitive_mild_fatigue_respiratory_moderate, cognitive_mild_fatigue_respiratory_severe, ', 
         'cognitive_severe_fatigue_respiratory_mild, cognitive_severe_fatigue_respiratory_moderate, 
         cognitive_severe_fatigue_respiratory_severe, gbs, any, midmod_any, hospital_any, icu_any]. If you have just added a new outcome ',
         'please update the save_epi_dataset() function in utils to allow for the new option.')
  }
  if (measure_id %ni% c(5, 6)) {
    stop('Supplied measure_id is not one of the following: [5, 6]')
  }
  
  
  # Setup
  m_n <- ifelse(measure_id == 5, 'prevalence', 'incidence')
  cat(paste0(loc_name, ' ', me_name, ' ', m_n, '\n'))
  
  
  # Check for square-ness
  .check_squareness(dt, stage, me_name, l)
  rm(l)
  
  
  # Pull output filepath
  out_dir <- paste0(get_core_ref('data_output', 'final'), 'FILEPATH')
  f_name <- paste0(loc_name, 'FILEPATH', '.csv')
  
  
  # Ensure output location exists
  .ensure_dir(out_dir)
  
  
  # Output csv
  cat(paste0('  Writing data :: '))
  fwrite(dt, 'FILEPATH')
  cat('done\n')
  
}
## --------------------------------------------------------------------- ----


## Time Checks --------------------------------------------------------- ----
total_end <- Sys.time()
cat(paste0('  Sourcing time: ', total_end - total_start, ' sec.\n\n'))
cat('---------------------------------------------------------\n\n')
rm(total_start, total_end)
## --------------------------------------------------------------------- ----