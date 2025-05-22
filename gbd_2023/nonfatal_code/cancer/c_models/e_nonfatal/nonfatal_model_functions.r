#!/usr/local/bin/R
#########################################
## Description: Loads functions common to the nonfatal modeling processes
## Input(s)/Output(s): see individual functions

## Contributors: INDIVIDUAL_NAME
#########################################
## Load Libraries
library(here)
library(dplyr)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r'))  
source(file.path(get_path('shared_r_libraries'), "get_population.R"))
source(file.path(get_path('shared_r_libraries'), "get_location_metadata.R"))
source(get_path('cdb_utils_r'))
library(data.table)
library(foreign)

#########################################
## Define Functions
#########################################
nonfatal_model.add_all_age <- function(df, uid_vars, data_var) {
    ## Returns the input "df" data with "all ages" data if "all ages" are not 
    ##      present
    ##
    if (22 %in% unique(df$age_group_id)) {
        return (df)
    }
    all_ages_members <- get_gbd_parameter('current_age_groups') #c( seq(2,20), seq( 30, 32), 235)
    data_variables <- names(df)[data_var %in% names(df)]
    to_aggregate <- subset(df, df$age %in% all_ages_members, data_variables)
    all_age_data <- as.data.frame(aggregate(to_aggregate, uid_vars, sum))
    return(rbind(df, all_age_data))
}

na_check <- function(df, draw_cols) {
  
  has_nas <- F
  for (i in seq_along(draw_cols)) {
    col_name <- draw_cols[i]
    col_to_test = select(df, col_name)
    if (any(is.na(col_to_test))) {
      print(paste0("Column #", i, ", named ", col_name, ", has NAs in it"))
      has_nas <- T
    }
  }
  return(has_nas)
}

nonfatal_model.test_draw_values <- function(df, draw_cols) {
  ## Checks that vales for all draws are > 0 
  ##
  # setting tolerance given floating point precision
  tol <- get_gbd_parameter("inc_zero_tol", 
                            parameter_type = "nonfatal_parameters")$inc_zero_tol
  draw_cols <- colnames(df)[grepl("inc",colnames(df))]
  bad_draw_col <- c()
  has_nas <- na_check(df, draw_cols)
  if (has_nas) {
    username <- Sys.getenv("USER")
    fp <- paste0("FILEPATH", username, "/")
    if (not (is.null(location_id))) {
      fp <- paste0(fp, toString(location_id), "_")
    }
    fp <- paste0(fp, "nf_nas_in_files.csv")
    write.csv(df, fp)
    print(paste("Wrote file with data to", fp))
  }
  for (d in draw_cols) { 
    tmp <- select(df, d) 
    if (any(tmp <= tol)) { # float point precision with R?
      print(paste0('zero/negative values exists for draw: ', d))
      bad_draw_col <- c(bad_draw_col, d)
    }
  }
  # exception locs to allow 0s to pass
  cur_loc <- df$location_id %>% unique
  except_locs <- get_gbd_parameter("zero_exception_locs", parameter_type = "nonfatal_parameters")
  if(cur_loc %in% except_locs){
      print("This is an exception location where 0s are allowed")
      bad_draw_col <- c()
  }
  print(paste0('bad_draw_col: ', bad_draw_col))
  return(bad_draw_col)
}

nonfatal_model.finalize_draws <- function(df, output_file, draw_vars) {
    ## Checks for duplicates then saves the input "df" as a stata file
    ##
    `%ni%` <- Negate(`%in%`)
    uid_vars <- colnames(df)[colnames(df) %ni% draw_vars]
    if (anyDuplicated(df, by=uid_vars)) {
        stop("duplicates found during finalization.")
    }
    if (nrow(df) < 10) {
        stop("dataframe is missing data")
    }
    bad_cols <- nonfatal_model.test_draw_values(df, draw_vars)
    # if (!(file.exists(paste0("FILEPATH", bad_cols, ".csv")))){
    #     write.csv(df, paste0("FILEPATH", bad_cols, ".csv"), row.names = FALSE)
    # }
    if (length(bad_cols) > 0) { 
        stop('0 or negative estimates exist')
    }
    ensure_dir(output_file)
    output_df = subset( df, , c( uid_vars, draw_vars) )
    write.csv(output_df , output_file, row.names=FALSE)
    print(paste("file saved at", output_file))
}


nonfatal_model.get_location_list <- function() {
    ## Returns a list of the expected locations for the nonfatal model 
    ##
    release_id = get_gbd_parameter("current_release_id")
    loc_meta = get_location_metadata(location_set_id=35, release_id = release_id)
    return(unique(loc_meta$location_id))
}



nonfatal_model.get_expected_ages <- function(cause) { 
    ## returns list of expected age_group_ids for a given cancer cause 
    ##

    get_young_ages <- function(age) {
      if (age == 0) {
          young_ages = get_gbd_parameter('young_ages_new')$young_ages_new
      }
      else if (age == 1) {
          young_ages = c(238, 34)
      }
      else if (age == 2) {
          young_ages = c(34)
      }
      else {
          young_ages = c()
      }
      return(young_ages)
    }

    release_id <- get_gbd_parameter('current_release_id')

    # load age restrictions from cancer database

    
    can_causes <- fread(paste0(get_path(
        process='nonfatal_model', key='database_cache'), '/registry_input_entity.csv'))
    can_causes <- can_causes[release_id == release_id, ]
    max_refresh <- max(can_causes[, refresh], na.rm=TRUE)
    can_causes <- can_causes[refresh == max_refresh, c('acause','yld_age_start','yld_age_end')]

    # list of age groups <5
    this_cause <- can_causes[acause == cause, ]
    young_ages = get_young_ages(this_cause[,yld_age_start])

    # pull all gbd_age groups and remove <5 groups 
    #can_ages <- as.data.table(cdb.get_table('cancer_age_conversion'))
    can_ages <- fread(paste0(get_path(
        process='nonfatal_model', key='database_cache'), '/cancer_age_conversion.csv'))
    young_ages_removed <- can_ages[start_age >= 5 &
                                    (gbd_age_id %ni% young_ages) &
                                    start_age >= this_cause[,yld_age_start] & 
                                    start_age <= this_cause[,yld_age_end], gbd_age_id]

    expected_ages <- c(young_ages, young_ages_removed)
    return(expected_ages)
}

nonfatal_model.get_modeledCauseList <- function(starting_with="neo_") {
    ## Returns a cause list for the selected model with optional subset
    ##
    
    cause_table <- read.csv(paste0(get_path(
        process='nonfatal_model', key='database_cache'), '/cnf_model_entity.csv'))
    if (starting_with != "neo_") {
        cause_table <- cause_table[startsWith(cause_table$acause, starting_with),]
    }
    cause_list <- as.list(unique(cause_table[cause_table$is_active == 1 & 
                                        grepl("custom", cause_table[['cancer_model_type']]), 
                                        'acause']))
    return(cause_list)
}

nonfatal_model.calc_asr <- function(asr_input, loc_id, data_var) {
    ## Returns a dataframe with the age_standardised rate for the input df
    ##
    load_age_weights <- function(){
        age_weights <- read.csv(get_path("age_standardized_weights"))
        setnames(age_weights, 'age_group_weight_value', 'weight')
        if (nrow(age_weights) == 0) Sys.stop("Error loading age weights")
        return(age_weights)
    }
    add_population <- function(df) {
        release_id = get_gbd_parameter("current_release_id")
        pop_data <- get_population(location_id = loc_id, age_group_id='all',
                                    year_id='all', sex_id='all', release_id = release_id)
        pop_data <- as.data.frame(pop_data)
        output <- merge(df, pop_data, on=uid_cols, all.y=FALSE)
        if (nrow(output)!=nrow(df)){
            Sys.exit("Error generating ASR when merging with population")
        }
        return(output)
    }
    print("Calculating asr...")
    # keep age-specific data and compile to asr
    asr_age_groups = get_gbd_parameter('current_age_groups') #c(1, 235, seq(6, 20), seq(30, 32))
    uid_cols <- c('location_id', 'year_id', 'sex_id', 'age_group_id')
    age_weights <- load_age_weights()
    #
    asr_input = as.data.frame(asr_input)
    calc_cols =  colnames(asr_input)[grepl(data_var, colnames(asr_input))]
    asr_input = subset(asr_input, age_group_id %in% asr_age_groups, )
    asr_merge = merge(asr_input, age_weights, on='age_group_id')
    asr_calc <- add_population(asr_merge)
    asr_calc[,'prop'] = asr_calc[,'weight'] / asr_calc[,'population']
    asr_calc = as.data.table(asr_calc)
    asr_calc[, (calc_cols) := lapply(.SD, function(x)
                                x * asr_calc[['prop']] ), .SDcols = calc_cols]
    asr_calc = as.data.frame(asr_calc)
    asr_calc['age_group_id']=27
    asr_data = aggregate(asr_calc[calc_cols], by=asr_calc[uid_cols], FUN=sum, na.rm=TRUE)

    return(asr_data)
}
