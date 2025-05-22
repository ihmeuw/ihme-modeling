################################################################################
## Description: Loads functions to calculate incidence data
## Input(s): gbd cause and location_id
## Output(s): saves formatted incidence draws for the cause and location id in  
##              the nonfatal workspace
## How To Use: intended for submission as a cluster job in the nonfatal pipeline 
##      (see "Run Functions" below), but can also be run with Rscript to retrieve
##       results for a single cause-location_id
## Notes: See additional notes in the format_mir_draws script

################################################################################
## load Libraries
options(error = quote({dump.frames(to.file=TRUE); q()}))

library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here::here())
    if (!grepl("cancer_estimation", code_repo)) {
        code_repo <- file.path(code_repo, 'cancer_estimation')
    }
}
library(plyr)
source(file.path(code_repo, 'FILEPATH/utilities.r'))
source(get_path('nonfatal_functions', process="nonfatal_model"))
source(get_path('load_mir_draws', process="nonfatal_model"))
source(get_path('load_mortality_draws', process="nonfatal_model"))
source(get_path("r_test_utilities"))
source(get_path('cdb_utils_r'))
library(magrittr)
library(feather)
rb_custom_rids <- get_gbd_parameter("neo_eye_rb_winsorize", 
                                    parameter_type = "nonfatal_parameters")$neo_eye_rb_winsorize
hbl_custom_rids <- get_gbd_parameter("neo_liver_hbl_mir_mean",
                                    parameter_type = "nonfatal_parameters")$neo_liver_hbl_mir_mean


################################################################################
## Define Functions
################################################################################
generate_incidence_draws <- function(this_cause, local_id, cnf_model_version_id,
                                                         faux_correct=TRUE,
                                                         is_resubmission=FALSE,
                                                         is_estimation_yrs=TRUE) {
    ## Runs function to calculate incidence draws, then finalizes (thus saving) 
    ##      the results
    ##
    if (this_cause == "neo_eye_rb" & cnf_model_version_id %in% rb_custom_rids) {
        print("Going through apply rb modeling hotfix in generate incidence draws")
        main_dir = get_path(process='nonfatal_model', key='incidence_temp_draws')
        output_folder = paste0(main_dir, '/', this_cause, '/pre_winsorize')
        output_file = paste0(output_folder, '/', local_id, '.csv')
    }
    else { 
        output_folder = get_path("incidence_draws_output", process="nonfatal_model")
        output_file = paste0(output_folder, "/", this_cause, "/", local_id, ".csv")
    }
    ensure_dir(output_folder)
    print('calling calculate_incidence')
    this_data <- calculate_incidence(this_cause, local_id, cnf_model_version_id,
                faux_correct, is_resubmission)
    draw_cols <- colnames(this_data)[grepl("draw",colnames(this_data))]
    inc_cols <- gsub("draw", "inc", draw_cols)
    setnames(this_data, draw_cols, inc_cols)
    nonfatal_model.finalize_draws(this_data, output_file, inc_cols)
    print("Incidence refreshed!")
}

calculate_incidence <- function(this_acause, this_location_id, cnf_model_version_id,
                                faux_correct=TRUE, is_resubmission=FALSE, is_estimation_yrs=TRUE) {
    
    
    

  print('calculate incidence called')
    uid_cols <- c('year_id','sex_id','age_group_id')
    #
    refresh = TRUE
    if (is_resubmission == TRUE) {
        print("resubmission mode")
        mir_dir = get_path("mir_draws_output", process="nonfatal_model")
        mir_file = paste0(mir_dir, "/", this_acause, "/", this_location_id, ".csv")
        mort_dir = get_path("mortality_draws_output", process="nonfatal_model")
        mortality_file = paste0(mort_dir, "/", this_acause, "/", this_location_id, ".csv")
        if (file.exists(mir_file) & file.exists(mortality_file)) {
            mir_data <- read.csv(mir_file)
            mortality_data <- read.csv(mortality_file)
            refresh = FALSE
        }
    }
    if (refresh){
        mir_dir = get_path("mir_draws_output", process="nonfatal_model")
        mir_file = paste0(mir_dir, "/", this_acause, "/", this_location_id, ".csv")
        print('calling format_mir.r')
        mir_data <-  as.data.frame(format_mir(this_acause, this_location_id, cnf_model_version_id))
        if (this_acause %in%  get_gbd_parameter('NB_causes')$NB_causes) { 
            names(mir_data)[names(mir_data) == 'year'] <- 'year_id' 
            names(mir_data)[names(mir_data) == 'sex'] <- 'sex_id'
        }
        print('calling format_mortality.r')
        mor_df = format_mortality(this_acause, this_location_id, cnf_model_version_id)
        mortality_data <- as.data.frame(mor_df)
    }
    #
    mor_cols <- colnames(mortality_data)[grepl("deaths",colnames(mortality_data))]
    mir_cols <- gsub("deaths", "mir", mor_cols)
    draw_cols <- gsub("deaths", "draw", mor_cols)
    

    # Generate 
    merged_data <- merge(mortality_data[,c(uid_cols,mor_cols)], 
                        mir_data[,c(uid_cols,mir_cols)], 
                        by=uid_cols)
    #
    `%ni%` <- Negate(`%in%`)
    merged_data <- subset(merged_data, merged_data$age_group_id %ni% c(22, 27), )

    if (cnf_model_version_id %in% hbl_custom_rids & (this_acause == "neo_liver_hbl")) { 
        print('applying MIR means for neo_liver_hbl!!!')
        merged_data <- as.data.table(merged_data)
        # calculate mean of MIR draws 
        merged_data[, mir_mean := rowMeans(.SD), .SDcols = mir_cols]
        merged_data <- as.data.frame(merged_data) # re-assign to data.frame 
        incidence_data <- merged_data[,mor_cols] / merged_data[, 'mir_mean']
    }
    else { 
        incidence_data <- merged_data[,mor_cols] / merged_data[,mir_cols]
    }

    colnames(incidence_data) <- draw_cols
    #
    incidence_draws <- cbind(merged_data[uid_cols] , incidence_data)
	  incidence_draws$location_id <- this_location_id
    incidence_draws$acause <- this_acause
    test_incidence_draws(incidence_draws, this_acause, uid_cols, is_estimation_yrs)
    return(incidence_draws)
}


################################################################################
## Define Tests
################################################################################
test_incidence_draws <- function(df, acause, uid_columns, is_estimation_yrs){
    ##
    ##
    results = list('missing columns'=c(), 'duplicates check'=c())
    results['duplicates check'] = test_utils.duplicateChecker(df, uid_columns)
    results['missing columns'] = test_utils.findMissingColumns(df, uid_columns)
    results['columns missing values'] = test_utils.findMissingValues(df, uid_columns)
    if (any(df['location_id'] < 1)) results['incorrect values'] = "see location_id column"
    results['squareness check'] = validate_square_data(df, acause, is_estimation_yrs) 
    test_utils.checkTestResults(results, "calculate_incidence")
}


validate_square_data <- function(df, acause, is_estimation_yrs, passResult = 'passed', failResult='failed') { 
    ##
    ##
    df <- as.data.table(df) 
    is_square = FALSE    
    print('Checking if data is square...')
    if (test_expected_sex(df, acause)) { 
        for (this_sex in unique(df[,sex_id])) { 
            sub <- df[sex_id==this_sex, ]
            if (test_expected_years(sub, is_estimation_yrs)) { 
                for (this_year in unique(sub[,year_id])) { 
                    final_sub <- sub[year_id==this_year, ]
                    print(paste0('Checking ages for sex: ', this_sex, ', year: ', this_year))
                    print(acause)
                    age_check <- test_expected_ages(final_sub, acause)
                    if (age_check == FALSE | (!test_expected_years(sub, is_estimation_yrs)) | (!test_expected_sex(df, acause))) { 
                        return(failResult)
                    } 
                }
            } 
        }
    }
    return(passResult)
}


test_expected_years <- function(df, is_estimation_yrs) { 
    ## 
    ##
    yrs_in_data <- unique(df[,year_id])
    if (is_estimation_yrs) { 
        expected_yrs <- get_gbd_parameter('estimation_years')$estimation_years
    }
    else { 
        min_year <- get_gbd_parameter('min_year_epi')$min_year_epi
        max_year <- get_gbd_parameter('max_year')$max_year
        expected_yrs <- seq.int(min_year, max_year)
    }
    if (any(expected_yrs %ni% yrs_in_data)) { 
        missing_yrs <- setdiff(expected_yrs, yrs_in_data)
        print(paste0('data is missing the following years: ', missing_yrs))
        return(FALSE)
    }
    else { 
        return(TRUE)
    }
}


test_expected_sex <- function(df, acause) { 
  ##
  ##
    sex_in_data <- unique(df[,sex_id])
    expected_sex <- c() 

    

    sex_metadata <- fread(paste0(sub('2021', '2022', get_path(process='nonfatal_model', key='database_cache')), '/registry_input_entity.csv'))

    # subset to single acause 
    CAUSE <- acause # rename because we cant have acause==acause below also added release_id bc otherwise it returns 5 rows of the table and errors out below
    this_cause <- sex_metadata[acause == CAUSE & release_id==16]

    if (this_cause[,male] == 1) { 
        expected_sex <- c(expected_sex, c(1))
    }
    if (this_cause[,female] == 1) { 
        expected_sex <- c(expected_sex, c(2))
    }
    if (any(expected_sex %ni% sex_in_data)) { 
        missing_sex <- setdiff(expected_sex, sex_in_data) 
        print(paste0('data is missing the follow sexes: ', missing_sex))
        return(FALSE)
    }
    else { 
        return(TRUE)
    }
}


test_expected_ages <- function(df, acause) {
    ##
    ##
    expected_ages <- nonfatal_model.get_expected_ages(acause)
    ages_in_data <- unique(df[,age_group_id])
    if (any(expected_ages %ni% ages_in_data)) { 
        missing_ages <- setdiff(expected_ages, ages_in_data) 
        print(paste0('data is missing for the following age groups: ', missing_ages))
        return(FALSE)
    }
    else { 
        return(TRUE)
    }

}
#########################################
## Run Functions
#########################################
if (!interactive()) {
    args <- commandArgs(trailingOnly=TRUE)
    acause <- args[1]
    location_id <- as.integer(args[2])
    cnf_model_version_id <- as.integer(args[3])
    faux_correct <- as.logical(as.integer(args[4]))
    is_resubmission <- as.logical(as.integer(args[5]))
    is_estimation_yrs <- as.logical(as.integer(args[6]))
    generate_incidence_draws(acause, location_id, cnf_model_version_id, 
                        faux_correct, is_resubmission, is_estimation_yrs)
} else {
  generate_incidence_draws('neo_eye', 349, 38, 0, 0, 0)
} 
