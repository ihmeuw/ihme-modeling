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
library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) {
        code_repo <- file.path(code_repo, 'cancer_estimation')
    }
}
source(file.path(code_repo, '/r_utils/utilities.r'))
source(get_path('nonfatal_functions', process="nonfatal_model"))
source(get_path('load_mir_draws', process="nonfatal_model"))
source(get_path('load_mortality_draws', process="nonfatal_model"))
source(get_path("r_test_utilities"))
library(magrittr)
library(feather)
library(plyr)


################################################################################
## Define Functions
################################################################################
generate_incidence_draws <- function(this_cause, local_id, cnf_model_version_id,
                                                         faux_correct,
                                                         is_resubmission=FALSE) {
    ## Runs function to calculate incidence draws, then finalizes (thus saving) 
    ##      the results
    ##
    output_folder = get_path("incidence_draws_output", process="nonfatal_model")
    output_file = paste0(output_folder, "/", this_cause, "/", local_id, ".csv")
    asr_output = paste0(output_folder, "/", this_cause, "/", local_id, "_asr.csv")
    this_data <- calculate_incidence(this_cause, local_id, cnf_model_version_id,
                is_resubmission, faux_correct)
    draw_cols <- colnames(this_data)[grepl("draw",colnames(this_data))]
    inc_cols <- gsub("draw", "inc", draw_cols)
    setnames(this_data, draw_cols, inc_cols)
    nonfatal_model.finalize_draws(this_data, output_file, inc_cols)
    print("Incidence refreshed!")
}


calculate_incidence <- function(this_acause, this_location_id, cnf_model_version_id,
                                        is_resubmission=FALSE, faux_correct) {
    ## Returns a dataframe of incidence draws by first loading results of 
    ##        format_mortality and format_mi, then uses them to calculate 
    ##        incidence. If formatted mortality or formatted mi_ratio draws are 
    ##        missing, will run function to generate the missing data.
    ##
    uid_cols <- c('year_id','sex_id','age_group_id')
    #
    refresh = TRUE
    if (is_resubmission) {
        print("resubmission mode")
        mir_dir = get_path("mir_draws_output", process="nonfatal_model")
        mir_file = paste0(mir_dir, "/", this_acause, "/", this_location_id, ".csv")
        mort_dir = get_path("mortality_draws_output", process="nonfatal_model")
        mortality_file = paste0(mort_dir, "/", this_acause, "/", this_location_id, ".csv")
        if (file.exists(mir_file) & file.exists(mortality_file)) {
            mir_data <- read.csv(mir_file)
            if (faux_correct == TRUE) { 
                mir_data <- subset(mir_data, (mir_data$year_id == 1990 | mir_data$year_id == 2000 | mir_data$year_id == 2017))
            }
            mortality_data <- read.csv(mortality_file)
            refresh = FALSE
        }
    }
    if (refresh){
        mir_dir = get_path("mir_draws_output", process="nonfatal_model")
        mir_file = paste0(mir_dir, "/", this_acause, "/", this_location_id, ".csv")
        mir_data <-  mir_data <- read.csv(mir_file)
        if (this_acause %in% c('neo_bone', 'neo_liver_hbl','neo_lymphoma_burkitt','neo_lymphoma_other','neo_eye','neo_eye_other','neo_eye_rb', 'neo_neuro','neo_tissue_sarcoma')) { 
            names(mir_data)[names(mir_data) == 'year'] <- 'year_id' 
            names(mir_data)[names(mir_data) == 'sex'] <- 'sex_id'
        }
        if (faux_correct == TRUE) { 
            mir_data <- subset(mir_data, (mir_data$year_id == 1990 | mir_data$year_id == 2000 | mir_data$year_id == 2017))
        }
        mortality_data <- as.data.frame(
            format_mortality(this_acause, this_location_id, cnf_model_version_id))
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
    incidence_data <- merged_data[,mor_cols] / merged_data[,mir_cols]
    colnames(incidence_data) <- draw_cols
    #
    incidence_draws <- cbind(merged_data[uid_cols] , incidence_data)
	incidence_draws$location_id <- this_location_id
    incidence_draws$acause <- this_acause
    test_incidence_draws(incidence_draws, uid_cols)
    return(incidence_draws)
}


################################################################################
## Define Tests
################################################################################
test_incidence_draws <- function(df, uid_columns){
    ##
    ##
    results = list('missing columns'=c(), 'duplicates check'=c())
    results['duplicates check'] = test_utils.duplicateChecker(df, uid_columns)
    results['missing columns'] = test_utils.findMissingColumns(df, uid_columns)
    results['columns missing values'] = test_utils.findMissingValues(df, uid_columns)
    if (any(df['location_id'] < 1)) results['incorrect values'] = "see location_id column"
    test_utils.checkTestResults(results, "calculate_incidence")
}


#########################################
## Run Functions
#########################################
if (!interactive()) {
    args <- commandArgs(trailingOnly=TRUE)
    acause <- args[1]
    location_id <- as.integer(args[2])
    cnf_model_version_id <- as.integer(args[3])
    faux_correct <- args[4]
    faux_correct <- FALSE
    if (length(args) == 5) {
        is_resubmission <- as.logical(as.integer(args[5]))
    } else { 
        is_resubmission <- FALSE
    }
    generate_incidence_draws(acause, location_id, cnf_model_version_id, faux_correct)
}

