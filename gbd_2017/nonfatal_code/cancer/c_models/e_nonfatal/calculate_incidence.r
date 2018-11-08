#########################################
## Description: Loads functions to calculate incidence data
## Input(s): gbd cause and location_id
## Output(s): saves formatted draws for the cause and location id in the nonfatal workspace
## How To Use: intended for submission as a cluster job in the nonfatal pipeline (see "Run Functions" below),
##                  can also be sourced to retrieve results for a single cause-location_id
#########################################
library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, '/r_utils/utilities.r'))
source(get_path('nonfatal_functions', process="nonfatal_model"))
source(get_path('load_mir_draws', process="nonfatal_model"))
source(get_path('load_mortality_draws', process="nonfatal_model"))
source(get_path("r_test_utilities"))
library(magrittr)
library(plyr)


#########################################
## Define Functions
#########################################
generate_incidence_draws <- function(this_cause, local_id, cnf_model_run_id, 
                                                        is_resubmission=FALSE) {
    ## Runs function to calculate incidence draws, then finalizes (thus saving) 
    ##      the results
    ##
    output_folder = get_path("incidence_draws_output", process="nonfatal_model")
    output_file = paste0(output_folder, "/", this_cause, "/", local_id, ".csv")
    asr_output = paste0(output_folder, "/", this_cause, "/", local_id, "_asr.csv")
    if (grepl("neo_leukemia_", this_cause)) {
        this_data <- calculate_squeezed_incidence(sub_cause = this_cause, 
                        parent_cause = "neo_leukemia", local_id,
                        cnf_model_run_id, is_resubmission)
    } else {
        this_data <- calculate_incidence(this_cause, local_id, cnf_model_run_id,
                        is_resubmission)
    }
    draw_cols <- colnames(this_data)[grepl("draw",colnames(this_data))]
    inc_cols <- gsub("draw", "inc", draw_cols)
    setnames(this_data, draw_cols, inc_cols)
    nonfatal_model.finalize_draws(this_data, output_file, inc_cols)
    print("Incidence refreshed!")
}


calculate_squeezed_incidence <- function(sub_cause, parent_cause, this_location_id,
                                                cnf_model_run_id, is_resubmission) {
    ## Returns "squeezed" leukemia incidence, incidence that has been adjusted 
    ##     to ensure that the total of the subtypes is equal to the parent 
    ##     estimate
    ##
    print("loading subtype data and aggregating to find total")
    uid_cols <- c('location_id','year_id','sex_id','age_group_id')
    subtypes <- nonfatal_model.get_modeledCauseList(starting_with="neo_leukemia_")
    subtype_data <- do.call(rbind, 
                            lapply(subtypes, calculate_incidence,  
                                    this_location_id = this_location_id,
                                    cnf_model_run_id = cnf_model_run_id,
                                    is_resubmission=is_resubmission)
                            )
    #
    this_cause_data <- subtype_data[subtype_data$acause == sub_cause,]
    draw_cols <- colnames(subtype_data)[grepl("draw", colnames(subtype_data))]
    totals <- aggregate(. ~location_id+year_id+sex_id+age_group_id, 
                            data= subtype_data[,c(uid_cols, draw_cols)], 
                            FUN=sum )
    setnames(totals, old=draw_cols, new=gsub("draw", "total", draw_cols))

    print("getting parent data")
    parent_data <- calculate_incidence(parent_cause, this_location_id, 
                                        cnf_model_run_id,is_resubmission)
    setnames(parent_data, old=draw_cols, new=gsub("draw", "parent", draw_cols))
    print("fitting subtype data to parent")

    proportions <- merge(this_cause_data, totals, by=uid_cols)
    proportions[,sub("draw", "prop", draw_cols)] = (
        proportions[draw_cols]/proportions[gsub("draw", "total", draw_cols)])
    sqzd_data <- merge(proportions, parent_data, by=uid_cols)
    sqzd_data[sub("draw", "final", draw_cols)] <- (
        sqzd_data[gsub("draw", "prop", draw_cols)]*sqzd_data[gsub("draw", "parent", draw_cols)])
    sqzd_data <- sqzd_data[c(uid_cols, sub("draw", "final", draw_cols))]
    setnames(sqzd_data, old=gsub("draw", "final", draw_cols), new=draw_cols)
    return(sqzd_data)
}


calculate_incidence <- function(this_acause, this_location_id, cnf_model_run_id,
                                                        is_resubmission=FALSE) {
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
            mortality_data <- read.csv(mortality_file)
            refresh = FALSE
        }
    }
    if (refresh){
        mir_data <- as.data.frame(
                    format_mir(this_acause, this_location_id, cnf_model_run_id))
        mortality_data <- as.data.frame(
                format_mortality(this_acause, this_location_id, cnf_model_run_id))
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
    incidence_draws$acause <- this_acause
	incidence_draws$location_id <- this_location_id
    test_incidence_draws(incidence_draws, uid_cols)
    return(incidence_draws)
}


#########################################
## Define Tests
#########################################
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
    cnf_model_run_id <- as.integer(args[3])
    if (length(args) == 4) {
        is_resubmission <- as.logical(as.integer(args[4]))
    } else is_resubmission <- FALSE
    #
    generate_incidence_draws(acause, location_id, cnf_model_run_id, is_resubmission)
}

