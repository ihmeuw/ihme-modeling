#!/usr/local/bin/R
#########################################
## Description: Loads functions to calculate incidence data
## Input(s): gbd cause and location_id
## Output(s): saves formatted draws for the cause and location id in the nonfatal workspace
## How To Use: intended for submission as a cluster job in the nonfatal pipeline (see "Run Functions" below),
##                  can also be sourced to retrieve results for a single cause-location_id
## Notes: See additional notes in the format_mi_draws script
#########################################
## load Libraries
source(file.path(h, 'cancer_estimation/utilities.R'))  # enables generation of common filepaths
source(get_path('nonfatal_functions', process="nonfatal_paths"))
source(get_path('generate_incidence_tests', process="nonfatal_paths"))
source(get_path('load_mi_draws', process="nonfatal_paths"))
source(get_path('load_mortality_draws', process="nonfatal_paths"))
library(magrittr)
library(plyr)

#########################################
## Define Functions
#########################################
calc_inc.generate_incidence_draws <- function(this_cause, local_id) {
## Runs function to calculate incidence draws, then finalizes (thus saving) the results
    output_file = paste0(get_path("incidence_draws_output", process="nonfatal_paths"), "/", this_cause, "/", local_id, ".dta")
    if (grepl("neo_leukemia_", this_cause)) {
        this_data <- calc_inc.calculate_squeezed_incidence(sub_cause = this_cause, parent_cause = "neo_leukemia", local_id)
    } else {
        this_data <- calc_inc.calculate_incidence(this_cause, local_id)
    }
    nonfatal_model.finalize_draws(this_data, output_file = output_file)
}

calc_inc.calculate_incidence <- function(incidence_cause, this_location_id) {
## Returns a dataframe of incidence draws by first loading results of format_mortality and
##        format_mi, then uses them to calculate incidence.
##        if formatted mortality or formatted mi_ratio draws are missing, will run function to
##        generate the missing data.
    uid_cols <- c('location_id','year','sex','age_group_id')

    mortality_file = paste0(get_path("mortality_draws_output", process="nonfatal_paths"), "/", incidence_cause, "/", this_location_id, ".dta")
    if (!file.exists(mortality_file)) format_mortality(incidence_cause, this_location_id)
    mortality_data <- read.dta(mortality_file)
    draw_cols <- colnames(mortality_data)[grepl("draw",colnames(mortality_data))]
    mor_cols <- gsub("draw", "mor", draw_cols)

    mi_file = paste0(get_path("mi_draws_output", process="nonfatal_paths"), "/", incidence_cause, "/", this_location_id, ".dta")
    if (!file.exists(mi_file)) format_mi_draws(incidence_cause, this_location_id)
    mi_data <- read.dta(mi_file)
    mi_cols <- gsub("draw", "mi", draw_cols)

    if ("draw_1" %in% colnames(mi_data)) setnames(mi_data, old = draw_cols, new = mi_cols)
    if ("year_id" %in% colnames(mortality_data)) setnames(mortality_data, old = c("year_id", "sex_id"), new=c("year", "sex") )
    setnames(mortality_data, old = draw_cols, new = mor_cols)

    merged_data <- merge(mortality_data[c(uid_cols,mor_cols)], mi_data[c(uid_cols,mi_cols)], by=uid_cols )
    merged_data <- subset(merged_data, !(merged_data$age_group_id %in% c(22, 27)), )
    incidence_data <- merged_data[,mor_cols] / merged_data[,mi_cols]
    colnames(incidence_data) <- draw_cols

    incidence_draws <- cbind(merged_data[uid_cols] , incidence_data)
    incidence_draws$acause <- incidence_cause
    calc_inc.test_incidence_draws(incidence_draws, uid_cols)
    return(incidence_draws)
}

calc_inc.calculate_squeezed_incidence <- function(sub_cause, parent_cause, this_location) {
## Returns "squeezed" leukemia incidence, incidence that has been adjusted to ensure that the total
##     of the subtypes is equal to the parent estimate
          print("loading subtype data and aggregating to find total")
          subtypes <- nonfatal_model.get_causes(starting_with="neo_leukemia_")
          subtype_data <- do.call(rbind, lapply(subtypes, calc_inc.calculate_incidence, local_id = this_location))
          this_cause_data <- subtype_data[subtype_data$acause == this_cause,]

          draw_cols <- colnames(subtype_data)[grepl("draw",colnames(subtype_data))]
          totals <- aggregate(. ~location_id+year+sex+age_group_id, data= subtype_data[,c(uid_cols, draw_cols)], FUN=sum )
          setnames(totals, old=draw_cols, new=gsub("draw", "total", draw_cols))

          print("getting parent data")
          parent_data <- calc_inc.calculate_incidence("neo_leukemia", this_location)
          setnames(parent_data, old=draw_cols, new=gsub("draw", "parent", draw_cols))

          print("fitting subtype data to parent")

          proportions <- merge(this_cause_data, totals, by=uid_cols)
          proportions[,sub("draw", "prop", draw_cols)] = proportions[draw_cols]/proportions[gsub("draw", "total", draw_cols)]
          squeezed_data <- merge(proportions, parent_data, by=uid_cols)
          squeezed_data[sub("draw", "final", draw_cols)] <- squeezed_data[gsub("draw", "prop", draw_cols)]*squeezed_data[gsub("draw", "parent", draw_cols)]
          squeezed_data <- squeezed_data[c(uid_cols, sub("draw", "final", draw_cols))]
          setnames(squeezed_data, old=gsub("draw", "final", draw_cols), new=draw_cols)
          return(squeezed_data)
}

#########################################
## Run Functions
#########################################
if (!interactive()) {
    acause <- commandArgs()[3]
    location_of_interest <- commandArgs()[4]
    calc_inc.generate_incidence_draws(acause, location_of_interest)
}

