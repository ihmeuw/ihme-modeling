#!/usr/local/bin/R
#########################################
## Description: Loads functions used by mi_prep script
## Input(s)/Outputs(s): see individual functions
## How To Use: must be sourced by mi_prep script
## IMPORTANT NOTE: mi_prep.set_uidColumns must be called by mi_prep before other functions will work
#########################################
require(data.table)
source(get_path("mi_prep_test_functions", process="cancer_model"))

## #######
## Define Globals, 'Set'  and 'Get' Functions
## #######
mi_prep.set_modelInputNumber <- function(){ # allows uid columns to be defined by the main script
    inputNumber <- as.numeric(readline(prompt="Enter a model input number from input config: "))
    mi_prep.test_modelInputNumber(inputNumber)
    mi_prep.modelInputNumber <<- inputNumber
}

mi_prep.get_modelInputNumber <- function(){
    if (!(exists("mi_prep.modelInputNumber"))) mi_prep.set_modelInputNumber()
    return(mi_prep.set_modelInputNumber)
}

mi_prep.set_userInfo <- function(){
    uInfo <- readline(prompt="Enter uwNetID: ")
    mi_prep.test_userInfo(uInfo)
    mi_prep.userInfo <<- uInfo
}

mi_prep.get_userInfo <- function(){
    if (!exists("mi_prep.userInfo")) mi_prep.set_userInfo()
    return(mi_prep.userInfo)
}


mi_prep.set_uidColumns <- function(columnList){ # allows uid columns to be defined by the main script
    mi_prep.uid_columns <<- columnList
}

mi_prep.get_uidColumns <- function(){
    if (!exists("mi_prep.uid_columns")){
        stop("Error. UID columns not correctly set. Please set uid columns with 'mi_prep.set_uidColumns' before using the mi_prep functions")
    } else {return(mi_prep.uid_columns)}
}

mi_prep.load_inputConfig <- function() {
    config_location <-get_path("mi_input_config", process="cancer_model")
    inputConfig <- read.csv(config_location, stringsAsFactors = FALSE)
    return(inputConfig)
}

mi_prep.set_inputConfig <- function(){
## Load the config file, then keep only the most recent entry per cause ##
    input_config <- mi_prep.load_inputConfig()
    input_config <- input_config[input_config$model_input == model_input_number & !is.na(input_config$model_input),]
    mi_prep.test_inputConfig(input_config)
    mi_prep.inputConfigDF <<- input_config
}

mi_prep.get_inputConfig <- function(){
   if (!exists("mi_prep.inputConfigDF")) mi_prep.set_inputConfig()
   return(mi_prep.inputConfigDF)
}

mi_prep.set_causeConfig <- function(){
## Load the config file, then keep only the most recent entry per cause ##
## NOTE: config_location must point to a csv ##
    config_location <- get_path("mi_cause_config", process="cancer_model")
    cause_config <- read.csv(config_location, stringsAsFactors=FALSE)
    cause_config <- cause_config[cause_config$model_input == model_input_number & !is.na(cause_config$model_input),]
    mi_prep.test_causeConfig(cause_config)
    mi_prep.causeConfigurationDF <<- cause_config
}

mi_prep.get_causeConfig <- function(){
    if (!exists("mi_prep.causeConfigurationDF")) mi_prep.set_causeConfig()
    return(mi_prep.causeConfigurationDF)
}

## #######
## Pipeline Functions
## #######
mi_prep.printDataAvailability <- function(df){
## Display breakdown of data availability by super region ##
    print('Data Availability by Super Region:')
    print(unique(df[, c('super_region_name','super_region_id')]))
}

mi_prep.refreshLocationInfo <- function() {
## Combine relevant information from modeled_locations with sdi values and quintiles ##
  print('Refreshing location_info...')
  ## set paths
    location_metadata_file <- get_path("mi_location_info", process="cancer_model")
    wealth_covariates <- get_path("sdi_values")
    sdi_quintiles <- get_path('sdi_quintiles')

  ## Get modeled locations
    modeled_locations <- read.csv(get_path("modeled_locations"))
    location_data <- modeled_locations[!(modeled_locations$location_type %in% c('global', 'superregion', 'region')),]
    location_data <- location_data[, c('location_id', 'country_id', 'super_region_id', 'super_region_name', 'region_id')]

  ## Attach sdi to the modeled locations
    sdi <- read.csv(wealth_covariates)
    sdi <- sdi[, c('location_id', 'year', 'sdi', 'sdi_quintile')]
    location_info <- merge(location_data, sdi, by = c('location_id'))
    location_info <- location_info[!duplicated(location_info),]

  ## Save
    mi_prep.test_refreshLocationInfo(location_info)
    write.csv(location_info, file =  location_metadata_file, row.names=FALSE)
}

mi_prep.generateMinimumCasesMaps <- function(){
##
    cause_config <- mi_prep.get_causeConfig()
    # subset cause_config to create a map of the mininum cases threshold. keep only the most recent entry for each acause
    # if no cause-specific minimum requirement is present, replace the missing requirement witht he general minimum_cases_requirement
    minimum_cases_map <- cause_config[, c('acause', 'cause_specific_minimum_cases')]
    names(minimum_cases_map)[names(minimum_cases_map) %in% c('cause_specific_minimum_cases')] <- 'minimum_cases_allowed'
    inputData_minCases <- minimum_cases_map

    inputData_minCases[is.na(inputData_minCases$minimum_cases_allowed), 'minimum_cases_allowed'] <- input_config$minimumCases_data

    lowerCap_minCases <- minimum_cases_map
    lowerCap_minCases[is.na(lowerCap_minCases$minimum_cases_allowed), 'minimum_cases_allowed'] <- input_config$minimumCases_lowerCaps

    upperCap_minCases <- minimum_cases_map
    upperCap_minCases['minimum_cases_allowed'] <- input_config$minimumCases_upperCaps  # upper caps use same minimum for all causes

    minimumCasesMaps <- list(
        "data" = inputData_minCases,
        "lowerCap" = lowerCap_minCases,
        "upperCap" = upperCap_minCases)

    return(minimumCasesMaps)
}

mi_prep.applyRestrictions <- function(input_df, min_cases_map) {
## Apply model input restrictions ##
## Note: min_cases_map is a dataframe mapping each cause to its minimum cases requirement
    print("Applying restrictions...")

    ## get information from the config
    input_config <- mi_prep.get_inputConfig()
    max_mi_accepted = input_config$max_mi_input_accepted
    drop_countries_input = strsplit(as.character(input_config$drop_countries), ",")[[1]]
    drop_countries = as.list(as.numeric(drop_countries_input))

     ## Calculate the mi ratio
    input_df$mi_ratio <- input_df$deaths/input_df$cases

    ## Drop MI ratios == 0
    input_df <- input_df[!(input_df$mi_ratio == 0),]

     ## Drop data flagged as "drop_country"
    input_df <- subset(input_df, !(input_df$country_id %in% drop_countries), )

     ## Keep only data with cases/deaths greater than an optional minimum threshold
    input_df <- merge(input_df, min_cases_map, all.x = TRUE)
    input_df <- input_df[!(input_df$cases < input_df$minimum_cases_allowed), ]
    input_df <- input_df[!(input_df$deaths < 1),]

    ## Drop MI ratios above the maximum accepted mi ratio
    input_df <- input_df[input_df$mi_ratio <= max_mi_accepted,]

     ## Drop data based on SDI quantile comparisons ##
    ## Determine median of high SDI by age, sex, cancer. Then collapse on age, sex, cancer
    high_sdi_data <- input_df[input_df$sdi_quintile == 5,]
    high_sdi_median <- aggregate(mi_ratio~age_group_id+sex+acause, data=high_sdi_data, FUN = median)
    names(high_sdi_median)[names(high_sdi_median)=='mi_ratio'] <- 'high_sdi_median'

    ## Merge high_sdi_median onto full dataset for comparison
    cancer_data <- as.data.table(merge(input_df, high_sdi_median, by = c('age_group_id', 'sex', 'acause')))

    ## Drop all data points for quintile 1-4 that are < high_sdi_median
    cancer_data <- cancer_data[sdi_quintile <= 4 & mi_ratio < high_sdi_median, drop := 1]
    cancer_data <- cancer_data[is.na(drop), drop := 0]
    cancer_data <- cancer_data[drop!=1, ]

    ## Drop all data for quintile 1-4 that are > quartile 3 by SDI quintile, age, sex, cancer, then collapse to max value by sdi_quintile, age, sex, cancer
      # NOTE: quartile_three variable here is actually the upper whisker of a box-and-whisker plot
    Q1 <- quantile(input_df$mi_ratio, probs = .25)
    Q3 <- quantile(input_df$mi_ratio, probs = .75)
    quartile_three <- cancer_data[, list(quartile_three = ((Q3-Q1)*1.5 + Q3)), by = c('age_group_id', 'sex', 'acause', 'sdi_quintile')]

    ## Merge onto full dataset
    cancer_data <- merge(cancer_data, quartile_three, by = c('age_group_id', 'sex', 'acause', 'sdi_quintile'))

    ## Drop all data points for quintile 1-4 that are > quartile 3
    cancer_data <- cancer_data[sdi_quintile <= 4 & mi_ratio > quartile_three, drop := 1]
    cancer_data <- cancer_data[is.na(drop), drop := 0]
    cancer_data <- cancer_data[drop!=1, ]

    ## revert to data.frame
    output_df <- as.data.frame(cancer_data)
    output_df <- output_df[, !(names(output_df) %in% c('drop', 'high_sdi_median', 'quartile_three'))]

    ## return restricted data
    mi_prep.test_applyRestrictions(output_df)
    return(output_df)
}

mi_prep.aggregatePerCauseConfig <- function(inputDf, aggregate_map, variable_to_reassign){
    if (!(variable_to_reassign %in% c("year", "age_group_id"))) {
        stop(print("cannot aggregate by", variable_to_reassign))
    } else { print(paste0('Aggregating by ', variable_to_reassign, '...')) }

    # create lists of data to be aggregated or kept
    cols_to_aggregate <- c('cases', 'deaths')
    if ('pop' %in% colnames(inputDf)) { cols_to_aggregate = c(cols_to_aggregate, 'pop')}
    uid_cols <- colnames(inputDf)[!(colnames(inputDf) %in% c(cols_to_aggregate, 'mi_ratio'))] # drop mi_ratio from the dataset, since it will be recalculated

    # subset data by whether or not it should be aggregated
    df <- inputDf
    to_ag <- merge(df, aggregate_map, by = 'acause', all=FALSE)
    no_ag <- subset(df, !(df$acause %in% unique(aggregate_map$acause)), c(uid_cols, cols_to_aggregate))

    # Replace values for the variable_to_reassign
    if (variable_to_reassign == "year") {
        to_ag$origYear <- to_ag$year
        to_ag$year <- to_ag$origYear - to_ag$origYear%%to_ag$bins  # subtract modulus of bin to set new estimation year
    } else if (variable_to_reassign == "age_group_id"){
        # adjust age groups to enable aggregation
        tooYoung = to_ag$age_group_id <= to_ag$bins
        to_ag[tooYoung, 'age_group_id'] <- to_ag[tooYoung, 'bins']
    } else {stop(paste("Error: variable_to_reassign must be 'year' or 'age_group_id'. You sent", variable_to_reassign))}

    # aggregate data
    aggregated <- aggregate(cbind(to_ag$cases, to_ag$deaths), by = to_ag[,uid_cols], FUN = sum)
    names(aggregated)[names(aggregated) %in% c('V1', 'V2')] <- cols_to_aggregate
    outputDf <- rbind(no_ag, aggregated)

    ## Calculate the mi ratio, dropping undefined values
    outputDf$mi_ratio <- outputDf$deaths/outputDf$cases
    outputDf <- outputDf[which(!is.na(outputDf$mi_ratio) & !is.nan(outputDf$mi_ratio) & !(is.infinite(outputDf$mi_ratio))), ]

    mi_prep.test_aggregatePerCauseConfig(outputDf, inputDf, variable_to_reassign, columnsAggregated=cols_to_aggregate)
    return(outputDf)
}

mi_prep.generateCaps <- function(type, storage_file, input_data, pctile, model_input) {
  ## Define function to generate upper caps
  print(paste("Calculating", type,  "caps..."))

    if (type == "upper") {
      caps <- aggregate(mi_ratio~age_group_id, data=input_data, FUN = quantile, probs=pctile)
    } else if (type == "lower") {
      caps <- aggregate(mi_ratio~age_group_id+acause, data=input_data, FUN=quantile, probs=pctile)
    }
    names(caps)[names(caps)%in%'mi_ratio'] <- paste0(type, "_cap")

  ## save copy of upper_caps for later reference
    caps$model_input <- model_input_number
    if ( file.exists(storage_file) ) {
        existing_map <- read.csv(storage_file)
        existing_map <- existing_map[as.integer(existing_map$model_input) != model_input_number, ]
        export_map <- rbind(existing_map, caps)
    } else {
        existing_map <- caps
        export_map <- caps
    }
    required_columns = c("model_input", "age_group_id")
    if (type == "lower") required_columns = c("model_input", "age_group_id", "acause")
    mi_prep.test_generateCaps(fullMapOfCaps = export_map, capType=type, required_columns, existing_map)
    write.csv(export_map, storage_file, row.names=FALSE)
    return(caps)
 }

 mi_prep.addVariance <-function(df, run_logit_models, offset){
    if (run_logit_models & is.na(offset)) stop("ERROR: NA sent as offset to addVariance")

    print('Adding variance...')
    ## calculate variance
    if (run_logit_models == TRUE) {
        df$variance <- (df$mi_ratio/df$upper_cap) * (1 - df$mi_ratio/df$upper_cap) / df$cases
        df[df$variance <= offset, 'variance'] <- offset
        df[df$variance >= 1, 'variance'] <- (1 - offset)
    } else {
        SD <- sd(df$mi_ratio)
        avg <- mean(df$mi_ratio)
        df$variance <-   (((exp(1)^(SD^2))-1)*exp(1)^(2*avg + SD^2)) / df$cases
    }

    ## ensure that there is no null variance
    if (nrow(df[is.na(df$variance),])) {
        stop("ERROR! Error in variance calculation. Some variance entries are null.")
    }
    if ((nrow(df[df$variance <= 0,]) > 0) | (nrow(df[df$variance >= 1,]) > 0)) {
        stop("ERROR! Error in variance calculation. Some variance entries are outside of the acceptable range.")
    }

    ## Convert to Probability if Running Logit. Offset Estimates where necessary
    if (run_logit_models) {
        ## convert data to probability space using the upper cap
        df$data <- df$mi_ratio / df$upper_cap

        ## Add offset
        df$data[df$data >= 1] <- 1-offset
        df$data[df$data <= offset] <- offset
    } else df$data <- df$mi_ratio

    mi_prep.test_addVariance(df)
    return(df)
 }

 mi_prep.addSTGPR_requirements <- function(df){
    print("Adding STGPR requirements...")
    mi_prep.set_uidColumns(columnList=c('nid', 'location_id', 'year_id', 'sex_id', 'acause', 'age_group_id'))
    setnames(df, old = c('year','NID', 'cases', 'sex'), new = c('year_id','nid', 'incident_cases', 'sex_id' ) )
    df$sample_size <- df$incident_cases
    me_map <- cancer_model.get_me_map()
    with_me_name <- merge(df, me_map, by='acause', all.x=TRUE, all.y=FALSE)
    output <- with_me_name
    return(output)
 }

## #######
## END
## #######