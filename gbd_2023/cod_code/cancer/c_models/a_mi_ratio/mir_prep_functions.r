#!/usr/local/bin/R
########################################################################

## Input(s)/Outputs(s): see individual functions

## Contributors: INDIVIDUAL_NAME
## Last Updated: REDACTED
########################################################################
library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
require(data.table)
library(plyr)
library(dplyr)
source(get_path("r_test_utilities", process='common'))
source(get_path('cdb_utils_r'))

################################################################################
## Define 'Set'  and 'Get' Functions
################################################################################
mir_prep.get_uidColumns <- function(){
    ## Loads the current list of UID columns. If new_setting is passed, sets 
    ##      the UID columns to the new_setting before returning
    ##
    uid_columns <- c('NID', 'location_id', 'year_id', 'sex_id', 
                                        'acause', 'age_group_id', 'age_type')
    return(uid_columns)
}


mir_prep.get_causeConfig <- function(){
    ## Loads and returns the cause_config for the current model input
    ##
    if (!exists("mir_prep.__causeConfigurationDF")) {
        model_input_id <- mir.get_mir_version_id()
        this_config <- as.numeric(subset(mir.get_inputConfig(), 
                            mir_model_version_id == model_input_id,
                            'mir_cause_config_id'))
        cause_config <- cdb.get_table("mir_cause_config")
        this_config_loc = (cause_config$mir_cause_config_id == this_config & 
                            !is.na(cause_config$mir_cause_config_id) )
        cause_config <- cause_config[this_config_loc,
                c('mir_cause_config_id', 'acause','year_agg','age_agg', 'min_cases','VR_data', 'use_cause_specific_upper_cap')]
        cause_config[is.na(cause_config$age_agg), 'age_agg'] <- 1
        mir_prep.__causeConfigurationDF <<- cause_config
    }
    return(mir_prep.__causeConfigurationDF)
}


mir_prep.get_minCasesMap <- function(which_type){
    ## Loads and returns a map from the cause_config and input_config which 
    
    ##
    if (!(which_type %in% c("data", "lower_cap", "upper_cap"))) {
        Sys.stop("wrong type sent to mir_prep.get_minCasesMap")
    }
    if (!exists("mir_prep.__minCasesMap")) {
        input_config <- mir.get_inputConfig() 
        cause_config <- mir_prep.get_causeConfig()
        
        minimum_cases_map <- cause_config[, c('acause', 'min_cases')]
        min_cases_name_all = names(minimum_cases_map) %in% c('min_cases')
        names(minimum_cases_map)[min_cases_name_all] <- 'min_cases'
        input_min <- minimum_cases_map
        # for each map, if no cause-specific minimum requirement is set, replace 
        #       the missing requirement with the general minimum_cases_requirement
        set_data = (input_min$min_cases == 0 | is.na(input_min$min_cases))
        input_min[set_data, 'min_cases'] <- input_config$min_cases_data
        lower_cap_min <- minimum_cases_map
        set_lower = (lower_cap_min$min_cases == 0 | is.na(lower_cap_min$min_cases))
        lower_cap_min[set_lower, 'min_cases'] <- input_config$min_cases_lower_caps
        # For upper caps use same minimum-cases requirement
        upper_cap_min <- minimum_cases_map
        set_upper = (upper_cap_min$min_cases == 0 | is.na(upper_cap_min$min_cases))
        upper_cap_min[set_upper, 'min_cases'] <-input_config$min_cases_upper_caps  
        mir_prep.__minCasesMap <<- list(
                                    "data" = input_min,
                                    "lower_cap" = lower_cap_min,
                                    "upper_cap" = upper_cap_min)
    }
    return(as.data.frame(mir_prep.__minCasesMap[[which_type]]))
}


mir_prep.load_aggregationConfig <- function(aggregate_type){
    ## Loads and returns an aggregate map from the cause_config which dictates 
    
    ##
    cause_config <- mir_prep.get_causeConfig()
    if (!(aggregate_type %in% c('year_id', 'age_group_id'))) {
        stop(print("cannot aggregate by", var_to_aggregate))
    } else if (aggregate_type == 'year_id') {
        aggregate_map <- subset(cause_config, 
                                cause_config$year_agg > 1, 
                                c('acause', 'year_agg'))
        setnames(aggregate_map, old='year_agg', new='agg')
    } else if (aggregate_type == "age_group_id") {
        aggregate_map  <- cause_config[, c('acause', 'age_agg')]
        setnames(aggregate_map, old='age_agg', new='agg')
    }
    return(aggregate_map)
}


mir_prep.get_VR_causes <- function(){
    ## Loads and returns a list of causes that use VR matched data
    cause_config <- mir_prep.get_causeConfig()
    VR_causes <- subset(cause_config, VR_data == 1)
    return(VR_causes)
}


########################################################################
## Pipeline Functions
########################################################################
mir_prep.aggData <- function(input_df, var_to_aggregate, mor_type){
    ## Loads and returns an aggregation config, then aggregates data per the  
    ##      agg assigned in that config
    ##
    uid_cols <- mir_prep.get_uidColumns()
    #  load cause_config to create a map of data that should be aggregated to bins
    aggregate_map <- mir_prep.load_aggregationConfig(var_to_aggregate)
    print(paste0('Aggregating by ', var_to_aggregate, '...'))

    # create lists of data to be aggregated or kept
    if (mor_type == "CR") {
        cols_to_aggregate <- c('cases', 'deaths', 'seq')
    } else {
        cols_to_aggregate <- c('cases', 'deaths')
    }
    if ('pop' %in% colnames(input_df)) {cols_to_aggregate = c(cols_to_aggregate, 'pop')}
    
    to_ag <- merge(input_df, aggregate_map, by = 'acause', all=FALSE)
    no_ag <- subset(input_df, 
                    !(input_df$acause %in% unique(aggregate_map$acause)), 
                    c(uid_cols, cols_to_aggregate))
    # Replace values for the var_to_aggregate
    if (var_to_aggregate == 'year_id') {
        to_ag$origYear <- to_ag$year_id
        
        to_ag$year_id <- to_ag$origYear - to_ag$origYear%%to_ag$agg  
    } else if (var_to_aggregate == "age_group_id"){
        # adjust age groups to enable aggregation
        tooYoung = to_ag$age_group_id <= to_ag$agg
        to_ag[tooYoung, 'age_group_id'] <- to_ag[tooYoung, 'agg']
    } else {
        stop(paste(
            "Error: var_to_aggregate must be 'year_id' or 'age_group_id'. You sent", 
            var_to_aggregate))
    }
    if (mor_type == "CR") {
        
        
        # to aggregate, take the minimum seq and replace all other seq values
        # with zero, then aggregate the seq column as well as deaths and cases
        to_ag <- to_ag %>% group_by(NID, location_id, year_id, sex_id, acause, age_group_id, age_type) %>% mutate(min_seq = min(seq))
        to_ag$seq[which(to_ag$seq!=to_ag$min_seq)]<- 0
        # aggregate data
        aggregated <- aggregate(cbind(to_ag$cases, to_ag$deaths, to_ag$seq), 
                                by = to_ag[,uid_cols], 
                                FUN = sum)
        setnames(aggregated, old = c('V1', 'V2', 'V3'), new=cols_to_aggregate)
        
        #    replace their NIDs with the generic NID and re-aggregate
        test_cols <- uid_cols[uid_cols %ni% "NID"]
        if ( nrow(aggregated[duplicated(aggregated[,test_cols]),])  > 0) {
            generic_nid = get_gbd_parameter("generic_cancer_nid")
            aggregated[duplicated(aggregated[,test_cols]),'NID'] <- generic_nid
            aggregated <- aggregate(cbind(to_ag$cases, to_ag$deaths, to_ag$seq), 
                                    by = to_ag[,uid_cols], 
                                    FUN = sum)
            setnames(aggregated, old = c('V1', 'V2','V3'), new=cols_to_aggregate)
        }
    } else {
        # aggregate data
        aggregated <- aggregate(cbind(to_ag$cases, to_ag$deaths), 
                                by = to_ag[,uid_cols], 
                                FUN = sum)
        setnames(aggregated, old = c('V1', 'V2'), new=cols_to_aggregate)
        
        #    replace their NIDs with the generic NID and re-aggregate
        test_cols <- uid_cols[uid_cols %ni% "NID"]
        if ( nrow(aggregated[duplicated(aggregated[,test_cols]),])  > 0) {
            generic_nid = get_gbd_parameter("generic_cancer_nid")
            aggregated[duplicated(aggregated[,test_cols]),'NID'] <- generic_nid
            aggregated <- aggregate(cbind(to_ag$cases, to_ag$deaths), 
                                    by = to_ag[,uid_cols], 
                                    FUN = sum)
            setnames(aggregated, old = c('V1', 'V2'), new=cols_to_aggregate)
        }
    }
    # Re-attach aggregated data to the data that was left aside
    outputDf <- rbind(no_ag, aggregated)
    ## Calculate the mi ratio, dropping undefined values
    outputDf$mi_ratio <- outputDf$deaths/outputDf$cases
    notUndefined = which(!is.na(outputDf$mi_ratio) & 
                        !is.nan(outputDf$mi_ratio) & 
                        !is.infinite(outputDf$mi_ratio))
    outputDf <- outputDf[notUndefined, ]
    mir_prep.test_aggData(outputDf, 
                            input_df, 
                            var_to_aggregate, 
                            columnsAggregated=cols_to_aggregate)
    return(outputDf)
}


mir_prep.applyRestrictions <- function(mi_df, restriction_type, mor_type, age_type) {
    
    ##
    print("Applying input restrictions...")
    restricted_data <- mir_prep.restrictByConfigSettings(mi_df, restriction_type, mor_type, age_type)
    restricted_data <- mir_prep.restrictByHAQ(restricted_data)
    return(restricted_data)
}


mir_prep.restrictByConfigSettings <- function(mi_df, config_type, mor_type, age_type) {
    
    ##      and the cause_config
    ## Note: min_cases_map is a dataframe mapping each cause to its minimum-cases 
    ##      requirement
    ##
    print("    applying config restrictions...")
    input_cols = unique(colnames(mi_df), 'mi_ratio')
    
    mi_df$mi_ratio <- mi_df$deaths/mi_df$cases
    input_config <- mir.get_inputConfig()
    max_mi_accepted = as.integer(input_config$max_mi_input_accepted)
    VR_causes <- mir_prep.get_VR_causes()
    if (mor_type == "CRVR" | mor_type == "VR") {
        mi_df_VR_causes <- mi_df[mi_df$acause %in% VR_causes$acause,]
        acceptable_mi_VR_causes = (mi_df_VR_causes$mi_ratio <= max_mi_accepted &
                                    mi_df_VR_causes$cases > 0)
        mi_df <- mi_df_VR_causes[acceptable_mi_VR_causes,]
    } else {
        acceptable_mi_old_causes = (mi_df$mi_ratio <= max_mi_accepted &
                                    mi_df$mi_ratio > 0 &
                                    mi_df$deaths > 1 &
                                    mi_df$cases > 0)
        mi_df <- mi_df[acceptable_mi_old_causes,]
    }
    
    #      If doing so would drop all data for a cause-age pair, reset the minimum
    min_cases_map <- mir_prep.get_minCasesMap(config_type)
    # Set minimum cases to zero for new causes
    #REDACTED
    #REDACTED
    min_cases_map$min_cases[min_cases_map$acause %in% VR_causes$acause] <- 0.01
    
    
    
    
    if (age_type == "pediatric"){
    min_cases_map$min_cases <- 0
    }
     
    mi_df <- merge(mi_df, min_cases_map, by="acause", all.x=TRUE)
    meets_minCasesReq = (mi_df$cases >= mi_df$min_cases)
    mi_df <- mi_df[meets_minCasesReq, input_cols]
    ## return restricted data
    mir_prep.test_applyRestrictions(mi_df)
    return(mi_df)
}

mir_prep.restrictByHAQ <- function(mi_df){
    ## Drops data based on haq quantile comparisons.
    
    
    
    ##       box-and-whisker plot
    ##
    # Attach current best version of HAQ values to the data
    print("    applying HAQ restrictions...")
    ## Load HAQ values
    input_config <- mir.get_inputConfig()
    release_id <- input_config$release_id
    haq <- mir.get_HAQValues(release_id)
    mi_df <- merge(mi_df, 
                   haq, 
                   by=c('location_id', 'year_id'), 
                   all.x=TRUE)
    
    high_haq_data <- mi_df[mi_df$haq_quintile == 5,]
    high_haq_median <- aggregate(mi_ratio~age_group_id+sex_id+acause, 
                                 data=high_haq_data, 
                                 FUN = median)
    names(high_haq_median)[names(high_haq_median)=='mi_ratio'] <- 'high_haq_median'
    ## Merge high_haq_median onto full dataset for comparison
    cancer_data <- as.data.table(merge(mi_df, 
                                       high_haq_median,
                                       by = c('age_group_id', 'sex_id', 'acause')))
    ## Drop all data points for quintile 1-4 that are < high_haq_median
    cancer_data <- cancer_data[haq_quintile <= 4 & mi_ratio < high_haq_median, 
                                drop := 1]
    cancer_data <- cancer_data[is.na(drop), drop := 0]
    cancer_data <- cancer_data[drop!=1, ]
    # REDACTED
    # REDACTED
    # REDACTED
    # REDACTED
    # REDACTED
    
    
    
    quartile_three <- cancer_data[,.(quartile_three = ((quantile(mi_ratio, probs =0.75) - quantile(mi_ratio, probs = 0.25))*1.5+ quantile(mi_ratio, probs =0.75))),
                                  by = c('age_group_id', 'sex_id', 'acause', 'haq_quintile')]
    
    ## Merge onto full dataset
    haqUID = c('age_group_id', 'sex_id', 'acause', 'haq_quintile')
    cancer_data <- merge(cancer_data, quartile_three, by=haqUID)
    ## Drop all data points for quintile 1-4 that are > quartile 3
    cancer_data <- cancer_data[haq_quintile <= 4 & mi_ratio > quartile_three, 
                                drop := 1]
    cancer_data <- cancer_data[is.na(drop), drop := 0]
    cancer_data <- cancer_data[drop!=1, ]
    ## revert to data.frame
    output_df <- as.data.frame(cancer_data)
    output_df <- output_df[, !(names(output_df) %in% c('drop', 'high_haq_median',
                                                             'quartile_three'))]
    mir_prep.test_applyRestrictions(output_df)
    return(output_df)
}


mir_prep.generateCaps <- function(type, mir_model_version_id, input_data, pctile) {
    ## Generate "caps", the Winsorization values used to convert data to logit space
    ##
    print(paste("    aggregating to", type,  "caps..."))
    
    cause_config <- mir_prep.get_causeConfig()
    cause_config <- as.data.table(cause_config)
   
     ## Define function to generate upper caps
    if (type == "upper") {
        caps_table = "mir_upper_cap"
        required_columns = c("mir_model_version_id", "age_group_id", "acause")
        
        ### making the cap input data the mean across subnats within a nat location
        source("FILEPATH/get_location_metadata.R")
        locs <- get_location_metadata(release_id=9, location_set_id = 35)
        print("prepping upper cap inputs to be mean of subnats")
        input_data <- setDT(input_data)
        input_data <- merge(input_data, locs[level>2,.(location_id, iso = substr(ihme_loc_id, 1,3))], by="location_id", all.x=T)
        input_data <- input_data[, mi_ratio := mean(mi_ratio), by=c("age_group_id", "sex_id", "acause", "year_id", "mor_type", "iso", "age_type")]
        input_data <- unique(input_data, by=c("age_group_id", "sex_id", "acause", "year_id", "mor_type", "iso", "age_type"))
        input_data <- as.data.table(input_data)
        input_data <- input_data[acause!="neo_ben_brain"] # remove neo_ben_brain fro upper caps 
        print("upper cap inputs now mean of subnats")

        ### 

        all_cause_caps <- aggregate(mi_ratio~age_group_id, 
                                data=input_data, 
                                FUN = quantile, 
                                probs=pctile)
        all_cause_caps <- as.data.table(all_cause_caps)
        these_caps <- aggregate(mi_ratio~age_group_id+acause, 
                                    data=input_data, 
                                    FUN = quantile, 
                                    probs=pctile)
        these_caps <- as.data.table(these_caps)
        age_groups <- unique(these_caps$age_group_id)
        
        
        for(cause in unique(cause_config$acause)){
            if(cause_config[acause==cause]$use_cause_specific_upper_cap==0){ #REDACTED
                for(age in age_groups){
                    all_cause_cap = all_cause_caps[age_group_id==age]$mi_ratio
                    
                    # Remove cause specific cap if exists
                    these_caps = these_caps[!(acause==cause&age_group_id==age)]
                    
                    # Add all cause cap for age group
                    add <- data.table("acause" = c(cause), "age_group_id" = c(age), "mi_ratio" = c(all_cause_cap))
                    these_caps <- rbind(these_caps, add)
                    
                }
            }
        }
        
REDACTED
        #tREDACTED
    } else if (type == "lower") {
        caps_table = "mir_lower_cap"
        required_columns = c("mir_model_version_id", "age_group_id", "acause")
        input_data <- setDT(input_data) 
        input_data <- input_data[!(acause=="neo_eye_other" & mi_ratio == 0)] 
        input_data <- input_data[!(acause=="neo_tissue_sarcoma"& mi_ratio==0)] # REDACTED
        input_data <- input_data[!(acause=="neo_lymphoma_burkitt"& mi_ratio==0)] 
        fwrite(input_data, paste0("FILEPATH", mir_model_version_id, "_", Sys.Date(),".csv"))

        
        release <- get_gbd_parameter('current_release_id')
        mir_outliers <- cdb.get_table('mir_outliers')
        mir_outliers <- mir_outliers[c('is_active','mir_bundle_id','location_id',
                                       'sex_id','year_id','age_group_id')]
        bundle_info <- cdb.get_table('mir_model_entity')
        bundle_info <- subset(bundle_info, bundle_info$release_id == release &
                                  bundle_info$is_active == 1 &
                                  bundle_info$mir_process_type_id == 1)
        bundle_info <- bundle_info[c('mir_bundle_id','acause')]
        acause_mir_outliers <- merge(mir_outliers, bundle_info, by=c('mir_bundle_id'))
        input_data <- merge(input_data, acause_mir_outliers,
                            by=c('location_id','sex_id','year_id','age_group_id','acause'),
                            all.x=TRUE)
        input_data$is_active[is.na(input_data$is_active)] <- 0

        
        input_data <- input_data[!(is_active== 1)] # REDACTED
        print("saving out data pre lower caps post removing outliers")
        fwrite(input_data, paste0("FILEPATH", mir_model_version_id, "_", Sys.Date(),".csv"))
        these_caps <- aggregate(mi_ratio~age_group_id+acause, 
                                data=input_data, 
                                FUN=quantile, 
                                probs=pctile)
    }
    names(these_caps)[names(these_caps)%in%'mi_ratio'] <- paste0(type, "_cap")
    which_model_input <- mir.get_mir_version_id()
    these_caps$mir_model_version_id <- which_model_input
    # Update caps record
    existing_record <- mir.load_mi_caps(type, mir_model_version_id)
    other_records = (existing_record$mir_model_version_id != which_model_input)
    existing_record <- existing_record[other_records, ]
    mir_prep.test_generateCaps(newMapOfCaps=rbind(these_caps, existing_record, fill=TRUE), 
                               capType=type, required_columns, existing_record)
    # Delete previous versions of this record, then attach the current version
    delete_old = paste( "DELETE FROM", caps_table,
                            "WHERE mir_model_version_id =", which_model_input)
    cdb.run_query(delete_old)
    cdb.append_to_table(caps_table, these_caps)
    return(these_caps)
 }


mir_prep.addVariance <-function(df, run_logit_models, is_logit_model=true){
    ## scales MIRs using the upper caps and the offset, then
    ## calculates variance using either the delta method or
    ## the variance using the binomial distribution
    if (run_logit_models) offset <- mir.get_inputConfig()$offset
    print('adding variance...')
    ## calculates scaled MIRs
    if (is_logit_model) {
        
        ## and winsorize to the offset
        df$data <- df$mi_ratio / df$upper_cap
        df$data[df$data >= (1 - offset)] <- 1 - offset
        df$data[df$data <= offset] <- offset
    } else {
        df$data <- df$mi_ratio
    }
    ## calculate variance
    if (run_logit_models == TRUE) {
        df$variance <- df$data*(1 - df$data)/df$cases
    } else {
        sd <- sd(df$mi_ratio)
        avg <- mean(df$mi_ratio)
        df$variance <- (((exp(1)^(sd^2))-1)*exp(1)^(2*avg + sd^2)) / df$cases
    }
    ## ensure variance for causes which allow MIR = 0 to pass variance test
    VR_causes <- mir_prep.get_VR_causes()
    df$variance[df$acause %in% VR_causes$acause] <- 0.000001
    mir_prep.test_addVariance(df)
    return(df)
 }


 mir_prep.addSTGPR_requirements <- function(df){
    ## re-formats and renames columns, and generates required columns for 
    ##     integration with shared st-gpr modeling function
    ##
    print("adding stgpr requirements...")
    final_data <- df
    if (class(final_data$nid) == "character"){
        final_data[final_data$nid==".", 'nid'] = ""
        final_data$nid <- as.numeric(final_data$nid)
    }
    generic_nid = get_gbd_parameter("generic_cancer_nid")
    final_data[is.null(final_data$nid), 'nid'] <- generic_nid
    final_data$sample_size <- final_data$cases
    final_data$mir_model_version_id <- mir.get_mir_version_id()
    final_data$me_name = paste0(final_data$acause, '_mi_ratio')
    setnames(final_data, 
            old = c('nid', 'cases'), 
            new = c('nid', 'incident_cases' ))
    return(final_data)
 }


mir_prep.updateOutliers <- function(new_mir_input, me_name) {
    
    ##  REDACTED
    
    
    
    ##
    
    outlier_dir = get_path("stgpr_outlier_dir", process="cancer_model")
    generic_nid = get_gbd_parameter("generic_cancer_nid")
    outlier_file <- paste0(outlier_dir, "/", me_name, "/outlier_db.csv")
    outliers <- read.csv(outlier_file)
    outlier_cols = colnames(outliers)
    unassigned_otlr = (outliers$nid %in% c(0, generic_nid) | 
                        is.na(outliers$nid))
    to_update <- outliers[unassigned_otlr,]
    if (nrow(to_update) > 0) {
        # archive the old version
        archive_folder <- get_path("mi_model", base_folder="workspace")
        a_file <- paste0("outlier_db_", gsub(" ", "_", Sys.time()), ".csv")
        archive_file <- file.path(archive_folder, me_name, a_file)
        ensure_dir(archive_file)
        write.csv(outliers, archive_file, row.names=FALSE)
        # update missing or generic nids
        setnames(new_mir_input, old="nid", new="new_nid")
        updated <- merge(to_update, new_mir_input, all.x=TRUE, all.y=FALSE)
        has_replacement = (!is.na(updated$new_nid) & 
                            updated$new_nid != updated$nid)
        updated[has_replacement, 'nid'] <- updated[has_replacement, 'new_nid']
        # re-combine with non-missing data and save
        no_update = outliers[!unassigned_otlr,]
        corrected <- rbind(no_update, updated[,outlier_cols])
        corrected <- corrected[!duplicated(corrected),]
        write.csv(corrected, outlier_file, row.names=FALSE) #new data
    }
}


## -------------------------------------------------- ##
## test functions
## -------------------------------------------------- ##
mir_prep.test_inputData <- function(df){
    results = list('missing columns'=c(), 'duplicates check'=c(), 
                    'missing data from previous round'=data.frame())
    # Run standard tests
    uidCols = mir_prep.get_uidColumns()
    required_columns = unique(c(uidCols, 'age_group_id', 'year_id', 'cases', 'deaths'))
    results <- mir_prep.standardTests(results, df, required_columns)
    test_utils.checkTestResults(results, "inputData")
}

mir_prep.test_inputConfig <- function(df){
    results = list('missing columns'=c(), 'duplicates check'=c())
    required_columns = c('mir_model_version_id', 'mir_model_input_version',
                         'max_mi_input_accepted', 'offset',
                          'upper_cap_percentile', 'lower_cap_percentile')
    results['missing columns'] <- test_utils.findMissingColumns(df, 
                                                                required_columns)
    results['duplicates check'] <- test_utils.duplicateChecker(df,
                                            uniqueidentifiers=c('mir_model_version_id'))
    test_utils.checkTestResults(results, "inputConfig")
}

mir_prep.test_causeConfig <- function(df){
    results = list('no data' =c(), 'missing columns'=c(), 'duplicates check'=c())
    required_columns = c('acause', 'mir_cause_config_id')
    if (nrow(df) == 0) {
        results['no data'] = "no entries present in the cause config for this model number"
    } else results['no data'] = "passed"
    results['missing columns'] <- test_utils.findMissingColumns(df, 
                                                                required_columns)
    results['duplicates check'] <- test_utils.duplicateChecker(df, 
                                                uniqueidentifiers=required_columns)
    test_utils.checkTestResults(results, "causeconfig")
}

mir_prep.test_applyRestrictions <- function(df) {
    results = list('missing columns'=c(), 'duplicates check'=c())
    required_columns = mir_prep.get_uidColumns()
    results <- mir_prep.standardTests(results, df, required_columns)
    test_utils.checkTestResults(results, "applyrestrictions")
}

mir_prep.test_aggData <- function(df, input_df, arUpdated, columnsAggregated){
    results = list('missing columns'=c(), 'duplicates check'=c())
    uidCols = mir_prep.get_uidColumns()
    required_columns = unique(c(uidCols, columnsAggregated))
    results <- mir_prep.standardTests(results, df, required_columns)
    test_utils.checkTestResults(results, "aggData")
}

mir_prep.test_generateCaps <- function(newMapOfCaps, capType, 
                                        required_columns, existingCapMap){
    results = list('replacement error' = "", 'dropping data'="", 
                    'missing columns'=c(), 'duplicates check'=c())
    if (length(unique(newMapOfCaps$mir_model_version_id)) < length(unique(existingCapMap$mir_model_version_id))) {
        replacement_message = paste("generateCaps is trying to delete", 
                                    capType, "cap history")
        results['replacement error'] <- replacement_message
    }
    for (c in required_columns) {
        if (c %in% c("uppder_cap", "lower_cap")) next
        existing_values <-unique(existingCapMap[[c]])
        new_values <- unique(newMapOfCaps[[c]])
        if (!all(existing_values %in% new_values)) {
            this_deletion_message = paste0("generateCaps is trying to delete ", 
                                            capType, " caps ", c, 
                                        " from cap history. If this is an ",
                                        "intentional change, update the ",
                                        "test_generateCaps function.")
            if (results['dropping data']=="") {
                results['dropping data']= this_deletion_message
            } else {
                results['dropping data'] = c(results['dropping data'], this_deletion_message)
            }
        }
    }
    results['missing columns'] <- test_utils.findMissingColumns(newMapOfCaps, 
                                                                required_columns)
    results['duplicates check'] <- test_utils.duplicateChecker(newMapOfCaps, 
                                                uniqueIdentifiers=required_columns)
    testName <- paste("generateCaps,", capType)
    test_utils.checkTestResults(results, testName)
}

mir_prep.test_cleanedData <- function(df){
    results = list('missing columns'=c(), 'duplicates check'=c())
    uidCols = mir_prep.get_uidColumns()
    required_columns = c(uidCols, 'mi_ratio')
    results <- mir_prep.standardTests(results, df, requiredColList = uidCols)
    test_utils.checkTestResults(results, "cleanedData")
}

mir_prep.test_addVariance <- function(df){
    results = list('missing columns'=c(), 'duplicates check'=c(),
                    'null variance entries'=c(), 
                    'some variance outside acceptable range'=c())
    uidCols = mir_prep.get_uidColumns()
    required_columns = c(uidCols, 'variance', 'mi_ratio')
    results <- mir_prep.standardTests(results, df, requiredColList = required_columns)
    results['null entries'] <- test_utils.booleanTest(!nrow(df[is.na(df$variance),]))
    results['some variance outside acceptable range'] <- test_utils.booleanTest(
                                                    !nrow(df[df$variance <= 0,]) &
                                                    !nrow(df[df$variance >= 1,]))
    test_utils.checkTestResults(results, "addVariance")
}

mir_prep.test_outputData <- function(df, input_df){
    results = list('missing columns'=c(), 'duplicates check'=c())
    uidCols = c((mir_prep.get_uidColumns()), 'nid')
    results <- mir_prep.standardTests(results, df, requiredColList = uidCols)
    test_utils.checkTestResults(results, "outputData")
}

mir_prep.standardTests <- function(results, testDf, requiredColList){
    
    ##       dataset: missing columns and duplicates
    uidCols = mir_prep.get_uidColumns()
    results['duplicates check'] <- test_utils.duplicateChecker(testDf, uidCols)
    results['missing columns'] <- test_utils.findMissingColumns(testDf, 
                                                                requiredColList)
    results['columns missing values'] <- test_utils.findMissingValues(testDf, 
                                                                requiredColList)
    return(results)
}

