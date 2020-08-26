##cription: Noise reduction for cancer moratlity and incidence data
## Contributors: NAME

# load Libraries
library(here)
if (!exists("code_repo"))  {
    code_repo <-  sub("cancer_estimation.*", 'cancer_estimation', here())
    if (!grepl("cancer_estimation", code_repo)) code_repo <- file.path(code_repo, 'cancer_estimation')
}
source(file.path(code_repo, 'r_utils/utilities.r'))
source(file.path(code_repo, 'r_utils/cluster_tools.r'))
##
library(parallel)
library(stats)
library(MASS)
library(data.table)


get_uid_vars <- function() {
    return(c('location_id', 'year_id','sex_id', 'age_group_id'))
}


get_required_cols <- function(metric) {
    return(c(get_uid_vars(), 'is_loc_agg', 'acause', 'country_id', metric))
}


add_sample_size <- function(df, metric) {
    ## Generates a sample-size equal to the number of events for the metric by
    ##     age-year-location-sex
    ##
    uid_cols <- get_uid_vars()
    print('within adding sample size')
    df$sample_size <- ave(df[,metric], df[,uid_cols], FUN=sum)

    # remove entries where sample_size <= 0
    # regressions will diverge if sample_size <= 0  
    df <- subset(df, df$sample_size > 0)
    return(df)
}


calculate_cause_fractions <- function(df) { 
    ##
    ##
    ##
    df$cf_raw <- df$deaths / df$sample_size
    return(df)
}


add_loc_agg <- function(df) { 
    ## Accepts a dataframe and marks entries that are location aggregates
    ## is_loc_agg == 1 iff country level AND country isn't modeled subnationally 
    subnat_countries <- get_gbd_parameter('subnationally_modeled_countries')
    df$is_loc_agg <- 0 
    df$is_loc_agg[(df$country_id %in% subnat_countries$subnationally_modeled_countries) & 
                    (df$country_id == df$location_id)] <- 1 

    # subnat id to help determine which countries are subnationally modeled
    df$subnat_id <- copy(df$location_id) 
    df$subnat_id[(df$location_id == df$country_id)] <- 0
    return(df) 
}


make_factors <- function(data) {
    ## Returns data with factorized columns for year_id, age, and location
    ##
    data$year_id <- as.factor(data$year_id)
    data$age_group_id <- as.factor(data$age_group_id)
    data$country_id <- as.factor(data$country_id)
    data$subnat_id <- as.factor(data$subnat_id)
    return(data)
}


run_model <- function(df) {
    ## Accepts a dataframe/datatable subset by cause, sex, and coutnry
    ## Returns the dataframe with columns for the predicted number of events for
    ##      the metric (pred_events) and the standard error
    ##
    formula <- determine_formula(df)
    glm.control(maxit=100)
    # Specify the model parameters
    output <- tryCatch({ pois_model <- glm(formula = formula,
                                                family = poisson(),
                                                data=df)
                         pois_converged <- pois_model$converged
                        },
                         error=function(cond) {
                         print("Errored, filling with average")
                         pois_model <- NULL
                         pois_converged <- FALSE
                         output <- list(pois_converged, pois_model)
                         return(output)
                        })

    pois_converged <- as.logical(output[1])
    # We are able to check if the maximum likelihood estimation converged
    print(paste0("Poisson converged? ", pois_converged))
    if (!pois_converged) { 
        df <- fill_with_average(df) 
    } 
    else {
        pois_results <- predict.glm(pois_model, new_data=df, se.fit=TRUE)

        # We exponentiate the results as the cases come out in the log space
        df$prior_fit_col <- exp(pois_results$fit)
        df$prior_std_err <- pois_results$se.fit
    }
    return(df)
}


determine_formula <- function(df) {
    ## Returns the formula to use based on whether the factor variables
    ## have only one value
    ##
    num_years = length(unique(df$year_id))
    num_ages = length(unique(df$age_group_id))
    num_countries = length(unique(df$country_id))
    num_subnats = length(unique(df$subnat_id))
    formula <- "deaths ~ offset(log(sample_size))"
    if (num_years > 1) {
        formula <- paste(formula, "+ year_id")
    }
    if (num_ages > 1) {
        formula <- paste(formula, "+ age_group_id")
    }
    if (num_subnats > 1) {
        formula <- paste(formula, "+ subnat_id")
    }
    if (num_countries >1) { 
        formula <- paste(formula, '+ country_id')
    }
    print(formula)
    return(formula)
}


fill_with_average <- function(df) {
    ## Accepts a dataframe/datatable subset by cause, sex, and country
    ## Returns a dataframe with the number of predicted events (pred_events)
    ##      equal to the product of the population and the mean rate by
    ##      age_group_id
    ##
    print('in fill_with_average')
    df$averaged <- 1 
    df$prior_fit_col <- ave(df$deaths, df$age_group_id, FUN=mean)
    df$prior_std_err <- rep(0, nrow(df))
    return(df)
}


reduce_noise <- function(df, cause, sex, loc_agg) {
    ##  Returns dataframe with reduced noise for the cause, sex, and country in
    ##      one of two ways: if data are 'sufficient', runs a poisson regression;
    ##       else, replaces all data with the mean rate by sex and cause
    ##
    print(paste("Modeling", cause, "for sex:", sex, 'location aggregate:', loc_agg))
    if (nrow(df) == 0) {
        return(df)
    } else if (mean(df$deaths) == 0) {
        df$prior_fit_col <- 0 
        df$prior_std_err <- 0
        return(df)
    } else if (nrow(df) >= 6) { # enough data points to run a poisson model 
        return(run_model(df))
    } else {
        return(fill_with_average(df))
    }
}

prep_noise <- function(data, metric) { 
    ##
    data <- add_loc_agg(data) 
    data <- make_factors(data)
    data <- add_sample_size(data, metric)
    data <- calculate_cause_fractions(data) 
    data <- subset(data, data$acause != 'cc_code')
    data <- data[order(data$year_id),]
    data$averaged <- 0 # column to keep track of subsets that got averaged 
    return(data) 
}

manage_noise_reduction <- function(data, metric) {
    ##
    ##
    required_cols <- get_required_cols(metric)
    data <- prep_noise(data, metric) 
    if (length(setdiff(required_cols, names(data))> 0)){
        print(paste(setdiff(required_cols, names(data)), "column(s) missing from input"))
    }
    result_appended <- data.frame()
    is_loc_agg <- unique(data$is_loc_agg)
    for (loc_agg in is_loc_agg) { 
        causes <- unique(data$acause)
        for (this_cause in causes) {
            sexes <- unique(data$sex_id) 
            for (this_sex in sexes) {
                this_data <- data[(data$acause == this_cause & 
                                    data$sex_id == this_sex & 
                                    data$is_loc_agg == loc_agg),]
                this_result <- reduce_noise(this_data, this_cause, this_sex, loc_agg)
                result_appended <- rbind(result_appended, this_result)
            }
        }
    }
    return(result_appended)
}



## Run functions if sourced independently
if (!interactive()){
    input_dir <- get_path(process='cod_mortality',key='mortality_split_cc')
    df <- read.csv(input_dir)
    output = manage_noise_reduction(data = df, metric='deaths')
    write.csv(output, get_path('NR_prior', process='cod_mortality'), row.names=F)
}