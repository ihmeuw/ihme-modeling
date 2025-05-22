# load Libraries
library(here)

repo_name <- "cancer_estimation"
code_repo <- here::here()
if (!grepl(repo_name, code_repo)) {
  code_repo <- paste0(code_repo, "/", repo_name)
}

source(file.path(code_repo, 'r_utils/utilities.r'))
source(file.path(code_repo, 'r_utils/cluster_tools.r'))
source(paste0(get_path('common', key='shared_r_libraries'), 'get_location_metadata.R'))

##
library(parallel)
library(stats)
library(MASS)
library(data.table)


get_uid_vars <- function() {
    return(c('location_id', 'year_id','sex_id', 'age_group_id'))
}


get_required_cols <- function(metric) {
    return(c(get_uid_vars(), 'acause', 'country_id', metric))
}


add_sample_size <- function(df, metric) {
    
    
    ##
    uid_cols <- get_uid_vars()
    print('within adding sample size')
    df$sample_size <- ave(df[,metric], df[,uid_cols], FUN=sum)

    df <- subset(df, df$sample_size > 0)
    return(df)
}


calculate_cause_fractions <- function(df) { 
    df$cf_raw <- df$deaths / df$sample_size
    return(df)
}

add_loc_agg <- function(df) { 
    subnat_countries <- get_gbd_parameter('subnationally_modeled_countries')
    df$is_loc_agg <- 0 
    df$is_loc_agg[(df$country_id %in% subnat_countries$subnationally_modeled_countries) & 
                    (df$country_id == df$location_id)] <- 1 

    
    df$subnat_id <- copy(df$location_id) 
    df$subnat_id[(df$location_id == df$country_id)] <- 0
    return(df) 
}

make_factors <- function(data) {
    data$year_id <- as.factor(data$year_id)
    data$age_group_id <- as.factor(data$age_group_id)
    data$country_id <- as.factor(data$country_id)
    data$subnat_id <- as.factor(data$subnat_id)
    return(data)
}

run_model <- function(df) {
    formula <- determine_formula(df)
    glm.control(maxit=100)
    
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


fill_with_average <- function(input_df) {
    ## Returns a dataframe with the number of predicted events (pred_events)
    df <- as.data.table(input_df)
    print('in fill_with_average')
    df[,average_prior_sample_size := mean(sample_size), by=age_group_id]
    df[,averaged:= 1]
    df[,prior_fit_col:= sum(deaths) / sum(sample_size) * sample_size, by=age_group_id] 
    df[,prior_std_err := 0]
    df <- as.data.frame(df)
    return(df)
}


reduce_noise <- function(df, cause, sex) {
    print(paste("Modeling",' ', cause, "for sex:", sex))#, 'location aggregate:', loc_agg))
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
    data <- add_loc_agg(data)
    data <- make_factors(data)
    data <- add_sample_size(data, metric)
    data <- calculate_cause_fractions(data) 
    data <- subset(data, data$acause != 'cc_code')
    data <- data[order(data$year_id),]
    data$averaged <- 0 # column to keep track of subsets that got averaged 
    return(data) 
}

manage_noise_reduction <- function(data, metric, this_cause, this_sex) {
    required_cols <- get_required_cols(metric)
    data <- prep_noise(data, metric)
    if (length(setdiff(required_cols, names(data))> 0)){
        print(paste(setdiff(required_cols, names(data)), "column(s) missing from input"))
    }
    this_data <- data[(data$acause == this_cause & 
                        data$sex_id == this_sex),]
    this_result <- reduce_noise(this_data, this_cause, this_sex)
    return(this_result)
}




if (!interactive()){
    print("Starting noise_reduction_prior.r")
    this_cause <- commandArgs(trailingOnly=TRUE)[1] 
    this_sex <- commandArgs(trailingOnly=TRUE)[2]
    input_dir <- get_path(process='cod_mortality',key='mortality_split_cc')
    df <- read.csv(input_dir)
    output = manage_noise_reduction(data = df, metric='deaths', this_cause,
                                this_sex)
    write.csv(output, paste0(get_path(process='cod_mortality', 
                                key='noise_reduction_workspace'),
                                '/',  this_cause,'_',this_sex,'.csv'),
                                row.names=F)
} else {
  this_cause <- 'neo_colorectal'
  this_sex <- 2
  input_dir <- get_path(process='cod_mortality',key='mortality_split_cc')
  df <- read.csv(input_dir)
  output = manage_noise_reduction(data = df, metric='deaths', this_cause,
                                  this_sex)
  write.csv(output, paste0(get_path(process='cod_mortality', 
                                    key='noise_reduction_workspace'),
                           '/',  this_cause,'_',this_sex,'.csv'),
            row.names=F)
}
  
