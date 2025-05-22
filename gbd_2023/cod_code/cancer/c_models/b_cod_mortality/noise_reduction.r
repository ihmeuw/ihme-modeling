##

# Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME
## load Libraries
library(here)  
repo_name <- "cancer_estimation"
code_repo <- here()
if (!grepl(repo_name, code_repo)) {
  code_repo <- paste0(code_repo, "/", repo_name)
}

source(file.path(code_repo, 'r_utils/utilities.r')) 
##
library(stats)
library(MASS)
library(data.table)


get_uid_vars <- function() {
    return(c('location_id','year_id','sex_id', 'age_group_id'))
}


get_required_cols <- function(metric) {
    return(c(get_uid_vars(), 'acause', 'pop', 'country_id', metric))
}
 

add_sample_size <- function(df, metric) {
    
    
    ##
    uid_cols <- get_uid_vars()
    df$sample_size <- ave(df[,metric], df[,uid_cols], FUN=sum)
    return(df)
}

make_factors <- function(data) {
    
    ##
    data$year_id <- as.factor(data$year_id)
    data$age_group_id <- as.factor(data$age_group_id)
    data$country_id <- as.factor(data$country_id)
    data$location_id <- as.factor(data$location_id)
    return(data)
}


sufficient_df <- function(df, metric) {
    ## Returns boolean indicator of sufficient sample size to run the regression
    ##
    # for subnationals must implement checking if subnational 
    # locations have > 50 sample_size
    subnationals = (length(unique(df$location_id)) > 1)
    nonzero_rows = nrow(df[df$rate > 0,])
    if (subnationals) {
        ok_rows = nrow(df[df$sample_size >= 50,])
    } else {
        ok_rows = nonzero_rows
    }
    return((ok_rows > 6) & (nonzero_rows > 0))
}


run_model <- function(df) {
    many_locations = (length(unique(df$location_id)) > 1)
    if (many_locations) {
        model_data <- df[df$sample_size >= 50,]
    } else {
        model_data <- df
    }
    formula <- determine_formula(model_data)
    glm.control(maxit=100)
    
    output <- tryCatch({ pois_model <- glm(formula = formula,
                                                family = poisson(),
                                                data=model_data)
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
        df = fill_with_average(df)
    } else{
        pois_results <- predict.glm(pois_model, new_data=df, se.fit=TRUE)
        # We exponentiate the results as the cases come out in the log space
        df$pred_rate <- exp(pois_results$fit)
        df$pred_events <- df$pred_rate * df$pop
        df$std_err <- pois_results$se.fit
    }
    # This produces the fitted values, including standard error for each 
    # observation. The standard error is for the mean result given each row's
    
    return(df)
}


run_model_with_region <- function(df) {
    
    ## Returns the dataframe with columns for the predicted number of events for
    ##      the metric (pred_events) and the standard error
    ##
    print(paste0("\n RUNNING WITH REGION"))
    a_formula <- determine_formula(df)
    glm.control(maxit=100)
    
    pois_model <- glm(formula = a_formula,
                    family = poisson(),
                    data=df)
    # We are able to check if the maximum likelihood estimation converged 
    pois_converged <- pois_model$converged
    if (!pois_converged) {
        df <- fill_with_average(df)
    } else {
        pois_results <- predict.glm(pois_model, se.fit=TRUE)
        # We exponentiate the results as the cases come out in the log space
        df$pred_events <- exp(pois_results$fit)
        df$pred_events <- df$pred_rate * df$pop
        df$std_err <- pois_results$se.fit
    }
    return(df)
}


determine_formula <- function(df) {
    num_years = length(unique(df$year_id))
    num_ages = length(unique(df$age_group_id))
    num_countries = length(unique(df$country_id))
    num_subnats = length(unique(df$location_id))
    
    formula <- "rate ~ offset(log(pop))"
    if (num_years > 1) {
        formula <- paste(formula, "+ year_id")
    }
    if (num_ages > 1) {
        formula <- paste(formula, "+ age_group_id")
    }
    if (num_countries > 1) {
        formula <- paste(formula, "+ location_id")
    }
    print(formula)
    return(formula)
}


fill_with_average <- function(df) {
    
    ## Returns a dataframe with the number of predicted events (pred_events) 
    df$pred_rate <- ave(df$rate, df$age_group_id, FUN=mean)
    df$pred_events <- df$pred_rate * df$pop
    df$std_err <- rep(0, nrow(df))
    subset(df, select=-c('mean_rate_final'))
    return(df)
}


reduce_noise <- function(df, cause, sex, country_id) {
    print(paste("Modeling", cause, "for sex:", sex, "in country_id", country_id))
    if (nrow(df) == 0) {
        return(df)
    } else if (mean(df$rate) == 0) {
        return(df)
    } else if (sufficient_df(df)) {
        return(run_model(df))
    } else { 

        return(fill_with_average(df))
    }
}


manage_noise_reduction <- function(data, metric) {
    required_cols <- get_required_cols(metric)
    input_data <- fread(input_file)
    if (length(setdiff(required_cols, names(input_data)))> 0){
        print(paste(setdiff(required_cols, names(input_data)), "column(s) missing from input"))
    }
    data <- make_factors(data)
    data <- add_sample_size(data)
    data$rate <- data[,metric]/data$pop

    data <- data[order(data$year_id),]
    result_appended <- data.frame()
    causes <- unique(data$acause)
    for (this_cause in causes) {
        sexes <- unique(data[data$acause == this_cause, 'sex'])
        for(this_sex in sexes) {
            country_locs <- unique(data[data$sex_id == this_sex & data$acause == this_cause, 'country_id'])

            for (this_country in country_locs) {
                this_data <- data[(data$acause == this_cause & data$sex_id == this_sex & data$country_id == this_country),]
                this_result <- reduce_noise(this_data, this_cause, this_sex, this_country)
                result_appended <- rbind(result_appended, this_result, fill=TRUE)
            }
        }
    }

    result_appended[,metric] <- result_appended$rate * result_appended$pop
    return(result_appended)
}



if (!interactive()){
    input_file <- get_path("combined_incidence", process="cod_mortality")
    df = read.csv(input_file)
    output = manage_noise_reduction(data = df, metric='cases')
    write.csv(output, get_path("noise_reduced", process="cod_mortality"), row.names=F)
}
