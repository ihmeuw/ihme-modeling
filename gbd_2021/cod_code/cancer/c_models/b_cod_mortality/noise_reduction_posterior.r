##
## Description: Noise reduction for cancer moratlity and incidence data
## Contributors: USERNAME 


# load libraries
library(here)
source(file.path('FILEPATH/utilities.r'))
source(file.path('FILEPATH/cluster_tools.r'))
##
library(parallel)
library(stats)
library(MASS)
library(data.table)


get_uid_vars <- function() {
    return(c('location_id','year_id','sex_id', 'age_group_id'))
}


get_required_cols <- function(metric) {
    return(c(get_uid_vars(), 'acause','country_id', metric))
}


add_sample_size <- function(df, metric) {
    ## Generates a sample-size equal to the number of events for the metric by
    ##     age-year-location-sex
    ##
    uid_cols <- get_uid_vars()
    print('within adding sample size')
    df$sample_size <- ave(df[,metric], df[,uid_cols], FUN=sum)

    # remove cc_code 
    df <- subset(df, df$acause != 'cc_code')
    return(df)
}


convert_to_cause_fractions <- function(df) { 
    ##
    ##
    df$pred_cf <- df$prior_fit_col / df$sample_size 
    return(df)
}


get_predicted_var <- function(DT, zero_std_err) { 
    ## set variance based on incidence 
    ## normalize standard error according to Brad's estimation 
    ##
    print('within predicting variance...')
    # standard error of prediction should only be 0 if averaged was used as the prior
    # use wilson score interval and prior sample size to calculate variance
    if (zero_std_err == TRUE) { 
        #DT[, average_prior_sample_size := mean(prior_sample_size), by=age_group_id]
        DT[,'pred_std_err' := get_wilson_std_err(DT[,pred_cf], DT[,average_prior_sample_size])]
        DT[,'pred_var':= pred_std_err^2 ] 
    }
    else {
        # apply a transformation of exp(se-1) in order to prevent the standard error
        # from being arbitrarily small, limiting the influence of the prior 
        DT[,'pred_std_err' := exp(prior_std_err - 1) * pred_cf] 

        # set predicted variance to high value when null
        DT[,'pred_var':=pred_std_err^2]
        DT[is.null(pred_var) | is.infinite(pred_var), pred_var := 10^91 * pred_cf]
    }
    return(DT) 
}


get_wilson_std_err <- function(cf, sample_size) { 
    cf_component = cf * (1 - cf) 
    return( 
        sqrt(
            (cf_component / sample_size) + 
        ((1.96^2) / (4 * sample_size ^ 2))
    ) / (1 + 1.96^2 / sample_size)
    )
}


make_metrics_using_results <- function(DT) { 
    ## calculate metric using predicted cases 
    ## 
    non_zero_std_err <- DT[prior_std_err != 0,]
    non_zero_std_err <- get_predicted_var(non_zero_std_err, FALSE) 

    if (nrow(DT[prior_std_err == 0,]) == 0){ 
        zero_std_err <- DT[prior_std_err == 0,]
    }
    else { 
        zero_std_err <- get_predicted_var(zero_std_err, TRUE)
    }
    DT <- rbind(non_zero_std_err, zero_std_err, fill=TRUE)
    DT[,'std_err_data':= get_wilson_std_err(DT[,cf_raw], DT[,sample_size])] 
    DT[,'variance_data' := DT[,std_err_data] ^ 2] 
    DT[,'mean_cf_data_component' := DT[,cf_raw] * (DT[,pred_var] / (DT[,pred_var] + DT[,variance_data]))]
    DT[,'mean_cf_pred_component' := (DT[,pred_cf] * (DT[,variance_data] / (DT[,pred_var] + DT[,variance_data])))]
    DT[,'mean' := (DT[,mean_cf_data_component] + DT[,mean_cf_pred_component])]
    DT[,'variance_numerator' := (DT[,pred_var] * DT[,variance_data])]
    DT[,'variance_denominator' := DT[,pred_var] + DT[,variance_data]]
    DT[,'variance' := DT[,variance_numerator] / DT[,variance_denominator]]
    return(DT)    
}  

replace_data <- function(DT) { 
    ## use models results to fill in values 
    ##
    DT[,'pre_deaths' := DT[,deaths]]
    DT[,'cf_final' := DT[,mean]]
    DT[,'deaths' := DT[,sample_size] * DT[,cf_final]]
    return(DT)
}



## Run functions if sourced independently
if (!interactive()){
    input_dir <- get_path('NR_prior_cc', process='cod_mortality')
    prior <- read.csv(input_dir)
    prior <- add_sample_size(prior ,'deaths') # re-insert sampe_size 
    output = convert_to_cause_fractions(prior) 
    DT <- as.data.table(output)
    output = make_metrics_using_results(DT)
    output = replace_data(output)

    write.csv(output, get_path("noise_reduced", process="cod_mortality"), row.names=F)
}