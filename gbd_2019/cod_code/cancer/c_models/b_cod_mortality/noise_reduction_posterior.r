##
## Description: Noise reduction for cancer moratlity and incidence data
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
    return(c('location_id','year_id','sex_id', 'age_group_id'))
}


get_required_cols <- function(metric) {
    return(c(get_uid_vars(), 'is_loc_agg', 'acause','country_id', metric))
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


get_predicted_var <- function(DT) { 
    ## set variance based on incidence 
    ## normalize standard error according to NAME estimation 
    ##
    print('within predicting variance...')
    DT[,'pred_std_err' := exp(prior_std_err - 1) * pred_cf]  

    # set predicted variance to high value when null
    DT[,'pred_var':=pred_std_err^2]
    DT[is.null(pred_var) | is.infinite(pred_var), pred_var := 10^91 * pred_cf]


    return(DT) 
}

make_metrics_using_results <- function(DT) { 
    ## calculate metric using predicted cases 
    ## 
    non_zero_std_err <- DT[prior_std_err != 0,]
    non_zero_std_err <- get_predicted_var(non_zero_std_err) 
    zero_std_err <- DT[prior_std_err == 0,]
    zero_std_err <- get_predicted_var(zero_std_err)
    DT <- rbind(non_zero_std_err, zero_std_err)

    # getting metrics for underlying data 
    # math magics formulars from NAME stata code 
    cf_component <- DT[,'cf_raw'] * (1 - DT[,'cf_raw'])
    DT[,'std_err_data':= sqrt((cf_component / DT[,sample_size]) + 
        ((1.96^2) / (4 * DT[,sample_size] ^ 2)))]
    
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
    input_dir <- get_path('NR_prior', process='cod_mortality')
    prior <- read.csv(input_dir)
    prior <- add_sample_size(prior ,'deaths') # re-insert sampe_size 
    output = convert_to_cause_fractions(prior) 
    DT <- as.data.table(output)
    output = make_metrics_using_results(DT)
    output = replace_data(output)

    write.csv(output, get_path("noise_reduced", process="cod_mortality"), row.names=F)
}