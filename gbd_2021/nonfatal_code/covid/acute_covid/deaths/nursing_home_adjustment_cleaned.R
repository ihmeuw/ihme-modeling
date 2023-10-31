## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Nursing Home proportion adjustment
## Contributors: 
## Date:4/13/2021
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
version <- 4


# source(paste0(.repo_base, 'FILEPATH/utils.R'))
# source(paste0(roots$'ROOT' ,"FILEPATH/get_location_metadata.R"))
# source(paste0(roots$'ROOT' ,"FILEPATH/get_age_metadata.R"))
library(openxlsx)
library(dplyr)
library(gtools)
#library(readxl)
library(msm, lib.loc = "FILEPATH")
#library(mrbrt001, lib.loc = 'FILEPATH')
library(mrbrt002, lib.loc = "FILEPATH")
library(data.table)
#setwd("FILEPATH")

outputfolder <- paste0('FILEPATH')
dir.create(outputfolder)



#data<- read_excel("FILEPATH/prop_deaths_nursing_homes_extraction.xlsx", sheet='extraction')
data<- read.xlsx("FILEPATH/prop_deaths_nursing_homes_extraction.xlsx", sheet='extraction')
data$prev <- data$mean_amongdeaths
data$standard_error <- sqrt((data$prev * (data$prev)) / data$sample_size)

#summary(data$mean)
#hist(data$mean)

#adding a dummy column
data$one <-1
df<- data

n_samples <- 1000L
max_draw <- n_samples - 1


######################################################################################
#  Nursing home adjustment
######################################################################################

    # model_dir <- paste0(out,"FILEPATH")
    # dir.create(paste0(outputfolder, model_dir))

### basically a loop that goes through each row and calcs the se in log space
    df$delta_log_se <- sapply(1:nrow(df), function(i) {
      ratio_i <- df[i, "prev"] # relative_risk column
      ratio_se_i <- df[i, "standard_error"]
      deltamethod(~log(x1), ratio_i, ratio_se_i^2)
    })
    df$log_se <- df$delta_log_se
    
    # df <- df[!is.na(df$standard_error) & !is.na(df$log_se),]
    
    # set up data
    mr_df <- MRData()
    
    mr_df$load_df(
      data = df, col_obs = "prev", col_obs_se = "standard_error",
      #col_covs = list("hospital_icu", "age_mid"), col_study_id = "study_id")
      col_covs = list("one"), col_study_id = "location")
    
    model <- MRBRT(
      data = mr_df,
      cov_models =list(
        LinearCovModel("intercept", use_re = TRUE),
        LinearCovModel("one", use_re = FALSE)
      ),
      inlier_pct = 0.9)
        
    
    # fit model
    model$fit_model(inner_print_level = 5L, inner_max_iter = n_samples)
    
    (coeffs <- rbind(model$cov_names, model$beta_soln))
    write.csv(coeffs, paste0(outputfolder, "coeffs_nursinghome_",version,".csv"))    
  
    # save model object
    py_save_object(object = model, filename = paste0(outputfolder, "mod1.pkl"), pickle = "dill")
    
    model <- py_load_object(filename = paste0(outputfolder, "mod1.pkl"), pickle = "dill")

    # make predictions for full year
    predict_matrix <- data.table(intercept = model$beta_soln[1], one=1)
    
    predict_data <- MRData()
    predict_data$load_df(
      data = predict_matrix,
      col_covs=list("one"))
    
    #n_samples <- 1000L
    samples <- model$sample_soln(sample_size = n_samples)
    
    draws_raw <- model$create_draws(
      data = predict_data,
      beta_samples = samples[[1]],
      gamma_samples = samples[[2]],
      random_study = TRUE,
      sort_by_data_id = TRUE)
    # write draws for pipeline
    draws_raw <- data.table(draws_raw)
    draws_raw <- cbind(draws_raw, predict_matrix)
    setnames(draws_raw, paste0("V", c(1:n_samples)), paste0("draw_", c(0:max_draw)))
    draws <- melt(data = draws_raw, id.vars = c("intercept", "one"))
    setnames(draws, "variable", "draw")
    setnames(draws, "value", "proportion")
#    draws$proportion <- exp(draws$proportion) / (1 + exp(draws$proportion))
    
    write.csv(draws, file =paste0(outputfolder, "predictions_draws_nursing_home_",version,".csv"))
    
    
    predict_matrix$pred <- model$predict(predict_data, sort_by_data_id = TRUE)
#    predict_matrix$pred <- exp(predict_matrix$pred) / (1 + exp(predict_matrix$pred))
    predict_matrix$pred_lo <- apply(draws_raw, 1, function(x) quantile(x, 0.025))
#    predict_matrix$pred_lo <- exp(predict_matrix$pred_lo) / (1 + exp(predict_matrix$pred_lo))
    predict_matrix$pred_hi <- apply(draws_raw, 1, function(x) quantile(x, 0.975))
#    predict_matrix$pred_hi <- exp(predict_matrix$pred_hi) / (1 + exp(predict_matrix$pred_hi))
    used_data <- cbind(model$data$to_df(), data.frame(w = model$w_soln))
    used_data <- as.data.table(used_data)
    rr_summaries <- copy(predict_matrix)
    
    rr_summaries
    rr_summaries$gamma <- mean(samples[[2]])
    write.csv(rr_summaries, file =paste0(outputfolder,"predictions_summary_nursing_home_", version,".csv"))
    

    
    
    
    used_data$weight <- 1/(15*used_data$obs_se)
    rr_summaries$study_id <- " ESTIMATE"
    used_data$obs_lo <- used_data$obs-2*used_data$obs_se
    used_data$obs_hi <- used_data$obs+2*used_data$obs_se
    rr_summaries <- rr_summaries[pred_lo<0, pred_lo := 0]
    
    
    plot <- ggplot(data=rr_summaries, aes(x=study_id, y=pred, ymin=pred_lo, ymax=pred_hi)) +
      geom_pointrange(color="blue") + 
      geom_pointrange(data=used_data[w==1], aes(x=study_id, y=obs, ymin=obs_lo, ymax=obs_hi)) + 
      geom_pointrange(data=used_data[w==0], aes(x=study_id, y=obs, ymin=obs_lo, ymax=obs_hi), shape=1) + 
      coord_flip() +
      ylab("Proportion") +
      xlab("Study") +
      ggtitle("Proportion community COVID deaths among all COVID deaths") +
      theme_minimal() +
      theme(axis.line=element_line(colour="black")) +
      scale_y_continuous(expand=c(0,0.02), breaks = seq(0,1,0.1), limits=c(0,1)) + 
      guides(fill=FALSE)
    
    plot
    
    ggsave(plot, filename=paste0("FILEPATH", version, "_prop_longterm_care_deaths.pdf"), width = 6, height = 8)
    
    
    if(save_final==1) {
      draws$intercept <- NULL
      final_draws <- reshape(draws, idvar = c("one"), timevar = "draw", direction = "wide")
      setnames(final_draws, paste0("proportion.draw_", c(0:999)), paste0("draw_", c(0:999)))
      final_draws[1:5,1:5]
      
      write.csv(final_draws, file =paste0("FILEPATH", "final_prop_longterm_care_deaths_draws.csv"))
    }
    