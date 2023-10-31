predict_xw <- function(choice_fit = NULL, lhs, raw_data = NULL, old_model_directory = NULL) {

    fit <- copy(choice_fit)
    
    eval(parse(text = paste0("predicted <- expand.grid(", paste0(cv_alts, "=c(0, 1)", collapse = ", "), ")")))
    
    if (is.null(choice_fit)) {
      predicted <- as.data.table(read.csv(paste0(old_model_directory, "model_summaries.csv")))
      
    } else {
      
      if (is.null(raw_data)) {
        predicted <- as.data.table(predict_mr_brt(fit)["model_summaries"])
      } else {
        predicted <- as.data.table(predict_mr_brt(fit, newdata = raw_data)["model_summaries"])
      }
      
    }
    
    names(predicted) <- gsub("model_summaries.", "", names(predicted))
    names(predicted) <- gsub("X_cv_", "cv_", names(predicted))
    predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
   
    if (lhs=="lratio") {
      predicted[, (c("Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
    } else if (lhs=="logit_dif") {
      predicted[, adj_logit:= Y_mean]
      predicted[, se_adj_logit:= Y_se]
      predicted[, (c("Y_mean", "Y_se", "Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
    }
    
    return(predicted)
    
}
    
transform_altdt <- function(raw_data, predicted, lhs) {

    crosswalked_data <- merge(raw_data, predicted, by=cv_alts)
    
    #How you transform the data if ratio of log was modeled
    if (lhs=="lratio") {
      
      crosswalked_data[, `:=` (log_mean = log(mean), log_se = deltamethod(~log(x1), mean, standard_error^2)), by = c("mean", "standard_error")]
      crosswalked_data[Y_mean != predicted[1,Y_mean], `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
      crosswalked_data[Y_mean != predicted[1,Y_mean], `:=` (mean = exp(log_mean), standard_error = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]
    
      crosswalked_data[Y_mean != predicted[1,Y_mean], `:=` (cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA)]
      crosswalked_data[, (c("Y_mean", "Y_se", "log_mean", "log_se")) := NULL]
      
      #How you transform the data if difference of logits was modeled
      } else if (lhs=="logit_dif") { 
        
        #Transform original data's mean and standard deviation (for prevalence, usually) into logit space
        crosswalked_data[, o_mean_logit:=log(mean/(1-mean))]
        crosswalked_data$o_se_logit <- sapply(1:nrow(crosswalked_data), function(i) {
          mean_i <- crosswalked_data[i, mean]
          standard_error_i <- crosswalked_data[i, standard_error]
          deltamethod(~log(x1/(1-x1)), mean_i, standard_error_i^2)
        })
        
        #Subtract the adj_logit from the o_mean_logit
        crosswalked_data[ , a_mean_logit:=o_mean_logit-adj_logit]
        #Calculate the standard error for the adjusted logit mean
        crosswalked_data[ , a_se_logit := sqrt(o_se_logit^2 + se_adj_logit^2)]
        
        #Transform the adjusted logit mean and se back to normal space
        crosswalked_data[, a_mean:= exp(a_mean_logit)/(1+exp(a_mean_logit))]
        
        crosswalked_data$a_standard_error <- sapply(1:nrow(crosswalked_data), function(i) {
          a_mean_logit_i <- crosswalked_data[i, a_mean_logit]
          a_se_logit_i <- crosswalked_data[i, a_se_logit]
          deltamethod(~exp(x1)/(1+exp(x1)), a_mean_logit_i, a_se_logit_i^2)
        })
       
        crosswalked_data[, `:=` (mean = a_mean, standard_error = a_standard_error, cases = NA, lower = NA, upper = NA, uncertainty_type_value = NA, input_type = "adjusted", uncertainty_type = NA)]
        crosswalked_data[ , c("adj_logit", "se_adj_logit", "o_mean_logit", "o_se_logit", "a_mean_logit", "a_se_logit", "a_mean", "a_standard_error"):=NULL]
        
    }
    
    if ("note_modeler" %in% names(crosswalked_data)) {
      crosswalked_data[ , note_modeler := paste0(note_modeler, "|row adjusted for non-reference study design with MR-BRT coefficient(s)")]
    } else {
      crosswalked_data[ , note_modeler := "row adjusted for non-reference study design with MR-BRT coefficient(s)"]
    }
    
  return(crosswalked_data)
  
}

