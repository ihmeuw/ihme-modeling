#############################################################################
## This code prepares the scalar used for fatal etiology PAFs for diarrhea;
## these scalars are based on the frequency of the pathogens in moderate-to-
## severe diarrhea (MSD) compared to all diarrhea. This is a proposed change
## to how this value was calculated in the past. In previous rounds of GBD,
## we leveraged a DisMod crosswalk value for cv_inpatient where we assumed
## that hospitalized diarrhea was representative of fatal diarrhea (in pathogen
## composition). For GBD 2019, we have done all crosswalks outside of DisMod,\
## mainly in MR-BRT, and we determined that using MSD from the individual-level
## data sources (GEMS1a and MALED) might be a better representation of fatal
## diarrhea than hospitalized episodes. Partly this is because there are pretty
## few hospitalizations in those two data sources and partly because children
## may be hospitalized for a variety of reasons that are not associated with
## diarrhea severity (but maybe the risk of death due to diarrhea) like 
## malnutrition, comorbid illness, or other reasons. 
### This code runs the same MR-BRT
## analysis as was used for the cv_inpatient crosswalk (that one has the dual
## purpose of adjusting hospitalized diarrhea proportions, still necessary
## for DisMod modeling) except that from the individual level data we are using
## the relative proportions in MSD compared to all diarrhea. 
############################################################################

# Prepare your needed functions and information
library(metafor)
library(msm)
library(openxlsx)
library(plyr)
library(boot)
library(data.table)
library(ggplot2)
source() # filepaths to central functions for data upload/download
source() # filepaths to central functions for MR-BRT
source("/filepath/bundle_crosswalk_collapse.R")
source("/filepath/age_split_mrbrt_weights.R")
source("/filepath/sex_split_mrbrt_weights.R")

locs <- read.csv("filepath/ihme_loc_metadata_2019.csv")

eti_info <- read.csv("filepath/eti_rr_me_ids.csv")
eti_info <- subset(eti_info, model_source=="dismod" & rei_id!=183 & cause_id==302)

##############################################################################################
## Set the type, must be "log" or "logit"
xw_transform <- "logit"

pdf("filepath", height=8, width=9)
out_res <- data.frame()
## Build loop here:
for(i in 1:11){
  name <- eti_info$name_colloquial[i]
  me_name <- eti_info$modelable_entity[i]
  me_id <- eti_info$modelable_entity_id[i]
  bundle <- eti_info$bundle_id[i]
  rei_name <- eti_info$rei_name[i]
  
  print(paste0("Modeling an MR-BRT crosswalk for ", name))
  
  df <- read.csv("filepath")
  
  # Keep track of the original mean value
    df$raw_mean <- df$mean
    df$raw_standard_error <- df$standard_error
  
  # Keep if year > 1985
    df <- subset(df, year_start >= 1985)
  
  ##-------------------------------------------------------------------------------------------------
  #### Inpatient sample population ####
    df$inpatient <- ifelse(df$cv_inpatient==1,1,ifelse(df$cv_inpatient_sample==1,1,0))
    df$inpatient[is.na(df$inpatient)] <- 0
    df$is_reference <- ifelse(df$inpatient==1,0,1)
    
    cv_inpatient <- bundle_crosswalk_collapse(df, 
                                              covariate_name="inpatient", 
                                              age_cut=c(0,1,5,20,40,60,80,100), 
                                              year_cut=c(seq(1980,2015,5),2019), 
                                              merge_type="within", 
                                              location_match="exact", 
                                              include_logit = T)
  
  # Rbind the GEMS1A and MALED results.  Data are prepared in a file that uses Mod to Severe / all diarrhea from GEMS1A and MALED
    g1m <- read.csv("filepath")
  
    g1m$sex <- "Both"
  
  # GEMS-1A was already extracted for inpatient, non-inpatient but uses MSD/LSD as its comparisons for cv_inpatient, which is not consistent with the crosswalk
  # So I am going to pull out those data for the actual crosswalk
    cv_inpatient <- subset(cv_inpatient, !(nid %in% c(224856,224855,224854,224849,224848,223566,224853)))
    
    cv_inpatient <- rbind(cv_inpatient, g1m)
  
  # Drop if the ratio is 1, likely this is an error in extraction.
    cv_inpatient <- subset(cv_inpatient, ratio!= 1)
  
  # Can be duplicated if there is an age split
    cv_inpatient <- cv_inpatient[!duplicated(cv_inpatient$ratio),]

  # Test trimming aggressiveness based on number of observations
    trim_pct <- 0.1
  
  # After age-splitting, we might have multiple rows of same values
    cv_inpatient$unique <- paste0(cv_inpatient$nid, "_", round(cv_inpatient$ratio,5))
    cv_inpatient <- subset(cv_inpatient, !duplicated(unique))
  
  # Now doing the crosswalks in logit space #
    if(xw_transform == "log"){
      cv_inpatient$response_var <- cv_inpatient$log_ratio
      cv_inpatient$response_se <- cv_inpatient$delta_log_se
    } else {
      cv_inpatient$response_var <- cv_inpatient$logit_ratio
      cv_inpatient$response_se <- cv_inpatient$logit_ratio_se
    }
    
  # Including NID as a study id
    fit1 <- run_mr_brt(
      output_dir = "filepath",
      model_label = "cv_msd",
      data = cv_inpatient,
      mean_var = "response_var",
      se_var = "response_se",
      study_id = "nid",
      trim_pct = trim_pct,
      method = "trim_maxL",
      overwrite_previous = TRUE
    )
  
    check_for_outputs(fit1)
    df_pred <- data.frame(intercept = 1)
    pred1 <- predict_mr_brt(fit1, newdata = df_pred)
    check_for_preds(pred1)
    pred_object <- load_mr_brt_preds(pred1)
    preds <- pred_object$model_summaries
  
  # Calculate log se
    preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
  # Convert the mean and standard_error to linear space
    if(xw_transform == "log"){
      preds$linear_se <- deltamethod(~exp(x1), preds$Y_mean, preds$se^2)
      preds$ratio <- exp(preds$Y_mean)
      preds$transformation <- "log"
    } else {
      preds$linear_se <- deltamethod(~exp(x1)/(1+exp(x1)), preds$Y_mean, preds$se^2)
      preds$ratio <- inv.logit(preds$Y_mean)
      preds$transformation <- "logit"
    }
  
  ## produce approximation of a funnel plot
    mod_data <- fit1$train_data
    mod_data$outlier <- ceiling(abs(mod_data$w - 1))
    mod_data$location_id <- mod_data$location_match
    mod_data$row_num <- 1:length(mod_data$mean)
    mod_data <- join(mod_data, locs[,c("location_id","location_name")], by="location_id")
    f <- ggplot(mod_data, aes(x=response_var, y=response_se, col=factor(outlier))) + geom_point(size=3) + 
      geom_vline(xintercept=preds$Y_mean) + scale_y_reverse("Standard error") +
      theme_bw() + scale_x_continuous("Transformed ratio") + scale_color_manual("", labels=c("Used","Trimmed"), values=c("#0066CC","#CC0000")) + 
      ggtitle(paste0(name, " inpatient ratio")) +
      geom_vline(xintercept=preds$Y_mean_lo, lty=2) + geom_vline(xintercept=preds$Y_mean_hi, lty=2)
    print(f)
  ## Create essentially a forest plot
    mod_data$label <- with(mod_data, paste0(nid,"_",location_name,"_",age_start,"-",age_end,"_",year_start,"-",year_end,"_",sex,"_",row_num))
    f <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=response_var + response_se*1.96, ymin=response_var - response_se*1.96)) + 
      geom_point(aes(y=response_var, x=label)) + geom_errorbar(aes(x=label), width=0) +
      theme_bw() + ylab("Transformed ratio") + xlab("") + coord_flip() + ggtitle(paste0(name, " inpatient ratio (",round(preds$ratio,3),")")) +
      geom_hline(yintercept=0) + geom_hline(yintercept=preds$Y_mean, col="purple") +
      geom_rect(data=preds, aes(ymin=Y_mean_lo, ymax=Y_mean_hi, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
    print(f)
  
  # Keep a record of the values
    p <- preds[,c("ratio","linear_se","Y_mean","Y_mean_lo","Y_mean_hi","transformation")]
    p$etiology <- name
    p$rei_name <- rei_name
    p$variable <- "cv_msd"
    p$count_obs <- length(cv_inpatient$ratio)
    out_res <- rbind(out_res, p)

}

dev.off()

## Save the summary file of all the crosswalks
setnames(out_res, "variable","crosswalk_name")
write.csv(out_res, "filepath")

