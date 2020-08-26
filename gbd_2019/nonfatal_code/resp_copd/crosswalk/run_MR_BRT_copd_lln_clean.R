##########################################################################################################################################
## Purpose: Prep and run MR-BRT 
## Created by: USERNAME, column re-arrangement based on script by USERNAME
## Date: March 2019
## 
## Step 1: Create master file of all crosswalks for a given cause - keep and standardize order of needed columns, bind together
## Step 2: Run MR-BRT
##
#########################################################################################################################################

rm(list=ls())


cause_path <- "FILEPATH"
cause_name <- "COPD_"
main_dir <- "FILEPATH"
crosswalk <- "LLN/GOLD"
 
filepath <- "FILEPATH" #this is the filepath for the most up-to-date dataset to be used for applying crosswalk
  
##Load csv files with crosswalk pairs for each covariate to be included in network analysis
#LLN pre and post:
df1 <- as.data.table(read.csv(paste0("FILEPATH")))
df2 <- as.data.table(read.csv(paste0("FILEPATH")))
df3 <- as.data.table(read.csv(paste0("FILEPATH")))

df_vector <- list(df1, df2, df3)

#Create function to order columns of crosswalk csv files consistently and drop unneeded columns

reorder_columns <- function(datasheet){
                                                                                                             
    ## set ordered list of columns for master crosswalk csv
    template_cols <- c("nid", "location_match", "region_name.x", "super_region_name.x", "age_start", "age_end", "sex", "year_start", "year_end", 
                       "ratio", "se", "log_ratio", "delta_log_se", "diff_logit", "se_diff_logit", "cv_gold_post.x", "cv_lln_pre.x",
                       "cv_lln_post.x", "cv_lln_pre.y", "cv_lln_post.y")
                         
                                                                                                             
    col_order <- template_cols
                                                                                                             
    ## find which column names are in the extraction template but not in your datasheet
    to_fill_blank <- c()
        for(column in col_order){
          if(!column %in% names(datasheet)){
            to_fill_blank <- c(to_fill_blank, column)
          }
       }
    
     ## create blank column which will be filled in for columns not in your datasheet
     len <- length(datasheet$nid.x)
     blank_col <- rep.int(NA,len)
    
     ## for each column not found in your datasheet, add a blank column and rename it appropriately
     for(column in to_fill_blank){
        datasheet <- cbind(datasheet,blank_col)
        names(datasheet)[names(datasheet)=="blank_col"]<-column
      }

    ## for columns in datasheet but not in epi template or cv list, delete
    dt_cols <- names(datasheet)
    datasheet <- as.data.table(datasheet)
      for(col in dt_cols){
        if(!(col %in% col_order)){
          datasheet[, c(col):=NULL]
        }
    }
                                                                                                             
   ## reorder columns with template columns
    setcolorder(datasheet, col_order)

    ## return
    return(datasheet)   
}    
    
#Create master file with all crosswalks for cause, remove duplicate rows, write csv
master_xwalk <- lapply(df_vector, reorder_columns) %>% rbindlist()

master_xwalk <- unique(master_xwalk)

#Create matrix for mr-brt
master_xwalk$cv_lln_pre <- master_xwalk$cv_lln_pre.y-master_xwalk$cv_lln_pre.x
master_xwalk$cv_lln_post <- master_xwalk$cv_lln_post.y - master_xwalk$cv_lln_post.x 
master_xwalk$cv_gold_post <- master_xwalk$cv_gold_post.x

#Add study ID
master_xwalk[, id := .GRP, by = c("nid", "location_match")]

write.csv(master_xwalk, paste0("FILEPATH"), row.names = F)


#########################################################################################################################################
## Link with launching and loading an MR-BRT model ##
                                                                                                           
library(dplyr)
library(data.table)
library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")
library(readxl)


repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
                                                                                                          

#Run MR-BRT model

 fit1 <- run_mr_brt(
   output_dir = paste0("FILEPATH"),
   model_label = paste0(cause_name, "master_xwalk_", covariate),
   data = paste0("FILEPATH"),
   mean_var = "diff_logit",
   se_var = "se_diff_logit",
   overwrite_previous = TRUE,
   remove_x_intercept = FALSE,
   method = "trim_maxL",
   trim_pct = 0.10,
   study_id = "id",
   lasso=FALSE,
   covs = list(   
     cov_info("cv_lln_pre", "X"),
     cov_info("cv_lln_post", "X"),
     cov_info("midage", "X", degree = 3, n_i_knots = 4, r_linear = T, l_linear = T)
 ))

 
 ## CREATE A RATIO PREDICTION FOR EACH OBSERVATION IN THE ORIGINAL DATA 
 #########################################################################################################################################
 #Prep original data
 actual_data <- as.data.table(read.csv("FILEPATH"))
 
 #Logit transform original data
 actual_data$mean_logit <- logit(actual_data$mean_orig)
 actual_data$se_logit <- sapply(1:nrow(actual_data), function(i) {
   mean_i <- actual_data[i, mean_orig]
   se_i <- actual_data[i, standard_error]
   deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
 })
 
 actual_data$midage <- (actual_data$age_start +actual_data$age_end)/2

 #Predict MR-BRT --------------------------------------------------------------------------------------------------------------
 # Check for outputs from model
 check_for_outputs(fit1, wait_seconds = 15)
 
 # Read raw outputs from model
 results1 <- load_mr_brt_outputs(fit1)
 
 names(results1)
 coefs <- results1$model_coefs
 metadata <- results1$input_metadata
 train <- results1$train_data
 
 pred_midage <- actual_data[(lln_pre==1 | lln_post == 0), ]
 pred_midage <- pred_midage[midage]
 df_pred <- expand.grid("cv_lln_pre"=c(0,1), "cv_lln_post"=c(1,0), midage=unique(pred_midage))
 
 pred <- predict_mr_brt(fit1, newdata = df_pred, z_newdata = df_pred, write_draws = T)
 check_for_preds(pred)
 pred_object <- load_mr_brt_preds(pred)
 predicted <- pred_object$model_summaries

 setnames(predicted, "X_cv_lln_pre", "cv_lln_pre")
 setnames(predicted, "X_cv_lln_post", "cv_lln_post")
 setnames(predicted, "X_midage", "midage")
 
 ##: APPLY RATIOS TO THE ORIGINAL DATA AND CREATE THE FINAL DATASET USED FOR NONFATAL MODELING
 #########################################################################################################################################
 
 #USING DAMIAN'S CODE
 predicted <- as.data.table(predicted)
 
 
 names(predicted) <- gsub("model_summaries.", "", names(predicted))
 names(predicted) <- gsub("X_d_", "cv_", names(predicted))
 predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
 crosswalk_reporting <- copy(predicted) # for reporting later
 
 predicted[, (c("Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
 
 pred0 <- data.frame("cv_lln_pre" = 0, "cv_lln_post"=0, "midage"=0, "Y_mean"=0, "Y_se"=0)
 predicted <- rbind(predicted, pred0)
 
 review_sheet_final <- merge(actual_data, predicted, by=c("cv_lln_pre", "cv_lln_post", "midage"))
 review_sheet_final <-as.data.table(review_sheet_final)
 
 setnames(review_sheet_final, "mean", "mean_orig")
 review_sheet_final[, `:=` (mean_logit = mean_logit - Y_mean, se_logit = sqrt(se_logit^2 + Y_se^2))]
 review_sheet_final[, `:=` (mean_new = inv.logit(mean_logit), standard_error_new = deltamethod(~exp(x1)/(1+exp(x1)), mean_logit, se_logit^2)), by = c("mean_logit", "se_logit")]
 review_sheet_final[, `:=` (lower_new = NA, upper_new = NA)]  
 #review_sheet_final[, `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
 review_sheet_final[, (c("Y_mean", "Y_se", "log_mean", "log_se", "mean_logit", "se_logit", "midage")) := NULL]
 

 # For upload validation #
 review_sheet_final[is.na(lower), uncertainty_type_value := NA]
 
 #THIS IS THE DATASET THAT WILL BE USED FOR NONFATAL MODELING
 write.csv(review_sheet_final, paste0("FILEPATH"), row.names = F)
 
 
 ## Plot predictions vs original data ----------------------------------------------------------------------------------------------------------------------------------
 library(ggplot2)
 
 review_sheet_final$covariate <- ifelse(review_sheet_final$cv_lln_pre==1,1,
                                                      ifelse(review_sheet_final$cv_lln_post==1,2,3))

 review_sheet_final$covariate <- as.character(review_sheet_final$covariate)
 
 gg_scatter <- ggplot(review_sheet_final, aes(x=mean_orig, y=mean_new, color=covariate)) + geom_point() +
   geom_abline(slope=1, intercept = 0) + facet_wrap(~midage) +
   xlim(0,0.5) + ylim(0,0.5) + coord_fixed() +
   ggtitle("Adjusted Means Vs Unadjusted Means") +
   theme_classic()
 
 gg_scatter 
