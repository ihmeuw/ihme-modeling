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
cause_name <- "asthma_"
main_dir <- "FILEPATH"

##Load csv files with crosswalk pairs for each covariate to be included in network analysis

df1 <- as.data.table(read.csv(paste0("FILEPATH")))
df2 <- as.data.table(read.csv(paste0("FILEPATH")))
df3 <- as.data.table(read.csv(paste0("FILEPATH")))
df4 <- as.data.table(read.csv(paste0("FILEPATH")))
df5 <- as.data.table(read.csv(paste0("FILEPATH")))
df6 <- as.data.table(read.csv(paste0("FILEPATH")))
df7 <- as.data.table(read.csv(paste0("FILEPATH")))

df1 <- as.data.table(read.csv(paste0("FILEPATH")))
df2 <- as.data.table(read.csv(paste0("FILEPATH")))


#add nid.x and nid.y to WHS datasets
df1$nid.x <- df1$nid
df1$nid.y <- df1$nid
df2$nid.x <- df2$nid
df2$nid.y <- df2$nid

df1$cv_self_report_current.x <- 0
df1$cv_self_report_ever.x <- 0
df2$cv_self_report_current.x <- 0
df2$cv_self_report_ever.x <- 0

df1$cv_self_report_current.y <- 0
df1$cv_self_report_ever.y <- 0
df2$cv_self_report_current.y <- 0
df2$cv_self_report_ever.y <- 0

df1$cv_diagnosis.x <- 0
df1$cv_wheezing.x <- 0
df2$cv_diagnosis.x <- 0
df2$cv_wheezing.x <- 0

df1$cv_diagnosis.y <- 0
df1$cv_wheezing.y <- 1
df2$cv_diagnosis.y <- 1
df2$cv_wheezing.y <- 0

#safety check for all covariates

df3$cv_self_report_current.y <- 1
df3$cv_self_report_ever.y <- 0
df3$cv_diagnosis.y <- 0
df3$cv_wheezing.y <- 0

df4$cv_self_report_current.y <- 0
df4$cv_self_report_ever.y <- 1
df4$cv_diagnosis.y <- 0
df4$cv_wheezing.y <- 0

df5$cv_self_report_current.y <- 0
df5$cv_self_report_ever.y <- 0
df5$cv_diagnosis.y <- 0
df5$cv_wheezing.y <- 1

df6$cv_self_report_current.y <- 0
df6$cv_self_report_ever.y <- 0
df6$cv_diagnosis.y <- 1
df6$cv_wheezing.y <- 0

df7$cv_self_report_ever.x <- 1
df7$cv_self_report_current.y <- 0
df7$cv_self_report_ever.y <- 0
df7$cv_diagnosis.y <- 0
df7$cv_wheezing.y <- 1

#Order columns of crosswalk csv files consistently and drop unneeded columns

reorder_columns <- function(datasheet){
                                                                                                             
    ## set ordered list of columns for master crosswalk csv
    template_cols <- c("nid.x", "nid.y", "location_match", "region_name.x", "super_region_name.x", "age_start", "age_end", "sex", "year_start", "year_end", 
                       "mean", "standard_error", "ratio", "se", "log_ratio", "delta_log_se", "diff_logit", "se_diff_logit", "cv_wheezing.x", "cv_diagnosis.x", 
                       "cv_self_report_ever.x", "cv_self_report_current.x",  "cv_wheezing.y", "cv_diagnosis.y", "cv_self_report_ever.y", "cv_self_report_current.y", "cv_gold_std.x")
                                                                                                             
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

df_vector <- list(df1, df2, df3, df4, df5, df6, df7)                                                                                  

master_xwalk <- lapply(df_vector, reorder_columns) %>% rbindlist()

master_xwalk <- unique(master_xwalk)

#Create matrix for mr-brt
master_xwalk$cv_wheezing <- master_xwalk$cv_wheezing.y-master_xwalk$cv_wheezing.x
master_xwalk$cv_diagnosis <- master_xwalk$cv_diagnosis.y-master_xwalk$cv_diagnosis.x
master_xwalk$cv_self_report_current <- master_xwalk$cv_self_report_current.y-master_xwalk$cv_self_report_current.x
master_xwalk$cv_self_report_ever <- master_xwalk$cv_self_report_ever.y - master_xwalk$cv_self_report_ever.x 

#Add study ID
master_xwalk[, id := .GRP, by = c("nid.x", "nid.y")]

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
                                                                                                           

## Plotting function ----------------------------------------------------------------

#Verify model names 
covariate_name <- "master_xwalk"    
cause_path <- "FILEPATH/"
cause_name <- "ILD_"


#Need to use/create column to specify if within study comparison or between

 fit1 <- run_mr_brt(
   output_dir = paste0("FILEPATH"),
   model_label = paste0(cause_name, covariate_name, "log"),
   data = paste0("FILEPATH"),
   mean_var = "log_ratio",
   se_var = "delta_log_se",
   overwrite_previous = TRUE,
   remove_x_intercept = TRUE,
   method = "trim_maxL",
   trim_pct = 0.10,
   study_id = "id",
   #lasso=FALSE,
   covs = list(   
     #cov_info("cv_wheezing", "X"),
     #cov_info("cv_diagnosis", "X"),
     #cov_info("cv_self_report_current", "X"),
     #cov_info("cv_self_report_ever", "X")
     #cov_info("age_start", "X", degree = 3, n_i_knots = 4, r_linear = T, l_linear = T))
     cov_info("cv_IPF.y", "X"),
     cov_info("cv_sarc.y","X"))
 )

 
 plot_mr_brt(fit1)
 
 ## CREATE A RATIO PREDICTION FOR EACH OBSERVATION IN THE ORIGINAL DATA 
 #########################################################################################################################################
 #Prep original data
 actual_data <- as.data.table(read.csv("FILEPATH"))
 
 #Logit transform original data
 actual_data$mean_logit <- logit(actual_data$mean)
 actual_data$se_logit <- sapply(1:nrow(actual_data), function(i) {
   mean_i <- actual_data[i, mean]
   se_i <- actual_data[i, standard_error]
   deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
 })
 
 
 #Predict MR-BRT --------------------------------------------------------------------------------------------------------------
 # Check for outputs from model
 check_for_outputs(fit1, wait_seconds = 15)
 
 # Read raw outputs from model
 results1 <- load_mr_brt_outputs(fit1)
 
 names(results1)
 coefs <- results1$model_coefs
 metadata <- results1$input_metadata
 train <- results1$train_data
 
 df_pred <- data.table("cv_diagnosis"=c(0, 1,0,0,0), "cv_wheezing"=c(0, 0,1,0,0), "cv_self_report_current"=c(0, 0,0,1,0), "cv_self_report_ever"=c(0, 0,0,0,1))
 df_pred <- data.table("cv_IPF.y"=c(1,0), "cv_sarc.y"=c(0,1))
 
 pred <- predict_mr_brt(fit1, newdata = df_pred, z_newdata = df_pred, write_draws = T)
 check_for_preds(pred)
 pred_object <- load_mr_brt_preds(pred)
 predicted <- pred_object$model_summaries
 
 #If already ran MR-Brt - open pred_object
 predicted <- as.data.table(read.csv(paste0("FILEPATH")))
 
 
 predicted <- unique(predicted)
 predicted <- predicted[2:5, ]
 
 
 setnames(predicted, "X_cv_wheezing", "cv_wheezing")
 setnames(predicted, "X_cv_diagnosis", "cv_diagnosis")
 setnames(predicted, "X_cv_self_report_current", "cv_self_report_current")
 setnames(predicted, "X_cv_self_report_ever", "cv_self_report_ever")
 
 setnames(predicted, "X_cv_IPF.y", "cv_IPF")
 setnames(predicted, "X_cv_sarc.y", "cv_sarc")
 
 ##: APPLY RATIOS TO THE ORIGINAL DATA AND CREATE THE FINAL DATASET USED FOR NONFATAL MODELING
 #########################################################################################################################################
 
 #USING DAMIAN'S CODE
 predicted <- as.data.table(predicted)
 
 
 names(predicted) <- gsub("model_summaries.", "", names(predicted))
 names(predicted) <- gsub("X_d_", "cv_", names(predicted))
 predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
 predicted[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
 
 pred1 <- predicted[1,]
 pred2 <- predicted[2,]
 pred3 <- predicted[3,]
 pred4 <- predicted[4,]
 pred1[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
 pred1<- pred1[,Y_se_norm]
 pred2[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
 pred2<- pred2[,Y_se_norm]
 pred3[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
 pred3<- pred3[,Y_se_norm]
 pred4[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
 pred4<- pred4[,Y_se_norm]
 Y_se_norm <- c(pred1,pred2,pred3,pred4)
 predicted <- cbind(predicted,Y_se_norm)
 
 crosswalk_reporting <- copy(predicted) # for reporting later
 
 predicted[, (c("Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
 
 no_cv <- data.frame("cv_wheezing" = 0,"cv_diagnosis" =0, "cv_self_report_current"=0, "cv_self_report_ever"=0, "Y_mean"=0, "Y_se"=0, "Y_se_norm"=0)
 predicted <- rbind(predicted, no_cv)
 
 review_sheet_final <- merge(actual_data, predicted, by=c("cv_wheezing", "cv_diagnosis", "cv_self_report_ever", "cv_self_report_current"))
 review_sheet_final <- merge(actual_data, predicted, by=c("cv_IPF","cv_sarc"))
 review_sheet_final <-as.data.table(review_sheet_final)

 setnames(review_sheet_final, "mean", "mean_orig")
 review_sheet_final[, `:=` (log_mean = log(mean_orig), log_se = deltamethod(~log(x1), mean_orig, standard_error^2)), by = c("mean_orig", "standard_error")]
 review_sheet_final[Y_mean != predicted[3,Y_mean], `:=` (log_mean = log_mean - Y_mean, log_se = sqrt(log_se^2 + Y_se^2))]
 review_sheet_final[Y_mean != predicted[3,Y_mean], `:=` (mean_new = exp(log_mean), standard_error_new = deltamethod(~exp(x1), log_mean, log_se^2)), by = c("log_mean", "log_se")]
 review_sheet_final[Y_mean != predicted[3,Y_mean], `:=` (cases_new = NA, lower_new = NA, upper_new = NA)]
 review_sheet_final[Y_mean == predicted[3,Y_mean], `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
 review_sheet_final[standard_error_new == "NA", `:=` (standard_error_new = standard_error)]
 review_sheet_final[, (c("Y_mean", "Y_se", "log_mean", "log_se")) := NULL]
 
 setnames(review_sheet_final, "mean", "mean_orig")
 review_sheet_final[Y_mean != predicted[5,Y_mean], `:=` (mean_logit = mean_logit - Y_mean, se_logit = sqrt(se_logit^2 + Y_se^2))]
 review_sheet_final[Y_mean != predicted[5,Y_mean], `:=` (mean_new = inv.logit(mean_logit), standard_error_new = deltamethod(~exp(x1)/(1+exp(x1)), mean_logit, se_logit^2)), by = c("mean_logit", "se_logit")]
 review_sheet_final[Y_mean != predicted[5,Y_mean], `:=` (lower_new = NA, upper_new = NA)]  
 review_sheet_final[Y_mean == predicted[5,Y_mean], `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
 review_sheet_final[standard_error_new == "NA", `:=` (standard_error_new = sqrt(standard_error^2 + Y_se_norm^2))]
 review_sheet_final[, (c("Y_mean", "Y_se", "mean_logit", "se_logit")) := NULL]
 

 # For upload validation #
 setnames(review_sheet_final, "lower", "lower_orig")
 setnames(review_sheet_final, "upper", "upper_orig")
 setnames(review_sheet_final, "standard_error", "standard_error_orig")
 setnames(review_sheet_final, "lower_new", "lower")
 setnames(review_sheet_final, "upper_new", "upper")
 setnames(review_sheet_final, "standard_error_new", "standard_error")
 setnames(review_sheet_final, "mean_new", "mean") 
 review_sheet_final[is.na(lower), uncertainty_type_value := NA]
 
 #THIS IS THE DATASET THAT WILL BE USED FOR NONFATAL MODELING
 write.csv(review_sheet_final, paste0("FILEPATH"), row.names = F)
 
 