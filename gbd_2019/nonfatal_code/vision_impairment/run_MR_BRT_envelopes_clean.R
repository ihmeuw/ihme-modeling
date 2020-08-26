##########################################################################################################################################
## Purpose: Prep and run MR-BRT
##
## Step 1: Create master file of all crosswalks for a given cause - keep and standardize order of needed columns, bind together
## Step 2: Run MR-BRT
##
#########################################################################################################################################

library(data.table)
library(dplyr)
library(plyr)
library(msm, lib.loc = "FILEPATH")
library(readxl)
library(stringr)
library(metafor, lib.loc = "FILEPATH")

rm(list=ls())

cause_path <- "FILEPATH"
cause_name <- "ENV"
main_dir <- "FILEPATH"


##Load csv files with crosswalk pairs for each covariate to be included in network analysis

df1 <- as.data.table(read.csv(paste0("FILEPATH")))
df2 <- as.data.table(read.csv(paste0("FILEPATH")))

#Create function to order columns of crosswalk csv files consistently and drop unneeded columns

df_vector <- list(df1,df2)


reorder_columns <- function(datasheet){

    ## set ordered list of columns for master crosswalk csv
    template_cols <- c("location_match", "sex", "nid.x", "nid.y", "location_name.x", "location_name.y", "year_start", "n_year_start", "year_end",  "n_year_end", "age_start", "n_age_start", "age_end", "n_age_end",
                       "not_rep_new.x", "cv_best_corrected.x", "cv_diag_loss.x", "not_rep_new.y", "cv_best_corrected.y", "cv_diag_loss.y", "log_ratio", "delta_log_se", "diff_logit", "se_diff_logit")

    col_order <- template_cols

    ## find which column names are in the extraction template but not in your datasheet
    to_fill_blank <- c()
       for(column in col_order){
         if(!column %in% names(datasheet)){
           to_fill_blank <- c(to_fill_blank, column)
         }
      }

    ## create blank column which will be filled in for columns not in your datasheet
    len <- length(datasheet$sex)
    blank_col <- rep.int(NA,len)

    ## for each column not found in your datasheet, add a blank column and rename it appropriately
    for(column in to_fill_blank){
       datasheet <- cbind(datasheet,blank_col)
       names(datasheet)[names(datasheet)=="blank_col"]<-column
     }

    ## for columns in datasheet but not in epi template, delete
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
master_xwalk$cv_best_corrected <- master_xwalk$cv_best_corrected.y
master_xwalk$cv_diag_loss <- master_xwalk$cv_diag_loss.y-master_xwalk$cv_diag_loss.x


#Add study ID
master_xwalk[, id := .GRP, by = c("nid.x", "nid.y")]


write.csv(master_xwalk, paste0("FILEPATH"), row.names = F)


#########################################################################################################################################
## Link with launching and loading an MR-BRT model ----------------------------------

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))



## Run MR-BRT ----------------------------------------------------------------------

  ## Including NID as a study id would be idea so obvious between vs within study comparison - could create new column for this
 fit1 <- run_mr_brt(
   output_dir = "FILEPATH",
   model_label = paste0(covariate_name),
   data = paste0("FILEPATH"),
   mean_var = "diff_logit",
   se_var = "se_diff_logit",
   overwrite_previous = TRUE,
   remove_x_intercept = TRUE,
   method = "trim_maxL",
   trim_pct = 0.10,
   study_id = "id",
   covs = list(
     cov_info("cv_diag_loss", "X"),
     cov_info("cv_best_corrected", "X")
   )
 )


## CREATE A RATIO PREDICTION FOR EACH OBSERVATION IN THE ORIGINAL DATA
#########################################################################################################################################
#Prep original data
actual_data <- as.data.table(read.csv("FILEPATH"))

# Drop data that is not population representative
actual_data[not_pop_rep == 1, `:=` (is_outlier=1)]
actual_data <- actual_data[group_review==1 | is.na(group_review), ]
actual_data <- unique(actual_data)

#Logit transform original data
actual_data$mean_logit <- logit(actual_data$mean)
actual_data$se_logit <- sapply(1:nrow(actual_data), function(i) {
  mean_i <- actual_data[i, mean]
  se_i <- actual_data[i, standard_error]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

#Remove any best corrected rows that also have presenting data rows
nid_bc_p <- actual_data[,.N,by="nid,diagcode"][,.N,by="nid"][N == 2, "nid"]
actual_data <- actual_data[!(nid %in% nid_bc_p$nid & diagcode == "ALL-DMILD-B"),]

#Create best corrected covariate
diagcode_p <- "ALL-DMOD-P"
diagcode_b <- "ALL-DMOD-B"
actual_data$cv_best_corrected <- ifelse((actual_data$diagcode==diagcode_b), 1, 0)

cov_names <- c("cv_diag_loss", "cv_best_corrected")


#Predict MR-BRT --------------------------------------------------------------------------------------------------------------
# Check for outputs from model
check_for_outputs(fit1, wait_seconds = 15)

# Read raw outputs from model
results1 <- load_mr_brt_outputs(fit1)

names(results1)
coefs <- results1$model_coefs
metadata <- results1$input_metadata
train <- results1$train_data

df_pred <- data.table("cv_best_corrected"=c(0,1,1,0), "cv_diag_loss"=c(1,0,1,0))

pred <- predict_mr_brt(fit1, newdata = df_pred, z_newdata = df_pred, write_draws = T)
check_for_preds(pred)
pred_object <- load_mr_brt_preds(pred)
predicted <- pred_object$model_summaries

predicted <- predicted[1:3, ]

setnames(predicted, "X_cv_best_corrected", "cv_best_corrected")
setnames(predicted, "X_cv_diag_loss", "cv_diag_loss")

##: APPLY RATIOS TO THE ORIGINAL DATA AND CREATE THE FINAL DATASET USED FOR NONFATAL MODELING
#########################################################################################################################################

#USING DAMIAN'S CODE
predicted <- as.data.table(predicted)

names(predicted) <- gsub("model_summaries.", "", names(predicted))
names(predicted) <- gsub("X_d_", "cv_", names(predicted))
  pred1 <- predicted[1,]
  pred2 <- predicted[2,]
  pred3 <- predicted[3,]
  pred1[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
  pred1<- pred1[,Y_se_norm]
  pred2[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
  pred2<- pred2[,Y_se_norm]
  pred3[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]
  pred3<- pred3[,Y_se_norm]
  Y_se_norm <- c(pred1,pred2,pred3)
  predicted <- cbind(predicted,Y_se_norm)

crosswalk_reporting <- copy(predicted) # for reporting later

predicted[, (c("Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]

pred0 <- data.frame("cv_diag_loss" = 0, "cv_best_corrected"=0, "Y_mean"=0, "Y_se"=0)
predicted <- rbind(predicted, pred0)

review_sheet_final <- merge(actual_data, predicted, by=c("cv_best_corrected", "cv_diag_loss"))
review_sheet_final <-as.data.table(review_sheet_final)

setnames(review_sheet_final, "mean", "mean_orig")
review_sheet_final[Y_mean != predicted[4,Y_mean], `:=` (mean_logit = mean_logit - Y_mean, se_logit = sqrt(se_logit^2 + Y_se^2))]
review_sheet_final[Y_mean != predicted[4,Y_mean], `:=` (mean_new = inv.logit(mean_logit), standard_error_new = deltamethod(~exp(x1)/(1+exp(x1)), mean_logit, se_logit^2)), by = c("mean_logit", "se_logit")]
review_sheet_final[Y_mean != predicted[4,Y_mean], `:=` (lower_new = NA, upper_new = NA)]
review_sheet_final[Y_mean == predicted[4,Y_mean], `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
review_sheet_final[standard_error_new == "NaN", `:=` (standard_error_new = standard_error)]
review_sheet_final[, (c("Y_mean", "Y_se", "mean_logit", "se_logit")) := NULL]

# For upload validation #
review_sheet_final[is.na(lower), uncertainty_type_value := NA]
setnames(review_sheet_final, "lower", "lower_orig")
setnames(review_sheet_final, "upper", "upper_orig")
setnames(review_sheet_final, "standard_error", "standard_error_orig")
setnames(review_sheet_final, "lower_new", "lower")
setnames(review_sheet_final, "upper_new", "upper")
setnames(review_sheet_final, "standard_error_new", "standard_error")
setnames(review_sheet_final, "mean_new", "mean")

#THIS IS THE DATASET THAT WILL BE USED FOR NONFATAL MODELING
write.csv(review_sheet_final, paste0("FILEPATH"), row.names = F)

