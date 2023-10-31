##########################################################################################################################################
## Purpose: Apply trachoma crosswalks
## Created by: USERNAME
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
cause_name <- "DVB_TRA_"
main_dir <- "FILEPATH"



## CREATE A RATIO PREDICTION FOR EACH OBSERVATION IN THE ORIGINAL DATA 
#########################################################################################################################################
#Prep original data
actual_data <- as.data.table(read.csv(paste0("FILEPATH")))

# Drop data that is not population representative
actual_data[cv_not_pop_rep == 1, `:=` (is_outlier=1)]
actual_data <- actual_data[group_review==1 | is.na(group_review), ]
actual_data <- unique(actual_data)


#Logit transform original data
actual_data$mean_logit <- logit(actual_data$mean)
actual_data$se_logit <- sapply(1:nrow(actual_data), function(i) {
  mean_i <- actual_data[i, mean]
  se_i <- actual_data[i, standard_error]
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

#Create dataframe for predictions 
#Trachoma blindness
predicted <- data.frame("cv_diag_loss" = c(1,0,1), "cv_best_corrected" = c(0,1,1), "Y_mean" = c(0.0646672,-0.14671365,-0.08178719), 
                        "Y_mean_lo" = c(-0.03315111,-0.18422112,-0.18258690), "Y_mean_hi"= c(0.15305595,-0.10810137,0.01198098))

#Trachoma low
predicted <- data.frame("cv_diag_loss" = c(1,0,1), "cv_best_corrected" = c(0,1,1), "Y_mean" = c(-0.08005201,-1.08910220,-1.15758465), 
                        "Y_mean_lo" = c(-1.307250,-2.326556,-2.394829), "Y_mean_hi"= c(1.1838163,0.1837027,0.1146925))


##: APPLY RATIOS TO THE ORIGINAL DATA AND CREATE THE FINAL DATASET USED FOR NONFATAL MODELING
#########################################################################################################################################

predicted <- as.data.table(predicted)

names(predicted) <- gsub("model_summaries.", "", names(predicted))
names(predicted) <- gsub("X_d_", "cv_", names(predicted))
predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
predicted[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2)))]

# If low vision
a <- (deltamethod(~exp(x1)/(1+exp(x1)),-0.08005201 ,0.6354878^2))
b <- (deltamethod(~exp(x1)/(1+exp(x1)),-1.08910220 , 0.6403839^2))
c <- (deltamethod(~exp(x1)/(1+exp(x1)), -1.15758465, 0.6401958^2))

# If blindness
a <- (deltamethod(~exp(x1)/(1+exp(x1)),0.06466720, 0.04750267^2))
b <- (deltamethod(~exp(x1)/(1+exp(x1)),-0.14671365, 0.01941866^2))
c <- (deltamethod(~exp(x1)/(1+exp(x1)), -0.08178719, 0.04963558^2))

Y_se_norm <- c(a,b,c)
predicted <- cbind(predicted, Y_se_norm)

pred0 <- data.frame("cv_diag_loss" = 0, "cv_best_corrected"=0, "Y_mean"=0, "Y_mean_lo"=0, "Y_mean_hi"=0, "Y_se"=0, "Y_se_norm"=0)
predicted <- rbind(predicted, pred0)

review_sheet_final <- merge(actual_data, predicted, by=c("cv_diag_loss", "cv_best_corrected"))
review_sheet_final <-as.data.table(review_sheet_final)

setnames(review_sheet_final, "mean", "mean_orig")
review_sheet_final[Y_mean != predicted[4,Y_mean], `:=` (mean_logit = mean_logit - Y_mean, se_logit = sqrt(se_logit^2 + Y_se^2))]
review_sheet_final[Y_mean != predicted[4,Y_mean], `:=` (mean_new = inv.logit(mean_logit), standard_error_new = deltamethod(~exp(x1)/(1+exp(x1)), mean_logit, se_logit^2)), by = c("mean_logit", "se_logit")]
review_sheet_final[Y_mean != predicted[4,Y_mean], `:=` (lower_new = NA, upper_new = NA)]  
review_sheet_final[Y_mean == predicted[4,Y_mean], `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
review_sheet_final[standard_error_new == "NaN", `:=` (standard_error_new = sqrt(standard_error^2 + Y_se_norm^2))]
review_sheet_final[, (c("Y_mean", "Y_se", "mean_logit", "se_logit", "Y_se_norm")) := NULL]

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



## Plot predictions vs original data ----------------------------------------------------------------------------------------------------------------------------------
library(ggplot2)

review_sheet_final$covariate <- ifelse(review_sheet_final$cv_best_corrected==1 & review_sheet_final$cv_diag_loss==1,1, 
                                       ifelse(review_sheet_final$cv_diag_loss==1,2, ifelse(review_sheet_final$cv_best_corrected==1,3,4))) 

review_sheet_final$covariate <- as.character(review_sheet_final$covariate)


gg_scatter <- ggplot(review_sheet_final, aes(x=mean_orig, y=mean, color=covariate)) + geom_point(size=2.5) +
  geom_abline(slope=1, intercept = 0) +
  xlim(0,0.9) + ylim(0,0.9) +
  ggtitle("Adjusted Means Vs Unadjusted Means") +
  theme_classic()

gg_scatter
 