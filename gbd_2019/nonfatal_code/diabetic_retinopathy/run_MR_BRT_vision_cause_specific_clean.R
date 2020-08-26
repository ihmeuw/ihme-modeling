##########################################################################################################################################
## Purpose: Apply crosswalks to vision causes
#########################################################################################################################################

library(data.table)
library(dplyr)
library(gtools)
library(plyr)
library(msm, lib.loc = "FILEPATH")
library(readxl)
library(stringr)
library(metafor, lib.loc = "FILEPATH")

rm(list=ls())

cause_path <- "FILEPATH"
cause_name <- "CAUSE"
main_dir <- "FILEPATH"




## CREATE A RATIO PREDICTION FOR EACH OBSERVATION IN THE ORIGINAL DATA
#########################################################################################################################################
#Prep original data
actual_data <- as.data.table(read.csv(paste0("FILEPATH")))

actual_data[is.na(mean) & !is.na(cases) & !is.na(sample_size), mean := cases/sample_size]

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

actual_data <- get_se(actual_data)

# Outlier data that is not population representative, check that all group reviewed rows are removed
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

actual_data$cv_diag_loss <- ifelse(actual_data$cv_diag_loss==1,1,0)

#Blindness - rapid
predicted <- data.frame("cv_diag_loss" = 1, "Y_mean" = 0.0646672,
                        "Y_mean_lo" = -0.03315111, "Y_mean_hi"= 0.15305595)

#Low vision - rapid
predicted <- data.frame("cv_diag_loss" = 1, "Y_mean" = 0.1432203,
                        "Y_mean_lo" = -1.27434, "Y_mean_hi"= 1.47746)



##: APPLY RATIOS TO THE ORIGINAL DATA AND CREATE THE FINAL DATASET USED FOR NONFATAL MODELING
#########################################################################################################################################

predicted <- as.data.table(predicted)

names(predicted) <- gsub("model_summaries.", "", names(predicted))
names(predicted) <- gsub("X_d_", "cv_", names(predicted))
predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]

pred0 <- data.frame("cv_diag_loss" = 0, "Y_mean"=0, "Y_mean_lo"=0, "Y_mean_hi"=0, "Y_se"=0, "Y_se_norm"=0)
predicted <- rbind(predicted, pred0)

crosswalk_reporting <- copy(predicted) # for reporting later

review_sheet_final <- merge(actual_data, predicted, by=c("cv_diag_loss"))
review_sheet_final <-as.data.table(review_sheet_final)


setnames(review_sheet_final, "mean", "mean_orig")
review_sheet_final[Y_mean != predicted[2,Y_mean], `:=` (mean_logit = mean_logit - Y_mean, se_logit = sqrt(se_logit^2 + Y_se^2))]
review_sheet_final[Y_mean != predicted[2,Y_mean], `:=` (mean_new = inv.logit(mean_logit), standard_error_new = deltamethod(~exp(x1)/(1+exp(x1)), mean_logit, se_logit^2)), by = c("mean_logit", "se_logit")]
review_sheet_final[Y_mean != predicted[2,Y_mean], `:=` (lower_new = NA, upper_new = NA)]
review_sheet_final[Y_mean == predicted[2,Y_mean], `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
review_sheet_final[standard_error_new == "NaN", `:=` (standard_error_new = sqrt(standard_error^2 + Y_se_norm^2))]
review_sheet_final[, (c("Y_mean", "Y_se", "mean_logit", "se_logit", "Y_se_norm")) := NULL]

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


## Plot predictions vs original data ----------------------------------------------------------------------------------------------------------------------------------
library(ggplot2)

review_sheet_final$cv_diag_loss <- as.character(review_sheet_final$cv_diag_loss)

gg_scatter <- ggplot(review_sheet_final, aes(x=mean_orig, y=mean, color=cv_diag_loss)) + geom_point() +
  geom_abline(slope=1, intercept = 0) +
  xlim(0,.02) + ylim(0,0.02) +
  ggtitle("Adjusted Means Vs Unadjusted Means") +
  labs(color="Adjustments") +
  scale_color_manual(labels = c("NON-RAAB", "RAAB"), values = c("blue", "red")) +
  theme_classic()

pdf(paste0(main_dir, cause_path, cause_name, "crosswalk_graph.pdf"))
gg_scatter
dev.off()


