# Author
# 3 June 2019
# GBD 2019
# Purpose: Prepping WHO data to identify within-study pairs and compute ratios

cat("\014")
rm(list=ls())

library(data.table)
library(msm)
library(foreign)
pacman::p_load(gtools)

# Load dataset
setwd("FILEPATH")
egdata <- "WHO_IPV_cx_input.csv"
dataset <- data.table(read.csv(egdata))


colnames(dataset)[colnames(dataset)=="prev_GS"] <- "GS_prev"
colnames(dataset)[colnames(dataset)=="n_GS"] <- "GS_n"
colnames(dataset)[colnames(dataset)=="se_GS"] <- "GS_se"

datalong <- reshape(dataset, varying = c("prev_curr", "prev_phys", "prev_sev", "prev_sex",
                                         "n_curr", "n_phys", "n_sev", "n_sex", "se_curr",
                                         "se_phys", "se_sev", "se_sex"),
                    idvar = c("countloc", "agegrp"), ids = 1:NROW(dataset),
                    direction = "long", sep = "_",)

colnames(datalong)[colnames(datalong)=="time"] <- "numerator"

# Logit the study means
data <- datalong
data[,logit_prev:=logit(prev)]
data[,logit_GS_prev:=logit(GS_prev)]

# Calculate the difference
data[,logit_diff:=logit_prev-logit_GS_prev]

# Calculate SE both logit means
message("applying the delta method to derive standard error of the logit-transformed means")
data[,logit_prev_se:=sapply(1:nrow(data), function(i){
  mean_i <- data[i,prev]
  mean_se_i <- data[i,se]
  deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
})]
data[,logit_GS_prev_se:=sapply(1:nrow(data), function(i){
  mean_i <- data[i,GS_prev]
  mean_se_i <- data[i,GS_se]
  deltamethod(~log(x1/(1-x1)),mean_i,mean_se_i^2)
})]

# Assume additive variance to calculate SE of logit diff
data[,logit_diff_se:=sqrt(logit_prev_se^2+logit_GS_prev_se^2)]




# Create dummy variables based on the string variable created earlier
cv_list <- c("curr_15_19", "curr_20_24", "curr_25_34", "curr_35_44", "curr_45plus", "phys", "sev", "sex")
data[, paste0("d_", "curr_15_19") := as.integer(grepl("curr", data$numerator) & agegrp=="15_19")]
data[, paste0("d_", "curr_20_24") := as.integer(grepl("curr", data$numerator) & agegrp=="20_24")]
data[, paste0("d_", "curr_25_34") := as.integer(grepl("curr", data$numerator) & agegrp=="25_34")]
data[, paste0("d_", "curr_35_44") := as.integer(grepl("curr", data$numerator) & agegrp=="35_44")]
data[, paste0("d_", "curr_45plus") := as.integer(grepl("curr", data$numerator) & agegrp=="45plus")]
data[, paste0("d_", "phys") := as.integer(grepl("phys", data$numerator))]
data[, paste0("d_", "sev") := as.integer(grepl("sev", data$numerator))]
data[, paste0("d_", "sex") := as.integer(grepl("sex", data$numerator))]


## Then the final step would be to convert the ratios and se into log-space

data$id = 1:NROW(data)

data2 <- data[data$d_curr_15_19==0 & data$d_curr_20_24==0 & data$d_curr_25_34==0 &
                data$d_curr_35_44==0 & data$d_curr_45plus==0,]
data2 <- subset(data2, select= -c(d_curr_15_19, d_curr_20_24, d_curr_25_34, d_curr_35_44, d_curr_45plus) )
write.csv(data2, "WHO_IPV_cx_input_prepped_logit_phys_sex_sev.csv")


data$d_curr_age[data$agegrp=="15_19"] <- 17.5
data$d_curr_age[data$agegrp=="20_24"] <- 22.5
data$d_curr_age[data$agegrp=="25_34"] <- 30
data$d_curr_age[data$agegrp=="35_44"] <- 40
data$d_curr_age[data$agegrp=="45plus"] <- 50

data2 <- data[data$d_phys==0 & data$d_sev==0 & data$d_sex==0,]
data2 <- subset(data2, select= -c(d_phys, d_sev, d_sex) )

write.csv(data, "WHO_IPV_cx_input_prepped_logit_spline_age.csv")

