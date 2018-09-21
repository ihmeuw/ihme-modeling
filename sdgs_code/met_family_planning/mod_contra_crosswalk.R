
#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Run a linear model to inform the prior of ST-GPR for modern contraceptive usage
#********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory, only if running locally - otherwise you want the df from the launcher to be passed through from global namespace
if (Sys.info()["sysname"] == "Windows") {
  rm(list=ls())
  
  # disable scientific notation
  options(scipen = 999)
}
# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  
} else { 
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

# set control flow arguments
run.interactively <- FALSE

# load packages
library(plyr)
library(foreign)
library(splines)
library(boot)
library(reshape2)
library(data.table)
library(stats)
library(lme4)
library(ggplot2)
#********************************************************************************************************************************

#----PREP------------------------------------------------------------------------------------------------------------------------

# Read in your model data if you are working interactively
df <- fread(paste0(j_root, "FILEPATH"))

# calculate variance
df[, variance := standard_error**2]
df[, raw_data := copy(data)]
df[, raw_variance := copy(variance)]

# offset unrealistic data
df[data == 0, data := 0.001]
df[data == 1, data := 0.999]

# transform data and variance to logit space
df[, variance := variance * (1/(data*(1-data)))^2]
df[, data := logit(data)]

# load and compile collapsed counterfactual extraction data
topics <- c("currmar","evermar")
id.vars <- c("nid","ihme_loc_id","age_group_id")
for (topic in topics) {
  df2 <- fread(paste0(j_root, "FILEPATH"))
  df2[,age_group_id := (age_start/5) + 5]
  df2 <- df2[var == "mod_contra" & sample_size > 19,c(id.vars,"mean"),with=F]
  df2[mean == 0, mean := 0.001]
  df2[mean == 1, mean := 0.999]
  df2[,mean := logit(mean)]
  setnames(df2,"mean",topic)
  df <- merge(df,df2,by=id.vars,all.x=T)
}

#---LINEAR MODEL-----------------------------------------------------------------------------------------------------------------------

# run age-specific linear regressions to determine effect of each counterfactual
# (no need to include any covariates since they are re-extractions of the same data)
coeffs <- data.table()
for (topic in topics) {
  counter <- df[!is.na(get(topic)),c(id.vars,"data",topic),with=F]
  counter <- melt(counter,id.vars=id.vars)
  counter[,cv := ifelse(variable=="data",0,1)]
  
  for (age in unique(counter$age_group_id)){
    mod <- lm(data = counter[age_group_id == age], value ~ cv)
    temp <- as.data.table(coef(summary(mod)), keep.rownames = T)
    temp <- temp[rn == "cv"]
    temp[,rn := paste0(topic,":",age)]
    coeffs <- rbind(coeffs,temp,fill=T)
  }
}

#----CROSSWALKING DATA-------------------------------------------------------------------------------------------------------------


# We will first crosswalk our data based on the results of the regressions
# Then, adjust the variance of datapoints, given that our adjusted data is less certain subject to the variance of the regression 

for (this.age in unique(df$age_group_id)) {
  
  cat("Adjusting points in for age group", this.age, "\n"); flush.console()
  
  # crosswalk coefficients and standard errors
  currmar_coeff <- as.numeric(coeffs[rn == paste0("currmar:",this.age), "Estimate", with=F])
  evermar_coeff <- as.numeric(coeffs[rn == paste0("evermar:",this.age), "Estimate", with=F])
  currmar_se <- as.numeric(coeffs[rn == paste0("currmar:",this.age), "Std. Error", with=F])
  evermar_se <- as.numeric(coeffs[rn == paste0("evermar:",this.age), "Std. Error", with=F])
  
  #adjust data and variance
  df[age_group_id == this.age, data := data - (currmar_only * currmar_coeff)]
  df[age_group_id == this.age, data := data - (evermar_only * evermar_coeff)]
  df[, variance := variance + (currmar_only^2 * currmar_se^2)]
  df[, variance := variance + (evermar_only^2 * evermar_se^2)]
}

# First reset all study level covariates to predict as the gold standard
# Also save the originals for comparison
df[, currmar_og := currmar_only]
df[, currmar_only := 0]
df[, evermar_og := evermar_only]
df[, evermar_only := 0]

#transform data and variance back to normal space
df[, "data" := inv.logit(data)]
df[, "variance" := variance / (1/(data*(1-data)))^2] # using reverse delta method for logspace

# formatting
df[,me_name := "mod_contra"]
df[is.na(sample_size),sample_size := quantile(df$sample_size,0.05,na.rm=T)]

# impute variance (particularly when it is estimated to be very low)
df[variance < 0.00001, variance := NA]
df[is.na(variance), variance := data*(1-data)/sample_size]

# outliers
df <- df[!grepl("Prevensjonsbruken", survey_name)]
df <- df[!grepl("modality by women in Norway", survey_name)]
df <- df[!grepl("International Health Foundation", survey_name) | location_id != 76 | age_group_id != 8]

# output
write.csv(df,file.path(j_root, "FILEPATH"),row.names=F)
write.csv(coeffs,file.path(j_root, "FILEPATH"),row.names=F)

