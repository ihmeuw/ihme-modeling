#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Run a linear model to inform the prior of ST-GPR for met demand for contraception
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
#----PREP------------------------------------------------------------------------------------------------------------------------

# Read in your model data if you are working interactively
df <- fread(paste0(j_root, "FILEPATH"))
setnames(df,"no_pregppa","missing_no_pregppa")

# calculate variance
df[, variance := standard_error**2]
df[, raw_data := copy(data)]
df[, raw_variance := copy(variance)]

# offset extreme data and modern contraceptive covariate
df[data==0, data := 0.001]
df[data==1, data := 0.999]
df[cv_mod==0, cv_mod := 0.001]
df[cv_mod==1, cv_mod := 0.999]

# transform data, variance, and modern contraception into logit space
df[, variance := variance * (1/(data*(1-data)))^2]
df[, data := logit(data)] 
df[, cv_mod := logit(cv_mod)]

# crosswalk modern contraception
coefficients <- fread(file.path(j_root,"FILEPATH"))
for (this.age in unique(df$age_group_id)) {
  cat("Adjusting points in for age group", this.age, "\n"); flush.console()
  currmar_coeff <- as.numeric(coefficients[rn == paste0("currmar:",this.age), "Estimate", with=F])
  evermar_coeff <- as.numeric(coefficients[rn == paste0("evermar:",this.age), "Estimate", with=F])
  df[age_group_id == this.age, cv_mod := cv_mod - (currmar_only * currmar_coeff)]
  df[age_group_id == this.age, cv_mod := cv_mod - (evermar_only * evermar_coeff)]
}

# list out all counterfactual extractions
topics <- c("currmar",
            "evermar",
            "fecund",
            "desire",
            "desire_later",
            "no_pregppa",
            "currmar_desire",
            "currmar_desire_later",
            "evermar_desire",
            "evermar_desire_later",
            "currmar_fecund",
            "currmar_no_pregppa",
            "evermar_fecund",
            "evermar_no_pregppa",
            "fecund_desire",
            "fecund_desire_later",
            "desire_no_pregppa",
            "desire_later_no_pregppa",
            "currmar_fecund_desire",
            "currmar_fecund_desire_later",
            "currmar_desire_no_pregppa",
            "currmar_desire_later_no_pregppa",
            "evermar_fecund_desire",
            "evermar_fecund_desire_later",
            "evermar_desire_no_pregppa",
            "evermar_desire_later_no_pregppa")
id.vars <- c("nid","ihme_loc_id","age_group_id")

# load and compile counterfactual extractions
for (topic in topics) {
  df2 <- fread(paste0(j_root, "FILEPATH"))
  df2[,age_group_id := (age_start/5) + 5]
  df2 <- df2[var == "met_need_demanded" & sample_size > 19,c(id.vars,"mean"),with=F]
  df2[mean == 0, mean := 0.001]
  df2[mean == 1, mean := 0.999]
  df2[,mean := logit(mean)]
  setnames(df2,"mean",topic)
  df <- merge(df,df2,by=c(id.vars),all.x=T)
}

#----LINEAR MODEL------------------------------------------------------------------------------------------------------------------------

# run age-specific linear regressions to determine effect of each counterfactual
# (no need to include any covariates since they are re-extractions of the exact same data)
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

# We will first crosswalk our data based on the results of the regression
# Then, adjust the variance of datapoints, given that our adjusted data is less certain subject to the variance of the regression 

for (this.age in unique(df$age_group_id)) {
  
  cat("Adjusting points in for age group", this.age, "\n"); flush.console()
  
  # Assign crosswalk coefficients from regressions
  currmar_coeff <- as.numeric(coeffs[rn == paste0("currmar:",this.age), "Estimate", with=F])
  evermar_coeff <- as.numeric(coeffs[rn == paste0("evermar:",this.age), "Estimate", with=F])
  fecund_coeff <- as.numeric(coeffs[rn == paste0("fecund:",this.age), "Estimate", with=F])
  desire_coeff <- as.numeric(coeffs[rn == paste0("desire:",this.age), "Estimate", with=F])
  desire_later_coeff <- as.numeric(coeffs[rn == paste0("desire_later:",this.age), "Estimate", with=F])
  no_pregppa_coeff <- as.numeric(coeffs[rn == paste0("no_pregppa:",this.age), "Estimate", with=F])
  currmar_fecund_coeff <- as.numeric(coeffs[rn == paste0("currmar_fecund:",this.age), "Estimate", with=F])
  evermar_fecund_coeff <- as.numeric(coeffs[rn == paste0("evermar_fecund:",this.age), "Estimate", with=F])
  currmar_no_pregppa_coeff <- as.numeric(coeffs[rn == paste0("currmar_no_pregppa:",this.age), "Estimate", with=F])
  evermar_no_pregppa_coeff <- as.numeric(coeffs[rn == paste0("evermar_no_pregppa:",this.age), "Estimate", with=F])
  fecund_desire_coeff <- as.numeric(coeffs[rn == paste0("fecund_desire:",this.age), "Estimate", with=F])
  fecund_desire_later_coeff <- as.numeric(coeffs[rn == paste0("fecund_desire_later:",this.age), "Estimate", with=F])
  desire_no_pregppa_coeff <- as.numeric(coeffs[rn == paste0("desire_no_pregppa:",this.age), "Estimate", with=F])
  desire_later_no_pregppa_coeff <- as.numeric(coeffs[rn == paste0("desire_later_no_pregppa:",this.age), "Estimate", with=F])
  currmar_desire_coeff <- as.numeric(coeffs[rn == paste0("currmar_desire:",this.age), "Estimate", with=F])
  evermar_desire_coeff <- as.numeric(coeffs[rn == paste0("evermar_desire:",this.age), "Estimate", with=F])
  currmar_desire_later_coeff <- as.numeric(coeffs[rn == paste0("currmar_desire_later:",this.age), "Estimate", with=F])
  evermar_desire_later_coeff <- as.numeric(coeffs[rn == paste0("evermar_desire_later:",this.age), "Estimate", with=F])
  currmar_fecund_desire_coeff <- as.numeric(coeffs[rn == paste0("currmar_fecund_desire:",this.age), "Estimate", with=F])
  evermar_fecund_desire_coeff <- as.numeric(coeffs[rn == paste0("evermar_fecund_desire:",this.age), "Estimate", with=F])
  currmar_fecund_desire_later_coeff <- as.numeric(coeffs[rn == paste0("currmar_fecund_desire_later:",this.age), "Estimate", with=F])
  evermar_fecund_desire_later_coeff <- as.numeric(coeffs[rn == paste0("evermar_fecund_desire_later:",this.age), "Estimate", with=F])
  currmar_desire_no_pregppa_coeff <- as.numeric(coeffs[rn == paste0("currmar_desire_no_pregppa:",this.age), "Estimate", with=F])
  evermar_desire_no_pregppa_coeff <- as.numeric(coeffs[rn == paste0("evermar_desire_no_pregppa:",this.age), "Estimate", with=F])
  currmar_desire_later_no_pregppa_coeff <- as.numeric(coeffs[rn == paste0("currmar_desire_later_no_pregppa:",this.age), "Estimate", with=F])
  evermar_desire_later_no_pregppa_coeff <- as.numeric(coeffs[rn == paste0("evermar_desire_later_no_pregppa:",this.age), "Estimate", with=F])
  
  # assign standard errors of crosswalk from regressions
  currmar_se <- as.numeric(coeffs[rn == paste0("currmar:",this.age), "Std. Error", with=F])
  evermar_se <- as.numeric(coeffs[rn == paste0("evermar:",this.age), "Std. Error", with=F])
  fecund_se <- as.numeric(coeffs[rn == paste0("fecund:",this.age), "Std. Error", with=F])
  desire_se <- as.numeric(coeffs[rn == paste0("desire:",this.age), "Std. Error", with=F])
  desire_later_se <- as.numeric(coeffs[rn == paste0("desire_later:",this.age), "Std. Error", with=F])
  no_pregppa_se <- as.numeric(coeffs[rn == paste0("no_pregppa:",this.age), "Std. Error", with=F])
  currmar_fecund_se <- as.numeric(coeffs[rn == paste0("currmar_fecund:",this.age), "Std. Error", with=F])
  evermar_fecund_se <- as.numeric(coeffs[rn == paste0("evermar_fecund:",this.age), "Std. Error", with=F])
  currmar_no_pregppa_se <- as.numeric(coeffs[rn == paste0("currmar_no_pregppa:",this.age), "Std. Error", with=F])
  evermar_no_pregppa_se <- as.numeric(coeffs[rn == paste0("evermar_no_pregppa:",this.age), "Std. Error", with=F])
  fecund_desire_se <- as.numeric(coeffs[rn == paste0("fecund_desire:",this.age), "Std. Error", with=F])
  fecund_desire_later_se <- as.numeric(coeffs[rn == paste0("fecund_desire_later:",this.age), "Std. Error", with=F])
  desire_no_pregppa_se <- as.numeric(coeffs[rn == paste0("desire_no_pregppa:",this.age), "Std. Error", with=F])
  desire_later_no_pregppa_se <- as.numeric(coeffs[rn == paste0("desire_later_no_pregppa:",this.age), "Std. Error", with=F])
  currmar_desire_se <- as.numeric(coeffs[rn == paste0("currmar_desire:",this.age), "Std. Error", with=F])
  evermar_desire_se <- as.numeric(coeffs[rn == paste0("evermar_desire:",this.age), "Std. Error", with=F])
  currmar_desire_later_se <- as.numeric(coeffs[rn == paste0("currmar_desire_later:",this.age), "Std. Error", with=F])
  evermar_desire_later_se <- as.numeric(coeffs[rn == paste0("evermar_desire_later:",this.age), "Std. Error", with=F])
  currmar_fecund_desire_se <- as.numeric(coeffs[rn == paste0("currmar_fecund_desire:",this.age), "Std. Error", with=F])
  evermar_fecund_desire_se <- as.numeric(coeffs[rn == paste0("evermar_fecund_desire:",this.age), "Std. Error", with=F])
  currmar_fecund_desire_later_se <- as.numeric(coeffs[rn == paste0("currmar_fecund_desire_later:",this.age), "Std. Error", with=F])
  evermar_fecund_desire_later_se <- as.numeric(coeffs[rn == paste0("evermar_fecund_desire_later:",this.age), "Std. Error", with=F])
  currmar_desire_no_pregppa_se <- as.numeric(coeffs[rn == paste0("currmar_desire_no_pregppa:",this.age), "Std. Error", with=F])
  evermar_desire_no_pregppa_se <- as.numeric(coeffs[rn == paste0("evermar_desire_no_pregppa:",this.age), "Std. Error", with=F])
  currmar_desire_later_no_pregppa_se <- as.numeric(coeffs[rn == paste0("currmar_desire_later_no_pregppa:",this.age), "Std. Error", with=F])
  evermar_desire_later_no_pregppa_se <- as.numeric(coeffs[rn == paste0("evermar_desire_later_no_pregppa:",this.age), "Std. Error", with=F])
  
  # adjust data for every combination of covariates by corresponding coefficient
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire == 0 & missing_desire_later == 0 & no_pregppa == 0,
     data := data - currmar_coeff]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire == 0 & missing_desire_later == 0 & no_pregppa == 0,
     data := data - evermar_coeff]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - fecund_coeff]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - no_pregppa_coeff]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     data := data - desire_coeff]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     data := data - desire_later_coeff]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - currmar_fecund_coeff]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - evermar_fecund_coeff]
  
  df[age_group_id == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - currmar_no_pregppa_coeff]
  
  df[age_group_id == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - evermar_no_pregppa_coeff]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire == 1,
     data := data - fecund_desire_coeff]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire_later == 1,
     data := data - fecund_desire_later_coeff]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire == 1,
     data := data - desire_no_pregppa_coeff]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire_later == 1,
     data := data - desire_later_no_pregppa_coeff]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     data := data - currmar_desire_coeff]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     data := data - evermar_desire_coeff]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     data := data - currmar_desire_later_coeff]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     data := data - evermar_desire_later_coeff]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire == 1,
     data := data - currmar_fecund_desire_coeff]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire == 1,
     data := data - evermar_fecund_desire_coeff]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire_later == 1,
     data := data - currmar_fecund_desire_later_coeff]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire_later == 1,
     data := data - evermar_fecund_desire_later_coeff]
  
  df[age_group_id == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire == 1,
     data := data - currmar_desire_no_pregppa_coeff]
  
  df[age_group_id == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire == 1,
     data := data - evermar_desire_no_pregppa_coeff]
  
  df[age_group_id == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire_later == 1,
     data := data - currmar_desire_later_no_pregppa_coeff]
  
  df[age_group_id == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire_later == 1,
     data := data - evermar_desire_later_no_pregppa_coeff]
  
  
  # adjust variance for every combination of covariates by corresponding coefficient
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire == 0 & missing_desire_later == 0 & no_pregppa == 0,
     variance := variance + (currmar_coeff**2)]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire == 0 & missing_desire_later == 0 & no_pregppa == 0,
     variance := variance + (evermar_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (fecund_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (no_pregppa_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     variance := variance + (desire_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     variance := variance + (desire_later_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (currmar_fecund_coeff**2)]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (evermar_fecund_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (currmar_no_pregppa_coeff**2)]
  
  df[age_group_id == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (evermar_no_pregppa_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire == 1,
     variance := variance + (fecund_desire_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire_later == 1,
     variance := variance + (fecund_desire_later_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire == 1,
     variance := variance + (desire_no_pregppa_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire_later == 1,
     variance := variance + (desire_later_no_pregppa_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     variance := variance + (currmar_desire_coeff**2)]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     variance := variance + (evermar_desire_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     variance := variance + (currmar_desire_later_coeff**2)]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     variance := variance + (evermar_desire_later_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire == 1,
     variance := variance + (currmar_fecund_desire_coeff**2)]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire == 1,
     variance := variance + (evermar_fecund_desire_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire_later == 1,
     variance := variance + (currmar_fecund_desire_later_coeff**2)]
  
  df[age_group_id == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire_later == 1,
     variance := variance + (evermar_fecund_desire_later_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire == 1,
     variance := variance + (currmar_desire_no_pregppa_coeff**2)]
  
  df[age_group_id == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire == 1,
     variance := variance + (evermar_desire_no_pregppa_coeff**2)]
  
  df[age_group_id == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire_later == 1,
     variance := variance + (currmar_desire_later_no_pregppa_coeff**2)]
  
  df[age_group_id == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire_later == 1,
     variance := variance + (evermar_desire_later_no_pregppa_coeff**2)]
  
}

#----PREDICT OFF OF CV_MOD-------------------------------------------------------------------------------------------------------------

# have much more data on mod_contra than on met demand. Since the two are very related, would be best to predict off of mod_contra
# data when corresponding met demand data is unavailable (and not doing so would lead to inconsistent model results between the two)
# Run linear model of relationship between mod_contra and met demand (taking into account relevant covariates) and predict where
# met demand data is missing
mod <- lm(data ~ as.factor(age_group_id) + sdi + cv_mod:as.factor(super_region_id), 
          data=df[nid != 18477 & nid != 18486],
          na.action=na.omit)

df[,prediction := predict(mod,df[,list(age_group_id,sdi,cv_mod,super_region_id,region_id)])]
df[is.na(data),data := prediction]

#----FORMATTING-------------------------------------------------------------------------------------------------------------

# Reset all study level covariates to predict as the gold standard
# Also save the originals for comparison
df[, currmar_og := currmar_only]
df[, currmar_only := 0]
df[, evermar_og := evermar_only]
df[, evermar_only := 0]
df[, nofecund_og := missing_fecund]
df[, missing_fecund := 0]
df[, nodesire_og := missing_desire]
df[, missing_desire := 0]
df[, nodesire_later_og := missing_desire_later]
df[, missing_desire_later := 0]
df[, no_pregppa_og := missing_no_pregppa]
df[, missing_no_pregppa := 0]

# transform data, variance, and cv_mod back to normal space
df[, cv_mod := inv.logit(cv_mod)]
df[, "data" := inv.logit(data)]
df[, "variance" := variance / (1/(data*(1-data)))^2] # using reverse delta method for logspace

# formatting
df[,me_name := "met_need_demand"]
df[is.na(sample_size),sample_size := quantile(df$sample_size,0.05,na.rm=T)]

# reset data points that are unrealistically high or low, and impute variance
df[data > 0.99, variance := NA]
df[data > 0.99, data := 0.99]
df[data < 0.01, variance := NA]
df[data < 0.01, data := 0.01]
df[variance < 0.0001, variance := NA]
df[is.na(variance), variance := data*(1-data)/sample_size]

# outliers
df <- df[!grepl("Prevensjonsbruken", survey_name)]
df <- df[!grepl("modality by women in Norway", survey_name)]
df <- df[!grepl("International Health Foundation", survey_name) | location_id != 76 | age_group_id != 8]

# output
write.csv(df,file.path(j_root, "FILEPATH"),row.names = F)
write.csv(coeffs,file.path(j_root, "FILEPATH"),row.names=F)
