#----HEADER----------------------------------------------------------------------------------------------------------------------
# Purpose: Run a linear model to inform the prior of ST-GPR for blood lead
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
# If running from the central st-gpr, the data will already be read in from the database
if (Sys.info()["sysname"] == "Windows") {
  df <- fread(paste0(j_root, "FILEPATH"))
  df[,standard_deviation := as.numeric(standard_deviation)]
  df[,standard_error := as.numeric(standard_error)]
  df[,variance := as.numeric(variance)]
  df[,nid := as.numeric(nid)]
}

# Calculate standard error and variance when standard deviation is present
impute_size <- df[!is.na(sample_size),quantile(sample_size,0.05)]
df[is.na(sample_size), sample_size := impute_size]
df[group_review == 0, standard_deviation := NA]
df[cv_mean_type == 0 & !is.na(standard_deviation), standard_error := exp(log(standard_deviation)-1)*data/sqrt(sample_size)]
df[cv_mean_type != 0 & !is.na(standard_deviation), standard_error := standard_deviation/sqrt(sample_size)]
df[!is.na(standard_error) & is.na(variance), variance := standard_error ** 2]

# transform data
df[, "raw_data" := copy(data)]
df[, "raw_variance" := copy(variance)]
df[, "variance" := variance * ((1/data)^2)]
df[, data := log(data)] #this is only relevant if being run before starting ST-GPR


#----MODEL------------------------------------------------------------------------------------------------------------------------
## Linear model

# format cv_mean_type, since there is now arithmetic mean and median to crosswalk between
df[cv_mean_type == 0, gm_mean := 1]
df[is.na(gm_mean), gm_mean := 0]
df[cv_mean_type == 3, median := 1]
df[is.na(median), median := 0]

# other adjustments before generating model
df[,super_region_id := as.factor(super_region_id)]
'%ni%' <- Negate('%in%')
outliers <- c(131566, 131729, 122603, 131642, 131712, 131713, 278389, 278477, 131635, 131629, 278475, 278096, 131553, 278447, 278505, 131699, 278016, 131692, 278118, 278433, 122535, 131454, 131446, 131683, 278121, 278333, 131696, 151104, 131662, 278315, 278455, 131623, 131733, 131638, 131649)
df[,urban := inv.logit(lt_urban)]

# model accounts for age, sdi, cv_mean_type, super region's interaction with urbanicity, and the outphase of leaded gasoline interacted 
# with number of vehicles per capita as proxy for leaded exhaust in air exposure
mod <- lm(data ~ as.factor(age_group_id) + super_region_id + urban:super_region_id + vehicles_pc*outphase_smooth + sdi + gm_mean + median,
            data=df[nid %ni% outliers],
            na.action=na.omit)

coefficients <- as.data.table(coef(summary(mod)), keep.rownames = T)

#********************************************************************************************************************************

#----CROSSWALKING DATA-------------------------------------------------------------------------------------------------------------

# We will first crosswalk our data based on the results of the regression
# Then, adjust the variance of datapoints, given that our adjusted data is less certain subject to the variance of the regression 

# To do this you will use the formula: Var(Ax + By) = a^2 * Var(x) + b^2 * Var(y) + 2ab * covariance(x,y) 
# (https://en.wikipedia.org/wiki/Variance)
# In our case, the variables are as follows:
# A: geometric_mean
# x: beta for geometric_mean
# B: lt_urban (logit of data urbanicity) - lt_prop_urban (logit of country urbanicity)
# y: beta for urbanicity in a given super_region

# Adjust datapoints to arithmetic mean
gm_coeff <- as.numeric(coefficients[rn == "gm_mean", 'Estimate', with=F])
median_coeff <- as.numeric(coefficients[rn == "median", 'Estimate', with=F])
df[, data := data - (gm_mean * gm_coeff)]
df[, data := data - (median * median_coeff)]

# Now adjust the variance for points crosswalked to arithmetic mean
gm_se <- as.numeric(coefficients[rn == "gm_mean", 'Std. Error', with=F])
median_se <- as.numeric(coefficients[rn == "median", 'Std. Error', with=F])

df[, variance := variance + (gm_mean^2 * gm_se^2)]
df[, variance := variance + (median^2 * median_se^2)]

df[, gm_mean_og := gm_mean]
df[, gm_mean := 0]
df[, median_og := median]
df[, median := 0]

# Adjust data urbanicity to the national average
# here, the variable lt_urban represents the urbanicity of the datapoint (in logit space)
# whereas the variable lt_prop_urban represents the national average urbanicity (in logit space)
# we want to crosswalk  data point urbanicity to what we would expect it to be if nationally representative
# in order to do this we multiply the beta on urbanicity in that super region by the difference in percent urbanicity between study and national
# ex, if study is 0 (rural) and country is 70% urban, we are multiplying the coefficent by -0.7

for (this.super.region in unique(df$super_region_id)) {

  cat("Adjusting points in super region #", this.super.region, "\n"); flush.console()

  # First, we will adjust the data based on urbanicity
  urban_coeff <- as.numeric(coefficients[rn == paste0("super_region_id",this.super.region,":urban"), "Estimate", with=F])
  urban_se <- as.numeric(coefficients[rn == paste0("super_region_id",this.super.region,":urban"), "Std. Error", with=F])
  cov_urb_gm <- vcov(mod)[paste0("super_region_id",this.super.region,":urban"),"gm_mean"]
  cov_urb_med <- vcov(mod)[paste0("super_region_id",this.super.region,":urban"),"median"]
  cov_arit_med <- vcov(mod)["gm_mean","median"]

  df[super_region_id == this.super.region,
     data := data - ((urban-inv.logit(lt_prop_urban)) * urban_coeff)]

  # Now adjust the variance for points crosswalked to urbanicity in a given superregion
  # here we will also take into account the covariance between urbanicity and geometric mean
  df[super_region_id == this.super.region,
     variance := variance + ((urban-inv.logit(lt_prop_urban))^2 * urban_se^2) + (2*(gm_mean*(urban-inv.logit(lt_prop_urban))) * cov_urb_gm) +
       (2*(median*(urban-inv.logit(lt_prop_urban))) * cov_urb_med)]

}
df[, urban_og := urban]
df[, urban := inv.logit(lt_prop_urban)]

# transform data and variance back to regular space
df[, data := exp(data)]
df[, variance := variance / (1/data)^2] # using reverse delta method for logspace

#impute variance
df[, CV := standard_error*sqrt(sample_size)/data]
cvmean <- mean(df$CV,na.rm=T)
df[is.na(standard_error), standard_error := cvmean*data/sqrt(sample_size)]
df[is.na(variance), variance := standard_error**2]

# remove group review data that is duplicate (only kept them to help inform crosswalk)
df <- df[group_review == 1 | is.na(group_review)]

# write crosswalk results and corresponding coefficients
write.csv(df, paste0(j_root, "FILEPATH"), row.names=F)
write.csv(coefficients, paste0(j_root, "FILEPATH"), row.names=F)

