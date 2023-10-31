##################################################
## will be ugly, trying to prep mask use for mandates testing
###################################################################
# load libraries
library(dplyr)
library(data.table)
library(tidyr)
library(slime, lib.loc = "FILEPATH")
#library(mrbrt001, lib.loc = "/ihme/code/mscm/R/packages/") # for R version 3.6.3
library(forecast)
library(ggplot2)
library(openxlsx)
library(RColorBrewer)
library(lme4)
library(metafor)

version <- "2020_07_24.02" #"best"

# install ihme.covid
# no library() call because we namespace all uses
tmpinstall <- system("mktemp -d --tmpdir=/tmp", intern = TRUE)
.libPaths(c(tmpinstall, .libPaths()))
devtools::install_github("FILEPATH/ihme.covid", upgrade = "never")

# This is just here to check how it is formatted for the mobility MRBRT model
mobility <- fread("FILEPATH/smooth_mobility_with_sd.csv")

## Meta-regress the data or the smoothed estimates?
option <- "data"

## Read in smoothed mask use estimates
mask_use <- fread(paste0("FILEPATH",version,"/mask_use.csv"))

## Read in mask data
mask_data <- fread(paste0("FILEPATH",version,"/used_data.csv"))
mask_data$mask_use <- mask_data$prop_always
mask_data <- subset(mask_data, source %in% c("Facebook","Premise","Adjusted Website","Week Binned GitHub Always"))

# Anticipating some requests from Steve/Chris
mask_data <- mask_data[order(location_id, source, date)]
mask_data[, non_mask_use := 1 - mask_use]
mask_data[, change_use := mask_use - shift(mask_use), by=c("location_id","source")]
mask_data[, previous_non_use := shift(non_mask_use), by=c("location_id","source")]
mask_data[, first_diff_ratio := change_use / previous_non_use]

## Read in mandate info
# New format is terrible
# Probably want a separate script to process
mask_mandates_data <- read.xlsx("FILEPATH/closure_criteria_sheet_masks.xlsx")
# mask_mandates_data <- mask_mandates_data[2:nrow(mask_mandates_data),] # First row is informative/legend
mask_mandates_data$date <- as.Date(mask_mandates_data$date, format = "%d.%m.%Y")
mask_mandates_data <- data.table(mask_mandates_data)

# Create some variables for enforcement
mask_mandates_data[, penalty := ifelse(non_compliance_penalty %like% "Fine", "Fine",
                                ifelse(non_compliance_penalty %like% "fine", "Fine",
                                         ifelse(non_compliance_penalty %like% "etention","Detention",
                                         ifelse(non_compliance_penalty %like% "Arrest","Detention",
                                                ifelse(non_compliance_penalty %like% "enalt", "Penalty",
                                                ifelse(non_compliance_penalty %like% "Legal", "Penalty",
                                                ifelse(non_compliance_penalty %like% "demeanor", "Penalty",
                                                      ifelse(non_compliance_penalty %like% "None","None",
                                                            ifelse(non_compliance_penalty %like% "Not","None","Other/unknown")))))))))]

# What I think is, keep if only 1 by location, if multiple, choose more general (general public)
mask_mandates_data[, row := 1]
mask_mandates_data[, location_rows := sum(row), by=location_id]
mask_mandates_data[, public_in_name := ifelse(targeted_ihme %in% c("General public","general public"),1,0)]
mask_mandates_data[, last_chance := ifelse(public_in_name == 1, 0, ifelse(targeted_ihme %in% c("Public places","public places","Public spaces", "public spaces"),1,0))]
mask_mandates_data[, keep_row := ifelse(location_rows == 1, 1, ifelse(public_in_name == 1, 1, ifelse(last_chance == 1,1,0)))]

mask_mandates <- mask_mandates_data[keep_row == 1]
mask_mandates[, first_date := min(date), by=location_id]
mask_mandates <- mask_mandates[date == first_date] # Keep only the earlier mandate

mask_mandates[, man_date := date]
mask_mandates[, has_mandate := 1]

if(option == "smoothed"){
  mask_df <- merge(mask_use, mask_mandates[,c("location_id","man_date","has_mandate","penalty")], by="location_id") # Intentionally allowing locations without matches dropped? #
  # Doesn't make sense to keep observations in the future since they are flat
  mask_df <- mask_df[date <= Sys.Date()]
  # Trying to limit data a bit, keep after March 1
  mask_df <- mask_df[date >= "2020-03-01"]
  
  # Keep +/- 2 weeks from mandate
  mask_df$date <- as.Date(mask_df$date)
  mask_df <- mask_df[abs(as.numeric(date - man_date)) < 60]
} else {
  mask_df <- merge(mask_data, mask_mandates[,c("location_id","man_date","has_mandate","penalty")], by="location_id", all.x=T) # Intentionally allowing locations without matches dropped?
}
mask_df[, man_date := as.character(man_date)]
mask_df[, use_date := ifelse(is.na(man_date), as.Date("2022-01-01"), as.Date(man_date))]
mask_df[, man_date := as.Date(man_date)]
mask_df[, mandate := ifelse(date >= man_date, 1, 0), by=location_id]
mask_df[, mandate := ifelse(mandate == TRUE, 1, 0)]
mask_df[, mandate := ifelse(is.na(mandate), 0, mandate)]
mask_df[, has_mandate := ifelse(is.na(has_mandate), 0, 1)]

##-----------------------------------------------------------------------------
## Copied from here: https://stash.ihme.washington.edu/projects/CVD19/repos/covid-beta-inputs/browse/src/covid_beta_inputs/mobility/forecast/02_run_mrbrt.R
# leave observed smoothed metric only and create a se variable
mask_df <- copy(mask_df)

# What is the value for the regression?
mask_df$mean <- mask_df$mask_use
#mask_df$mean <- mask_df$first_diff_ratio
mask_df <- mask_df[!is.na(mean)]

mask_df[, std_se := sqrt(var(mean)/nrow(mask_df))]

# add a dummy
mask_df$intercept <- 0
mask_df <- mask_df[,c("location_id","mean","std_se","mandate","intercept","date","has_mandate","penalty")]
# mask_df <- mask_df[has_mandate == 1]

# define the data object
data_mrbrt <- MRData(
  df = mask_df,
  col_group = 'location_id',
  col_obs = 'mean',
  col_obs_se = 'std_se',
  col_covs = c('mandate', 'intercept') 
)
# adding priors and bound for the total effects, no restriction on the random effects
cov_model1  <- CovModel('mandate', use_re = F, re_var = 0.001, bounds = array(c(-1000,1000)))
cov_model2  <- CovModel('intercept', use_re = T, re_var = 0.001, bounds = array(c(0,1000)))
# compile the models
cov_models = CovModelSet(list(cov_model1, cov_model2))

# run the models 
model = MRModel(data_mrbrt, cov_models)
model$fit_model()

# save the results (effects of SD mandates by location) from the regression
results <- model$result

locs <- unique(mask_df$location_id)
effs <- NULL
for(loc in locs){
  eff <- results[[toString(loc)]]
  eff <- c(loc, eff)
  names(eff) <- c('location_id','mandate_eff','intercept')
  effs <- rbind(effs, eff)
}

effs <- data.table(effs)
effs[, sum := intercept + mandate_eff]
effs
colMeans(effs)
hist(effs$intercept)

ggplot(mask_df, aes(x=as.Date(date), y=mean, col=factor(mandate))) + geom_point(alpha=0.5, pch=1) + theme_bw() + 
  scale_color_manual(values = c("black","red"))
ggplot(mask_df[has_mandate==1], aes(x=factor(mandate), y=mean, col=factor(mandate))) + geom_boxplot() + theme_bw() + 
  scale_color_manual(values = c("black","red"))
ggplot(mask_df[has_mandate==1], aes(x=factor(mandate), y=mean, col=penalty)) + geom_boxplot() + theme_bw()

## What is happening? I get a result much higher than RMA or LMER. Not sure why.
# res.rma <- rma(mean, 1/std_se^2, mods =  ~ mandate, data=mask_df)
# summary(res.rma)
res.lme <- lmer(mean ~ mandate + (1|location_id), data = mask_df)
summary(res.lme)

summary(lmer(mean ~ mandate:penalty + (1|location_id), data=mask_df))

ggplot(mask_df, aes(x=as.Date(date), y=mean, col=factor(mandate), group = location_id)) + geom_point(alpha=0.3) +
  theme_minimal()
