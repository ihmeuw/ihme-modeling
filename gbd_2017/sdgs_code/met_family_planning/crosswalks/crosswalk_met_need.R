############################################################################################################
## Purpose: crosswalk met need data and perform age-splits when needed
###########################################################################################################

## clear memory
rm(list=ls())

## runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

## libraries and sourcing
pacman::p_load(data.table,magrittr,ggplot2,parallel,rhdf5)
model_root <- "FILEPATH"
setwd(model_root)
source("init.r")
source(file.path(j,"FILEPATH"))
source(file.path(j, "FILEPATH"))
source(file.path(j, "FILEPATH"))
source(file.path(j, "FILEPATH"))

## settings
presplit <- F
years <- seq(1970,2017)
mod_contra_run <- 43808 ## run_id of best mod_contra run to determine denominator in age-split (ideally the post-split model)
met_demand_run <- 43812  ## run_id of pre-split met_demand model to determine age trend for splitting

## in/out
in.dir <- file.path(j,"FILEPATH")
out.dir <- file.path(j,"FILEPATH")
model.dir <- "FILEPATH"

## load locations and SDI
locations <- get_location_metadata(gbd_round_id = 5, location_set_id = 22)
locs <- locations[,list(location_id,ihme_loc_id,region_id,super_region_id)]
sdi <- get_covariate_estimates(covariate_id = 881) #sdi
sdi <- sdi[,list(location_id,year_id,mean_value)]
setnames(sdi,"mean_value","sdi")

## load mapping between age_group and age_group_id for most-detailed ages
age_map <- get_ids(table = "age_group")
age_map <- age_map[age_group_id %in% seq(8,14)]
age_map[,age_group := gsub(" to ","-",age_group_name)]
age_map[,age_group_name := NULL]


# PREP DATA ---------------------------------------------------------------


## read in data that is already age-specific, format, and impute sample size
df <- fread(file.path(in.dir,"FILEPATH"))
df <- merge(df[,-c("age_group"),with=F],age_map,by="age_group_id")
df[is.na(sample_size),sample_size := quantile(df$sample_size,0.05,na.rm=T)]

if (presplit) {
  ## indentify step of crosswalk for writing outputs
  out.end <- "_presplit"

} else {
  ## indentify step of crosswalk for writing outputs
  out.end <- ""

  ## read in data that needs to be split, format, and impute sample size
  tosplit <- fread(file.path(in.dir,"FILEPATH"))
  tosplit[,age_group_id := NULL]
  tosplit[,age_start := round_any(age_start,5)]
  tosplit[,age_end := round_any(age_end,5,floor) - 1 + round_any(age_end + 1 - round_any(age_end,5,floor),5)]
  tosplit[,age_group := paste0(age_start,"-",age_end)]
  tosplit[,cv_mod := mod_contra]
  tosplit[,data := met_demand]
  tosplit[,sample_size := demand_sample_size]
  tosplit[sample_size < 20, data := NA]
  tosplit[is.na(sample_size),sample_size := quantile(tosplit$sample_size,0.05,na.rm=T)]

  ## all rows should at least exist for cv_mod
  tosplit[is.na(cv_mod)]

  ## merge data to be split onto age-specific data for crosswalk
  df <- rbind(df,tosplit,fill=T)
}

## for surveys with demand_only rows, remove the non-demand_only rows
demand_only <- df[demand_row_only == 1,list(ihme_loc_id,nid,year_id)] %>% unique
demand_only[,remove := 1]
df <- merge(df,demand_only,by=c("ihme_loc_id","nid","year_id"),all=T)
df <- df[is.na(remove) | demand_row_only == 1]
df[,remove := NULL]

## don't need rows with missing met_demand data until later
df <- df[!is.na(data)]

## offset extreme data points to halfway between highest/lowest otherwise observed point and the boundary
df[, raw_data := copy(data)]
lower_offset <- 0.5*df[data != 0, min(data)]
upper_offset <- 0.5*(1 - df[data != 1, max(data)])
df[data == 0, data := lower_offset]
df[data == 1, data := 1 - upper_offset]

## impute standard_error when missing or unreasonable and calculate variance
df[is.na(standard_error) | standard_error < .001, standard_error := sqrt(data*(1-data)/sample_size)]
df[, variance := standard_error**2]
df[, raw_variance := copy(variance)]

## logit transform the data and variance (using delta transform)
df[, variance := variance * (1/(data*(1-data)))^2]
df[, data := logit(data)]

# CREATE TRAINING DATASET ------------------------------------------------

## list out all possible counterfactual re-extractions and id.vars
id.vars <- c("nid","ihme_loc_id","year_start","age_group")
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

## if just using age_specific data, then prepped regular-extraction data is already in the
## dataset. If crosswalk includes aggregate ages, most were removed from the dataset post-collapse,
## so need to train crosswalk using collapse outputs directly
if (presplit) {
  training <- copy(df)

  ## prevent confusing colname overlap from merging on counterfactuals
  setnames(training,"no_pregppa","missing_no_pregppa")

} else {
  training <- fread(file.path(in.dir,"FILEPATH"))
  training <- training[sample_size >= 20 & var == "met_need_demanded" & ihme_loc_id != "",c(id.vars,"mean"),with=F]
  training[mean < lower_offset, mean := lower_offset]
  training[mean > (1 - upper_offset), mean := 1 - upper_offset]
  training[,mean := logit(mean)]
  setnames(training,"mean","data")
}

## merge counterfactual re-extracted data onto the regular extraction for training crosswalk
for (topic in topics) {
  df2 <- fread(file.path(in.dir,"FILEPATH",paste0(topic,".csv")))

  ## survey specific fixes:
  ## GEO RHS survey only went up to age 44, shouldn't be used to crosswalk age groups that
  ## go up to 49
  df2 <- df2[nid != 95336 | substr(age_group,4,5) != "49"]

  ## cut out any collapsed data pionts with tiny sample sizes
  df2 <- df2[var == "met_need_demanded" & sample_size >= 20,c(id.vars,"mean"),with=F]
  df2[mean < lower_offset, mean := lower_offset]
  df2[mean > (1 - upper_offset), mean := 1 - upper_offset]
  df2[,mean := logit(mean)]
  setnames(df2,"mean",topic)
  training <- merge(training,df2,by=id.vars,all.x=T)
}

# LINEAR MODEL ------------------------------------------------------------


## generate coefficients for each counterfactual by running linear model on directly re-extracted survey data
coeffs <- data.table()
for (topic in topics) {
  ## subset to observations with data from both the regular extraction and the counterfactual re-extraction
  counter <- training[!is.na(get(topic)),c(id.vars,"data",topic),with=F]

  ## melt the dataset and use "cv" to mark the covariate
  counter <- melt(counter,id.vars=id.vars)
  counter[,cv := ifelse(variable=="data",0,1)]

  ## run model by age_group
  for (age in unique(counter$age_group)){
    ## no need for covariates in model, since two extractions use exact same underlying data
    mod <- lm(data = counter[age_group == age], value ~ cv)
    temp <- as.data.table(coef(summary(mod)), keep.rownames = T)
    temp <- temp[rn == "cv"]
    temp[,rn := paste0(topic,":",age)]

    ## append the coefficients from the regression to be used in the crosswalk
    coeffs <- rbind(coeffs,temp,fill=T)
  }
}

# CROSSWALK THE DATA ------------------------------------------------------


## We will first crosswalk our data based on the results of the regression
## Then, adjust the variance of datapoints, given that our adjusted data is
## less certain subject to the variance of the regression

for (this.age in unique(df$age_group)) {

  cat("Adjusting points in for age group", this.age, "\n"); flush.console()

  ## store regression coefficients and corresponding standard errors for each combination of counterfactual conditions
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

  ## Adjust data points by counterfactual coefficient corresponding to the survey's issues (so gold standard surveys will not change)
  ## note that currmar_only and evermar_only are mutually exclusive (so if currmar_only == 1 then evermar_only == 0, but both can also be 0)
  ## and missing_desire and missing_desire_later are mutually exclusive, and missing_fecund and no_pregppa are also mutually exclusive
  df[age_group == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire == 0 & missing_desire_later == 0 & no_pregppa == 0,
     data := data - currmar_coeff]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire == 0 & missing_desire_later == 0 & no_pregppa == 0,
     data := data - evermar_coeff]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - fecund_coeff]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - no_pregppa_coeff]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     data := data - desire_coeff]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     data := data - desire_later_coeff]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - currmar_fecund_coeff]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - evermar_fecund_coeff]

  df[age_group == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - currmar_no_pregppa_coeff]

  df[age_group == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     data := data - evermar_no_pregppa_coeff]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire == 1,
     data := data - fecund_desire_coeff]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire_later == 1,
     data := data - fecund_desire_later_coeff]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire == 1,
     data := data - desire_no_pregppa_coeff]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire_later == 1,
     data := data - desire_later_no_pregppa_coeff]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     data := data - currmar_desire_coeff]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     data := data - evermar_desire_coeff]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     data := data - currmar_desire_later_coeff]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     data := data - evermar_desire_later_coeff]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire == 1,
     data := data - currmar_fecund_desire_coeff]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire == 1,
     data := data - evermar_fecund_desire_coeff]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire_later == 1,
     data := data - currmar_fecund_desire_later_coeff]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire_later == 1,
     data := data - evermar_fecund_desire_later_coeff]

  df[age_group == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire == 1,
     data := data - currmar_desire_no_pregppa_coeff]

  df[age_group == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire == 1,
     data := data - evermar_desire_no_pregppa_coeff]

  df[age_group == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire_later == 1,
     data := data - currmar_desire_later_no_pregppa_coeff]

  df[age_group == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire_later == 1,
     data := data - evermar_desire_later_no_pregppa_coeff]


  ## adjust variance from corresponding adjustments
  df[age_group == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire == 0 & missing_desire_later == 0 & no_pregppa == 0,
     variance := variance + (currmar_coeff**2)]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire == 0 & missing_desire_later == 0 & no_pregppa == 0,
     variance := variance + (evermar_coeff**2)]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (fecund_coeff**2)]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (no_pregppa_coeff**2)]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     variance := variance + (desire_coeff**2)]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     variance := variance + (desire_later_coeff**2)]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (currmar_fecund_coeff**2)]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (evermar_fecund_coeff**2)]

  df[age_group == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (currmar_no_pregppa_coeff**2)]

  df[age_group == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire == 0 & missing_desire_later == 0,
     variance := variance + (evermar_no_pregppa_coeff**2)]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire == 1,
     variance := variance + (fecund_desire_coeff**2)]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & missing_fecund == 1 & missing_desire_later == 1,
     variance := variance + (fecund_desire_later_coeff**2)]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire == 1,
     variance := variance + (desire_no_pregppa_coeff**2)]

  df[age_group == this.age & currmar_only == 0 & evermar_only == 0 & no_pregppa == 1 & missing_desire_later == 1,
     variance := variance + (desire_later_no_pregppa_coeff**2)]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     variance := variance + (currmar_desire_coeff**2)]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire == 1 & no_pregppa == 0,
     variance := variance + (evermar_desire_coeff**2)]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     variance := variance + (currmar_desire_later_coeff**2)]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 0 & missing_desire_later == 1 & no_pregppa == 0,
     variance := variance + (evermar_desire_later_coeff**2)]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire == 1,
     variance := variance + (currmar_fecund_desire_coeff**2)]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire == 1,
     variance := variance + (evermar_fecund_desire_coeff**2)]

  df[age_group == this.age & currmar_only == 1 & missing_fecund == 1 & missing_desire_later == 1,
     variance := variance + (currmar_fecund_desire_later_coeff**2)]

  df[age_group == this.age & evermar_only == 1 & missing_fecund == 1 & missing_desire_later == 1,
     variance := variance + (evermar_fecund_desire_later_coeff**2)]

  df[age_group == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire == 1,
     variance := variance + (currmar_desire_no_pregppa_coeff**2)]

  df[age_group == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire == 1,
     variance := variance + (evermar_desire_no_pregppa_coeff**2)]

  df[age_group == this.age & currmar_only == 1 & no_pregppa == 1 & missing_desire_later == 1,
     variance := variance + (currmar_desire_later_no_pregppa_coeff**2)]

  df[age_group == this.age & evermar_only == 1 & no_pregppa == 1 & missing_desire_later == 1,
     variance := variance + (evermar_desire_later_no_pregppa_coeff**2)]
}


# REFORMATTING ----------------------------------------------------------------


# First reset all study level covariates to predict as the gold standard
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
df[, no_pregppa_og := no_pregppa]
df[, no_pregppa := 0]

## convert met demand and variance back to normal space
df[, data := inv.logit(data)]
df[, variance := variance / (1/(data*(1-data)))^2] # using reverse delta method for logit-space

## print locations where additional location-splitting would be necessary for data to be usable
## (until then, drop from dataset)
df[is.na(location_id),list(nid,ihme_loc_id)] %>% unique
df <- df[!is.na(location_id)]

## reset sample size to effective sample size based on variance adjustments (so that
## subsequent variance recalculation using age-split sample sizes retains effect from crosswalk)
df[,imputed_sample_size := sample_size]
df[,sample_size := data*(1-data)/variance]


# AGE-SPLITTING -----------------------------------------------------------

id.vars <- c("location_id","year_id","age_group_id","sex_id")

## if dataset includes aggregate ages, need to perform an age-split
if (!presplit) {

  ## separate back out data to be split and age-specific data
  tosplit <- df[!age_group %in% unique(age_map$age_group)]
  df <- df[age_group %in% unique(age_map$age_group)]

  ## give each row an id and determine how many splits need to occur
  tosplit[,survey_id := .I]
  tosplit[,reps := (age_end + 1 - age_start)/5]

  ## expand each survey by the number of splits and determine the new age_group_ids
  expanded <- data.table(survey_id = rep(tosplit$survey_id, tosplit$reps), age_start = rep(tosplit$age_start, tosplit$reps))
  expanded[,split_id := seq(1,.N), by=survey_id]
  expanded[,age_start := age_start + 5*(split_id - 1)]
  expanded[,age_group_id := (age_start/5) + 5]
  tosplit <- merge(tosplit[,-c("age_start","age_end","reps","age_group_id")],expanded[,list(survey_id,age_group_id)],by="survey_id")

  ## read in modeled estimates and populations and merge both onto dataset
  pops <- data.table(get_population(location_set_id = 22, year_id = years, sex_id = 2, location_id = -1, age_group_id = seq(8,14)))
  pops <- pops[,c("sex_id","run_id") := list(3,NULL)]
  mod_gpr <- model_load(mod_contra_run, 'raked')
  demand_gpr <- model_load(met_demand_run, 'raked')
  tosplit <- merge(tosplit,mod_gpr[,c(id.vars,"gpr_mean"),with=F],by=id.vars,all.x=T)
  setnames(tosplit,"gpr_mean","mod_gpr")
  tosplit <- merge(tosplit,demand_gpr[,c(id.vars,"gpr_mean"),with=F],by=id.vars,all.x=T)
  setnames(tosplit,"gpr_mean","demand_gpr")
  tosplit <- merge(tosplit,pops,by=c(id.vars),all.x=T)

  ## Age-split: adjust age-specific ST-GPR estimates such that their weighted average yields
  ## the extracted aggregate-age data
  tosplit[demand_gpr < mod_gpr,demand_gpr := mod_gpr]
  tosplit[,need := mod_gpr/demand_gpr]
  tosplit[,need_total := sum(population*need),by=survey_id]
  tosplit[,adj_factor := data/sum(demand_gpr*population*need/need_total),by=survey_id]
  tosplit[,presplit_data := data]
  tosplit[,data := demand_gpr*adj_factor]

  ## if some data goes above 1, reset to halfway between 1 and the largest data observed < 1
  tosplit[data >= 1]
  tosplit[data >= 1, data := 1 - tosplit[data < 1,.5*(1-max(data))]]

  ## split sample size and use to increase variance from the split (though doesn't incorporate
  ## any additional uncertainty from the split itself)
  tosplit[,presplit_sample_size := sample_size]
  tosplit[,sample_size := sample_size*population*need/need_total]
  tosplit[,variance := data*(1-data)/sample_size]

  ## data is NA for studies from before the first years at which we model
  tosplit[is.na(data)]
  tosplit <- tosplit[!is.na(data)]

  ## format and merge back on to age-specific data
  tosplit[,c("survey_id") := NULL]
  tosplit[,cv_split := 1]
  df <- rbind(df,tosplit, fill=T)
}


# PREDICT OUT MET DEMAND FROM MOD CONTRA ----------------------------------
## We have many more sources for contraceptive prevalence than for met demand, but since both indicators are very
## related, it would be wasteful (and lead to inconsistent models) to throw away mod_contra data in the met_demand model.
## Here, we run a regression using age, sdi, and super region to inform the relationship between mod_contra and met_demand,
## and for all observations where we have mod_contra data but no corresponding met demand data, use the relationship to
## predict met demand

## read in final mod_contra dataset
mod_contra <- fread(file.path(out.dir,"mod_contra_xwalked.csv"))
mod_contra <- mod_contra[,c("nid",id.vars,"data","notes"),with=F]
setnames(mod_contra,"data","cv_mod")

## survey specific fixes: issue if one survey gives multiple estimates of same loc-year-age group
mod_contra[nid == 353397 & location_id == 44540 & grepl("pondicherry",tolower(notes)),mod_only := 1]

df <- merge(df[,-c("cv_mod"),with=F],mod_contra[is.na(mod_only),-c("notes"),with=F],by=c("nid",id.vars),all=T)
df <- rbind(df,mod_contra[mod_only == 1],fill=T)
df[,mod_only := NULL]

## all data should have cv_mod
df[is.na(cv_mod)]

## Check that after crosswalking no met demand data is smaller than the modern contraceptive prevalence data
## (that is impossible and could result in inconsistent models for met demand and modern contraception)
## So far haven't had to deal with this issue, but just in case reset both values to be equal in this scenario
df[cv_mod > data,.N]
df[cv_mod > data,list(ihme_loc_id,nid,cv_mod,data)]
df[cv_mod > data,data := cv_mod]

## fill in sdi and super_region for mod_contra rows that were added
df <- merge(df[,-c("ihme_loc_id","region_id","super_region_id")],locs,by="location_id")
df <- merge(df[,-c("sdi")],sdi,by=c("location_id","year_id"))

## transform variables to logit space
df[,data := logit(data)]
df[,cv_mod := logit(cv_mod)]

## run the regression
mod <- lm(data ~ as.factor(age_group_id) + sdi + cv_mod:as.factor(super_region_id),
          data=df[!is.na(data)],na.action=na.omit)

## make predictions
df[,prediction := predict(mod,df[,list(age_group_id,sdi,cv_mod,super_region_id,region_id)])]
df[is.na(data),cv_prediction := 1]
df[is.na(data),data := prediction]

## transform back to normal space
df[,cv_mod := inv.logit(cv_mod)]
df[,data := inv.logit(data)]

## impute variance and sample size for new rows
df[is.na(sample_size),sample_size := quantile(df$sample_size,0.05,na.rm=T)]
df[is.na(variance), variance := data*(1-data)/sample_size]

# OUTPUT RESULTS -----------------------------------------------------------

## check for oddities
df[cv_mod > data]

## formatting
df[,me_name := "met_need_demand"]

## output dataset and crosswalk coefficients
write.csv(df, file.path(out.dir, paste0("met_demand_xwalked",out.end,".csv")),row.names=F)
write.csv(coeffs, file.path(out.dir, paste0("met_demand_xwalked",out.end,"_coefs.csv")),row.names=F)
