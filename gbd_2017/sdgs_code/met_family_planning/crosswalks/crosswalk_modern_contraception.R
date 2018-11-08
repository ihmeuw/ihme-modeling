############################################################################################################
## Purpose: crosswalk modern contraception data and perform age-splits when needed (if presplit = T, just runs on
##          age-specific data to provide data for preliminary model to determine age-trend for subsequent age split)
###########################################################################################################

## clear memory
rm(list=ls())

## disable scientific notation
options(scipen = 999)

## runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

## settings
presplit <- F ## T if just using age-specific data, F if split is needed
years <- seq(1970,2017)
mod_contra_run <- 43787 ## run_id of pre-split mod_contra run to determine age trend for splitting

## libraries and sourcing
pacman::p_load(data.table,magrittr,ggplot2,parallel,rhdf5)
model_root <- "FILEPATH"
setwd(model_root)
source("init.r")
source(file.path(j,"FILEPATH"))
source(file.path(j, "FILEPATH"))

## in/out
in.dir <- file.path(j,"FILEPATH")
out.dir <- file.path(j,"FILEPATH")
model.dir <- "FILEPATH"

## load mapping between age_group and age_group_id (only exists for most-detailed ages)
age_map <- get_ids(table = "age_group")
age_map <- age_map[age_group_id %in% seq(8,14)]
age_map[,age_group := gsub(" to ","-",age_group_name)]
age_map[,age_group_name := NULL]


# PREP DATA ---------------------------------------------------------------

## read in data that is already age-specific, merge on age_group, and impute sample size
df <- fread(file.path(in.dir,"FILEPATH"))[is.na(demand_row_only) | demand_row_only == 0]
df <- merge(df[,-c("age_group"),with=F],age_map,by="age_group_id")
df[is.na(sample_size),sample_size := quantile(df$sample_size,0.05,na.rm=T)]

if (presplit) {
  ## indentify step of crosswalk for writing outputs
  out.end <- "_presplit"

} else {
  ## indentify step of crosswalk for writing outputs
  out.end <- ""

  ## read in data that needs to be split, format ages, and impute sample size
  tosplit <- fread(file.path(in.dir,"FILEPATH"))[is.na(demand_row_only) | demand_row_only == 0]
  tosplit[,data := mod_contra]
  tosplit[,age_group_id := NULL]
  tosplit[,age_start := round_any(age_start,5)]
  tosplit[,age_end := round_any(age_end,5,floor) - 1 + round_any(age_end + 1 - round_any(age_end,5,floor),5)]
  tosplit[,age_group := paste0(age_start,"-",age_end)]
  tosplit[is.na(sample_size),sample_size := quantile(tosplit$sample_size,0.05,na.rm=T)]

  ## remove any rows with sample size < 20
  tosplit[sample_size < 20]
  tosplit <- tosplit[sample_size >= 20]

  ## add data to-be-split onto age-specific data for crosswalk
  df <- rbind(df,tosplit,fill=T)
}

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


# CREATE TRAINING DATASET -------------------------------------------------------

## list out id.vars and counterfactual re-extractions for training the crosswalk
topics <- c("currmar","evermar")
id.vars <- c("nid","ihme_loc_id","year_start","age_group")

## if just using age_specific data, then prepped regular-extraction data is already in the
## dataset. If crosswalk includes aggregate ages, most of those rows were removed from the
## dataset post-collapse, so need to train crosswalk using collapse outputs directly
if (presplit) {
  training <- copy(df)
} else {
  training <- fread(file.path(in.dir,"collapsed/most_recent/contraception.csv"))
  training <- training[sample_size >= 20 & var == "mod_contra" & ihme_loc_id != "",c(id.vars,"mean"),with=F]
  training[mean < lower_offset, mean := lower_offset]
  training[mean > (1 - upper_offset), mean := 1 - upper_offset]
  training[,mean := logit(mean)]
  setnames(training,"mean","data")
}

## merge counterfactual re-extracted data onto the regular extraction for training crosswalk
for (topic in topics) {
  df2 <- fread(file.path(in.dir,"collapsed/most_recent",paste0(topic,".csv")))

  ## survey specific fixes:
  ## GEO RHS survey only went up to age 44, shouldn't be used to crosswalk age groups that
  ## go up to 49
  df2 <- df2[nid != 95336 | substr(age_group,4,5) != "49"]

  ## cut out any counterfactual extractions with tiny sample sizes
  df2 <- df2[var == "mod_contra" & sample_size >= 20,c(id.vars,"mean"),with=F]
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
  counter <- melt(counter,id.vars=c(id.vars))
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
  cat("Adjusting points in age group", this.age, "\n"); flush.console()

  ## store regression coefficients and corresponding standard errors for each counterfactual
  currmar_coeff <- as.numeric(coeffs[rn == paste0("currmar:",this.age), "Estimate", with=F])
  evermar_coeff <- as.numeric(coeffs[rn == paste0("evermar:",this.age), "Estimate", with=F])
  currmar_se <- as.numeric(coeffs[rn == paste0("currmar:",this.age), "Std. Error", with=F])
  evermar_se <- as.numeric(coeffs[rn == paste0("evermar:",this.age), "Std. Error", with=F])

  ## adjust data and variance based on the counterfactual re-extraction regression
  df[age_group == this.age, data := data - (currmar_only * currmar_coeff)]
  df[age_group == this.age, data := data - (evermar_only * evermar_coeff)]
  df[, variance := variance + (currmar_only^2 * currmar_se^2)]
  df[, variance := variance + (evermar_only^2 * evermar_se^2)]
}


# REFORMATTING ----------------------------------------------------------------

# First reset all study level covariates to predict as the gold standard
# Also save the originals for comparison
df[, currmar_og := currmar_only]
df[, currmar_only := 0]
df[, evermar_og := evermar_only]
df[, evermar_only := 0]

## transform data and variance back to normal space
df[, data := inv.logit(data)]
df[, variance := variance / (1/(data*(1-data)))^2] # using reverse delta method for logit-space
df[,me_name := "mod_contra"]

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
  gpr <- model_load(mod_contra_run, 'raked')
  tosplit <- merge(tosplit,gpr[,c(id.vars,"gpr_mean"),with=F],by=id.vars,all.x=T)
  tosplit <- merge(tosplit,pops,by=c(id.vars),all.x=T)

  ## Age-split: adjust age-specific ST-GPR estimates such that their weighted average yields
  ## the extracted aggregate-age data
  tosplit[,poptotal := sum(population),by=survey_id]
  tosplit[,adj_factor := data/sum(gpr_mean*population/poptotal),by=survey_id]
  tosplit[,presplit_data := data]
  tosplit[,data := gpr_mean*adj_factor]

  ## if some data goes above 1, reset to halfway between 1 and the largest data observed < 1
  tosplit[data >= 1]
  tosplit[data >= 1, data := 1 - tosplit[data < 1,.5*(1-max(data))]]

  ## split sample size and use to increase variance from the split (though doesn't incorporate
  ## any additional uncertainty from the split itself)
  tosplit[,presplit_sample_size := sample_size]
  tosplit[,sample_size := sample_size*population/poptotal]
  tosplit[,variance := data*(1-data)/sample_size]

  ## data is NA for studies from before the first years at which we model
  tosplit[is.na(data)]
  tosplit <- tosplit[!is.na(data)]

  ## format and merge back on to age-specific data
  tosplit[,c("survey_id") := NULL]
  tosplit[,cv_split := 1]
  df <- rbind(df,tosplit, fill=T)
}


# OUTPUT RESULTS -----------------------------------------------------------

## check for oddities
x <- df[,.N,by=c("nid",id.vars)]
x[N>1]

## output dataset and crosswalk coefficients
write.csv(df, file.path(out.dir, paste0("mod_contra_xwalked",out.end,".csv")),row.names=F)
write.csv(coeffs, file.path(out.dir, paste0("mod_contra_xwalked",out.end,"_coefs.csv")),row.names=F)
