############################################################################################################################
# This script is used to adjust systematic bias in alternative case definitions using logit-transformation and MR-BRT analysis
###########################################################################################################################

library(dplyr)
library(plyr)
library(msm, lib.loc="FILEPATH")
library(metafor, lib.loc="FILEPATH")
library(readxl)
library(data.table)
library(readxl)
source("FILEPATH/get_location_metadata.R")
locs <- get_location_metadata(location_set_id = 35)
main_dir <- "FILEPATH"
draws <- paste0("draw_", 0:999)

######################################################################################################################
###### SETTING UP COVARIATES ######
###Ref: Population-based studies that ascertained cases from general populations using ultrasound
###Alt 1: Hospital data from all locations + Taiwan claims data
###Alt 2: Marketscan 2010-2014
###Alt 3: Marketscan 2000

#########################################################################################################################################
#STEP 1: MATCH REFERENCE AND ALTERNATIVE CASE DEFINITIONS
#########################################################################################################################################
# Adjust Alt 1 first
merge_type <- "between"
location_match <- "exact"
year_range <- 5
age_range <-5
covariate_name <- "cv_hospital"  
cause_path <- "Total_biliary/"
cause_name <- "digest_bile"

#CREATE COVARIATES AND TAG REFERENCE vs. ALTERNATIVE
df <- as.data.table(read.csv("FILEPATH FOR SEX-SPLIT DATA"))
df$cv_stringent <- ifelse((df$cv_ultrasonology==1),1,0)
df = within(df, {cv_hospital = ifelse( clinical_data_type=="inpatient" | location_name=="Taiwan", 1, 0)})

df$is_reference <- ifelse((df$cv_stringent==1),1,0)
df$is_alt <-ifelse(df$cv_hospital==1,1,0)


## FILL OUT MEAN/CASES/SAMPLE SIZE from EMMA's code
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS 
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

# BACK CALCULATE CASES AND SAMPLE SIZE FROM SE
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)


##AGGREGATE ITALY AND CHINA SUBNATIONALS
 df$cv_italy <- ifelse((df$nid==334464 | df$nid==334465  | df$nid== 334464 ),1,0)
 df$cv_china <- ifelse((df$nid==337619),1,0)
 
    aggregate_italy <- function(mark_dt){
      dt <- copy(mark_dt)
      marketscan_dt <- copy(dt[cv_italy==1])
       by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
       marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
       marketscan_dt[, `:=` (location_id = 67, location_name = "Japan", mean = cases/sample_size,
                             lower = NA, upper = NA)]
       z <- qnorm(0.975)
       marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
      marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
       marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
       marketscan_dt <- unique(marketscan_dt, by = by_vars)
       full_dt <- rbind(dt, marketscan_dt)
       return(full_dt)
     }
    aggregate_china <- function(mark_dt){
      dt <- copy(mark_dt)
      marketscan_dt <- copy(dt[cv_china==1])
      by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
      marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
      marketscan_dt[, `:=` (location_id = 6, location_name = "China", mean = cases/sample_size,
                             lower = NA, upper = NA)]
       z <- qnorm(0.975)
       marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
       marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
       marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
       marketscan_dt <- unique(marketscan_dt, by = by_vars)
       full_dt <- rbind(dt, marketscan_dt)
       return(full_dt) 
        }
    df <- aggregate_italy(df)
    df <- aggregate_china(df)

# REMOVE OUTLIER ROWS
df <- df[!is_outlier == 1 & measure %in% c("prevalence", "incidence")] 

# CREATE A WORKING DATAFRAME
wdf <- df[,c("measure", "location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
             "is_reference", "is_alt")]
covs <- select(df, matches("^cv_"))
wdf <- cbind(wdf, covs)
wdf <- join(wdf, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")

#CREATE INDICATORS FOR MERGING
if(location_match=="exact"){
  wdf$location_match <- wdf$location_id
} else if(location_match=="country"){
  wdf$location_match <- substr(wdf$ihme_loc_id,1,3)
} else if(location_match=="region"){
  wdf$location_match <- wdf$region_name
} else if(location_match=="super"){
  wdf$location_match <- wdf$super_region_name
} else{
  print("The location_match argument must be [exact, country, region, super]")
}

wdf$ihme_loc_abv <- substr(wdf$ihme_loc_id,1,3)

#AGE RANGE USING MID-POINT AGE
wdf$age_mid = (wdf$age_start + wdf$age_end)/2

#MID-YEAR CALCULATION
wdf$mid_year <- (wdf$year_start + wdf$year_end)/2


## Collapse to the desired merging ##
ref <- subset(wdf, is_reference == 1)
nref <- subset(wdf, is_alt == 1)

#MERGE AND MATCH REFERENCE AND ALTERNATIVE
  setnames(nref, c("mean","standard_error","cases","sample_size", "year_start", "year_end", "mid_year", "age_mid"), c("n_mean","n_standard_error","n_cases","n_sample_size", "n_year_start", "n_year_end", "n_mid_year", "n_age_mid"))
  wmean <- merge(ref, nref, by=c("location_match"), allow.cartesian = T)
  wmean[, c("ihme_loc_id.x", "ihme_loc_id.y", "super_region_name.x",  "ihme_loc_abv.x", "ihme_loc_abv.y", "location_id.x", "location_id.y")] <- NULL
  wmean <- wmean[abs(wmean$mid_year - wmean$n_mid_year) <= year_range, ] #set this to AND instead of OR to be stricter

  #Drop rows if sexes are not the same for sex and n_sex in a given row - then combine sex.x and sex.y into one sex column
  wmean <- wmean[wmean$sex.x==wmean$sex.y, ]
  wmean$sex <- wmean$sex.x
  wmean[, c("sex.x", "sex.y")] <- NULL
  
  #Drop duplicates
  wmean <- unique(wmean)
  

#AGGREGATE MATCHED ADMIN DATA
aggregate_admin_by_age <- function(mark_dt){
  dt <- copy(mark_dt)
  if (covariate_name == "cv_marketscan") marketscan_dt <- copy(dt[cv_marketscan.y==1])
  if (covariate_name == "cv_ms2000") marketscan_dt <- copy(dt[cv_ms2000.y==1])
  if (covariate_name == "cv_hospital") marketscan_dt <- copy(dt[cv_hospital.y==1])
  
  marketscan_dt$age_match[(marketscan_dt$age_start.x >= marketscan_dt$age_start.y) & (marketscan_dt$age_end.x <= marketscan_dt$age_end.y)] <-1 
  marketscan_dt <- subset(marketscan_dt, age_match==1)
  marketscan_dt[, id := .GRP, by = c("age_start.x", "age_end.x")]
  
  by_vars <- c("id", "n_year_start", "sex")
  marketscan_dt[, `:=` (n_cases = sum(n_cases), n_sample_size = sum(n_sample_size)), by = by_vars]
  marketscan_dt[, `:=` (n_mean = n_cases/n_sample_size, lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(n_standard_error), n_standard_error := sqrt(n_mean*(1-n_mean)/n_sample_size + z^2/(4*n_sample_size^2))]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)

  return(marketscan_dt)
}
wmean <- aggregate_admin_by_age(wmean)
wmean$age_start.x <-wmean$age_start.y
wmean$age_end.x <-wmean$age_end.y
wmean[, "id"] <-NULL


#GET THE RATIO
wmean$n_mean[is.na(wmean$n_mean)]  <- wmean$n_cases/wmean$n_sample_size
wmean$n_standard_error[is.na(wmean$n_standard_error)] <- sqrt(wmean$n_mean * (1-wmean$n_mean)/wmean$n_sample_size)

wmean$prev_ref <- wmean$mean
wmean$se_prev_ref <- wmean$standard_error
wmean$prev_alt <- wmean$n_mean
wmean$se_prev_alt <- wmean$n_standard_error

wmean <- subset(wmean, standard_error > 0 )
wmean <- subset(wmean, mean != 0 )
wmean <- subset(wmean, n_mean != 0 )


#LOGIT TRANSFORM THE META-REGRESSION DATA
# -- alternative
wmean$prev_logit_alt <- log(wmean$prev_alt / (1-wmean$prev_alt))
wmean$se_prev_logit_alt <- sapply(1:nrow(wmean), function(i) {
  prev_i <- wmean[i, prev_alt]
  prev_se_i <- wmean[i, se_prev_alt]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

# -- reference
wmean$prev_logit_ref <- log(wmean$prev_ref / (1-wmean$prev_ref))
wmean$se_prev_logit_ref <- sapply(1:nrow(wmean), function(i) {
  prev_i <- wmean[i, prev_ref]
  prev_se_i <- wmean[i, se_prev_ref]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

wmean$diff_logit <- wmean$prev_logit_alt - wmean$prev_logit_ref
wmean$se_diff_logit <- sqrt(wmean$se_prev_logit_alt^2 + wmean$se_prev_logit_ref^2)

#SUMMARIZE RESULTS 
mod <- rma(yi=diff_logit, sei=se_diff_logit, data=wmean, measure="RR")
results1 <- data.frame(variable = rownames(mod$b), mean=exp(mod$b), lower=exp(mod$ci.lb), upper=exp(mod$ci.ub))  
results1

#WRITE OUTPUT TO CSV  
write.csv(wmean1, "FILEPATH", row.names = F)
write.csv(results1, "FILEPATH", row.names = F)


#########################################################################################################
# Next, adjust Alt 2
location_match <- "exact"
year_range <-0       #SPECIFY YEAR RANGE FOR BETWEEN STUDY COMPARISON
covariate_name <- "cv_marketscan"   #set covariate of interest for crosswalk (alt)


#CREATE COVARIATES AND TAG REFERENCE vs. ALTERNATIVE
df <- as.data.table(read.csv("FILEPATH"))
df = within(df, {cv_marketscan = ifelse((year_start!=2000 & clinical_data_type=="claims" & location_name!="Taiwan"), 1, 0)})
df = within(df, {cv_hospital = ifelse( clinical_data_type=="inpatient" | location_name=="Taiwan", 1, 0)})

df$is_reference <- ifelse((df$cv_hospital==1),1,0)
df$is_alt <-ifelse(df$cv_marketscan==1,1,0)


## FILL OUT MEAN/CASES/SAMPLE SIZE 
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS 
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

# BACK CALCULATE CASES AND SAMPLE SIZE FROM SE
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)


# REMOVE OUTLIER ROWS
df <- df[!is_outlier == 1 & measure %in% c("prevalence", "incidence")] 


# CREATE A WORKING DATAFRAME
wdf <- df[,c("measure", "location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
             "is_reference", "is_alt")]
covs <- select(df, matches("^cv_"))
wdf <- cbind(wdf, covs)
wdf <- join(wdf, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")

#AGGREGATING MARKETSCAN DATA
aggregate_marketscan <- function(mark_dt){
  dt <- copy(mark_dt)
  if (covariate_name == "cv_marketscan") marketscan_dt <- copy(dt[cv_marketscan==1])
  if (covariate_name == "cv_ms2000") marketscan_dt <- copy(dt[cv_ms2000==1])
  
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(dt, marketscan_dt, fill=TRUE)
  return(full_dt)
}
wdf <- aggregate_marketscan(wdf)

#CREATE INDICATORS FOR MERGING
if(location_match=="exact"){
  wdf$location_match <- wdf$location_id
} else if(location_match=="country"){
  wdf$location_match <- substr(wdf$ihme_loc_id,1,3)
} else if(location_match=="region"){
  wdf$location_match <- wdf$region_name
} else if(location_match=="super"){
  wdf$location_match <- wdf$super_region_name
} else{
  print("The location_match argument must be [exact, country, region, super]")
}

wdf$ihme_loc_abv <- substr(wdf$ihme_loc_id,1,3)

#EXACT AGE MATCH
wdf$age_start_match <- wdf$age_start
wdf$age_end_match <- wdf$age_end

#MID-YEAR CALCULATION
wdf$mid_year <- (wdf$year_start + wdf$year_end)/2


#COLLAPSE TO THE DESIRED MERGING
ref <- subset(wdf, is_reference == 1)
nref <- subset(wdf, is_alt == 1)

#MERGE AND MATCH REFERENCE AND ALTERNATIVE
setnames(nref, c("mean","standard_error","cases","sample_size", "year_start", "year_end", "mid_year"), c("n_mean","n_standard_error","n_cases","n_sample_size", "n_year_start", "n_year_end", "n_mid_year"))
wmean <- merge(ref, nref, by=c("age_start_match", "age_end_match", "location_match", "measure"), allow.cartesian = T)
wmean[, c("ihme_loc_id.x", "ihme_loc_id.y", "super_region_name.x",  "ihme_loc_abv.x", "ihme_loc_abv.y", "location_id.x", "location_id.y")] <- NULL
wmean <- wmean[abs(wmean$mid_year - wmean$n_mid_year) <= year_range, ]

#DROP ROWS IF SEXES ARE NOT THE SAME FOR SEX AND N=SEX IN A GIVE ROW - THEN COMBINE SEX.X AND SEX.Y INTO ONE SEX COLUMN
wmean <- wmean[wmean$sex.x==wmean$sex.y, ]
wmean$sex <- wmean$sex.x
wmean[, c("sex.x", "sex.y")] <- NULL

#DROP DUPLICATES
wmean <- unique(wmean)

# Get the ratio!
wmean$n_mean[is.na(wmean$n_mean)]  <- wmean$n_cases/wmean$n_sample_size
wmean$n_standard_error[is.na(wmean$n_standard_error)] <- sqrt(wmean$n_mean * (1-wmean$n_mean)/wmean$n_sample_size)

wmean$prev_ref <- wmean$mean
wmean$se_prev_ref <- wmean$standard_error
wmean$prev_alt <- wmean$n_mean
wmean$se_prev_alt <- wmean$n_standard_error

wmean <- subset(wmean, standard_error > 0 )
wmean <- subset(wmean, mean != 0 )
wmean <- subset(wmean, n_mean != 0 )


# logit transform the meta-regression data
# -- alternative
wmean$prev_logit_alt <- log(wmean$prev_alt / (1-wmean$prev_alt))
wmean$se_prev_logit_alt <- sapply(1:nrow(wmean), function(i) {
  prev_i <- wmean[i, prev_alt]
  prev_se_i <- wmean[i, se_prev_alt]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

# -- reference
wmean$prev_logit_ref <- log(wmean$prev_ref / (1-wmean$prev_ref))
wmean$se_prev_logit_ref <- sapply(1:nrow(wmean), function(i) {
  prev_i <- wmean[i, prev_ref]
  prev_se_i <- wmean[i, se_prev_ref]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})


wmean$diff_logit <- wmean$prev_logit_alt - wmean$prev_logit_ref
wmean$se_diff_logit <- sqrt(wmean$se_prev_logit_alt^2 + wmean$se_prev_logit_ref^2)


#SUMMARIZE RESULTS 
mod <- rma(yi=diff_logit, sei=se_diff_logit, data=wmean, measure="RR")
results2 <- data.frame(variable = rownames(mod$b), mean=exp(mod$b), lower=exp(mod$ci.lb), upper=exp(mod$ci.ub))  
results2


#WRITE OUTPUT TO CSV  
write.csv(wmean2, "FILEPATH", row.names = F)
write.csv(results2, "FILEPATH", row.names = F)


#######################################################################################################################
# Lastly, adjust Alt 3 
covariate_name <- "cv_ms2000"   

df$is_reference <- ifelse((df$cv_hospital==1),1,0)
df$is_alt <-ifelse(df$cv_ms2000==1,1,0)


## FILL OUT MEAN/CASES/SAMPLE SIZE 
get_cases_sample_size <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(mean), mean := cases/sample_size]
  dt[is.na(cases) & !is.na(sample_size), cases := mean * sample_size]
  dt[is.na(sample_size) & !is.na(cases), sample_size := cases / mean]
  return(dt)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS 
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

# BACK CALCULATE CASES AND SAMPLE SIZE FROM SE
calculate_cases_fromse <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(cases) & is.na(sample_size) & measure == "prevalence", sample_size := (mean*(1-mean)/standard_error^2)]
  dt[is.na(cases) & is.na(sample_size) & measure == "incidence", sample_size := mean/standard_error^2]
  dt[is.na(cases), cases := mean * sample_size]
  return(dt)
}


df <- get_cases_sample_size(df)
df <- get_se(df)
df <- calculate_cases_fromse(df)


# REMOVE OUTLIER ROWS
df <- df[!is_outlier == 1 & measure %in% c("prevalence", "incidence")] 


# CREATE A WORKING DATAFRAME
wdf <- df[,c("measure", "location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
             "is_reference", "is_alt")]
covs <- select(df, matches("^cv_"))
wdf <- cbind(wdf, covs)
wdf <- join(wdf, locs[,c("location_id","ihme_loc_id","super_region_name","region_name")], by="location_id")

#AGGREGATING MARKETSCAN DATA
aggregate_marketscan <- function(mark_dt){
  dt <- copy(mark_dt)
  if (covariate_name == "cv_marketscan") marketscan_dt <- copy(dt[cv_marketscan==1])
  if (covariate_name == "cv_ms2000") marketscan_dt <- copy(dt[cv_ms2000==1])
  
  by_vars <- c("age_start", "age_end", "year_start", "year_end", "sex")
  marketscan_dt[, `:=` (cases = sum(cases), sample_size = sum(sample_size)), by = by_vars]
  marketscan_dt[, `:=` (location_id = 102, location_name = "United States", mean = cases/sample_size,
                        lower = NA, upper = NA)]
  z <- qnorm(0.975)
  marketscan_dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  marketscan_dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  marketscan_dt <- unique(marketscan_dt, by = by_vars)
  full_dt <- rbind(dt, marketscan_dt, fill=TRUE)
  return(full_dt)
}
wdf <- aggregate_marketscan(wdf)

#CREATE INDICATORS FOR MERGING
if(location_match=="exact"){
  wdf$location_match <- wdf$location_id
} else if(location_match=="country"){
  wdf$location_match <- substr(wdf$ihme_loc_id,1,3)
} else if(location_match=="region"){
  wdf$location_match <- wdf$region_name
} else if(location_match=="super"){
  wdf$location_match <- wdf$super_region_name
} else{
  print("The location_match argument must be [exact, country, region, super]")
}

wdf$ihme_loc_abv <- substr(wdf$ihme_loc_id,1,3)

#EXACT AGE MATCH
wdf$age_start_match <- wdf$age_start
wdf$age_end_match <- wdf$age_end

#MID-YEAR CALCULATION
wdf$mid_year <- (wdf$year_start + wdf$year_end)/2


#COLLAPSE TO THE DESIRED MERGING
ref <- subset(wdf, is_reference == 1)
nref <- subset(wdf, is_alt == 1)

#MERGE AND MATCH REFERENCE AND ALTERNATIVE
setnames(nref, c("mean","standard_error","cases","sample_size", "year_start", "year_end", "mid_year"), c("n_mean","n_standard_error","n_cases","n_sample_size", "n_year_start", "n_year_end", "n_mid_year"))
wmean <- merge(ref, nref, by=c("age_start_match", "age_end_match", "location_match", "measure"), allow.cartesian = T)
wmean[, c("ihme_loc_id.x", "ihme_loc_id.y", "super_region_name.x",  "ihme_loc_abv.x", "ihme_loc_abv.y", "location_id.x", "location_id.y")] <- NULL
wmean <- wmean[abs(wmean$mid_year - wmean$n_mid_year) <= year_range, ]

#DROP ROWS IF SEXES ARE NOT THE SAME FOR SEX AND N=SEX IN A GIVE ROW - THEN COMBINE SEX.X AND SEX.Y INTO ONE SEX COLUMN
wmean <- wmean[wmean$sex.x==wmean$sex.y, ]
wmean$sex <- wmean$sex.x
wmean[, c("sex.x", "sex.y")] <- NULL

#DROP DUPLICATES
wmean <- unique(wmean)

# Get the ratio!
wmean$n_mean[is.na(wmean$n_mean)]  <- wmean$n_cases/wmean$n_sample_size
wmean$n_standard_error[is.na(wmean$n_standard_error)] <- sqrt(wmean$n_mean * (1-wmean$n_mean)/wmean$n_sample_size)

wmean$prev_ref <- wmean$mean
wmean$se_prev_ref <- wmean$standard_error
wmean$prev_alt <- wmean$n_mean
wmean$se_prev_alt <- wmean$n_standard_error

wmean <- subset(wmean, standard_error > 0 )
wmean <- subset(wmean, mean != 0 )
wmean <- subset(wmean, n_mean != 0 )


# logit transform the meta-regression data
# -- alternative
wmean$prev_logit_alt <- log(wmean$prev_alt / (1-wmean$prev_alt))
wmean$se_prev_logit_alt <- sapply(1:nrow(wmean), function(i) {
  prev_i <- wmean[i, prev_alt]
  prev_se_i <- wmean[i, se_prev_alt]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})

# -- reference
wmean$prev_logit_ref <- log(wmean$prev_ref / (1-wmean$prev_ref))
wmean$se_prev_logit_ref <- sapply(1:nrow(wmean), function(i) {
  prev_i <- wmean[i, prev_ref]
  prev_se_i <- wmean[i, se_prev_ref]
  deltamethod(~log(x1/(1-x1)), prev_i, prev_se_i^2)
})


wmean$diff_logit <- wmean$prev_logit_alt - wmean$prev_logit_ref
wmean$se_diff_logit <- sqrt(wmean$se_prev_logit_alt^2 + wmean$se_prev_logit_ref^2)


#SUMMARIZE RESULTS 
mod <- rma(yi=diff_logit, sei=se_diff_logit, data=wmean, measure="RR")
results3 <- data.frame(variable = rownames(mod$b), mean=exp(mod$b), lower=exp(mod$ci.lb), upper=exp(mod$ci.ub))  
results3


#WRITE OUTPUT TO CSV  
write.csv(wmean3, "FILEPATH", row.names = F)
write.csv(results3, "FILEPATH", row.names = F)


########################################################################################################################################################################
# APPEND THREE DATASETS TOGETHER
 df1 <- as.data.table(wmean1)
 df2 <- as.data.table(wmean2)
 df3 <- as.data.table(wmean3)
 
 #Create function to order columns of crosswalk csv files consistently and drop unneeded columns
 df_vector <- list(df1,df2, df3)                                                                                  

 reorder_columns <- function(datasheet){
   ## set ordered list of columns for master crosswalk csv
   template_cols <- c("location_match", "age_start.y", "age_end.y", "age_start.x", "age_end.x", "sex", "diff_logit", "se_diff_logit", 
                      "cv_marketscan.x", "cv_ms2000.x", "cv_hospital.x","cv_marketscan.y", "cv_ms2000.y", "cv_hospital.y",
                      "nid.x", "year_start", "year_end", "nid.y", "n_year_start", "n_year_end", "age_start.y", "age_end.y")
   col_order <- template_cols
   ## find which column names are in the extraction template but not in your xwalk_master
   to_fill_blank <- c()  
   for(column in col_order){
     if(!column %in% names(xwalk_master)){
       to_fill_blank <- c(to_fill_blank, column)
     }}
   ## create blank column which will be filled in for columns not in your xwalk_master
   len <- length(xwalk_master$nid.x)
   blank_col <- rep.int(NA,len)
   
   ## for each column not found in your xwalk_master, add a blank column and rename it appropriately
   for(column in to_fill_blank){
     xwalk_master <- cbind(xwalk_master,blank_col)
     names(xwalk_master)[names(xwalk_master)=="blank_col"]<-column
   }
   ## for columns in xwalk_master but not in epi template or cv list, delete
   dt_cols <- names(xwalk_master)
   xwalk_master <- as.data.table(xwalk_master)
   for(col in dt_cols){
     if(!(col %in% col_order)){
       xwalk_master[, c(col):=NULL]
     }}
   ## reorder columns with template columns
   setcolorder(xwalk_master, col_order)
   ## return
   return(df1)   
 }    
 
 #Create master file with all crosswalks for cause, remove duplicate rows, write csv
 master_xwalk <- lapply(df_vector, reorder_columns) %>% rbindlist()

 #FOR INDIRECT COMPARISONS, APPROPRIATELY ASSIGN +1 and -1
 master_xwalk$cv_marketscan.x[master_xwalk$cv_marketscan.y==1] <- 1
 master_xwalk$cv_hospital.x[master_xwalk$cv_marketscan.y==1] <- -1
 
 master_xwalk$cv_ms2000.x[master_xwalk$cv_ms2000.y==1] <- 1
 master_xwalk$cv_hospital.x[master_xwalk$cv_ms2000.y==1] <- -1
 
 master_xwalk <- unique(master_xwalk)
 master_xwalk[, id := .GRP, by = c("nid.x", "nid.y")]
 
 write.csv(master_xwalk, "FILEPATH", row.names = F)


#########################################################################################################################################
#STEP 2: DATA PREP TO RUN MR-BRT AND APPLY PREDICTION TO THE ORIGINAL DATA 
#########################################################################################################################################

# Load original extracted data
# -- read in data frame
# -- specify how to identify reference studies
# -- identify mean, standard error and covariate variables
dat_original <- as.data.table(read.csv("FILEPATH FOR SEX-SPLIT DATA"))
 
#Set your reference if already not done
dat_original$is_reference <- ifelse((dat_original$cv_ultrasonology==1 ),1,0)
rem_data <- subset(dat_original, measure=="remission")
dat_original <- subset(dat_original, measure=="prevalence")

reference_var <- "is_reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- c("cv_hospital", "cv_ms2000", "cv_marketscan")  

tmp_metareg <- copy(master_xwalk)
setnames(tmp_metareg, c("cv_ms2000.y", "cv_marketscan.y", "cv_hospital.y"),c("cv_ms2000", "cv_marketscan", "cv_hospital"))

# logit transform the original data
#  -- SEs transformed using the delta method
 dat_original$mean_logit <- log(dat_original$mean / (1-dat_original$mean))
 dat_original$se_logit <- sapply(1:nrow(dat_original), function(i) {
   mean_i <- dat_original[i, "mean"]
   se_i <- dat_original[i, "standard_error"]
   deltamethod(~log(x1 / (1-x1)), mean_i, se_i^2)
 })
 


#########################################################################################################################################
#STEP 3: FIT THE MR-BRT MODEL 
#########################################################################################################################################

repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

trim <- 0.10 

#Run MR-BRT
fit1 <- run_mr_brt(
  output_dir =  "FILEPATH",
  model_label = paste0("step3_logit_",cause_name, "_", trim),
  data = tmp_metareg,
  overwrite_previous = TRUE,
  remove_x_intercept = TRUE,
  study_id = "id",
  method = "trim_maxL",
  trim_pct = trim, 
  mean_var = "diff_logit",
  se_var = "se_diff_logit",
     covs = list(   
     cov_info("cv_ms2000", "X"),
     cov_info("cv_marketscan", "X"),
     cov_info("cv_hospital", "X")
  ))


#########################################################################################################################################
#STEP 4: CREATE A RATIO PREDICTION FOR EACH OBSERVATION IN THE ORIGINAL DATA 
#########################################################################################################################################
eval(parse(text = paste0("predicted <- expand.grid(", paste0(cov_names, "=c(0, 1)", collapse = ", "), ")")))
predicted <- as.data.table(predict_mr_brt(fit1, newdata = predicted)["model_summaries"])

names(predicted) <- gsub("model_summaries.", "", names(predicted))
names(predicted) <- gsub("X_d_", "cv_", names(predicted))
predicted[, `:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
predicted[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2))), by=1:nrow(predicted)] 

predicted <- predicted %>%
  mutate(
    cv_ms2000 = X_cv_ms2000,
    cv_marketscan = X_cv_marketscan,
    cv_hospital = X_cv_hospital)
predicted <-as.data.table(predicted)


predicted[, (c("X_cv_ms2000","X_cv_marketscan", "X_cv_hospital", "Z_intercept", "Y_negp", "Y_mean_lo", "Y_mean_hi", "Y_mean_fe", "Y_negp_fe", "Y_mean_lo_fe", "Y_mean_hi_fe")) := NULL]
write.xlsx(predicted, "FILEPATH", row.names = F)

review_sheet_final <- merge(dat_original, predicted, by=gsub("d_", "cv_", cov_names))
review_sheet_final <-as.data.table(review_sheet_final)

setnames(review_sheet_final, "mean", "mean_orig")
review_sheet_final[Y_mean != predicted[2,Y_mean], `:=` (mean_logit = mean_logit - Y_mean, se_logit = sqrt(se_logit^2 + Y_se^2))]
review_sheet_final[Y_mean != predicted[2,Y_mean], `:=` (mean_new = inv.logit(mean_logit), standard_error_new = deltamethod(~exp(x1)/(1+exp(x1)), mean_logit, se_logit^2)), by = c("mean_logit", "se_logit")]
review_sheet_final[Y_mean != predicted[2,Y_mean], `:=` (lower_new = NA, upper_new = NA)]  
review_sheet_final[Y_mean == predicted[2,Y_mean], `:=` (mean_new = mean_orig, standard_error_new = standard_error)]
review_sheet_final[standard_error_new == "NaN", `:=` (standard_error_new = sqrt(standard_error^2 + Y_se_norm^2))]
review_sheet_final[, (c("Y_mean", "Y_se", "mean_logit", "se_logit", "Y_se_norm")) := NULL]
review_sheet_final[is.na(lower), uncertainty_type_value := NA]

review_sheet_final[, (c("standard_error", "upper", "lower")) := NULL]

setnames(review_sheet_final, "mean_new", "mean")
setnames(review_sheet_final, "standard_error_new", "standard_error")
setnames(review_sheet_final, "lower_new", "lower")
setnames(review_sheet_final, "upper_new", "upper")

review_sheet_final <- rbind(review_sheet_final,rem_data, fill=TRUE)

write.xlsx(review_sheet_final, "FILEPATH", row.names = F)



