###############################
## Date: 8/22/16
## Purpose: Generate age model results, run them through STGPR
## Steps:
##    1. Load in sex model results, generate sex-specific 5q0 from the ratio in the model and the birth sex ratio
##    2. Read in age model coefficents, generated in 02_fit models
##    3. Make stage 1 predictions, with and without random effects for input into space time
##            a) predictions are in conditional probability space, first put it into qx space and then put it into logit space
##    4. Run space time
## Inputs:
## Outputs:
###################################

## Initializing R, libraries

rm(list=ls())

library(RMySQL)
library(data.table)
library(foreign)
library(plyr)
library(haven)
library(assertable)
library(devtools)
library(methods)
library(argparse)


# Parse arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of age-sex')
parser$add_argument('--sex', type="character", required=TRUE,
                    help='The 5q0 version for this run of age-sex')
parser$add_argument('--version_5q0_id', type="integer", required=TRUE,
                    help='The 5q0 version for this run of age-sex')
parser$add_argument('--version_ddm_id', type="integer", required=TRUE,
                    help='The DDM version for this run of age-sex')
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
sex_arg <- args$sex
version_5q0_id <- args$version_5q0_id
version_ddm_id <- args$version_ddm_id

# Get file path
output_dir <- "FILEPATH"
output_5q0_dir <- "FILEPATH"
output_ddm_dir <- "FILEPATH"
data_density_file <- "FILEPATH"

age_sex_input_file <- "FILEPATH"
live_births_file <- "FILEPATH"
model_input_5q0_file <- "FILEPATH"
estimates_5q0_file <- "FILEPATH"
vr_child_completeness_file <- "FILEPATH"


# Load location data
st_locs <- get_spacetime_loc_hierarchy(prk_own_region = F, old_ap=F, gbd_year = 2017)
regs <- data.table(get_locations(level="all", gbd_year = 2017))[,.(ihme_loc_id, region_name)]
location_data <- data.table(get_locations(level="estimate", gbd_year = 2017))

# Define locations to assert
assert_ihme_loc_ids <- location_data[, ihme_loc_id]

# Define years to assert
assert_year_ids = c(1950:2017)
assert_mid_years = c(1950:2017) + 0.5

# Define age groups
age <- c("enn", "lnn", "pnn", "inf", "ch")


################
## Functions
################
## write function to get data density
get_data_density <- function() {
  library(foreign)

  st_locs <- data.table(get_spacetime_loc_hierarchy(gbd_year = 2017))[,.(ihme_loc_id, region_name)]

  # Read in age-sex input data
  data <- data.table(read.dta(age_sex_input_file))

  # Keep sex-specific data
  data <- data[sex==sex_arg]

  # Keep just the columns we need
  keep1 <- names(data)[grepl("exclude_", names(data))]
  keep <- c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", "q_enn", "q_lnn", "q_pnn", "q_inf", "q_ch", "q_u5", keep1)
  data <- data[,names(data) %in% keep, with=F]

  # Copy the data temporarily
  temp <- copy(data)

  #
  data <- melt.data.table(data, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", keep1),
                          measure.vars=c("q_enn", "q_lnn", "q_pnn", "q_ch", "q_inf"), variable.name = "age_group_name" ,value.name="qx_data")
  data[,age_group_name := gsub("q_", "", age_group_name)]


  temp <- temp[,c("q_enn", "q_lnn", "q_pnn", "q_ch", "q_inf") := NULL]
  temp <- melt.data.table(temp, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource"), measure.vars = keep1, variable.name="age_group_name", value.name="exclude")
  temp[,age_group_name := gsub("exclude_", "", age_group_name)]

  data <- data[,!names(data) %in% keep1, with=F]
  data <- data[!is.na(qx_data)]
  data <- merge(data, temp, by=c("region_name", "ihme_loc_id", "year", "sex", "age_group_name", "source", "broadsource"), all.x =T)
  data <- unique(data, by=c())

  data <- data[,"region_name":=NULL]
  data <- merge(data, st_locs, by="ihme_loc_id", allow.cartesian = T)
  data <- data[!is.na(qx_data)]

  data <- data[exclude==0]
  data <- data[sex!="both"]
  data_den <- copy(data)
  structure_dt <- data.table(expand.grid(region_name = unique(data$region_name),
                                         sex= unique(data$sex), age_group_name = unique(data$age_group_name)))
  data_den <- data[, n:=.N, by = .(region_name, sex, age_group_name)]
  data_den <- merge(structure_dt, data_den, by=c("region_name", "sex", "age_group_name"), all=T)
  data_den[is.na(n), n := 0]
  setnames(data_den, "age_group_name", "age")
  data_den <- data_den[,.(region_name, sex, age, n)]
  data_den <- unique(data_den)

  return(data_den)
}

data_den <- get_data_density()



###########################
## Set-up, Reading in Data
###########################
# Read in age model parameters
bin_params <- data.table(read.dta("FILEPATH"))
params <- data.table(read.dta("FILEPATH"))

# Read in
covs <- fread(model_input_5q0_file)
vr_list <- fread(vr_child_completeness_file)


# Read in age-sex dataset
data <- data.table(read.dta(age_sex_input_file))
data <- data[!is.na(q_u5)]


################################
## Sex specific 5q0 calculation
#################################
# Read in means of GPR sex model
compiled_sex_model_file <- "FILEPATH"
sexmod <- fread(compiled_sex_model_file)
assert_ids(sexmod,
           list(ihme_loc_id=assert_ihme_loc_ids, year=assert_mid_years),
           assert_combos = TRUE, assert_dups = TRUE, ids_only = TRUE,
           warn_only = FALSE, quiet = FALSE)

# Format sex model estimates
sexmod[,year_id:=floor(year)]
sexmod[,c("lower", "upper"):=NULL]

# Do some calculations to the mean
sexmod[,med := exp(med)/(1+exp(med))]
sexmod[,med := (med * 0.7) + 0.8]

# Read and format births
births <- fread(live_births_file)
births <- merge(births, location_data[, c('location_id', 'ihme_loc_id')],
                by=c('location_id'))
births <- births[,c("location_name", "sex_id", "location_id"):=NULL]
births <- dcast.data.table(births, ihme_loc_id+year~sex, value.var="births")
births[,birth_sexratio:=male/female]
births <- births[, c("both", "female", "male"):=NULL]
setnames(births, "year", "year_id")
assert_ids(births,
           list(ihme_loc_id=assert_ihme_loc_ids, year_id=assert_year_ids),
           assert_combos = TRUE, assert_dups = TRUE, ids_only = TRUE,
           warn_only = FALSE, quiet = FALSE)

# Read in and format both sex 5q0
child <- fread(estimates_5q0_file)
child <- child[estimate_stage_id == 3, ]
qu5 <- copy(child)
child <- merge(child, location_data[, c('location_id', 'ihme_loc_id')],
               by=c('location_id'))
child <- child[, c('ihme_loc_id', 'year_id', 'mean')]
setnames(child, "mean", "both")
assert_ids(child,
           list(ihme_loc_id=assert_ihme_loc_ids, year_id=assert_year_ids),
           assert_combos = TRUE, assert_dups = TRUE, ids_only = TRUE,
           warn_only = FALSE, quiet = FALSE)

# Merge sex model, births, and both-sex 5q0 estimates
sexmod <- sexmod[,year:=NULL]
sexmod <- merge(sexmod, births, all.x=T, by=c("ihme_loc_id", "year_id"))
sexmod <- merge(sexmod, child, all.x=T, by=c("ihme_loc_id", "year_id"))
assert_ids(sexmod,
           list(ihme_loc_id=assert_ihme_loc_ids, year_id=assert_year_ids),
           assert_combos = TRUE, assert_dups = TRUE, ids_only = TRUE,
           warn_only = FALSE, quiet = FALSE)

# calculating sex specific 5q0
sexmod[,female:= (both*(1+birth_sexratio))/(1+(med * birth_sexratio))]
sexmod[,male:= female * med]
sexmod[,c("med"):=NULL]

# Reshape
sexmod <- melt.data.table(sexmod, id.vars=c("ihme_loc_id", "year_id", "birth_sexratio"), measure.vars=c("both", "male", "female"), variable.name="sex", value.name="q_u5")

# Get a list of Group 1A HIV locations
hiv_location_information <- fread(paste0(output_dir, "/hiv_location_information.csv"))
hiv_group_1a_locations <- hiv_location_information[group == "1A", ihme_loc_id]

# Import the calculated hiv_free_ratios
hiv_free_ratios <- fread(paste0(output_dir, "/hiv_free_ratios.csv"))

# Merge HIV-free ratios
sexmod <- merge(sexmod, hiv_free_ratios, by=c("ihme_loc_id", "year_id", "sex"),
                all.x = T)


cdr <- copy(covs)
cdr <- cdr[,.(ihme_loc_id, year, hiv)]
cdr <- unique(cdr, by=c())
setnames(cdr, "hiv", "hiv_cdr")

hivfree <- copy(cdr)
hivfree$hiv_beta <- 1.3868677
hivfree <- hivfree[order(ihme_loc_id, year)]
hivfree[,year_id:=floor(year)]
hivfree[,year:=NULL]

sexmod <- merge(hivfree, sexmod, by=c("ihme_loc_id", "year_id"))

# Convert 5q0 to 5m0, this represent with-HIV
sexmod[,hiv_mx := -log(1-q_u5)/5]
# Calculate
sexmod[,no_hiv_mx := hiv_mx - (1 * hiv_cdr)]
# Get the hiv free mx by multiplying the HIV free ratio by the mx with HIV
sexmod[ihme_loc_id %in% hiv_group_1a_locations & !is.na(hiv_free_ratio), no_hiv_mx := hiv_mx * hiv_free_ratio]
# Convert HIV-free mx back to qx
sexmod[,no_hiv_qx := 1-exp(-5*no_hiv_mx)]
sexmod <- sexmod[,.(ihme_loc_id, year_id, no_hiv_qx, q_u5, sex, birth_sexratio)]

## finagling a merge back on
temp <- data.table(expand.grid(ihme_loc_id = sexmod$ihme_loc_id, age_group_name = c("enn", "lnn", "pnn", "inf", "ch")))
temp <- unique(temp)
sexmod <- merge(sexmod, temp, all=T, by="ihme_loc_id", allow.cartesian = T)

for_later <- copy(sexmod)
sexmod[age_group_name %in% c("enn", "lnn"), q_u5 := no_hiv_qx]

sexmod[,merge_log_q_u5:= log(q_u5)]
sexmod[,merge_log_q_u5:= round(merge_log_q_u5, 2)]

## keep only relevant sex
sexmod <- sexmod[sex==sex_arg]

d <- copy(sexmod)
d[,no_hiv_qx:=NULL]


#########################
## Age Model Parameters and Coefficients
#########################
## bin parameters
bin_params <- bin_params[,names(bin_params) %in% c("merge_log_q_u5", paste0("rebin_", age, "_", sex_arg)), with=F]
bin_params[,merge_log_q_u5:= as.numeric(merge_log_q_u5)]
bin_params[,merge_log_q_u5:= round(merge_log_q_u5, 2)]

# reshaping long by age
measures <- c()
for(age_group in age){
  temp <- paste0("rebin_", age_group, "_", sex_arg)
  measures <- c(measures, temp)
}
bin_params <- melt.data.table(bin_params, id.vars="merge_log_q_u5", meausure.vars= measures, value.name="rebin", variable.name="age_group_name")
bin_params[,age_group_name:=gsub("rebin_", "", age_group_name)]
bin_params[,age_group_name:=gsub(paste0("_", sex_arg), "", age_group_name)]

## non bin parameters

## just in case
keep1 <- names(params)[!grepl("nosim", names(params))]
params <- params[,names(params) %in% keep1, with=F]
keep <- c()
for(age_group in age){
  temp <- c("ihme_loc_id", grep(paste0(age_group, "_", sex_arg), names(params), value=T))
  keep <- c(keep, temp)
}

params <- params[,names(params) %in% keep, with=F]

# reshaping long by age
to_melt <- names(params)[!names(params)=="ihme_loc_id"]
params <- melt.data.table(params, id.vars="ihme_loc_id", measure.vars=to_melt) #long by everything
# wrangle variable names so we can go back wide by coeff
params[,variable:=as.character(variable)]
params[,age_group_name:=variable]
params[,age_group_name:=sapply(strsplit(age_group_name, "_"), "[[",2)]
exception <- unique(params$variable[grepl(("m_educcoeff"), params$variable) | grepl("s_compcoeff", params$variable)])
params[variable %in% exception, age_group_name:=sapply(strsplit(variable, "_"), "[[",3)]

params[,coeff:=variable]
params[,coeff:=sapply(strsplit(variable, "_"), "[[",1)]
params[variable %in% exception, coeff:=sapply(strsplit(variable, "_"), "[[",2)]

params[,variable:=NULL]
#reshaping back wide by coeff
params <- dcast.data.table(params, ihme_loc_id+age_group_name~coeff, value.var="value")

## checking that only inf and pnn have NA values for compcoeff and educoeff, and switching to 0 values
if(nrow(params[is.na(compcoeff) & age_group_name=="ch"])>0) stop("you have missing comecoeff values for child age group")
if(nrow(params[is.na(educcoeff) & age_group_name=="ch"])>0) stop("you have missing educoeff values for child age group")
if(nrow(params[is.na(hivcoeff) & age_group_name %in% c("ch", "inf", "pnn")])>0) stop("you have missing hivcoeff")
if(nrow(params[is.na(intercept)])>0) stop("you have missing intercept")
if(nrow(params[is.na(regbd)])>0) stop("regbd")
params[is.na(compcoeff), compcoeff := 0]
params[is.na(hivcoeff), hivcoeff := 0]
params[is.na(educcoeff), educcoeff := 0]

## Covariates
covs[, year_id:= floor(year)]
setnames(covs, "maternal_edu", "maternal_educ")
covs <- covs[,.(ihme_loc_id, year_id, hiv, maternal_educ)]
covs <- unique(covs)

## merging
d <- merge(d, bin_params, by=c("merge_log_q_u5", "age_group_name"), all.x=T)
d <- merge(d, params, by=c("ihme_loc_id", "age_group_name"))
d <- merge(d, covs, by=c("ihme_loc_id", "year_id"))


#############################
## Making Stage 1 Estimates
#############################
## Note, the equation below is correct even though we're using three different models for the different age groups
## For enn and lnn, we're only predicting the with REs, including the binned + loessed 5q0 REs, so compcoeff, educoeff, and hivcoeff are all 0 for those ages
## For pnn and infant, we're predicting with the REs above plus a fixed effect on hiv. so hivcoeff has a non-0 value but compcoeff and educoeff are 0.
## For ch (1-4), we are predicting with all REs and FEs seen in the equation below

## without RE for spacetime input
d[,pred_log_qx_s1:= intercept + rebin + hivcoeff*hiv + educcoeff*maternal_educ+1*compcoeff]
d[,pred_log_qx_s1:=exp(pred_log_qx_s1)]

## with RE for MAD calculation
d[,pred_log_qx_s1_wre:= intercept + rebin + hivcoeff*hiv + educcoeff*maternal_educ+1*compcoeff + reiso]
d[,pred_log_qx_s1_wre:=exp(pred_log_qx_s1_wre)]

## converting into qx space
d[,q_u5:=NULL]
d <- merge(d, for_later, by=c("ihme_loc_id", "year_id", "sex", "birth_sexratio", "age_group_name"), all.x=T)

d <- melt.data.table(d, id.vars=c("ihme_loc_id", "year_id", "age_group_name", "merge_log_q_u5", "birth_sexratio", "sex", "q_u5", "no_hiv_qx"),
                     measure.vars=c("pred_log_qx_s1", "pred_log_qx_s1_wre"))



d <- dcast.data.table(d, ihme_loc_id+year_id+birth_sexratio+sex+q_u5+variable+no_hiv_qx~age_group_name, value.var = "value")
d[,enn := no_hiv_qx * enn]
d[,lnn := (no_hiv_qx * lnn) / ((1-enn))]
d[,pnn := (q_u5 * pnn) / ((1-enn)*(1-lnn))]
d[,ch := (q_u5 * ch)  / ((1-enn)*(1-lnn)*(1-pnn))]
d[,inf:= (q_u5 * inf)]


# # reshaping back long, coverting to logit qx space
d <- melt.data.table(d, id.vars=c("ihme_loc_id", "year_id", "birth_sexratio", "sex", "q_u5", "variable"),
                      measure.vars=c("ch", "enn", "inf", "lnn", "pnn"), variable.name="age_group_name")
d[,value:=log(value)]
d <- dcast.data.table(d, ihme_loc_id+year_id+birth_sexratio+sex+q_u5+age_group_name ~variable, value.var="value")


## merging empirical data back on

# Reshape age long
data <- data[sex==sex_arg]
keep1 <- names(data)[grepl("exclude_", names(data))]
keep <- c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", "q_enn", "q_lnn", "q_pnn", "q_inf", "q_ch", "q_u5", keep1)
data <- data[,names(data) %in% keep, with=F]
temp <- copy(data)
data <- melt.data.table(data, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", keep1),
                        measure.vars=c("q_enn", "q_lnn", "q_pnn", "q_ch", "q_inf"), variable.name = "age_group_name" ,value.name="qx_data")
data[,age_group_name := gsub("q_", "", age_group_name)]
temp <- temp[,c("q_enn", "q_lnn", "q_pnn", "q_ch", "q_inf") := NULL]
temp <- melt.data.table(temp, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource"), measure.vars = keep1, variable.name="age_group_name", value.name="exclude")
temp[,age_group_name := gsub("exclude_", "", age_group_name)]
data <- data[,!names(data) %in% keep1, with=F]
data <- data[!is.na(qx_data)]
data <- merge(data, temp, by=c("region_name", "ihme_loc_id", "year", "sex", "age_group_name", "source", "broadsource"), all.x =T)
data <- unique(data, by=c())

data[,year_id:=round(year)]
data[,log_qx_data:=log(qx_data)]
d <- merge(d, data, all.x=T, by=c("ihme_loc_id", "year_id", "sex", "age_group_name"))


# Get data density
keep_param_cols <- c("ihme_loc_id", "complete_vr_deaths", "scale", "amp2x",
                     "lambda", "zeta", "best")
params <- fread(data_density_file)
params[, lambda := lambda * 2]
params <- params[, keep_param_cols, with = FALSE]

for(ages in unique(d$age_group_name)) {
  write.csv(params, "FILEPATH", row.names=F)
}

################
## Space-Time
################
st_data <- copy(d)

# calculating residual
st_data[,resid:=pred_log_qx_s1 - log_qx_data]

# don't want residuals for outliered points
st_data[exclude!=0, resid:=NA]

# get a square dataset for spacetime input

st_data <- st_data[,.(ihme_loc_id, year_id, resid, region_name, age_group_name)]
setnames(st_data,"year_id", "year")

# one residual for each country-year with data for space-time, so as not to give years with more data more weight --
st_data <- ddply(st_data, .(ihme_loc_id,year, age_group_name),
                 function(x){
                   data.frame(region_name = x$region_name[1],
                              ihme_loc_id = x$ihme_loc_id[1],
                              age_group_name = x$age_group_name[1],
                              year = x$year[1],
                              resid = mean(x$resid, na.rm=T))
                 })

## merging on fake regions
st_data <- as.data.table(st_data)
st_data[,"region_name":=NULL]
st_data <- merge(st_data, st_locs, all.x=T, by="ihme_loc_id", allow.cartesian=T)

## finding locations that have some age-sex combination with few enough datapoints
## so that we want to use super-regions in space-time
data_sparse_regs <- data_den[n<=6]
data_sparse_regs <- unique(data_sparse_regs$region_name)
print(data_sparse_regs)

## running space time function
write.csv(st_data, "FILEPATH", row.names=F)
st_pred <- list()
if(length(data_sparse_regs>0)){
  for(age_groups in age){
    temp <- copy(st_data)
    temp <- temp[age_group_name==age_groups]
    params <- fread("FILEPATH")
    st_pred[[age_groups]] <- resid_space_time(data=temp, use_super_regs=data_sparse_regs, print=F, sex_input=sex_arg, age_input= age_groups, params=params)
    st_pred[[age_groups]]$age_group_name=age_groups
  }
  st_pred <- rbindlist(st_pred, use.names=T)
} else if(length(data_sparse_regs)==0){
  for(age_groups in age){
    temp <- copy(st_data)
    temp <- temp[age_group_name==age_groups]
    params <- fread("FILEPATH")
    st_pred[[age_groups]] <- resid_space_time(data=temp, print=F, sex_input=sex_arg, age_input= age_groups, params=params)
    st_pred[[age_groups]]$age_group_name=age_groups
  }
  st_pred <- rbindlist(st_pred, use.names=T)
}

## processing space-time results
st_pred <- data.table(st_pred)
st_pred[,year := as.numeric(year)]
st_pred[,year:=floor(year)]
st_pred[,"weight":=NULL]
st_pred <- st_pred[keep==1]
st_pred[,"keep":=NULL]

## merge in previous model results
s1 <- copy(d)
s1 <- s1[,.(ihme_loc_id, year_id, pred_log_qx_s1, pred_log_qx_s1_wre, age_group_name)]
s1 <- unique(s1)
setnames(s1, "year_id", "year")

st_pred <- merge(st_pred, s1, all=T, by=c("ihme_loc_id", "year", "age_group_name"))

## calculate stage 2 estimates from residuals
st_pred[,pred_log_qx_s2:= pred_log_qx_s1 - pred.2.resid]

## saving stage 1 and 2  model
write.csv(st_pred, "FILEPATH", row.names=F)


####################
## Prepping for GPR
####################

gpr_input <- copy(st_pred)
setnames(gpr_input, "year", "year_id")

## merging in empirical data, regions
data <- data[,.(ihme_loc_id, year_id, year, exclude, qx_data, age_group_name, log_qx_data, source, broadsource)]
gpr_input <- merge(gpr_input, data, all.x=T, all.y=T, by=c("ihme_loc_id", "year_id", "age_group_name"))
gpr_input <- merge(gpr_input, regs, all.x=T, by="ihme_loc_id")

# Generate amplitude for GPR
gpr_input <- merge(gpr_input,
  params[, c('ihme_loc_id', 'complete_vr_deaths')],
  by=('ihme_loc_id'), all.x = T)
# Calculate the average amplitude value where data density >= 50
high_data_density <- copy(gpr_input[complete_vr_deaths >= 50 & year >= 1990 & exclude != "do not keep",])
high_data_density[, diff := pred_log_qx_s2 - pred_log_qx_s1]
variance_diff <- tapply(high_data_density$diff, high_data_density$ihme_loc_id, function(x) var(x, na.rm=T))
high_data_density <- high_data_density[ , .(varidifance_diff = var(diff, na.rm = T)), by = c('ihme_loc_id', 'age_group_name')]
high_data_density <- high_data_density[ , .(mse = mean(variance_diff, na.rm = T)), by = c('age_group_name')]
gpr_input <- merge(gpr_input,
  high_data_density[, c('age_group_name', 'mse')],
  by=('age_group_name'), all.x = T)


gpr_input[exclude!=0, year:= NA]
gpr_input[exclude!=0, qx_data:= NA]
gpr_input[exclude!=0, log_qx_data:= NA]
gpr_input[exclude!=0, broadsource:= NA]
gpr_input[exclude!=0, exclude:= NA]
gpr_input <- gpr_input[exclude==0 | is.na(exclude)]

setkey(gpr_input, ihme_loc_id, year_id, age_group_name, pred.2.resid, qx_data)
gpr_input <- unique(gpr_input, by=c())

## 3/18/2017 change in data variance calculation
## creating list of locations that will use the regional variance
data_per_loc <- list()
for(locs in unique(gpr_input$ihme_loc_id)){
  for(ages in unique(gpr_input$age_group_name)){
    data_per_loc[[paste0(locs, ages)]] <- data.table(ihme_loc_id=locs, age_group_name=ages, data_density=nrow(gpr_input[ihme_loc_id==locs & age_group_name==ages & !is.na(log_qx_data) & exclude=="keep"]))
  }
}
data_per_loc <- rbindlist(data_per_loc)
low_data_locs <- data_per_loc[data_density<3]
low_data_locs[,age_loc := paste(ihme_loc_id, age_group_name, sep="_")]
low_data_locs <- low_data_locs$age_loc

## creating national data variance
nat_var <- copy(gpr_input)
nat_var[,diff := log_qx_data - pred_log_qx_s2]
setkey(nat_var, ihme_loc_id, age_group_name)
nat_var <- nat_var[,.(year_id=year_id, data_var=var(diff, na.rm=T)), by=key(nat_var)]
setkey(nat_var, NULL)
nat_var <- unique(nat_var)

## creating regional data variance
reg_var <- copy(gpr_input)
reg_var[,diff := log_qx_data - pred_log_qx_s2]
setkey(reg_var, region_name, age_group_name)
reg_var <- reg_var[,.(year_id=year_id, region_data_var=var(diff, na.rm=T)), by=key(reg_var)]
setkey(reg_var, NULL)
reg_var <- unique(reg_var)

gpr_input <- merge(gpr_input, reg_var, by=c("year_id", "region_name", "age_group_name"))
gpr_input <- merge(gpr_input, nat_var, by=c("year_id", "ihme_loc_id", "age_group_name"))

gpr_input[,age_loc := paste(ihme_loc_id, age_group_name, sep="_")]

gpr_input[age_loc %in% low_data_locs, data_var := region_data_var]
gpr_input[,region_data_var:= NULL]
gpr_input[,age_loc := NULL]

if(nrow(gpr_input[is.na(data_var)])>0) stop("missing data varaince")


## other required variables
gpr_input[,data:=0]
gpr_input[!is.na(log_qx_data), data:=1]

## adapted from sex model
## using child completeness to assign vr_unbaised category for GPR

gpr_input[,source_type:=broadsource]
setnames(vr_list, "year", "year_id")
gpr_input <- merge(gpr_input, vr_list, by=c("source_type", "year_id", "ihme_loc_id"), all.x=T)

 nrow1 <- nrow(gpr_input) ## make sure you didn't lose any data
#

missing <- gpr_input[!is.na(log_qx_data) & is.na(complete) & (source_type=="VR" | source_type=="DSP" | source_type=="SRS")]
gpr_input <- gpr_input[!(!is.na(log_qx_data) & is.na(complete) & (source_type=="VR" | source_type=="DSP" | source_type=="SRS"))]
temp <- copy(vr_list)
temp <- temp[,"year_id":=NULL]
temp <- temp[,.(complete=mean(complete)), by=c("ihme_loc_id", "source_type")]
temp[complete>0, complete:=1]

missing <- missing[,"complete":=NULL]
missing <- merge(missing, temp, all.x=T, by=c("ihme_loc_id", "source_type"))
gpr_input <- rbind(missing, gpr_input)

if(nrow(gpr_input)!=nrow1) stop("you lost some data!")

test <- gpr_input[!is.na(log_qx_data) & (source_type=="VR" | source_type=="DSP" | source_type=="SRS")]
if (nrow(test[is.na(complete)])!=0) stop ("you are missing completeness designations")

## further categorizing sources
gpr_input[source_type=="DSP" | source_type=="SRS",source_type:="VR"]
gpr_input[source_type!="VR", source_type:="other"]
gpr_input[source_type=="VR" & complete==1, source_type:="vr_unbiased"]
gpr_input[source_type=="VR" & complete==0, source_type:="vr_other"]

## checking to make sure that source designations were all correctly assigned
if(nrow(gpr_input[is.na(source_type) & data==1])!=0) stop("missing source designation")

## setting age and sex
gpr_input[,sex:=sex_arg]



## saving gpr file, split because gpr is parallel by age and sex
setnames(gpr_input, c("year", "source_type"), c("year_id", "category"))
for(age_group in age){
  temp <- gpr_input[age_group_name==age_group]
  write.csv(temp, "FILEPATH", row.names=F)
}

write.csv(gpr_input, "FILEPATH", row.names=F)
