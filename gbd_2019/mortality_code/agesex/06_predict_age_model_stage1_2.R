#####################################################################
## Purpose: Generate age model results, run them through STGPR
## Steps:
##    1. Load in sex model results, generate sex-specific 5q0 from
##          the ratio in the model and the birth sex ratio
##    2. Read in age model coefficents, generated in 02_fit models
##    3. Make stage 1 predictions, with and without random effects
##          for input into space time
##            a) predictions are in conditional probability space,
##            first put it into qx space and then put it into logit space
##    4. Run space time
######################################################################

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
library(splines)
library(mortdb, lib = "FILEPATH")

username <- Sys.getenv("USER")
code_dir <- "FILEPATH"

# Load central functions
source(paste0(code_dir, "age-sex/space_time.r"))

# Get arguments
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

version_id <- args$version_id
sex_arg <- args$sex
version_5q0_id <- args$version_5q0_id
version_ddm_id <- args$version_ddm_id

# Get file path
output_dir <- "FILEPATH"
output_5q0_dir <- "FILEPATH"
output_ddm_dir <- "FILEPATH"
data_density_5q0_file <- "FILEPATH"

age_sex_input_file <- paste0(output_dir, "/input_data_original.dta")
live_births_file <- paste0(output_dir, "/live_births.csv")
model_input_5q0_file <- paste0(output_5q0_dir, "/data/model_input.csv")
estimates_5q0_file <- paste0(output_5q0_dir, "/upload_estimates.csv")
vr_child_completeness_file <- paste0(output_ddm_dir, "/data/child_completeness_list.csv")

# Make folders
dir.create(paste0(output_dir), showWarnings = FALSE)
dir.create(paste0(output_dir, "/age_model"), showWarnings = FALSE)
dir.create(paste0(output_dir, "/age_model/stage_1"), showWarnings = FALSE)
dir.create(paste0(output_dir, "/age_model/stage_2"), showWarnings = FALSE)

# Load location data
st_locs <- data.table(read.csv(paste0(output_dir, "/st_locs.csv")))
regs <- data.table(read.csv(paste0(output_dir, "/as_cplus_for_stata.csv")))[,.(ihme_loc_id, region_name)]
location_data <- data.table(read.csv(paste0(output_dir, "/as_locs_for_stata.csv")))

# Define locations to assert
assert_ihme_loc_ids <- location_data[, ihme_loc_id]

assert_year_ids = c(1950:2019)
assert_mid_years = c(1950:2019) + 0.5

# Define age groups
age <- c("enn", "lnn", "pnn", "inf", "ch")


################
## Data density
################

## write function to get data density
get_data_density <- function() {
  library(foreign)
  
  st_locs <- data.table(read.csv(paste0(output_dir, "/st_locs.csv")))[,.(ihme_loc_id, region_name)]
  
  # Read in age-sex input data
  data <- data.table(read.dta(age_sex_input_file))
  
  # Keep sex-specific data
  data <- data[sex==sex_arg]
  
  # Keep just the columns we need
  keep1 <- names(data)[grepl("exclude_", names(data))]
  keep <- c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", "q_enn",
            "q_lnn", "q_pnn", "q_inf", "q_ch", "q_u5", keep1)
  data <- data[,names(data) %in% keep, with=F]
  
  # Copy the data temporarily
  temp <- copy(data)
  
  data <- melt.data.table(data, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", keep1),
                          measure.vars=c("q_enn", "q_lnn", "q_pnn", "q_ch", "q_inf"),
                          variable.name = "age_group_name", value.name="qx_data")
  data[,age_group_name := gsub("q_", "", age_group_name)]
  
  
  temp <- temp[,c("q_enn", "q_lnn", "q_pnn", "q_ch", "q_inf") := NULL]
  temp <- melt.data.table(temp, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource"),
                          measure.vars = keep1, variable.name="age_group_name", value.name="exclude")
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

# Read in age-sex dataset
data <- data.table(read.dta(age_sex_input_file))
data <- data[!is.na(q_u5)]

## Covariates
covs <- fread(model_input_5q0_file)
vr_list <- fread(vr_child_completeness_file)
covs[, year_id:= floor(year)]
covs <- covs[,.(ihme_loc_id, year_id, hiv, maternal_edu)]
covs <- unique(covs)

# get locations
locs <- fread(paste0(output_dir,'/as_locs_for_stata.csv'))
locs <- locs[level_all==1,.(ihme_loc_id,standard)]
fake_regions <- fread(paste0(output_dir,'/st_locs.csv'))
fake_regions <- fake_regions[keep==1]
setnames(fake_regions,'region_name','gbdregion')
fake_regions[,c('location_id','keep'):=NULL]
locs <- merge(locs,fake_regions,by='ihme_loc_id')

################################
## Sex specific 5q0 calculation
#################################

# Read in means of GPR sex model
compiled_sex_model_file <- paste0(output_dir,"/sex_model/gpr/00_compiled_mean_gpr.csv")
if (file.exists(compiled_sex_model_file)) {
  sexmod <- fread(compiled_sex_model_file)
} else {
  locs <- data.table(get_locations(level="estimate", gbd_year = 2019))
  locs <- locs[,ihme_loc_id]
  missing_files <- c()
  gpr <- list()
  for (loc in locs){
    file <- paste0(output_dir, "/sex_model/gpr/gpr_", loc, ".txt")
    if(file.exists(file)){
      gpr[[paste0(file)]] <- fread(file)
    } else {
      missing_files <- c(missing_files, file)
    }
  }
  if(length(missing_files)>0) stop("Files are missing.")
  gpr <- rbindlist(gpr)
  write.csv(gpr, compiled_sex_model_file, row.names=F)
  sexmod <- copy(gpr)
}
assert_ids(sexmod,
           list(ihme_loc_id=assert_ihme_loc_ids, year=assert_mid_years),
           assert_combos = TRUE, assert_dups = TRUE, ids_only = TRUE,
           warn_only = FALSE, quiet = FALSE)

# Format sex model estimates
sexmod[,year_id:=floor(year)]
sexmod[,c("lower", "upper"):=NULL]

sexmod[,med := exp(med)/(1+exp(med))]
sexmod[,med := (med * 0.7) + 0.8]

# Read and format births
births <- fread(live_births_file)
if('ihme_loc_id' %in% names(births)) births[,ihme_loc_id:=NULL]
births <- merge(births, location_data[, c('location_id', 'ihme_loc_id')],
                by=c('location_id'))
births <- births[,c("location_name", "sex_id", "location_id"):=NULL]
births <- dcast.data.table(births, ihme_loc_id+year~sex, value.var="births")
births[,birth_sexratio:=male/female]
births <- births[, c("female", "male","both"):=NULL]
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
sexmod <- melt.data.table(sexmod, id.vars=c("ihme_loc_id", "year_id", "birth_sexratio"),
                          measure.vars=c("both", "male", "female"), variable.name="sex",
                          value.name="q_u5")

# Get a list of Group 1A HIV locations
hiv_location_information <- fread(paste0(output_dir, "/hiv_location_information.csv"))
hiv_group_1a_locations <- hiv_location_information[group == "1A", ihme_loc_id]

# Import the calculated hiv_free_ratios
hiv_free_ratios <- fread(paste0(output_dir, "/hiv_free_ratios.csv"))

# Merge HIV-free ratios
sexmod <- merge(sexmod, hiv_free_ratios, by=c("ihme_loc_id", "year_id", "sex"),
                all.x = T)

cdr <- copy(covs)
cdr <- cdr[,c('ihme_loc_id', 'year_id', 'hiv')]
cdr <- unique(cdr, by=c())
setnames(cdr, "hiv", "hiv_cdr")

hivfree <- copy(cdr)
hivfree$hiv_beta <- 1.3868677
hivfree <- hivfree[order(ihme_loc_id, year_id)]
hivfree[,year_id:=floor(year_id)]

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

write.csv(sexmod, paste0(output_dir, "/age_model/stage_2/sc_", sex_arg, "_8.csv"), row.names=F)

temp <- data.table(expand.grid(ihme_loc_id = sexmod$ihme_loc_id,
                                age_group_name = c("enn", "lnn", "pnn", "inf", "ch")))
temp <- unique(temp)
sexmod <- merge(sexmod, temp, all=T, by="ihme_loc_id", allow.cartesian = T)

for_later <- copy(sexmod)
sexmod[age_group_name %in% c("enn", "lnn"), q_u5 := no_hiv_qx]

sexmod[,log_q_u5:= log(q_u5)]

## keep only relevant sex
sexmod <- sexmod[sex==sex_arg]

d <- copy(sexmod)
d[,no_hiv_qx:=NULL]

#########################
## Prep for predict
#########################

## merge on covariates
d <- merge(d, covs, by=c("ihme_loc_id", "year_id"))

## add s_comp=1 for hypothetically complete dataset
d[,s_comp:=1]

# log-transform HIV
d[,hiv:=log(hiv+0.000000001)]

# merge on locs for gbdregion
d <- merge(d, locs, by='ihme_loc_id')

## read in model fit objects
model_enn <- readRDS(paste0(output_dir,'/fit_models/age_model_object_enn_',sex_arg,'.RDS'))
model_lnn <- readRDS(paste0(output_dir,'/fit_models/age_model_object_lnn_',sex_arg,'.RDS'))
model_pnn <- readRDS(paste0(output_dir,'/fit_models/age_model_object_pnn_',sex_arg,'.RDS'))
model_inf <- readRDS(paste0(output_dir,'/fit_models/age_model_object_inf_',sex_arg,'.RDS'))
model_ch  <- readRDS(paste0(output_dir,'/fit_models/age_model_object_ch_',sex_arg,'.RDS'))

setnames(d,'log_q_u5','log_q5_var')

#############################
## Making Stage 1 Estimates
#############################

for(aa in age){
  temp <- d[age_group_name==aa]
  temp$pred_log_qx_s1 <- predict(get(paste0('model_',aa)), newdata=temp, allow.new.levels=T)
  d <- rbind(d,temp,fill=T)
}
d <- d[!is.na(pred_log_qx_s1),]
setnames(d, 'log_q5_var', 'log_q_u5')

# transform, keep naming for convenience
d[,pred_log_qx_s1:=exp(pred_log_qx_s1)]

## convert to qx space
d[,q_u5:=NULL]
d <- merge(d, for_later, by=c("ihme_loc_id", "year_id", "sex", "birth_sexratio", "age_group_name"), all.x=T)

d <- melt.data.table(d, id.vars=c("ihme_loc_id", "year_id", "age_group_name", "log_q_u5", "birth_sexratio", "sex", "q_u5", "no_hiv_qx"),
                     measure.vars=c("pred_log_qx_s1"))

d <- dcast.data.table(d, ihme_loc_id+year_id+birth_sexratio+sex+q_u5+variable+no_hiv_qx~age_group_name, value.var = "value")
d[,enn := no_hiv_qx * enn]
d[,lnn := (no_hiv_qx * lnn) / ((1-enn))]
d[,pnn := (q_u5 * pnn) / ((1-enn)*(1-lnn))]
d[,ch := (q_u5 * ch)  / ((1-enn)*(1-lnn)*(1-pnn))]
d[,inf:= (q_u5 * inf)]

## reshaping back long, coverting to logit qx space
d <- melt.data.table(d, id.vars=c("ihme_loc_id", "year_id", "birth_sexratio", "sex", "q_u5", "variable"),
                     measure.vars=c("ch", "enn", "inf", "lnn", "pnn"), variable.name="age_group_name")
d[,value:=log(value)]
d <- dcast.data.table(d, ihme_loc_id+year_id+birth_sexratio+sex+q_u5+age_group_name ~variable, value.var="value")

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
temp <- melt.data.table(temp, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource"),
                        measure.vars = keep1, variable.name="age_group_name", value.name="exclude")
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
params <- fread(data_density_5q0_file)
params[, lambda := lambda * 2]
params <- params[, keep_param_cols, with = FALSE]

for(ages in unique(d$age_group_name)) {
  write.csv(params, paste0(output_dir, "/age_model/stage_2/", sex_arg, "_", ages, "_model_params.txt"), row.names=F)
}

################
## Space-Time
################

st_data <- copy(d)

# calculate esidual
st_data[,resid:=pred_log_qx_s1 - log_qx_data]
st_data[exclude!=0, resid:=NA]

# get a square dataset for spacetime input
st_data <- st_data[,.(ihme_loc_id, year_id, resid, region_name, age_group_name)]
setnames(st_data,"year_id", "year")

# one residual for each country-year with data for space-time
st_data <- ddply(st_data, .(ihme_loc_id,year, age_group_name),
                 function(x){
                   data.frame(region_name = x$region_name[1],
                              ihme_loc_id = x$ihme_loc_id[1],
                              age_group_name = x$age_group_name[1],
                              year = x$year[1],
                              resid = mean(x$resid, na.rm=T))
                 })

## merge on fake regions
st_data <- as.data.table(st_data)
st_data[,"region_name":=NULL]
st_data <- merge(st_data, st_locs, all.x=T, by="ihme_loc_id", allow.cartesian=T)

data_sparse_regs <- data_den[n<=6]
if(nrow(data_sparse_regs)==0){
  data_sparse_regs <- c()
} else {
  data_sparse_regs <- levels(data_sparse_regs$region_name)
}
print(data_sparse_regs)

## run space time function
write.csv(st_data, paste0(output_dir, "/age_model/stage_2/spacetime_input_", sex_arg, ".csv"), row.names=F)
st_pred <- list()
if(length(data_sparse_regs>0)){
  for(age_groups in age){
    temp <- copy(st_data)
    temp <- temp[age_group_name==age_groups]
    params <- fread(paste0(output_dir, "/age_model/stage_2/", sex_arg, "_", ages, "_model_params.txt"))
    st_pred[[age_groups]] <- resid_space_time(data=temp, use_super_regs=data_sparse_regs, print=F,
                                              sex_input=sex_arg, age_input= age_groups, params=params)
    st_pred[[age_groups]]$age_group_name=age_groups
  }
  st_pred <- rbindlist(st_pred, use.names=T)
} else if(length(data_sparse_regs)==0){
  for(age_groups in age){
    temp <- copy(st_data)
    temp <- temp[age_group_name==age_groups]
    params <- fread(paste0(output_dir, "/age_model/stage_2/", sex_arg, "_", ages, "_model_params.txt"))
    st_pred[[age_groups]] <- resid_space_time(data=temp, print=F, sex_input=sex_arg, age_input= age_groups, params=params)
    st_pred[[age_groups]]$age_group_name=age_groups
  }
  st_pred <- rbindlist(st_pred, use.names=T)
}

## process space-time results
st_pred <- data.table(st_pred)
st_pred[,year := as.numeric(year)]
st_pred[,year:=floor(year)]
st_pred[,"weight":=NULL]
st_pred <- st_pred[keep==1]
st_pred[,"keep":=NULL]

## merge in previous model results
s1 <- copy(d)
s1 <- s1[,.(ihme_loc_id, year_id, pred_log_qx_s1, age_group_name)]
s1 <- unique(s1)
setnames(s1, "year_id", "year")

st_pred <- merge(st_pred, s1, all=T, by=c("ihme_loc_id", "year", "age_group_name"))

## calculate stage 2 estimates from residuals
st_pred[,pred_log_qx_s2:= pred_log_qx_s1 - pred.2.resid]

## save stage 1 and 2  model
write.csv(st_pred, paste0(output_dir, "/age_model/stage_1/stage_1_age_pred_", sex_arg, ".csv"), row.names=F)


####################
## Prep for GPR
####################

gpr_input <- copy(st_pred)
setnames(gpr_input, "year", "year_id")

## merge in empirical data, regions
data <- data[,.(ihme_loc_id, year_id, year, exclude, qx_data, age_group_name, log_qx_data, source, broadsource)]
gpr_input <- merge(gpr_input, data, all.x=T, all.y=T, by=c("ihme_loc_id", "year_id", "age_group_name"))
gpr_input <- merge(gpr_input, regs, all.x=T, by="ihme_loc_id")

# Generate amplitude for GPR
write.csv(gpr_input, paste0(output_dir, "/age_model/stage_2/smt_", sex_arg, "_1.csv"), row.names = F)
gpr_input <- merge(gpr_input,
                   params[, c('ihme_loc_id', 'complete_vr_deaths')],
                   by=('ihme_loc_id'), all.x = T)
write.csv(gpr_input, paste0(output_dir, "/age_model/stage_2/smt_", sex_arg, "_2.csv"), row.names = F)

# Calculate the average amplitude value where data density >= 50
high_data_density <- copy(gpr_input[complete_vr_deaths >= 50 & year >= 1990 & exclude != "do not keep",])
write.csv(high_data_density, paste0(output_dir, "/age_model/stage_2/smt_", sex_arg, "_2a.csv"), row.names = F)
high_data_density[, diff := pred_log_qx_s2 - pred_log_qx_s1]
write.csv(high_data_density, paste0(output_dir, "/age_model/stage_2/smt_", sex_arg, "_2b.csv"), row.names = F)
variance_diff <- tapply(high_data_density$diff, high_data_density$ihme_loc_id, function(x) var(x, na.rm=T))
high_data_density <- high_data_density[ , .(varidifance_diff = var(diff, na.rm = T)), by = c('ihme_loc_id', 'age_group_name')]
write.csv(high_data_density, paste0(output_dir, "/age_model/stage_2/smt_", sex_arg, "_2c.csv"), row.names = F)
high_data_density <- high_data_density[ , .(mse = mean(variance_diff, na.rm = T)), by = c('age_group_name')]
write.csv(high_data_density, paste0(output_dir, "/age_model/stage_2/smt_", sex_arg, "_2d.csv"), row.names = F)
gpr_input <- merge(gpr_input,
                   high_data_density[, c('age_group_name', 'mse')],
                   by=('age_group_name'), all.x = T)
write.csv(gpr_input, paste0(output_dir, "/age_model/stage_2/smt_", sex_arg, "_3.csv"), row.names = F)

gpr_input[exclude!=0, year:= NA]
gpr_input[exclude!=0, qx_data:= NA]
gpr_input[exclude!=0, log_qx_data:= NA]
gpr_input[exclude!=0, broadsource:= NA]
gpr_input[exclude!=0, exclude:= NA]
gpr_input <- gpr_input[exclude==0 | is.na(exclude)]

setkey(gpr_input, ihme_loc_id, year_id, age_group_name, pred.2.resid, qx_data)
gpr_input <- unique(gpr_input, by=c())

data_per_loc <- list()
for(locs in unique(gpr_input$ihme_loc_id)){
  for(ages in unique(gpr_input$age_group_name)){
    data_per_loc[[paste0(locs, ages)]] <- data.table(ihme_loc_id=locs, age_group_name=ages,
                                                      data_density=nrow(gpr_input[ihme_loc_id==locs & age_group_name==ages &
                                                                                  !is.na(log_qx_data) & exclude=="keep"]))
  }
}
data_per_loc <- rbindlist(data_per_loc)
low_data_locs <- data_per_loc[data_density<3]
low_data_locs[,age_loc := paste(ihme_loc_id, age_group_name, sep="_")]
low_data_locs <- low_data_locs$age_loc

## create national data variance
nat_var <- copy(gpr_input)
nat_var[,diff := log_qx_data - pred_log_qx_s2]
setkey(nat_var, ihme_loc_id, age_group_name)
nat_var <- nat_var[,.(year_id=year_id, data_var=var(diff, na.rm=T)), by=key(nat_var)]
setkey(nat_var, NULL)
nat_var <- unique(nat_var)

## create regional data variance
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

if(nrow(gpr_input[is.na(data_var)])>0) stop("missing data variance")

## other required variables
gpr_input[,data:=0]
gpr_input[!is.na(log_qx_data), data:=1]

gpr_input[,source_type:=broadsource]
setnames(vr_list, "year", "year_id")
gpr_input <- merge(gpr_input, vr_list, by=c("source_type", "year_id", "ihme_loc_id"), all.x=T)

nrow1 <- nrow(gpr_input)

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

gpr_input <- gpr_input[ihme_loc_id == "SAU" & source_type == "VR", complete := 0]
gpr_input <- gpr_input[ihme_loc_id == "IDN" & source_type == "VR" & year_id == 2010, complete := 0]
gpr_input <- gpr_input[ihme_loc_id == "MWI" & source_type == "VR", complete := 0]
gpr_input <- gpr_input[ihme_loc_id == "IND_43872" & source_type == "VR" & year_id %in% c(2014, 2016), complete := 0]
gpr_input <- gpr_input[ihme_loc_id == "IND_43891" & source_type == "VR" & year_id == 2016, complete := 0]

gpr_input[!is.na(log_qx_data) & is.na(complete) &
            (source_type=="VR" | source_type=="DSP" | source_type=="SRS"), complete := 0]

write.csv(gpr_input, paste0(output_dir, "/age_model/stage_1/stage_1_age_pred_", sex_arg, "_t1.csv"), row.names=F)
test <- gpr_input[!is.na(log_qx_data) & (source_type=="VR" | source_type=="DSP" | source_type=="SRS")]
if (nrow(test[is.na(complete)])!=0) stop ("you are missing completeness designations")

## further categorizing sources
gpr_input[source_type=="DSP" | source_type=="SRS",source_type:="VR"]
gpr_input[source_type!="VR", source_type:="other"]
gpr_input[source_type=="VR" & complete==1, source_type:="vr_unbiased"]
gpr_input[source_type=="VR" & complete==0, source_type:="vr_other"]

if(nrow(gpr_input[is.na(source_type) & data==1])!=0) stop("missing source designation")

## setting age and sex
gpr_input[,sex:=sex_arg]

setnames(gpr_input, c("year", "source_type"), c("year_id", "category"))
for(age_group in age){
  temp <- gpr_input[age_group_name==age_group]
  write.csv(temp, paste0(output_dir, "/age_model/stage_2/gpr_input_file_", sex_arg, "_", age_group, ".csv"), row.names=F)
}

write.csv(gpr_input, paste0(output_dir, "/age_model/stage_2/gpr_input_file_", sex_arg, ".csv"), row.names=F)
