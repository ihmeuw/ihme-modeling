######################################################
## Purpose: Space-time and GPR prep for sex model
#######################################################

rm(list=ls())

## Initializing R, libraries
library(RMySQL)
library(foreign)
library(haven)
library(data.table)
library(readstata13)
library(plyr)
library(devtools)
library(methods)
library(argparse)
library(mortdb, lib = "FILEPATH")

# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of age-sex')
parser$add_argument('--version_5q0_id', type="integer", required=TRUE,
                    help='The 5q0 version for this run of age-sex')
parser$add_argument('--version_ddm_id', type="integer", required=TRUE,
                    help='The DDM version for this run of age-sex')
args <- parser$parse_args()

version_id <- args$version_id
version_5q0_id <- args$version_5q0_id
version_ddm_id <- args$version_ddm_id

username <- Sys.getenv("USER")

# existing directories needed
code_dir <- "FILEPATH"
output_dir <- "FILEPATH"
output_5q0_dir <- "FILEPATH"
output_ddm_dir <- "FILEPATH"

# get 5q0 data density
data_density_5q0_file <- paste0(output_5q0_dir, "/data/assigned_hyperparameters.csv")

# create output directories
dir.create(paste0(output_dir), showWarnings = FALSE)
dir.create(paste0(output_dir, "/sex_model"), showWarnings = FALSE)
dir.create(paste0(output_dir, "/sex_model/stage_2"), showWarnings = FALSE)

## load space-time functions
source(paste0(code_dir, "/age-sex/space_time.r"))

## loading in location lists
regs <- data.table(read.csv(paste0(output_dir, "/as_cplus_for_stata.csv")))
regs <- regs[,.(ihme_loc_id, region_name)]

## loading space time location hierarchy
st_locs <- data.table(read.csv(paste0(output_dir, "/st_locs.csv")))

## convenience function
logit <- function(x){
  return(log(x/(1-x)))
}

# Data Processing -------------------------------------

## Reading in files: input data, births, 5q0 estimates, ddm completeness estimates, location data
d <- data.table(read.dta(paste0(output_dir, "/input_data_original.dta")))
d <- d[!is.na(q_u5)]
births <- fread(paste0(output_dir, "/live_births.csv"))
child <- fread(paste0(output_5q0_dir, "/upload_estimates.csv"))
child <- child[estimate_stage_id == 3, ]
vr_list <- fread(paste0(output_ddm_dir, "/data/child_completeness_list.csv"))
location_data <- data.table(read.csv(paste0(output_dir, "/as_locs_for_stata.csv")))

## Read in stage 1 estimates
s1 <- as.data.table(read.csv(paste0(output_dir,"/sex_model/stage_1/sex_model_stage1.csv")))
setnames(s1, "logit_q5_sexratio_pred", "pred_logitratio_s1")

## format
st_data <- copy(d)
st_data[exclude_sex_mod!=0, exclude:="do not keep"]
st_data[exclude!="do not keep", exclude:="keep"]
st_data <- st_data[,.(region_name, ihme_loc_id, year, sex, exclude, q_u5, source, broadsource)]
st_data <- st_data[sex!="both"]
st_data <- dcast.data.table(st_data, region_name+ihme_loc_id+year+exclude+source+broadsource~sex, value.var="q_u5")
st_data[,logit_q5_sexratio := male/female]
st_data[logit_q5_sexratio>=1.5 | logit_q5_sexratio<=0.8, exclude:="do not keep"]
st_data[logit_q5_sexratio>=1.5 | logit_q5_sexratio<=0.8, exclude_sex_mod:=4]

st_data[,logit_q5_sexratio:=(logit_q5_sexratio-0.8)/(1.5-0.8)]
st_data[,logit_q5_sexratio:=logit(logit_q5_sexratio)]

## save the empirical sex ratio data for later gpr use
for_gpr <- copy(st_data)
st_data <- st_data[,c("source", "female", "male"):=NULL]
st_data[,year:=round(year)]

s1 <- s1[,c("q_u5_both", "q_u5_sexratio_pred", "q_u5_female", "q_u5_male"):=NULL]
s1[,year:=floor(year)]

st_data <- merge(st_data, s1, all.x=T, by=c("ihme_loc_id", "year"))

# Get data density
keep_param_cols <- c("ihme_loc_id", "complete_vr_deaths", "scale", "amp2x",
                     "lambda", "zeta", "best")
params <- fread(data_density_5q0_file)
params[, lambda := lambda * 2]
params <- params[, keep_param_cols, with = FALSE]

write.csv(params, paste0(output_dir, "/sex_model/stage_2/sex_model_params.txt"), row.names=F)

# Space-Time -------------------------------------------------

# calculating residual
st_data[,resid:=pred_logitratio_s1 - logit_q5_sexratio]

# exclude residuals for outliered points
st_data[exclude=="do not keep", resid:=NA]

# get a square dataset for spacetime input
sq <- copy(s1)
sq <- merge(sq, regs, all.x=T, by="ihme_loc_id")
sq <- sq[,.(ihme_loc_id, year, region_name)]
st_data <- merge(sq, st_data, all.x=T, all.y=T, by=c("ihme_loc_id","year", "region_name"))

st_data <- st_data[,c("exclude", "logit_q5_sexratio", "pred_logitratio_s1"):=NULL]

# one residual for each country-year with data for space-time
st_data <- ddply(st_data, .(ihme_loc_id,year),
                 function(x){
                   data.frame(region_name = x$region_name[1],
                              ihme_loc_id = x$ihme_loc_id[1],
                              year = x$year[1],
                              resid = mean(x$resid))
                 })

## merging on space time location hierarchy
st_data <- as.data.table(st_data)
st_data <- st_data[,"region_name":=NULL]
st_data <- merge(st_data, st_locs, by="ihme_loc_id", all.x=T, allow.cartesian = T)

write.csv(st_data, paste0(output_dir, "/sex_model/stage_2/spacetime_data.csv"), row.names=F)

st_pred <- resid_space_time(data=st_data, sex_mod=T, params=params, print=F)
st_pred <- as.data.table(st_pred)

## processing space-time results
st_pred <- data.table(st_pred)
st_pred[,year:=floor(year)]
st_pred[,"weight":=NULL]
st_pred <- st_pred[keep==1]
st_pred[,"keep":=NULL]

st_pred <- merge(st_pred, s1, all=T, by=c("ihme_loc_id", "year"))
st_pred[,pred_logitratio_s2:=pred_logitratio_s1-pred.2.resid]

#########################################################
## make a separate results file for graphing
#########################################################

s2_est <- copy(st_pred)
s2_est[,pred_ratio_s1:=exp(pred_logitratio_s1)/(1+exp(pred_logitratio_s1))]
s2_est[,pred_ratio_s1:=(pred_ratio_s1*0.7) + 0.8]
s2_est[,pred_ratio_s2:=exp(pred_logitratio_s2)/(1+exp(pred_logitratio_s2))]
s2_est[,pred_ratio_s2:=(pred_ratio_s2*0.7) + 0.8]
s2_est[,c("pred.2.resid", "pred_logitratio_s1", "pred_logitratio_s2"):=NULL]

# getting the birth sex ratio
if('ihme_loc_id' %in% names(births)) births[,ihme_loc_id:=NULL]
births <- merge(births, location_data[, c('location_id', 'ihme_loc_id')], by=c('location_id'))
births <- births[,c("location_name", "sex_id", "location_id"):=NULL]
births <- dcast.data.table(births, ihme_loc_id+year~sex, value.var="births")
births[,birth_sexratio:=male/female]
births <- births[,c("female", "male"):=NULL]

# getting the both sex 5q0
child[, year := floor(year_id)]
child <- merge(child, location_data[, c('location_id', 'ihme_loc_id')],
               by=c('location_id'))
child <- child[, c('ihme_loc_id', 'year', 'mean')]
setnames(child, "mean", "both_5q0")

# merging
s2_est <- merge(s2_est, births, all.x=T, by=c("ihme_loc_id", "year"))
s2_est <- merge(s2_est, child, all.x=T, by=c("ihme_loc_id", "year"))

# calculating sex specific 5q0
s2_est[,pred_f5q0_s1:= (both_5q0*(1+birth_sexratio))/(1+(pred_ratio_s1 * birth_sexratio))]
s2_est[,pred_m5q0_s1:= pred_f5q0_s1 * pred_ratio_s1]

s2_est[,pred_f5q0_s2:= (both_5q0*(1+birth_sexratio))/(1+(pred_ratio_s2 * birth_sexratio))]
s2_est[,pred_m5q0_s2:= pred_f5q0_s2 * pred_ratio_s2]

write.csv(s2_est, paste0(output_dir, "/sex_model/stage_2/stage_1.2_results_to_graph.csv"), row.names=F)

######################
## Prepping for GPR
######################

# merging emprical data back on
gpr_input <- copy(st_pred)
for_gpr <- for_gpr[,c("region_name", "male", "female") :=NULL]
for_gpr[,year:=round(year)]
gpr_input <- merge(gpr_input, for_gpr, all.x=T, all.y=T, by=c("ihme_loc_id", "year"))
gpr_input <- merge(gpr_input, regs, all.x=T, by="ihme_loc_id")

# Generate amplitude for GPR
write.csv(gpr_input, paste0(output_dir, "/sex_model/stage_2/smt_1.csv"))
gpr_input <- merge(gpr_input,
                   params[, c('ihme_loc_id', 'complete_vr_deaths')],
                   by=('ihme_loc_id'), all.x = T)
write.csv(gpr_input, paste0(output_dir, "/sex_model/stage_2/smt_2.csv"))

# Calculate the average amplitude value where data density >= 50
high_data_density <- copy(gpr_input[complete_vr_deaths >= 50 & year >= 1990 & exclude != "do not keep",])
write.csv(high_data_density, paste0(output_dir, "/sex_model/stage_2/smt_2a.csv"))
high_data_density[, diff := pred_logitratio_s2 - pred_logitratio_s1]
write.csv(high_data_density, paste0(output_dir, "/sex_model/stage_2/smt_2b.csv"))
variance_diff <- tapply(high_data_density$diff, high_data_density$ihme_loc_id, function(x) var(x, na.rm=T))
print(variance_diff)
amp_value <- mean(variance_diff)
print(amp_value)
gpr_input[, mse := amp_value]
write.csv(gpr_input, paste0(output_dir, "/sex_model/stage_2/smt_3.csv"))

## creating list of locations that will use the regional variance
data_per_loc <- list()
for(locs in unique(gpr_input$ihme_loc_id)){
  data_per_loc[[locs]] <- data.table(ihme_loc_id=locs, data_density=nrow(gpr_input[ihme_loc_id==locs & !is.na(logit_q5_sexratio) & exclude=="keep"]))
}
data_per_loc <- rbindlist(data_per_loc)
low_data_locs <- data_per_loc[data_density<3]
low_data_locs <- low_data_locs$ihme_loc_id

## creating national data variance
nat_var <- copy(gpr_input)
nat_var[,diff := logit_q5_sexratio - pred_logitratio_s2]
setkey(nat_var, ihme_loc_id)
nat_var <- nat_var[,.(year=year, data_var=var(diff, na.rm=T)), by=key(nat_var)]
setkey(nat_var, NULL)
nat_var <- unique(nat_var)

## creating regional data variance
reg_var <- copy(gpr_input)
reg_var[,diff := logit_q5_sexratio - pred_logitratio_s2]
setkey(reg_var, region_name)
reg_var <- reg_var[,.(year=year, region_data_var=var(diff, na.rm=T)), by=key(reg_var)]
setkey(reg_var, NULL)
reg_var <- unique(reg_var)

gpr_input <- merge(gpr_input, reg_var, by=c("year", "region_name"))
gpr_input <- merge(gpr_input, nat_var, by=c("year", "ihme_loc_id"))

gpr_input[ihme_loc_id %in% low_data_locs, data_var := region_data_var]
gpr_input[,region_data_var:= NULL]

if(nrow(gpr_input[is.na(data_var)])>0){
  print(paste0('Dropping rows missing data variance for these locations: ', unique(gpr_input[is.na(data_var),'ihme_loc_id'])))
  gpr_input <- gpr_input[!is.na(data_var)]
}

gpr_input[,data:=0]
gpr_input[!is.na(logit_q5_sexratio), data:=1]

gpr_input[,source_type:=broadsource]
gpr_input <- merge(gpr_input, vr_list, by=c("source_type", "year", "ihme_loc_id"), all.x=T)
nrow1 <- nrow(gpr_input)

missing <- gpr_input[!is.na(logit_q5_sexratio) & is.na(complete) & (source_type=="VR" | source_type=="DSP" | source_type=="SRS")]
gpr_input <- gpr_input[!(!is.na(logit_q5_sexratio) & is.na(complete) & (source_type=="VR" | source_type=="DSP" | source_type=="SRS"))]
temp <- copy(vr_list)
temp <- temp[,"year":=NULL]
temp <- temp[,.(complete=mean(complete)), by=c("ihme_loc_id", "source_type")]
temp[complete>0, complete:=1]

missing <- missing[,"complete":=NULL]
missing <- merge(missing, temp, all.x=T, by=c("ihme_loc_id", "source_type"))
gpr_input <- rbind(missing, gpr_input)

if(nrow(gpr_input)!=nrow1) stop("you lost some data!")

gpr_input[is.na(complete), complete:=0]

test <- gpr_input[!is.na(logit_q5_sexratio) & (source_type=="VR" | source_type=="DSP" | source_type=="SRS")]
if (nrow(test[is.na(complete)])!=0) stop ("you are missing completeness designations")

## further categorizing sources
gpr_input[source_type=="DSP" | source_type=="SRS",source_type:="VR"]
gpr_input[source_type!="VR", source_type:="other"]
gpr_input[source_type=="VR" & complete==1, source_type:="vr_unbiased"]
gpr_input[source_type=="VR" & complete==0, source_type:="vr_other"]

## saving gpr file
setnames(gpr_input, c("year", "source_type"), c("year_id", "category"))
write.csv(gpr_input, paste0(output_dir, "/sex_model/stage_2/gpr_input_file.csv"), row.names=F)