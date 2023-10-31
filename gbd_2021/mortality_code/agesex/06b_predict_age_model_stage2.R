###########################################################################
## Purpose: Runs age model results through ST and preps results for GPR
## Steps:
##    1. Load in age model stage 1 results
##    2. Run space time
##    3. Prepare data for GPR
############################################################################

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
library(mortdb, lib.loc = "FILEPATH")

# Parse arguments
# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of age-sex')
parser$add_argument('--version_ddm_id', type="integer", required=TRUE,
                    help='The DDM version for this run of age-sex')
parser$add_argument('--sex', type="character", required=TRUE,
                    help='The sex')
parser$add_argument('--age_group_name', type="character", required=TRUE,
                    help='The age group')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help="GBD Year")
parser$add_argument('--end_year', type="integer", required=TRUE,
                    help="last year we produce estimates for")
parser$add_argument('--code_dir', type="character", required=TRUE,
                    help="Directory where age-sex code is cloned")
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
version_ddm_id <- args$version_ddm_id
sex_arg <- args$sex
age_arg <- args$age_group_name
gbd_year <- args$gbd_year
end_year <- args$end_year
code_dir <- args$code_dir

# Load central functions
source(paste0(code_dir, "/space_time.r"))

# Get file path
output_dir <- paste0("FILEPATH")
output_ddm_dir <- paste0("FILEPATH")
age_sex_input_file <- paste0("FILEPATH")

# Load location data
st_locs <- data.table(read.csv(paste0("FILEPATH")))
regs <- data.table(read.csv(paste0("FILEPATH")))[,.(ihme_loc_id, region_name)]
location_data <- data.table(read.csv(paste0("FILEPATH")))

# Define age groups
if(age_arg=="pna" | age_arg=="pnb"){
  age <- c("pnn", age_arg)
  } else{
    age <- age_arg
    }

################
## Data density
################

## write function to get data density
get_data_density <- function() {
  library(foreign)
  
  st_locs <- data.table(read.csv(paste0("FILEPATH")))[,.(ihme_loc_id, region_name)]
  
  # Read in age-sex input data
  data <- fread(age_sex_input_file)
  
  # Keep sex-specific data
  data <- data[sex==sex_arg]
  
  # Keep just the columns we need
  keep1 <- names(data)[grepl("exclude_", names(data))]
  keep <- c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", "q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_inf", "q_ch", "q_cha", "q_chb", "q_u5", keep1)
  data <- data[,names(data) %in% keep, with=F]
  
  # Copy the data temporarily
  temp <- copy(data)
  
  data <- melt.data.table(data, id.vars=c("region_name", "ihme_loc_id", "year", "sex", "source", "broadsource", keep1),
                          measure.vars=c("q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_inf", "q_ch", "q_cha", "q_chb"), variable.name = "age_group_name" ,value.name="qx_data")
  data[,age_group_name := gsub("q_", "", age_group_name)]
  
  
  temp <- temp[,c("q_enn", "q_lnn", "q_pnn", "q_pna", "q_pnb", "q_inf", "q_ch", "q_cha", "q_chb") := NULL]
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

if(age_arg=="pna" | age_arg=="pnb" ){
  data_den <- data_den[age==age_arg | age=="pnn"]
} else {
  data_den <- data_den[age==age_arg]
}

################
## Space-Time
################
st_data <- fread(paste0("FILEPATH"))
d<- fread(paste0("FILEPATH"))
data <- fread(paste0("FILEPATH"))
vr_list <- fread(paste0("FILEPATH"))

if(age_arg=="pna" | age_arg=="pnb" ){
  st_data <- st_data[age_group_name==age_arg | age_group_name=="pnn"]
  d <- d[age_group_name==age_arg | age_group_name=="pnn"]
  data <- data[age_group_name==age_arg | age_group_name=="pnn"]
} else {
  st_data <- st_data[age_group_name==age_arg]
  d <- d[age_group_name==age_arg]
  data <- data[age_group_name==age_arg]
}

params <- fread(paste0("FILEPATH"))

# calculating residual
st_data[,resid:=pred_log_qx_s1 - log_qx_data]

# don't want residuals for outliered points
st_data[exclude!=0, resid:=NA]

# get a square dataset for spacetime input
st_data <- st_data[,.(ihme_loc_id, year_id, resid, region_name, age_group_name)]
setnames(st_data,"year_id", "year")

# one residual for each country-year with data for space-time, so as not to give years with more data more weight --
st_data <- st_data[, .(region_name, resid = mean(resid, na.rm=T)), by=c('ihme_loc_id', 'year', 'age_group_name')]

## merging on fake regions and adjust year
st_data <- as.data.table(st_data)
st_data[,"region_name":=NULL]
st_data <- merge(st_data, st_locs, all.x=T, by="ihme_loc_id", allow.cartesian=T)
st_data[, year := year + 0.5]

## finding locations that have some age-sex combination with few enough datapoints
## so that we want to use super-regions in space-time
data_sparse_regs <- data_den[n<=6]
data_sparse_regs <- data_sparse_regs[sex == sex_arg]
data_sparse_regs[, c("sex", "n") := NULL]
if(nrow(data_sparse_regs)==0){
  data_sparse_regs <- c()
}
head(data_sparse_regs)

## running space time function
temp_st_data <- st_data[age_group_name==age_arg]
write.csv(temp_st_data, paste0("FILEPATH"), row.names=F)

start_time <- Sys.time()
st_pred <- list()
if(length(data_sparse_regs>0)){
  for(age_groups in age){
    temp <- copy(st_data)
    temp <- temp[age_group_name==age_groups]
    use_super_regs <- unique(data_sparse_regs[age == age_groups]$region_name)
    if(length(use_super_regs)==0) use_super_regs <- F
    params <- fread(paste0("FILEPATH"))
    st_pred[[age_groups]] <- resid_space_time(data = temp,
                                              gbd_year = gbd_year,
                                              use_super_regs = use_super_regs,
                                              print = F,
                                              sex_input = sex_arg,
                                              age_input = age_groups,
                                              params = params,
                                              max_year = end_year)
    st_pred[[age_groups]]$age_group_name=age_groups
  }
  st_pred <- rbindlist(st_pred, use.names=T)
} else if(length(data_sparse_regs)==0){
  for(age_groups in age){
    temp <- copy(st_data)
    temp <- temp[age_group_name==age_groups]
    use_super_regs <- levels(data_sparse_regs[age == age_groups]$region_name)
    params <- fread(paste0("FILEPATH"))
    st_pred[[age_groups]] <- resid_space_time(data = temp,
                                              gbd_year = gbd_year,
                                              print = F,
                                              sex_input = sex_arg,
                                              age_input = age_groups,
                                              params = params,
                                              max_year = end_year)
    st_pred[[age_groups]]$age_group_name=age_groups
  }
  st_pred <- rbindlist(st_pred, use.names=T)
}
end_time <- Sys.time()

time_diff <- end_time - start_time
time_diff

## processing space-time results
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

## saving stage 1 and 2  model
temp_st_pred <- st_pred[age_group_name==age_arg]
write.csv(temp_st_pred, paste0("FILEPATH"), row.names=F)

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
temp_gpr_input <- gpr_input[age_group_name==age_arg]
write.csv(temp_gpr_input, paste0("FILEPATH"), row.names = F)
gpr_input <- merge(gpr_input,
                   params[, c('ihme_loc_id', 'complete_vr_deaths')],
                   by=('ihme_loc_id'), all.x = T)
temp_gpr_input <- gpr_input[age_group_name==age_arg]
write.csv(temp_gpr_input, paste0("FILEPATH"), row.names = F)

# Calculate the average amplitude value where data density >= 50
high_data_density <- copy(gpr_input[complete_vr_deaths >= 50 & year >= 1990 & exclude ==0,])
temp_high_data_density <- high_data_density[age_group_name==age_arg]
write.csv(temp_high_data_density, paste0("FILEPATH"), row.names = F)

high_data_density[, diff := pred_log_qx_s2 - pred_log_qx_s1]
temp_high_data_density <- high_data_density[age_group_name==age_arg]
write.csv(temp_high_data_density, paste0("FILEPATH"), row.names = F)

variance_diff <- tapply(high_data_density$diff, high_data_density$ihme_loc_id, function(x) var(x, na.rm=T))
high_data_density <- high_data_density[ , .(varidifance_diff = var(diff, na.rm = T)), by = c('ihme_loc_id', 'age_group_name')]
temp_high_data_density <- high_data_density[age_group_name==age_arg]
write.csv(temp_high_data_density, paste0("FILEPATH"), row.names = F)

high_data_density <- high_data_density[ , .(mse = mean(varidifance_diff, na.rm = T)), by = c('age_group_name')]

missing_ages <- setdiff(unique(gpr_input$age_group_name), high_data_density$age_group_name)
if (length(missing_ages) > 0) {
  for (age in missing_ages) {
    temp <- data.table(age_group_name = age, mse = high_data_density[age_group_name=="pnn", mse])
    high_data_density <- rbind(high_data_density, temp)
  }
}

temp_high_data_density <- high_data_density[age_group_name==age_arg]
write.csv(temp_high_data_density, paste0("FILEPATH"), row.names = F)
gpr_input <- merge(gpr_input,
                   high_data_density[, c('age_group_name', 'mse')],
                   by=('age_group_name'), all.x = T)

assertable::assert_values(gpr_input, "mse", test = "not_na")

temp_gpr_input <- gpr_input[age_group_name==age_arg]
write.csv(temp_gpr_input, paste0("FILEPATH"), row.names = F)

gpr_input[exclude!=0, year:= NA]
gpr_input[exclude!=0, qx_data:= NA]
gpr_input[exclude!=0, log_qx_data:= NA]
gpr_input[exclude!=0, broadsource:= NA]
gpr_input[exclude!=0, exclude:= NA]
gpr_input <- gpr_input[exclude==0 | is.na(exclude)]

setkey(gpr_input, ihme_loc_id, year_id, age_group_name, pred.2.resid, qx_data)
gpr_input <- unique(gpr_input, by=c())

data_per_loc <- gpr_input[!is.na(log_qx_data) & (exclude=="keep" | exclude==0), .(data_density = .N), by=c('ihme_loc_id', 'age_group_name')]

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

# If regional data variance is null, use super-region
# First calculate super region variance
super_regions <- mortdb::get_locations(gbd_year = gbd_year)
super_reg <- merge(gpr_input, unique(super_regions[, .(region_name, super_region_name)]),
                   by='region_name')
super_reg[, diff := log_qx_data - pred_log_qx_s2]
setkey(super_reg, super_region_name, age_group_name)
super_reg <- super_reg[, .(year_id, super_data_var = var(diff, na.rm=T)), by=key(super_reg)]
setkey(super_reg, NULL)
super_reg <- unique(super_reg)

# If super region still missing, use global variance instead
global_var <- gpr_input[, .(year_id, global_data_var = var((log_qx_data - pred_log_qx_s2), na.rm=T)), by='age_group_name']
global_var <- unique(global_var)
super_reg <- merge(super_reg, global_var, by=c('age_group_name', 'year_id'))
super_reg[is.na(super_data_var), super_data_var := global_data_var]
super_reg[, global_data_var := NULL]

# Merge super region variance onto regional variance, replace NAs
reg_var <- merge(reg_var, unique(super_regions[, .(region_name, super_region_name)]),
                 by='region_name')
reg_var <- merge(reg_var, super_reg, by=c('super_region_name', 'age_group_name', 'year_id'))
reg_var[is.na(region_data_var), region_data_var := super_data_var]
reg_var[, c('super_region_name', 'super_data_var') := NULL]

# If national data variance is null, use regional variance
nat_var <- merge(nat_var, super_regions[, .(ihme_loc_id, region_name)], by='ihme_loc_id')
nat_var <- merge(nat_var, reg_var, by=c('region_name', 'age_group_name', 'year_id'))
nat_var[is.na(data_var), data_var := region_data_var]
nat_var[, c('region_name', 'region_data_var') := NULL]

# If regional data variance is null, use super-region
# First calculate super region variance
super_regions <- mortdb::get_locations(gbd_year=gbd_year)
super_reg <- merge(gpr_input, unique(super_regions[, .(region_name, super_region_name)]),
                   by='region_name')
super_reg[, diff := log_qx_data - pred_log_qx_s2]
setkey(super_reg, super_region_name, age_group_name)
super_reg <- super_reg[, .(year_id, super_data_var = var(diff, na.rm=T)), by=key(super_reg)]
setkey(super_reg, NULL)
super_reg <- unique(super_reg)

# If super region still missing, use global variance instead
global_var <- gpr_input[, .(year_id, global_data_var = var((log_qx_data - pred_log_qx_s2), na.rm=T)), by='age_group_name']
global_var <- unique(global_var)
super_reg <- merge(super_reg, global_var, by=c('age_group_name', 'year_id'))
super_reg[is.na(super_data_var), super_data_var := global_data_var]
super_reg[, global_data_var := NULL]

# Merge super region variance onto regional variance, replace NAs
reg_var <- merge(reg_var, unique(super_regions[, .(region_name, super_region_name)]),
                 by='region_name')
reg_var <- merge(reg_var, super_reg, by=c('super_region_name', 'age_group_name', 'year_id'))
reg_var[is.na(region_data_var), region_data_var := super_data_var]
reg_var[, c('super_region_name', 'super_data_var') := NULL]

# If national data variance is null, use regional variance
nat_var <- merge(nat_var, super_regions[, .(ihme_loc_id, region_name)], by='ihme_loc_id')
nat_var <- merge(nat_var, reg_var, by=c('region_name', 'age_group_name', 'year_id'))
nat_var[is.na(data_var), data_var := region_data_var]
nat_var[, c('region_name', 'region_data_var') := NULL]

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

## adapted from sex model
## using child completeness to assign vr_unbaised category for GPR

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

temp_gpr_input <- gpr_input[age_group_name==age_arg]
write.csv(temp_gpr_input, paste0("FILEPATH"), row.names=F)
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
temp <- gpr_input[age_group_name==age_arg]
write.csv(temp, paste0("FILEPATH"), row.names=F)
