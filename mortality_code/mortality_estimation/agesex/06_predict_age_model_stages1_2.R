###############################
## Purpose: Generate age model results, run them through STGPR
###################################

## Initializing R, libraries

rm(list=ls())

library(RMySQL)
library(data.table)
library(foreign)
library(plyr)
library(haven)

if (Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "FILEPATH"
  dir <- paste0("FILEPATH")
  sex_arg <- "male" 
  local <- T
} else {
  username <- Sys.getenv("USER")
  root <- "FILEPATH"
  dir <- paste0("FILEPATH")
  sex_arg <- commandArgs()[3]
  age <- commandArgs()[4]
}



################
## Functions
################

load(paste0(root, "FILEPATH"))
betahiv16 <- model$coefficients$fixed[3]

load(paste0(root, "FILEPATH"))
betahiv15 <- model$coefficients$fixed[3]

## load central functions
source(paste0(root, "FILEPATH/get_locations.r"))
source(paste0(root, "FILEPATH/get_age_map.r"))
source(paste0(root, "FILEPATH/agg_results.R"))
source(paste0(dir, "FILEPATH/space_time.r"))
source(paste0(root, "FILEPATH/get_spacetime_loc_hierarchy.R"))

## loading in location lists
st_locs <- get_spacetime_loc_hierarchy(prk_own_region = F, old_ap=F)
regs <- data.table(get_locations(level="all"))[,.(ihme_loc_id, region_name)]

## write function to get data density
get_data_density <- function(){
  if (Sys.info()[1]=="Windows") root <- "J:/" else root <- "/home/j/"
  library(foreign)
  source(paste0(root, "FILEPATH/get_spacetime_loc_hierarchy.R"))
  
  st_locs <- data.table(get_spacetime_loc_hierarchy())[,.(ihme_loc_id, region_name)]
  
  data <- data.table(read.dta(paste0(root, "FILEPATH/input_data.dta")))
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

# Create and load in mean level sex model results

##compiling gpr files 
locs <- data.table(get_locations(level="estimate"))
locs <- locs[,ihme_loc_id]

missing_files <- c()
gpr <- list()
for (loc in locs){
  file <- paste0("FILEPATH/gpr_", loc, ".txt")
  if(file.exists(file)){ 
    gpr[[paste0(file)]] <- fread(file)
  } else {
    missing_files <- c(missing_files, file)
  }
}

if(length(missing_files)>0) stop("Files are missing.")
gpr <- rbindlist(gpr)
write.csv(gpr, "FILEPATH/00_compiled_mean_gpr.csv", row.names=F)
write.csv(gpr, paste0(root, "FILEPATH/00_compiled_mean_gpr.csv"), row.names=F)

sexmod <- copy(gpr)

births <- data.table(read_dta(paste0(root, "FILEPATH/births_gbd2016.dta")))
child <- fread(paste0(root, "FILEPATH/estimated_5q0_noshocks.txt"))
qu5 <- copy(child)

bin_params <- data.table(read.dta("FILEPATH/age_model_parameters_bins.dta"))
params <- data.table(read.dta("FILEPATH/age_model_parameters_other.dta"))

covs <- fread(paste0(root, "FILEPATH/prediction_input_data.txt"))
data <- data.table(read.dta(paste0(root, "FILEPATH/input_data.dta")))
vr_list <- fread(paste0(root,"FILEPATH/child_completeness_list.csv"))
data <- data[!is.na(q_u5)]

## defining age groups
age <- c("enn", "lnn", "pnn", "inf", "ch")

################################
## Sex specific 5q0 calculation
#################################

sexmod[,year_id:=floor(year)]
sexmod[,c("lower", "upper"):=NULL]

sexmod[,med := exp(med)/(1+exp(med))]
sexmod[,med := (med * 0.7) + 0.8]

# getting the birth sex ratio
births <- births[,c("location_name", "sex_id", "source", "location_id"):=NULL]
births <- dcast.data.table(births, ihme_loc_id+year~sex, value.var="births")
births[,birth_sexratio:=male/female]
births <- births[,c("both", "female", "male"):=NULL]
setnames(births, "year", "year_id")

# getting the both sex 5q0 
child <- child[,c("upper", "lower"):=NULL]
child[,year:=floor(year)]
setnames(child, "med", "both")
setnames(child, "year", "year_id")

# merging 
sexmod <- sexmod[,year:=NULL]
sexmod <- merge(sexmod, births, all.x=T, by=c("ihme_loc_id", "year_id"))
sexmod <- merge(sexmod, child, all.x=T, by=c("ihme_loc_id", "year_id"))

# calculating sex specific 5q0
sexmod[,female:= (both*(1+birth_sexratio))/(1+(med * birth_sexratio))]
sexmod[,male:= female * med]
sexmod[,c("med"):=NULL]

# Reshape
sexmod <- melt.data.table(sexmod, id.vars=c("ihme_loc_id", "year_id", "birth_sexratio"), measure.vars=c("both", "male", "female"), variable.name="sex", value.name="q_u5")

### for enn and lnn use HIV free 5q0 in prediction
locs15 <- get_locations(level="estimate", gbd_year=2015)
locs16 <- get_locations(level="estimate")
locs16 <- locs16[!locs16$ihme_loc_id %in% locs15$ihme_loc_id,]

locs16$hiv_beta <- betahiv16
locs15$hiv_beta <- betahiv15

betas <- rbind(locs15, locs16)
betas <- betas[,colnames(betas) %in% c("ihme_loc_id", "hiv_beta")]

cdr <- copy(covs)
cdr <- cdr[,.(ihme_loc_id, year, hiv)]
cdr <- unique(cdr, by=c())
setnames(cdr, "hiv", "hiv_cdr")
betas <- data.table(betas)

hivfree <- merge(betas, cdr, all=T, by="ihme_loc_id")
hivfree <- hivfree[order(ihme_loc_id, year)]
hivfree[,year_id:=floor(year)]
hivfree[,year:=NULL]

sexmod <- merge(hivfree, sexmod, by=c("ihme_loc_id", "year_id"))

sexmod[,hiv_mx := -log(1-q_u5)/5]
sexmod[,no_hiv_mx := hiv_mx - (1 * hiv_cdr)]
sexmod[,no_hiv_qx := 1-exp(-5*no_hiv_mx)]
sexmod <- sexmod[,.(ihme_loc_id, year_id, no_hiv_qx, q_u5, sex, birth_sexratio)]

## merge back on
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
params <- melt.data.table(params, id.vars="ihme_loc_id", measure.vars=to_melt) 
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

if(nrow(params[is.na(compcoeff) & age_group_name=="ch"])>0) stop("you have missing comecoeff values for child age group")
if(nrow(params[is.na(educcoeff) & age_group_name=="ch"])>0) stop("you have missing educoeff values for child age group")
if(nrow(params[is.na(hivcoeff) & age_group_name %in% c("ch", "inf", "pnn")])>0) stop("you have missing hivcoeff")
if(nrow(params[is.na(intercept)])>0) stop("you have missing intercept")
if(nrow(params[is.na(regbd)])>0) stop("regbd")
params[is.na(compcoeff), compcoeff := 0]
params[is.na(hivcoeff), hivcoeff := 0]
params[is.na(educcoeff), educcoeff := 0]

## Covariates
covs[,year_id:= floor(year)]
covs <- covs[,.(ihme_loc_id, year_id, hiv, maternal_educ)]
covs <- unique(covs)

## merging
d <- merge(d, bin_params, by=c("merge_log_q_u5", "age_group_name"), all.x=T)
d <- merge(d, params, by=c("ihme_loc_id", "age_group_name"))
d <- merge(d, covs, by=c("ihme_loc_id", "year_id"))

#############################
## Making Stage 1 Estimates
#############################

d[,pred_log_qx_s1:= intercept + rebin + hivcoeff*hiv + educcoeff*maternal_educ+1*compcoeff]
d[,pred_log_qx_s1:=exp(pred_log_qx_s1)]

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


d <- melt.data.table(d, id.vars=c("ihme_loc_id", "year_id", "birth_sexratio", "sex", "q_u5", "variable"), 
                      measure.vars=c("ch", "enn", "inf", "lnn", "pnn"), variable.name="age_group_name")
d[,value:=log(value)]
d <- dcast.data.table(d, ihme_loc_id+year_id+birth_sexratio+sex+q_u5+age_group_name ~variable, value.var="value")


## merging empirical data back on
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


hparams <- copy(data)
hparams <- data[!is.na(log_qx_data)]
hparams <- hparams[year>=1970 & exclude==0]

locs <- data.table(get_locations(level="estimate"))[,.(ihme_loc_id, region_name)]
locs <- expand.grid(ihme_loc_id = locs$ihme_loc_id, age_group_name = c("enn", "lnn", "pnn", "inf", "ch"))

hparams <- merge(hparams, locs, all=T, by=c("ihme_loc_id", "age_group_name"))

den <- subset(hparams)[,.N, by=c("ihme_loc_id", "age_group_name", "sex")]
hparams <- merge(hparams, den, by=c("ihme_loc_id", "age_group_name", "sex"))
hparams[,age_group_name := as.character(age_group_name)]

for(ages in unique(hparams$age_group_name)){
  temp <- copy(hparams)
  temp <- temp[age_group_name==ages]
  
  temp[N>=40, lambda := 1]
  temp[N>=40, zeta := 0.99]
  temp[N>=40, scale := 5]
  
  temp[N<40 & N>=30, lambda := 1.5]
  temp[N<40 & N>=30, zeta := 0.9]
  temp[N<40 & N>=30, scale := 10]
  
  temp[N<30 & N>=20, lambda := 2]
  temp[N<30 & N>=20, zeta := 0.8]
  temp[N<30 & N>=20, scale := 15]
  
  temp[grepl("_", ihme_loc_id) | N<20, lambda := 2]
  temp[grepl("_", ihme_loc_id) | N<20, zeta := 0.7]
  temp[grepl("_", ihme_loc_id) | N<20, scale := 20]
  
  temp[,amp2x := 1]
  temp[,best:=1]
  
  temp <- temp[,.(ihme_loc_id, scale, amp2x, lambda, zeta, best)]
  temp <- unique(temp, by=c())
  
  write.csv(temp, paste0(root, "FILEPATH/", sex_arg, "_", ages, "_model_params.txt"), row.names=F)
  write.csv(temp, paste0(root, "FILEPATH/", sex_arg, "_", ages, "_model_params", Sys.Date(), ".txt"), row.names=F)
}

################
## Space-Time
################
st_data <- copy(d)


st_data[,resid:=pred_log_qx_s1 - log_qx_data]


st_data[exclude!=0, resid:=NA]


st_data <- st_data[,.(ihme_loc_id, year_id, resid, region_name, age_group_name)]
setnames(st_data,"year_id", "year")

st_data <- ddply(st_data, .(ihme_loc_id,year, age_group_name), 
                 function(x){
                   data.frame(region_name = x$region_name[1],
                              ihme_loc_id = x$ihme_loc_id[1],
                              age_group_name = x$age_group_name[1],
                              year = x$year[1],
                              resid = mean(x$resid, na.rm=T))
                 })

st_data <- as.data.table(st_data)
st_data[,"region_name":=NULL]
st_data <- merge(st_data, st_locs, all.x=T, by="ihme_loc_id", allow.cartesian=T)

data_sparse_regs <- data_den[n<=6]
data_sparse_regs <- unique(data_sparse_regs$region_name)
print(data_sparse_regs)

st_pred <- list()
if(length(data_sparse_regs>0)){
  for(age_groups in age){
    temp <- copy(st_data)
    temp <- temp[age_group_name==age_groups]
    st_pred[[age_groups]] <- resid_space_time(data=temp, use_super_regs=data_sparse_regs, print=F, sex_input=sex_arg, age_input= age_groups)
    st_pred[[age_groups]]$age_group_name=age_groups
  }
  st_pred <- rbindlist(st_pred, use.names=T)
} else if(length(data_sparse_regs)==0){
  for(age_groups in age){
    temp <- copy(st_data)
    temp <- temp[age_group_name==age_groups]
    st_pred[[age_groups]] <- resid_space_time(data=temp, print=F, sex_input=sex_arg, age_input= age_groups)
    st_pred[[age_groups]]$age_group_name=age_groups
  }
  st_pred <- rbindlist(st_pred, use.names=T)
}

st_pred <- data.table(st_pred)
st_pred[,year := as.numeric(year)]
st_pred[,year:=floor(year)]
st_pred[,"weight":=NULL]
st_pred <- st_pred[keep==1]
st_pred[,"keep":=NULL]


s1 <- copy(d)
s1 <- s1[,.(ihme_loc_id, year_id, pred_log_qx_s1, pred_log_qx_s1_wre, age_group_name)]
s1 <- unique(s1)
setnames(s1, "year_id", "year")

st_pred <- merge(st_pred, s1, all=T, by=c("ihme_loc_id", "year", "age_group_name"))

## calculate stage 2 estimates from residuals
st_pred[,pred_log_qx_s2:= pred_log_qx_s1 - pred.2.resid]

## saving stage 1 and 2  model

write.csv(st_pred, paste0("FILEPATH/stage_12_age_pred_", sex_arg, ".csv"), row.names=F)
write.csv(st_pred, paste0"(FILEPATH/stage_12_age_pred_", sex_arg, ".csv"), row.names=F)


####################
## Prepping for GPR
####################

gpr_input <- copy(st_pred)
setnames(gpr_input, "year", "year_id")

## merging in empirical data, regions
data <- data[,.(ihme_loc_id, year_id, year, exclude, qx_data, age_group_name, log_qx_data, source, broadsource)]
gpr_input <- merge(gpr_input, data, all.x=T, all.y=T, by=c("ihme_loc_id", "year_id", "age_group_name"))
gpr_input <- merge(gpr_input, regs, all.x=T, by="ihme_loc_id")


nats <- copy(gpr_input)
locs <- data.table(get_locations(level="estimate"))
locs <- locs[level==3 | ihme_loc_id == "CHN_44533"]
locs <- locs$ihme_loc_id
nats <- nats[ihme_loc_id %in% locs]
nats[, diff := pred_log_qx_s2 - pred_log_qx_s1]
setkey(nats, ihme_loc_id, age_group_name)
nats <- nats[,.(year_id=year_id, mse=var(diff)), by=key(nats)]

gpr_input[,merge_ihme := sapply(strsplit(ihme_loc_id, "_"), "[[",1)]
gpr_input[merge_ihme=="CHN", merge_ihme:="CHN_44533"]
setnames(nats, "ihme_loc_id", "merge_ihme")
setkey(nats, NULL)
nats <- unique(nats)

gpr_input <- merge(gpr_input, nats, by=c("merge_ihme", "age_group_name", "year_id"), all.x=T)
gpr_input[,merge_ihme:=NULL]

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
    data_per_loc[[paste0(locs, ages)]] <- data.table(ihme_loc_id=locs, age_group_name=ages, data_density=nrow(gpr_input[ihme_loc_id==locs & age_group_name==ages & !is.na(log_qx_data) & exclude=="keep"]))
  }
}
data_per_loc <- rbindlist(data_per_loc)
low_data_locs <- data_per_loc[data_density<3]
low_data_locs[,age_loc := paste(ihme_loc_id, age_group_name, sep="_")]
low_data_locs <- low_data_locs$age_loc

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


test <- gpr_input[!is.na(log_qx_data) & (source_type=="VR" | source_type=="DSP" | source_type=="SRS")]
if (nrow(test[is.na(complete)])!=0) stop ("you are missing completeness designations")

gpr_input[source_type=="DSP" | source_type=="SRS",source_type:="VR"]
gpr_input[source_type!="VR", source_type:="other"]
gpr_input[source_type=="VR" & complete==1, source_type:="vr_unbiased"]
gpr_input[source_type=="VR" & complete==0, source_type:="vr_other"]

if(nrow(gpr_input[is.na(source_type) & data==1])!=0) stop("missing source designation")

gpr_input[,sex:=sex_arg]

setnames(gpr_input, c("year", "source_type"), c("year_id", "category"))
for(age_group in age){
  temp <- gpr_input[age_group_name==age_group]
  write.csv("FILEPATH/gpr_input_file_", sex_arg, "_", age_group, ".csv"), row.names=F)
}

write.csv(gpr_input, "FILEPATH/gpr_input_file_", sex_arg, ".csv"), row.names=F)
