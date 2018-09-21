
##############################
## Author: Rachel Kulikoff
## Date: August 10, 2016
## Purpose: Add Space-time and GPR to the sex model
## Steps: 
## Inputs:
##    - mean level prediction file
##    - input data
## Outputs: 
#################################

## Initializing R, libraries

rm(list=ls())

library(RMySQL)
library(foreign)
library(haven)
library(data.table)
library(readstata13)
library(plyr)


if (Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "FILEPATH"
  dir <- paste0("FILEPATH")
  local <- T
} else {
  username <- Sys.getenv("USER")
  root <- "FILEPATH"
  dir <- paste0("FILEPATH")
  local <- F
}

## load central functions
source(paste0(root, "FILEPATH"))
source(paste0(root, "FILEPATH"))
source(paste0(root, "FILEPATH"))
source(paste0(dir, "FILEPATH"))
source(paste0(root, "FILEPATH"))

## loading in location lists
regs <- data.table(get_locations(level="all"))
regs <- regs[,.(ihme_loc_id, region_name)]

## loading space time location hierarchy
st_locs <- get_spacetime_loc_hierarchy(prk_own_region = F, old_ap=F)

## convenience function
logit <- function(x){
  return(log(x/(1-x)))
}

#########################
## Data Processing
##########################

## Reading in files
d <- data.table(read.dta(paste0(root, "FILEPATH"))) 
d <- d[!is.na(q_u5)] 
births <- data.table(read_dta(paste0(root, "FILEPATH")))
child <- fread(paste0(root, "FILEPATH"))
vr_list <- fread(paste0(root,"FILEPATH"))



if(local==F){
  # compiling stage 1 model files
  locs <- data.table(get_locations(level="estimate"))
  locs <- locs[,ihme_loc_id]
  
  missing_files <- c()
  stage1 <- list()
  for (loc in locs){
    file <- paste0("FILEPATH")
    if(file.exists(file)){ 
      stage1[[paste0(file)]] <- fread(file)
    } else {
      missing_files <- c(missing_files, file)
    }
  }
  
  if(length(missing_files)>0) stop("Files are missing.")
  s1 <- rbindlist(stage1)
  write.csv(s1, "FILEPATH", row.names=F)
  write.csv(s1, paste0("FILEPATH"), row.names=F)
  write.csv(s1, paste0(root, "FILEPATH"), row.names=F)
} else {
  s1 <- fread(paste0(root, "FILEPATH"))
}


setnames(s1, "logit_q5_sexratio_pred", "pred_logitratio_s1")


st_data <- copy(d)
st_data[exclude_sex_mod!=0, exclude:="do not keep"]
st_data[exclude!="do not keep", exclude:="keep"]
st_data <- st_data[,.(region_name, ihme_loc_id, year, sex, exclude, q_u5, source, broadsource)]
st_data <- st_data[sex!="both"]
st_data <- dcast.data.table(st_data, region_name+ihme_loc_id+year+exclude+source+broadsource~sex, value.var="q_u5")
st_data[,logit_q5_sexratio := male/female]
st_data[logit_q5_sexratio>=1.5 ,logit_q5_sexratio:=1.49]
st_data[logit_q5_sexratio<=0.8 ,logit_q5_sexratio:=0.81]

st_data[,logit_q5_sexratio:=(logit_q5_sexratio-0.8)/(1.5-0.8)]
st_data[,logit_q5_sexratio:=logit(logit_q5_sexratio)]

for_gpr <- copy(st_data) 
st_data <- st_data[,c("source", "female", "male"):=NULL]
st_data[,year:=round(year)]


s1 <- s1[,c("q_u5_both", "q_u5_sexratio_pred", "q_u5_female", "q_u5_male"):=NULL]
s1[,year:=floor(year)]

st_data <- merge(st_data, s1, all.x=T, by=c("ihme_loc_id", "year"))



params <- copy(for_gpr)
params <- params[!is.na(logit_q5_sexratio)]
params <- params[exclude!="do not keep"]
params <- params[year>=1970]

locs <- data.table(get_locations(level="estimate"))[,.(region_name, ihme_loc_id)]
params <- merge(params, locs, all=T, by=c("ihme_loc_id", "region_name"))

den <- subset(params)[,.N, by="ihme_loc_id"]
params <- merge(params, den, by="ihme_loc_id", all.x=T)

params[N>=40, lambda := 1]
params[N>=40, zeta := 0.99]
params[N>=40, scale := 5]

params[N<40 & N>=30, lambda := 1.5]
params[N<40 & N>=30, zeta := 0.9]
params[N<40 & N>=30, scale := 10]

params[N<30 & N>=20, lambda := 2]
params[N<30 & N>=20, zeta := 0.8]
params[N<30 & N>=20, scale := 15]

params[grepl("_", ihme_loc_id) | N<20, lambda := 2]
params[grepl("_", ihme_loc_id) | N<20, zeta := 0.7]
params[grepl("_", ihme_loc_id) | N<20, scale := 20]

params[,amp2x := 1]
params[,best:=1]


params[,lambda:= 2]

params <- params[,.(ihme_loc_id, scale, amp2x, lambda, zeta, best)]
params <- unique(params, by=c())

write.csv(params, paste0(root, "FILEPATH"), row.names=F)
write.csv(params, paste0(root, "FILEPATH"), row.names=F)

################
## Space-Time
################

# calculating residual
st_data[,resid:=pred_logitratio_s1 - logit_q5_sexratio]
st_data[exclude=="do not keep", resid:=NA]


sq <- copy(s1)
sq <- merge(sq, regs, all.x=T, by="ihme_loc_id")
sq <- sq[,.(ihme_loc_id, year, region_name)]
st_data <- merge(sq, st_data, all.x=T, all.y=T, by=c("ihme_loc_id","year", "region_name"))

st_data <- st_data[,c("exclude", "logit_q5_sexratio", "pred_logitratio_s1"):=NULL]


st_data <- ddply(st_data, .(ihme_loc_id,year), 
            function(x){
              data.frame(region_name = x$region_name[1],
                         ihme_loc_id = x$ihme_loc_id[1],
                         year = x$year[1],
                         resid = mean(x$resid))
            })

st_data <- as.data.table(st_data)
st_data <- st_data[,"region_name":=NULL]
st_data <- merge(st_data, st_locs, by="ihme_loc_id", all.x=T, allow.cartesian = T)

st_pred <- resid_space_time(data=st_data, sex_mod=T)


st_pred <- data.table(st_pred)
st_pred[,year:=floor(year)]
st_pred[,"weight":=NULL]
st_pred <- st_pred[keep==1]
st_pred[,"keep":=NULL]

st_pred <- merge(st_pred, s1, all=T, by=c("ihme_loc_id", "year"))
st_pred[,pred_logitratio_s2:=pred_logitratio_s1-pred.2.resid]


s2_est <- copy(st_pred)
s2_est[,pred_ratio_s1:=exp(pred_logitratio_s1)/(1+exp(pred_logitratio_s1))]
s2_est[,pred_ratio_s1:=(pred_ratio_s1*0.7) + 0.8]
s2_est[,pred_ratio_s2:=exp(pred_logitratio_s2)/(1+exp(pred_logitratio_s2))]
s2_est[,pred_ratio_s2:=(pred_ratio_s2*0.7) + 0.8]
s2_est[,c("pred.2.resid", "pred_logitratio_s1", "pred_logitratio_s2"):=NULL]

# getting the birth sex ratio
births <- births[,c("location_name", "sex_id", "source", "location_id"):=NULL]
births <- dcast.data.table(births, ihme_loc_id+year~sex, value.var="births")
births[,birth_sexratio:=male/female]
births <- births[,c("both", "female", "male"):=NULL]

# getting the both sex 5q0 
child <- child[,c("upper", "lower"):=NULL]
child[,year:=floor(year)]
setnames(child, "med", "both_5q0")

# merging 
s2_est <- merge(s2_est, births, all.x=T, by=c("ihme_loc_id", "year"))
s2_est <- merge(s2_est, child, all.x=T, by=c("ihme_loc_id", "year"))

# calculating sex specific 5q0
s2_est[,pred_f5q0_s1:= (both_5q0*(1+birth_sexratio))/(1+(pred_ratio_s1 * birth_sexratio))]
s2_est[,pred_m5q0_s1:= pred_f5q0_s1 * pred_ratio_s1]

s2_est[,pred_f5q0_s2:= (both_5q0*(1+birth_sexratio))/(1+(pred_ratio_s2 * birth_sexratio))]
s2_est[,pred_m5q0_s2:= pred_f5q0_s2 * pred_ratio_s2]
s2_est <- s2_est[,"logit_q5_sexratio_wre":=NULL]

write.csv(s2_est, paste0(root, "FILEPATH"), row.names=F)
write.csv(s2_est, paste0(root, "FILEPATH"), row.names=F)

######################
## Prepping for GPR
######################

# merging emprical data back on

gpr_input <- copy(st_pred)
for_gpr <- for_gpr[,c("region_name", "male", "female") :=NULL]
for_gpr[,year:=round(year)]
gpr_input <- merge(gpr_input, for_gpr, all.x=T, all.y=T, by=c("ihme_loc_id", "year"))
gpr_input <- merge(gpr_input, regs, all.x=T, by="ihme_loc_id")


nats <- copy(gpr_input)
locs <- data.table(get_locations(level="estimate"))
locs <- locs[level==3 | ihme_loc_id == "CHN_44533"]
locs <- locs$ihme_loc_id
nats <- nats[ihme_loc_id %in% locs]
nats[, diff := pred_logitratio_s2 - pred_logitratio_s1]
setkey(nats, ihme_loc_id)
nats <- nats[,.(year=year, mse=var(diff)), by=key(nats)]

gpr_input[,merge_ihme := sapply(strsplit(ihme_loc_id, "_"), "[[",1)]
gpr_input[merge_ihme=="CHN", merge_ihme:="CHN_44533"]
setnames(nats, "ihme_loc_id", "merge_ihme")
setkey(nats, NULL)
nats <- unique(nats)

gpr_input <- merge(gpr_input, nats, by=c("merge_ihme", "year"), all.x=T)
gpr_input[,merge_ihme:=NULL]

data_per_loc <- list()
for(locs in unique(gpr_input$ihme_loc_id)){
  data_per_loc[[locs]] <- data.table(ihme_loc_id=locs, data_density=nrow(gpr_input[ihme_loc_id==locs & !is.na(logit_q5_sexratio) & exclude=="keep"]))
}
data_per_loc <- rbindlist(data_per_loc)
low_data_locs <- data_per_loc[data_density<3]
low_data_locs <- low_data_locs$ihme_loc_id

nat_var <- copy(gpr_input)
nat_var[,diff := logit_q5_sexratio - pred_logitratio_s2]
setkey(nat_var, ihme_loc_id)
nat_var <- nat_var[,.(year=year, data_var=var(diff, na.rm=T)), by=key(nat_var)]
setkey(nat_var, NULL)
nat_var <- unique(nat_var)

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

if(nrow(gpr_input[is.na(data_var)])>0) stop("missing data varaince")


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


gpr_input[source_type=="DSP" | source_type=="SRS",source_type:="VR"]
gpr_input[source_type!="VR", source_type:="other"]
gpr_input[source_type=="VR" & complete==1, source_type:="vr_unbiased"]
gpr_input[source_type=="VR" & complete==0, source_type:="vr_other"]


## saving gpr file
setnames(gpr_input, c("year", "source_type"), c("year_id", "category"))
write.csv(gpr_input, "FILEPATH", row.names=F)
write.csv(gpr_input, paste0("FILEPATH"), row.names=F)
write.csv(gpr_input, paste0(root, "FILEPATH"), row.names=F)

