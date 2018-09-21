rm(list=ls())
gc()
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
list.of.packages <- c("data.table","ggplot2","haven","parallel","gtools","lme4","haven","plyr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
code.dir <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH"))
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
cores <- 5
source(paste0(jpath,"FILEPATH/multi_plot.R"))

### Arguments
if(!is.na(commandArgs()[3])) {
  c.fbd_version <- commandArgs()[3]
  c.iso <- commandArgs()[4]
  c.scenario <- commandArgs()[5]
} else {
  c.fbd_version <- "20170720_dah10"
  c.iso <- "ZWE_44844"
  c.scenario <- "reference"
}

c.args <- fread(paste0(code.dir,"FILEPATH/run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]
dah.scalar <- c.args[["dah_scalar"]]
if(dah.scalar != 0) {
  dah_scenario <- T
  c.ref <- c.args[["reference_dir"]]
} else {
  dah_scenario <- F
}




##################################################################################
### Load Data
##################################################################################
file <- paste0(jpath,"FILEPATH/", c.iso, ".csv")
data <- fread(file)
#Set values after 2016 to missing
data[year > extension.year + 1,coverage_mean := NaN]
data[,pop:= NULL]

#Bound Coverage to be Safe
data[coverage_mean < .01,coverage_mean := .01 ]
data[coverage_mean > .999,coverage_mean := .999 ]

#create blank vals for 2016-2040
extension.dt <- data[year == extension.year]
n = 2040 - extension.year
replicated.dt <- extension.dt[rep(seq_len(nrow(extension.dt)), n), ]
for (i in 1:n) {
  replicated.dt[seq(((i-1)*nrow(extension.dt) + 1), i*nrow(extension.dt)), year := (extension.year + i)]
}
bound.dt <- data.table(rbind(data[year <= extension.year], replicated.dt))
bound.dt[year > extension.year + 1,coverage_mean := NaN]
bound.dt[order(iso3,sex,age,CD4,year)]
data <- bound.dt


##################################################################################
##Covariates -- get HIV DAH and GHES, adjust using ART price forecasts
##################################################################################
inputs <- fread(paste0(jpath,"FILEPATH/forecasted_inputs.csv"))[ihme_loc_id == c.iso & scenario == c.scenario]
inputs <- inputs[variable %in% c("ART Price","GHES","HIV DAH","LDI")]
#reshape wide on covariate
inputs[,V1:=NULL]
inputs <- dcast.data.table(inputs,ihme_loc_id+year_id+scenario~variable,value.var=c("pred_mean"))
#create doses covariates by rescaling by price
setnames(inputs,old=c("ART Price","GHES","HIV DAH","LDI","year_id","ihme_loc_id"),new=c("art_price","ghes_pc","hiv_dah","gdppc","year","iso3"))
inputs[,hiv_dah_doses:= hiv_dah/art_price]
inputs[,ghes_pc_doses:= ghes_pc/art_price]

#merge covariates onto data
data_comp<- data.table(merge(data,inputs,by=c("iso3","year"),all.x=T,allow.cartesian = T))
data_comp[is.na(hiv_dah),hiv_dah:=0]
data_comp[is.na(hiv_dah_doses),hiv_dah_doses:=0]

if(dah_scenario){
  load(paste0(jpath,"FILEPATH/model_fit.RData")) 
} else {
  load(paste0(jpath,"FILEPATH/model_fit.RData")) 
}

##################################################################################
####Function to use use higher level regression to forecast granular trends (use of function allows for mclapply)
##################################################################################

#Make ART Coverage Caps
iso.data <- data_comp
coeffs <- fread(paste0(jpath,"FILEPATH/coverage_caps_coeffs.csv"))
iso.data[,CAP:= gtools::inv.logit(exp(coeffs$intercept[1] + (coeffs$ldi_slope[1]  * gdppc)  + (coeffs$cd4_slope[1]  * CD4))-coeffs$offset)*.95]
iso.data[CD4>250,CAP:= gtools::inv.logit(exp(coeffs$intercept[1] + (coeffs$ldi_slope[1]  * gdppc)  + (coeffs$cd4_slope[1]  * 250))-coeffs$offset)*.95]
iso.data[year<1980,CAP:= .05]

any.dah <- iso.data[,.(max_dah=max(hiv_dah)),by=.(iso3)]
any.dah[max_dah==0,indic_dah:=0]
any.dah[max_dah>0,indic_dah:=1]
iso.data <- data.table(merge(iso.data,any.dah,by=c('iso3'),all.x=T))
iso.data[hiv_dah == 0,hiv_dah_doses:=0]

#Load in draws of past data
draws <-  fread("FILEPATH/", c.iso, ".csv")

#make rows for 2016-2040
extension.dt <- draws[year == extension.year]
n = 2040 - extension.year
replicated.dt <- extension.dt[rep(seq_len(nrow(extension.dt)), n), ]
for (i in 1:n) {
  replicated.dt[seq(((i-1)*nrow(extension.dt) + 1), i*nrow(extension.dt)), year := (extension.year + i)]
}
draws <- data.table(rbind(draws[year <= extension.year], replicated.dt))
draws[order(iso3,sex,age,CD4,year)]
#Merge on draws
draws <- data.table(merge(draws,iso.data,by=c("iso3","age","sex","CD4","year")))
#Set draws to NaN after last year of data
draws[year>extension.year, paste0("coverage_", 0:999) := NaN]

#Make predictions
draws[,temp_logit_cov_pred := predict(lin.mod,newdata=draws)]

#Apply Intercept Shift at draw level to propogate past uncertainty
int.shifts <- draws[year == extension.year]
cov.mat <- as.matrix(int.shifts[, paste0("coverage_", 0:999), with=F])
#bound coverage
cov.mat[cov.mat < 0.01] <- .01
cov.mat[cov.mat > 0.999] <- .999
logit.cov.mat <- logit(cov.mat)
preds.vector <- int.shifts[, temp_logit_cov_pred]
matrix(preds.vector,nrow=1000,ncol=length(preds.vector),byrow=TRUE)
preds.mat <- as.matrix(replicate(1000,preds.vector))
shifts.mat <- logit.cov.mat - preds.mat
subtracted.dt <- data.table(shifts.mat)
names(subtracted.dt) <- paste0("shifts_", 0:999)
shifted.dt <- cbind(int.shifts, subtracted.dt)
shifted.dt <- shifted.dt[,.SD,.SDcols=c("iso3","age","sex","CD4",paste0("shifts_", 0:999))]
draws <- data.table(merge(draws,shifted.dt,by=c("iso3","age","sex","CD4")))


big.shifts.mat <- as.matrix(draws[, paste0("shifts_", 0:999), with=F])
big.preds.mat <- as.matrix(replicate(1000,draws[,temp_logit_cov_pred]))
shifted.big.preds.mat <- big.preds.mat + big.shifts.mat
norm.shifted.big.preds.mat <- inv.logit(shifted.big.preds.mat)
future <- data.table(norm.shifted.big.preds.mat)
names(future) <- paste0("art_coverage_", 0:999)
panel.vars <- draws[,.SD,.SDcols=c("iso3","age","sex","CD4","year","CAP")]
future <- cbind(panel.vars,future)

#Calculate empirical max values
emp.caps <- draws[year <= extension.year]
emp.caps <- emp.caps[, lapply(.SD, max), .SDcols = paste0("coverage_",0:999),by=.(iso3,age,sex,CD4)]
names(emp.caps)[5:1004] <- paste0("emp_cap_", 0:999)
future <- data.table(merge(future,emp.caps,by=c("iso3","age","sex","CD4")))

#Apply Caps, take the max of the wealth-based cap,empirical cap, then min of cap or the prediction
emp.caps.mat <- as.matrix(future[, paste0("emp_cap_", 0:999), with=F])
wealth.caps.mat <- as.matrix(replicate(1000,future[, CAP]))
final.caps.mat <- pmax(emp.caps.mat,wealth.caps.mat)
all.preds.mat <-  as.matrix(future[, paste0("art_coverage_", 0:999), with=F])
trunc.preds.mat <- pmin(all.preds.mat,final.caps.mat)          
trunc.preds.mat[trunc.preds.mat < 0.01] <- .01
trunc.preds.mat[trunc.preds.mat > 0.999] <- .999
trunc.forecasts <- data.table(cbind(panel.vars,trunc.preds.mat))

#append forecasts onto past
future <- trunc.forecasts[year> extension.year]
past <- draws[,.SD,.SDcols=c("iso3","age","sex","CD4","year","CAP",paste0("coverage_",0:999))]
past <- past[year<=extension.year]
names(past) <- c("iso3","age","sex","CD4","year","CAP",paste0("art_coverage_",0:999))
forecast.draws <- rbind(past,future)
forecast.draws[order(iso3,age,sex,CD4,year)]

#Calculate mean, upper and lower
forecast.summary <-  forecast.draws[,.SD,.SDcols=c("iso3","age","sex","CD4","year")]
forecast.summary[,mean_art_coverage:= rowMeans(na.rm=T,as.matrix(forecast.draws[, paste0("art_coverage_", 0:999), with=F]),dims=1)]
forecast.summary[,lower_art_coverage:= apply(as.matrix(forecast.draws[, paste0("art_coverage_", 0:999), with=F]),1, quantile, probs = c(0.05),na.rm=T )]
forecast.summary[,upper_art_coverage:= apply(as.matrix(forecast.draws[, paste0("art_coverage_", 0:999), with=F]),1, quantile, probs = c(0.95),na.rm=T)]
forecast.summary[order(iso3,age,sex,CD4,year)]

#Save summaries
output.dir  <- paste0("FILEPATH/forecast_art_coverage_summary/")
dir.create(showWarnings = F, path=output.dir,recursive = T)
write.csv(forecast.summary,file=paste0(output.dir,c.iso,".csv"),row.names = F)

#Draws
output.dir  <- paste0("FILEPATH/forecast_art_coverage_draws/")
dir.create(showWarnings = F, path=output.dir,recursive = T)
write.csv(forecast.draws,file=paste0(output.dir,c.iso,".csv"),row.names = F)





