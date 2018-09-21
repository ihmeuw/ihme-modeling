rm(list=ls())
gc()
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
list.of.packages <- c("data.table","ggplot2","haven","parallel","gtools","lme4","haven","plyr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
code.dir <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
cores <- 20
source(paste0(jpath,"FILEPATH/multi_plot.R"))

### Arguments
if(!is.na(commandArgs()[3])) {
	c.fbd_version <- commandArgs()[3]
} else {
	c.fbd_version <- "20170719"
}

c.args <- fread(paste0(code.dir,"FILEPATH/run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]


##################################################################################
### Load Data
##################################################################################
cov.mean.files <- list.files(paste0(jpath,"FILEPATH/detailed_art/"),full.names = T)
ptm <- proc.time()
data.list <- mclapply(cov.mean.files, function(file) {
data <- fread(file)
},mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))
data <- rbindlist(data.list,fill=T)
print(proc.time() - ptm)

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
setnames(data, "iso3", "ihme_loc_id")


##################################################################################
##Covariates -- get HIV DAH and GHES, adjust using ART price forecasts
##################################################################################
inputs <- fread(paste0(jpath,"FILEPATH/forecasted_inputs.csv"))
inputs <- inputs[variable %in% c("ART Price","GHES","HIV DAH","LDI")]
#reshape wide on covariate
inputs[,V1:=NULL]
inputs <- dcast.data.table(inputs,ihme_loc_id+year_id+scenario~variable,value.var=c("pred_mean"))
#create doses covariates by rescaling by price
setnames(inputs,old=c("ART Price","GHES","HIV DAH","LDI","year_id"),new=c("art_price","ghes_pc","hiv_dah","gdppc","year"))
inputs[,hiv_dah_doses:= hiv_dah/art_price]
inputs[,ghes_pc_doses:= ghes_pc/art_price]

#merge covariates onto data
data_comp<- data.table(merge(data,inputs,by=c("ihme_loc_id","year"),all.x=T,allow.cartesian = T))
data_comp[is.na(hiv_dah),hiv_dah:=0]
data_comp[is.na(hiv_dah_doses),hiv_dah_doses:=0]
data_comp[, iso3 := ihme_loc_id]

##################################################################################
#collapse to country-year-cd4, fit regression
##################################################################################
iso.year.covs <- data_comp[scenario=="reference"]
iso.year.covs <- iso.year.covs[,.(coverage_mean=mean(coverage_mean),
                              coverage_upper=mean(coverage_upper),
                              ghes_pc=mean(ghes_pc),hiv_dah=mean(hiv_dah),
                              gdppc=mean(gdppc), ghes_pc_doses = mean(ghes_pc_doses),
                              hiv_dah_doses=mean(hiv_dah_doses),
                              art_price = mean(art_price)
                              ),by=.(ihme_loc_id,iso3,year,CD4)]

#calculate indicator for any DAH
any.dah <- iso.year.covs[,.(max_dah=max(hiv_dah)),by=.(iso3)]
any.dah[max_dah==0,indic_dah:=0]
any.dah[max_dah>0,indic_dah:=1]
iso.year.covs <- data.table(merge(iso.year.covs,any.dah,by=c('iso3'),all.x=T))

#take logit of coverage
iso.year.covs[,logit_cov := logit(coverage_mean)]

##################################################################################
##Fit Regression
##################################################################################
lin.mod <- lm(logit_cov~ghes_pc_doses + hiv_dah_doses:factor(indic_dah) +  factor(CD4) ,data=iso.year.covs[year>=2000])
summary(lin.mod)

save(lin.mod, file = paste0(jpath,"FILEPATH/model_fit.RData"))