
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 6/18/2019
# Purpose: convert radon exposure values to log space and crosswalk non-representative sources
#          
# source("FILEPATH.R", echo=T)

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  } else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  }

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","dplyr","metafor","msm","openxlsx","ggplot2")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

decomp <- "step4"
bundle <- 1529
rerun_crosswalk <- F
xw_version <- 1 
description <- "new data added for step 4"

date <- format(Sys.Date(), "%m%d%y")


# Directories -------------------------------------------------------------

#bundle functions
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))

#mr-brt
source(file.path(j_root,"FILEPATH.R"))

#get_locations
source(file.path(central_lib,"FILEPATH.R"))
locations <- get_location_metadata(location_set_id=22)
locs <- locations[is_estimate==1]

`%ni%` <- Negate(`%in%`)

#radon directory
home_dir <- "FILEPATH"

#bundle_dir
out_path <- file.path(j_root,"FILEPATH",bundle,"FILEPATH.xlsx")

#pdf for plots
pdf(paste0(home_dir,"FILEPATH",Sys.Date(),".pdf"))

dt <- openxlsx::read.xlsx(file.path(home_dir,"FILEPATH.xlsx"),
                          sheet="Extraction",rows=c(1,4:100000)) %>% as.data.table
dt <- dt[is_outlier==0]

# Recreate distributions (in log space) -----------------------------------

# convert factors to numeric
dt[,c("AM","GM","ASD","GSD","Median"):=lapply(.SD,function(x){as.numeric(as.character(x))}),.SDcols=c("AM","GM","ASD","GSD","Median")]

# GSD should never be less than 1
dt[GSD<=1,GSD:=NA]

# estimate distribution based off parameters we have, assuming lognormal
# am, gm, lm, asd, gsd, and lsd for arithmetic mean, geometric mean, log mean, arithmetic standard deviation, etc.

# Decide method for each row:
dt[!is.na(GM) & !is.na(GSD), method:="GM & GSD"]
dt[is.na(method) & !is.na(Median) & !is.na(GSD), method:="Median & GSD"]
dt[is.na(method) & !is.na(AM) & !is.na(ASD), method:="AM & ASD"]
dt[is.na(method) & !is.na(AM) & other_error_description=="SE", method:="AM & SE"]
dt[is.na(method) & !is.na(AM) & !is.na(GM), method:="AM & GM"]
dt[is.na(method) & !is.na(AM) & !is.na(Median), method:="AM & Median"]
dt[is.na(method) & !is.na(Median) & other_error_description=="IQR", method:="Median & IQR"]
dt[is.na(method) & !is.na(Median) & other_error_description=="95% CI", method:="Median & CI"]
dt[is.na(method) & !is.na(GM) & other_error_description=="Range", method:="GM & Range"]
dt[is.na(method) & !is.na(AM) & other_error_description=="Range", method:="AM & Range"]
dt[is.na(method) & !is.na(AM) & other_error_description=="Maximum value", method:="AM & Max"]
dt[is.na(method) & other_error_description=="Range", method:="Range only"]
dt[is.na(method) & !is.na(GM) & !is.na(sample_size), method:="GM & N"]
dt[is.na(method) & !is.na(AM) & !is.na(sample_size), method:="AM & N"]
dt[is.na(method) & !is.na(AM), method:="AM only"]
dt[is.na(method) & !is.na(GM), method:="GM only"]


# number of studies for each method
dt[,.N,by="method"][order(-N)]


# 1. GM or Median (equal in lognormal) and GSD

if("GM & GSD" %in% dt$method | "Median & GSD" %in% dt$method){
  dt[method=="GM & GSD", gm:=GM]
  dt[method=="Median & GSD", gm:=Median]
  dt[method %in% c("GM & GSD","Median & GSD"), lm:=log(gm)]
  dt[method %in% c("GM & GSD","Median & GSD"), gsd:=GSD]
  dt[method %in% c("GM & GSD","Median & GSD"), lsd:=log(gsd)]
}


# 2. AM & ASD

if("AM & ASD" %in% dt$method){
  dt[method == "AM & ASD", am:=AM]
  dt[method == "AM & ASD", asd:=ASD]
  dt[method == "AM & ASD", lsd:=sqrt(log(1+(asd^2)/(am^2)))]
  dt[method == "AM & ASD", lm:=log(am)-0.5*(lsd^2)]
}

# 3. AM & SE

if("AM & SE" %in% dt$method){
  dt[method == "AM & SE", am:=AM]
  dt[method == "AM & SE", asd:=as.numeric(other_error)*sqrt(sample_size)]
  dt[method == "AM & SE", lsd:=sqrt(log(1+(asd^2)/(am^2)))]
  dt[method == "AM & SE", lm:=log(am)-0.5*(lsd^2)]
}


# 4. AM and GM/Median
if("AM & GM" %in% dt$method | "AM & Median" %in% dt$method){
  dt[method=="AM & GM", gm:=GM]
  dt[method=="AM & Median", gm:=Median]
  dt[method %in% c("AM & GM", "AM & Median"), am:=AM]
  dt[method %in% c("AM & GM", "AM & Median"), lsd:=sqrt((log(am)-log(gm))*2)]
  dt[method %in% c("AM & GM", "AM & Median"), lm:=log(gm)]
}


# 5. median and IQR or 95% CI

# the equation for relating lsd to IQR or 95% CI is not easily solved. I will create the following lookup table of values to get the lsd to the nearest 0.001
# these come from taking the percentiles of the lognormal distribution. For example, we would expect IQR=exp(ln(GM)+(LSD*Zscore(0.75)))-exp(ln(GM)+(LSD*Zscore(0.25)))
# which simplifies to IQR=GM*exp(LSD*Zscore(0.75))-GM*exp(LSD*Zscore(0.25)) and IQR/GM=exp(LSD*Zscore(0.75))-exp(LSD*Zscore(0.25))

if("Median & IQR" %in% dt$method | "Median & CI" %in% dt$method){
  lookup <- data.table(lsd=seq(0,3,0.001))
  lookup[, iqr_gm_ratio:=exp(lsd*qnorm(.75))-exp(lsd*qnorm(.25))]
  lookup[, ci_gm_ratio:=exp(lsd*qnorm(.975))-exp(lsd*qnorm(.025))]
  
  dt[method %in% c("Median & IQR","Median & CI"), gm:=Median]
  dt[method %in% c("Median & IQR","Median & CI"), lm:=log(gm)]
}

# rows with IQR
if("Median & IQR" %in% dt$method){
  dt[method=="Median & IQR", iqr_gm_ratio:=as.numeric(other_error)/gm]
  dt[method=="Median & IQR", lsd:=lookup[dt[method=="Median & IQR"],lsd,on="iqr_gm_ratio",roll="nearest"]]
  dt[,iqr_gm_ratio:=NULL]
}

# rows with 95% CI
if("Median & CI" %in% dt$method){
  dt[method=="Median & CI", ci_gm_ratio:=mapply(function(x) {abs(eval(parse(text=x)))}, other_error)/gm]
  dt[method=="Median & CI", lsd:=lookup[dt[method=="Median & CI"],lsd,on="ci_gm_ratio",roll="nearest"]]
  dt[,ci_gm_ratio:=NULL]
}


# 6. GM/AM and Range # Using approximation of range/4=sd in log space.
if("GM & Range" %in% dt$method | "AM & Range" %in% dt$method){
  dt[method=="GM & Range", gm:=GM]
  dt[method=="GM & Range", lm:=log(gm)]
  dt[method %in% c("GM & Range", "AM & Range"), min:=as.numeric(unlist(tstrsplit(other_error,split="-",keep=1)))]
  dt[method %in% c("GM & Range", "AM & Range"), max:=as.numeric(unlist(tstrsplit(other_error,split="-",keep=2)))]
  dt[method %in% c("GM & Range", "AM & Range"), lsd:=(log(max)-log(min))/4]
  dt[method=="AM & Range", am:=AM]
  dt[method=="AM & Range", lm:=log(am)-0.5*(lsd^2)]
  dt[,c("min","max"):=NULL]
}


# 7. AM and Max # using lognormal symmetry to estimate range and range/4=sd in log space
# uses forumla for lm and the assumption that log(max) will be about 2 standard deviations away from lm
# solve with quadratic formula
# lm=log(am)-0.5*lsd^2 
# log(max) = lm+2*lsd

if("AM & Max" %in% dt$method){
  dt[method=="AM & Max", am:=AM]
  dt[method=="AM & Max", lsd:=(4-sqrt(16-4*(-2*log(am)+2*log(as.numeric(other_error)))))/2] # 2 positive solutions to quadratic. using smaller solution because it gives reasonable values
  dt[method=="AM & Max", lm:=log(am)-0.5*(lsd^2)]
}

# 8. For range only, assume symmetrical (lm = midpoint(log(min),log(max))) and lsd=range/4 in log space

if("Range only" %in% dt$method){
  dt[method=="Range only", min:=as.numeric(unlist(tstrsplit(other_error,split="-",keep=1)))]
  dt[method=="Range only", max:=as.numeric(unlist(tstrsplit(other_error,split="-",keep=2)))]
  dt[method=="Range only", lsd:=(log(max)-log(min))/4]
  dt[method=="Range only", lm:=(log(max)+log(min))/2]
  dt[,c("min","max"):=NULL]
}


# 9. For AM and GM with sample size, we will predict lsd based on sample size, only using our best methods of predicting lsd and excluding the outliers of lsd

if("AM & N" %in% dt$method | "GM & N" %in% dt$method){
  mod <- lm(lsd ~ log(sample_size),
            data=dt[method %in% c("GM & GSD","Median & GSD","AM & ASD","AM & GM","AM & Median","Median & IQR","Median & CI")])
  
  dt[method %in% c("AM & N","GM & N"), lsd:=mod$coefficients[1]+log(sample_size)*mod$coefficients[2]]
  dt[method=="GM & N", gm:=GM]
  dt[method=="GM & N", lm:=log(gm)]
  dt[method=="AM & N", am:=AM]
  dt[method=="AM & N", lm:=log(am)-0.5*(lsd^2)]
}


# 10. for anything that is left, (AM only, GM only and methods for which equations were unsolvable) impute median lsd

med_lsd <- median(dt$lsd,na.rm=T)

dt[is.na(lsd), lsd:=med_lsd]

dt[is.na(lm), gm:=GM]
dt[is.na(lm), lm:=log(gm)]

dt[is.na(lm), am:=AM]
dt[is.na(lm), lm:=log(am)-0.5*(lsd^2)]


# rows with lm < 0. Drop for now

dt <- dt[lm>0]

# COMPUTE variance of the mean where VARIANCE=SE^2=lsd^2/n

dt[,lvar_mean:=lsd^2/sample_size]

# when sample size is missing, impute median sample size. We will do this separately for representative studies (smaller_site_unit=0) and smaller area studies (smaller_site_unit=1)

rep_mean_n <- dt[smaller_site_unit==0,mean(sample_size,na.rm=T)]
nonrep_mean_n <- dt[smaller_site_unit==1,mean(sample_size,na.rm=T)]

dt[is.na(lvar_mean) & smaller_site_unit==0, lvar_mean:=lsd^2/rep_mean_n]
dt[is.na(lvar_mean) & smaller_site_unit==1, lvar_mean:=lsd^2/nonrep_mean_n]

dt[,am:=exp(lm+0.5*lsd^2)]
dt[,asd:=sqrt(exp(2*lm+lsd^2)*(exp(lsd^2)-1))]
dt[,gm:=exp(lm)]
dt[,gsd:=exp(lsd)]
dt[,lse:=sqrt(lvar_mean)]

# merge on location information for plot
dt <- merge(dt,locs[,.(location_id,super_region_name)],by="location_id",all.x=T)

gg <- ggplot(data=dt,aes(x=lm,y=lsd,color=method))+geom_point()+ggtitle("SD v. Mean in log space by method")
print(gg)

gg <- ggplot(data=dt,aes(x=sample_size,y=lsd,color=super_region_name,shape=as.character(smaller_site_unit)))+
  geom_point()+
  ggtitle("SD (in log space) v. Sample Size by Super Region and representativeness (pre xw)")+
  scale_x_log10()+
  scale_shape_manual(values=c("1"=1,"0"=19),labels=c("representative","non-representative"))
print(gg)


# crosswalk non-representative sources ------------------------------------

# run model to determine variance inflation weight for non-representative studies
ref <- dt[smaller_site_unit==0]
alt <- dt[smaller_site_unit==1]

# convert to log space using delta method in order to apply crosswalk later
alt[,lm_log:=log(lm)]
alt[,lse_log:=deltamethod(~log(x1),lm,lse^2),by=1:nrow(alt)]

if(rerun_crosswalk){
  #generate dataset of all possible matches between rep and nonrep sources in the same location
  xwalk <- merge(ref,alt,by="location_id",all.x=F,all.y=F,suffixes=c(".ref",".alt"))
  
  xwalk[,ratio:=lm.alt/lm.ref]
  # calculate standard error of the ratio
  xwalk[,ratio_se:=sqrt(lm.alt^2/lm.ref^2 * (lse.alt^2/lm.alt^2 + lse.ref^2/lm.ref^2))]
  #generate unique identifier for each NID combination
  xwalk[,group:=paste(nid.ref,nid.alt,sep="_")]
  
  xwalk[,ratio_log:=log(ratio)]
  xwalk[,ratio_log_se:=deltamethod(~log(x1),ratio,ratio_se^2),by=1:nrow(xwalk)]
  
  
  fit1 <- run_mr_brt(
    output_dir = file.path(home_dir,"crosswalk"),
    model_label = xw_version,
    data = xwalk,
    mean_var = "ratio_log",
    se_var = "ratio_log_se",
    overwrite_previous = TRUE,
    study_id = "group",
    method = "trim_maxL",
    trim_pct = 0.1
  )
  
  check_for_outputs(fit1)
  
  #system(capture.output(plot_mr_brt(fit1))[3])
  plot_mr_brt(fit1)
  
  saveRDS(fit1, file.path(home_dir,"FILEPATH",xw_version,"FILEPATH.RDS"))
  
} else {
  fit1 <- readRDS(file.path(home_dir,"FILEPATH",xw_version,"FILEPATH.RDS"))
}


df_pred <- data.frame(intercept = 1)
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries 

# pull beta and SE out of model (in this case it represents the estimated log of the ratio)
beta0 <- preds$Y_mean
beta0_se_tau <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92

# Adjust the alternate estimates in log(log) space
alt[,lm_log_adj:=lm_log-beta0]
alt[,lse_log_adj:=sqrt(lse_log^2 + beta0_se_tau^2)]

# convert from log(log) to log space using delta method for se
alt[,lm_adj:=exp(lm_log_adj)]
alt[,lse_adj:=deltamethod(~exp(x1),lm_log_adj,lse_log_adj^2),by=1:nrow(alt)]

# plot extracted against adjusted
gg <- ggplot(data=alt,aes(x=lm,y=lm_adj,color=super_region_name))+
  geom_abline(slope=1,intercept=0)+
  geom_point(shape=1)+
  ggtitle("Adjusted Mean (log space) for non-representative points")
print(gg)

gg <- ggplot(data=alt,aes(x=lse,y=lse_adj,color=super_region_name,xmin=min(lse,lse_adj),ymin=min(lse,lse_adj)))+
  geom_abline(slope=1,intercept=0)+
  geom_point(shape=1)+
  ggtitle("Adjusted SE (log space) for non-representative points")
print(gg)

dev.off()

# adjust raw data to bind back with reference data
alt[,lm:=lm_adj]
alt[,lse:=lse_adj]
alt[,lvar_mean:=lse^2]

alt <- alt[,names(ref),with=F]

dt_out <- rbind(alt,ref,use.names=T)

#drop am, asd, gm, gsd. Not used in modelling
dt_out[,c("am","asd","gm","gsd","super_region_name"):=NULL]

# save crosswalked data ---------------------------------------------------

write.xlsx(dt_out,out_path,row.names=F,sheetName="extraction")
write.csv(dt_out,paste0(home_dir,"FILEPATH",decomp,"FILEPATH",date,".csv"),row.names=F)