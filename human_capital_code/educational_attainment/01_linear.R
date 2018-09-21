## Joe Friedman - jrfried@uw.edu
##Compile Education Data, Fit linear model 
#Install and Load Required Packages
list.of.packages <- c("lme4","splines","reldist","sp","data.table","plyr","ggplot2","plotly","haven","parallel","assertable")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
#if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
library(SDMTools, lib.loc = " ")
#Arguments
jpath <- ifelse(Sys.info()[1]=="Windows", "J:/", "/home/j/")
cores <- 35
start_year <- 1950
end_year <- 2040
model_version <- " "
########################################################################################
#Compile Education Data
########################################################################################
##Create List of Files
data.files <- c(list.files(paste0("...", "/tabulated_data"),full.names = T,pattern=".csv"),
                list.files(paste0("...", "/tabulated_data/binned"),full.names = T))

##Load data Using multi-processing if on the cluster
data.list <- mclapply(data.files,fread,mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))

# data.list <- lapply(data.files,FUN = function(x){print(x)
#   fread(x)})
#Collapse list of data.tables into one data.table
edu.dists <- rbindlist(data.list,fill=T)

#Subset to correct years
edu.dists <- edu.dists[year <= end_year & year >= start_year]
edu.dists[,iso3:=ihme_loc_id]
#Just keep means
edu.dists[,sample_size:=total_ss]
edu.means <- edu.dists[,.(mean=mean(mean),mean_se=mean(mean_se)),by=.(age_start,sex,year,iso3,sample_size,type, nid)]
#drop if less than 20 people
edu.means <- edu.means[sample_size>20]

#add surveys which are provided to us already collapsed, use 0 for se since they're representative !! nse will come later
data.files.avgs <- list.files(paste0("...", "demographic_averages/"),full.names = T,pattern=".csv")
dem.avgs <- rbindlist(mclapply(data.files.avgs,fread,mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores)), fill = T)
dem.avgs[, mean := edu_start]
dem.avgs[, c("edu_start", "edu_end", "bins", "age_end", "location_name", "age_group_id", "location_id") := NULL]
setnames(dem.avgs, "count", "sample_size")
dem.avgs[,mean_se := 0]

edu.means <-rbind(edu.means, dem.avgs)

#Drop all of LSMS since the extraction is very low quality currently
edu.means <- edu.means[type!= "LSMS"]
edu.means <- edu.means[type!= "NHIS"]

#tabulate country-survey-years and write file of source records
temp <- edu.means[nchar(iso3)==3,.(mean=mean(mean)),by=.(iso3, year,type)]
temp$obs <- 1
temp2 <- temp[,.(Number=sum(obs)),by=.(type)]
dir.create(paste0("...", "/output_data/",model_version, "/"),showWarnings = T)
          
write.csv(temp2[order(-Number)],
paste0("...", "output_data/",model_version,"/sources_record.csv"),row.names=F)

########################################################################################
#Do Cohort Backcast and Forecast
########################################################################################
##Functions to backcast and forecast -- need to add SE multipliers once draws are implemented
back_cast <- function(dt,time) {
  timecast.dt <- dt[age_start>=25+time]
  timecast.dt <- timecast.dt[,age_start:= age_start - time]
  timecast.dt <- timecast.dt[,year:=year - time]
  timecast.dt[,point_type:= -1*time]
  timecast.dt[,mean_se:=mean_se * ((time/5)+1)]
  return(timecast.dt)
}
for_cast <- function(dt,time) {
  timecast.dt <- dt[age_start<=95-time & age_start>=25]
  timecast.dt <- timecast.dt[,age_start:= age_start + time]
  timecast.dt <- timecast.dt[,year:=year + time]
  timecast.dt[,point_type:=time]
  timecast.dt[,mean_se:=mean_se * ((time/5)+1)]  
  return(timecast.dt)
}

#Create Backcasts and Forecasts
back.casts.list <- mclapply(X=seq(5,55,5),FUN=back_cast,dt=edu.means,mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))
for.casts.list <- mclapply(X=seq(5,70,5),FUN=for_cast,dt=edu.means,mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))
back.casts <- rbindlist(back.casts.list)
for.casts <- rbindlist(for.casts.list)

#Create one dataset
orig.summs <- edu.means
orig.summs[,point_type:=0]
all.means <- rbind(edu.means,for.casts,back.casts)


########################################################################################
#Fit First Stage Linear Model
########################################################################################

locs <- get_location_metadata(location_set_id=22)[level > 2]
template <- expand.grid(year=seq(start_year,end_year,1),iso3=unique(locs$ihme_loc_id),age_start=unique(all.means$age_start),sex=c(1,2))
#merge on regional info
locs[,iso3:=ihme_loc_id]
locs<- locs[,.SD,.SDcols=c("iso3","region_name","super_region_name")]
template <- data.table(merge(template,locs,by='iso3'))
template <- template[,iso3:= as.character(iso3)]
#merge on data
all.means$sex <- as.double(all.means$sex)
all.data <- data.table(merge(all.means,template,by=c('iso3','year','age_start','sex'),all.y=T))
all.data <- all.data[year <= end_year & year >= start_year]
#make region-sex var
all.data[,reg_sex:=paste0(region_name,sex)]
all.data[,iso3_age:=paste0(iso3,age_start)]
#delete infants and under-5
all.data <- all.data[age_start >1]
#rescale and take logit of data
all.data[mean < .05,mean:=.05]
all.data[mean > 17.95,mean:=17.95]
all.data[,logit_mean:=logit(mean/18)]
#set different logit spaces for younger age groups
all.data[age_start == 5 & mean > 2.95,mean:=2.95]
all.data[age_start == 5,logit_mean:=logit(mean/3)]
all.data[age_start == 10 & mean > 7.95,mean:=7.95]
all.data[age_start == 10,logit_mean:=logit(mean/8)]
all.data[age_start == 15 & mean > 12.95,mean:=12.95]
all.data[age_start == 15,logit_mean:=logit(mean/13)]
all.data[age_start>=40,age_type:="old"]
all.data[age_start<40,age_type:="young"]
all.data[,cohort :=year - age_start]
all.data[,sup_reg_sex:=paste0(super_region_name,sex)]


#Fit linear model for mean
for (c.reg_sex in unique(all.data$reg_sex)){
  print(c.reg_sex)

  linear.prior.mean <- lmer(logit_mean~ year + (1|iso3) +ns(age_start,df=all.data[reg_sex==c.reg_sex],knots=c(15,25)),data=all.data[reg_sex==c.reg_sex])

  all.data[reg_sex==c.reg_sex &  age_start > 15,logit_prior_mean:= predict(linear.prior.mean,newdata=all.data[reg_sex==c.reg_sex &  age_start > 15],allow.new.levels=T, re.form = ~(1|iso3))]
  all.data[reg_sex==c.reg_sex &  age_start > 15,prior_mean:=inv.logit(logit_prior_mean) * 18]

  all.data[age_start == 15 & reg_sex==c.reg_sex,logit_prior_mean:= predict(linear.prior.mean,newdata=all.data[age_start == 15& reg_sex==c.reg_sex],allow.new.levels=T, re.form = ~(1|iso3))]
  all.data[age_start == 15 & reg_sex==c.reg_sex,prior_mean:=inv.logit(logit_prior_mean) * 13]
  
  all.data[age_start == 10 & reg_sex==c.reg_sex,logit_prior_mean:= predict(linear.prior.mean,newdata=all.data[age_start == 10& reg_sex==c.reg_sex],allow.new.levels=T, re.form = ~(1|iso3))]
  all.data[age_start == 10 & reg_sex==c.reg_sex,prior_mean:=inv.logit(logit_prior_mean) * 8]
  
  all.data[age_start == 5 & reg_sex==c.reg_sex,logit_prior_mean:= predict(linear.prior.mean,newdata=all.data[age_start == 5& reg_sex==c.reg_sex],allow.new.levels=T, re.form = ~(1|iso3))]
  all.data[age_start == 5 & reg_sex==c.reg_sex,prior_mean:=inv.logit(logit_prior_mean) * 3]
}


#add non-sampling error to data uncertainty
all.data[!is.na(mean) & is.na(mean_se), mean_se := .04]
nse <- all.data
nse[,abs.dev := abs(mean - prior_mean)]
nse = nse[,.(nse=median(abs.dev,na.rm=T))]

nse_add <- as.numeric(nse[,nse[1]])
all.data[,mean_se:=mean_se + nse_add]


#delta transform variance
all.data[,mean_variance:=mean_se^2]
all.data[age_start == 5,logit_mean_variance:=mean_variance/(3^2) * (1/((mean/3)*(1-(mean/3))))^2]
all.data[age_start == 5 & mean < .1,logit_mean_variance:=mean_variance/(3^2) * (1/((.1/3)*(1-(.1/3))))^2]
all.data[age_start == 10,logit_mean_variance:=mean_variance/(8^2) * (1/((mean/8)*(1-(mean/8))))^2]
all.data[age_start == 10 & mean < .1,logit_mean_variance:=mean_variance/(8^2) * (1/((.1/8)*(1-(.1/8))))^2]
all.data[age_start == 15,logit_mean_variance:=mean_variance/(13^2) * (1/((mean/13)*(1-(mean/13))))^2]
all.data[age_start == 15 & mean < .5,logit_mean_variance:=mean_variance/(13^2) * (1/((.5/13)*(1-(.5/13))))^2]
all.data[age_start > 15,logit_mean_variance:=mean_variance/(18^2) * (1/((mean/18)*(1-(mean/18))))^2]
all.data[age_start > 15 & mean < .5,logit_mean_variance:=mean_variance/(18^2) * (1/((.5/18)*(1-(.5/18))))^2]

#calculate regional MAD for GPR
reg_mad <- all.data
reg_mad[, res := logit_mean - logit_prior_mean][, med := median(res, na.rm = T), by = .(region_name)]
reg_mad = reg_mad[,.(mad= median(abs(res-med),na.rm=T)), by=.(region_name)] 
all.data <- data.table(merge(all.data,reg_mad,by=c("region_name")))


########################################################################################
#Save First Stage Predictions
########################################################################################
###standardize panel variables
setnames(all.data,old="sex",new="sex_id")
setnames(all.data,old="iso3",new="ihme_loc_id")
setnames(all.data,old="year",new="year_id")
all.data[age_start < 80,age_group_id:=((age_start - 15) /5 ) + 8]
all.data[age_start > 75,age_group_id:=((age_start - 15) /5 ) + 17]
all.data[age_start ==95,age_group_id:=235]

locs <- get_location_metadata(location_set_id = 22)
locs <- locs[locs$level >=3]
locs <- locs[,c("location_id","location_name","ihme_loc_id"),with=F]
save.data <- data.table(merge(all.data,locs,by=c('ihme_loc_id')))

var05th <- quantile(save.data$logit_mean_variance,probs=.05,na.rm=T)
save.data[logit_mean_variance < var05th,logit_mean_variance:= var05th ]
var95th <- quantile(save.data$logit_mean_variance,probs=.95,na.rm=T)
save.data[logit_mean_variance > var95th,logit_mean_variance:= var95th ]

##save preds - assert all combinations of LASY exist
dir.create(paste0("...", "/output_data/",model_version),showWarnings = T)

# some dups exist bc saving data too not just preds
save.data <- save.data[age_group_id %in%  c(6:20, 30:32, 235)]
assert_ids(save.data, id_vars = list(ihme_loc_id = locs$ihme_loc_id, age_group_id = c(6:20, 30:32, 235), sex_id = 1:2, year_id = 1950:2040), assert_dups = F)

save.data$sex_id <- as.integer(save.data$sex_id)
save.data$sample_size <- as.integer(save.data$sample_size)
save.data$sex_id <- as.integer(save.data$sex_id)

write.csv(save.data,paste0("...",output_data/",model_version,"/linear_prior.csv"),row.names=F)
 
