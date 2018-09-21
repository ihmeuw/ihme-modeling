#Crosswalk prev to mean
#5/10/17

prev<-read.csv(filepath)
prev$prev_mean<-prev$mean
prev$prev_mean_3<-  round(prev$prev_mean,digits=3)
prev$prev_mean_2<-  round(prev$prev_mean,digits=2)

prev<-prev[prev$me_name=='fpg'|prev$me_name=='diabetes',]
table(prev$measure,exclude=NULL)

prev$age<-round((prev$age_end-prev$age_start)/2)+prev$age_start
table(prev$sex)
mround <- function(x,base){ 
  base*round(x/base) 
} 
prev$age<-  mround(prev$age,5)

prev$sex_id<-prev$sex
prev$sex<-ifelse(prev$sex_id=='Male',1,2)
summary(prev$prev_mean_2)
training<-read.csv(filepath)
training<-training[!is.na(training$prev_upper), ]
training<-training[!is.na(training$prev_lower), ]
training<-training[!is.na(training$prev_mean), ]
summary(training$prev_mean)
#aggregate training frame
#round 

training$prev_mean_3<-round(training$prev_mean,digits=3)
training$prev_lower_3<-round(training$prev_lower,digits=3)
training$prev_upper_3<-round(training$prev_upper,digits=3)

training$prev_mean_2<-round(training$prev_mean,digits=2)
training$prev_lower_2<-round(training$prev_lower,digits=2)
training$prev_upper_2<-round(training$prev_upper,digits=2)

training<-as.data.table(training)
library(dplyr)
library(tidyr)     

test_3sig<-training %>% group_by(age, sex, prev_mean_3) %>% 
  summarise(new_mean =mean(mean)) 

test_2sig<-training %>% group_by(age, sex, prev_mean_2) %>% 
  summarise(new_mean =mean(mean), new_lower =mean(lower), new_upper =mean(upper)) 

#merge files
data_pred_cont<-merge(prev,test_3sig,by=c('prev_mean_3','age','sex'), all.x=T)

pred_1<-data_pred_cont[!is.na(data_pred_cont$new_mean),]

check<-data_pred_cont[is.na(data_pred_cont$new_mean),]
check1<-merge(check,test_2sig,by=c('prev_mean_2','age','sex'), all.x=T)
check1$new_mean<-check1$new_mean.y

pred_1<-as.data.table(pred_1)
check1<-as.data.table(check1)
predicted<-rbind(check1, pred_1, fill=T)
hist(predicted$new_mean)

predicted$log_se<-log(predicted$standard_error)
predicted$log_mean<-log(predicted$new_mean)
ggplot(data=predicted, aes(x=log_mean, y=log_se))+geom_point()

summary(predicted$new_mean)
predict_use<-predicted[!is.na(predicted$new_mean),]
nopred<-predicted[is.na(predicted$new_mean),]
summary(nopred$prev_mean)
predict_use$mean<-predict_use$new_mean
write.csv(filepath)




#create frame for exposure model
data_cont <- read.csv(filepath)
data_cont$mean<-data_cont$mean_adjust
data_cont$age<-round((data_cont$age_end-data_cont$age_start)/2)+data_cont$age_start

mround <- function(x,base){ 
  base*round(x/base) 
} 
data_cont$age<-mround(data_cont$age,5)
table(data_cont$sex_id,exclude=NULL)

data_pred_cont <- read.csv(filepath)
table(data_pred_cont$sex_id,exclude=NULL)
data_pred_cont$sex_id<-tolower(data_pred_cont$sex_id)
data_pred_cont$sex<-ifelse(data_pred_cont$sex_id=='2','female','male')
table(data_pred_cont$sex_id, data_pred_cont$sex)
data_cont<-as.data.table(data_cont)
data_pred_cont<-as.data.table(data_pred_cont)

st_gpr<-rbind(data_cont, data_pred_cont, fill=T)
table(st_gpr$location_id,exclude=NULL)

st_gpr_loc<-st_gpr[!is.na(st_gpr$location_id),]
st_gpr_ihme<-st_gpr[!is.na(st_gpr$ihme_loc_id) & is.na(st_gpr$location_id),]

loc<-read.csv(filepath)
keep<-c('ihme_loc_id','location_id')
loc<-loc[keep]
st_gpr_ihme<-merge(st_gpr_ihme,loc, by=c('ihme_loc_id'))
st_gpr_ihme$location_id<-st_gpr_ihme$location_id.y
table(st_gpr_ihme$location_id)
st_gpr<-rbind(st_gpr_ihme,st_gpr_loc, fill=T)
table(st_gpr$location_id,exclude=NULL)

# keep only columns needed
st_gpr$variance<-st_gpr$standard_error
st_gpr$data<-st_gpr$mean
st_gpr<-st_gpr[st_gpr$age>20,]
st_gpr$year_id<-round((st_gpr$year_end-st_gpr$year_start)/2)+st_gpr$year_start
table(st_gpr$year_id)
summary(st_gpr$urban_id)



age<-read.csv(filepath)
age$age<-age$age_lower
keep<-c('age_group_id','age')
age<-age[keep]
table(age$age)
st_gpr<-merge(st_gpr,age,by=c('age'))
table(st_gpr$age_group_id,exclude=NULL)

keep<-c('location_id','age_group_id','age','sex_id','nid','sample_size','year_start','year_end','sex','mean',
'ihme_loc_id','data','me_name','file_name','subnat_id','urban_id','error','variance','year_id')
table(st_gpr$file_name)
st_gpr<-st_gpr[keep]
#
#subset out data to only include location_set 22 since ST-GPR doesn't upload regional data
#

loc<-read.csv(filepath)

keep<-c('location_id','super_region_name')
loc<-loc[keep]
data_check<-merge(st_gpr, loc, by=c('location_id'))
data_check$sex_id<-tolower(data_check$sex_id)
data_check$sex<-tolower(data_check$sex)

table(data_check$sex_id,data_check$sex)

data_check$sex[data_check$sex=='males']<-'male'
data_check$sex_id[data_check$sex_id==1]<-'male'
data_check$sex_id[data_check$sex_id==2]<-'female'
data_check$sex[data_check$sex_id=='female' & data_check$sex=='male']<-'female'
data_check$sex_id[data_check$sex_id=='both' & data_check$sex=='male']<-'male'

data_check$sex_id[data_check$sex=='male']<-1
data_check$sex_id[data_check$sex=='female']<-2

data_check$me_name<-'metab_fpg_cont'
write.csv(filepath)

summary(data_check$mean)
data_check$log_se<-log(data_check$standard_error)
data_check$log_error<-log(data_check$error)
summary(data_check$error)
data_check$log_mean<-log(data_check$mean)
ggplot(data=data_check, aes(x=log_mean, y=log_error))+geom_point()+ggtitle("scatterplot of log mean and log se of observations run in revised FPG exposure model 5-20-17")

ggplot(data=data_check, aes(x=age, y=data,colour=super_region_name.y))+geom_point()+ggtitle("scatterplot of mean fpg and age of observations run in revised FPG exposure model 5-20-17")

    table(data_check$super_region_name.y)
    ggplot(data_check, aes(data, colour = super_region_name.y))+facet_wrap(~sex_id) +
      geom_freqpoly()
    
    table(data_check$sex_id,data_check$sex,exclude=NULL)
table(data_check$super_region_name)
table(data_check$uncertainty_type,exclude=NULL)
summary(data_check$variance)

data_check1<-unique(data_check)
