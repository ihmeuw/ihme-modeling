###
#Code to split Marketscan into OA-Hip and OA-knee because ICD codes don't differentiate the 2 types
#Data split using NHANES 1971 data
# written by ###
####

library(readstata13)
library (ggplot2)
library(openxlsx)
library(data.table)

path_j<-filepath
path<-paste0(path_j,filepath)

############
#Create the split ratios 
############

#Bring data in from NHANES
oa<-read.dta13(paste0(path,filepath))

oa<-oa[,which(names(oa) %in% c('n1ar0300','n1ar0299','n1ar0484','n1ar0506','n1ar0527','n1ar0266','oa$n1ar0265','n1ar0549','n1ar0550','n1ar0585','n1ar0586','n1ar0620','n1ar0621' ,'n1ar0101','n1ar0104', 'n1ar0158'))]

#remove people who didn't have any test
oa$blank<-ifelse(is.na(oa$n1ar0484) & is.na(oa$n1ar0506) & is.na(oa$n1ar0527) & is.na(oa$n1ar0549) & is.na(oa$n1ar0550) & is.na(oa$n1ar0585) & is.na(oa$n1ar0586) & is.na(oa$n1ar0620) & is.na(oa$n1ar0621),1,0)
oa<-oa[oa$blank!=1,]

#recode
oa$wt<-oa$n1ar0158
oa$wt.1<-ifelse(is.na(oa$wt),0,1)
oa$knee_pain[oa$n1ar0300==1]<-1 #pain in knee area
oa$knee_pain[oa$n1ar0299==1]<-1 #pain in or around knee
oa$knee_1[oa$n1ar0484!=0]<-1 #osteoarthrosis of the knee. moderate, or severe. reader #1 
oa$knee_2[oa$n1ar0506!=0]<-1 #osteoarthrosis of the knee. moderate, or severe. reader #2
oa$knee_3[oa$n1ar0527!=0]<-1 #osteoarthrosis of the knee. moderate, or severe. reader #3 (adjudicator)

oa$hip_pain[oa$n1ar0266==1]<-1 #has pain in hip area
oa$hip_pain[oa$n1ar0265==1]<-1 #has pain in hip joint
oa$hip_1[oa$n1ar0549!=0]<-1 #osteoarthrosis of the right hip. moderate, or severe. reader #1
oa$hip_2[oa$n1ar0550!=0]<-1 #osteoarthrosis of the left hip. moderate, or severe. reader #1
oa$hip_3[oa$n1ar0585!=0]<-1 #osteoarthrosis of the right hip. moderate, or severe. reader #2
oa$hip_1a[oa$n1ar0586!=0]<-1 #osteoarthrosis of the left hip.moderate, or severe. reader #2
oa$hip_2a[oa$n1ar0620!=0]<-1 #osteoarthrosis of the right hip. moderate, or severe. reader #3 (adjudicator)
oa$hip_3a[oa$n1ar0621!=0]<-1 #osteoarthrosis of the left hip. moderate, or severe. reader #3 (adjudicator)

oa[is.na(oa)]<-0

oa$knee_chart<-ifelse((oa$knee_1+oa$knee_2+oa$knee_3)>1,1,0) #require 2/3 readers to see OA

oa$hip_right<-ifelse(((oa$hip_1+oa$hip_2a+oa$hip_3)>1),1,0) #require 2/3 readers to see OA right hip
oa$hip_left<-ifelse(((oa$hip_1a+oa$hip_2+oa$hip_3a)>1),1,0) #require 2/3 readers to see OA left hip
oa$hip_chart<-ifelse((oa$hip_right==1|oa$hip_left==1),1,0) #require 2/3 readers to see OA in either right hip or left hip

#definition for oa hip and oa knee
oa$hip<-ifelse(oa$hip_chart+oa$hip_pain==2,1,0) #requires hip pain and 2/3 readers to see OA hip
oa$knee<-ifelse(oa$knee_chart+oa$knee_pain==2,1,0) #requires knee pain and 2/3 readers to see OA knee

#oa$age<-oa$n1ar0101 #didn't stratify by age because the the estimates weren't reasonable.
oa$sex<-oa$n1ar0104

############################

sr_knee<-aggregate(knee~sex, oa, FUN=sum)
sr_hip<-aggregate(hip~sex, oa, FUN=sum)
sr_total<-aggregate(total~sex, oa, FUN=sum)


sr_knee_prev<-merge(sr_knee, sr_total, by=c('sex'),all.x=T)
sr_knee_prev$mean<-sr_knee_prev$knee/sr_knee_prev$total

sr_hip_prev<-merge(sr_hip, sr_total, by=c('sex'),all.x=T)
sr_hip_prev$mean<-sr_hip_prev$hip/sr_hip_prev$total

#############################
#calculate prevalence of OA by type in NHANES

sr_hip_prev<-as.data.table(sr_hip_prev)
sr_knee_prev<-as.data.table(sr_knee_prev)
sr_hip_prev$var<-'hip'
sr_knee_prev$var<-'knee'

sr<-rbind(sr_hip_prev, sr_knee_prev, fill=T)
sr$sex_id<-ifelse(sr$sex==1,'Male','Female')

ggplot(sr, aes( var, prev))+facet_grid(~sex_id)+geom_col() #plot of hip and knee prevalence by sex.

#create hip/knee ratio to use on OA claims data
sr$cases<-ifelse(is.na(sr$hip), sr$knee, sr$hip)
knee<-sr[sr$var=='knee',]
hip<-sr[sr$var=='hip',]

ratio<-merge(knee,hip, by=c('sex'),all=T)
ratio$total<-ratio$mean.x+ratio$mean.y
ratio$fraction_knee<-ratio$mean.x/ratio$total #ratio of knee to hip
ratio$fraction_hip<-ratio$mean.y/ratio$total #ratio of knee to hip
ratio<-as.data.frame(ratio)
t<-ratio[,which(names(ratio) %in% c('sex','fraction_knee','fraction_hip'))]

mdata<-melt(t,id=c('sex') )
mdata$sex_id<-ifelse(mdata$sex==1,'Male','Female')

#Diagnostics of fraction used to split marketscan
pdf(paste0(path,filepath))
p<-ggplot(mdata, aes(x=sex_id, y=value)) + geom_col(aes(fill = variable))+ggtitle('OA fraction of knee and hip from NHANES I. \n Radiologically confirmed and self-report pain. \n Use to split marketscan') 
p<-p+ylab('Proportion')+xlab('Gender')
print(p)
dev.off()

#write file of fractions used to split OA data
write.csv(mdata,paste0(path, filepath))

############
#Apply the split ratios to marketscan
############
#read in fractions 
mdata<-read.csv(paste0(path,'/marketscan_frac.csv'))

#read in marketscan data
bundle<-  bundle_id       
name<-cause_name  
library(readxl)
new_us<-as.data.frame(read_excel(paste0(filepath)))
new_twn<-as.data.frame(read_excel(paste0(filepath)))

new_us<-new_us[new_us$year_start!=2015,]
 
#create new covariate
new_us$cv_marketscan<-ifelse(new_us$year_start>2009,1,0)
new_us$cv_taiwan_claims_data<-0
new_twn$cv_taiwan_claims_data<-ifelse(new_twn$nid==336203,1,0)
new_twn$cv_marketscan[new_twn$nid!=336203]<-1

new_us<-new_us[,-which(names(new_us) %in% c("egeoloc"))]

new<-rbind(new_us, new_twn)

#split value for knee 
knee<-mdata[mdata$variable=='fraction_knee',]
knee$sex<-knee$sex_id
knee<-knee[,which(names(knee) %in% c('sex','value'))]

#merge the claims data to the knee fraction
knee_split<-merge(new,knee, by=c('sex'), all.y=T)
knee_split$mean<-knee_split$mean*knee_split$value
knee_split$cases<-knee_split$cases*knee_split$value

knee_split<-knee_split[,-which(names(knee_split) %in% c('value'))]
ggplot(knee_split,aes(x=age_start, y=mean, color=as.factor(year_start)))+facet_wrap(~location_name)+geom_point() #plot to see how OA knee looks

#split value for hip 
hip<-mdata[mdata$variable=='fraction_hip',]
hip$sex<-hip$sex_id
hip<-hip[,which(names(hip) %in% c('sex','value'))]

#merge the claims data to the hip fraction
hip_split<-merge(new,hip, by=c('sex'), all.y=T)
hip_split$mean<-hip_split$mean*hip_split$value
hip_split$cases<-hip_split$cases*hip_split$value

hip_split<-hip_split[,-which(names(hip_split) %in% c('value'))]

ggplot(hip_split,aes(x=age_start, y=mean, color=as.factor(year_start)))+facet_wrap(~location_name)+geom_point() #plot to see how OA hip looks


write.xlsx(knee_split,paste0(filepath),sheetName='extraction')
write.xlsx(hip_split,paste0(filepath),sheetName='extraction')

#upload processed claims data with no marketscan correction
source("FILEPATH/upload_epi_data.R")

upload_epi_data(bundle_id,paste0(filepath))
upload_epi_data(bundle_id,paste0(filepath))

##############
##########Pull out marketscan data 
##############
source("FILEPATH/get_epi_data.R")
hip_data<-get_epi_data(bundle=bundle_id)
knee_data<-get_epi_data(bundle=bundle_id)

