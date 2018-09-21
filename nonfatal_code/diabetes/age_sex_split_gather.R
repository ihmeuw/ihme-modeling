#code that splits age groups >20 years, study years >5 years, and when sex is marked as "both"

#age/sex split data
library (data.table)
library (ggplot2)
library(psych)

# bring in data to age/sex split.
clean_data<-(filepath)
diagnostics<-(filepath)
age<-read.csv(filepath)

##########Function

mround <- function(x,base){ 
  base*round(x/base) 
} 

#########Create measure for split

#bring in age/sex split estimates
source(filepath)
age_split_prev<-get_model_results(gbd_team='epi', gbd_id=2005, model_version_id=160280, location_id=1, measure_id=5) #prevalence
age_split_mean<-get_model_results(gbd_team='epi',  model_version_id=156584, location_id=1, measure_id=19) #mean
age_split_prev<-merge(age_split_prev, age, by=c('age_group_id')) 
age_split_prev$type<-'prev'
age_split_mean<-merge(age_split_mean, age, by=c('age_group_id')) 
age_split_mean$type<-'cont'

#combine estimates from mean and prevalence that is marked with the type.
age_split<-rbind(age_split_mean, age_split_prev)
age_split$sex_id<-as.character(age_split$sex_id)
head(age_split)

write.csv(filepath)

#sex split code
sex_split<-age_split
sex_split.w <- reshape(sex_split, 
                       timevar = "sex_id",
                       idvar = c("age_group_id", "year_id",'model_version_id','measure_id','type'),
                       direction = "wide")

sex_split.w$sex_gender_mean<- sex_split.w$mean.2 / sex_split.w$mean.1 #create ratio between males to females
sex_split.w$sex_gender_lower<- sex_split.w$lower.2 / sex_split.w$lower.1 #create ratio between males to females
sex_split.w$sex_gender_upper<- sex_split.w$upper.2 / sex_split.w$upper.1 #create ratio between males to females

sex_split.w$sex_gender_mean_calc<- sex_split.w$mean.2 + sex_split.w$mean.1 #calculate total between mean to be used for split
sex_split.w$sex_gender_lower_calc<- sex_split.w$lower.2 + sex_split.w$lower.1 #calculate total between mean to be used for split
sex_split.w$sex_gender_upper_calc<- sex_split.w$upper.2 + sex_split.w$upper.1 #calculate total between mean to be used for split


pdf (filepath)
for (i in unique(sex_split.w$year_id)){
  temp<-sex_split.w[sex_split.w$year_id==i,]
  p<-ggplot(temp, aes(x=age_lower.2, y=sex_gender_mean))+geom_point(shape=1) +facet_grid(~type)+ylab("female to male ratio")+xlab('age')
  p<-p+ggtitle(paste0('ratio (females to males) between sexes over age at global by year:', i)) +  ylim(.5, 1.5)
  print(p)
}
dev.off()

#Split sex first because age pattern is more pronounced
#average ratio
sex_split_ratio<-aggregate(cbind(sex_gender_mean,sex_gender_lower,sex_gender_upper,
                                 sex_gender_mean_calc,sex_gender_lower_calc,sex_gender_upper_calc,
                                 mean.1,mean.2)~year_id+type, FUN=mean,data=sex_split.w) 

sex_split_ratio$year_id.2<-sex_split_ratio$year_id
write.csv(sex_split_ratio,paste0(filepath))

##############
#plot of age_distribution that will get used by time.
##############

pdf (filepath)
for (i in unique(age_split$year_id)){
  temp<-age_split[age_split$year_id==i,]
  p<-ggplot(temp, aes(x=age_lower, y=mean, colour=sex_id))+geom_point(shape=1)+facet_wrap(~type, scales="free")
  p<-p+ggtitle(paste0('age/sex distribution over time at global by year:', i))
  print(p)
}
dev.off()



#######################
#######################
#######################

#bring in base file. This gets used to add to nonfatal model only
base<-read.csv(filepath)

#bring in file that need to be split. 
age_sex_split<-read.csv(filepath, stringsAsFactors = F)

age_sex_split$year_check<-(age_sex_split$year_end-age_sex_split$year_start)
age_sex_split<-age_sex_split[ , -which(names(age_sex_split) %in% c("X.2","X.1","X"))]
age_sex_split<-unique(age_sex_split)

age_sex_split$drop[age_sex_split$nid==226224 & age_sex_split$specificity=='total']<-1
age_sex_split<-age_sex_split[is.na(age_sex_split$drop),]
age_sex_split<-as.data.table(age_sex_split)

#bring in file that doesn't need to be split
age_sex_check<-read.csv(filepath, stringsAsFactors = F)
age_sex_check<-age_sex_check[ , -which(names(age_sex_check) %in% c("X.2","X.1","X"))]
age_sex_check<-unique(age_sex_check)
age_sex_check<-as.data.table(age_sex_check)



################
####Time split
################

time_split<-rbind(age_sex_split,age_sex_check,fill=T)

time_split$year_check<-time_split$year_end-time_split$year_start

#plot difference in number of years by measure
pdf (filepath)
p<-ggplot(time_split, aes(year_check))+geom_histogram()+facet_wrap(~measure) +ylab("Number of years") +xlab("Difference between year study start and stopped")
p<-p+ggtitle(paste0('# of observations affected by difference in number of years a study \n is conducted by model type'))
print(p)
dev.off()


#for studies that last longer than 5 years.
#split based on the length of time a study gets to be held inside the model.
#currently set at 5 years
time_split_f<-time_split[time_split$year_check<6,]
time_split_t<-time_split[time_split$year_check>=6,]

#replicate rows for every year the study is conducted

time_split_t$split<-round(time_split_t$year_check/5)
time_split_t$split<-ifelse(time_split_t$split==1,2,time_split_t$split)
adjust_yr<-time_split_t
table(time_split_t$split)

adjust_yr<-as.data.table(adjust_yr)

table(adjust_yr$split)

adjust_yr.1<-adjust_yr[rep(1:.N,split)] #replicate rows
adjust_yr.1[, split_id := 1:.N , by=.(year_start,nid,age_start,sex,location_id,measure)]  ##sy: create 'split_id' col, describes the iteration number of a split by age from the original row
adjust_yr.1[, year:= year_start + (split_id-1)*5  ]     ##sy: makes new appropriate year for the new rows. Keep it as the midpoint of the 5 year moving average

#create a year for observations that have <5 year difference. Make it the floor of the midpoint
adjust_yr_u5<-time_split_f

adjust_yr_u5$year<-round((adjust_yr_u5$year_end-adjust_yr_u5$year_start)/2)+adjust_yr_u5$year_start
table(adjust_yr_u5$year,exclude=NULL)
##Recombine data set for age/sex split
adjust<-rbind(adjust_yr.1, adjust_yr_u5, fill=T)

write.csv(adjust, filepath)
############################
##############################
#############
#############Age/Sex SPLIT
#############
adjust<-read.csv(filepath)

table(adjust$location_id,exclude=NULL)
#impute sample_size when not available. make it 5th percentile pg the group  

adjust$est_ss<-ave(adjust$sample_size,adjust$measure,FUN=function(x) quantile(x,.05, na.rm=T))
summary(adjust$sample_size)
table(adjust$est_ss,adjust$measure)
adjust$sample_size<-ifelse(is.na(adjust$sample_size),adjust$est_ss,adjust$sample_size)

#remove obs with age_start>age_end
adjust<-adjust[adjust$age_check>=0,]
table(adjust$age_end,exclude=NULL)
#create datasets that need to be age/sex split

age_sex_true_split<-adjust[adjust$sex=="Both"|adjust$age_check>=20,]
age_sex_false_split<-adjust[adjust$sex!="Both" & adjust$age_check<20,]
table(age_sex_false_split$location_id,exclude=NULL)


# datasets to split

#df : create df to be split
table(age_sex_true_split$measure, age_sex_true_split$sex,exclude=NULL)
split_m_age1_sex1<-age_sex_true_split[age_sex_true_split$age_check>=20 & age_sex_true_split$sex=="Both",]
split_m_age1_sex1$group<-1
split_m_age1_sex1$group_review<-1
split_m_age1_sex1$specificity<-'age/sex split'

split_m_age0_sex1<-age_sex_true_split[age_sex_true_split$age_check<20 & age_sex_true_split$sex=="Both",]
split_m_age0_sex1$group<-1
split_m_age0_sex1$group_review<-1
split_m_age0_sex1$specificity<-'sex split'

split_m_age1_sex0<-age_sex_true_split[age_sex_true_split$age_check>=20 & age_sex_true_split$sex!="Both",]
split_m_age1_sex0$group<-1
split_m_age1_sex0$group_review<-1
split_m_age1_sex0$specificity<-'age split'

split_m_age0_sex0<-age_sex_true_split[age_sex_true_split$age_check<20  & age_sex_true_split$sex!="Both",]

split_m_age1_sex1<-as.data.frame(split_m_age1_sex1)
split_m_age0_sex1<-as.data.frame(split_m_age0_sex1)
split_m_age1_sex0<-as.data.frame(split_m_age1_sex0)
split_m_age0_sex0<-as.data.frame(split_m_age0_sex0)
################
###sex split first
################

# make DF that needs to be sexsplit
split_sex<-rbind(split_m_age1_sex1,split_m_age0_sex1)
table(split_sex$measure)
split_sex$adjust.sex<-1
split_sex$year_id<-NA
split_sex$year_id<-as.numeric(split_sex$year_id)
split_sex$year_id<-split_sex$year
split_sex$year_id[split_sex$year<=1990]<-1990 #since only have gbd estimates 1990 and later obs from before 1990 are set at 1990
table(split_sex$year, split_sex$year_id)

split_sex$year_id.2<-mround(split_sex$year_id,5) #create year_id.2 to round year for merging on sex/split distribution

table(split_sex$year_id.2, split_sex$year,exclude=NULL)
split_sex$year_id.2[split_sex$year_id.2==2015]<-2016
####
sex_split_ratio<-read.csv(filepath)

split_sex.1<-merge(split_sex,sex_split_ratio,by=c('year_id.2','measure'))
table(split_sex.1$measure)

split_sex.1$lower<-as.numeric(as.character(split_sex.1$lower))
split_sex.1$upper<-as.numeric(as.character(split_sex.1$upper))

####Adjusting datapoints
table(split_male$measure,split_male$sex,exclude=NULL)
table(split_sex.1$measure,split_sex.1$sex,exclude=NULL)

split_male<-split_sex.1
split_male$sex<-'Male'
split_female<-split_sex.1
split_female$sex<-'Female'

#male to female ratio

#split approach differs depending on if the measure is mean or prevalence.
head(split_male)
table(split_male$measure)
#for continuous
table(split_male$measure,exclude=NULL)
split_prev<-split_male[split_male$measure=='prevalence',]
split_male<-split_male[split_male$measure=='continuous',]
split_male$new_mean[split_male$measure=='continuous']<- ((1/split_male$sex_gender_mean)*split_male$mean)
split_male$new_lower[split_male$measure=='continuous']<- ((1/split_male$sex_gender_lower)*split_male$lower)
split_male$new_upper[split_male$measure=='continuous']<- ((1/split_male$sex_gender_upper)*split_male$upper)

split_prev_f<-split_female[split_female$measure=='prevalence',]
split_female<-split_female[split_female$measure=='continuous',]
  split_female$new_mean[split_female$measure=='continuous']<- (split_female$mean*split_female$sex_gender_mean)
  split_female$new_lower[split_female$measure=='continuous']<- (split_female$lower*split_female$sex_gender_lower)
  split_female$new_upper[split_female$measure=='continuous']<- (split_female$lower*split_female$sex_gender_upper)

split_sex_cont<-rbind(split_male,split_female)

ggplot(split_sex_cont, aes(x=mean, y=new_mean, colour=sex))+geom_point(shape=1) +ylab("estimated mean")+xlab('extracted mean(BOTH)')


#for prev
table(split_prev$measure,split_prev$me_name,exclude=NULL)
  split_prev_f$new_mean[split_prev_f$measure=='prevalence']<-((1/split_prev_f$sex_gender_mean)*split_prev_f$mean)
  split_prev_f$new_lower[split_prev_f$measure=='prevalence']<- ((1/split_prev_f$sex_gender_lower)*split_prev_f$lower)
  split_prev_f$new_upper[split_prev_f$measure=='prevalence']<- ((1/split_prev_f$sex_gender_upper)*split_prev_f$upper)
  
  split_prev$new_mean[split_prev$measure=='prevalence']<-((split_prev$sex_gender_mean)*split_prev$mean)
  split_prev$new_lower[split_prev$measure=='prevalence']<-((split_prev$sex_gender_lower)*split_prev$lower)
  split_prev$new_upper[split_prev$measure=='prevalence']<-((split_prev$sex_gender_upper)*split_prev$upper)
  
split_sex_prev<-rbind(split_prev, split_prev_f)
table(split_sex_prev$new_mean)
ggplot(split_sex_prev, aes(x=mean, y=new_mean, colour=sex))+geom_point(shape=1) +ylab("estimated mean")+xlab('extracted mean(BOTH)')


split_sex_combine<-rbind(split_sex_prev, split_sex_cont)
table(split_sex_combine$new_mean,exclude=NULL)

pdf (filepath)
for (i in unique(split_sex_combine$measure)){
  temp<-split_sex_combine[split_sex_combine$measure==i,]
  p<-ggplot(temp, aes(x=mean, y=new_mean, colour=sex))+geom_point(shape=1) +ylab("split estimate")+xlab('reported estimate for Both gender')
  p<-p+ggtitle(paste0('Adjusted estimate for reported observation with both gender by year:', i)) #+  ylim(.5, 1.5)
  print(p)
  }
dev.off()

#### #### #### #### 
#### Create dataset for age split
#### #### #### #### #### 

#obs that don't need to be age split
split_age_false<-split_sex_combine[split_sex_combine$age_check<20,]

#obs that need to be age split
split_age_true<-split_sex_combine[split_sex_combine$age_check>=20,]

split_m_age1_sex0$new_mean<-split_m_age1_sex0$mean #carry over the estimates into new variable name.
split_m_age1_sex0$new_lower<-split_m_age1_sex0$lower
split_m_age1_sex0$new_upper<-split_m_age1_sex0$upper

table(split_age_true$year,exclude=NULL)
split_m_age1_sex0<-as.data.table(split_m_age1_sex0)

#create file that needs to be age split.
age_adjust<-rbind(split_age_true,split_m_age1_sex0,fill=T)
###################

age_adjust$year_id<-age_adjust$year
age_adjust$year_id.2<-ifelse(is.na(age_adjust$year_id.2),age_adjust$year_id,age_adjust$year_id.2)

age_adjust$year_id.3<-mround(age_adjust$year_id.2,5)
age_adjust$year_id.3<-ifelse(age_adjust$year_id.3<1990,1990,age_adjust$year_id.3)
age_adjust$year_id.3[age_adjust$year_id.3==2015]<-2016
table(age_adjust$year_id.3)

#Identify number of rows that need to be created
table(age_adjust$age_check,exclude=NULL)
age_adjust$split_age<-round(age_adjust$age_check/5)

age_adjust$split_age<-ifelse(age_adjust$split_age==1,2,age_adjust$split_age)

age_adjust<-as.data.table(age_adjust)
adjust_age.1<-age_adjust[rep(1:.N,split_age)] #replicate rows

adjust_age.1[, split_age.1 := 1:.N , by=.(sample_size,year,nid,age_start,sex,location_id,measure, me_name, new_mean,site_memo)]  ##sy: create 'split_id' col, describes the iteration number of a split by age from the original row
adjust_age.1[, age:= age_start + (split_age.1-1)*5  ]     ##sy: makes new appropriate year for the new rows. Keep it as the midpoint of the 5 year moving average



###########
#Identify the lower bound of the smallest age group that needs to be split.

adjust_age.1<-as.data.frame(adjust_age.1)
adjust_age.1$age_lower<-mround(adjust_age.1$age_start,5) 
table(adjust_age.1$age_lower)
adjust_age.1<-as.data.table(adjust_age.1)
adjust_age.1[, age_lower1:= age_lower + (split_age.1-1)*5  ]     ##sy: makes new appropriate year for the new rows. Keep it as the midpoint of the 5 year moving average
adjust_age.1$age_lower<-adjust_age.1$age_lower1
adjust_age.1<-adjust_age.1[adjust_age.1$age_lower<100,] #throw out upper age groups.
table(adjust_age.1$age_lower)

#########
#Throw out obs with age<25
adjust_age.1<-adjust_age.1[adjust_age.1$age_lower>20,]

###########

#read in age_split reference file

age_split<-read.csv(filepath)
table(age_split$age_lower)
age_split$age_start<-age_split$age_lower

age_split$year_id.3<-age_split$year_id
age_split$sex<-ifelse(age_split$sex_id==1,'Male','Female')

head(age_split)
#fix upper age group mean for now
#wont' split <25 for mean
#80+ means have the same mean as 75-79 because the model that was used to produce those estimates.
#adjust_age.2<-adjust_age.1[(adjust_age.1$measure =='prevalence' |(adjust_age.1$measure=='continuous' & adjust_age.1$age_start>25)),]
#table(adjust_age.2$age_start,adjust_age.2$measure,exclude=NULL)

adjust_age.2<-adjust_age.1

adjust_age_test.2<-merge(adjust_age.2, age_split, by=c('age_lower','year_id.3','measure','sex'),all.x=T)

table(adjust_age_test.2$nid)
adjust_age_test.2<-as.data.frame(adjust_age_test.2)

######################

adjust_age_test.2<-as.data.table(adjust_age_test.2)
adjust_age_test.2$age_start<-adjust_age_test.2$age_lower
adjust_age_test.2$age_end<-adjust_age_test.2$age_start + 5

#### Split age for mean
adjust_age_test.2a<-adjust_age_test.2[adjust_age_test.2$measure=='continuous']
#############
###########
adjust_age_test.2a[, age.avg_prev := mean(mean.y), by=.(sample_size, nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2a[, age.avg_lower := mean(lower.y), by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2a[, age.avg_upper := mean(upper.y), by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   

adjust_age_test.2a[, age.ratio_prev := mean.y/age.avg_prev, by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2a[, age.ratio_lower := lower.y/age.avg_lower, by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2a[, age.ratio_upper := upper.y/age.avg_upper, by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   

adjust_age_test.2a[, new_mean.2 := new_mean*age.ratio_prev, by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2a[, new_lower.2  := new_lower*age.ratio_lower, by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2a[, new_upper.2  := new_upper*age.ratio_upper, by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   

library(ggplot2)
#head(adjust_age_test.2a)
ggplot(adjust_age_test.2a,aes(y=new_mean.2, x=age_lower))+geom_point()+facet_grid(~sex)
ggplot(adjust_age_test.2a,aes(y=new_mean.2, x=new_mean))+geom_point()+facet_grid(~sex)

#### Split age for prevalence
#delete observations where the mean is >0.6

adjust_age_test.2b<-adjust_age_test.2[adjust_age_test.2$measure=='prevalence']
adjust_age_test.2b<-adjust_age_test.2b[adjust_age_test.2b$new_mean<0.6,]
adjust_age_test.2b[, age.tot_prev := sum(mean.y), by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2b[, age.tot_lower := sum(lower.y), by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2b[, age.tot_upper := sum(upper.y), by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   

adjust_age_test.2b[, age.prop_prev := mean.y/age.tot_prev]   #prev
adjust_age_test.2b[, age.prop_lower := lower.y/age.tot_lower]   #prev
adjust_age_test.2b[, age.prop_upper := upper.y/age.tot_upper]   #prev

adjust_age_test.2b[, age.tot_prev_extract := sum(new_mean), by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2b[, age.tot_lower_extract := sum(new_lower), by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   
adjust_age_test.2b[, age.tot_upper_extract := sum(new_upper), by=.(sample_size,nid,  year_id.3, sex,location_id.x, age_start.x)]   


adjust_age_test.2b[, new_mean.2 := age.prop_prev*age.tot_prev_extract]   #prev
adjust_age_test.2b[, new_lower.2 := age.prop_lower*age.tot_lower_extract]   #prev
adjust_age_test.2b[, new_upper.2 :=  age.prop_upper*age.tot_upper_extract]   #prev


ggplot(adjust_age_test.2b,aes(y=new_mean.2, x=age_lower))+geom_point()+facet_grid(~sex)
ggplot(adjust_age_test.2b,aes(y=new_mean.2, x=new_mean))+geom_point()+facet_grid(~sex)

x<-adjust_age_test.2b[adjust_age_test.2b$new_mean.2<.01,]
ggplot(x,aes(y=new_mean.2, x=age_lower))+geom_point()+facet_grid(~sex)
ggplot(x,aes(y=new_mean.2, x=new_mean))+geom_point()+facet_grid(~sex)

#x<-adjust_age_test.2b[adjust_age_test.2b$nid==235397 & adjust_age_test.2b$sex=="Male",]
#head(adjust_age_test.2b)
#ggplot(x,aes(y=new_mean.2, x=new_mean))+geom_point()+facet_grid(~sex)
#table(x$nid, x$mean.x)
#x.1<-x[x$nid==235397,]
#head(x.1, 50)
####Combine files. 
adjust_age_test.2a<-as.data.table(adjust_age_test.2a)
adjust_age_test.2b<-as.data.table(adjust_age_test.2b)
done_adjust<-rbind(adjust_age_test.2a,adjust_age_test.2b,fill=T)
done_adjust$mean<-done_adjust$new_mean.2
done_adjust$lower<-done_adjust$new_lower.2
done_adjust$upper<-done_adjust$new_upper.2
done_adjust$location_id<-done_adjust$location_id.x
table(done_adjust$age_start,done_adjust$age_end,exclude=NULL)
table(done_adjust$location_id,exclude=NULL)

age_sex_false_split<-as.data.table(age_sex_false_split)

age_sex_false_split$age<-((age_sex_false_split$age_end-age_sex_false_split$age_start)/2)+age_sex_false_split$age_start
age_sex_false_split$age_start<-mround(age_sex_false_split$age,5)
age_sex_false_split$age_end<-age_sex_false_split$age_start+5
age_sex_false_split<-age_sex_false_split[age_sex_false_split$age_start>20,]

table(age_sex_false_split$year_start,age_sex_false_split$year_end,exclude=NULL)
table(age_sex_false_split$location_id,exclude=NULL)

split_age_false<-as.data.table(split_age_false)
split_age_false$age<-((split_age_false$age_end-split_age_false$age_start)/2)+split_age_false$age_start
split_age_false$age_start<-mround(split_age_false$age,5)
split_age_false$age_end<-split_age_false$age_start+5
split_age_false$mean<-split_age_false$new_mean
split_age_false$lower<-split_age_false$new_lower
split_age_false$upper<-split_age_false$new_upper

split_age_false<-split_age_false[split_age_false$age_start>20,]

table(split_age_false$year_start,split_age_false$year_end,exclude=NULL)
table(age_sex_false_split$location_id,exclude=NULL)
#split_m_age0_sex0<-as.data.table(split_m_age0_sex0)


#doesn't include <25 and non-prevalence /mean/claims values. 
combine<-rbind(done_adjust,age_sex_false_split,split_age_false, fill=T)
table(combine$year_start,exclude=NULL)
length(unique(combine$nid))
length(unique(adjust$nid))



table(combine$location_id,exclude=NULL)
table(combine$age_start, combine$age_end,exclude=NULL)

#fix group,group-review,specificity for this
combine$group<-NA
combine$group_review<-NA
combine$specificity<-NA

combine$check<-combine$age_end-combine$age_start

table(combine$uncertainty_type,exclude=NULL)

#Fixing data for upload.
combine$representative_name[combine$representative_name=='Samples drawn from 2 companies there, maybe biased towards adult male']<-'Unknown'
combine$representative_name[combine$representative_name=='']<-'Unknown'
combine$representative_name[combine$representative_name=='Representative of subnational locations and urban population']<-'Representative for subnational location and urban/rural'
combine$uncertainty_type[combine$uncertainty_type=='SD']<-'Standard error'

#check upper and lower bound
combine$lower<-as.numeric(combine$lower)
combine$upper<-as.numeric(combine$upper)
combine$check.1[!is.na(combine$lower) & (combine$mean<combine$lower)]<-1
combine$check.2[!is.na(combine$upper) & (combine$mean>combine$upper)]<-1
table(combine$check.1, combine$check.2,exclude=NULL)

combine.1<-combine[is.na(combine$check.1) & is.na(combine$check.2)]
length(unique(combine.1$nid))

##########

table(combine.1$measure,combine.1$me_name,exclude=NULL)
combine.1<-as.data.frame(combine.1)


epi_upload<-c('seq','seq_parent','input_type','underlying_nid','nid','underlying_field_citation_value',
              'field_citation_value','file_path','page_num','table_num','source_type','location_name','location_id','ihme_loc_id',
              'smaller_site_unit','site_memo','sex','sex_issue','year_start','year_end','year_issue','age_start',
              'age_end','age_issue','age_demographer','measure','mean','lower','upper','standard_error',
              'effective_sample_size','cases','sample_size','design_effect',
              'unit_type','unit_value_as_published','measure_issue','measure_adjustment','uncertainty_type',
              'uncertainty_type_value',
              'representative_name','urbanicity_type','recall_type','recall_type_value','sampling_type','response_rate',
              'case_name','case_definition','case_diagnostics','group','specificity','group_review',
              'note_modeler','note_SR','extractor','is_outlier',
              'data_sheet_filepath','bundle_id','bundle_name','cv_marketscan_all_2000','cv_marketscan_inp_2000','cv_marketscan_all_2010',
              'cv_marketscan_inp_2010','cv_marketscan_all_2012','cv_marketscan_inp_2012','cv_fpg110',
              'cv_fpg122_self_drug','cv_fpg126','cv_fpg126_ogtt200','cv_fpg126_ogtt200_self_drug','cv_fpg126_ppg200',
              'cv_fpg126_ppg200_self_drug','cv_fpg140','cv_fpg140_ppg200','cv_fpg140_ppg200_self_drug',
              'cv_fpg140_self_drug','cv_ha1c65_self_drug','cv_ppg200','age','year','Standardized.case.definition','me_name')

combine.1<-combine.1[epi_upload]

combine.1$unit_type<-'Person'
combine.1$uncertainty_type[!is.na(combine.1$lower)]<-'Confidence interval'
combine.1$uncertainty_type_value[combine.1$uncertainty_type=='Confidence interval']<-95

table(combine.1$uncertainty_type,combine.1$uncertainty_type_value, exclude=NULL)
combine.1[is.na(combine.1)]<-''
write.csv(combine.1,paste0(diagnostics,filepath))
#Upload into epi-uploader to get variance estimates

table(combine$uncertainty_type)
