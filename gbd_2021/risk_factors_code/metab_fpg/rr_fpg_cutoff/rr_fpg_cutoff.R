##############################################################################
## DESCRIPTION: Creates the average exposure level above and below a cutpoint
##############################################################################
rm(list = ls())
# System info
os <- Sys.info()[1]
user <- Sys.info()[7]
# Drives
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "FILEPATH") else if (os == "Windows") "FILEPATH"
# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- 'FILEPATH'
user<-'USERNAME'
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "FILEPATH") else if (os == "Windows") ""
## Load dependencies & packages
source(paste0(code_dir, 'FILEPATH'))
library(dplyr)
library(rio)
library(lme4)
library(mvtnorm)
library(actuar)
library(zipfR)
library(Rcpp)
#functions
source(paste0(code_dir, 'FILEPATH'))
source("FILEPATH")
source("FILEPATH")
sourceCpp("FILEPATH")
source("FILEPATH")
source("FILEPATH")
run_id <-192363
#bring in arguments from wrapper call
location_id <- c(1, 5 ,  9,  21,  32,  42,  56  ,65,  70,  73,  96, 100, 104, 120, 124, 134, 138, 159, 167, 174, 192, 199)
save_dir <-'FILEPATH'
if(!dir.exists(save_dir)){
  dir.create(save_dir)}
message( paste0("This is the log file for location id: ", location_id, " and fpg run_id: ", run_id))
####
if(F){
  location_id <- c(1)#, 5 ,  9,  21,  32,  42,  56  ,65,  70,  73,  96, 100, 104, 120, 124, 134, 138, 159, 167, 174, 192, 199)
  run_id <- 162530
}
#import weights, means, and sds
blist <- fread("FILEPATH")
blist <- blist[1, c("exp" , "gamma",     "llogis",    "gumbel",  "weibull",     "lnorm" , "norm" ,"glnorm"  ,  "betasr" ,    "mgamma"   , "mgumbel")]
# FPG - 18705
sd_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id=18705, location_id = location_id, source="epi", gbd_round_id = 7, decomp_step = "iterative")

df <- get_draws(source = "epi",gbd_id_type = "modelable_entity_id", gbd_id= 8909, location_id = location_id,  gbd_round_id = 7, decomp_step = "iterative")

#make some edits to imported objects
years <- c(2020) #c(2000,2005,2010,2015,2017,2019)
location_id <- c(1, 5 ,  9,  21,  32,  42,  56  ,65,  70,  73,  96, 100, 104, 120, 124, 134, 138, 159, 167, 174, 192, 199)
sd_draws <- sd_draws[year_id %in% years & age_group_id %in% c(13:19)]
df <- df[location_id %in% location_id & year_id  %in% years & age_group_id %in% c(13:19)]   #focus on 60-64 years

#add clean age and sex labels for the plots
ages <- get_age_metadata(12)
ages[, age_start:=round(age_group_years_start)]
ages[, age_end:=round(age_group_years_end)]
df <- merge(df, ages, by="age_group_id")
df[, age_label:=paste0(age_start, "-", age_end)]
df[ age_group_id==235, age_label:="95+"]
df[,sex := ifelse(sex_id == 1, "Male","Female")]
setnames(df, "draw_0", "draw_1000")
setnames(sd_draws, "draw_0", "draw_1000")
pdf(file=paste0(save_dir, location_id, "_diabetes_dist.PDF"))
dlist <- c(classA,classB,classM)
options(warn=-1)
upper<-NULL
lower<-NULL

m<-7
for (cut in m) {
#to_plot <- sample(1:2,1,replace=F)
for (i in 1:50) {
  message(i)
 # df[,paste0('prev_',i)]=NA
  for(j in 1:nrow(df))
    ## tryCatch loop - some draws have a non-finite function error
    tryCatch({
      age <- df[j, age_label]
      sex <- df[j, sex]
      year <- df[j, year_id]
      loc <- df[j, location_id]
      Mval <- df[j,get(paste0("draw_",i))]   #pulls out of draw of the mean
      Spred <- sd_draws[sex_id==df[j, sex_id] & age_group_id==df[j, age_group_id] & year_id==df[j, year_id] & location_id==df[j, location_id], get(paste0("draw_",i))]   #pulls out draw of the sd, ensuring it matches demographics of mean
      D <- NULL
      D <- get_edensity(weights=blist,mean=Mval,sd=Spred,scale=TRUE)  #use function to get a density function based on distribution weights and mean/sd, scales to 1
          ## predict prev
      den = approxfun(D$x, D$fx, yleft=0, yright=0)

      # curve(den,D$XMIN,D$XMAX)
      TOTAL_INTEG = integrate(den,D$XMIN,D$XMAX)$value
     # TOTAL_INTEG = integrate(den,7,D$XMAX)$value
      #cut <- 0.01
      #cut<-5.6
      fpg_max<-D$XMAX
      fpg_min<-D$XMIN
      PROP= integrate(den,cut,D$XMAX)$value/TOTAL_INTEG    #integrate higher than the cutpoint


      d_density<-as.data.frame(D$fx)
      d_mean<-as.data.frame(D$x)
      d_update<-cbind(d_density,d_mean)

      names(d_update)[names(d_update) == "D$x"] <- "mean"
      names(d_update)[names(d_update) == "D$fx"] <- "density"
      d_update_upper<-d_update[d_update$mean>=cut,] #change to upper bound
      d_update_upper<-as.data.table(d_update_upper)
      range_85<-quantile(d_update_upper$mean, c(.85))
      update_upper<-d_update_upper[,j=list(mean_upper=sum(mean*density)/sum(density))]
      update_upper<-cbind(j,update_upper, age, sex, year,loc,cut,fpg_max, range_85)

      # integrate(curve, lower, upper)
      #PROP = ifelse(is.na(PROP),0,PROP)
      #if you want to print plots, comment these in and choose demographics

      df_lower<-df
      d_update_lower<-d_update[d_update$mean<cut,] #change to upper bound
      d_update_lower<-as.data.table(d_update_lower)
      range_15<-quantile(d_update_lower$mean, c(.15))
      update_lower<-d_update_lower[,j=list(mean_lower=sum(mean*density)/sum(density))]
      update_lower<-cbind(j,update_lower, age, sex, year,loc,cut,fpg_min,range_15)

       upper<-rbind(upper, update_upper, fill=T)
      lower<-rbind(lower, update_lower, fill=T)
       D<-as.data.frame(D)
    #  if(i %in% to_plot & age %in% c("60-64","40-45",'50-54','70-74')){
    #    p<-ggplot(D, aes(x=x, y=fx))+geom_point()+ggtitle(paste0("draw", i, " for location ", location_id,", ", sex, "s ", age))+ xlab(" FPG (mmol/L)") + ylab("Population density")
    #    p<-p+geom_vline(xintercept=cut, col='red')+geom_vline(xintercept=cut, col='red')
      #  print(text(x=D$XMAX,y=0.2, labels=paste0( round(PROP, 2)), col="red"))

  #    }
      print(paste0("DRAW:",i," ROW = ",j,"/",nrow(df)))
  # }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
    })
}



#dev.off()
options(warn=0)
upper$type<-'upper'
lower$type<-'lower'
df_out<-rbind(upper,lower,fill=T)

#df_out<-upper
#df_out <- df %>% dplyr::select(location_id,age_group_id,sex_id,year_id,starts_with("draw_"))
#df_out <- df_out %>% rename(draw_0 = draw_1000)
#setnames(df_out, "draw_1000", "draw_0")
#df_out <- df_out %>% mutate(measure_id=19)
message("Hooray! All draws are complete!")
write.csv(df_out,paste0(save_dir,"average_region_cut_7.csv"), row.names = FALSE)
}
########find levels#######
source('FILEPATH')
loc<-as.data.frame(get_location_metadata(location_set_id = 35, gbd_round_id=7,decomp_step = 'iterative'))
loc<-loc[,which(names(loc) %in% c('location_id','ihme_loc_id','location_name','region_name','parent_id','location_ascii_name','location_type'))]


#df_out<-df_out[!is.na(df_out$mean),]
df_out$location_id<-df_out$loc
df_out$mean<-ifelse(is.na(df_out$mean_lower),df_out$mean_upper,df_out$mean_lower)

df_out<-merge(df_out,loc,by=c('location_id'),all.x=T)

df_out1<-df_out

pdf('FILEPATH', width=11, height=8)
for (i in unique(df_out1$location_name)){
  temp<-df_out1[df_out1$location_name==i,]
p<-ggplot(temp, aes(mean))+facet_grid(type~age)+geom_histogram(bins=10)+theme_bw()
p<-p+ggtitle(paste0('# of draws with mean FPG by age and location ', i))
print(p)
}
dev.off()

range<-df_out[,j=list(mean=mean(mean),mean_max=mean(fpg_max),mean_85=mean(range_85),mean_15=mean(range_15),mean_min=mean(fpg_min)),by=c('year','type','loc', 'cut')]
range$location_id<-range$loc
range<-as.data.frame(range)
range<-merge(range, loc, by=c('location_id'),all.x=T)
range<-range[,which(names(range) %in% c('location_name','mean','cut','mean_85','mean_max','mean_15','mean_min'))]
range$region_name[range$location_ascii_name=='Global']<-'Global'
write.csv(range,paste0(save_dir,"average_region_cut_7_summary.csv"), row.names = FALSE)

pdf(paste0(save_dir,"_average_region_cut.pdf"))
for (i in unique(range$sex)){
  temp<-range[range$sex==i,]
  for (j in unique(temp$region_name)){
    temp1<-temp[temp$region_name==j,]
    for (k in unique(temp1$cut)){
      temp2<-temp1[temp1$cut==k,]
p<-ggplot(temp2, aes(x=as.factor(year), y=mean, color=as.factor(sex)))+facet_grid(age~type)+geom_bar(stat='identity')+ geom_text(aes(label=round(mean,2)), vjust=0, color='red')
p<-p+ggtitle (paste0('Mean FPG values below and above ',k,' mmol/L by sex and year in \n ',j,'. \n Use for ES unknown imputation.'))+theme_bw()+coord_cartesian( ylim = c(2, 20))
print(p)
}
  }
}
dev.off()

#create all year/all age average
source('FILEPATH')
source('FILEPATH')

age<-as.data.frame(get_age_metadata(age_group_set_id = 12, gbd_round_id=7))
age<-age[,which(names(age) %in% c('age_group_id','age_group_years_start','age_group_years_end','age_group_weight_value'))]
pop<-data.frame(get_population(location_id=location_id,sex_id = c(1,2),year_id= c(1990,2000,2019),age_group_id=c(13:19),gbd_round_id = 7,decomp_step = 'step3'))

pop$age[pop$age_group_id==13]<-'40-45'
pop$age[pop$age_group_id==14]<-'45-50'
pop$age[pop$age_group_id==15]<-'50-55'
pop$age[pop$age_group_id==16]<-'55-60'
pop$age[pop$age_group_id==17]<-'60-65'
pop$age[pop$age_group_id==18]<-'65-70'
pop$age[pop$age_group_id==19]<-'70-75'
pop$sex<-ifelse(pop$sex_id==1,'Male','Female')
pop$year<-pop$year_id


range1<-merge(range,pop, by=c('sex', 'year','age'), all.x=T)
range1<-merge(range1,age,by=c('age_group_id'),all.x=T)
total<-aggregate(age_group_weight_value~year_id+sex+region_name+cut,range1, FUN=sum)
range1<-merge(range1,total, by=c('year_id','sex','region_name','cut'),all.x=T)
range1$wt<-(range1$age_group_weight_value.x/range1$age_group_weight_value.y)*2

range1$wt_updated<-range1$wt*range1$mean
val<-aggregate(wt_updated~type+year+sex+region_name+cut, range1,FUN=sum)
overall<-aggregate(wt_updated~type+region_name+cut, val, FUN=mean)
overall$year<-'1990,2000,2019'
overall$sex<-'Both'
head(overall)
val<-rbind(val,overall)

pdf(paste0(save_dir,"_average_region_cut_overall.pdf"))
for (i in unique(val$region_name)){
  temp<-val[val$region_name==i,]
  for (k in unique(temp$cut)){
    temp1<-temp[temp$cut==k,]

  p<-ggplot(temp1, aes(x=year, y=wt_updated))+facet_grid(sex~type)+geom_bar(stat='identity')
  p<-p+ geom_text(aes(label=round(wt_updated,2)), vjust=0, color='red')
  p<-p+ggtitle(paste0('age-weighted overall estimate in ',i, ' and cut point ',k))
  print(p)
}
}
dev.off()

#################
################
#update age weights
range2<-read.csv('FILEPATH')
range2$cut<-7
df_out1<-rbind(df_out, range2)
#df_out1<-df_out
range<-df_out1[,j=list(mean=mean(mean)),by=c('sex','year','type','age','loc', 'cut')]
range$location_id<-range$loc

range1<-merge(range,pop, by=c('sex', 'year','age','location_id'), all.x=T)
range1<-merge(range1,loc, by=c('location_id'),all.x=T)
range1<-merge(range1,age,by=c('age_group_id'),all.x=T)

total_updated_total<-range1[,j=list(total=(sum(age_group_weight_value))), by=c('location_name','sex','year','type','cut')]
range1<-merge(range1,total_updated_total, by=c('location_name','sex','year','type','cut'), all.x=T)
total_updated_wt<-range1[,j=list(wt=(age_group_weight_value/total)), by=c('location_name','sex','year','type','age_group_years_start','cut')]
total_updated_wt<-unique(total_updated_wt)
range1<-as.data.frame(range1)

total_updated_wt<-as.data.frame(total_updated_wt)
range1<-merge(range1,total_updated_wt, by=c('location_name', 'sex','year','type','age_group_years_start','cut'), all.x=T)
range1<-as.data.table(range1)
range1_updated_wt<-range1[,j=list(wt.new=sum(mean*wt)), by=c('location_name','sex','year','type','cut')]

#create overall averages
overall<-range1_updated_wt[,j=list(wt.new=mean(wt.new)), by=c('location_name','type','cut')]
overall$sex<-'Both'
overall$year<-9999
range1_updated_wt<-rbind(range1_updated_wt,overall)

#reshape file
library(reshape2)
data_wide <- dcast(range1_updated_wt, location_name+sex+year+cut ~ type, value.var="wt.new")

data_wide$imputed_lower<-(data_wide$lower-(data_wide$cut-data_wide$lower))
data_wide$imputed_upper<-(data_wide$upper + (data_wide$upper-data_wide$cut))

p<-ggplot(data_wide[data_wide$cut<8,],aes(x=as.factor(cut),y=imputed_lower, color=location_name))+facet_grid(sex~year)+geom_point()+theme_bw()
p<-p+xlab('mmol/L (upper bound cut point)')+ylab('imputed lower bound value')
p

p<-ggplot(data_wide[data_wide$cut >=7,],aes(x=as.factor(cut),y=imputed_upper, color=location_name))+facet_grid(sex~year)+geom_point()+theme_bw()
p<-p+xlab('mmol/L (unknown upper bound)')+ylab('imputed upper bound value')
p
