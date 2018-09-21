

#load libraries
library(mgcv) #for GAMM
library(plotrix)

library(doMC)

library(caTools)

registerDoMC(cores=10)

gdb_2016_country_list <- read.csv("FILEPATH", sep=",",stringsAsFactors=F)


#import dataset

setwd("FILEPATH")


TreatSeek <- read.csv("FILEPATH")

#Shorten some of the long country names

TreatSeek$Country <- as.factor(TreatSeek$Country)


TreatSeek$CountryYear <- paste(TreatSeek$Country, TreatSeek$Year, sep=" ")



union_list<-list()

for (i_sim in 1 :10){
  
 
  cleandata <- droplevels(na.omit(TreatSeek))

  
  cleandata <- droplevels(na.omit(TreatSeek[,c('Any_treat','WHO_Sub','Year','HealthExTotal','PregWomenCare','PrimaryComplete','DPT','GDPGrowth','Country','Country_ISO3Code','Any_treat_low', 'Any_treat_high')]))
  

  
  
  pub_treat<-numeric()
  
  for (i in 1:dim(cleandata)[1]){
    
    mean_y<-cleandata$Any_treat[i]
    
    sd_y<-(round(cleandata$Any_treat[i],2)- round(cleandata$Any_treat_low[i],2))/2
    

    
    pub_treat<- c(pub_treat,rnorm(1,mean=mean_y,sd=sd_y))
  }
  
  cleandata$Any_treat<-pub_treat 
  
  formula_hmis2 <-  Any_treat~  (Year) + PrimaryComplete+ PregWomenCare
  

  
  chosen_model_hmis2<-gam(formula_hmis2, data=cleandata )
  
  
  ##### Create database prediction #####
  
  
  file_name<-c(
               '/FILEPATH',
        
               'FILEPATH'

  )
  
  
  column_names<-c('PrimaryComplete' , 'PregWomenCare')
  
  
  ###############################################################################################
  ###############################################################################################
  
  national_country<-read.csv('FILEPATH')
  
  
  any_treat<-numeric()
  
  for (i in 1:dim(national_country)[1]){
    
    mean_y<-national_country$any_ts[i]
    
    sd_y<-national_country$se_any[i]
    
  
    
    any_treat<- c(any_treat,rnorm(1,mean=mean_y,sd=sd_y))
  }
  
  
  national_country$any_ts_new<-any_treat
  
  
  d_m3<-tapply(national_country$any_ts_new,list(national_country$map_state,factor(national_country$year,levels=1980:2016)),mean)
  
  pred_list<-foreach (i = 1:10) %dopar%{ 
    
    list_value<-list()
    
    for (i_f in 1:2){
      
      cov_db<-read.csv(file_name[i_f],sep=';',stringsAsFactors=F)
      
      new_value<-numeric()
      
      for (i_iso in sort(unique(cov_db$iso))){
        
        country_db<- cov_db[cov_db$iso==i_iso,]
        
        
        for ( i in 1:37){
          
          new_value<-c(new_value, rnorm(1,mean=country_db$gp_value[i],sd=sqrt(country_db$mse[i]))) 
          
          
        }
        
        
      }
      
      list_value[[length(list_value)+1]]<-new_value
      
    }
    
    pred_model_data<-cbind(cov_db[,1:3],do.call(cbind,list_value))
    
    names(pred_model_data)[4:5]<-column_names
    
    
    #model_coef<-coef(chosen_model_hmis2)
    
    mean_model<-summary(chosen_model_hmis2)$p.table[,1]
    
    sd_model<-summary(chosen_model_hmis2)$p.table[,2]
    
    model_coef2<-numeric()
    
    for (i_m in 1:length(mean_model)){
      
      model_coef2<-c(model_coef2,rnorm(1,mean=mean_model[i_m],sd=sd_model[i_m]))
      
      
    }
    

    
    
    d_m3<-tapply(national_country$any_ts_new,list(national_country$map_state,factor(national_country$year,levels=1980:2016)),mean)
    
    
    iso_country<-numeric()
    
    #par(mfrow=c(3,2))
    
    list_alg<-as.numeric()
    
    for (cnty in rownames(d_m3)){
      
      # all country
      
      model_coef<-model_coef2
      
      
      if (cnty=='SWZ'){
        model_coef[4]<-0
        
      }
      
      
      
      iso_country<-c(iso_country,cnty)
      
      
      d_m_pos<-which(rownames(d_m3)==cnty)
      
      cov_country<-pred_model_data[ pred_model_data$iso=='IND', ] 
      
      cov_country$year[cov_country$year<1981]<-1980
      
      seek_value<-d_m3[d_m_pos,]#c(rep(NA,10),d_m3[d_m_pos,],NA,NA)
      
      pos_data<-which(seek_value>0)
      

      
      start_na<-as.numeric()
      
      value=1  
      
      while (is.na(seek_value[value]))
      {  
        start_na<-c(start_na,value)
        
        value=value+1
      } 
      
      
      
      for (v in rev(start_na))
      {  
        
        
        year_cov1<-cov_country[value, ]
        
        year_cov2<-cov_country[v, ]
        
        intercept_rand<-seek_value[value]-(year_cov1[,3]*model_coef[2])-(year_cov1[,4]*model_coef[3])-(year_cov1[,5]*model_coef[4])
        
        missing_data<-intercept_rand+(year_cov2[,3]*model_coef[2])+(year_cov2[,4]*model_coef[3])+(year_cov2[,5]*model_coef[4])
        
        
        seek_value[v]<-missing_data
        
      }
      
      

      
      if (sum(seek_value[-1],na.rm=T)==0){
        
        old_method<-'no'
        
      }else{
        
        
        old_method<-'yes'
        
      }
      
      
      
      if (old_method=='yes')
        
      {  
        
        end_na<-as.numeric()
        
        value=length(seek_value)  
        
        while (is.na(seek_value[value]))
        {  
          end_na<-c(end_na,value)
          
          value=value-1
        } 
        
        
        
        if (value<37)
          
        {
          
          for (v in rev(end_na))
            
          {
            
            
            year_cov1<-cov_country[v-1, ]
            
            year_cov2<-cov_country[v, ]
            
            intercept_rand<-seek_value[v-1]-(year_cov1[,3]*model_coef[2])-(year_cov1[,4]*model_coef[3])-(year_cov1[,5]*model_coef[4])
            
            missing_data<-intercept_rand+(year_cov2[,3]*model_coef[2])+(year_cov2[,4]*model_coef[3])+(year_cov2[,5]*model_coef[4])
            
            
            seek_value[v]<-missing_data
            
            
            
          }
          
          
        }
        
      }else{
        
        
        if (is.na(tail(seek_value,1)))
          
        {
          seek_value[length(seek_value)]<-tip_tail
          
        }
        
        
        
      }
      
      
      
      
      #  missing value in the middle of time series
      
      
      if(sum(as.numeric(is.na(seek_value)))>0)
        
      {
        
        
        empty_v<-which(is.na(seek_value))
        
        middle_na<-as.numeric()
        
        value=length(seek_value)  
        
        
        # first primer
        
        
        seek_value2<-seek_value
        
        front_back<-as.numeric()
        
        for (v in 1:value)
        {  
          
          
          
          if (is.na(seek_value2[v])){
            
            prev<-v-1
            
            after_value<-NA
            
            n=1
            
            while ( is.na(after_value))
            {
              
              
              after<-v+n
              
              after_value<-seek_value2[after]
              
              n=n+1
            }
            
            
            year_cov1<-cov_country[prev, ]
            
            year_cov2<-cov_country[v, ]
            
            year_cov3<-cov_country[after, ]
            
            intercept_rand1<-seek_value2[prev]-(year_cov1[,3]*model_coef[2])-(year_cov1[,4]*model_coef[3])-(year_cov1[,5]*model_coef[4])
            
            missing_data1<-intercept_rand1+(year_cov2[,3]*model_coef[2])+(year_cov2[,4]*model_coef[3])+(year_cov2[,5]*model_coef[4])
            
            intercept_rand2<-seek_value2[after]-(year_cov1[,3]*model_coef[2])-(year_cov3[,4]*model_coef[3])-(year_cov3[,5]*model_coef[4])
            
            missing_data2<-intercept_rand2+(year_cov2[,3]*model_coef[2])+(year_cov2[,4]*model_coef[3])+(year_cov2[,5]*model_coef[4])
            
            exp_mid3<-(missing_data1+missing_data2)/2
            
            
            seek_value2[v]<-exp_mid3
            
            front_back<-c(front_back,exp_mid3)
            
            
          }
        } 
        
        
        
        # second primer
        
        
        seek_value3<-seek_value
        
        
        back_front<-as.numeric()
        
        for (v in value:1)
        {  
          
          # print (v)
          
          if (is.na(seek_value3[v])){
            
            after<-v+1
            
            pre_value<-NA
            
            n=1
            
            while ( is.na(pre_value))
            {
              
              
              prev<-v-n
              
              pre_value<-seek_value3[prev]
              
              n=n+1
            }
            
            
            year_cov1<-cov_country[prev, ]
            
            year_cov2<-cov_country[v, ]
            
            year_cov3<-cov_country[after, ]
            
            intercept_rand1<-seek_value3[prev]-(year_cov1[,3]*model_coef[2])-(year_cov1[,4]*model_coef[3])-(year_cov1[,5]*model_coef[4])
            
            missing_data1<-intercept_rand1+(year_cov2[,3]*model_coef[2])+(year_cov2[,4]*model_coef[3])+(year_cov2[,5]*model_coef[4])
            
            intercept_rand2<-seek_value3[after]-(year_cov1[,3]*model_coef[2])-(year_cov3[,4]*model_coef[3])-(year_cov3[,5]*model_coef[4])
            
            missing_data2<-intercept_rand2+(year_cov2[,3]*model_coef[2])+(year_cov2[,4]*model_coef[3])+(year_cov2[,5]*model_coef[4])
            
            
            exp_mid3<-(missing_data1+missing_data2)/2
            
            seek_value3[v]<-exp_mid3
            
            
            back_front<-c(back_front,exp_mid3)
            
            
          }
        } 
        
        
        
        seek_value<-(seek_value2+seek_value3)/2
        
        
      }
      
     
      
      list_alg<-c(list_alg,seek_value)
    }
    
    return(list_alg)
    
  }
  
  
  first_sim_res<-do.call(cbind,pred_list)  
  
  pred_algo_data<-data.frame(iso=rep(rownames(d_m3),each=37),year= rep(1980:2016,length(rownames(d_m3))))
  
  pred_algo_data<-cbind(pred_algo_data,first_sim_res)
  
  
  missing_state=0
  
  if(missing_state==1){
  
  
  #############################################
  ###### predict place without values #########
  #############################################
  
  
  d_m3<-tapply(cleandata$Any_treat,list(cleandata$Country_ISO3Code,cleandata$Year),mean)
  
  extra_countries<-sort(gdb_2016_country_list$iso[-c(match(rownames(d_m3),gdb_2016_country_list$iso))])
  
  extra_country=1
  
  if(extra_country==1){
    
    
    pred_list2<-foreach (i = 1:10) %dopar%{ 
      
      list_value<-list()
      
      for (i_f in 1:2){
        
        cov_db<-read.csv(file_name[i_f],sep=';',stringsAsFactors=F)
        
        cov_db<-cov_db[cov_db$iso %in% extra_countries,]
        
        new_value<-numeric()
        
        for (i_iso in sort(unique(cov_db$iso))){
          
          country_db<- cov_db[cov_db$iso==i_iso,]
          
          
          for ( i in 1:37){
            
            new_value<-c(new_value, rnorm(1,mean=country_db$gp_value[i],sd=sqrt(country_db$mse[i]))) 
            
            
          }
          
          
        }
        
        list_value[[length(list_value)+1]]<-new_value
        
      }
      
      pred_model_data<-cbind(cov_db[,1:3],do.call(cbind,list_value))
      
      
      names(pred_model_data)<-c('Country','iso','Year',column_names)
      
      pred_model_data<-pred_model_data[ order(pred_model_data$iso,pred_model_data$Year),   ]
      
      #head(pred_model_data)
      
      x_pred<-as.data.frame(predict(chosen_model_hmis2,pred_model_data,se.fit=T))
      
      pred_seek_treat<-numeric()
      
      for(i_nor in 1: dim(x_pred)[1]){
        
        pred_seek_treat<-c(pred_seek_treat,rnorm(1,mean=x_pred[i_nor,1],sd=x_pred[i_nor,2]))
        
        
      }
      
      
      return(pred_seek_treat)
      
      
    }
    
    cov_db<-read.csv(file_name[1],sep=';',stringsAsFactors=F)
    
    cov_db<-cov_db[cov_db$iso %in% extra_countries,]
    
    cov_db<-cov_db[order(cov_db$iso,cov_db$year),]
    
    pred_model_data<-cbind(cov_db[,2:3],do.call(cbind,pred_list2))
    

    
    
  }
  
  
  #### UNION #####
  
  
  union_data_set<-rbind(pred_algo_data, pred_model_data)
  
  #######################
  #######################
  #######################
  
  union_list[[length(union_list)+1]]<-union_data_set
}else{
  
  
  union_list[[length(union_list)+1]]<-pred_algo_data
  
}
  
  
}


multi_list<-list()

for (i_u in 1:length(union_list)){
  
  x<-union_list[[i_u]]
  
  multi_list[[length(multi_list)+1]]<-x[,-c(1,2)]
  
}

multi_db<-do.call(cbind,multi_list)


sim_mean<-runmean(apply(multi_db,1,mean),1) 

sim_sd<-apply(multi_db,1,sd)

sim_ui<-runmean(sim_mean+1.96*sim_sd,1)

sim_li<-runmean(sim_mean-1.96*sim_sd,1)  


sim_mean[which(sim_mean<0)]<-0

sim_mean[which(sim_mean>0.98)]<-0.98

sim_ui[which(sim_ui<0)]<-0

sim_ui[which(sim_ui>1)]<-1

sim_li[which(sim_li<0)]<-0

sim_li[which(sim_li>1)]<-1


x<-union_list[[i_u]]

treat_seek_any<-data.frame(iso=x[,1],year=x[,2],mean=sim_mean,upper=sim_ui,lower=sim_li)

write.csv(treat_seek_any,'FILEPATH',quote=F,row.names=F)


par(mfrow=c(3,2))

for( i in unique(treat_seek_any$iso) ){
  
  
  country_db<-treat_seek_any[ treat_seek_any$iso==i, ]
  
  country_db<-treat_seek_any[ treat_seek_any$iso==i, ]
  
  country_db$mean[which(country_db$mean>0.9999)]<-0.98
  
  country_db$mean[which(country_db$mean<0.1)]<-0.1
  
  country_db$upper[which(country_db$upper>0.9999)]<-0.98
  
  country_db$upper[which(country_db$upper<0.1)]<-0.1
  
  country_db$lower[which(country_db$lower>0.9999)]<-0.98
  
  country_db$lower[which(country_db$lower<0.1)]<-0.1
  
  
  plot(0,0,type='n',ylim=c(0,1),xlim=c(1980,2016),main=i,ylab='% Sougth Treatment',xlab='Year')
  
  plotCI(1980:2016,country_db$mean,ui=country_db$upper,li=country_db$lower,add=T)
  
  in_out<-which(national_country$map_state==i)
  
  if(length(in_out)>0){
    
    
   country_line<- national_country[which(national_country$map_state==i),]
    
    points(country_line$year,country_line$any_ts,pch=21,bg=2)
    
  }
  
  
}




library(doMC)


reclassify = function(data, inCategories, outCategories)
{
  outCategories[ match(data, inCategories)]
}



# Deaths


setwd('FILEPATH')




load('FILEPATH')

cfr_betas<-read.csv('FILEPATH')

pop_gbd<-read.csv('FILEPATH')



load('FILEPATH')

#API_all_pf_pv 

load('FILEPATH')

#GBD_2016_results_codcorrect

load('FILEPATH')

registerDoMC(20)

GBD_2016_results_codcorrect_pv_pf<-GBD_2016_results_codcorrect[GBD_2016_results_codcorrect$location_id %in% unique(API_all_pf_pv$location_id),]

SSA_location_id<- location_metadata$location_id[which(location_metadata$super_region_name=="Sub-Saharan Africa" )]

SSA_location_id<-SSA_location_id[which(SSA_location_id %in% API_all_pf_pv$location_id)]

percentile_10<-c(195,198,178,181,193,179,180,214,217,189)

location_id<-SSA_location_id




for (i_loc  in 1: length(location_id)){
  

  print(i_loc)
  
  country_incidence<-API_all_pf_pv[API_all_pf_pv$location_id==location_id[i_loc],]
  
  country_deaths<-GBD_2016_results_codcorrect_pv_pf[GBD_2016_results_codcorrect_pv_pf$location_id==location_id[i_loc],]
  
  country_deaths$lyas<-paste(country_deaths$location_id,country_deaths$year_id,country_deaths$age_group_id,country_deaths$sex_id,sep='_')
  
  country_env<-new_envelope[ new_envelope$location_id==location_id[i_loc] & new_envelope$year_id >1979, ]
  
  country_env$lyas<-paste(country_env$location_id,country_env$year_id,country_env$age_group_id,country_env$sex_id,sep='_')
  
  country_pop<-pop_gbd [pop_gbd$location_id==location_id[i_loc] ,]
  
  country_pop$lyas<-paste(country_pop$location_id,country_pop$year_id,country_pop$age_group_id,country_pop$sex_id,sep='_')
  
  split_db<-merge(country_deaths[,c(2,8,11,3,4)],country_env[,c(5,9)],by='lyas')
  
  split_db<-merge(split_db, country_pop[,c(5,7)], by='lyas')
  
  names(split_db)<-c('lyas','year_id','deaths','sex_id','age_group_id','envelop','population')
  
  split_db<-split_db[order(split_db$age_group_id,split_db$sex_id),]
  
  split_db$age_bin<-reclassify(split_db$age_group_id, sort(unique(split_db$age_group_id)),c(rep('infants',4),rep('children',2),rep('adults',17)))
  
  split_db$beta_age<-reclassify(split_db$age_bin,unique(split_db$age_bin),c(0,-0.167772, -0.808675))
  
  split_db$beta_log_deaths<-log(split_db$envelop/split_db$population) *0.622771
  
  split_db$pseudo_cfr<-exp( split_db$beta_log_deaths+split_db$beta_age)/(1+exp(split_db$beta_log_deaths+split_db$beta_age))
  
  split_db$pseudo_cases<-split_db$deaths/ split_db$pseudo_cfr
  
  split_db$ay<-paste(split_db$year_id,split_db$age_bin,sep='_')
  
  country_incidence$ay<-paste(country_incidence$year_id,country_incidence$age_bin,sep='_')
  
  
  
  MAP_draws<-country_incidence[match(split_db$ay, country_incidence$ay),1:1000]
  
  pseduo_cases<-tapply(split_db$pseudo_cases,split_db$ay,sum)
  
  split_db$tot_pseudo_cases<-pseduo_cases[match(split_db$ay,names(pseduo_cases))]
  
  split_db$fraction<- split_db$pseudo_cases/ split_db$tot_pseudo_cases
  
  pop_ay<-tapply(split_db$population,split_db$ay,sum)
  
  split_db$pop_tot<-pop_ay[match(split_db$ay,names(pop_ay))]
  
 
  
  MAP_draws_cases<-MAP_draws*matrix(split_db$pop_tot,nrow = 1702,ncol = 1000)
  
  
  
  MAP_draws_age_cases<-MAP_draws_cases*matrix(split_db$fraction,nrow = 1702,ncol = 1000)
  

  MAP_draws_age_rate<-MAP_draws_age_cases/matrix(split_db$population,nrow = 1702,ncol = 1000)
  

  
  MAP_draws_age_rate[is.na( MAP_draws_age_rate)]<-0
  
  MAP_draws_age_rate[ MAP_draws_age_rate<0]<-0
  
  MAP_draws_age_rate[ MAP_draws_age_rate==Inf]<-0
  
  duration = (runif(1702, min = 14, max = 28))/365 
  
  MAP_draws_age_rate_short<-MAP_draws_age_rate#*matrix(duration,nrow = 1702,ncol = 1000)
  
  country_all_age_rate_ready<-cbind( MAP_draws_age_rate_short,split_db$age_group_id,split_db$sex_id,split_db$year_id)
  
  country_all_age_rate_ready$location_id<-location_id[i_loc]
  
 names(country_all_age_rate_ready)<-c(paste('draw',0:999,sep='_'),'age_group_id','sex_id','year_id','location_id')  

  
  write.csv(country_all_age_rate_ready,paste('FILEPATH',location_id[i_loc],'.csv',sep=''),row.names=F,quote=F)
  

  
  
}
  


#######################
#### Age pattern ####

#####################

age_pattern_all_list<-list()


location_id<-percentile_10

for (i_loc in 1: length(location_id)) {

  print(i_loc)
  
  country_incidence<-API_all_pf_pv[API_all_pf_pv$location_id==location_id[i_loc],]
  
  country_deaths<-GBD_2016_results_codcorrect_pv_pf[GBD_2016_results_codcorrect_pv_pf$location_id==location_id[i_loc],]
  
  country_deaths$lyas<-paste(country_deaths$location_id,country_deaths$year_id,country_deaths$age_group_id,country_deaths$sex_id,sep='_')
  
  country_env<-new_envelope[ new_envelope$location_id==location_id[i_loc] & new_envelope$year_id >1979, ]
  
  country_env$lyas<-paste(country_env$location_id,country_env$year_id,country_env$age_group_id,country_env$sex_id,sep='_')
  
  country_pop<-pop_gbd [pop_gbd$location_id==location_id[i_loc] ,]
  
  country_pop$lyas<-paste(country_pop$location_id,country_pop$year_id,country_pop$age_group_id,country_pop$sex_id,sep='_')
  
  split_db<-merge(country_deaths[,c(2,8,11,3,4)],country_env[,c(5,9)],by='lyas')
  
  split_db<-merge(split_db, country_pop[,c(5,7)], by='lyas')
  
  names(split_db)<-c('lyas','year_id','deaths','sex_id','age_group_id','envelop','population')
  
  split_db<-split_db[order(split_db$age_group_id,split_db$sex_id),]
  
  split_db$age_bin<-reclassify(split_db$age_group_id, sort(unique(split_db$age_group_id)),c(rep('infants',4),rep('children',2),rep('adults',17)))
  
  split_db$beta_age<-reclassify(split_db$age_bin,unique(split_db$age_bin),c(0,-0.167772, -0.808675))
  
  split_db$beta_log_deaths<-log(split_db$envelop/split_db$population) *0.622771
  
  split_db$pseudo_cfr<-exp( split_db$beta_log_deaths+split_db$beta_age)/(1+exp(split_db$beta_log_deaths+split_db$beta_age))
  
  split_db$pseudo_cases<-split_db$deaths/ split_db$pseudo_cfr
  
  split_db$ay<-paste(split_db$year_id,split_db$age_bin,sep='_')
  
  country_incidence$ay<-paste(country_incidence$year_id,country_incidence$age_bin,sep='_')
  

  
  pseduo_cases<-tapply(split_db$pseudo_cases,split_db$ay,sum)
  
  split_db$tot_pseudo_cases<-pseduo_cases[match(split_db$ay,names(pseduo_cases))]
  
  split_db$fraction<- split_db$pseudo_cases/ split_db$tot_pseudo_cases
  
 
  age_pattern_all_list[[length(age_pattern_all_list)+1]]<-split_db$fraction


}



country_all_db<-do.call(cbind,age_pattern_all_list)






###### Rest of WORLD #####

load('FILEPATH')


all_location_id<- epi_demographic$location_id[-c(which(epi_demographic$location_ids %in% SSA_location_id ))]

location_id<-all_location_id

location_id<-location_id[which(location_id %in% unique(API_all_pf_pv$location_id) )]

location_id<-location_id[which(location_id %in% new_envelope$location_id)]

location_done<-c(SSA_location_id,location_id)


for (i_loc  in 1: length(location_id)) {
  
  
  print(i_loc)
  
  country_incidence<-API_all_pf_pv[API_all_pf_pv$location_id==location_id[i_loc],]
  
  a<-country_incidence[1:20,1:500]
  
  length(which(rowSums(a-a[,1])==0))>0
  
  if(length(which(rowSums(a-a[,1])==0))>0){
    
    mat_fraction<-country_all_db[,sample(c(1:7),1000,replace = T)]
    
  }else{
    
    
    
    mat_fraction<-matrix(rowMeans(country_all_db,na.rm=T),nrow=1702,ncol=1000)
    
    
  }
  
  country_deaths<-GBD_2016_results_codcorrect_pv_pf[GBD_2016_results_codcorrect_pv_pf$location_id==location_id[i_loc],]
  
  country_deaths$lyas<-paste(country_deaths$location_id,country_deaths$year_id,country_deaths$age_group_id,country_deaths$sex_id,sep='_')
  
  country_env<-new_envelope[ new_envelope$location_id==location_id[i_loc] & new_envelope$year_id >1979, ]
  
  country_env$lyas<-paste(country_env$location_id,country_env$year_id,country_env$age_group_id,country_env$sex_id,sep='_')
  
  country_pop<-pop_gbd [pop_gbd$location_id==location_id[i_loc] ,]
  
  country_pop$lyas<-paste(country_pop$location_id,country_pop$year_id,country_pop$age_group_id,country_pop$sex_id,sep='_')
  
  split_db<-merge(country_deaths[,c(2,8,11,3,4)],country_env[,c(5,9)],by='lyas')
  
  split_db<-merge(split_db, country_pop[,c(5,7)], by='lyas')
  
  names(split_db)<-c('lyas','year_id','deaths','sex_id','age_group_id','envelop','population')
  
  split_db<-split_db[order(split_db$age_group_id,split_db$sex_id),]
  
  split_db$age_bin<-reclassify(split_db$age_group_id, sort(unique(split_db$age_group_id)),c(rep('infants',4),rep('children',2),rep('adults',17)))
  
  split_db$beta_age<-reclassify(split_db$age_bin,unique(split_db$age_bin),c(0,-0.167772, -0.808675))
  
  split_db$beta_log_deaths<-log(split_db$envelop/split_db$population) *0.622771
  
  split_db$pseudo_cfr<-exp( split_db$beta_log_deaths+split_db$beta_age)/(1+exp(split_db$beta_log_deaths+split_db$beta_age))
  
  split_db$pseudo_cases<-split_db$deaths/ split_db$pseudo_cfr
  
  split_db$ay<-paste(split_db$year_id,split_db$age_bin,sep='_')
  
  country_incidence$ay<-paste(country_incidence$year_id,country_incidence$age_bin,sep='_')
  
 
  
  MAP_draws<-country_incidence[match(split_db$ay, country_incidence$ay),1:1000]
  
  pseduo_cases<-tapply(split_db$pseudo_cases,split_db$ay,sum)
  
  split_db$tot_pseudo_cases<-pseduo_cases[match(split_db$ay,names(pseduo_cases))]
  
  split_db$fraction<- split_db$pseudo_cases/ split_db$tot_pseudo_cases
  
  pop_ay<-tapply(split_db$population,split_db$ay,sum)
  
  split_db$pop_tot<-pop_ay[match(split_db$ay,names(pop_ay))]
  
  
  
  MAP_draws_cases<-MAP_draws*matrix(split_db$pop_tot,nrow = 1702,ncol = 1000)
  
  
  
  MAP_draws_age_cases<-MAP_draws_cases*mat_fraction
  
 
  
  MAP_draws_age_rate<-MAP_draws_age_cases/matrix(split_db$population,nrow = 1702,ncol = 1000)
  
  
  
  MAP_draws_age_rate[is.na( MAP_draws_age_rate)]<-0
  
  MAP_draws_age_rate[ MAP_draws_age_rate<0]<-0
  
  MAP_draws_age_rate[ MAP_draws_age_rate==Inf]<-0
  
  duration = (runif(1702, min = 14, max = 28))/365 
  
  MAP_draws_age_rate_short<-MAP_draws_age_rate
  
  country_all_age_rate_ready<-cbind( MAP_draws_age_rate_short,split_db$age_group_id,split_db$sex_id,split_db$year_id)
  
  country_all_age_rate_ready$location_id<-location_id[i_loc]
 
names(country_all_age_rate_ready)<-c(paste('draw',0:999,sep='_'),'age_group_id','sex_id','year_id','location_id')
 
  
  
  
  write.csv(country_all_age_rate_ready,paste('FILEPATH',location_id[i_loc],'.csv',sep=''),row.names=F,quote=F)
  
  
}
  



##### NO malaria #####


country_all_age_rate_ready<-read.csv('short_term/5_491.csv')



no_malaria_location<-epi_demographic$location_ids[ -c(which(epi_demographic$location_ids %in% location_done))]

country_all_age_rate_ready[,grep('draw',names(country_all_age_rate_ready))]<-0

location_id<-no_malaria_location

for (i_loc in 1: length(location_id))  {
  
  
  
  country_all_age_rate_ready$location_id<-location_id[i_loc]
  


  write.csv(country_all_age_rate_ready,paste('FILEPATH',location_id[i_loc],'.csv',sep=''),row.names=F,quote=F)
  
}




reclassify = function(data, inCategories, outCategories)
{
  outCategories[ match(data, inCategories)]
}


ihme_code<-read.csv('FILEPATH')


# non-carto #

master.country.list<-as.vector(read.csv('FILEPATH')[,2])

data_location_id<-ihme_code$loc_id[match(master.country.list,ihme_code$ihme_lc_id)]

data_iso3<-as.character(ihme_code$ihme_lc_id[match(master.country.list,ihme_code$ihme_lc_id)])

# infants



load('FILEPATH')


API_timeseries_realisations_array_infants<-API_timeseries_realisations_array_infants[order(API_timeseries_realisations_array_infants$location_id),]

API_infants_list<-list()


for (i in unique(API_timeseries_realisations_array_infants$location_id)){
  
  country_incidence<-API_timeseries_realisations_array_infants[which(API_timeseries_realisations_array_infants$location_id==i),]
  
  year_draws<-t(country_incidence[,2:37])/1000
  
  year_draws<-rbind(year_draws,year_draws[36,])
  
  year_draws_db<-as.data.frame(t(apply(year_draws,1,as.numeric)))
  
  year_draws_db$year_id<-1980:2016
  
  year_draws_db$location_id<-i
  
  year_draws_db$age_bin<-'infants'
  
  API_infants_list[[length(API_infants_list)+1]]<-year_draws_db
  
}
  
API_infants_db<-do.call(rbind,API_infants_list)
  

# children


load('FILEPATH')

API_timeseries_realisations_array_children<-API_timeseries_realisations_array_children[order(API_timeseries_realisations_array_children$location_id),]

API_children_list<-list()


for (i in unique(API_timeseries_realisations_array_children$location_id)){
  
  country_incidence<-API_timeseries_realisations_array_children[which(API_timeseries_realisations_array_children$location_id==i),]
  
  year_draws<-t(country_incidence[,2:37])/1000
  
  year_draws<-rbind(year_draws,year_draws[36,])
  
  year_draws_db<-as.data.frame(t(apply(year_draws,1,as.numeric)))
  
  year_draws_db$year_id<-1980:2016
  
  year_draws_db$location_id<-i
  
  year_draws_db$age_bin<-'children'
  
  API_children_list[[length(API_children_list)+1]]<-year_draws_db
  
}

API_children_db<-do.call(rbind,API_children_list)



# adults


load('FILEPATH')

API_timeseries_realisations_array_adults<-API_timeseries_realisations_array_adults[order(API_timeseries_realisations_array_adults$location_id),]

API_adults_list<-list()


for (i in unique(API_timeseries_realisations_array_adults$location_id)){
  
  country_incidence<-API_timeseries_realisations_array_adults[which(API_timeseries_realisations_array_adults$location_id==i),]
  
  year_draws<-t(country_incidence[,2:37])/1000
  
  year_draws<-rbind(year_draws,year_draws[36,])
  
  year_draws_db<-as.data.frame(t(apply(year_draws,1,as.numeric)))
  
  year_draws_db$year_id<-1980:2016
  
  year_draws_db$location_id<-i
  
  year_draws_db$age_bin<-'adults'
  
  API_adults_list[[length(API_adults_list)+1]]<-year_draws_db
  
}

API_adults_db<-do.call(rbind,API_adults_list)


API_non_carto_db<-rbind(API_infants_db,API_children_db,API_adults_db)

names(API_non_carto_db)[1:1000]<-paste('draws',0:999,sep='_')

#############################################################################################################
#############################################################################################################
#############################################################################################################
#############################################################################################################


# Tim subnational #


# Arabia

incidence_arabia<-read.csv('FILEPATH')[,1:5]

# Brazil

incidence_brazil<-read.csv('FILEPATH')[,1:5]

# China

incidence_china<-read.csv('FILEPATH')[,1:5]

# India

incidence_india<-read.csv('FILEPATH')[,1:5]

incidence_india$Rate[which(incidence_india$Rate<0)]<-0

# Indonesia

incidence_indonesia<-read.csv('FILEPATH.csv')[,1:5]

# Mexico

incidence_mexico<-read.csv('FILEPATH')[,1:5]

# South Africa

incidence_southafrica<-read.csv('FILEPATH')[,1:5]


API_Tim_subnational<-rbind(incidence_arabia,
      incidence_brazil,
      incidence_china,
      incidence_india,
      incidence_indonesia,
      incidence_mexico,
      incidence_southafrica
      )

API_Tim_subnational$Age_Bin<-reclassify(API_Tim_subnational$Age_Bin,unique(API_Tim_subnational$Age_Bin),c('infants','children','adults'))



#############################################################################################################
#############################################################################################################
#############################################################################################################
#############################################################################################################



# carto #


incidence_carto<-read.csv('/home/zool1286/Desktop/MAP_data/GBD2016/Results/Dans_summarized_rasters_and_tables/africa/tables/CODEM_incidence_rate.csv')[,1:5]

incidence_carto_clean<-incidence_carto[-c(which(incidence_carto$Rate<0)),]

incidence_carto_clean<-incidence_carto_clean[-c(which(incidence_carto_clean$ID<0)),]

incidence_carto_subnational<-rbind(incidence_carto_clean,API_Tim_subnational)

country_non_carto<-unique(incidence_carto_subnational$ID)[which(unique(incidence_carto_subnational$ID) %in% data_location_id)]

incidence_carto_subnational<-droplevels(incidence_carto_subnational[-c(which(incidence_carto_subnational$ID %in% country_non_carto)),])



API_carto_subnational<-list()

for (i_loc in unique(incidence_carto_subnational$ID)){
  
  print(i_loc)
  
  country_incidence<-incidence_carto_subnational[incidence_carto_subnational$ID==i_loc,]
  
  array_draws<-tapply(country_incidence$Rate,list(country_incidence$Year,country_incidence$Realization,country_incidence$Age_Bin),mean)
  
  
  infants_draws<-array_draws[1:36,,3]
  
  for(i in 1:9){
    
    infants_draws<-  cbind(infants_draws,infants_draws[,1:100])
    
  }
  
  infants_draws_db<-as.data.frame(rbind(infants_draws,infants_draws[36,]))
  
  infants_draws_db$year_id<-1980:2016
  
  infants_draws_db$location_id<-i_loc
  
  infants_draws_db$age_bin<-'infants'
  
  
  children_draws<-array_draws[1:36,,2]  
  
  for(i in 1:9){
    
    children_draws<-  cbind(children_draws,children_draws[,1:100])
    
  }
  
  children_draws_db<-as.data.frame(rbind(children_draws,children_draws[36,]))
    
  children_draws_db$year_id<-1980:2016
  
  children_draws_db$location_id<-i_loc
  
  children_draws_db$age_bin<-'children'
  
  
  adults_draws<-array_draws[1:36,,1]
  
  for(i in 1:9){
    
    adults_draws<-  cbind(adults_draws,adults_draws[,1:100])
    
  }
  
  adults_draws_db<-as.data.frame(rbind(adults_draws,adults_draws[36,]))
  
  adults_draws_db$year_id<-1980:2016
  
  adults_draws_db$location_id<-i_loc
  
  adults_draws_db$age_bin<-'adults'
  
  
  all_draws_db<-rbind(infants_draws_db,children_draws_db,adults_draws_db)
  
  API_carto_subnational[[length(API_carto_subnational)+1]]<- all_draws_db
  
}

API_carto_subnational_db<-do.call(rbind,API_carto_subnational)

names(API_carto_subnational_db)[1:1000]<-paste('draws',0:999,sep='_')


API_all_location_year_age_bin<-rbind(API_non_carto_db,API_carto_subnational_db)

API_all_location_year_age_bin2<-API_all_location_year_age_bin


### Adjust PV ###

incidence_pv_pf<-read.csv('FILEPATH')

incidence_pv_pf$lya<-paste(incidence_pv_pf$location_id,incidence_pv_pf$year_id,incidence_pv_pf$age_group_id,sep='_')

incidence_pv_pf_mecs<-incidence_pv_pf[incidence_pv_pf$location_id %in% unique(API_all_location_year_age_bin$location_id),]

API_all_location_year_age_bin$lya<-paste(API_all_location_year_age_bin$location_id,API_all_location_year_age_bin$year_id,API_all_location_year_age_bin$age_bin,sep='_')

API_all_location_year_age_bin$new_prop_pv<-incidence_pv_pf_mecs$new_prop_pv[match(API_all_location_year_age_bin$lya,incidence_pv_pf_mecs$lya)]

API_all_location_year_age_bin$new_prop_pf<-incidence_pv_pf_mecs$new_prop_pf[match(API_all_location_year_age_bin$lya,incidence_pv_pf_mecs$lya)]

draws_api<-API_all_location_year_age_bin[,grep('draws',names(API_all_location_year_age_bin))]

draws_api[draws_api<0]<-0

API_all_location_year_age_bin$new_prop_pf[API_all_location_year_age_bin$new_prop_pf==0]<-1000000000000000000000000000000000

draws_api_pv<-draws_api/API_all_location_year_age_bin$new_prop_pf



API_all_location_year_age_bin[,grep('draws',names(API_all_location_year_age_bin))]<-draws_api_pv

country_national_id<- c(135,6,11,163,130,152,196,338)

API_all_location_year_age_bin<-API_all_location_year_age_bin[-c(which(API_all_location_year_age_bin$location_id %in% country_national_id)),]




## Add Pv back to the data ##

API_all_location_year_age_bin_pv<-API_all_location_year_age_bin[API_all_location_year_age_bin$new_prop_pv>=1,]



draws_pv<-list()

for (i in 1: dim(API_all_location_year_age_bin_pv)[1]){
  
  row_pv<-API_all_location_year_age_bin_pv[i,]
  
  rate_pv<-incidence_pv_pf$mean_cases_pv_age[match(row_pv$lya,incidence_pv_pf$lya)]/incidence_pv_pf$pop_tot[match(row_pv$lya,incidence_pv_pf$lya)]
  
  rate_pv[which(rate_pv==Inf)]<-0
  
  rate_pv[which(is.na(rate_pv))]<-0
  
  rate_pv[which(rate_pv<0)]<-0
  
  row_pv[,grep('draws',names(row_pv))]<-rate_pv
    
  draws_pv[[length(draws_pv)+1]]<- row_pv
  
}

draws_pv_list<-do.call(rbind,draws_pv)

API_all_location_year_age_bin_no_100_pv<-API_all_location_year_age_bin[API_all_location_year_age_bin$new_prop_pv<1,]


API_all_location_year_age_bin_pv_pf<-rbind(API_all_location_year_age_bin_no_100_pv,draws_pv_list)



# Country 100% Pv #


country_100_pv_iso3<-c('ARG','ARM','AZE','CRI','SLV','GEO','IRQ','KGZ','PRK','PRY','KOR','TUR','TKM','UZB')#'MEX'

country_100_pv_location_id<-ihme_code$loc_id[match(country_100_pv_iso3,ihme_code$ihme_lc_id)]

incidence_pv_pf_100_pv<-incidence_pv_pf[which(incidence_pv_pf$location_id %in% country_100_pv_location_id),]

API_100_pv<-API_all_location_year_age_bin_pv_pf[API_all_location_year_age_bin_pv_pf$location_id %in% country_100_pv_location_id,]

country_pv_lits<-list()

for (i in country_100_pv_location_id){

country_db<-API_100_pv[API_100_pv$location_id==i,]  

country_db<-country_db[order(country_db$age_bin,country_db$year_id),]
  
country_incidence<-incidence_pv_pf_100_pv[incidence_pv_pf_100_pv$location_id==i,]

country_incidence<-country_incidence[order(country_incidence$age_group_id,country_incidence$year_id),]

pv_rate<-as.numeric(country_incidence$mean_cases_pv_age/country_incidence$pop_tot)



pv_rate[which(pv_rate<0)]<-0

pv_rate[which(pv_rate==Inf)]<-0

pv_rate[which(is.na(pv_rate))]<-0

rep_pv_deaths_mat<-matrix(pv_rate,ncol=1000,nrow =length(pv_rate))

country_db[,grep('draws',names(country_db))]<-rep_pv_deaths_mat

country_pv_lits[[length(country_pv_lits)+1]]<-country_db

}


API_100_pv2<-do.call(rbind,country_pv_lits)

API_all_location_year_age_bin_no_100_pv<-API_all_location_year_age_bin_pv_pf[-c(which(API_all_location_year_age_bin_pv_pf$location_id %in% country_100_pv_location_id)),]

API_all_pf_pv<-rbind(API_all_location_year_age_bin_no_100_pv,API_100_pv2)

api_draws<-API_all_pf_pv[,grep('draws',names(API_all_pf_pv))]

api_draws[which(api_draws==Inf)]<-0

api_draws[which(is.na(api_draws))]<-0

api_draws[which(api_draws<0)]<-0

API_all_pf_pv[,grep('draws',names(API_all_pf_pv))]<-api_draws


# Save Draws #

save(API_all_pf_pv,file='FILEPATH')











