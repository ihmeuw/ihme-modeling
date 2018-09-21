#


library(readstata13)

library(rworldmap)

library(raster)



library(mgcv)

library(MuMIn)

library(lme4)

library(R2BayesX)

library(maptools)

library(colorRamps)

library(readstata13)

library(caTools)

library(Kmisc)

reclassify = function(data, inCategories, outCategories)
{
  outCategories[ match(data, inCategories)]
}

setwd('FILEPATH')

admin_africa<-readShapePoly('FILEPATH')

cfr<-read.dta13('FILEPATH',generate.factors=T )

covariates_pred<-read.dta13('FILEPATH',generate.factors=T )

covariates_pred_selected<-covariates_pred[,c('location_id','location_name','year_id','age_group','sex_id','age_bin',"mort_rate", "log_mort_rate")]


cfr_clean<-cfr[,-c(grep('map_untreated',names(cfr)))]

newmap <- getMap(resolution = "low")

plot(newmap,lwd=0.5)

points(cfr_clean$longitude1, cfr_clean$latitude1, bg=2,pch=21)


# Night light

DMSP<-raster('FILEPATH')


cfr_clean$DMSP<-raster::extract(DMSP,cbind(cfr_clean$longitude1, cfr_clean$latitude1),method='simple',fun=max)

cfr_clean$DMSP[cfr_clean$DMSP>25]<-25

# Accessability

Accessibility<-raster('FILEPATH')

Accessibility <- focal(Accessibility, w=matrix(1/25,nrow=5,ncol=5))

cfr_clean$accessibility<-raster::extract(Accessibility,cbind(cfr_clean$longitude1, cfr_clean$latitude1),method='simple',fun=max)

cfr_clean$accessibility[which(cfr_clean$accessibility<1)]<-1


# Landcover

land_1<-raster('FILEPATH')

land_2<-raster('FILEPATH')

land_3<-raster('FILEPATH')

land_4<-raster('FILEPATH')

land_5<-raster('FILEPATH')

perc_forest<-land_1+land_2+land_3+land_4+land_5


cfr_clean$perc_forest<-raster::extract(perc_forest,cbind(cfr_clean$longitude1, cfr_clean$latitude1),method='simple',fun=max)


land_6<-raster('FILEPATH')

land_7<-raster('FILEPATH')

land_8<-raster('FILEPATH')

land_9<-raster('FILEPATH')

land_10<-raster('FILEPATH')

land_16<-raster('FILEPATH')

perc_open_grass<-land_6+land_7+land_8+land_9+land_10+land_16



cfr_clean$perc_open_grass<-raster::extract(perc_open_grass,cbind(cfr_clean$longitude1, cfr_clean$latitude1),method='simple',fun=max)


land_12<-raster('FILEPATH')

land_14<-raster('FILEPATH')


perc_crop<-land_12+land_14




cfr_clean$perc_crop<-raster::extract(perc_crop,cbind(cfr_clean$longitude1, cfr_clean$latitude1),method='simple',fun=max)


land_13<-raster('FILEPATH')

perc_urban<-land_13


cfr_clean$perc_urban<-raster::extract(perc_urban ,cbind(cfr_clean$longitude1, cfr_clean$latitude1),method='simple',fun=max)


cfr_clean$cod_age<-reclassify(cfr_clean$age_bin,unique(cfr_clean$age_bin),c('0-to-5','5-to-15','15-plus'))

 unt_inc<-numeric()
 
 for (i in 1: dim(cfr_clean)[1]){
   
   unt_inc_raster<-raster(paste('FILEPATH'
                                ,cfr_clean$year_id[i],'_',cfr_clean$cod_age[i],'FILEPATH',sep=''))
   
   
   unt_inc<-c(unt_inc,as.vector(unt_inc_raster))
   
   unt_inc<-c(unt_inc,raster::extract(unt_inc_raster,cbind(cfr_clean$longitude1[i], cfr_clean$latitude1[i]),method='simple',fun=max))
   
   
   
 } 
 
 cfr_clean$unt_inc<-unt_inc
 
 
 cfr_clean$unt_inc[cfr_clean$unt_inc>1]<-1
 
 act_year<-raster(paste('FILEPATH',1980,'.ACT.tif',sep=''))
 
 values( act_year)[values( act_year) < 10000] = 0
 
 pop_year<-numeric()
 
 for (i in 1: dim(cfr_clean)[1]){
   
   pop_inc_raster<-raster(paste('FILEPATH',cfr_clean$year_id[i],'_sum_5k_global.tif',sep=''))
   
   afri_pop<-pop_inc_raster+act_year
   
   pop_inc_raster<-raster(paste('FILEPATH'
                                ,cfr_clean$year_id[i],'_00-05.tif',sep=''))
   
   pop_year<-c( afri_pop,raster::extract(pop_inc_raster,cbind(cfr_clean$longitude1[i], cfr_clean$latitude1[i]),method='simple',fun=max))
   
   
 } 
 
 cfr_clean$map_pop<-pop_year
 
 
 
 cfr_clean$cfr_calculated<-((((cfr_clean$study_deaths)/(cfr_clean$sample_size))*cfr_clean$mean_env_hivdeleted)/cfr_clean$pop_scaled)/cfr_clean$unt_inc
 
 
 cfr_clean$cfr_calculated[which(cfr_clean$cfr_calculated=='Inf')]<-0
 
 cfr_clean<-cfr_clean[cfr_clean$cfr_calculated>0,]
 
 cfr_clean$age_bin<-factor(cfr_clean$age_bin,levels=c('infants','children','adults'))
 
 
 cfr_clean$log_cfr<- log(cfr_clean$cfr_calculated/(1-cfr_clean$cfr_calculated))
 
 model_cfr<-uGamm(log_cfr ~ s(DMSP) + s(accessibility) + s(perc_forest) + s(perc_open_grass) + s(perc_crop) +  s(perc_urban) + cod_age 
               ,data=cfr_clean,family='gaussian', random=list(location_id= ~1))
 
 

 
 
 model_selection<-dredge(model_cfr)
 
 

# 
 z_score<-(cfr_clean$sample_size - mean(cfr_clean$sample_size))/sd(cfr_clean$sample_size)
 
 cfr_clean$z_score<-z_score + (min(z_score)*-1)
 
 cfr_clean$weigth_point<-(cfr_clean$sample_size - mean(cfr_clean$sample_size))/sd(cfr_clean$sample_size)
 
 cfr_clean$all_death_rate<-cfr_clean$sample_size/cfr_clean$pop_scaled
 
 
 cfr_clean$log_mortality<-log(cfr_clean$mean_env_hivdeleted / cfr_clean$pop_scaled)
 
 cfr_clean$log_map_pop<-log(cfr_clean$pop_scaled)
 
 cfr_clean<-cfr_clean[cfr_clean$location_id!=181, ]
 
 cfr_clean_infants<-cfr_clean[cfr_clean$age_bin=='infants',]
 
 cfr_clean_children<-cfr_clean[cfr_clean$age_bin=='children',]
 
 cfr_clean_adults<-cfr_clean[cfr_clean$age_bin=='adults',]


 
 
 
 best_model_cfr<-bayesx(log_cfr ~ (DMSP)
#                        
#                        # +(perc_forest)  
#                        
#                        #  + sx(perc_crop) 
#                        
                        +(log_mortality)
                        
                        + (perc_open_grass)
#                        
#                        #  + (perc_urban)
#                        
                        + (accessibility)
#                        
#                        # +(log_map_pop)
#                        
                        +age_bin
                        
                        +sex_id
                        
                     # + sx( sex_id,age_bin, bs = "te")
                       
                         +sx(year_id)
#                        
#                        +sx(location_id,bs='re'), 
                        
                        outfile='FILEPATH',method='MCMC',data=cfr_clean,family='gaussian')
# 

# 
 draws_1000_betas<- read.table(paste(best_model_cfr$bayesx.prg$file.dir,'/bayesx.estim_FixedEffects1_sample.raw',sep=''), header=TRUE, quote="\"")
 

# 
# 
 model_pred<-exp(predict(best_model_cfr))/(1+exp(predict(best_model_cfr)))
 

# 
# 
# 
# 
# ######################
# ##### MODEL BETAS ####
# ######################
# 
# # Fixed effect
# 
 coef(best_model_cfr)
 
 beta_int<-coef(best_model_cfr)[1,1]
 
 beta_DMSP<-coef(best_model_cfr)[2,1]
 

 
 beta_log_mortality<-coef(best_model_cfr)[3,1]
# 
 beta_perc_open_grass<-coef(best_model_cfr)[4,1]
# 

# 
beta_accessibility<-coef(best_model_cfr)[5,1]
# 
 beta_infants<-0
# 
 beta_children<-coef(best_model_cfr)[6,1]
# 
 beta_adults<-coef(best_model_cfr)[7,1]
# 
beta_sex<-coef(best_model_cfr)[8,1]
# 


beta_perc_forest<-coef(best_model_cfr)[3,1]




beta_accessibility<-coef(best_model_cfr)[6,1]



beta_perc_open_grass<-coef(best_model_cfr)[5,1]

beta_perc_urban<-coef(best_model_cfr)[6,1]



beta_year<-coef(best_model_cfr)[9,1]




######################
##### PREDICTION #####
######################


limits_raster<-raster('FILEPATH')

limits_pts <- rasterToPoints(limits_raster, spatial=TRUE)

limits_pts2<-subset(limits_pts,limits_pts$Pf_limits>-1)

new_cluster_db<-data.frame(cluster_un=1:length(limits_pts2)+10000000)

new_cluster_db$lon<-coordinates(limits_pts2)[,1]

new_cluster_db$lat<-coordinates(limits_pts2)[,2]

new_x1<-SpatialPoints(cbind(new_cluster_db$lon,new_cluster_db$lat))


rx<-raster('FILEPATH')

new_cluster_db$pf_limits<-raster::extract(rx,new_x1,method='simple',fun=max)


##### other rasters ####

#
# Night light

new_cluster_db$DMSP<-raster::extract(DMSP,new_x1,method='simple',fun=max)

new_cluster_db$DMSP[new_cluster_db$DMSP>25]<-25

# Accessability

new_cluster_db$accessibility<-raster::extract(Accessibility,new_x1,method='simple',fun=max)

new_cluster_db$accessibility[new_cluster_db$accessibility<1]<-1

# Landcover

new_cluster_db$perc_forest<-raster::extract(perc_forest,new_x1,method='simple',fun=max)

new_cluster_db$perc_forest[new_cluster_db$perc_forest>0.5]<-0.5

new_cluster_db$perc_open_grass<-raster::extract(perc_open_grass,new_x1,method='simple',fun=max)



new_cluster_db$perc_crop<-raster::extract(perc_crop,new_x1,method='simple',fun=max)

new_cluster_db$perc_crop[new_cluster_db$perc_crop>0.5]<-0.5

new_cluster_db$perc_urban<-raster::extract(perc_urban ,new_x1,method='simple',fun=max)

new_cluster_db$perc_urban[new_cluster_db$perc_urban>0.5]<-0.5

# Incidence


# Population


#Envelope

load('FILEPATH')# object name env_new_MAP_age

covariates_pred_selected<-env_new_MAP_age

covariates_pred_selected$log_mort_rate<-log(covariates_pred_selected$mean)




### Country ###

id_code_rastr<-raster('FILEPATH')

new_cluster_db$gaul_code<-raster::extract(id_code_rastr ,new_x1,method='simple',fun=max)



### Limits ###

new_cluster_db$limits<-raster::extract(limits_raster ,new_x1,method='simple',fun=max)

new_cluster_db$limits[which(new_cluster_db$limits!=2)]<-NA

new_cluster_db$limits[which(new_cluster_db$limits==2)]<-1


#### under five ####

list_deaths_u5<-list()

list_unt_inc_u5<-list()

list_pop_u5<-list()

list_cfr_u5<-list()

year_db<-1980:2015

plot_map<-0


### 1000 draws

year_estimates<-numeric()

year_estimates_min<-numeric()

year_estimates_max<-numeric()



draws_1000_betas<- read.csv('FILEPATH')

coef_betas<-apply(draws_1000_betas[,-1],2,mean)



for (i in 1:length(year_db)){
  
  
  pop_inc_raster<-raster(paste('FILEPATH',year_db[i],'_sum_5k_global.tif',sep=''))
  
  act_year<-raster(paste('FILEPATH',year_db[i],'.ACT.tif',sep=''))
  
  values( act_year)[values( act_year) < 10000] = 0
  
  afri_pop<-pop_inc_raster+act_year
  
  new_cluster_db$pop<-raster::extract(afri_pop,cbind(new_cluster_db$lon, new_cluster_db$lat),method='simple',fun=max)
  
  act_year<-raster(paste('FILEPATH',year_db[i],'.ACT.tif',sep=''))
  
  act_year[act_year>1]<-1
  
  act_year<-1-act_year
  
  new_cluster_db$act<-raster::extract(act_year,cbind(new_cluster_db$lon, new_cluster_db$lat),method='simple',fun=max)
  
 file_list<-system('ls FILEPATH', intern=T)  



  list_1000_draws<-list()
  

  
  for (i_1 in 1:80){
    
    
    x1<-proc.time()
    
   
    incidence<-raster(paste('FILEPATH.',year_db[i],'.',i_1,'.inc.rate.adults.full.tif',sep=''))
    

 
    
    inc_act_pop<-incidence*act_year*afri_pop
   

    new_cluster_db$ inc_act_pop<-raster::extract( inc_act_pop,cbind(new_cluster_db$lon, new_cluster_db$lat))
    
       
    mortality_pred_db<-covariates_pred_selected[covariates_pred_selected$year_id==year_db[i] & covariates_pred_selected$age_bin=='adults' & covariates_pred_selected$sex_id==2,]
    
    
    new_cluster_db$log_mort_rate<-mortality_pred_db$log_mort_rate[match(new_cluster_db$gaul_code,mortality_pred_db$location_id)]
    
    
   
   for(i_beta in 1:1000){
   
   
    

    line_betas<-draws_1000_betas[sample(c(1:1000),1),-1]
    
    
    #mean
    
    beta_int<- as.numeric(line_betas[1])
    
    beta_DMSP<- as.numeric(line_betas[2])
    

    
    beta_log_mortality<- as.numeric(line_betas[3])
    
    beta_perc_open_grass<-as.numeric( line_betas[4])
    
 
    
    beta_accessibility<- as.numeric(line_betas[5])
    
    beta_infants<-0
    
    beta_children<- as.numeric(line_betas[6])
    
    beta_adults<- as.numeric(line_betas[7])
    
	beta_sex<-as.numeric(line_betas[8])
    
    beta_matrix<-cbind(beta_int,
                       
                       
                       beta_DMSP *new_cluster_db$DMSP,
                       
                     
                       
                       beta_log_mortality * new_cluster_db$log_mort_rate,
                       
                       beta_perc_open_grass * new_cluster_db$perc_open_grass,
                       
                       
                       
                       beta_accessibility * new_cluster_db$accessibility,
                       

                       
                        beta_sex,
                       
                       beta_adults
    )
    
    
    
    new_cluster_db$prediction_cfr<-rowSums(beta_matrix)
    
    
    
    new_cluster_db$cfr<-exp(new_cluster_db$prediction_cfr)/(1+exp(new_cluster_db$prediction_cfr))
    
   
    new_cluster_db$deaths<-new_cluster_db$inc_act_pop*new_cluster_db$cfr
    

   
   list_1000_draws[[length( list_1000_draws)+1]]<-tapply_(new_cluster_db$deaths,new_cluster_db$gaul_code,sum,na.rm=T)
   
   }
  
  }
  
  draws_db<-data.frame(cbind(names(tapply_(new_cluster_db$deaths,new_cluster_db$gaul_code,sum,na.rm=T)),2,year_db[i],round(do.call(cbind,list_1000_draws),1)))
  
  names(draws_db)<-c('location_id','sex_id','year_id',paste('draws',1:(dim(draws_db)[2]-3),sep='_'))
  
  draws_db$age_group<-'adults'
  
  
  write.csv(draws_db,paste('FILEPATH',year_db[i],'.csv',sep=''),quote=F)
    
  
}
    



