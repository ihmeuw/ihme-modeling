library('RSQLite')

library(raster)

library(akima)

library(automap)

reclassify = function(data, inCategories, outCategories)
{
  outCategories[ match(data, inCategories)]
}



drv <- dbDriver("SQLite")

con <- dbConnect(drv, "FILEPATH")

all_db<-dbGetQuery(con, "SELECT * FROM ihme_db") 

all_db$anti_all<-ifelse(rowSums(all_db[,c("anti_sp", "anti_chloroquine", "anti_act")],na.rm=T)>0,1,0)

anti_all<-ifelse(rowSums(all_db[,c("anti_sp", "anti_chloroquine", "anti_act")],na.rm=T)>0,1,0)  

all_db$cluster_code<-paste(all_db$survey_code_num,all_db$cluster_number,sep='_')

country_malaria<-read.csv('FILEPATH')


all_country_list<-unique(as.character(country_malaria$country))


#### Fevers treated per clusters ####

all_fevers<-all_db[which(all_db$fever==1),]



#####################################


repeated_cluster_db<-all_fevers[,c('survey_code_num','cluster_number','lon','lat','cluster_code','country','year')]

cluster_db<-repeated_cluster_db[which(duplicated(repeated_cluster_db)==F),]


rx<-raster('FILEPATH')

x1<-SpatialPoints(cbind(cluster_db$lon,cluster_db$lat))

cluster_db$pf_limits<-extract(rx,x1,method='simple',fun=max)

cluster_db$pf_limits[which (cluster_db$pf_limits==-9999)]<-NA

cluster_db$pfpr[ cluster_db$pf_limits==0]<-0

cluster_db<-cluster_db[-c(which(is.na(cluster_db$pf_limits))),]

x1<-SpatialPoints(cbind(cluster_db$lon,cluster_db$lat))

#### ENVIROMENTAL VARIABLES #####

##### other rasters ####

# Population

AfriPop<-raster('FILEPATH')

cluster_db$Afripop<-findInterval(extract(AfriPop,x1,method='simple',fun=max),c(seq(0,10000,500)))

 Night light

DMSP<-raster('FILEPATH')


cluster_db$DMSP<-findInterval(extract(DMSP,x1,method='simple',fun=max),c(seq(0,50,5)))

 Accessability

Accessibility<-raster('FILEPATH')

cluster_db$accessibility<-findInterval(extract(Accessibility,x1,method='simple',fun=max), c(seq(0,1000,100)))

# Landcover

land_1<-raster('FILEPATH')

land_2<-raster('FILEPATH')

land_3<-raster('FILEPATH')

land_4<-raster('FILEPATH')

land_5<-raster('FILEPATH')

perc_forest<-land_1+land_2+land_3+land_4+land_5



cluster_db$perc_forest<-findInterval(extract(perc_forest,x1,method='simple',fun=max)*100, c(seq(0,50,10)))


land_6<-raster('FILEPATH')

land_7<-raster('FILEPATH')

land_8<-raster('FILEPATH')

land_9<-raster('FILEPATH')

land_10<-raster('FILEPATH')

land_16<-raster('FILEPATH')

perc_open_grass<-land_6+land_7+land_8+land_9+land_10+land_16



cluster_db$perc_open_grass<-findInterval(extract(perc_open_grass,x1,method='simple',fun=max)*100, c(seq(0,50,10)))


land_12<-raster('FILEPATH')

land_14<-raster('FILEPATH')


perc_crop<-land_12+land_14


cluster_db$perc_crop<-findInterval(extract(perc_crop,x1,method='simple',fun=max)*100, c(seq(0,50,10)))


land_13<-raster('FILEPATH')

perc_urban<-land_13


cluster_db$perc_urban<-findInterval(extract(perc_urban ,x1,method='simple',fun=max)*100, c(seq(0,50,10)))

# Raster pixel #


raster_id<-AfriPop

raster_id[1:length(raster_id)]<-1:length(raster_id)

id_pixel<-extract(raster_id,x1,method='simple',fun=max)

cluster_db$id_pixel<-id_pixel

cluster_db$cluster_un<-1:length(cluster_db[,1])

head(cluster_db)




####################################
###### RASTER FOR PREDICTION #######
####################################



#####################
### limit malaria ###
#####################

limits_raster<-raster('FILEPATH')

limits_pts <- rasterToPoints(limits_raster, spatial=TRUE)

limits_pts2<-subset(limits_pts,limits_pts$Pf_limits>-1)

new_cluster_db<-data.frame(cluster_un=1:length(limits_pts2))

new_cluster_db$lon<-coordinates(limits_pts2)[,1]

new_cluster_db$lat<-coordinates(limits_pts2)[,2]

new_x1<-SpatialPoints(cbind(new_cluster_db$lon,new_cluster_db$lat))


#### new limits ###


rx<-raster('FILEPATH')

new_cluster_db$pf_limits<-extract(rx,new_x1,method='simple',fun=max)





##### other rasters ####



# Population

new_cluster_db$Afripop<-findInterval(extract(AfriPop,new_x1,method='simple',fun=max),c(seq(0,10000,500)))

# Night light

#new_cluster_db$DMSP<-findInterval(extract(DMSP,new_x1,method='simple',fun=max),c(seq(0,50,5)))

# Accessability

new_cluster_db$accessibility<-findInterval(extract(Accessibility,new_x1,method='simple',fun=max), c(seq(0,500,50)))

# Landcover

new_cluster_db$perc_forest<-findInterval(extract(perc_forest,new_x1,method='simple',fun=max)*100, c(seq(0,50,10)))


new_cluster_db$perc_open_grass<-findInterval(extract(perc_open_grass,new_x1,method='simple',fun=max)*100, c(seq(0,50,10)))


new_cluster_db$perc_crop<-findInterval(extract(perc_crop,new_x1,method='simple',fun=max)*100, c(seq(0,50,10)))

new_cluster_db$perc_urban<-findInterval(extract(perc_urban ,new_x1,method='simple',fun=max)*100, c(seq(0,50,10)))


new_cluster_db$pfpr<-findInterval(extract(pfpr ,new_x1,method='simple',fun=max)*100, c(seq(5,75,10)))

### Country ###

gaul_code_rastr<-raster('FILEPATH')

new_cluster_db$gaul_code<-extract(gaul_code_rastr ,new_x1,method='simple',fun=max)

gaul_db<-read.csv('FILEPATH')

new_cluster_db$country<-reclassify(new_cluster_db$gaul_code,gaul_db$Gaul_Code,gaul_db$country)

head(new_cluster_db)





survey_db_list <- read.csv("FILEPATH")[,c(1,3,5)]

survey_db_list<-merge(survey_db_list,gaul_db,by='country')

survey_db_list<-survey_db_list[survey_db_list$DHS_id %in% all_db$survey_code_num,]

survey_db_list<-survey_db_list[-c(which(survey_db_list$country=='Lesotho')),]

dhs_country<-unique(survey_db_list$country)



list_country_out<-as.numeric()

model_betas<-list()

start_y<-c(2000,2005,2011)

end_y<-c(2004,2010,2015)

for ( i_y in 1:lenght(strat_y)){

  survey_db_list2<-survey_db_list[survey_db_list$year>= start_y[i_y] & survey_db_list$year<= end_y[i_y],]
  
  dhs_country<-unique(survey_db_list2$country)

db_all_country<-list()  
  
  
  
for (c_i in 1:length (dhs_country)){

  
  #### POP ###
  
  AfriPop<-raster(paste('FILEPATH',year_ihme[s_i],'_00-05.tif',sep=''))
  
  country_new_cluster$Afripop<-findInterval(extract(AfriPop,country_new_x1,method='simple',fun=max),c(seq(0,10000,500)))
  
  Afripop_db<-effect_db[ effect_db$variable=='Afripop',]
  
  country_new_cluster$Afripop_effect<-reclassify(country_new_cluster$Afripop,Afripop_db[,2],Afripop_db[,3])
  
  
  
  
  pfpr2<-raster(paste('FILEPATH',year_ihme[s_i],'.',n_map,'.PR.tif',sep=''))
  
  
  country_new_cluster$pfpr<-findInterval(extract(pfpr2 ,country_new_x1,method='simple',fun=max)*100, c(seq(5,75,10)))
  
  pfpr_db<-effect_db[ effect_db$variable=='pfpr',]
  
  country_new_cluster$pfpr[ country_new_cluster$pf_limits==0]<-0
  
  country_new_cluster$mal_g_effect<-reclassify(country_new_cluster$pfpr,pfpr_db[,2],pfpr_db[,3])
  

  
  
}
#country_surveys<-survey_db_list2 [survey_db_list2$country==dhs_country[c_i], ]

country_surveys<-country_surveys[order(country_surveys$year),]

print (dhs_country[c_i])

country_fever_db<-all_fevers[all_fevers$survey_code_num %in% country_surveys$DHS_id,]

treated<-sum(c(as.numeric(table(country_fever_db$anti_all)[2]),1),na.rm=T)

n_survey=1

if(treated>100){

years_run<-start_y[i_y]:end_y[i_y]

when_change<-match(years_run,country_surveys$year)

when_change[which(is.na(when_change))]<-0



old_n_survey=0

anti_70_16<-list()




for (i in 1:length(years_run)){

#### Country Model ####

print (i)


year=years_run[i]


if (old_n_survey!=n_survey){
  
  old_n_survey<-old_n_survey+1


require(sp)
require(rgeos)
library(maptools)
library(R2BayesX)

#cluster_db2<-cluster_db[cluster_db$country=='Kenya',]

cluster_db3<-cluster_db[which(cluster_db$survey_code_num==country_surveys$DHS_id[n_survey]),]

country_x1<-SpatialPoints(cbind(cluster_db3$lon,cluster_db3$lat))

## PFPR

n_map<-sample(c(1:100),1)

#### PfPR #####

pfpr<-raster(paste('FILEPATH',country_surveys$year[n_survey],'.',n_map,'.PR.tif',sep=''))

cluster_db3$pfpr<-findInterval(extract(pfpr ,country_x1,method='simple',fun=max)*100, c(seq(5,75,10)))

#### pop ####
  
  AfriPop<-raster(paste('FILEPATH',year,'_00-05.tif',sep=''))
  
  cluster_db3$Afripop<-findInterval(extract(AfriPop,country_x1,method='simple',fun=max),c(seq(0,10000,500)))
  
  

d=cluster_db3

coordinates(d)=~lon+lat

proj4string(d)=CRS("+init=epsg:4326")

 # d <- spTransform( d, CRS("+init=epsg:4326") )   
  
buf = gBuffer(d,width=0.01,byid=T)

#plot(buf)


writeSpatialShape(buf,paste('FILEPATH',year,sep=''))

unloadNamespace('shapefiles')

bnd_fila<-shp2bnd(paste('FILEPATH',year,sep=''),regionnames = 'cluster_un')


surv_db<-all_fevers[which(all_fevers$survey_code_num==country_surveys$DHS_id[n_survey]),]

model_db<-merge(surv_db,cluster_db3[,c(5,9:15)],by='cluster_code')

 
  

i_map=n_map

n_folder<-round(runif(1)*10000000)

if(sum(model_db$anti_all)>100){  
  
raster_mod<-bayesx(anti_all~
                   
                   +sx(pfpr)
                  # +sx(perc_forest) 
                   +sx(Afripop)  
                   + sx(perc_open_grass) 
                   + sx(perc_crop)
                   + sx(accessibility)
                   
                   +sx(cluster_un, bs = "gs", map = bnd_fila)
                   
                   #+sx(cluster_number,bs='re')
                   
                   ,data=model_db,method='REML',maxit=10,predict=T,verbose=F, family='binomial',
                   
                   outfile=paste('FILEPATH',i_map,'_',n_folder,'/',sep=''))

# Model Effect #

# intercept

int_betas<-data.frame(variable='intercept',x=1,y=as.numeric(coef(raster_mod)[1]))




x<-raster_mod$effects$'sx(perc_open_grass)'[,1]

y<-raster_mod$effects$'sx(perc_open_grass)'[,2]

pred_x<-0:10

pred_y<-predict(lm(y~x+I(x^2)+I(x^3)),newdata=data.frame(x=pred_x))
  
perc_open_grass_betas<-data.frame(variable='perc_open_grass',x=pred_x,y=pred_y)  


# perc_crop_effect

x<-raster_mod$effects$'sx(perc_crop)'[,1]

y<-raster_mod$effects$'sx(perc_crop)'[,2]

pred_x<-0:10

pred_y<-predict(lm(y~x+I(x^2)+I(x^3)),newdata=data.frame(x=pred_x))

perc_crop_betas<-data.frame(variable='perc_crop',x=pred_x,y=pred_y) 


# accessibility_effect betas

x<-raster_mod$effects$'sx(accessibility)'[,1]

y<-raster_mod$effects$'sx(accessibility)'[,2]

pred_x<-0:30

pred_y<-predict(lm(y~x+I(x^2)+I(x^3)),newdata=data.frame(x=pred_x))

accessibility_betas<-data.frame(variable='accessability',x=pred_x,y=pred_y)  

# Afripop betas 

x<-raster_mod$effects$'sx(Afripop)'[,1]

y<-raster_mod$effects$'sx(Afripop)'[,2]

pred_x<-0:50

pred_y<-predict(lm(y~x+I(x^2)+I(x^3)),newdata=data.frame(x=pred_x))

Afripop_betas<-data.frame(variable='Afripop',x=pred_x,y=pred_y)  

# pfpr betas

x<-raster_mod$effects$'sx(pfpr)'[,1]

y<-raster_mod$effects$'sx(pfpr)'[,2]

pred_x<-0:8

pred_y<-predict(lm(y~x+I(x^2)+I(x^3)),newdata=data.frame(x=pred_x))

pfpr_betas<-data.frame(variable='pfpr',x=pred_x,y=pred_y) 

betas_db_model<-rbind(int_betas,Afripop_betas,perc_open_grass_betas,
                      #perc_forest_betas,
                      perc_crop_betas,accessibility_betas,pfpr_betas)


model_betas[[length(model_betas)+1]]<-betas_db_model


}
  

system(paste('rm -rf FILEPATH',i_map,'_',n_folder,'/',sep=''))

if (when_change[i]!=0){
  
  n_survey<-when_change[i]
  
}


}



}

togliere par  
  
##### create map Africa ####  

for(s_i in 1:46){
  
  
  year_ihme<-1970:2015
  
  for (m_i in 1: length(model_betas)){
  
  
  country_new_cluster<-new_cluster_db
  
  country_new_x1<-SpatialPoints(cbind(country_new_cluster$lon,country_new_cluster$lat))
  
  effect_db<-model_betas[[m_i]]
  
  int_db<-effect_db[ effect_db$variable=='intercept',]
  
  country_new_cluster$intercept_effect<-int_db[1,3]
  
  #perc_forest_db<-effect_db[ effect_db$variable=='perc_forest',]
  
  #country_new_cluster$perc_forest_effect<-reclassify(country_new_cluster$perc_forest,perc_forest_db[,2],perc_forest_db[,3])
  
  
  perc_open_grass_db<-effect_db[ effect_db$variable=='perc_open_grass',]
  
  country_new_cluster$perc_open_grass_effect<-reclassify(country_new_cluster$perc_open_grass,perc_open_grass_db[,2],perc_open_grass_db[,3])
  
  perc_crop_db<-effect_db[ effect_db$variable=='perc_crop',]
  
  country_new_cluster$perc_crop_effect<-reclassify(country_new_cluster$perc_crop,perc_crop_db[,2],perc_crop_db[,3])
  
  accessibility_db<-effect_db[ effect_db$variable=='accessability',]
  
  country_new_cluster$accessibility_effect<-reclassify(country_new_cluster$accessibility,accessibility_db[,2],accessibility_db[,3])
  
  #### POP ###
  
  AfriPop<-raster(paste('FILEPATH',year_ihme[s_i],'_00-05.tif',sep=''))
  
  country_new_cluster$Afripop<-findInterval(extract(AfriPop,country_new_x1,method='simple',fun=max),c(seq(0,10000,500)))
  
  Afripop_db<-effect_db[ effect_db$variable=='Afripop',]
  
  country_new_cluster$Afripop_effect<-reclassify(country_new_cluster$Afripop,Afripop_db[,2],Afripop_db[,3])
  
  
  
  
  pfpr2<-raster(paste('FILEPATH',year_ihme[s_i],'.',n_map,'.PR.tif',sep=''))
  

  country_new_cluster$pfpr<-findInterval(extract(pfpr2 ,country_new_x1,method='simple',fun=max)*100, c(seq(5,75,10)))

  pfpr_db<-effect_db[ effect_db$variable=='pfpr',]
  
  country_new_cluster$pfpr[ country_new_cluster$pf_limits==0]<-0
  
  country_new_cluster$mal_g_effect<-reclassify(country_new_cluster$pfpr,pfpr_db[,2],pfpr_db[,3])
  
  
  
  ##### Prediction ####
  
  betas_db<-country_new_cluster[,grep('effect',names(country_new_cluster))]
  
  country_new_cluster$first_pred<-round(exp(rowSums(betas_db))*100,1)
  
  country_new_cluster$first_pred[ country_new_cluster$pf_limits==0]<-0
  
  country_new_cluster$first_pred[ country_new_cluster$pf_limits==1]<-0
  
  
  country_new_cluster$first_pred[which(country_new_cluster$first_pred<0)]<-0
  
  country_new_cluster$first_pred[which(country_new_cluster$first_pred>100)]<-100
  
  
  
  #anti_70_16[[i]]<-(country_new_cluster)
  
  
  
  new_data_grid<-data.frame(lon=country_new_cluster$lon,
   lat=country_new_cluster$lat,
  z=country_new_cluster$first_pred)
  
  
  gridded(new_data_grid) =~ lon+lat
  
  
  plot(raster(new_data_grid),main=year)
  
  
  
  
  
  }
  
}  
  
  
  
  
}
