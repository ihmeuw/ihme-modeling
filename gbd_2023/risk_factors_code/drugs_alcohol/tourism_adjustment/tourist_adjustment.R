# 01_custom_stage_1_model_tourism ######################################
## Build custom model for total visitor stgpr model

## This script creates the custom stage-1 models for the total visitor stgpr model.
# Required format for custom stage 1: square for the locations, years, ages, and sexes being modeled. 
# Required columns are location_id, year_id, age_group_id, sex_id, and cv_custom_stage_1

# The first (USED FOR GBD2023) mod explicitly ignores the impacts from covid. These impacts will 
# be implemented after stgpr modeling using the observed drops in visitors (see COV_impact_ratios at the bottom)
# The first model fills in tourists using a simple lmer model, then models visitors as a function of tourists and covariates.


#the model is squared by copying national ests to subnationals and then scaling based on population ratios

### SETUP #################
pacman::p_load(data.table,tidyverse,lme4,splines,gridExtra,mice,ggrepel)
invisible(sapply(list.files('FILEPATH', full.names = T), source))

locs <- get_location_metadata(location_set_id=22,release_id = 16)
#set up population scalars -  proportion of population of subnat compared to parent - used to scale mods
pops <- get_population(release_id = 16,age_group_id=22,sex_id=3,year_id=c(1980:2024),location_id = 'all',location_set_id = 22)
pops <- merge(pops,locs[,.(location_id,level,parent_id,ihme_loc_id)])

#pull out pop level 3
pops_use <- pops[level==3] 
pops_use[,use_loc_id := location_id] 
#pull out pops level 4
pops4 <- pops[level==4]
pops4[,use_loc_id := parent_id]
pops4[,orig_loc_id := location_id]
#combine, calc ratio
pops_scalar4 <- merge(pops_use,pops4[,.(orig_loc_id,use_loc_id,year_id,population)],by=c('use_loc_id','year_id'),all=T)
pops_scalar4[,scalar := population.y/population.x]
pops_scalar4 <- unique(pops_scalar4[,.(orig_loc_id,year_id,scalar)])
setnames(pops_scalar4,'orig_loc_id','location_id')

#repeast for ration 5/4
pops_use <- pops[level==4] 
pops_use[,use_loc_id := location_id]
pops5 <- pops[level==5]
pops5[,use_loc_id := parent_id]
pops5[,orig_loc_id := location_id]

pops_scalar5 <- merge(pops_use,pops5[,.(orig_loc_id,use_loc_id,year_id,population)],by=c('use_loc_id','year_id'),all=T)
pops_scalar5[,scalar := population.y/population.x]
pops_scalar5 <- unique(pops_scalar5[,.(orig_loc_id,year_id,scalar)])
setnames(pops_scalar5,'orig_loc_id','location_id')

scalars <- rbind(pops_scalar4,pops_scalar5)

rm(pops4,pops5,pops_use,pops_scalar4,pops_scalar5,pops)

#get covariates for stage one model & combine
sdi <- get_covariate_estimates(covariate_id=881,release_id = 16) %>% setnames(.,'mean_value','sdi')
haqi <- get_covariate_estimates(covariate_id=1099,release_id = 16) %>% setnames(.,'mean_value','haqi')
cig_pc <- get_covariate_estimates(covariate_id=14,release_id = 16) %>% setnames(.,'mean_value','cigarettes_pc')
all_covs <- merge(sdi,haqi,by=c('location_id','location_name','year_id','age_group_id','sex_id'),all=T)
all_covs <- merge(all_covs,cig_pc,by=c('location_id','location_name','year_id','age_group_id','sex_id'),all=T)
all_covs <- all_covs[,.(location_id,location_name,age_group_id,sex_id,year_id,sdi,haqi,cigarettes_pc)]

# setup done

### Format training data ##############################################
# read in 'training data.' It contains the total visitors data and the total tourists data, as a predictor for total visitors
train_data <- fread('FILEPATH')%>%unique()
train_data[,total := as.numeric(total)]
train_data[is.nan(total),total := NA ]
train_data[is.nan(sheet),sheet := NA ]

#cast wide, by sheet
train_data <- dcast.data.table(train_data[!(is.na(sheet))],location_name+location_id+year_id~sheet,value.var='total',fun.aggregate = mean)
train_data$V1 <- NULL
cols <- names(train_data)[!(names(train_data)%in%c('location_name','year_id','location_id'))]
train_data[,c(cols) := lapply(.SD, function(x) ifelse(is.nan(x), NA, x)), .SDcols = cols]
setkey(train_data,location_name)

#visitors formatting - fill in with visitors by country of res or by nationality
train_data[!(is.na(`121_cp`)),visitors := `121_cp`]
train_data[is.na(visitors) & !(is.na(`121`)),visitors := `121`]
train_data[is.na(visitors) & !(is.na(`122`)),visitors := `122`]

#tourists formatting - by country, by nationality, and also fill in by total stays in absence of all data
train_data[!(is.na(`112`)),tourists := `112`]
train_data[is.na(tourists) & !(is.na(`111`)),tourists := `111`]
train_data[is.na(tourists) & !(is.na(`1912`)),tourists := `1912`]
train_data[is.na(tourists) & !(is.na(`1911`)),tourists := `1911`]

#combine with other model variables
train_data <- train_data[,.(location_name,location_id,year_id,visitors,tourists)]
train_data <- merge(train_data,all_covs[location_id%in%locs[level==3]$location_id],by=intersect(names(train_data),names(all_covs)),all=T)
train_data <- train_data[year_id %in%c(1980:2024)]
train_data <- train_data[location_id %in% locs[level==3]$location_id]
train_data[,COVIDyrs := ifelse(year_id%in%c(2020:2022),1,0)]

train_data <- merge(train_data,locs[,.(location_id,super_region_name)],by='location_id')

### Impute tourists and predict (two versions of the) custom model ########################
#Option #1, drop anything that shows the covid changes. Fill tourist vals with lme4 model.
#Option #2, with covid explicitly modeled.

#Option 1
if(T){
  #NO COVID MODEL
  #model missing tourists, then model visitors
  train_data_filled <- train_data
  train_data_filled[year_id>2019,tourists:= NA] #drop evidence of 2020 drop
  train_data_filled[year_id>2019,visitors:= NA] #drop evidence of 2020 drop
  #model for tourists
  mod <- lme4::lmer(log(tourists) ~ log(sdi) +
                      log(cigarettes_pc) + 
                      log(haqi) + (1|location_id) + #random intercept on loc
                      ns(year_id, knots=c(1997,2010,2018)),
                    data=train_data_filled, na.action = na.omit)
  
  train_data_filled[,fill_tourists := exp(predict(mod, newdata=train_data_filled,allow.new.levels = TRUE))]
  
  #visitors
  mod2 <- lme4::lmer(log(visitors) ~ log(sdi) +
                       log(fill_tourists)+
                      log(cigarettes_pc) + 
                      log(haqi) + (1|location_id) + #random intercept on loc
                      ns(year_id, knots=c(1997,2018)),
                    data=train_data_filled, na.action = na.omit)
  
  train_data_filled[,mod := exp(predict(mod2, newdata=train_data_filled,allow.new.levels = TRUE))]
  
 
}

#Option2
if(F){
 ## YES COVID EXISTING MODEL  
  #Impute missing tourists, model visitors
  use_impute <- train_data
  use_impute$visitors <- NULL 
  md.pattern(use_impute)
  # Method argument can be adapted based on the nature of your variables (e.g., 'pmm' for predictive mean matching for continuous variables)
  imputed_data <- mice(use_impute, 
                       maxit=5, 
                       seed = 500,
                       method='pmm',
                       m=5)
  
  head(imputed_data$imp)
  
  # Completing the data by choosing one of the imputed datasets 
  completed_data <- data.table()
  for(i in 1:5){
    completed_temp <- as.data.table(complete(imputed_data, i))[,.(tourists)]
    setnames(completed_temp,'tourists',paste0('imputed_tourists',i))
    completed_data <- cbind(completed_data,completed_temp)
  }
  train_data_filled <- cbind(train_data,completed_data)
  names(train_data_filled)
  ggplot(train_data_filled[location_name=='Poland'])+
    geom_point(aes(x=year_id,y=tourists))+
    geom_line(aes(x=year_id,y=imputed_tourists1))+
    geom_line(aes(x=year_id,y=imputed_tourists2),color='red')+
    geom_line(aes(x=year_id,y=imputed_tourists3),color='blue')
  
  # smoothing imputed_tourists_1 using lag/lead
  train_data_filled[,yrlag := lag(imputed_tourists1,order_by = year_id),by=c('location_id')]
  train_data_filled[,yrlead := lead(imputed_tourists1,order_by = year_id),by=c('location_id')]
  train_data_filled[,lead1diff := yrlead - imputed_tourists1]
  train_data_filled[lead1diff < 400, imputed_tourists1 := yrlead]
  
  mod <- lme4::lmer(log(visitors) ~ log(sdi) + 
                      log(cigarettes_pc) + 
                      log(haqi) + 
                      log(imputed_tourists1) + 
                      COVIDyrs + 
                      (1|location_id), 
                    data=train_data_filled, na.action = na.omit)
  
    #predict using model
  train_data_filled$mod <- exp(predict(mod, newdata=train_data_filled,allow.new.levels = TRUE))
  
    
}

### Clean/format custom stage one
custom_stage_1 <- copy(train_data_filled)%>%setnames(.,'mod','cv_custom_stage_1')%>%.[,.(location_id,year_id,age_group_id,sex_id,cv_custom_stage_1)]%>%.[,cv_custom_stage_1:=cv_custom_stage_1/10000]
custom_stage_1 <- merge(locs[,.(location_name,super_region_name,location_id)],custom_stage_1)

custom_stage_1[,temp_parent := location_id]
template <- as.data.table(expand.grid(location_id=unique(locs[level>2]$location_id),year_id=1980:2024))
template <- merge(template,locs[,.(location_id,parent_id,level)])
custom_stage_1 <- merge(custom_stage_1,template,by=c('location_id','year_id'),all=T)

custom_stage_1[!(is.na(temp_parent)),parent_id:=temp_parent]
(custom_stage_1[location_id==6&year_id==1980])
custom_stage_1[,cv_temp := mean(cv_custom_stage_1,na.rm=T),by=.(parent_id,year_id)]

#merge on population scalars
custom_stage_1 <- merge(custom_stage_1,scalars,all=T)
custom_stage_1[is.na(cv_custom_stage_1),cv_custom_stage_1:=cv_temp*scalar]

#location level 5
custom_stage_1[,c('temp_parent','parent_id','cv_temp')] <- NULL
custom_stage_1 <- merge(custom_stage_1,locs[level>2,.(parent_id,location_id)],by='location_id',all=T)
custom_stage_1[level==4,parent_id := location_id]
custom_stage_1[level%in%c(4,5),cv_temp := mean(cv_custom_stage_1,na.rm=T),by=.(parent_id,year_id)]
custom_stage_1[level==5&(is.na(cv_custom_stage_1)|is.nan(cv_custom_stage_1)),cv_custom_stage_1:=cv_temp*scalar]

table(custom_stage_1[is.na(cv_custom_stage_1)]$level)
custom_stage_1 <- custom_stage_1[!(is.na(location_id))]

custom_stage_1$age_group_id <- 22
custom_stage_1$sex_id <- 3
custom_stage_1[!(complete.cases(custom_stage_1[,.(location_id,year_id,age_group_id,sex_id,cv_custom_stage_1)]))]

### Save ######################
fwrite(custom_stage_1[,.(location_id,year_id,age_group_id,sex_id,cv_custom_stage_1)],
       paste0('FILEPATH'))

train_data_sub <- train_data[year_id>=2017]
#format long by type
train_data_sub <- melt(train_data[year_id%in%c(2018:2022)],id.vars=c('location_id','location_name','super_region_name','year_id'),measure.vars=c('tourists','visitors'),variable.name='type',value.name = 'total')
#cast wide by year to calc ratio
train_data_wide <- dcast(train_data_sub,location_id+location_name+super_region_name+type~paste0('yr_',year_id),value.var='total')
#add loc_id
train_data_wide <- merge(train_data_wide,locs[,.(location_id,ihme_loc_id)],by='location_id')

# calculate ratios of visitors in 2019/2020, 2019/2021, and 2019/2022
train_data_wide[is.na(yr_2019)&!(is.na(yr_2018)), yr_2019:=yr_2018]
train_data_wide[,covyr1:=yr_2020/yr_2019]
train_data_wide[,covyr2:=yr_2021/yr_2019]
train_data_wide[,covyr3:=yr_2022/yr_2019]

#super region average
train_data_wide[,sr_covyr1:=mean(covyr1,na.rm=T),by=super_region_name]
train_data_wide[,sr_covyr2:=mean(covyr2,na.rm=T),by=super_region_name]
train_data_wide[,sr_covyr3:=mean(covyr3,na.rm=T),by=super_region_name]

# clean final ratios after visualizing
COV_impact_ratios <- dcast.data.table(train_data_wide[,.(location_name,super_region_name,type,covyr1,covyr2,covyr3,sr_covyr1,sr_covyr2,sr_covyr3)],location_name+super_region_name~type,value.var=c('covyr1','covyr2','covyr3','sr_covyr1','sr_covyr2','sr_covyr3'))
# select visitors ratio if present, if not then tourist ratios, then fill with SR average
COV_impact_ratios[is.na(covyr1_visitors),covyr1_visitors:=covyr1_tourists]
COV_impact_ratios[is.na(covyr2_visitors),covyr2_visitors:=covyr2_tourists]
COV_impact_ratios[is.na(covyr3_visitors),covyr3_visitors:=covyr3_tourists]
COV_impact_ratios[is.na(covyr1_visitors),covyr1_visitors:=sr_covyr1_visitors]
COV_impact_ratios[is.na(covyr2_visitors),covyr2_visitors:=sr_covyr2_visitors]
COV_impact_ratios[is.na(covyr3_visitors),covyr3_visitors:=sr_covyr3_visitors]
# save to be applied after stgpr visiors model
COV_impact_long <- COV_impact_ratios[,.(location_name,covyr1_visitors,covyr2_visitors,covyr3_visitors)]
setnames(COV_impact_long,c('covyr1_visitors','covyr2_visitors','covyr3_visitors'),c('2020','2021','2022'))
COV_impact_long <- melt(COV_impact_long,id.vars='location_name',variable.name = 'year_id',value.name = 'ratio')
fwrite(COV_impact_long, paste0('FILEPATH'))
