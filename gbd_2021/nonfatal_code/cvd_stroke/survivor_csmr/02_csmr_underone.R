#==============================================================================
#  02_csmr_underone script calculate CSMR input for First ever {subtype} stroke w CSMR models:
#  1. Get populations for all age groups, sexes, years, and locations 
#  2. Calculate global deaths due to {subtype} stroke using outpuf from stage 1
#  and Dismod-generated CSMR
#  3. Calculate {subtype} proportion within all stroke global deaths
#  4. Calculate {subtype} CoDCorrected death rates
#  5. Calculate CSMR from output of step 3 & 4
#==============================================================================
################# child script #################

rm(list = setdiff(ls(), c(lsf.str(), "jpath", "hpath", "os")))
#date<-gsub("-", "_", Sys.Date())
source('FILEPATH/get_ids.R')
source('FILEPATH/get_outputs.R')
source('FILEPATH/get_model_results.R')
source('FILEPATH/get_draws.R')
source('FILEPATH/get_location_metadata.R')
source('FILEPATH/get_age_metadata.R')
source('FILEPATH/get_demographics.R')
source('FILEPATH/get_population.R')

## Paths
if (Sys.info()[1] == "Linux") jpath <- "FILEPATH"
if (Sys.info()[1] == "Windows") jpath <- "FILEPATH"

## Libraries
library(openxlsx)
library(ggplot2)
'%ni%' <- Negate('%in%')

############ step 0: Initialize Parameters for processing ############
#Arguments for parallel process 
args <- commandArgs(trailingOnly = T)
parameters_filepath <- args[1]  #locations. 
print(parameters_filepath)
date <- args[2]
decomp_step <- args[3]

## Retrieving array task_id
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
print(task_id)

parameters <- fread(parameters_filepath) #for reading table data. 

location <- parameters[task_id, locs] 
print(paste0("Spliting CSMR for location ", location))

############ step 1: Get Populations for all age groups, sexes, years, and locations ############

print(paste0("Spliting CSMR for location ", location))
ages <- c(2,3, 6:20, 30, 31, 32, 235, 34, 238, 388, 389 , 28) #c(2:21, 30, 31, 32, 235, 34,238,388,389)
years <- c(seq(1990,2015,5),2019) #seq(1990,2019,1) #c(seq(1990,2015,5),2017)
sexes <- c(1,2)

####Load population file
pop <- readRDS(paste0("FILEPATH/pop_",date,".rds"))

############ step 2: Get CSMR from step 1 dismod acute & chronic produced in parent script  ###########

me_list=c(3952,1832,3953,1844,18730,18732)

## Pull in global CSMR step 1 acute and chronic draws from DisMod, produced in parent script

all_df_global <- readRDS(paste0("FILEPATH/all_df_global_",date,".rds"))

############ step 3: Calculate {subtype} proportion within all stroke global deaths  ###########

draw=1000 #only taking one-hundred draws to be compatible with faux correct results. 

## Aggregate under 1 - more granular ages cause model fit issues 

age_ids_under_one <- c(2,3,388,389) ## age groups to aggregate. 
all_df_global_under_one <- all_df_global[which(all_df_global$age_group_id %in% age_ids_under_one),] ## subset to these age groups

# merge on population (need to take out of rate space into death space in order to aggregate)
pop_global_under_one <- data.frame(get_population(age_group_id=c(2,3,388,389,28), location_id=1, year_id=years, sex_id=c(1,2),
                                                  status="best", decomp_step='iterative', gbd_round_id=7))

all_df_global_under_one <- merge(all_df_global_under_one,pop_global_under_one[,c('age_group_id','year_id','sex_id','population')], by = c('age_group_id','year_id','sex_id'))
all_df_global_under_one[,c(grep("draw_", names(all_df_global_under_one)))] <- lapply(all_df_global_under_one[,c(grep("draw_", names(all_df_global_under_one)))], function(x, y) x*y, y=all_df_global_under_one$population) #transforms deaths from rate to number space

col_names <- names(all_df_global_under_one)[grep('draw',names(all_df_global_under_one))]        ## get draw columns
all_df_global_under_one <- aggregate(all_df_global_under_one[,col_names],
                                     by=list(all_df_global_under_one$sex_id,all_df_global_under_one$modelable_entity_id,all_df_global_under_one$year_id,
                                             all_df_global_under_one$metric_id, all_df_global_under_one$measure_id, all_df_global_under_one$location_id,
                                             all_df_global_under_one$model_version_id),
                                     FUN = sum)

# setnames for later rbind. 
setnames(all_df_global_under_one, c('Group.1','Group.2','Group.3','Group.4','Group.5','Group.6','Group.7'),
         c('sex_id','modelable_entity_id','year_id','metric_id','measure_id','location_id','model_version_id'))

# set new age group id, using 28 as it is under 1. 
all_df_global_under_one$age_group_id <- 28 
all_df_global_under_one <- merge(all_df_global_under_one,pop_global_under_one[,c('age_group_id','year_id','sex_id','population','location_id')], by = c('age_group_id','year_id','sex_id','location_id'))

all_df_global_under_one[,c(grep("draw_", names(all_df_global_under_one)))] <- lapply(all_df_global_under_one[,c(grep("draw_", names(all_df_global_under_one)))], function(x, y) x/y, y=all_df_global_under_one$population) #transforms deaths from number to rate space
all_df_global_under_one$population  <- NULL 

## remove under 1 in previous df. 

all_df_global <- all_df_global[-which(all_df_global$age_group_id %in% age_ids_under_one),]

## add on newly calculated under 1 csmr. 

all_df_global <- rbind(all_df_global, all_df_global_under_one)

## Assign cause id based on me id 

all_df_global$cause_id <- NA
all_df_global$cause_id[which(all_df_global$modelable_entity_id== 3952 | all_df_global$modelable_entity_id==1832)]   <- 495 #assign ischemic strokes 
all_df_global$cause_id[which(all_df_global$modelable_entity_id== 3953 | all_df_global$modelable_entity_id==1844)]   <- 496 #assign intracerebral hemorrhages 
all_df_global$cause_id[which(all_df_global$modelable_entity_id== 18730 | all_df_global$modelable_entity_id==18732)] <- 497 #assign subarachnoid hemorrhages

## compute ratio of acute to chronic stroke for isch,ich,sah per year - per age group - per sex - 
all_df_props   <- expand.grid(unique(all_df_global$age_group_id),unique(all_df_global$location_id),unique(all_df_global$sex_id),unique(all_df_global$year_id), unique(all_df_global$cause_id))#data.frame(matrix(NA,nrow = nrow(all_df_global)/2, ncol=ncol(all_df_global)))
colnames(all_df_props)<-c('age_group_id','location_id','sex_id','year_id','cause_id')
draws          <- data.frame(matrix(NA,nrow = nrow(all_df_props), ncol=draw)) #empty frame for draws
all_df_props   <- cbind(all_df_props,draws)                                      #bind
colnames(all_df_props)[seq(6,ncol(all_df_props))]<- colnames(all_df_global)[grep('draw',colnames(all_df_global))] #rename. 

## Loop through and calculate proportions
for(i in 1:nrow(all_df_props)){

  #subset by sex, year, age, and cause from the GLOBAL dataset all_df_global (has global draws)

  temp <- data.frame(subset(all_df_global, sex_id==all_df_props$sex_id[i] & year_id==all_df_props$year_id[i] & age_group_id==all_df_props$age_group_id[i] & cause_id==all_df_props$cause_id[i]))

  #transfer ratio draws in the PROPS dataset (has props draws)

  if(1832 %in% temp$modelable_entity_id){ #if ischemic
    all_df_props[i,c(grep('draw',names(all_df_props)))] <- as.numeric(temp[which(temp$modelable_entity_id==3952),c(grep('draw',names(temp)))])/(as.numeric(temp[which(temp$modelable_entity_id==1832),c(grep('draw',names(temp)))])+as.numeric(temp[which(temp$modelable_entity_id==3952),c(grep('draw',names(temp)))]))
  }
  if(3953 %in% temp$modelable_entity_id){ #if intracerebral
    all_df_props[i,c(grep('draw',names(all_df_props)))] <- as.numeric(temp[which(temp$modelable_entity_id==3953),c(grep('draw',names(temp)))])/(as.numeric(temp[which(temp$modelable_entity_id==1844),c(grep('draw',names(temp)))])+as.numeric(temp[which(temp$modelable_entity_id==3953),c(grep('draw',names(temp)))]))
  }
  if(18730 %in% temp$modelable_entity_id){ #if intracerebral
    all_df_props[i,c(grep('draw',names(all_df_props)))] <- as.numeric(temp[which(temp$modelable_entity_id==18730),c(grep('draw',names(temp)))])/(as.numeric(temp[which(temp$modelable_entity_id==18730),c(grep('draw',names(temp)))])+as.numeric(temp[which(temp$modelable_entity_id==18732),c(grep('draw',names(temp)))]))
  }
}

all_df_props <- all_df_props[,-which(names(all_df_props)=='location_id')]
names(all_df_props) <- gsub("draw_", "prop.draw_", names(all_df_props))

############ step 4: Get CSMR for strokes for recent cod-correct run ###########
cause_ids <- unique(all_df_global$cause_id) #get cause ids

#GBD 2020 iteration: 
cod.draws <- data.frame(get_draws("cause_id", cause_ids, location_id=location, year_id=years, source="codcorrect",
                                  age_group_id=ages , measure_id=1, gbd_round_id = 7, decomp_step='step2')) 


## Aggregate less than age one codcorrect deaths. 

age_ids_under_one <- c(2,3,388,389) ## age groups to aggregate. 
cod.draws.under.one <- cod.draws[which(cod.draws$age_group_id %in% age_ids_under_one),] ## subset to these age groups
col_names <- names(cod.draws.under.one)[grep('draw',names(cod.draws.under.one))]        ## get draw columns

#aggregate!
cod.draws.under.one <- aggregate(cod.draws.under.one[,col_names],
                  by=list(cod.draws.under.one$sex_id,cod.draws.under.one$cause_id,cod.draws.under.one$year_id,
                          cod.draws.under.one$location_id,cod.draws.under.one$metric_id, cod.draws.under.one$measure_id),
                  FUN = sum)

#setnames for later rbind. 
setnames(cod.draws.under.one, c('Group.1','Group.2','Group.3','Group.4','Group.5','Group.6'),
                              c('sex_id','cause_id','year_id','location_id','metric_id','measure_id'))

#set new age group id, using 28 as it is under 1. 
cod.draws.under.one$age_group_id <- 28 

#remove age-specific under 1 data from cod.draws  
cod.draws<- cod.draws[-which(cod.draws$age_group_id %in% c(age_ids_under_one,28)),]
cod.draws <- rbind(cod.draws,cod.draws.under.one)

all_death_rates <- merge(cod.draws, pop, by=c("location_id", "year_id", "sex_id", "age_group_id"), all.x=T)
all_death_rates[,c(grep("draw_", names(all_death_rates)))] <- lapply(all_death_rates[,c(grep("draw_", names(all_death_rates)))], function(x, y) x/y, y=all_death_rates$population) #transforms deaths from number to rate space
names(all_death_rates) <- gsub("draw_", "corrected.draw_", names(all_death_rates))

############ Intermediate step: Save CoD Data ################

all_death_rates_save <- copy(all_death_rates)
all_death_rates_save$mean <- rowMeans(all_death_rates_save[,grep('corrected',names(all_death_rates_save))]) #take mean of the draws. 
all_death_bounds<- apply(all_death_rates_save[,grep('corrected',names(all_death_rates_save))], 1, function(x) quantile(x, probs=c(.025,.975),na.rm=T))
all_death_bounds<-data.frame(t(all_death_bounds))
all_death_rates_save$lower <- all_death_bounds$X2.5.
all_death_rates_save$upper <- all_death_bounds$X97.5.
all_death_rates_save <- all_death_rates_save[,c("cause_id","location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")]

############ step 5: Calculate final csmr for acute and chronic {subtype} stroke ###########

all_csmr <- merge(all_death_rates, all_df_props, by=c('age_group_id','sex_id','cause_id','year_id'))

chronic_rates <- data.frame(matrix(NA,nrow = nrow(all_csmr), ncol=draw)) #store corrected chronic csmr -
all_csmr <- cbind(all_csmr,chronic_rates)
colnames(all_csmr)[grep('X',colnames(all_csmr))] <- paste0('chronic',colnames(all_csmr)[grep('prop',names(all_csmr))])
names(all_csmr)<- gsub('chronicprop.draw_','chronic.csmr_', names(all_csmr))

acute_rates   <- data.frame(matrix(NA,nrow = nrow(all_csmr), ncol=draw))
all_csmr <- cbind(all_csmr,acute_rates)
colnames(all_csmr)[grep('X',colnames(all_csmr))] <- paste0('acute',colnames(all_csmr)[grep('prop',names(all_csmr))])
names(all_csmr)<- gsub('acuteprop.draw_','acute.csmr_', names(all_csmr))

## take all rates and multiply by all proportions according to following mathematical logic: 
# Chronic_csmr = Global_cod_correct_csmr*(1-prop)
# Acute_csmr   = Global_cod_correct_csmr*prop

for(i in 1:nrow(all_csmr)){

  all_csmr[i,grep('acute',names(all_csmr))]<- as.numeric(all_csmr[i,grep('corrected',names(all_csmr))]) * ((as.numeric(all_csmr[i,grep('prop',names(all_csmr))]))) 
  all_csmr[i,grep('chronic',names(all_csmr))]<- as.numeric(all_csmr[i,grep('corrected',names(all_csmr))])*(1-(as.numeric(all_csmr[i,grep('prop',names(all_csmr))])))
}    

final_acute_csmr <- all_csmr[,c(1,2,3,4,5,grep('acute',names(all_csmr)))]          #final acute csmr
final_chronic_csmr <- all_csmr[,c(1,2,3,4,5,grep('chronic',names(all_csmr)))]      #final chronic csmr 

#Append MEs to appropriate split cause 
final_acute_csmr$modelable_entity_id <- NA                                       #append NA column to add in MEs. 
final_acute_csmr$modelable_entity_id[which(final_acute_csmr$cause_id==495)] <- 3952   #acute stroke me for ischemic 
final_acute_csmr$modelable_entity_id[which(final_acute_csmr$cause_id==496)] <- 3953   #acute stroke me for ich
final_acute_csmr$modelable_entity_id[which(final_acute_csmr$cause_id==497)] <- 18730  #acute stroke me for sah 

final_chronic_csmr$modelable_entity_id <- NA                                       #append NA column to add in MEs. 
final_chronic_csmr$modelable_entity_id[which(final_chronic_csmr$cause_id==495)] <- 1832   #chronic stroke me for ischemic 
final_chronic_csmr$modelable_entity_id[which(final_chronic_csmr$cause_id==496)] <- 1844   #chronic stroke me for ich
final_chronic_csmr$modelable_entity_id[which(final_chronic_csmr$cause_id==497)] <- 18732  #chronic stroke me for sah 

#Take summary statisitcs of draws for upload 

final_acute_csmr$mean <- rowMeans(final_acute_csmr[,grep('acute',names(final_acute_csmr))]) #take mean of the draws. 
acute_bounds<- apply(final_acute_csmr[,grep('acute',names(final_acute_csmr))], 1, function(x) quantile(x, probs=c(.025,.975),na.rm=T))
acute_bounds<-data.frame(t(acute_bounds))
final_acute_csmr$lower <- acute_bounds$X2.5.
final_acute_csmr$upper <- acute_bounds$X97.5.

final_chronic_csmr$mean <- rowMeans(final_chronic_csmr[,grep('chronic',names(final_chronic_csmr))]) #take mean of the draws. 
chronic_bounds<- apply(final_chronic_csmr[,grep('chronic',names(final_chronic_csmr))], 1, function(x) quantile(x, probs=c(.025,.975),na.rm=T))
chronic_bounds<-data.frame(t(chronic_bounds))
final_chronic_csmr$lower <- chronic_bounds$X2.5.
final_chronic_csmr$upper <- chronic_bounds$X97.5.

#Now that summary statistics are obtained - remove draws. 

final_acute_csmr <- final_acute_csmr[,-grep('acute',names(final_acute_csmr))]
final_chronic_csmr <- final_chronic_csmr[,-grep('chronic',names(final_chronic_csmr))]

#subset into MEs for upload

final_acute_csmr_is  <- subset(final_acute_csmr, modelable_entity_id==3952)
final_acute_csmr_ich <- subset(final_acute_csmr, modelable_entity_id==3953)
final_acute_csmr_sah <- subset(final_acute_csmr, modelable_entity_id==18730)
  
final_chronic_csmr_is  <- subset(final_chronic_csmr, modelable_entity_id==1832)
final_chronic_csmr_ich <- subset(final_chronic_csmr, modelable_entity_id==1844)
final_chronic_csmr_sah <- subset(final_chronic_csmr, modelable_entity_id==18732)

folder <- "FILEPATH"
outdir <- paste0(folder, decomp_step, "/",date,"/")
dir.create(outdir, showWarnings = FALSE)
saveRDS(final_acute_csmr_is[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "is_acute_", location, ".rds"))
saveRDS(final_acute_csmr_ich[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "ich_acute_", location, ".rds"))
saveRDS(final_acute_csmr_sah[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "sah_acute_", location, ".rds"))

saveRDS(final_chronic_csmr_is[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "is_chronic_", location, ".rds"))
saveRDS(final_chronic_csmr_ich[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "ich_chronic_", location, ".rds"))
saveRDS(final_chronic_csmr_sah[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "sah_chronic_", location, ".rds"))

## Save CSMR (whole) for each cause. 

saveRDS(all_death_rates_save, file=paste0(outdir, "cod_csmr", location, ".rds"))

print('CSMR process complete')
