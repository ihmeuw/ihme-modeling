
#==============================================================================
#  02_csmr script calculate CSMR input for First ever {subtype} stroke w CSMR models:
#  1. Get populations for all age groups, sexes, years, and locations 
#  2. Calculate global deaths due to {subtype} stroke using outpuf from step 1
#  and Dismod-generated CSMR
#  3. Calculate {subtype} proportion within all stroke global deaths
#  4. Calculate {subtype} CoDCorrected death rates
#  5. Calculate CSMR from output of step 3 & 4
#==============================================================================

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
source('FILEPATH/interpolate.R')
library(data.table)

if (Sys.info()[1] == "Linux") jpath <- "FILEPATH"
if (Sys.info()[1] == "Windows") jpath <- "FILEPATH"

library(openxlsx)
library(ggplot2)
'%ni%' <- Negate('%in%')

############ step 0: Initialize Parameters for processing ############

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

ages <- c(2:21, 30, 31, 32, 235)
years <- seq(1990,2017,1)
sexes <- c(1,2)
#location <- 101

####Load population file
#Population
pop <- data.frame(get_population(age_group_id=ages, location_id=location, year_id=years, sex_id=c(1,2),
                                status="best", decomp_step=decomp_step))

############ step 2: Get CSMR from step 1 dismod  ###########

me_list=c(3952,1832,3953,1844,18730,18732)

############ step 3: Calculate {subtype} proportion within all stroke global deaths  ###########
all_df_global = data.frame(get_draws('modelable_entity_id', gbd_id=me_list, 'epi', location_id=1, year_id=years, sex_id=sexes, gbd_round_id=6, decomp_step='step1', measure_id = 15 , age_group_id = ages,downsample=T, n_draws=draw)) 

all_df_global$cause_id <- NA
all_df_global$cause_id[which(all_df_global$modelable_entity_id== 3952 | all_df_global$modelable_entity_id==1832)]   <- 495 #assign ischemic strokes 
all_df_global$cause_id[which(all_df_global$modelable_entity_id== 3953 | all_df_global$modelable_entity_id==1844)]   <- 496 #assign intracerebral strokes 
all_df_global$cause_id[which(all_df_global$modelable_entity_id== 18730 | all_df_global$modelable_entity_id==18732)] <- 497 #assign subarachnoid strokes 


all_df_global$ratio <- NA #empty ratio of acute/chronic stroke. 
#compute ratio of acute to chronic stroke for isch,ich,sah per year - per age group - per sex - 

all_df_props   <- expand.grid(unique(all_df_global$age_group_id),unique(all_df_global$location_id),unique(all_df_global$sex_id),unique(all_df_global$year_id), unique(all_df_global$cause_id))#data.frame(matrix(NA,nrow = nrow(all_df_global)/2, ncol=ncol(all_df_global)))
colnames(all_df_props)<-c('age_group_id','location_id','sex_id','year_id','cause_id')
draws          <- data.frame(matrix(NA,nrow = nrow(all_df_props), ncol=draw)) #empty frame for draws
all_df_props   <- cbind(all_df_props,draws)                                      #bind
print(colnames(all_df_props))
colnames(all_df_props)[seq(6,ncol(all_df_props))]<- colnames(all_df_global)[grep('draw',colnames(all_df_global))] #rename. 
 
for(i in 1:nrow(all_df_props)){
  if(i %% 100 ==0){
    print(paste0('iter ', i))
  }
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
 
############ step 4: Get CSMR for strokes for recent COD-correct run ###########
cause_ids <- unique(all_df_global$cause_id) #get cause ids
cod.draws <- data.frame(get_draws("cause_id", cause_ids, location_id=location, year_id=years, source="codcorrect",
                                  age_group_id=ages, version_id = 101, measure_id=1, gbd_round_id = 6, decomp_step='step4')) #all deaths from faux correct


all_death_rates <- merge(cod.draws, pop, by=c("location_id", "year_id", "sex_id", "age_group_id"), all.x=T)
all_death_rates[,c(grep("draw_", names(all_death_rates)))] <- lapply(all_death_rates[,c(grep("draw_", names(all_death_rates)))], function(x, y) x/y, y=all_death_rates$population) #transforms deaths from number to rate space
names(all_death_rates) <- gsub("draw_", "corrected.draw_", names(all_death_rates))

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

#take all rates and multiply by all proportions according to following mathematical logic: 
#Chronic_csmr = Global_cod_correct_csmr*(1-prop)
#Acute_csmr   = Global_cod_correct_csmr*prop

all_csmr <- data.table(all_csmr)


all_csmr[,c(grep("acute", names(all_csmr)))] <- all_csmr[,c(grep("corrected", names(all_csmr))),with=F] * all_csmr[,c(grep("prop", names(all_csmr))),with=F]
all_csmr[,grep('chronic',names(all_csmr))]<- all_csmr[,grep('corrected',names(all_csmr)),with=F]*(1-all_csmr[,grep('prop',names(all_csmr)),with=F])

all_csmr <- data.frame(all_csmr)
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
#print('debug flag 4')

print('debug flag 8')
final_acute_csmr <- final_acute_csmr[,-grep('acute',names(final_acute_csmr))]
final_chronic_csmr <- final_chronic_csmr[,-grep('chronic',names(final_chronic_csmr))]

#subset into MEs for upload

print('debug flag 9')
final_acute_csmr_is  <- subset(final_acute_csmr, modelable_entity_id==3952)
final_acute_csmr_ich <- subset(final_acute_csmr, modelable_entity_id==3953)
final_acute_csmr_sah <- subset(final_acute_csmr, modelable_entity_id==18730)

final_chronic_csmr_is  <- subset(final_chronic_csmr, modelable_entity_id==1832)
final_chronic_csmr_ich <- subset(final_chronic_csmr, modelable_entity_id==1844)
final_chronic_csmr_sah <- subset(final_chronic_csmr, modelable_entity_id==18732)

folder <- "FILEPATH/" #storage folder
outdir <- paste0(folder)


saveRDS(final_acute_csmr_is[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "is_acute_", location, ".rds"))
saveRDS(final_acute_csmr_ich[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "ich_acute_", location, ".rds"))
saveRDS(final_acute_csmr_sah[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "sah_acute_", location, ".rds"))

saveRDS(final_chronic_csmr_is[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "is_chronic_", location, ".rds"))
saveRDS(final_chronic_csmr_ich[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "ich_chronic_", location, ".rds"))
saveRDS(final_chronic_csmr_sah[,c("location_id", "age_group_id", "sex_id", "year_id", "mean", "lower","upper")], file=paste0(outdir, "sah_chronic_", location, ".rds"))


