


user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILEPATH", user, "FILEPATH")
#install.packages("BayesianTools", lib=path)
#library(BayesianTools, lib.loc=path)
library(dplyr)
require(splines)
library(INLA)
library(ggplot2)

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

#pull in geographic restrictions and drop non-endemic countries and drop restricted locations


lf_geo<-read.csv(sprintf("FILEPATH"), stringsAsFactors = FALSE)
dataset<-lf_geo[,1:50]

#convert to long
melt_data<-melt(dataset, id.vars = c('location_id',
                                     'parent_id',
                                     'level',
                                     'type',
                                     'region_id',
                                     'ihme_loc_id'))

melt_data$value<-as.character(melt_data$value)

status_list<-subset(melt_data, type=='status')

presence_list<-subset(status_list,value=='p' |value=='pp')

#list of unique lf endemic locations
unique_lf_locations<-unique(presence_list$location_id)

#preliminary model of hydrocele v. lf infection prevalence
mod_data<-read.csv("FILEPATH")

#drop all age data
mod_data$age_diff<-mod_data$age_end-mod_data$age_start
mod_data<-mod_data[mod_data$age_diff<25,]

#keep only hydrocele means <100%
mod_data<-mod_data[mod_data$hydro_mean<1.0,]

#drop age groups with small sample size, drop mean hydrocele >30%, drop data over age 70
mod_data<-mod_data[mod_data$hydro_ss>=25,]
summary(mod_data$hydro_mean)
mod_data<-mod_data[mod_data$hydro_mean<.2,]
mod_data<-mod_data[mod_data$age_start<70,]

plot(mod_data$mean_adjusted,mod_data$hydro_mean)
plot(mod_data$age_start,mod_data$hydro_mean)

#drop outlier on lf prevalence, drop 0 prevalence -this will skew the results

summary(mod_data$mean_adjusted)
summary(mod_data$hydro_cases)

plot(mod_data$age_start,mod_data$hydro_mean)
plot(mod_data$mean_adjusted,mod_data$hydro_mean)

#impose age restrict for under 5s 
mod_data$hydro_mean<-ifelse(mod_data$age_start<=5,0,mod_data$hydro_mean)
mod_data$hydro_cases<-ifelse(mod_data$age_start<=5,0,mod_data$hydro_cases)

mod_data$hydro_mean<-ifelse(mod_data$mean_adjusted<=0.008,0,mod_data$hydro_mean)
mod_data$hydro_cases<-ifelse(mod_data$mean_adjusted<=0.008,0,mod_data$hydro_cases)
#drop high prev, zero cases

mod_data<-mod_data[!(mod_data$hydro_mean<0.01 & mod_data$mean_adjusted>.05), ]

#test out logistic regression
#outlier at locations where infection prev = 0

ggplot(mod_data, aes(x=mean_adjusted, y=hydro_mean, color=age_start)) +
  geom_point()

#drop row 1 from dataset (read-in error)
mod_data<-mod_data[-1,]

#add spline to mean_adjusted

mod1<-glm(hydro_mean~bs(mean_adjusted, knots=c(.05,.1)), weights=hydro_ss, family=binomial(link="logit"), data=mod_data)

#plot predictions
summary(mod1)
library(boot)
preds<-as.data.frame(inv.logit(predict(mod1)))

names(preds)[1]<-"prediction"

mod_data2<-mod_data[!is.na(mod_data$mean_adjusted),]


#append
mod_data3<-cbind(mod_data2,preds)


plot(mod_data3$age_start,mod_data3$prediction)
plot(mod_data3$hydro_mean,mod_data3$prediction)
#view by age
ggplot(mod_data3, aes(x=mean_adjusted, y=prediction, color=age_start)) +
  geom_point()
ggplot(mod_data3, aes(x=mean_adjusted, y=hydro_mean, color=age_start)) +
  geom_point()

###data processing to implement in INLA

#get age_group_ids 

age_info<-read.csv("FILEPATH")

#keep age_start, mean_adjusted, sex, lymph_mean

keeps <- c("age_start","mean_adjusted","sex","hydro_mean","location_id","year_start","hydro_ss","hydro_cases")
df = mod_data[keeps]

#rename year_start to year_id

df <- rename(df, year_id = year_start)
#create indicator to flag what data are used to fit the model (dataset=0); dataset=1 will be the 
#prevalence draws used for prediction
df$dataset<-0

#pull in age group ids
df2<-merge(age_info,df,by="age_start")


#recode sex to male =1 female =2 
df2$sex_id<-ifelse(df2$sex=="Male",1,2)

#drop unnecessary variables
df2$X<-NULL
df2$age_group_name<-NULL
df2$age_end<-NULL
#df2$age_mid<-NULL
df2$sex<-NULL



###----------calculate mean prevalence across the draws -------- #

#get draws 
ages<-unique(age_info$age_group_id)


#pull in prevalence estimates
lf_prev<-get_model_results(gbd_team='epi', gbd_id=10519, gbd_round_id=6,age_group_id=ages, sex_id=c(1,2), location_id=unique_lf_locations, year_id=c(1990,1995,2000, 2005, 2010,2015, 2017, 2019), decomp_step="iterative")

#convert to data table
prev2<-as.data.table(lf_prev)
prev2$mean_adjusted<-prev2$mean

#merge age_mid together
prev2_b<-merge(prev2,age_info,by="age_group_id")

lf_df3<-select(prev2_b, age_group_id, age_mid, year_id, sex_id, location_id, mean_adjusted, age_start)

lf_df3$hydro_mean<-NA
lf_df3$hydro_ss<-NA
lf_df3$hydro_cases<-NA
lf_df3$dataset<-1
#append model inputs

### append all together to generate model dataset with rows for prediction
mod_1b<-rbind(lf_df3,df2,fill=TRUE)

##############RUN MODEL ##############3

summary(mod_data$hydro_mean)
summary(df2$hydro_mean)
summary(mod_1b$hydro_mean)
summary(mod_1b$mean_adjusted)

mod_1b$hydro_ss<-ceiling(mod_1b$hydro_ss)
mod_1b$hydro_cases<-ceiling(mod_1b$hydro_cases)


plot(mod_1b$mean_adjusted,mod_1b$hydro_mean)
#keep <15%

mod_1b2<-mod_1b[mod_1b$hydro_mean<.15 | is.na(mod_1b$hydro_mean),]

knots=seq(.2,.4)
formula<-hydro_cases~bs(mean_adjusted,knots=knots)
result3 <- inla(formula, family="binomial", control.compute=list(config = TRUE),control.predictor=list(link=1), Ntrials=hydro_ss, data=mod_1b2, verbose=F)
summary(result3)

range<-nrow(mod_1b2)
results_simple<-result3$summary.fitted.values[1:range, ]

#append to input data

combined<-cbind(mod_1b2,results_simple)

#plot results of the model 
plot(combined$hydro_mean, combined$mean)
plot(combined$age_mid, combined$mean)
plot(combined$mean_adjusted, combined$mean)
plot(combined$year_id,combined$mean)
#look at lf-mean by age group
ggplot(combined, aes(x=mean_adjusted, y=mean, color=age_start)) +
  geom_point()

#check range of predictions by year - 
test90<-combined[combined$year_id==1990,]
summary(test90$mean)

test17<-combined[combined$year_id==2017,]
summary(test17$mean)


#-------------------generate 1000 draws from inla
results <- inla.posterior.sample(1000, result3,use.improved.mean=FALSE)
predictions <- lapply(results, function(p) p$latent[1:range])
predictions <- do.call(cbind, predictions)

predictions2<-as.data.frame(predictions)
#bind the first columns from mod_dat2
draws3<-predictions2 %>%
  dplyr::rename_all(
    ~stringr::str_replace_all(.,"V","draw_")
  )
#change draw_1000 to draw_0
names(draws3)[names(draws3) == "draw_1000"] <- "draw_0"

ilog <- function(x) ((exp(x)/(1+exp(x))))
draws4<-draws3 %>% mutate_all(ilog)
#keep first six columns of dataset 
columns<-select(mod_1b2, age_group_id, age_mid, year_id, sex_id, location_id, dataset, mean_adjusted)

draws<-cbind(columns,draws4)
#change variable names from V# to draw_0 etc.

#add to draws3 to view the results

#now just need to keep predictions from model, so subset out where dataset=1

draws<-draws[draws$dataset==1,]
#visualize draws as a test
ggplot(draws, aes(x=mean_adjusted, y=draw_0, color=age_group_id)) +
  geom_point()
#check year
ggplot(draws, aes(x=year_id, y=draw_0, color=age_group_id)) +
  geom_point()

summary(draws$draw_0)

ggplot(draws, aes(x=age_mid, y=draw_1)) +
  geom_point()

summary(draws$mean_adjusted)
plot(draws$mean_adjusted,draws$draw_0)

#drop mean_adjusted
#draws$mean_adjusted<-NULL
draws$dataset<-NULL

draw.cols <- paste0("draw_", 0:999)

draws2<-setDT(draws)
draws2[, id := .I]
draws2[age_group_id<6, (draw.cols) := 0, by=id]
#drop very low national prevalence to zero hydrocele
draws2[mean_adjusted<0.0005, (draw.cols):=0,by=id]
#hard code elimination values for togo, egypt, cambodia, thailand, sri lanka - achieved before 2017
draws2[location_id==141 & year_id>=2010, (draw.cols):=0, by=id]
draws2[location_id==10 & year_id>=2010, (draw.cols):=0, by=id]
draws2[location_id==18 & year_id>=2010, (draw.cols):=0, by=id]
draws2[location_id==218 & year_id>=2015, (draw.cols):=0, by=id]
draws2[location_id==17 & year_id >= 2010, (draw.cols):=0, by=id]

#zero out females as hydrocele prevalence = 0
draws2[sex_id==2, (draw.cols) := 0, by=id]

#write out results so that they can be used in ADL estimates
write.csv(draws2,"FILEPATH")

table(draws2$year_id)

for(i in unique_lf_locations){
  upload_file<-draws2[draws2$location_id==i,]
  upload_file$modelable_entity_id<-ADDRESS
  upload_file$measure_id<-5
 
  write.csv(upload_file,(paste0("FILEPATH", i,".csv")))
}

locs<-get_location_metadata(location_set_id = 35,gbd_round_id=6)
locs<-locs[locs$is_estimate==1,]

location_list<-unique(locs$location_id)

#export all zeros to non-endemic locations

ne_locs<-location_list[! location_list %in% unique_lf_locations]

#OUTPUT zero draws for non-endemic locations for 
for(i in ne_locs){
  #pull in single country and spit out
  upload_file<-draws2[draws2$location_id==211,]
  upload_file$location_id<-i
  upload_file$modelable_entity_id<-ADDRESS
  upload_file$measure_id<-5
  #set all draws to zero
  s1<-setDT(upload_file)
  s1[, id := .I]
  s1[, (draw.cols) := 0, by=id]
  write.csv(s1,(paste0("FILEPATH", i,".csv")))  
}


#save results

source("FILEPATH")
save_results_epi(input_dir =paste0("FILEPATH"),
                 input_file_pattern = "{location_id}.csv", 
                 modelable_entity_id = ADDRESS,
                 description = "Resubmission-spline .1,.4",
                 measure_id = ADDRESS,
                 decomp_step = "iterative",
                 gbd_round_id=ADDRESS,
                 mark_best = TRUE
)       
