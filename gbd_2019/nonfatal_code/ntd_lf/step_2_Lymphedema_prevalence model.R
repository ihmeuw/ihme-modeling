


user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILEPATH", user, "FILEPATH")
#library(BayesianTools, lib.loc=path)
library(dplyr)
require(splines)
library(INLA)

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

library(ggplot2)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

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

mod_data<-read.csv("FILEPATH")

#summarize main variables
summary(mod_data$lymph_mean)
summary(mod_data$mean_adjusted)
plot(mod_data$mean_adjusted,mod_data$lymph_mean)
#high value lymph  mean due to age splitting small sample sizes



mod_data$age_diff<-mod_data$age_end-mod_data$age_start
mod_data$age_start<-ifelse(mod_data$age_diff>25,40,mod_data$age_start)
mod_data<-mod_data[mod_data$age_diff<25,]

#drop where lymph_ss (sample size) <1 - implausible denominator for national level analysis
#outlier
mod_data<-mod_data[mod_data$lymph_ss>=25,]
mod_data<-mod_data[mod_data$lymph_mean<.2,]

mod_data$lymph_mean<-ifelse(mod_data$age_start<=5,0,mod_data$lymph_mean)
mod_data$lymph_cases<-ifelse(mod_data$age_start<=5,0,mod_data$lymph_cases)

ggplot(mod_data, aes(x=mean_adjusted, y=lymph_mean, color=age_start)) +
  geom_point()


mod_data<-mod_data[!(mod_data$mean_adjusted<0.05 & mod_data$lymph_mean>.01),]

#add spline to mean_adjusted
mod1<-glm(lymph_mean~age_start+sex+bs(mean_adjusted, knots = c(.1,.15)), weights=lymph_ss, family=binomial(link="log"), data=mod_data)

#plot predictions
summary(mod1)

preds<-as.data.frame(exp(predict(mod1)))

names(preds)[1]<-"prediction"

mod_data2<-mod_data[!is.na(mod_data$mean_adjusted),]

#append
mod_data3<-cbind(mod_data2,preds)
plot(mod_data3$lymph_mean,mod_data3$mean_adjusted)

plot(mod_data3$age_start,mod_data3$prediction)


#ggplot to view by age
ggplot(mod_data3, aes(x=mean_adjusted, y=prediction, color=age_start)) +
  geom_point()


###data processing to implement in INLA

#get age_group_ids - GBD 2019

age_info<-read.csv("FILEPATH")

#keep age_start, mean_adjusted, sex, lymph_mean

keeps <- c("FILEPATH")
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

lf_prev<-get_model_results(gbd_team='epi', gbd_id=ADDRESS, gbd_round_id=6,age_group_id=ages, sex_id=c(1,2), location_id=unique_lf_locations, year_id=c(1990,1995,2000, 2005, 2010,2015, 2017, 2019), decomp_step="iterative")

#convert to data table
prev2<-as.data.table(lf_prev)
#create ID for each row
#name as mean_adjusted to append to other data
prev2$mean_adjusted<-prev2$mean

#merge age_mid together
prev2_b<-merge(prev2,age_info,by="age_group_id")

lf_df3<-select(prev2_b, age_group_id, age_mid, year_id, sex_id, location_id, mean_adjusted, age_start)

#create empty variables to enable prediction from model
lf_df3$lymph_mean<-NA
lf_df3$lymph_ss<-NA
lf_df3$lymph_cases<-NA
#dataset is an indicator variable that classifies observations as belonging to the final
#set we want to output; 0 - are records from the observations used to fit the model
lf_df3$dataset<-1
#append model inputs

### append all together to generate model dataset with rows for prediction
mod_1bL<-rbind(lf_df3,df2)


##############RUN MODEL ##############3



summary(mod_1bL$lymph_mean)
summary(mod_1bL$mean_adjusted)

plot(mod_1bL$mean_adjusted,mod_1bL$lymph_mean)
mod_1bL<-mod_1bL[!(mod_1bL$mean_adjusted<.05 & mod_1bL$lymph_mean>.05 & mod_1bL$dataset==0),]

mod_1bL<-mod_1bL[!(mod_1bL$mean_adjusted>.1 & mod_1bL$lymph_mean>.06 & mod_1bL$dataset==0),]

#Run model in INLA- test without regional_random effects
knots=seq(.05,.4)
formula <- lymph_cases~sex_id+mean_adjusted #+f(super_region_id, model="iid")
result2 <- inla(formula, family="binomial", control.compute=list(config = TRUE), control.predictor=list(link=1), Ntrials=lymph_ss, data=mod_1bL, verbose=F)

#result2 stores the results of the inla model
summary(result2)


#here the range = # of rows is the input dataset - this allows INLA to generate predictions

range<-nrow(mod_1bL)
results_simple<-result2$summary.fitted.values[1:range, ]

#append to input data

combined<-cbind(mod_1bL,results_simple)

#plot results of the model 
#variable - mean is the result of the model 
plot(combined$lymph_mean, combined$mean)
plot(combined$age_mid, combined$mean)
plot(combined$mean_adjusted, combined$mean)
plot(combined$year_id, combined$mean)

library(ggplot2)
#plot the mean LF prevalence on the x-axis and mean lymph prediction on the y-axis
ggplot(combined, aes(x=mean_adjusted, y=mean, color=age_start)) +
  geom_point()

summary(combined$mean)

high_30<-combined[combined$mean>.3,]

table(high_30$location_id)


#-------------------generate 1000 draws from inla
results <- inla.posterior.sample(1000, result2,use.improved.mean=FALSE)
predictions <- lapply(results, function(p) p$latent[1:range])
predictions <- do.call(cbind, predictions)

predictions2<-as.data.frame(predictions)
#bind the first columns 
draws3<-predictions2 %>%
  dplyr::rename_all(
    ~stringr::str_replace_all(.,"V","draw_")
  )
#change draw_1000 to draw_0
names(draws3)[names(draws3) == "draw_1000"] <- "draw_0"

ilog <- function(x) ((exp(x)/(1+exp(x))))
draws4<-draws3 %>% mutate_all(ilog)

#keep first six columns of dataset 
columns<-select(mod_1bL, age_group_id, age_mid, year_id, sex_id, location_id, dataset, mean_adjusted)

draws<-cbind(columns,draws4)
#change variable names from V# to draw_0 etc.

#now just need to keep predictions from model, so subset out where dataset=1

draws<-draws[draws$dataset==1,]

ggplot(draws, aes(x=mean_adjusted, y=draw_0, color=age_group_id)) +
  geom_point()



draws$dataset<-NULL

#reduce draws by 67% of cases as early lymphedema (stage 1-2)
#cite: https://journals.plos.org/plosntds/article?id=10.1371/journal.pntd.0004917
for(j in grep('draw', names(draws))){
 set(draws, i= which(draws[[j]]<1), j= j, value=draws[[j]]*.33)
}

#output draws to folder by location
draw.cols <- paste0("draw_", 0:999)


draws2<-setDT(draws)
draws2[, id := .I]
#set all draws for age 5 and under to 0
draws2[age_group_id<6, (draw.cols) := 0, by=id]
#hard code elimination values for togo, egypt, cambodia, thailand, sri lanka - achieved before 2017
draws2[location_id==141 & year_id>=2010, (draw.cols):=0, by=id]
draws2[location_id==10 & year_id>=2010, (draw.cols):=0, by=id]
draws2[location_id==18 & year_id>=2010, (draw.cols):=0, by=id]
draws2[location_id==218 & year_id>=2015, (draw.cols):=0, by=id]
draws2[location_id==17 & year_id >= 2010, (draw.cols):=0, by=id]

#write out results so that they can be used in ADL estimates

write.csv(draws2,"FILEPATH")

for(i in unique_lf_locations){
  upload_file<-draws2[draws2$location_id==i,]
  upload_file$modelable_entity_id<-ADDRESS
  upload_file$measure_id<-5
    s1<-setDT(upload_file)
  write.csv(s1,(paste0("FILEPATH", i,".csv")))
}

locs<-get_location_metadata(location_set_id = 35, gbd_round_id=6)
locs<-locs[locs$is_estimate==1,]

location_list<-unique(locs$location_id)

#export all zeros to non-endemic locations

ne_locs<-location_list[! location_list %in% unique_lf_locations]

#OUTPUT zero draws for non-endemic locations 

for(i in ne_locs){
  #pull in single country and spit out
  upload_file<-draws[draws$location_id==211,]
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
                 description = "Iterative model resub test2",
                 measure_id = ADDRESS,
                 decomp_step = "iterative",
                 mark_best = TRUE
)       
