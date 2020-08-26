
user <- Sys.info()[["user"]]    ## Get current user name
path <- FILEPATH
install.packages("simsem", lib=path)
install.packages("vctrs", lib=path)
install.packages("tidyverse",lib=path)
library(vctrs, lib.loc=path)
library(plyr)
library(dplyr)
library(lme4)
library(simsem)
library(tidyverse)

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
    prefix <- "FILEPATH"
}   else {
    prefix <- "FILEPATH"
}

#source central functions
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))

source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))
source(FILEPATH))


#import deaths data 
zika_deaths_input_new<- get_bundle_data(bundle_id = ADDRESS, decomp_step = 'iterative', export=FALSE)

required_locations<-zika_deaths_input_new$location_id


#only gets population for required locations and for years with any Zika infection
population_file<-get_population(age_group_id="all",
                                    sex_id=c(1,2),
                                    location_id=required_locations,
                                    year_id=c(2015,2016,2017,2018,2019),
                                    decomp_step = "step4")


#import draws from GBD 2017 as a place-holder for COD Step 4 deadline
nf_draws<-read.csv("FILEPATH")


nf_draws_loc_2015<- subset(nf_draws, nf_draws$year_id==2016)
nf_draws_loc_2015$year_id<- 2015

nf_draws_loc_2018<- subset(nf_draws, nf_draws$year_id==2017)
nf_draws_loc_2018$year_id<- 2018

nf_draws_loc_2019<- subset(nf_draws, nf_draws$year_id==2017)
nf_draws_loc_2019$year_id<- 2019


nf_draws_loc<- rbind(nf_draws, nf_draws_loc_2015, nf_draws_loc_2018, nf_draws_loc_2019)
#this reduces the nf draws file to just the locations that are in the deaths dataset and we make an estimate for

nf_draws_loc_new<-merge(population_file, nf_draws_loc, by= c("location_id","year_id","age_group_id","sex_id"))
#this does not have Saint Kitts & Nevis as it was not a location in the nf_draws file


#calculate row means from draws
draws<-subset(nf_draws_loc_new, select = grep("^[d]", names(nf_draws_loc_new))) 
#this gets the loc_id, year, age, pop, sex, and run id (so everything other than the draws)
locs1<-nf_draws_loc_new[,1:6]

means_nf<-rowMeans(draws,na.rm=FALSE, dims=1)
nf_draws_loc2<-cbind(locs1,means_nf)

nf_draws_loc2$cases<-nf_draws_loc2$means_nf*nf_draws_loc2$population


#drop mean of zero - but it drops entire locations
nf_draws_loc2<-nf_draws_loc2[(nf_draws_loc2$cases>0), ]

#summarize cases over all locations, years
agg_data<- nf_draws_loc2[, .("total_cases" = sum(cases), "total_pop" = sum(population)), by = c("location_id", "year_id")]


colnames(agg_data)[colnames(agg_data)=="year_id"] <- "year_start"

draws_deaths<-merge(agg_data,zika_deaths_input_new, by=c("location_id","year_start"))

##run a mixed effects model 
#center total_cases
mean_cases<-mean(draws_deaths$total_cases,na.rm=T)
draws_deaths$center_cases<-(mean_cases-draws_deaths$total_cases)*.001

#drop missing incidence
draws_deaths<-draws_deaths[!(is.na(draws_deaths$center_cases)), ]


mod1<-glmer.nb(cases~center_cases + (1|location_id), family="binomial", data=draws_deaths)


#model outputs are predictions for 2016
beta0<-mod1@beta[1]
beta1<-mod1@beta[2]

rei1<-ranef(mod1,condVar=TRUE, whichel = "location_id")
dotplot.ranef.mer(rei1)

#returns intercept for each location_id
intercepts_loc<-as.data.frame(ranef(mod1,condVar=TRUE, whichel = "location_id"))
intercepts_loc$location_id<-as.numeric(as.character(intercepts_loc$grp))

#add intercept to dataset
step1<-merge(draws_deaths,intercepts_loc,by="location_id")
#calculate mean deaths 
step1$val<-exp(beta0+beta1*step1$center_cases+step1$condval)


#simulate uncertainty from mean predictions - REs not stable for locations with 0 deaths
#using placeholder standard deviation
step1$val_sd<-sd(step1$val)

#generate from beta distribution
draws.required <- 1000
draw.cols <- paste0("draw_", 0:999)
step1_dt<-data.table(step1)


step1_dt[, beta:=total_pop-val]
step1_dt[, id := .I]
step1_dt[, (draw.cols) := as.list((rnorm(draws.required, val, val_sd))/2), by=id]


#check means are around what we predicted
draws<-subset(step1_dt, select = grep("^[draw_]", names(step1_dt))) 
draws2<-draws[,9:1008]
means<-rowMeans(draws2,na.rm=FALSE, dims=1)
means2<-as.data.frame(means)
means2[156,]

###Age and sex distribution###

#clean up year 2016 - set to age group ADDRESS (50 year olds)
step1_dt$age_group_id<-ADDRESS1

males<-step1_dt
males$sex_id<-1

females<-step1_dt
females$sex_id<-2

#append together
total_2016<-rbind(males,females)

#set negative draws to 0
for(j in grep('draw', names(total_2016))){
    set(total_2016, i= which(total_2016[[j]]<0), j= j, value=0)
}

setDT(total_2016)

#drop unnecessary columns from total_2016
del <- c('total_cases','total_pop','Deaths','center_cases','grpvar','grp','condval','condsd','val','val_sd','beta','id','term')
total_2016_b<-total_2016[, (del):= NULL]


##generate all other ages for males and females 1000 draws 0 values

#generate empty data frame for all age/sex rows but only for required locations 

step2<-get_population(age_group_id = "all", sex_id=c(1,2), decomp_step = "step4", year_id=2016, location_id=required_locations)

draws.required <- 1000
draw.cols <- paste0("draw_", 0:999)
step2_dt<-data.table(step2)

step2_dt[, id := .I]
step2_dt[, (draw.cols) := as.list(0), by=id]

del <- c('population','id','run_id')
step2_dt<-step2_dt[, (del):= NULL]


#drop age_group_id = ADDRESS (already estimated above)
step2_dt <-step2_dt[ age_group_id != ADDRESS2] 

#keep relevant variables for dataset step1  & step 2
#age_group_id, sex_id, location_id, year_id, draws 


#append 2016 results together
zika_2016<-rbind(step2_dt,total_2016_b)

table(zika_2016$location_id)
table(zika_2016$year_id)
table(zika_2016$age_group_id)
table(zika_2016$sex_id)



#check distribution of draws (only for non-zero draw locs)
check<-zika_2016[zika_2016$age_group_id==ADDRESS3,]

library(matrixStats)
draws<-as.matrix(subset(check, select = grep("^[d]", names(check)))) 
mins<-rowMins(draws,na.rm=FALSE, dims=1)
maxs<-rowMaxs(draws,na.rm=FALSE, dims=1)
means<-rowMeans(draws,na.rm=FALSE,dims=1)



###output deaths draws for 2016 
write.csv(zika_2016,"FILEPATH")
write.csv(zika_2016,"FILEPATH")
