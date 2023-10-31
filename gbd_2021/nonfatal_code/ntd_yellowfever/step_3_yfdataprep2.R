# Purpose: Data management of input data - clean, dedup
# Notes: Takes training dataset and estimates beta coefficients off of covariate values

### ========================= BOILERPLATE ========================= ###
user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILEPATH")


library(data.table)
library(MASS)
library(splines)
require(dplyr)
library(tibble)
library(lme4)
library(foreign)
rm(list = ls())

os <- .Platform$OS.type
if (os == ADDRESS) {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_bundle_data.R")

###-----------PULL IN AGE DATA FROM BUNDLE---------------------
bundle_id <- ADDRESS
decomp_step <- ADDRESS
gbd_round_id = ADDRESS
bundle_df <- get_bundle_data(bundle_id, decomp_step, gbd_round_id=gbd_round_id)
##########################################

#flatfile - for step 2 
bundle_df<-read.csv("FILEPATH")


#drop non-endemic locations

#note must keep 214 and 135 for brazil, kenya ethiopia and nigeria: we implement national level analysis first
yf_gr<-read.csv("FILEPATH")
yf_gr<-yf_gr[yf_gr$most_detailed==0,]
yf_gr<-yf_gr[yf_gr$year_start==2019,]
yf_gr1<-yf_gr[yf_gr$value_endemicity==1,]


yf_gr<-read.csv("FILEPATH")
yf_gr<-yf_gr[yf_gr$most_detailed==1,]
yf_gr<-yf_gr[yf_gr$year_start==2019,]
yf_gr_d<-yf_gr[yf_gr$value_endemicity==1,]

gr<-rbind(yf_gr_d,yf_gr1)

#list of unique YF endemic locations
yf_locs<-unique(gr$location_id)
years<-(1980:2022)
#############


### ----------------BUILD TRAINING DATASET - CASES, YEAR AND SDI WITH POPULATION ---------------------
#drop if measure!=incidence
bundle_df<-bundle_df[bundle_df$measure=="incidence",]

#drop if subnational
bundle_df<-bundle_df[bundle_df$specificity!="subnational",]
bundle_df<-bundle_df[bundle_df$specificity!="subnational x date",]

#drop if year_start!=year_end
bundle_df<-bundle_df[bundle_df$year_start==bundle_df$year_end,]

#drop if group_review!=1
bundle_df<-bundle_df[bundle_df$group_review==1,]

#-exact values on these fields
bundle_df2<-bundle_df %>% distinct(location_id, year_start, year_end, sex, age_start, age_end, cases, .keep_all = TRUE)

check<-as.data.frame(table(bundle_df2$location_id,bundle_df2$year_start))
check2<-check[check$Freq>1,]

#pull all-age data 
all_age<-bundle_df2[bundle_df2$age_start==0 & bundle_df2$age_end==99,]

age_spec<-bundle_df2[!(bundle_df2$age_start==0 & bundle_df2$age_end==99),]

#aggregate age_spec into single age category
age2<-as.data.table(age_spec)[, sum(cases), by = .(location_id, year_start)]
age3<-as.data.table(age_spec)[, sum(sample_size), by = .(location_id, year_start)]
age3<-age3[,3]

names(age2)[names(age2)=="V1"] <- "cases"
names(age3)[names(age3)=="V1"] <- "sample_size"
age4<-cbind(age2,age3, fill=TRUE)

#append age4 to dataset
all2<-rbind(all_age,age4,fill=TRUE)

#merge population into sample_size variable

#rename all2 year variable
names(all2)[names(all2)=="year_start"] <- "year_id"

#drop unnecessary variables
myvars <- c("location_id", "cases","year_id")

all3 <- as.data.frame(all2)[myvars]
all3<-all3[!is.na(all3$year_id),]

# drop if cases == 0 under the assumption that zeros are not true zeros in endemic countries
all3<-all3[all3$cases>0,]
#create variable to identify training dataset
all3$training_set<-1

#merge population

#only merge on locs - YF  locations from training set
years<-unique(all3$year_id)
locs<-unique(all3$location_id)
pops<-get_population(age_group_id=22, year_id=years, sex_id=3,gbd_round_id=ADDRESS,location_id=locs,decomp_step = ADDRESS)

#merge population with dataset

all4<-merge(all3,pops,by=c("location_id","year_id"))
all4$run_id<-NULL

#get location meta data - region_id, super_region_id and countryIso
meta <- get_location_metadata(location_set_id = 35, gbd_round_id=ADDRESS, decomp=ADDRESS)
meta$countryIso <- substr(meta$ihme_loc_id,1,3)
meta2 <- meta[,c( "location_id","super_region_id","region_id",
                  "countryIso")]
#merge meta data
all4_b<-merge(all4,meta2,by="location_id")

#merge sdi with dataset 
#match covariates sdi

sdi<-get_covariate_estimates(covariate_id=881,gbd_round=ADDRESS,location_id=locs,year_id=years, decomp_step=ADDRESS)
myvars<- c("location_id","year_id","mean_value")
sdi <- as.data.frame(sdi)[myvars]
sdi$sdi<-sdi$mean_value
sdi$mean_value<-NULL

t_sdi<-merge(all4_b,sdi,by=c("location_id","year_id"))
t_sdi$age_group_id<-NULL
t_sdi$sex_id<-NULL


### ----------T_SDI IS TRAINING DATASET --------------

### ---- CODE TO APPEND ALL-AGE SKELETON  -------------------------
#append skeleton 
skeleton<-read.csv("FILEPATH")

#add variable to indicate these are estimation rows
skeleton$training_set<-0

#merge cases in where reported
cases<-all3[,1:3]

#national only 
cases_nat<-cases[(cases$location_id!=135 & cases$location_id!=214 & cases$location_id!=179 & cases$location_id!=180), ]
skeleton2<-merge(skeleton,cases_nat,by=c("location_id","year_id"),all=TRUE)

#subnational
cases_subnat<-cases[(cases$location_id==135 | cases$location_id==214 | cases$location_id==179 | cases$location_id==180), ]

#rename location id to parent id 
cases_subnat <-cases_subnat %>%                     
  rename(parent_id="location_id", cases_sub="cases")

#national level cases are linked to all subnational locations
skeleton3<-merge(skeleton2,cases_subnat,by=c("parent_id","year_id"),all=TRUE)


myvars <- c("location_id", "sex_id", "population","year_id","training_set","cases","cases_sub","region_id","super_region_id","countryIso","parent_id")
skeleton <- as.data.frame(skeleton3)[myvars]

#new years
years2<-(1980:2022)

#match covariates sdi - for location years in skeleton
locs2<-unique(skeleton$location_id)
sdi<-get_covariate_estimates(covariate_id=881,gbd_round=ADDRESS,location_id=locs2,year_id=years2, decomp_step=ADDRESS)
myvars<- c("location_id","year_id","mean_value")
sdi <- as.data.frame(sdi)[myvars]
sdi$sdi<-sdi$mean_value
sdi$mean_value<-NULL

#merge sdi with dataset
sk2<-merge(skeleton,sdi,by=c("location_id","year_id"))
sk2$sex_id<-NULL

#over-write subnational SDI with national level 
nats_sdi<-get_covariate_estimates(covariate_id=881,gbd_round=ADDRESS,location_id=c(135,179,180,214),year_id=years2, decomp_step=ADDRESS)
myvars<- c("location_id","year_id","mean_value")
nats_sdi <- as.data.frame(nats_sdi)[myvars]
nats_sdi$sdi<-sdi$mean_value
nats_sdi$mean_value<-NULL

nats_sdi <-nats_sdi %>%                     
  rename(parent_id="location_id")

#over-write subnational population with national 
nats_pops<-get_population(age_group_id=22, year_id=years2, sex_id=3,gbd_round_id=ADDRESS,location_id=c(135,179,180,214),decomp_step = ADDRESS)
myvars<- c("location_id","year_id","population")
nats_pops <- as.data.frame(nats_pops)[myvars]

nats_pops <-nats_pops %>%                     
  rename(parent_id="location_id")

nats_pops$subnat<-1

#append skeleton and training set together
#merge subnational/national sdi
sk3<-merge(sk2,nats_sdi,by=c("parent_id","year_id"),all=TRUE)
#merge subnational/national population
sk4<-merge(sk3,nats_pops,by=c("parent_id","year_id"),all=TRUE)

sk4 <-sk4 %>%                     
  rename(sub_nat_pop="population.y", population="population.x")
sk4$parent_id<-NULL


####------COMBINED TRAINING DATASET AND ESTIMATION FRAME

t_sdi$subnat<-0
t_sdi$cases_sub<-NA

t_sdi$sub_nat_pop<-NA
all6<-rbind(sk4,t_sdi)

#create center variable 
all6$yearC<-all6$year_id-((2022-1990)/2)

#convert countryIso to string
all6$countryIso2<-as.character(all6$countryIso)

#all6 will contain the training dataset and the estimation rows (one per location-year)

#need to drop non-edemic locations completely

#locs= gr locs

all7<-filter(all6, location_id %in% yf_locs)

#output dataset to feed into incidence model (training set + estimation frame)
write.dta(all7,"FILEPATH")

