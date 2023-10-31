### Code to prep input data for custom model,
### gets covariate values for location - years of data,
### pulls in all-age both sex incidence after inflation for under-reporting 

library(dplyr)
library(data.table)


#create full covariate set 
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")

#STEP 0 SET UP ARGUMENTS AND LOCATION LISTS

###-----------Define Arguments-----------------------####

#define GBD decomp step (decomp_step) and round id (rd_id), last year of estimation for the study (end_year)
decomp_step<-ADDRESS
rd_id<-ADDRESS
end_year<-2022

###------------------------------------------------###

#define a list of locations for the entire GBD study 
d_locs1<-get_location_metadata(gbd_round_id = rd_id, location_set_id = 22,decomp_step=decomp_step)
d_locs1<-d_locs1[d_locs1$is_estimate==1,]
#all locations
location_list<-unique(d_locs1$location_id)

###-----------------------------------------------##

#define locations for dengue endemic settings 

d_geo<-read.csv(sprintf("FILEPATH/gr_ntd_dengue_lgr.csv"), stringsAsFactors = FALSE)
d_geo<-d_geo[d_geo$most_detailed==1,]
d_geo<-d_geo[d_geo$year_start==2019,]
d_geo<-d_geo[d_geo$value_endemicity==1,]

#list of unique dengue endemic locations
unique_d_locations<-unique(d_geo$location_id)


###---------------------------------------------------##

##STEP 1 Pull covariate values AND POPULATION -------------------------------

csmr<-get_covariate_estimates(covariate_id=ADDRESS, gbd_round=rd_id, location_id=location_list, decomp_step=decomp_step)
csmr$csmr_cov<-csmr$mean_value

#just keep males for the purpose of the model 
csmr<-csmr[csmr$sex_id==1,]

deng_prob<-get_covariate_estimates(covariate_id=ADDRESS, gbd_round_id=rd_id,location_id=location_list,decomp_step=decomp_step)
deng_prob$deng_prob_cov<-deng_prob$mean_value


pop_dens<-get_covariate_estimates(covariate_id=ADDRESS, gbd_round_id=rd_id,location_id=location_list,decomp_step=decomp_step)
pop_dens$popdens_cov<-pop_dens$mean_value

#get populations - years from 1980 to last year of cycle
years<-seq(1980,end_year,1)
pops<-get_population(age_group_id = 22,location_id=location_list,year_id=years, gbd_round_id=rd_id, decomp_step=decomp_step)

#merge population total and covariates together
covs<-merge(pops,csmr,by=c("location_id","year_id"))
covs2b<-merge(covs,deng_prob, by=c("location_id","year_id"))

covs2b<-covs2b%>%
  select(-sex_id.y, -sex_id.x, -age_group_id.x, -age_group_id.y, -location_name.x,-location_name.y)

covs2<-merge(covs2b,pop_dens, by=c("location_id","year_id"))
#get location metadata

meta<-get_location_metadata(gbd_round_id = ADDRESS, location_set_id = 22, decomp_step = ADDRESS)

covs3<-merge(meta, covs2, by="location_id")

#reported incidence should be merged to the covariate file
#pull in adjusted case data to copy reported incidence over
cases<-read.csv("FILEPATH/dengue_cases.csv")
cases<-select(cases,cases,sample_size,location_id, year_id)

cases$inc_input<-cases$cases/cases$sample_size

test<-cases[cases$location_id==17,]

covs4<-merge(covs3,cases,by=c("location_id","year_id"),all=TRUE)
covs4<-select(covs4,-cases,-sample_size)
test<-covs4[covs4$location_id==17,]

#output the covariate file - 
write.csv(covs4,"FILEPATH/covariate_file_GBD2020.csv",na=".")


#########STEP 2---------------IMPORT THE ALL AGE BOTH SEX INCIDENCE DATA post UR xwalk ----------------- ####
dt<-read.csv("FILEPATH/dengue_URxwalk.csv")
#create year_id variable to facilitate merge
dt$year_id<-dt$year_start


##aggregate by sex if sex!=both

dt_sex<-dt%>%
  filter(sex!="Both")%>%
  group_by(location_id, year_id, age_start, age_end)%>%
  summarize(new_cases2 = sum(new_cases, na.rm = TRUE),sample_size = sum(effective_sample_size, na.rm=TRUE))

dt_sex$new_cases<-dt_sex$new_cases2
dt_sex$effective_sample_size<-dt_sex$sample_size

dt_sex$new_cases2<-NULL


#append dt_sex to dt

dt<-dt[dt$sex=="Both",]

#keep variables
dt<-select(dt, location_id, year_id, new_cases, age_start, age_end, effective_sample_size, sample_size)

dt<-as.data.frame(dt)
dt_sex<-as.data.frame(dt_sex)

dt3<-as.data.frame(rbind(dt,dt_sex))
                                                
#aggregate only if n per location_id year >10 (age groups 5 x 10 = 50 years of age data )

dt_agg<-dt3%>%
  subset(age_start!=0 & age_end!=99)%>%
  group_by(location_id,year_id)%>%
  count()

#merge dt_agg by age to the main dataset

dt4<-merge(dt3, dt_agg, by=c("location_id","year_id"),all=TRUE)


#drop if n<10 and age_start age_end not 0 - 99 : if we have less than 10 age groups we do not treat this as covering enough of the age distribution and drop

total<-dt4%>%
  filter(is.na(n)|n>10)%>%
  group_by(location_id, year_id)%>%
  summarize(cases = sum(new_cases, na.rm = TRUE), sample_size=sum(sample_size,na.rm=TRUE))


#### STEP 3 ---------MERGE DATA WITH COVARIATE VALUES AND POPULATION TOTALS 
ur<-merge(total, covs2,by=c("location_id","year_id"),all=TRUE)


#output dataset - cases and covariate values, with population
ur$model_dataset<-1


##merge with population  meta_data to control parent/subnats

ur2<-merge(d_locs1,ur,by=c("location_id","location_name"))

write.csv(ur2,"FILEPATH/dengue_cases.csv",na = "")

### STEP 4----------------------output populations by age --------------------------------##


pops<-get_population(sex_id=c(1,2), age_group_i="all",location_id=unique_d_locations,year_id=c(1990,1995,2000,2005,2010,2015,2019,2020,2021,2022), gbd_round_id=rd_id, decomp_step=decomp_step)

#subset age groups needed
pops<-pops[((pops$age_group_id>1 & pops$age_group_id<=238)| pops$age_group_id>=388),]
pops<-pops[pops$age_group_id<=32 | pops$age_group_id==235|  pops$age_group_id==34|  pops$age_group_id==388| pops$age_group_id==238| pops$age_group_id==389,]
pops<-pops[pops$age_group_id<=20 | pops$age_group_id>=30,]
pops<-pops[pops$age_group_id!=5,]
write.csv(pops,"FILEPATH/pop_age20.csv")

#----- DONE ---#

#MOVE ON TO CUSTOM STATA SCRIPT TO RUN MODEL 


#----exploratory review of  data --if concerned about specific locations/covariate combinations

dplot<-read.csv("FILEPATH/dengue_cases.csv")

test<-dplot[dplot$location_id==17,]

library(ggplot2)

dplot$test_mean<-dplot$cases/dplot$sample_size

dplot<-dplot%>%
  subset(csmr_cov<.0001)%>%
  subset(test_mean<.1)

ggplot(dplot, aes(x=csmr_cov, y=test_mean, color=region_id)) +
  geom_point() + 
  geom_smooth(method=lm)


test<-dplot[dplot$csmr_cov>.0001,]
table(test$location_id)
