# Purpose: CL bundle data prep 
# Notes: converts raw case data into all-age, both-sex incidence

library(dplyr)
library(foreign)
library(stringr)

source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")

 
cl_dt <- read.csv("FILEPATH")
table(cl_dt$is_outlier)

cl_dt <- cl_dt[cl_dt$is_outlier!=1 | is.na(cl_dt$is_outlier),]

#CHECK FOR SUBNATIONALS
table(cl_dt$location_name)
##only need to subset Brazil subnationals


#pull all_age data out-this is both sex also
all_age<-cl_dt[cl_dt$age_start==0 & cl_dt$age_end==99,]

#cross-ref locs x years 
all_age_locs<-select(all_age, location_id, year_start)
all_age_locs2<-all_age_locs %>% distinct(location_id, year_start)
all_age_locs2<-as.data.table(all_age_locs2)
all_age_locs2<-add_count(all_age_locs2, location_id, year_start, wt = NULL, sort = FALSE, name = "n")

#check frequency of "n"

#subset age-spec for collapsing
age_spec<-cl_dt[!(cl_dt$age_start==0 & cl_dt$age_end==99),]
#sort by location_id, year_start


age_spec2<-age_spec %>%
  group_by(nid,location_id, year_start) %>%
  summarize(cases_total = sum(cases, na.rm = TRUE))

age_spec_loc<-select(age_spec2, location_id, year_start)
age_spec_loc2<-age_spec_loc%>% distinct(location_id, year_start)

#count records by location_id and year_start
age_spec_loc2<-add_count(age_spec_loc2, location_id, year_start, wt = NULL, sort = FALSE, name = "n")
#check frequency of "n"

##compare all_age_locs2 with age_spec_loc2
age_spec_loc2$nid<-NULL
setDT(age_spec_loc2)
setDT(all_age_locs2)
test<-as.data.table(rbind(age_spec_loc2,all_age_locs2))
test_count<-add_count(test, location_id, year_start, wt = NULL, sort = FALSE, name = "n2")
#check frequency of n2
table(test_count$n2)
#need to reconcile n2=2 and n2=3

count2<-test_count[test_count$n2>1,]
#create flag 
count2$flag<-1
#drop these from age specific totals

#########

#pull variables identifying the data rows
age_vars<-age_spec %>% distinct(nid, location_id, year_start, .keep_all = TRUE)
age_vars$cases<-NULL
age_vars$sex<-"Both"
age_vars$age_start<-0
age_vars$age_end<-99


#append age_spec2 to all_age
#rename cases_total to cases 
age_spec2<-age_spec2 %>% 
  rename(
    cases = cases_total)

#merge age_vars to age_spec2
age_spec3<-merge(age_spec2, age_vars, by=c("nid","location_id","year_start"))

#drop count2 data locations/years
age_spec4<-merge(age_spec3,count2,by=c("location_id","year_start"), all=TRUE)
agedt<-age_spec4[is.na(age_spec4$flag),]
agedt<-select(agedt, -c(flag, n, n2))

cl_inc<-rbind(all_age,agedt)
cl_inc<-cl_inc[!is.na(cl_inc$year_start),]
cl_inc<-cl_inc[!is.na(cl_inc$location_id),]

#pull in population
years<-unique(cl_inc$year_start)
locs<-unique(cl_inc$location_id)
cl_pops<-get_population(age_group_id = 22, year_id=years, sex_id=3, location_id=locs, gbd_round_id="ADDRESS", decomp_step="ADDRESS")

cl_pops<-cl_pops %>% 
  rename(
    year_start = year_id)

#recalculate mean, lower, upper
cl_inc2<-merge(cl_pops,cl_inc, by=c("location_id","year_start"))


cl_inc2$effective_sample_size<-cl_inc2$population
cl_inc2$sample_size<-cl_inc2$population
cl_inc2$mean<-cl_inc2$cases/cl_inc2$population
cl_inc2$standard_error<-(sqrt(cl_inc2$cases)/cl_inc2$population)


#get iso3 codes
mdt<-get_location_metadata(gbd_round_id="ADDRESS",location_set_id = 22, decomp_step="ADDRESS")

#keep location_id and ihme_loc_id
myvars <- c("location_id", "ihme_loc_id")
mdt<-as.data.frame(mdt)
mdt2 <- mdt[myvars]

#drop ihme_loc_id so we can merge complete back in
cl_inc2$ihme_loc_id<-NULL

cl_inc3<-merge(cl_inc2,mdt2,by="location_id")

cl_inc3$iso3<-str_sub(cl_inc3$ihme_loc_id,1,3)

table(cl_inc3$sex)
#apply under-reporting adjustment in stata


#output dataset to params:
write.dta(cl_inc3,"FILEPATH/inc20.dta")

###NEXT Step is to run the Step 1 code to apply Under-reporting scalars
