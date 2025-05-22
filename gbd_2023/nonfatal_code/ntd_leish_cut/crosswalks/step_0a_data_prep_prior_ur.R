# Purpose: CL bundle data prep 
# Notes: converts raw case data into all-age, both-sex incidence for input into ST-GPR incidence model
# As of GBD 2022: offsetting zeroes to minimum incidence * 0.5 due to limitations of ST-GPR

# Define paths
data_root <- 'FILEPATH'
params_dir <- paste0(data_root, "/FILEPATH/params")

library(dplyr)
library(foreign)
library(stringr)
library(data.table)
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_bundle_version.R")
source("/FILEPATH/get_bundle_data.R")
source("/FILEPATH/save_bundle_version.R")

release_id <- ADDRESS
date <- Sys.Date()

get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[(is.na(standard_error) | standard_error == 0) & is.numeric(lower) & is.numeric(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[(is.na(standard_error) | standard_error == 0) & (measure == "prevalence" | measure == "proportion"),
     standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[(is.na(standard_error) | standard_error == 0) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}

##################################################################

### Pull data 
cl_dt <- get_bundle_version(ADDRESS)

# adding outliering of brazil subnats 
bra_subnats <- get_location_metadata(location_set_id = 35, release_id = release_id)[parent_id == ID]$location_id
cl_dt$is_outlier <- ifelse(cl_dt$location_id %in% bra_subnats & cl_dt$year_start < 2000 & cl_dt$cases < 10, 1, cl_dt$is_outlier)

table(cl_dt$is_outlier)
cl_dt <- cl_dt[cl_dt$is_outlier!=1 | is.na(cl_dt$is_outlier),]

#CHECK FOR SUBNATIONALS

table(cl_dt$ihme_loc_id)
cl_dt$mean <- cl_dt$cases / cl_dt$sample_size

## offset zeroes
min_val <- cl_dt  %>% 
  summarise(min_val = min(mean[mean > 0] * .5))
cl_dt$mean <- ifelse(cl_dt$mean == 0, min_val$min_val, cl_dt$mean)

## recalculate incident cases
cl_dt$cases <- cl_dt$mean * cl_dt$sample_size


#pull all-age data out-this is both sex also
all_age <- cl_dt[cl_dt$age_start==0 & cl_dt$age_end==99,]

### Some new Brazil reported data for previous years - need to aggregate
check <- all_age[, num_rows := (count = .N), by = c('year_start','year_end','location_id','sex')] 
check_keep <- subset(check, num_rows == 1)

#cross-ref locs x years 
check_agg <- select(all_age, location_id, year_start, num_rows)
check_agg <- subset(check_agg, num_rows > 1)
check_agg <- check_agg[, num_rows := NULL]
check_agg <- distinct(check_agg)

check_agg2 <- left_join(check_agg, all_age, by = c('location_id','year_start'))
check_agg2 <- check_agg2 %>% 
  group_by(year_start,location_id) %>% 
  mutate(cases = sum(cases))

# Remove duplicated rows 
check_agg3 <- check_agg2 %>%
  distinct(location_id,sex,age_start,age_end,year_start,year_end,cases, .keep_all = TRUE)

# add to previous all-age
all_age <- rbind(check_keep,check_agg3)
all_age <- all_age[, num_rows := NULL]

#cross-ref locs x years 
all_age_locs <- select(all_age, location_id, year_start)
all_age_locs2 <- all_age_locs %>% distinct(location_id, year_start)
all_age_locs2 <- as.data.table(all_age_locs2)
all_age_locs2 <- add_count(all_age_locs2, location_id, year_start, wt = NULL, sort = FALSE, name = "n")

#check frequency of "n"

#subset age-spec for collapsing
age_spec <- cl_dt[!(cl_dt$age_start==0 & cl_dt$age_end==99),]

#sort by location_id, year_start
age_spec2 <- age_spec %>%
  group_by(nid,location_id, year_start) %>%
  summarize(cases_total = sum(cases, na.rm = TRUE))

age_spec_loc <- select(age_spec2, location_id, year_start)
age_spec_loc2 <- age_spec_loc%>% distinct(location_id, year_start)

#count records by location_id and year_start
age_spec_loc2 <- add_count(age_spec_loc2, location_id, year_start, wt = NULL, sort = FALSE, name = "n")
#check frequency of "n"

##compare all_age_locs2 with age_spec_loc2
age_spec_loc2$nid <- NULL
setDT(age_spec_loc2)
setDT(all_age_locs2)
test <- as.data.table(rbind(age_spec_loc2, all_age_locs2))
test_count <- add_count(test, location_id, year_start, wt = NULL, sort = FALSE, name = "n2")
#check frequency of n2
table(test_count$n2)
#need to reconcile n2=2 and n2=3

count2 <- test_count[test_count$n2>1,]
#create flag 
count2$flag <- 1
#drop these from age specific totals

#########

#pull variables identifying the data rows
age_vars <- age_spec %>% distinct(nid, location_id, year_start, .keep_all = TRUE)
age_vars$cases<-NULL
age_vars$sex<-"Both"
age_vars$age_start<-0
age_vars$age_end<-99


#append age_spec2 to all_age
age_spec2<-age_spec2 %>% 
  rename(
    cases = cases_total)

#merge age_vars to age_spec2
age_spec3 <- merge(age_spec2, age_vars, by=c("nid","location_id","year_start"))

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
cl_pops<-get_population(age_group_id = 22, year_id=years, sex_id=3, location_id=locs, release_id = release_id)

cl_pops<-cl_pops %>% 
  rename(
    year_start = year_id)

#recalculate mean, lower, upper
cl_inc2<-merge(cl_pops,cl_inc, by=c("location_id","year_start"))


cl_inc2$effective_sample_size<-cl_inc2$population
cl_inc2$sample_size<-cl_inc2$population
cl_inc2$mean<-cl_inc2$cases/cl_inc2$population
cl_inc2$standard_error<-(sqrt((cl_inc2$mean)/cl_inc2$population)) 
cl_inc2$lower <- cl_inc2$mean - (1.96*cl_inc2$standard_error)
cl_inc2$upper <- cl_inc2$mean + (1.96*cl_inc2$standard_error)

mdt<-get_location_metadata(location_set_id = 22, release_id = release_id)
myvars <- c("location_id", "ihme_loc_id")
mdt<-as.data.frame(mdt)
mdt2 <- mdt[myvars]

#drop ihme_loc_id so we can merge complete back in
cl_inc2$ihme_loc_id<-NULL

cl_inc3<-merge(cl_inc2,mdt2,by="location_id")

cl_inc3$iso3<-str_sub(cl_inc3$ihme_loc_id,1,3)

table(cl_inc3$sex)
#apply under-reporting adjustment in next step

#output dataset to feed into incidence model (training set + estimation frame)
write.csv(cl_inc3, paste0(params_dir, "/FILEPATH"), row.names = FALSE)
