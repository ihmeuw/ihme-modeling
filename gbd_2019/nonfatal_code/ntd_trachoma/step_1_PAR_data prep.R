###CLEAN AND PREP TRACHOMA PAR ESTIMATES

user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILEPATH", user, "FILEPATH")
library(dplyr)
library(tidyr)

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

#load functions 

source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source("FILEPATH")
#pull in geographic restrictions and drop non-endemic countries and drop restricted locations


#pull in geographic restrictions file 

tra_geo<-read.csv(sprintf("FILEPATH"), stringsAsFactors = FALSE)

dataset<-tra_geo[,1:50]

#convert to long
melt_data<-melt(dataset, id.vars = c('loc_id',
                                     'parent_id',
                                     'level',
                                     'type',
                                     'loc_nm_sh',
                                     'loc_name',
                                     'spr_reg_id',
                                     'region_id',
                                     'ihme_lc_id',
                                     'GAUL_CODE'))

melt_data$value<-as.character(melt_data$value)

status_list<-subset(melt_data, type=='status')

presence_list<-subset(status_list,value=='p' |value=='pp')

#list of unique trachoma endemic locations
unique_tra_locations<-unique(presence_list$loc_id)

#use cod to generate a list of years for annual every year_ids
pops<-get_demographics(gbd_team = "cod")
years<-unique(pops$year_id)

#all location id meta data are sourced here
locs<-get_location_metadata(location_set_id = 35)
location_list<-unique(locs$location_id)


#-------------------------PAR ESTIMATES--------------------------------------------------#

#pull in PAR original data files from WHO GHO observatory NIDs
#Data from GHO 2005 - 2012

par_1<-read.csv("FILEPATH")

#set case management=0 and no data to NA
par_1$par_n<-as.numeric(as.character(par_1$par)) 

#import data from 2013 - 2017 
par_2<-read.csv("FILEPATH")

#convert wide to long
par2_long <- gather(par_2, year_id, population, X2013:X2017, factor_key=TRUE)
par2_long$par_n<-as.numeric(as.character(par2_long$population)) 
par2_long$NID<-NID

#remove X from year_id
par2a<-par2_long %>%
    mutate(year_id = recode(year_id, "X2017"=2017, "X2016"=2016, "X2015"=2015, "X2014"=2014,"X2013"=2013))

#simplify columns
par1<-par_1 %>%
  select(NID, country_name, par_n, year_id)

par2<-par2a %>%
  select(NID, country_name, par_n, year_id)

#append two datasets
pars<-rbind(par1,par2)

#change china zeros to missing
pars$par_n<-ifelse(pars$country_name=="China",NA,pars$par_n)


#rename country_name to location_name
colnames(pars)[colnames(pars)=="country_name"] <- "location_name"

#merge names to location_id
pops_par<-merge(pars,locs, by="location_name")

#drop countries for wich PAR data and GRs are inconsistent: Botswana (193), 
#change Mexico to Chiapas = Location_id 130 to 4649
#drop brazil 135, iran 142, india 163, pakistan 165, congo 170, Djibouti 177, ethiopia 179, kenya 180, Namibia 195, 214 Nigeria, 19 timor leste

#subset out dataset for subnationals

subnational<-pops_par %>% 
  filter(location_id==6| location_id==142 |location_id==153 |location_id==165|location_id==214 | location_id==179 |location_id==130 | location_id==135 | location_id==163 |location_id==180)

pops_par<-pops_par %>%
  filter(location_id!=6 & location_id!=193 & location_id!=19 & location_id!= 177 & location_id!=170 & location_id!=195& location_id!=142 & location_id!=153 & location_id!=165 & location_id!=214 & location_id!=179 & location_id!=130 & location_id!=135 & location_id!=163 & location_id!=180)


#generates a dataset to check if we didn't match loc ids, should be zero
test<-pops_par %>% filter(!is.na(par_n) & is.na(location_id))

#drop variables and simplify columns
pops_par2<-pops_par %>%
  select(NID, location_name, par_n, year_id, location_id)

#merge with year_ids 


#unique locations for par estimates
unique_par_locs<-unique(pops_par2$location_id)

#use cod for annual every year_ids
pops<-get_demographics(gbd_team = "cod")
years<-unique(pops$year_id)
pop_gbd<-get_population(location_id = unique_par_locs, age_group_id = c(22),sex_id = c(3), year_id=years, decomp_step = "step3")

#merge population with the location and year_id of the PAR data

#this will have all years for all trachoma_endemic locations
cov1<-merge(pop_gbd,pops_par2,by=c("location_id","year_id"),all=TRUE)


#-------------calculate proportion -----------------------#

cov1$proportion<-cov1$par_n/cov1$population
cov1$proportion[cov1$proportion>1]<-1

#--------------fill time series -----------------

cov2<-cov1%>%
group_by(location_id) %>%
  fill(proportion)

cov3<-cov2%>%
  group_by(location_id) %>%
  fill(proportion, .direction=c("up"))

#for trachoma endemic countries for which  no PAR data is available we set PAR to 25%

cov3$proportion[is.na(cov3$proportion)] <-.25 

### estimate maximum PAR 
cov4<-cov3 %>% group_by(location_id) %>% 
  summarize(max_prop = max(proportion, na.rm = TRUE))

#drop location id 25344, Niger state not Niger country
cov4<-cov4%>%
  filter(location_id!=25344)

##merge maximum with main data
cov5<-merge(cov3,cov4,by="location_id")

##  Keep maixmum population at risk data 

#dataset of maximum PAR by country ID
cov6<-cov5 %>%
  select(NID, age_group_id, max_prop, year_id, location_id)

###--------------FIX SUBNATIONALS ------------------------###

#apply parent max proportions to subnationals that are known endemic

##subnational##

#drop variables and simplify columns
sub2<-subnational %>%
  select(NID, location_name, par_n, year_id, location_id)

#merge with year_ids 
#create list of subnationals
subnats<-unique(sub2$location_id)

pop_gbd_s<-get_population(location_id = subnats, age_group_id = 22,sex_id = c(3), year_id=years, decomp_step = "step3")

#merge population with the location and year_id of the PAR data

sub3<-merge(pop_gbd_s,sub2,by=c("location_id","year_id"),all=TRUE)
table(sub3$year_id)
table(sub3$location_id)


#-------------calculate proportion -----------------------#

sub3$proportion<-sub3$par_n/sub3$population
sub3$proportion[sub3$proportion>1]<-1

#--------------fill time series -----------------

sub4<-sub3%>%
  group_by(location_id) %>%
  fill(proportion)

sub5<-sub4%>%
  group_by(location_id) %>%
  fill(proportion, .direction=c("up"))

#for trachoma endemic countries for which  no PAR data is available we set PAR to 25%
sub5$proportion[is.na(sub5$proportion)] <-.25 

### estimate maximum PAR 
sub6<-sub5 %>% group_by(location_id) %>% 
  summarize(max_prop = max(proportion, na.rm = TRUE))

##merge maximum with main data
sub7<-merge(sub6,sub5,by="location_id")

##  Keep maixmum population at risk data 
#dataset of maximum PAR by country ID
cov6<-cov5 %>%
  select(NID, age_group_id, max_prop, year_id, location_id)


#take location metadata and subset out for parent locations
#add parent id field to sub proportions
sub7$parent_id<-sub7$location_id
sub8<-merge(sub7,locs, by="parent_id")

#drop extra variables
sub9<-sub8 %>%
  select(NID, max_prop, age_group_id, year_id, location_id.y)

colnames(sub9)[colnames(sub9)=="location_id.y"] <- "location_id"

#drop anything not included in TRA endemic list
tras<-as.data.frame(unique_tra_locations)
colnames(tras)[colnames(tras)=="unique_tra_locations"] <- "location_id"
sub9b<-merge(sub9,tras,by="location_id")

###apend subnational and national together

par_cov<-rbind(sub9b,cov6)
par_cov$NID<-NID
colnames(par_cov)[colnames(par_cov)=="max_prop"] <- "mean_value"
par_cov$lower_value<-par_cov$mean_value
par_cov$upper_value<-par_cov$mean_value
par_cov$model_version_id<-1

unique_cov_est<-unique(par_cov$location_id)

#write endemic locations out
for(i in unique_cov_est){
  upload_file<-par_cov[par_cov$location_id==i,]
  upload_file$covariate_id<-ADDRESS
  upload_file$sex_id<-3
    write.csv(upload_file,(paste0("FILEPATH", i,"FILEPATH")))
}

###______________________add non - endemic locations as zeros 

#all location id meta data are sourced here 
locs<-get_location_metadata(location_set_id = 22)

location_list<-unique(locs$location_id)

ne_locs<-location_list[! location_list %in% unique_cov_est]

#create shell dataset
zero<-par_cov[par_cov$location_id==211,]
zero$mean_value<-0
zero$lower_value<-0
zero$upper_value<-0
zero$model_version_id<-1
zero$covariate_id<-ADDRESS
zero$sex_id<-3

for(i in ne_locs){
  
  zero$location_id<-i

  write.csv(zero,(paste0("FILEPATH", i,"FILEPATH")))
}

desc="trachoma max pop at risk, update GR"
save_results_covariate(input_dir = "FILEPATH",
                       input_file_pattern = "{location_id}.csv",
                       covariate_id = ADDRESS, 
                       decomp_step = "iterative", 
                       description = desc,
                       mark_best = TRUE)

