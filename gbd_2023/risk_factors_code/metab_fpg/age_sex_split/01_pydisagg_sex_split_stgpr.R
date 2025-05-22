################################################################################
#Purpose: Date prep - Sex splitting using pydisagg (for STGPR BUNDLES)
#Date: Aug 23 2024
#Description: age splitting using age pattern and pydisagg beta

################################################################################

rm(list = ls())
user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())

#other set up
library(data.table)
library(ggplot2)
library(openxlsx)
library(dplyr)
invisible(sapply(list.files('FILEPATH', full.names = T), source))
output_path<- 'FILEPATH'

#set up pydisagg
library(reticulate)
reticulate::use_python('FILEPATH')
splitter <-import('FILEPATH')

#load metadata
info<-get_demographics(gbd_team="epi",release_id = 16)
age_ids<-unique(info$age_group_id)

age<-get_age_metadata(release_id = 16) 
age<-age %>% select(age_group_id, age_group_years_start, age_group_years_end)

loc<-get_location_metadata(release_id=16, location_set_id=35)
region<-loc %>% select(location_id, region_id,region_name) %>% distinct()
super_region_name<-loc %>% select(super_region_id, super_region_name) %>% distinct()

# LOAD IN THE DATA FOR SPLITTING------------------------------------------------

#Data frames

#data to split
df<- read.csv('FILEPATH')
n_distinct(df$nid)
df<-as.data.table(df)
unique(df$sex)
df<-df[,midage:=round((age_start+age_end)/2,digit=0)]
no_split<-df[sex!="Both",]
# any(df$midage==0) # not applicable to fpg data 
# df<-df[midage==0, midage:=0.5]
df_tosplit<-df[sex=="Both"]


#population
ages <- get_ids("age_group")
pops <- get_population(location_id="all", year_id=unique(df_tosplit$year_id), release_id=16, sex_id=c(1,2), single_year_age=T, age_group_id="all")
pops<-left_join(pops, ages, by="age_group_id")
pops<-as.data.table(pops)
pops[age_group_name == "<1 year", age_group_name := 0.5]
pops[age_group_name == "95 plus", age_group_name := 97]
pops[age_group_name == "12 to 23 months", age_group_name := 1.5]

unique(pops$age_group_name)
pops$midage<-pops$age_group_name
pops$age_group_name<-NULL
pops$midage<-as.numeric(pops$midage)

#LOAD in sex ratio 
pat_all<-read.csv('FILEPATH')
pat_all<-pat_all[,se:=(pred1_hi-pred1)/1.96]

# configure input file columns ------------------------------------------------

#data to be split 
colnames(df_tosplit)
df_tosplit<-df_tosplit[sex=="Both", sex_id:=3]
unique(df_tosplit$sex_id)

df_tosplit$id<-1:nrow(df_tosplit)
df_tosplit<-df_tosplit[is.na(standard_error),standard_error:=sqrt(variance)]
save_col<-df_tosplit %>% select(-nid, -seq, -location_id, -year_start, -year_end, -year_id, -sex_id, -age_start, -age_end, -midage,
                                -upper, -lower, -standard_deviation, - variance, -val)

any(colnames(pops)=="midage")

# set up all config
sex_splitter = splitter$SexSplitter(
  data=splitter$SexDataConfig(
    index=c("nid","seq", "id","location_id", "year_id", "sex_id","age_start","age_end", "midage","year_start", "year_end"),
    val="val",
    val_sd="standard_error"
  ),
  pattern=splitter$SexPatternConfig(
    by=list('midage'),
    val='ratio',
    val_sd='se'
  ),
  population=splitter$SexPopulationConfig(
    index=c('location_id', 'year_id', 'midage'),
    sex='sex_id',
    sex_m=1,
    sex_f=2,
    val='population'
  )
)

#---- RUN MODEL -----------------
#Model can be "rate" or "logodds"
#Output type should stay "rate" (for now)
result <- sex_splitter$split(
  data=df_tosplit,
  pattern=pat_all,
  population=pops,
  model="rate",
  output_type="rate")


n_distinct(df_tosplit$nid)
nrow(df_tosplit)
n_distinct(result$nid)
result<-as.data.table(result)
colnames(result)
