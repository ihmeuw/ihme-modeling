# Age split lead data

rm(list=ls())

#libraries ################################
library(reticulate)
reticulate::use_python("FILEPATH")
splitter <- import("pydisagg.ihme.splitter")

library(readr)
library(data.table)

source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")

# values ##########################
gbd<-"GBD2022"
release<-16 #release id

# Import data ######################################
#get age group ids
ages<-get_age_metadata(release_id = release)[,c("age_group_id","age_group_years_start","age_group_years_end")]

#grab all of the age group ids
age_ids<-ages$age_group_id

## age pattern ###############
pattern<-fread(paste0("FILEPATH/",gbd,"/FILEPATH/age_split_pattern.csv"))

## data ##############
#this is the sex split results
data<-fread(paste0("FILEPATH/",gbd,"/FILEPATH/lead_sex_split.csv"))

#make a column that is the OG seq, so we don't mess up the seq or crosswalk_parent_seq column
data[is.na(seq),seq_og:=crosswalk_parent_seq]
data[is.na(seq_og),seq_og:=seq]

# only keep the data that need to be split
pre_split<-data[!(age_group_id %in% age_ids) | is.na(age_group_id),]

#if the upper age is higher than 125, force it to be 125 (this is because the highest age in the pattern is 125, and the data can't have a higher age)
pre_split[age_end_orig>125, age_end_orig:=125]

#also remove rows where age start and end are equal
pre_split<-pre_split[age_start_orig!=age_end_orig,]


#fill in the standard error column
pre_split[is.na(standard_error) & !is.na(standard_deviation) & !is.na(sample_size), standard_error:=standard_deviation/sqrt(sample_size)]
pre_split[is.na(standard_error) & !is.na(sample_size) & !is.na(variance), standard_error:=sqrt(variance)/sqrt(sample_size)]
pre_split[is.na(standard_error) & !is.na(upper) & !is.na(lower), standard_error:=(upper-lower)/(1.96*2)]


#grab the years and locations in the dataset to get for the population data
years<-unique(pre_split$year_id)
locs<-unique(pre_split$location_id)

## population data ###########
pop<-get_population(release_id = release, year_id = years, age_group_id = "all", location_id = locs,sex_id = c(1,2))

# config ####################################
age_data_config <- splitter$AgeDataConfig(
  index=c("nid", "location_id", "year_id", "sex_id","seq_og"),
  age_lwr="age_start_orig",
  age_upr="age_end_orig",
  val="val",
  val_sd="standard_error"
)

#Will look for column names with "draw_" at the beginning
draw_cols <- grep("^draw_", names(pattern), value = TRUE)

age_pattern_config <- splitter$AgePatternConfig(
  by=list("sex_id"),
  age_key="age_group_id",
  age_lwr="age_group_years_start",
  age_upr="age_group_years_end",
  draws=draw_cols, #Either draw columns OR val & val_sd can be provided
)

age_pop_config <- splitter$AgePopulationConfig(
  index=c("age_group_id", "location_id", "year_id", "sex_id"),
  val="population"
)

# Age Split #####################################

age_splitter <- splitter$AgeSplitter(
  data=age_data_config, pattern=age_pattern_config, population=age_pop_config
)

result <- age_splitter$split(
  data=pre_split,
  pattern=pattern,
  population=pop,
  model="rate", #model can be "rate" or "logodds"
  output_type="rate" #or "count"
)

# Table edits #############################
#only keep the columns that we need from the results table
result<-result[,c("standard_error","nid","val","age_end_orig","seq_og","year_id","sex_id","location_id","age_start_orig","age_group_id","age_split_result","age_split_result_se")]

#change some of the column names
setnames(result,c("val","standard_error","seq_og","age_split_result","age_split_result_se"),
         c("val_og","standard_error_og","crosswalk_parent_seq","val","standard_error"))

# add split data to original table ###########################
# Add all of the missing column that use to be in the original data to the split data
#make a copy of the data table but without the column that are in the results table
data_result<-data[!(age_group_id %in% age_ids) | is.na(age_group_id),-c("standard_error_og","val_og","val","standard_error","location_id","year_id",
                                                            "age_start_orig","age_end_orig","crosswalk_parent_seq","age_group_id")]

#merge by the seq column
data_result<-as.data.table(merge(result,data_result,by.x=c("crosswalk_parent_seq","sex_id","nid"),by.y=c("seq_og","sex_id","nid"),all.x=T))

#if there is anything in the crosswalk_parent_seq then make seq NA
data_result[!is.na(crosswalk_parent_seq),seq:=NA]

#by merging it, the seq column disappears. so we need to add it back in
# data_result[,seq:=crosswalk_parent_seq]

#first remove all rows that would have been in the presplit data, and rows were age start and end are equal, since we will deal with those next
non_split<-data[age_group_id %in% age_ids & !is.na(age_group_id) & age_start_orig!=age_end_orig,]

#Now append the new age split data to the original dataset
final<-rbind(non_split,data_result,fill=T)

# equal age start and end ###################################################
#now fill in the age group ids for the rows where the age start and end are the same

#make a subset that is just the rows where age start and end are the same
age_equal<-data[age_start_orig==age_end_orig,-c("age_group_id")]


#now merge ages to the subset, so we can get the age_group_id that the single ages fall into
age_equal2 <- ages[age_equal, 
                   on = .(age_group_years_start <= age_start_orig, age_group_years_end > age_end_orig)]

#when age start and end are 0, it is not mapping to the correct age group so merging it here
age_equal3<-merge(age_equal2[is.na(age_group_id),-c("age_group_id")],ages[,.(age_group_id,age_group_years_start)],by="age_group_years_start",all.x=T)

#remove the NAs in age_equal2 as these are the rows that are in age_equal3
age_equal2<-age_equal2[!is.na(age_group_id),]

age_equal_final<-rbind(age_equal2,age_equal3,fill=T)

#now rbind the data back into the og and results table
final<-rbind(final,age_equal_final[,-c("age_group_years_start","age_group_years_end")],fill=T)

#remove the age_start and age_end columns, so we can instead fill it in using the age metadata
final<-final[,-c("age_start","age_end")]

#add in the age start and end using the age metadata
final<-merge(final,ages,by="age_group_id",all.x=T)

#change the names for age start and end
setnames(final,c("age_group_years_start","age_group_years_end"),c("age_start","age_end"))

#adjust some of the age_ends as what is in the age metadata does not match up with what epi uploader is looking for
final[, age_end := fifelse(age_end > 1, age_end - 1, 
                          fifelse(age_group_id == 2, 0.01915068493150685,
                                  fifelse(age_group_id == 3, 0.07668493150684931,
                                          fifelse(age_group_id == 388, 0.499,
                                                  fifelse(age_group_id == 389, 0.999, age_end)))))]

#if there is anything in the crosswalk_parent_seq then make seq NA
final[!is.na(crosswalk_parent_seq),seq:=NA]

#calculate standard error
final[!is.na(sample_size) & is.na(standard_error) & !is.na(standard_deviation), standard_error:=(standard_deviation/sqrt(sample_size))]
final[!is.na(upper) & !is.na(lower) & is.na(standard_error),standard_error:=(upper-lower)/(1.96*2)]

# Export #########################################
write_excel_csv(final, paste0("FILEPATH/",gbd,"/FILEPATH/age_split.csv"))


