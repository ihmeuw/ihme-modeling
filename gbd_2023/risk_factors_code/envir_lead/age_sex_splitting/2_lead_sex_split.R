# Sex split lead data

rm(list=ls())

#libraries ################################
library(reticulate)
reticulate::use_python("FILEPATH")
splitter <- import("pydisagg.ihme.splitter")

library(readr)
library(data.table)

source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")

# values ##########################
gbd<-"GBD2022"
b_version <- 49239 #bundle version
release<-16 #release id

# Import data ######################################
## Sex pattern ###############
sex_pattern<-fread(paste0("FILEPATH/",gbd,"/FILEPATH/lead_sex_pattern.csv"))

#grab the draw columns
draw_cols <- grep("^draw_", names(sex_pattern), value = TRUE)

## bundle data ##############
data <- get_bundle_version(bundle_version_id = b_version, fetch = "all")

#if the original age_start and end column is NA, Fill it in with the age_start and age_end columns
data[is.na(age_start_orig),age_start_orig:=age_start]
data[is.na(age_end_orig),age_end_orig:=age_end]

## mean_type ###########################
#fill in the mean_type column
#Arithmetic mean
data[mean_type == "am_mean" | cv_arth_mean==1 | cv_mean_unk==1, ':='(am_mean = 1, mean_type="am_mean")] #if the mean_type is ="am_mean", make a column called am_mean and fill it in with 1s
data[is.na(am_mean), am_mean := 0] #if any of cells in the am_mean column are na, replace it with a 0

#Geometric mean
data[mean_type == "gm_mean" | cv_geomean==1, ':='(gm_mean = 1, mean_type="gm_mean")]
data[is.na(gm_mean), gm_mean := 0]

#median
data[mean_type == "median" | cv_median==1, ':='(median = 1, mean_type="median")]
data[is.na(median), median := 0]

#merge on sex ids
sex<-get_ids("sex")
data<-merge(data,sex,by="sex",all.x = T)

#only keep both sex
pre_split<-data[sex_id==3,]

#fill in the standard deviation column
pre_split[is.na(standard_deviation),standard_deviation:=sqrt(variance)]

#only keep the required columns
pre_split<-pre_split[,c("nid","seq","location_id","year_id","sex_id","age_start_orig","age_end_orig","standard_deviation","val")]

#grab the years and locations in the dataset to get for the population data
years<-unique(pre_split$year_id)
locs<-unique(pre_split$location_id)

## population data ###########
pop<-get_population(release_id = release, year_id = years, sex_id = "all", location_id = locs)


# Configs ########################################
#fill out the configs for pydisagg

sex_splitter = splitter$SexSplitter(
  data=splitter$SexDataConfig(
    index=c("nid","seq", "location_id", "year_id", "sex_id","age_start_orig","age_end_orig"),
    val="val",
    val_sd="standard_deviation"
  ),
  # The SexPattern is anticipating a female/ male ratio for val, val_sd or draws
  pattern=splitter$SexPatternConfig(
    by=list('year_id'),
    draws=draw_cols
  ),
  population=splitter$SexPopulationConfig(
    index=c('location_id', 'year_id'),
    sex="sex_id",
    sex_m=1,
    sex_f=2,
    val='population'
  )
)

# sex split ##############################
# preform sex split
result <- sex_splitter$split(
  data=pre_split,
  pattern=sex_pattern,
  population=pop,
  model="rate", #model can be "rate" or "logodds"
  output_type="rate" #or "count"
)

# edit table #########################
#remove columns that we don't need anymore
result<-result[,c("nid","seq","location_id","year_id","sex_id","age_start_orig","age_end_orig","standard_deviation","val","sex_split_result","sex_split_result_se")]

#change column names
setnames(result,c("seq","standard_deviation","val","sex_split_result","sex_split_result_se"),
         c("crosswalk_parent_seq","standard_deviation_og","val_og","val","standard_error"))

# Add all of the missing column that use to be in the original data to the split data
#make a copy of the data table but without the column that are in the results table
data_result<-data[sex_id==3,-c("standard_deviation_og","val_og","val","standard_error","nid","location_id","year_id","sex_id","sex","age_start_orig","age_end_orig")]

#merge by the seq column
data_result<-as.data.table(merge(result,data_result,by.x=c("crosswalk_parent_seq"),by.y=("seq"),all.x=T))

#add sex ids back in
data_result<-merge(data_result,sex,by="sex_id",all.x=T)

# merge with the original dataset
data<-data[sex_id!=3,]

data<-rbind(data,data_result,fill=T)

# export ##########################
write_excel_csv(data,paste0("FILEPATH/",gbd,"/FILEPATH/lead_sex_split.csv"))






