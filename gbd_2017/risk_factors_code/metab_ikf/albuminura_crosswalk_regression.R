#---HEADER--------------------------------------------------------------------------------------------------
# Author: 
# Date: 4/20/2017
# Purpose: Crosswalk albuminuria definitions
# Inputs/options: 
#   1. age_standardize: if TRUE, age-standardizes NHANES data 
#   2. in_dir: filepath to directory containing all NHANES datasheets in epi-upload format SAVED AS .CSVs 
#      -- datasheets must have the following variables: nid,year_start,year_end,age_start,age_end,sample_size,
#      mean,standard_error,location_id,modelable_entity_name
#       -- modelable_entity_name should differentiate between definitions
#   3. pattern: character string in filename of all datasheets you want to import -- code will import any
#      file from in_dir that has this string in its name 
#   4. output_file: filpath and file name (.txt) of text file containing betas from regression
#   5. ages: vector of age group ids for GBD age groups 
#   6. pdf_path: filepath and file name (.pdf) for regression plot 
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# clear workspace
rm(list=ls())

# options
age_standardize <- TRUE

# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  h_root <- '/homes/USERNAME/'
} else { 
  j_root <- 'J:/'
  h_root <- 'H:/'
}

# directories and objects
in_dir<-paste0("FILEPATH")
pattern<-"04-21"
output_file<-paste0("FILEPATH",'regression_summaries_test.txt')
central_func_dir<-paste0("FILEPATH")
age_table_path<-paste0("FILEPATH")
ages <- c(2:20,30:32, 235)
pdf_path<-paste0("FILEPATH",'alb_crosswalks.pdf')

# source functions
source(paste0("FILEPATH",'get_ids.R'))
source(paste0("FILEPATH",'get_population.R'))
source(paste0("FILEPATH")) # pulls IHME population weights 

# load packages 
require(data.table)
require(magrittr)
require(ggplot2)
require(uwIntroStats)
#-------------------------------------------------------------------------------------------------------------

# --- LOAD DATA-----------------------------------------------------------------------------------------------
data<-list.files(path=in_dir,pattern=pattern,full.names=T)%>%lapply(.,fread)%>%rbindlist()

# subset to required variables
data<-data[,.(nid,year_start,year_end,age_start,age_end,sample_size,mean,standard_error,location_id,
              modelable_entity_name)]
data<-data[location_id==102]
data[location_id==102,svy:="NHANES"]
data[,svy:=paste(svy,year_start,sep=" ")]
#-------------------------------------------------------------------------------------------------------------

# ---GET POPS AND AGES------------------------------------------------------------------------------------------
# import age table and merge to add age_group_id based on age_start and age_end
age_table <- fread(age_table_path)
data <- merge(data, age_table, by = c("age_start", "age_end"), all.x = T)

# get gbd standard age-weights
age_weights <- as.data.table(get_age_weights())
age_weights <- age_weights[gbd_round_id==4 & age_group_weight_description=="IHME standard age weight",]
age_weights <- age_weights[age_group_id %in% ages,.(age_group_id,age_group_weight_value)]

data <- merge(data, age_weights, by = "age_group_id")
#---------------------------------------------------------------------------------------------------------------

# ---AGE-STANDARDIZE--------------------------------------------------------------------------------------------
if (age_standardize == T) {
  by_vars<-c("location_id","year_start","modelable_entity_name","nid")
  
  # sum age weights for a given location/age/year/ME
  data <- data[, sum := sum(age_group_weight_value), by =by_vars]
  # calculate new age weight 
  data <- data[, new_weight := age_group_weight_value/sum, by = by_vars]
  
  # multiply prev/inc by age weight and sum over age
  # add a column titled "age_std_mean" with the age-standardized mean for the location
  data[, as_mean := mean * new_weight]
  data[, as_mean := sum(as_mean), by = by_vars]
  
  # drop mean column and remove duplicates, so you just have location_id and age_std_mean
  data <- data[, .(nid,year_start,year_end,location_id,modelable_entity_name,as_mean,svy)]
  data <- unique(data)
}
#---------------------------------------------------------------------------------------------------------------

# --- REGRESS + PLOT--------------------------------------------------------------------------------------------
if (age_standardize==F){
  # reshape wide
  data.wide<-dcast.data.table(data = data, nid+svy+age_start+age_end+year_start+year_end+location_id~modelable_entity_name,
                            value.var = c("mean"))
  # reshape long to regress 
  data.long<-melt(data=data.wide,id.vars =c("nid","age_start","age_end","year_start","year_end","location_id","Albuminuria"))
  lm_17_30<-lm(alb_17~Albuminuria,data=data.wide)
  lm_20_30<-lm(alb_20~Albuminuria,data=data.wide)
  lm_25_30<-lm(alb_25~Albuminuria,data=data.wide)
  
  # write ouput file with regression coefficients 
  sink(output_file)
  lm_17_30
  lm_20_30
  lm_25_30
  sink()
  
} else if (age_standardize==T){
  # reshape wide
  data.wide<-dcast.data.table(data = data, nid+svy+year_start+year_end+location_id~modelable_entity_name,
                              value.var = c("as_mean"))
  # reshape long to regress 
  data.long<-melt(data=data.wide,id.vars =c("nid","year_start","year_end","location_id","Albuminuria","svy"))
  lm_17_30<-regress("mean",alb_17~Albuminuria,data=data.wide)
  lm_20_30<-regress("mean",alb_20~Albuminuria,data=data.wide)
  lm_25_30<-regress("mean",alb_25~Albuminuria,data=data.wide)
  
  # write ouput file with regression coefficients
  sink(output_file)
  lm_17_30
  lm_20_30
  lm_25_30
  sink()
}

pdf(pdf_path,width = 11,height = 8.5)

gg<-ggplot(data=data.long,aes(x= Albuminuria,y= value,group=variable,color=variable))+
  geom_smooth(method = lm)+
  geom_point()+
  geom_text(data=data.long[variable=="alb_17"],aes(label=svy),hjust=0.5,vjust=-1,color="black",size=3)+
  theme_bw()+
  scale_colour_brewer(palette = c("BuGn"))
gg

dev.off()
#-------------------------------------------------------------------------------------------------------------


