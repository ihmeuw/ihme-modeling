################################################################################
#Purpose: Date prep - mean FPG<-> Diabetes Prevalence crosswalk
#Date: Feb 16, 2024
#Description:
#crosswalk 

#NOTE:  
################################################################################

rm(list = ls())
user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())

library(dplyr)
library(ggplot2)
invisible(sapply(list.files,'FILEPATH', full.names = T), source))


#get crosswalking function
source('FILEPATH')
# crosswalk_version<-43538
 check<-get_crosswalk_version(46181)
 to_cw<- check %>% filter(diabetes_fpg_crosswalk==1 & val!=0 & age_end>=25)
 n_distinct(to_cw$nid)

# CROSSWALING DM TO FPG---------------------------------------------------------

dir.create(paste0('FILEPATH',date,"/"))
xwalk<-diabetes_fpg_xwalk(fpg_ver= 43497, 
                          db_ver=43538,
                          function_val="db_to_fpg",
                          output_path=paste0('FILEPATH',date,"/"))



final<- xwalk$x_walk #so only rows with diabetes_fpg_crosswalk ==1?
