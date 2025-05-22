################################################################################
#Purpose: Date prep - Age splitting using pydisagg (FPG STGPR SPECIFIC)
#Date: June 8 2024
#Description: age splitting using age pattern and pydisagg beta

################################################################################

rm(list = ls())
user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())

#set up pydisagg
library(reticulate)
reticulate::use_python('FILEPATH')
splitter <-import("pydisagg.ihme.splitter")


#other set up
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)

  invisible(sapply(list.files('FILEPATH', full.names = T), source))
  output_path<- 'FILEPATH'


# LOAD IN THE DATA FOR SPLITTING------------------------------------------------

#Data frames for this example.

#   pops: comes from demographic team get_pop function (updated to 2021)
#   fpg_df: dismod output to be split
#   fpg_pats: age pattern for disease (from get_draws)
  #population
  pops_orig <- get_population(location_id="all", year_id="all", release_id=16, sex_id=c(1,2), single_year_age=F, age_group_id="all")
    fpg_df_orig<-as.data.table(read.csv('FILEPATH'))
  
  n_distinct(fpg_df_orig$nid) #30
  
  #pattern
  fpg_pats_orig <- get_draws( gbd_id_type = "modelable_entity_id", 
                      source="epi",
                      gbd_id = 8909,
                      release_id=16,
                      year_id=2015,
                      age_group_id=c(10:20, 30:32, 235),
                      location_id=1,
                      sex_id=c(1,2),
                      version_id=212350)
  

  pops<-copy(pops_orig)
  fpg_df<-copy(fpg_df_orig)
  fpg_pats<-copy(fpg_pats_orig)
  
# configure input file columns
  # A breakdown of all the variables are in the full user guide
  
  #data to be split 
  colnames(fpg_df)
  fpg_df<-as.data.table(fpg_df)
  fpg_df<-fpg_df[,standard_deviation:=sqrt(variance)]
  any(colnames(fpg_df)=="standard_deviation")
  check<-fpg_df[standard_deviation==0,]
    #drop the one with 0 because samplesize ==1 
    fpg_df<-fpg_df[!(standard_deviation==0 & nid== 30131),]
    #temp fix for extraction
    fpg_df<-fpg_df[standard_deviation==0 & nid== 200160, `:=`(standard_deviation=0.001, note_sr="reported sd is 0, so code sd to 0.001")]
    
  fpg_df<-fpg_df[sex=="Female", sex_id:=2]
  fpg_df<-fpg_df[sex=="Male", sex_id:=1]
  fpg_df<-fpg_df[sex=="Both", sex_id:=3]
  fpg_df<-fpg_df[sex!="Both",]
  drop<-fpg_df[age_end<=25,]
  n_distinct(drop$nid) # 129
  unique(drop[,.(age_start,age_end)])
  write.csv(drop, paste0(output_path,"age/fpg_pydisagg/2024_09_04/drop_age_end_lessthan25.csv"))
  fpg_df<-fpg_df[age_end>25,]
  fpg_df<-fpg_df[age_start<25, `:=`(note_modeler=paste0(note_modeler,";age start hard coded to 25, orig age start is ", orig_age_start), age_start=25)]
  unique(fpg_df[,.(age_start,age_end)])
  n_distinct(fpg_df$nid) #679
  fpg_df$id<-1:nrow(fpg_df)
  save_col<-fpg_df %>% select(-nid, -seq, -location_id, -year_id, -sex_id, -age_start, -age_end, -age_group_id,
                              -variance, -upper, -lower, - standard_deviation,-val)
  
  #age_pattern column prep
  #Will look for column names with "draw_" at the beginning
  draw_cols <- grep("^draw_", names(fpg_pats), value = TRUE)
  
  fpg_pats<-as.data.table(fpg_pats)
  colnames(fpg_pats)
  age<-get_age_metadata(release_id = 16)
  fpg_pats<-left_join(fpg_pats,age, by="age_group_id")
  
  # populatin column prep
  colnames(pops)
  pops<-left_join(pops,age,by="age_group_id")
  
  # set up config
  age_data_config <-splitter$AgeDataConfig(
    index=c("nid","seq","id", "location_id", "year_id", "sex_id"),
    age_lwr="age_start",
    age_upr="age_end",
    val="val",
    val_sd="standard_deviation"
  )
  
  age_pattern_config <- splitter$AgePatternConfig(
    by=list("sex_id"),
    age_key="age_group_id",
    age_lwr="age_group_years_start",
    age_upr="age_group_years_end",
    draws=draw_cols)
  

  
  age_pop_config <- splitter$AgePopulationConfig(
    index=c("age_group_id", "location_id", "sex_id","year_id"),
    val="population"
  )
  
  age_splitter <- splitter$AgeSplitter(
    data=age_data_config, pattern=age_pattern_config, population=age_pop_config
  )
  
 #Model can be "rate" or "logodds"
  #Output type should stay "rate" (for now)
  result <- age_splitter$split(
    data=fpg_df,
    pattern=fpg_pats,
    population=pops,
    model="rate",
    output_type="rate"
  )
  
