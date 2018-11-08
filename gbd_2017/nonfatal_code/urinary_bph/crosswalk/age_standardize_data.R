# 1/12/2018

# Function to age standardize data -- must have a column named mean, age_start, age_end 
age_standardize_data<-function(datasheet,byvars= c("location_id", "sex", "year_start", "year_end", "nid"),mean_col="mean"){
  # ---SETUP-----------------------------------------------------------------------------------------------
  # set runtime configuration 
  if (Sys.info()['sysname'] == 'Linux') {
    h_root <- 'FILEPATH'
  } else { 
    h_root <- 'FILEPATH'
  }
  
  #load packages
  require(pacman)
  p_load(data.table,RMySQL)
  
  # source functions
  source(paste0(h_root,"FILEPATH/function_lib.R"))
  source(paste0(h_root,'FILEPATH/age_weights_R.r'))
  
  # standardize input 
  datasheet<-as.data.table(datasheet)
  #--------------------------------------------------------------------------------------------------------
  
  # ---ANALYZE----------------------------------------------------------------------------------------------
  # get age map and merge onto datasheet 
  print("getting age map")
  ages<-get_age_map(5,add_under_1=T)
  
  # if age_start and age_end exist in data 
  if (all(c("age_start","age_end")%in%names(datasheet))){
    # make terminal age group consistent 
    datasheet[age_start==95,age_end:=99]
    if ("age_group_id"%in%names(datasheet)){
      if (all(is.na(datasheet[,age_group_id]))==T){
        datasheet[,age_group_id:=NULL]
      }
    }
  }
  
  # merge on age_group_ids 
  if (!"age_group_id"%in%names(datasheet)){
    datasheet<-merge(datasheet,ages,by=c("age_start","age_end"),all.x=T)
  }
  
  # calculate age-standardized prevalence/incidence
  # get gbd standard age-weights & subset to age groups that apply to cause
  print("getting age weights")
  weights<-get_age_weights()[gbd_round_id==5]
  weights<-weights[age_group_id %in% unique(datasheet[,age_group_id]),] 
  datasheet<-datasheet[age_group_id%in%unique(weights[,age_group_id])]
  datasheet <- merge(datasheet, weights[,.(age_group_id,age_group_weight_value)], by = "age_group_id",all.x=T)
  
  #create new age weights for each data source
  datasheet[, sum := sum(age_group_weight_value), by = byvars]
  datasheet[, new_weight := age_group_weight_value/sum, by = byvars]
  
  #multiply prev/inc by age weight and sum over age
  datasheet[, as_mean := get(mean_col) * new_weight]
  datasheet[, as_mean := sum(as_mean), by = byvars]
  
  return(datasheet)
}
