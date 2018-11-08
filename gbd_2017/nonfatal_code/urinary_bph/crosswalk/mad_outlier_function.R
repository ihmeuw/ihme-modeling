# 1/12/2018

mad_analysis<-function(datasheet, margin){
  # set runtime configuration 
  if (Sys.info()['sysname'] == 'Linux') {
    h_root <- 'FILEPATH'
  } else { 
    h_root <- 'FILEPATH'
  }

  # source functions
  source(paste0(h_root,"FILEPATH/age_standardize_data.R"))

  # subset sheet to only hospital data to calculate m.a.d and assign outliers 
  nonhospital_data<-copy(datasheet[cv_hospital!=1])
  datasheet<-datasheet[cv_hospital==1]
  
  #age standardize data 
  datasheet<-age_standardize_data(datasheet)
  
  #log transform mean
  datasheet[!as_mean == 0, as_mean := log(as_mean)]
  datasheet[as_mean == 0, is_outlier := 1]
  
  # calculate median aboluste deviation
  print("calculating median absolute deviation")
  datasheet[as_mean == 0, as_mean := NA] # don't count zeros in median calculations
  datasheet[,mad:=mad(as_mean,na.rm = T),by=c("sex")]
  datasheet[,median:=median(as_mean,na.rm = T),by=c("sex")]
  
  # assign outliers based on m.a.d. -- set "margin" in config (ie. outlier == 2x m.a.d or 3x m.a.d., etc.)
  datasheet[as_mean>((margin*mad)+median), is_outlier := 1]
  datasheet[as_mean<(median-(margin*mad)), is_outlier := 1]
  
  # document outliering 
  if (any(grepl("note_modeler",names(datasheet)))==T){
    datasheet[as_mean>((margin*mad)+median), note_modeler := ifelse(is.na(note_modeler),
                                                                    paste("outliered because is higher than", margin, "MAD above median"),
                                                                    paste0(note_modeler, " | outliered because is higher than ",margin," MAD above median"))]
    datasheet[as_mean<(median-(margin*mad)), note_modeler := ifelse(is.na(note_modeler),
                                                                    paste("outliered because is lower than", margin, "MAD below median"),
                                                                    paste0(note_modeler, " | outliered because is lower than ",margin," MAD below median"))]
  }else{
    datasheet[as_mean>((margin*mad)+median), note_modeler :=paste("outliered because is higher than", margin, "MAD above median")]
    datasheet[as_mean<(median-(margin*mad)), note_modeler :=paste("outliered because is lower than", margin, "MAD below median")]
  }
  datasheet[, c("sum", "new_weight", "as_mean", "median", "mad", "age_group_weight_value") := NULL]
  print("assigned outliers based on m.a.d.")
  
  # Add back in non-hospital data 
  datasheet<-rbindlist(list(datasheet,nonhospital_data),use.names = T,fill=T)
  
  return(datasheet)
}
