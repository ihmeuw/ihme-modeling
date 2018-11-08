# ---HEADER------------------------------------------------------------------------------------------------
# Date: 1/2/2018
# Purpose: Outlier hospital data based on mean absolute deviation 
# Inputs: Data download sheet 
# Process: age-standardize, calculate median and median absolute deviation, outlier any country/year/sex that 
# is >= 2 MAD from the median 
#----------------------------------------------------------------------------------------------------------
function_dir<-paste0(h_root,"FILEPATH")
invisible(lapply(list.files(function_dir,full.names = T),source))

outlier_hospital<-function(acause,bundle,method,margin,haqi_scalar,compare_claims_hosp,crosswalk,delete_prev_data,output_bundle){
  # source functions
  source(paste0(h_root,"FILEPATH/function_lib.R"))
  source_shared_functions(c("get_epi_data","upload_epi_data","get_location_metadata"))
  #--------------------------------------------------------------------------------------------------------
  
  datasheet <- get_epi_data(bundle_id = bundle)
  #---------------------------------------------------------------------------------------------------------
  
  # ---PROCESS----------------------------------------------------------------------------------------------
  # Assess outliers using specified method 
  if (method=="mad"){
    datasheet<-mad_analysis(datasheet,margin)
  }
  if (crosswalk==1){
    datasheet<-oodm_xwalk(acause, bundle, datasheet)
  }
  #---------------------------------------------------------------------------------------------------------
  
  # ---OUTPUT=----------------------------------------------------------------------------------------------
  # Add necessary columns, order columns, and upload 
  date<-gsub("-","_",Sys.Date())
  cv_cols<-grep("cv_",names(datasheet),value=T)
  datasheet<-order_epi_cols(dt = datasheet,cv_cols = cv_cols, add_columns = T, delete_columns = T)
  filepath<-paste0(j_root, "FILEPATH/", bundle, "_", date,"_",margin,"_",method,"_outliers_",modification,".xlsx")
  print(paste("writing outlier sheet as",filepath))
  write.xlsx(datasheet,filepath,sheetName = "extraction",showNA = F, row.names = F)
  #---------------------------------------------------------------------------------------------------------
}
