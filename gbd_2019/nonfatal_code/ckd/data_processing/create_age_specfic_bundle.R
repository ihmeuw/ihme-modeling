# Project: CKD ESRD etiology proportion models
# Purpose: pull all data for given CKD ESRD etiology. subset that data to points
#   with an age range <50 years. upload that subset to the etiology-specifc 
#   ESRD age-split bundle to run a dismod model on it
# Notes: Code gneerates a diagnostic plot -- run interatively to view this;
#   CODE WILL CLEAR THE AGE-SPLIT BUNDLE -- make sure this is what you want 

# setup -------------------------------------------------------------------

j_root<-"FILEPATH"
source("FILEPATH/function_lib.R")
source_shared_functions(c("get_epi_data","upload_epi_data"))
require(openxlsx)
require(ggplot2)


# define function ---------------------------------------------------------

create_age_specific_dataset<-function(input_bundle_id,acause,output_bundle_id,output_filename, upload){
  # pull data from epi db 
  dt<-get_epi_data(input_bundle_id)
  
  # drop data where the age range is > 50 years 
  dt[,age_range:=age_end-age_start]
  as<-dt[age_range<50]
  n_drop<-nrow(dt)-nrow(as)
  print(paste("Dropped",n_drop,"out of",nrow(dt),"rows of data"))
  
  # plot
  # create age-midpoint to label on graph 
  as[,mid_age:=(age_end+age_start)/2]
  gg<-ggplot(data=as)+
    geom_errorbarh(mapping=aes(y=mean, x=mid_age, xmin=age_start, xmax=age_end), height=0.05, size=0.5, color="blue")+
    facet_wrap(c("location_name"))
  print(gg)
  
  if (upload==T){
    # write upload 
    # make sure all columns are present 
    as<-order_epi_cols(dt = as,add_columns = T,delete_columns = T)
    # clear the seq 
    as[,seq:=NA]
    # wipe the processed-data db
    wipe_db(output_bundle_id,acause)
    # write new data as upload
    write_upload(as,output_bundle_id,acause,output_filename)
    # set filepath to upload
    path<-get_epi_path(output_bundle_id,acause,upload = T)
    # upload
    upload_epi_data(output_bundle_id,paste0(path,output_filename,".xlsx"))
  }
}

# set objects  ------------------------------------------------------------

input_bundle_id<-554
acause<-"FILEPATH"
output_bundle_id<-3176
output_filename<-paste0("FILEPATH")
upload<-T

# run ---------------------------------------------------------------------

create_age_specific_dataset(input_bundle_id, acause, output_bundle_id, output_filename, upload)
