# ---HEADER--------------------------------------------------------------------------------------------------
# Date: 12/23/2016
# Project: Non-fatal CKD Remission Calculation
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

# load packages 
require(data.table)
require(magrittr)
require(openxlsx,lib.loc = paste0(h_root,"rlibs"))

# source functions
source(paste0(j_root,'FILEPATH'))
source(paste0(j_root,'FILEPATH'))

# set objects 
args<-commandArgs(trailingOnly = TRUE)
out_dir<-args[1]
#-------------------------------------------------------------------------------------------------------------

# ---BODY-----------------------------------------------------------------------------------------------------
#Create list of locations that was parallelized over 
location_dt<- get_location_metadata(location_set_id = 9,gbd_round_id = 4) #location_set_id 9 = locations used in GBD 
location_dt<-location_dt[most_detailed==1,list(location_id,location_name,location_type)] 
locations<-location_dt[,location_id]

file_count<-list.files(out_dir,pattern = "\\.csv$")%>%length()
print(paste(timestamp(),"file count =",file_count))

while (file_count<length(locations)){
  Sys.sleep(60)
  file_count<-list.files(out_dir,pattern = "\\.csv$")%>%length()
  print(paste(timestamp(),"file count =",file_count))
} 

# once all files are present, append them and output .csv
if (file_count==length(locations)){
  file_list<-list.files(out_dir,pattern = "\\.csv$")
  remission<-rbindlist(lapply(paste0(out_dir,'/',file_list),fread))
  #merge on location_name from the location_dt 
  remission<-remission[age_start>1]
  extraction<-merge(remission,location_dt[,-c('location_type'),with=F],by='location_id',all.x=TRUE)
  upload_dir<-paste0(FILEPATH)
  dir.create(upload_dir)
  write.xlsx(extraction[,-c('V1'),with=F],paste0(upload_dir,'extraction.xlsx'),row.names=FALSE,sheetName="extraction")
  upload_epi_data(denom_bundle_id,filepath = paste0(upload_dir,'extraction.xlsx'))
  print("done!")
}
#-------------------------------------------------------------------------------------------------------------
