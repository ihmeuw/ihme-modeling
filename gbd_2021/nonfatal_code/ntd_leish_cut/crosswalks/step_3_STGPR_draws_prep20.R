# NTDS: Cutaneous leishmaniasis
# Purpose: create estimates from ST-GPR inputs 
# Description: copy ST/GPR results from CL incidence model into location-specific folders


#pull in geographic restrictions and drop non-endemic countries and drop restricted locations
#output draws to folder by location
cl_geo<-read.csv("FILEPATH.csv")

cl_geo<-cl_geo[cl_geo$most_detailed==1,]
cl_geo<-cl_geo[cl_geo$year_start==2019,]
cl_geo<-cl_geo[cl_geo$value_endemicity==1,]

#list of unique CL endemic locations
unique_cl_locations<-unique(cl_geo$location_id)

#####write st/gpr draws out to location specific csv files - change ST-GPR model version as needed

for(i in unique_cl_locations){
  #pull in single country and spit out
  upload_file<-read.csv(paste0("FILEPATH",i,".csv"))
  upload_file$model_id<-"ADDRESS"
  upload_file$measure_id<-6


  write.csv(upload_file,(paste0("FILEPATH", i,".csv")))  
}
