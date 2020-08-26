#Covariate creation from geographic restrictions formatted template

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

#load shared functions
source(sprintf("FILEPATH",prefix))


#import file

dataset<-read.csv(sprintf("FILEPATH",prefix))

#subset to those with type='status'
dataset<-subset(dataset, dataset$type=='status')

#trim to loc_id, and, years 1980-2017
dataset<-dataset[,c(1,12:49)]

#melt data to long form
core_data<-melt(dataset,
                id.vars="loc_id")

#coerce "variable" to be a character called year_id and remove X tag

core_data$year_id<-as.numeric(substr(core_data$variable,2,5))

#translate the p, pa, pp, a tags to 0,1 values

for (i in 1:nrow(core_data)){
  if (core_data$value[i]=='p'){
    core_data$mean_value[i]<-1
    core_data$lower_value[i]<-1
    core_data$upper_value[i]<-1
  }
  if (core_data$value[i]=='pp'){
    core_data$mean_value[i]<-1
    core_data$lower_value[i]<-1
    core_data$upper_value[i]<-1
  }
  if (core_data$value[i]=='pa'){
    core_data$mean_value[i]<-0
    core_data$lower_value[i]<-0
    core_data$upper_value[i]<-0
  }
  if (core_data$value[i]=='a'){
    core_data$mean_value[i]<-0
    core_data$lower_value[i]<-0
    core_data$upper_value[i]<-0
  }
}

#trim to loc_id (renamed "location_id"), year_id and estimates
names(core_data)[1]<-"location_id"
core_data<-core_data[,c(1,4:7)]

#add covariate_id column
core_data$covariate_id<-rep(212, nrow(core_data))
core_data$age_group_id<-rep(22, nrow(core_data))
core_data$sex_id<-rep(3, nrow(core_data))


#save as a csv
write.csv(core_data, row.names=FALSE,
          sprintf("FILEPATH",prefix))

#test upload
save_results_covariate(input_dir=sprintf("FILEPATH",prefix),
                       "FILEPATH",
                       covariate_id=212,
                       description="GBD2017 first revision - CL ONLY")
