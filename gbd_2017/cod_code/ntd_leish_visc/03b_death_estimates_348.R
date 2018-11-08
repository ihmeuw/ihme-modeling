# qsub to create fatal estimates for GBD2017 VL - makes cause id = 348 "Visceral leishmaniasis"

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}
load(paste0(prefix, "FILEPATH/workspace.RData"))
source(sprintf("FILEPATH/get_location_metadata.R",prefix))
source(sprintf("FILEPATH/get_demographics.R",prefix))
source(sprintf("FILEPATH/get_population.R",prefix))

arg1 = as.numeric(commandArgs()[4])
i<-arg1

  #pull in cfr, which is time invariant
  if(i %in% presence_locations){

    cfr_file<-cfr_list[[which(required_locations==i)]]
    cfr_file<-cfr_file[,-1:-6]
    for(j in required_years){

      #pull in incidence
      incidence_file<-read.csv(paste0(prefix,"FILEPATH", i,"_",j,".csv"))
      incidence_file<-subset(incidence_file, incidence_file$measure_id==6)
      #pull in demographics
      population_file_male<-get_population(age_group_id=required_ages,
                                           sex_id=1,
                                           location_id=i,
                                           year_id=j)
      population_file_female<-get_population(age_group_id=required_ages,
                                             sex_id=2,
                                             location_id=i,
                                             year_id=j)
      population_file<-rbind(population_file_male, population_file_female)

      incidence_vals<-incidence_file[8:ncol(incidence_file)]
      population_ref<-data.frame(population = population_file$population)
      population_vals<-population_ref
      for(k in 1:1000){
        population_vals[,k]<-population_ref
      }

      #multiply
      output<-incidence_vals*cfr_file*population_vals

      #structure for upload
      upload_file<-incidence_file[2:5]
      upload_file$cause_id<-rep(348, nrow(upload_file))
      upload_file$measure_id<-rep(1, nrow(upload_file))
      upload_file<-cbind(upload_file, output)

      write.csv(upload_file,
                paste0(prefix, "FILEPATH", i,"_",j,".csv"))
    }
  }else{
    for(j in required_years){
#dummy file to make a dim(output) 46, 1000
      upload_file<-read.csv(paste0(prefix,"FILEPATH/491_2017.csv"))
      upload_file<-subset(upload_file, upload_file$measure_id==6)
      output<-upload_file[8:ncol(upload_file)]*0
      upload_file<-upload_file[,2:6]
      upload_file$location_id<-rep(i, nrow(upload_file))
      upload_file$year_id<-rep(j, nrow(upload_file))
      upload_file$cause_id<-rep(348, nrow(upload_file))
      upload_file$measure_id<-rep(1, nrow(upload_file))

      upload_file<-cbind(upload_file, output)

      write.csv(upload_file,
                paste0(prefix, "FILEPATH", i,"_",j,".csv"))
    }
  }
