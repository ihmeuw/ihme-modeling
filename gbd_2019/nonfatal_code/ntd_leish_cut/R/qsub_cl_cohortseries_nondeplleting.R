arg1 = as.numeric(commandArgs()[4])
i<-arg1

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

#load in saved workspace
load(file = paste0(prefix,'FILEPATH'))

source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))

##############################################################################
#for the presence locations, identify the incident proportion with lifelong sequelae, and add to ongoing cohorts of already written files
#in order to estimate 95+ age_group_id, need to use age_group_id ADDRESS1 from year 1895(!)
complete_year_set<-seq(1895,2017,1)

#create a string of cohort ages that can be used to match years to age groups
aging_df<-expand.grid(full_age_set[3:length(full_age_set)],c(1,1,1,1,1))
aging_string<-as.vector(aging_df[,1])
aging_string<-aging_string[order(aging_string)]
aging_string<-aging_string[-1:-4]
aging_string<-aging_string[-98:-101]

source(sprintf("FILEPATH",prefix))
haq_data<-get_covariate_estimates(covariate_id = ADDRESS, location_id = unique_cl_locations, year_id = 1990:2017)

#string created should have one year in age_group_id ADDRESS1, five years in all other age groups, one year in age_group_id 235


  for (gamma in complete_year_set){
    #load in that year's estimate and subset to incidence
    #If gamma is pre-1990, it loads in 1990
    if(gamma <= 1990){
      reference_frame<-read.csv(paste0(prefix, "FILEPATH",i,"_",1990,".csv"))
    }else{
      reference_frame<-read.csv(paste0(prefix, "FILEPATH",i,"_",gamma,".csv"))
    }

    reference_frame<-subset(reference_frame, reference_frame$measure_id == ADDRESS)
    reference_frame<-subset(reference_frame, reference_frame$age_group_id != 2)
    reference_frame<-subset(reference_frame, reference_frame$age_group_id != 3)

    for (j in 1:nrow(reference_frame)){

      ref_age_group<-reference_frame$age_group_id[j]

      ref_sex<-reference_frame$sex_id[j]
      # 47.6% of incident cases are on the face [and qualify for lifelong sequelae definition]
      # 1-HAQ (as a percentage: in GBD2017 HAQ is max 100) of cases are not treated and therefore have lifelong scarring
      haq_gamma<-gamma
      if(haq_gamma < 1990){haq_gamma <- 1990}
      reference_haq<-haq_data$mean_value[which(haq_data$location_id == i & haq_data$year_id == haq_gamma)]
      ref_rate<-reference_frame[j,8:1007]*0.476*((100-reference_haq)/100)

      aging_cut<-aging_string[which(aging_string >= ref_age_group)]
      if(ref_age_group %in% c(5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,30,31,32) ){aging_cut<-aging_cut[-1:-3]}

      #create a master cohort lookup table - links age_group_id to gamma-th year

      cohort_lookup<-data.frame(year_id = seq(gamma, gamma+length(aging_cut)-1, 1),
                                age_group_id = aging_cut)
      #subset the lookup table to 1990-2017 range

      cohort_lookup<-subset(cohort_lookup, cohort_lookup$year_id > 1989)
      cohort_lookup<-subset(cohort_lookup, cohort_lookup$year_id < 2018)

      #for each estimation year, populate the relevant row

      if (nrow(cohort_lookup) !=0){
        for (m in 1:nrow(cohort_lookup)){
          dataset<-read.csv(paste0(prefix, "FILEPATH",i,"_",cohort_lookup$year_id[m],".csv"))
          addition_age<-cohort_lookup$age_group_id[m]
          #look up the prevalence column
          lookup_row<-which(dataset$age_group_id == addition_age & dataset$sex_id == ref_sex & dataset$measure_id == 5)
          dataset[lookup_row, 8:1007]<-dataset[lookup_row, 8:1007]+ref_rate

          write.csv(dataset, paste0(prefix, "FILEPATH",i,"_",cohort_lookup$year_id[m],".csv"),row.names=FALSE)
        }
      }else{
        #print("No estimate years to add")
      }
    }
    print(paste0("Done year ",gamma))
  }
