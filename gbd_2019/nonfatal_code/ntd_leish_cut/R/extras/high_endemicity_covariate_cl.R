#Code to make CL high endemicity covariate

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

#load shared functions
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))
source(sprintf("FILEPATH",prefix))

#load in the dataset - 
dataset<-read.csv(sprintf("FILEPATH", prefix),
                  stringsAsFactors = FALSE)

#trim to be just nid, location_id, year_start, year_end, cases

dataset<-data.frame(nid=dataset$nid,
                    location_id=dataset$location_id,
                    cases=dataset$cases,
                    year_start=dataset$year_start,
                    year_end=dataset$year_end)

#drop the zeroes. 
dataset<-subset(dataset,dataset$cases>0)
#years must be post 1979
dataset<-subset(dataset,dataset$year_start>1979)

#load in geographic restrictions as a template
gr_cl<-read.csv(sprintf("FILEPATH", prefix),
                stringsAsFactors = FALSE)

#consider the gr_cl as three separate but overlapping matrices

endemicity_cl_status<-subset(gr_cl, gr_cl$type=="status")
endemicity_cl_citation<-subset(gr_cl, gr_cl$type=="citation")
endemicity_cl_quality<-subset(gr_cl, gr_cl$type=="quality")

#do a grid search for the relevant location and rewrite the relevant fields with appropriate data
subset_check<-data.frame()
row_ref<-NA
col_ref<-NA
for (i in 1:nrow(dataset)){
  row_ref[i]<-which(endemicity_cl_status$loc_id==dataset$location_id[i])

  #given column 12 = 1980, 19XX = column 12+ (19XX-1980)
  col_ref[i]<-12+(dataset$year_start[i]-1980)

  #check if is maximum value
  subset_check<-subset(dataset, dataset$location_id==dataset$location_id[i] & dataset$year_start==dataset$year_start[i])
  if(nrow(subset_check)>1){
    endemicity_cl_status[row_ref[i], col_ref[i]]<-max(subset_check$cases)
    endemicity_cl_citation[row_ref[i], col_ref[i]]<-dataset$nid[which(dataset$cases==max(subset_check$cases))[1]]
    endemicity_cl_quality[row_ref[i], col_ref[i]]<-'b2'
  }else{
    endemicity_cl_status[row_ref[i], col_ref[i]]<-dataset$cases[i]
    endemicity_cl_citation[row_ref[i], col_ref[i]]<-dataset$nid[i]
    endemicity_cl_quality[row_ref[i], col_ref[i]]<-'b2'
  }
}


ULoc<-unique(endemicity_cl_status$loc_id)
country_key<-data.frame(loc_id=ULoc, location_name=endemicity_cl_status$loc_name, median_value=rep(NA, length(ULoc)), year_2017<-endemicity_cl_status$X2017)
melt_status<-melt(endemicity_cl_status, id.var=c(1:10))
subset_country<-list()
for (i in 1:length(ULoc)){
  subset_country[[i]]<-subset(melt_status, melt_status$loc_id==ULoc[i])
  country_key$median_value[i]<-median(na.omit(as.numeric(subset_country[[i]]$value)))
}

#take the melt dataframe and generate the binary 0/1 with the toggled threshold value
threshold_value<-15
#drop the pre1980 fields
melt_status<-subset(melt_status, melt_status$variable!="pre1980")
melt_status$variable<-as.numeric(substr(melt_status$variable,2,5))

covariate_dataset<-data.frame(location_id=melt_status$loc_id,
                              year_id=melt_status$variable,
                              mean_value=rep(NA, nrow(melt_status)),
                              lower_value=rep(NA, nrow(melt_status)),
                              upper_value=rep(NA, nrow(melt_status)),
                              age_group_id=rep(22, nrow(melt_status)),
                              sex_id=rep(3,nrow(melt_status))
)

for (i in 1:nrow(melt_status)){
  if(is.na(as.numeric(melt_status$value[i]))){

    if(melt_status$value[i]=="a"){
      covariate_dataset$mean_value[i]<-0
      covariate_dataset$lower_value[i]<-0
      covariate_dataset$upper_value[i]<-0
    }
    if(melt_status$value[i]=="pa"){
      covariate_dataset$mean_value[i]<-0
      covariate_dataset$lower_value[i]<-0
      covariate_dataset$upper_value[i]<-0
    }
    if(melt_status$value[i]=="pp"){
      if(is.na(country_key$median_value[which(country_key$loc_id==melt_status$loc_id[i])])){
        covariate_dataset$mean_value[i]<-0
        covariate_dataset$lower_value[i]<-0
        covariate_dataset$upper_value[i]<-0
      }else{
        if(country_key$median_value[which(country_key$loc_id==melt_status$loc_id[i])]>=threshold_value){
          covariate_dataset$mean_value[i]<-1
          covariate_dataset$lower_value[i]<-1
          covariate_dataset$upper_value[i]<-1
        }else{
          covariate_dataset$mean_value[i]<-0
          covariate_dataset$lower_value[i]<-0
          covariate_dataset$upper_value[i]<-0
        }}
    }
    if(melt_status$value[i]=="p"){
      if(is.na(country_key$median_value[which(country_key$loc_id==melt_status$loc_id[i])])){
        covariate_dataset$mean_value[i]<-0
        covariate_dataset$lower_value[i]<-0
        covariate_dataset$upper_value[i]<-0
      }else{
        if(country_key$median_value[which(country_key$loc_id==melt_status$loc_id[i])]>=threshold_value){
          covariate_dataset$mean_value[i]<-1
          covariate_dataset$lower_value[i]<-1
          covariate_dataset$upper_value[i]<-1
        }else{
          covariate_dataset$mean_value[i]<-0
          covariate_dataset$lower_value[i]<-0
          covariate_dataset$upper_value[i]<-0
        }}
    }
  }else{
    if(as.numeric(melt_status$value[i])>=threshold_value){
      covariate_dataset$mean_value[i]<-1
      covariate_dataset$lower_value[i]<-1
      covariate_dataset$upper_value[i]<-1
    }
    if(as.numeric(melt_status$value[i])<=threshold_value){
      covariate_dataset$mean_value[i]<-0
      covariate_dataset$lower_value[i]<-0
      covariate_dataset$upper_value[i]<-0
    }
  }
}


#filtered GR to pp/p then looked at published literature (Alvar et al. 2012) to assign 1 to subnationals with highly endemic

for (j in 1:nrow(covariate_dataset)){
  if(covariate_dataset$location_id[j] %in% c(4868,#india Rajasthan
                                             #
                                             44852, 44854, 44861,44855, #Ethiopia Tigray, Amhara, Addis, Oromiya
                                             4460, 4672, 4662, 4649, 4665, 4646, 4669, #Mexico Nayarit, Veracruz_llAve, Oaxaca, Chiapas, Quintana Roo, Campeche, Tabasco
                                             # Kenya none
                                             44864:44894) #
  ){
  covariate_dataset$mean_value[j]<-1
  covariate_dataset$lower_value[j]<-1
  covariate_dataset$upper_value[j]<-1
  }
}

df<-data.frame()
#check for Kenya Regions - for CL all are zero
if(44800 %in% unique(covariate_dataset$location_id)){

}else{
  for (k in 44793:44800){
  df<-data.frame(location_id=rep(k,length(1980:2017)),
             year_id=c(1980:2017),
             mean_value=rep(0,length(1980:2017)),
             lower_value=rep(0,length(1980:2017)),
             upper_value=rep(0,length(1980:2017)),
             age_group_id=rep(22,length(1980:2017)),
             sex_id=rep(3,length(1980:2017)))
  covariate_dataset<-rbind(covariate_dataset,df)
  }
}


write.csv(covariate_dataset,
          sprintf("FILEPATH", prefix))

save_results_covariate(input_dir=sprintf("FILEPATH",prefix),
                       "FILEPATH",
                       covariate_id=ADDRESS,
                       description="GBD2017 first upload - CL ONLY")
