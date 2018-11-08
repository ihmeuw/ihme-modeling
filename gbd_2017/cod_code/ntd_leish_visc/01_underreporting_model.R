########################################################
########################################################

##	Prep R

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}

##	Load shared function
library(RColorBrewer)
source(sprintf("FILEPATH/get_demographics.r",prefix))
source(sprintf("FILEPATH/get_covariate_estimates.r",prefix))
source(sprintf("FILEPATH/get_location_metadata.r",prefix))
source(sprintf("FILEPATH/get_population.r",prefix))

ilogit <- function(x)1/(1+exp(-x))

##	Set up directories

in_dir <- sprintf("FILEPATH",prefix)
tmp_in_dir <- sprintf("FILEPATH",prefix)
code_dir <- sprintf("FILEPATH",prefix)
underreport_dir <- sprintf("FILEPATH",prefix)

experiment_name<-"full_model_90split10_27Feb2017"


##	Create underreporting model

haqi <- get_covariate_estimates(1099)
sdi <- get_covariate_estimates(881)
leish_presence <- get_covariate_estimates(211)
leish_endemic_vl <- get_covariate_estimates(213)
leish_endemic_cl <- get_covariate_estimates(192)

underreport_raw <- read.csv(sprintf("%s/underreporting_status_leish_v4.csv",in_dir))

n_reps<-1000
underreport_list<-list()
year_string<-NA
proportion_string<-NA
proportion_error<-NA
higher_bound<-NA
lower_bound<-NA
proportion_sample<-NA
std_dev<-NA
random_location<-NA
#create a string of unique locations, as want to include effect of including/excluding
uniqlo<-unique(underreport_raw$loc_id)

for (k in 1:n_reps){
  for (j in 1:nrow(underreport_raw)){

    if(length(underreport_raw$year_start[j]:underreport_raw$year_end[j])==1){
      year_string[j]<-underreport_raw$year_start[j]
    }else{
      year_string[j]<-sample(underreport_raw$year_start[j]:underreport_raw$year_end[j],1)
    }

    proportion_string[j]<-underreport_raw$observed_cases[j]/underreport_raw$true_cases[j]
    proportion_error[j]<-sqrt((proportion_string[j]*(1-proportion_string[j]))/underreport_raw$true_cases[j])
    higher_bound[j]<-proportion_string[j]+(1.96*proportion_error[j])
    lower_bound[j]<-proportion_string[j]-(1.96*proportion_error[j])
    std_dev[j]<-proportion_error[j]*(sqrt(underreport_raw$true_cases[j]))
    proportion_sample[j]<-rnorm(1,proportion_string[j], proportion_error[j])


  }



  underreport_list[[k]]<-data.frame(location_name=underreport_raw$location_name,
                                    year_start=year_string,
                                    loc_id=underreport_raw$loc_id,
                                    proportion=proportion_string,
                                    prop_low=lower_bound,
                                    prop_upper=higher_bound,
                                    std_dev=std_dev,
                                    proportion_sample=proportion_sample,
                                    pathogen=underreport_raw$pathogen,
                                    observed_cases=underreport_raw$observed_cases,
                                    true_cases=underreport_raw$true_cases

  )
  random_location[k]<-sample(uniqlo, 1)
  underreport_list[[k]]<-subset(underreport_list[[k]], underreport_list[[k]]$loc_id!=random_location[k])
}

#subset each into a training and a test dataset
#determine 90/10 fraction
fraction<-0.9


underreport_train<-list()
underreport_test<-list()
sampling_frame<-list()
omission_frame<-list()

for (i in 1:n_reps){
  reference_string<-seq(1, nrow(underreport_list[[i]]),1)
  sampling_frame[[i]]<-sample(reference_string,round(nrow(underreport_list[[i]])*fraction))
  omission_frame[[i]]<-setdiff(reference_string, sampling_frame[[i]])
  underreport_train[[i]]<-underreport_list[[i]][sampling_frame[[i]],]
  underreport_test[[i]]<-underreport_list[[i]][omission_frame[[i]],]
}


mod<-list()
pred_test<-list()
for (k in 1:n_reps){

  underreport<-underreport_train[[k]]

  pulled_year <- pulled_haqi <- pulled_sdi <- pulled_leish_e <- rep(NA,length(underreport[,1]))

  for (i in 1:length(underreport[,1])){
    pulled_year[i] <- underreport$year_start[i]

    pulled_haqi[i] <- haqi[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value

    pulled_sdi[i] <- sdi[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value

    if (underreport$pathogen[i]=='cl'){
      pulled_leish_e[i] <- leish_endemic_cl[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value
    }
    if (underreport$pathogen[i]=='vl'){
      pulled_leish_e[i] <- leish_endemic_vl[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value
    }
  }


  pulled_data <- data.frame(observed = underreport$observed_cases,
                            true = underreport$true_cases,
                            pathogen = as.factor(underreport$pathogen),
                            year = pulled_year,
                            haqi = pulled_haqi,
                            sdi = pulled_sdi,
                            leish_endemic = pulled_leish_e)

  mod[[k]] <- glm(observed/true ~ pathogen + year + sdi, data = pulled_data, weights=true,family = binomial())
  #mod[[k]] <- glm(observed/true ~ pathogen + year + haqi + sdi + leish_endemic, data = pulled_data, weights=true,family = binomial())

  LocToPull <- 4844
  pred_year <- 1980:2017
  pred_path <- rep(levels(pulled_data$pathogen)[2], length(pred_year))
  #pred_haqi <- haqi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
  pred_sdi <- sdi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
  #pred_leish_e <- leish_endemic_vl[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value


  pred_test[[k]] <- predict(mod[[k]],data.frame(year = pred_year, pathogen = pred_path,  sdi = pred_sdi),se=TRUE)
  #pred_test[[k]] <- predict(mod[[k]],data.frame(year = pred_year, pathogen = pred_path, haqi = pred_haqi, sdi = pred_sdi, leish_endemic = pred_leish_e),se=TRUE)
}

#create a named .RData file tagged with experiment_name

save(mod,
     underreport_train,
     underreport_test,
     underreport_raw,
     file = sprintf("%s/%s.RData",underreport_dir,experiment_name))
