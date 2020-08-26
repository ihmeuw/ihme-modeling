#error estimation comparing new methodology with existing
rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		


in_dir <- sprintf("FILEPATH",prefix)
underreport_dir <- sprintf("FILEPATH",prefix)

experiment_name<-"FILEPATH"

##	Load shared function

source(sprintf("%s/FILEPATH",prefix))
source(sprintf("%s/FILEPATH",prefix))
source(sprintf("%s/FILEPATH",prefix))
source(sprintf("%s/FILEPATH",prefix))

ilogit <- function(x)1/(1+exp(-x))

out_sample<-TRUE
in_sample<-TRUE

#need to manually check these match
haqi <- get_covariate_estimates(ADDRESS)
sdi <- get_covariate_estimates(ADDRESS)
leish_presence <- get_covariate_estimates(ADDRESS)
leish_endemic <- get_covariate_estimates(ADDRESS)

#import RData with mod (as model list)
# underreport_test as testing list
# underreport_train as training list

load(sprintf("%s/%s.RData",underreport_dir,experiment_name))

#import Alvar parameters
underreporting_alvar <- read.csv(sprintf("FILEPATH",in_dir))

#need to add subnationals to the registry for Alvar - Bihar
additional_row<-underreporting_alvar[underreporting_alvar$location_name=='India',]
additional_row$location_id<-4844
additional_row$location_name<-'Bihar'

underreporting_alvar<-rbind(underreporting_alvar, additional_row)

underreporting_alvar_cl<-underreporting_alvar[,-8:-9]
underreporting_alvar_vl<-underreporting_alvar[,-6:-7]

if(in_sample==TRUE){
#take test dataset and identify the unique loc_ids
summary_tables<-list()
predict_deviance_string<-NA
predict_sq_deviance_string<-NA

alvar_deviance_string<-NA
alvar_sq_deviance_string<-NA


for (alpha in 1:length(mod)){ #note length(mod) should equal n_reps from underreporting models

  test_list<-list()
  
  test_df<-data.frame(location_name = underreport_train[[alpha]]$location_name,
                      year_id = underreport_train[[alpha]]$year_start,
                      loc_id = underreport_train[[alpha]]$loc_id,
                      observed_cases = underreport_train[[alpha]]$observed_cases,
                      true_cases = underreport_train[[alpha]]$true_cases
                      )
  
  for(bravo in 1:nrow(underreport_train[[alpha]])){
  LocToPull <- underreport_train[[alpha]]$loc_id[bravo]
  pred_year <- underreport_train[[alpha]]$year_start[bravo]
  pred_path <- underreport_train[[alpha]]$pathogen[bravo]
  pred_haqi <- haqi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
  pred_sdi <- sdi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
  pred_leish_e <- leish_endemic[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
  
  test_list[[bravo]] <- predict(mod[[alpha]],data.frame(year = pred_year, pathogen = pred_path, sdi = pred_sdi),se=TRUE)
  #test_list[[bravo]] <- predict(mod[[alpha]],data.frame(year = pred_year, pathogen = pred_path, haqi = pred_haqi, sdi = pred_sdi, leish_endemic = pred_leish_e),se=TRUE)
  test_df$predict_logit_meanfit[bravo] <-test_list[[bravo]]$fit
  test_df$predict_logit_sefit[bravo] <-test_list[[bravo]]$se.fit
  test_df$predict_meanfit[bravo]<-ilogit(test_list[[bravo]]$fit)
  test_df$predict_sefit[bravo]<-ilogit(test_list[[bravo]]$se.fit)
  
  #assign Alvar estimates as a runif between min and max level 
  if(pred_path=='cl'){
  # test_df$alvar_value[bravo]<-1/runif(1,
  #                                     min = underreporting_alvar_cl$cl_underreporting_lo[which(underreporting_alvar_cl$location_id==LocToPull)],
  #                                     max = underreporting_alvar_cl$cl_underreporting_hi[which(underreporting_alvar_cl$location_id==LocToPull)])
     test_df$alvar_value[bravo]<-1/((underreporting_alvar_cl$cl_underreporting_hi[which(underreporting_alvar_cl$location_id==LocToPull)]+underreporting_alvar_cl$cl_underreporting_lo[which(underreporting_alvar_cl$location_id==LocToPull)])/2)
  }
  if(pred_path=='vl'){
    # test_df$alvar_value[bravo]<-1/runif(1,
    #                                     min = underreporting_alvar_vl$vl_underreporting_lo[which(underreporting_alvar_vl$location_id==LocToPull)],
    #                                     max = underreporting_alvar_vl$vl_underreporting_hi[which(underreporting_alvar_vl$location_id==LocToPull)])
     test_df$alvar_value[bravo]<-1/((underreporting_alvar_vl$vl_underreporting_hi[which(underreporting_alvar_vl$location_id==LocToPull)]+underreporting_alvar_vl$vl_underreporting_lo[which(underreporting_alvar_vl$location_id==LocToPull)])/2)
  }
  
  #calculate mean deviance for prediction and alvar
  test_df$predict_deviance[bravo]<-(test_df$observed_cases[bravo]/test_df$true_cases[bravo])-test_df$predict_meanfit[bravo]
  test_df$alvar_deviance[bravo]<-(test_df$observed_cases[bravo]/test_df$true_cases[bravo])-test_df$alvar_value[bravo]
  test_df$predict_sqdeviance[bravo]<-(test_df$predict_deviance[bravo]*test_df$predict_deviance[bravo])
  test_df$alvar_sqdeviance[bravo]<-(test_df$alvar_deviance[bravo]*test_df$alvar_deviance[bravo])
  
  summary_tables[[alpha]]<-test_df
  }
  predict_deviance_string[alpha]<-mean(summary_tables[[alpha]]$predict_deviance)
  predict_sq_deviance_string[alpha]<-mean(summary_tables[[alpha]]$predict_sqdeviance)
  
  alvar_deviance_string[alpha]<-mean(summary_tables[[alpha]]$alvar_deviance)
  alvar_sq_deviance_string[alpha]<-mean(summary_tables[[alpha]]$alvar_sqdeviance)
}

prediction_summary<-data.frame(Test=c("mean bias error","mean absolute error","mean square error", "root mean square error"),
                               New_Method = c(NA,NA,NA,NA),
                               Alvar_Method = c(NA,NA,NA,NA)
)

prediction_summary$New_Method[prediction_summary$Test=='mean bias error']<-mean(predict_deviance_string)
prediction_summary$New_Method[prediction_summary$Test=='mean absolute error']<-mean(abs(predict_deviance_string))
prediction_summary$New_Method[prediction_summary$Test=='root mean square error']<-sqrt(mean(predict_sq_deviance_string))
prediction_summary$New_Method[prediction_summary$Test=='mean square error']<-mean(predict_sq_deviance_string)

prediction_summary$Alvar_Method[prediction_summary$Test=='mean bias error']<-mean(alvar_deviance_string)
prediction_summary$Alvar_Method[prediction_summary$Test=='mean absolute error']<-mean(abs(alvar_deviance_string))
prediction_summary$Alvar_Method[prediction_summary$Test=='root mean square error']<-sqrt(mean(alvar_sq_deviance_string))
prediction_summary$Alvar_Method[prediction_summary$Test=='mean square error']<-mean(alvar_sq_deviance_string)


insample_predict_deviance_string<-predict_deviance_string
insample_alvar_deviance_string<-alvar_deviance_string
insample_prediction_summary<-prediction_summary
}

if(out_sample==TRUE){
  #take test dataset and identify the unique loc_ids
  summary_tables<-list()
  predict_deviance_string<-NA
  predict_sq_deviance_string<-NA
  
  alvar_deviance_string<-NA
  alvar_sq_deviance_string<-NA
  
  
  for (alpha in 1:length(mod)){ #note length(mod) should equal n_reps from underreporting models
    
    test_list<-list()
    
    test_df<-data.frame(location_name = underreport_test[[alpha]]$location_name,
                        year_id = underreport_test[[alpha]]$year_start,
                        loc_id = underreport_test[[alpha]]$loc_id,
                        observed_cases = underreport_test[[alpha]]$observed_cases,
                        true_cases = underreport_test[[alpha]]$true_cases
    )
    
    for(bravo in 1:nrow(underreport_test[[alpha]])){
      LocToPull <- underreport_test[[alpha]]$loc_id[bravo]
      pred_year <- underreport_test[[alpha]]$year_start[bravo]
      pred_path <- underreport_test[[alpha]]$pathogen[bravo]
      pred_haqi <- haqi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
      pred_sdi <- sdi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
      pred_leish_e <- leish_endemic[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value
      
      test_list[[bravo]] <- predict(mod[[alpha]],data.frame(year = pred_year, pathogen = pred_path, sdi = pred_sdi),se=TRUE)
      #test_list[[bravo]] <- predict(mod[[alpha]],data.frame(year = pred_year, pathogen = pred_path, haqi = pred_haqi, sdi = pred_sdi, leish_endemic = pred_leish_e),se=TRUE)
      test_df$predict_logit_meanfit[bravo] <-test_list[[bravo]]$fit
      test_df$predict_logit_sefit[bravo] <-test_list[[bravo]]$se.fit
      test_df$predict_meanfit[bravo]<-ilogit(test_list[[bravo]]$fit)
      test_df$predict_sefit[bravo]<-ilogit(test_list[[bravo]]$se.fit)
      
      #assign Alvar estimates as a runif between min and max level - 
      if(pred_path=='cl'){
        # test_df$alvar_value[bravo]<-1/runif(1,
        #                                     min = underreporting_alvar_cl$cl_underreporting_lo[which(underreporting_alvar_cl$location_id==LocToPull)],
        #                                     max = underreporting_alvar_cl$cl_underreporting_hi[which(underreporting_alvar_cl$location_id==LocToPull)])
        test_df$alvar_value[bravo]<-1/((underreporting_alvar_cl$cl_underreporting_hi[which(underreporting_alvar_cl$location_id==LocToPull)]+underreporting_alvar_cl$cl_underreporting_lo[which(underreporting_alvar_cl$location_id==LocToPull)])/2)
      }
      if(pred_path=='vl'){
        # test_df$alvar_value[bravo]<-1/runif(1,
        #                                     min = underreporting_alvar_vl$vl_underreporting_lo[which(underreporting_alvar_vl$location_id==LocToPull)],
        #                                     max = underreporting_alvar_vl$vl_underreporting_hi[which(underreporting_alvar_vl$location_id==LocToPull)])
        test_df$alvar_value[bravo]<-1/((underreporting_alvar_vl$vl_underreporting_hi[which(underreporting_alvar_vl$location_id==LocToPull)]+underreporting_alvar_vl$vl_underreporting_lo[which(underreporting_alvar_vl$location_id==LocToPull)])/2)
      }
      
      #calculate mean deviance for prediction and alvar
      test_df$predict_deviance[bravo]<-(test_df$observed_cases[bravo]/test_df$true_cases[bravo])-test_df$predict_meanfit[bravo]
      test_df$alvar_deviance[bravo]<-(test_df$observed_cases[bravo]/test_df$true_cases[bravo])-test_df$alvar_value[bravo]
      test_df$predict_sqdeviance[bravo]<-(test_df$predict_deviance[bravo]*test_df$predict_deviance[bravo])
      test_df$alvar_sqdeviance[bravo]<-(test_df$alvar_deviance[bravo]*test_df$alvar_deviance[bravo])
      
      summary_tables[[alpha]]<-test_df
    }
    predict_deviance_string[alpha]<-mean(summary_tables[[alpha]]$predict_deviance)
    predict_sq_deviance_string[alpha]<-mean(summary_tables[[alpha]]$predict_sqdeviance)
    
    alvar_deviance_string[alpha]<-mean(summary_tables[[alpha]]$alvar_deviance)
    alvar_sq_deviance_string[alpha]<-mean(summary_tables[[alpha]]$alvar_sqdeviance)
  }
  
  prediction_summary<-data.frame(Test=c("mean bias error","mean absolute error","mean square error", "root mean square error"),
                                 New_Method = c(NA,NA,NA,NA),
                                 Alvar_Method = c(NA,NA,NA,NA)
  )
  
  prediction_summary$New_Method[prediction_summary$Test=='mean bias error']<-mean(predict_deviance_string)
  prediction_summary$New_Method[prediction_summary$Test=='mean absolute error']<-mean(abs(predict_deviance_string))
  prediction_summary$New_Method[prediction_summary$Test=='root mean square error']<-sqrt(mean(predict_sq_deviance_string))
  prediction_summary$New_Method[prediction_summary$Test=='mean square error']<-mean(predict_sq_deviance_string)
  
  prediction_summary$Alvar_Method[prediction_summary$Test=='mean bias error']<-mean(alvar_deviance_string)
  prediction_summary$Alvar_Method[prediction_summary$Test=='mean absolute error']<-mean(abs(alvar_deviance_string))
  prediction_summary$Alvar_Method[prediction_summary$Test=='root mean square error']<-sqrt(mean(alvar_sq_deviance_string))
  prediction_summary$Alvar_Method[prediction_summary$Test=='mean square error']<-mean(alvar_sq_deviance_string)
  
  outsample_predict_deviance_string<-predict_deviance_string
  outsample_alvar_deviance_string<-alvar_deviance_string
  outsample_prediction_summary<-prediction_summary 
}
save(
  outsample_predict_deviance_string,
  outsample_alvar_deviance_string,
  outsample_prediction_summary,
  insample_predict_deviance_string,
  insample_alvar_deviance_string,
  insample_prediction_summary,
  file = sprintf("%s/%sFILEPATH",underreport_dir,experiment_name)
  )