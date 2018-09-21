# Launch all stages of off-ART process

# Set up
rm(list=ls())

if (Sys.info()[1] == "Linux") {
  root <- FILEPATH
  user <- Sys.getenv("USER")
  code_dir <- paste0(FILEPATH)
} else {
  root <- FILEPATH
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0(FILEPATH)
}


start <- 1
archive <- T
run_date <- Sys.Date()
run_date <- gsub("-","",run_date)
test <- F

source(paste0("FILEPATH/get_locations.r"))
locations <- get_locations()
loc_list <- unique(locations$ihme_loc_id)

data_dir <- paste0(FILEPATH)
extract_dir <- paste0(FILEPATH)
spectrum_dir <- paste0(FILEPATH)
draw_save_dir <- FILEPATH

##################################################################
## Delete appropriate outputs, to ensure that jobs don't run on old data

setwd(data_dir)

if (start<=1) file.remove(paste0(extract_dir,"/prepped/cum_mort_all_cause.dta"))
if (start<=2) {
  file.remove(paste0(extract_dir,"/compiled/weibull_alpha_zaf.csv"))
  file.remove(paste0(extract_dir,"/prepped/hiv_specific_weibull_alpha_zaf.csv"))
  file.remove("regression/input/hiv_specific_mort.dta")
  file.remove(paste0(extract_dir,"/prepped/all_unaids_hiv_specific_prepped.csv"))
}
if (start<=3) {
  file.remove("regression/output/logit_hazard_draws_re_study.csv")
  file.remove("regression/output/draw_uncertainty.csv")
  for(aaa in c("15_25","25_35","35_45","45_100")) file.remove(paste0("compartmental_model/input/",aaa,".csv"))
  
}
if (start<=4) {
  file.remove("compartmental_model/output/surv_draws.dta")
  file.remove("compartmental_model/output/survival_curve_preds.csv")
  file.remove("compartmental_model/output/survival_uncertainty.csv")
  file.remove("compartmental_model/output/median_survival.csv")
  for(aaa in c("15_25","25_35","35_45","45_100")) {
    for(type in c("mortality","progression","survival_predictions")) {
      file.remove(paste0("compartmental_model/output/",aaa,"_sample_",type,".csv"))
    }
  }
}
if (start<=5) {
  file.remove("compartmental_model/output/re_age_specific_parameter_bounds.csv")
  file.remove("compartmental_model/output/coeff_var_table.csv")
  file.remove("compartmental_model/unaids/compare_pars_ihme_unaids.csv")

}
if (start<=6) file.remove("hiv_survival/compare_models.csv")


##################################################################
## Launch each job 

setwd(code_dir)

if (start<=1) qsub("nart01", paste0(code_dir,"/01_prep_km_all_cause_data.do"), slots=2,submit=!test)
if (start<=2) {
  qsub("nart02a", paste0(code_dir,"/02a_prep_unaids_weibull_data_compare.do"), hold="nart01", slots=2,submit=!test)
  qsub("nart02b", paste0(code_dir,"/02b_subtract_background_mortality.do"), hold="nart02a", slots=2,submit=!test)
  qsub("nart02c", paste0(code_dir,"/02c_prep_unaids_fit_no_bg_mort.do"), hold="nart02b", slots=2,submit=!test) 
}
if (start<=3) {
  qsub("nart03a", paste0(code_dir,"/03a_logit_regression.do"), hold="nart02c", slots=2,submit=!test)
  qsub("nart03b", paste0(code_dir,"/03b_plot_regression_draws.r"), hold="nart03a", slots=2,submit=!test)
}
if (start <= 4) {
  jobids <- NULL
  count <- 0 
  
  ages <- c("15_25", "25_35", "35_45", "45_100")
  for(age in unique(ages)) {
    qsub(paste0("nart_04_",age), paste0(code_dir,"/04b_optimize.r"), hold = "nart03b", pass = age, slots=5,submit=T)
    count <- count + 1
    jobids[count] <- paste0("nart_04_",age)
  }
 
  qsub("nart04e", paste0(code_dir,"/04e_prep_predicted_survival_from_comp_model.do"), hold=paste(jobids, collapse=","), slots=2,submit=!test)
  qsub("nart04f", paste0(code_dir,"/04f_plot_predicted_survival.r"), hold="nart04e", slots=2,submit=!test)
}
if (start <= 5) {
  dir.create(paste0(spectrum_dir,"/HIVmort_noART/",run_date,"_input"))
  dir.create(paste0(spectrum_dir,"/HIVmort_noART/",run_date,"_input"))
  qsub("nart05a", paste0(code_dir,"/05a_save_model_output_pars.do"), hold="nart04f", slots=2,pass = run_date, submit=!test)
  qsub("nart05b", paste0(code_dir,"/05b_prep_parameter_comparison.do"), hold="nart05a", slots=2,submit=!test)
  qsub("nart05c", paste0(code_dir,"/05c_graph_parameter_distributions.r"), hold="nart05b", slots=2,submit=!test)
}
if (start <= 6) {
  qsub("nart06a", paste0(code_dir,"/06a_prep_hiv_relative_survival_comparison.do"), hold="nart05c", slots=2,submit=!test)
  qsub("nart06b", paste0(code_dir,"/06b_compare_all_hiv_relative_survival.r"), hold="nart06a", slots=2,submit=!test)
}

##################################################################
## If we want to archive, archive all outputs here
## Note: Not all UN input files are archived (rarely change)

if (archive == T) {
  setwd(data_dir)
  
  print("Waiting 20 minutes before checking for end files")
  Sys.sleep(1200) 
  counter <- 0
  time_counter <- 0
  while(counter == 0) {
    test <- file.exists(paste0("hiv_survival/compare_models.csv"))
    if(test == T) {
      counter <- 1
      print("Jobs have finished, moving on to archiving")
    } else {
      print(paste0("Waiting for jobs to finish, at ",Sys.time()))
      time_counter <- time_counter + 1
      if(time_counter > 180) stop("Jobs are taking over 3 hours -- stopping execution") # Should take around 35-40 minutes total
      Sys.sleep(60)
    }
  }
  
  if (start<=1) file.copy(paste0(extract_dir,"/prepped/cum_mort_all_cause.dta"),
                          paste0(extract_dir,"/prepped/archive/cum_mort_all_cause_",run_date,".dta"),
                          overwrite = T)
  if (start<=2) {
    file.copy(paste0(extract_dir,"/prepped/hiv_specific_weibull_alpha_zaf.csv"),
              paste0(extract_dir,"/prepped/archive/hiv_specific_weibull_alpha_zaf_",run_date,".csv"),
              overwrite = T) 
    file.copy("regression/input/hiv_specific_mort.dta",
              paste0("regression/input/archive/hiv_specific_mort_",run_date,".dta"),
              overwrite = T) 
  }
  if (start<=3) {
    file.copy("regression/output/logit_hazard_draws_re_study.csv",
              paste0("regression/output/archive/logit_hazard_draws_re_study_",run_date,".csv"),
              overwrite = T)
    file.copy("regression/output/draw_uncertainty.csv",
              paste0("regression/output/archive/draw_uncertainty_",run_date,".csv"),
              overwrite = T)

    for(aaa in c("15_25","25_35","35_45","45_100")) {
      file.copy(paste0("compartmental_model/input/",aaa,".csv"),
                paste0("compartmental_model/input/archive/",aaa,"_",run_date,".csv"),
                overwrite = T)
    }   
  }
  if (start<=4) {
    file.copy("compartmental_model/output/surv_draws.dta",
              paste0("compartmental_model/output/archive/surv_draws_",run_date,".dta"),
              overwrite = T)
    for(fff in c("survival_curve_preds","survival_uncertainty","median_survival")) {
      file.copy(paste0("compartmental_model/output/",fff,".csv"),
                paste0("compartmental_model/output/archive/",fff,"_",run_date,".csv"),
                overwrite = T) 
    }

    for(aaa in c("15_25","25_35","35_45","45_100")) {
      for(type in c("mortality","progression","survival_predictions")) {
        file.copy(paste0("compartmental_model/output/",aaa,"_sample_",type,".csv"),
                  paste0("compartmental_model/output/archive/",aaa,"_sample_",type,"_",run_date,".csv"),
                  overwrite = T)
      }
    }
  }
  if (start<=5) {
    for(fff in c("re_age_specific_parameter_bounds","coeff_var_table","mortality_compiled","progression_compiled")) {
      file.copy(paste0("compartmental_model/output/",fff,".csv"),
                paste0("compartmental_model/output/archive/",fff,"_",run_date,".csv"),
                overwrite = T) 
    }
    file.copy("compartmental_model/unaids/compare_pars_ihme_unaids.csv",
              paste0("compartmental_model/unaids/archive/compare_pars_ihme_unaids_",run_date,".csv"),
              overwrite = T)
    
  }
  if (start<=6) file.copy("hiv_survival/compare_models.csv",
                          paste0("hiv_survival/archive/compare_models_",run_date,".csv"),
                          overwrite = T)
}




