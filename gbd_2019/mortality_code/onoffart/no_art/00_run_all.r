# June 22 2015
# Launch all stages of off-ART process

# Set up
rm(list=ls())

if (Sys.info()[1] == "Linux") {
  root <- "ADDRESS"
  user <- Sys.getenv("USER")
  code_dir <- paste0("ADDRESS",user,"FILEPATH")
} else {
  root <- "J:"
  user <- Sys.getenv("USERNAME")
  code_dir <- paste0("ADDRESS",user,"FILEPATH")
}


start <- 1
archive <- T
run_date <- Sys.Date()
run_date <- gsub("-","",run_date)
run_comment <- "numbat"
run_date <- paste0(run_date,"_",run_comment)
test <- F
create_mean_mx <- T

library(data.table)

source(paste0(root,"FILEPATH"))
locations <- get_locations()
loc_list <- unique(locations$ihme_loc_id)

library(mortdb, lib = paste0(root,"FILEPATH"))
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")
library(ltcore, lib = "FILEPATH")


data_dir <- paste0(root,"FILEPATH")
extract_dir <- paste0(root,"FILEPATH")
spectrum_dir <- paste0(root,"FILEPATH")
draw_save_dir <- "FILEPATH"


## ###############################################################
## Set up QSUB function

qsub <- function(jobname, code, hold=NULL, pass=NULL, slots=1, submit=F) { 
  # choose appropriate shell script 
  if(grepl(".r", code, fixed=T)) shell <- "r_shell.sh" else if(grepl(".py", code, fixed=T)) shell <- "python_shell.sh" else shell <- "stata_shell.sh" 
  # set up number of slots
  if (slots > 1) { 
    slot.string = paste(" -pe multi_slot ", slots, sep="")
  } 
  # set up jobs to hold for 
  if (!is.null(hold)) { 
    hold.string <- paste(" -hold_jid \"", hold, "\"", sep="")
  } 
  # set up arguments to pass in 
  if (!is.null(pass)) { 
    pass.string <- ""
    for (ii in pass) pass.string <- paste(pass.string, " \"", ii, "\"", sep="")
  }  
  # construct the command 
  sub <- paste("qsub", 
  # sub <- paste("FILEPATH", 
               if (slots>1) slot.string, 
               if (!is.null(hold)) hold.string, 
               paste0(" -e FILEPATH"),
               " -N ", jobname, " ", 
              " -P proj_hiv ",
               shell, " ",
               code, " ",
               if (!is.null(pass)) pass.string, 
               sep="")
  # submit the command to the system
  if (submit) {
    system(sub) 
  } else {
    cat(paste("\n", sub, "\n\n "))
    flush.console()
  } 
}  


##create mean_mx for the bg subtraction 
if (create_mean_mx == T){ 
  
  file.remove(paste0(data_dir,"FILEPATH"))
    
    ###will make it#### 
    years = seq(1950,2017,by=1)
  
    mean_mx <- data.table(get_mort_outputs(model_name = "mlt life table",
                                           model_type = "estimate", 
                                           gbd_year = 2017, 
                                           life_table_parameter_ids = 1,
                                           sex_ids = c(1,2), year_ids = years))
  
    mean_mx <- subset(mean_mx, estimate_stage_id == 13)
    mean_mx <- mean_mx[,c("sex_id", "year_id", "ihme_loc_id", "age_group_id","mean"),with = FALSE]
    colnames(mean_mx) <- c("sex","year","ihme_loc_id","age_group_id","mean_mx") 
    mean_mx <- mean_mx[!is.na(ihme_loc_id),]
    write.csv(mean_mx,paste0(data_dir,"FILEPATH"),row.names=FALSE) 
    #####end#####
}


##################################################################
## Delete appropriate outputs, to ensure that jobs don't run on old data

setwd(data_dir)

if (start<=1) file.remove(paste0(extract_dir,"/FILEPATH/cum_mort_all_cause.dta"))
if (start<=2) {
  file.remove(paste0(extract_dir,"/FILEPATH/weibull_alpha_zaf.csv"))
  file.remove(paste0(extract_dir,"/FILEPATH/hiv_specific_weibull_alpha_zaf.csv"))
  file.remove("FILEPATH/hiv_specific_mort.dta")
  file.remove(paste0(extract_dir,"/FILEPATH/all_unaids_hiv_specific_prepped.csv"))
}
if (start<=3) {
  file.remove("FILEPATH/logit_hazard_draws_re_study.csv")
  file.remove("FILEPATH/draw_uncertainty.csv")
  for(aaa in c("15_25","25_35","35_45","45_100")) file.remove(paste0("FILEPATH",aaa,".csv"))
  
}
if (start<=4) {
  file.remove("FILEPATH/surv_draws.dta")
  file.remove("FILEPATH/survival_curve_preds.csv")
  file.remove("FILEPATH/survival_uncertainty.csv")
  file.remove("FILEPATH/median_survival.csv")
  for(aaa in c("15_25","25_35","35_45","45_100")) {
    for(type in c("mortality","progression","survival_predictions")) {
      file.remove(paste0("compartmental_model/output/",aaa,"_sample_",type,".csv"))
    }
  }
}
if (start<=5) {
  file.remove("FILEPATH/re_age_specific_parameter_bounds.csv")
  file.remove("FILEPATH/coeff_var_table.csv")
  file.remove("FILEPATH/compare_pars_ihme_unaids.csv")

}
if (start<=6) file.remove("FILEPATH/compare_models.csv")
# if (start<=7) {
#  file.remove("FILEPATH/matched_draw_median_survival.csv")
#  file.remove("FILEPATH/matched_draw_median_survival_stats.csv")
#  file.remove("FILEPATH/median_survival.csv")
# }


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
# if (start <= 7) {
#   qsub("nart07a", paste0(code_dir,"/07a_calculate_median_survival_by_country.do"), hold="nart06b", slots=2,submit=!test)
#   qsub("nart07b", paste0(code_dir,"/07b_median_survival_number_plug.do"), hold="nart07a", slots=2,submit=!test)
#   qsub("nart07c", paste0(code_dir,"/07c_graph_median_survival_country.r"), hold="nart07b", slots=2,submit=!test)
# }

##################################################################
## If we want to archive, archive all outputs here
## Note: Not all UN input files are archived (rarely change)

if (archive == T) {
  setwd(data_dir)
  
  # Do we have the final output that we need to archive?
  # If we do, then archive everything
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
  
  if (start<=1) file.copy(paste0(extract_dir,"/FILEPATH/cum_mort_all_cause.dta"),
                          paste0(extract_dir,"/FILEPATH/cum_mort_all_cause_",run_date,".dta"),
                          overwrite = T)
  if (start<=2) {
    file.copy(paste0(extract_dir,"/FILEPATH/hiv_specific_weibull_alpha_zaf.csv"),
              paste0(extract_dir,"/FILEPATH/archive/hiv_specific_weibull_alpha_zaf_",run_date,".csv"),
              overwrite = T) 
    file.copy("FILEPATH/hiv_specific_mort.dta",
              paste0("FILEPATH/hiv_specific_mort_",run_date,".dta"),
              overwrite = T) 
  }
  if (start<=3) {
    file.copy("FILEPATH/logit_hazard_draws_re_study.csv",
              paste0("FILEPATH/logit_hazard_draws_re_study_",run_date,".csv"),
              overwrite = T)
    file.copy("FILEPATH/draw_uncertainty.csv",
              paste0("FILEPATH/draw_uncertainty_",run_date,".csv"),
              overwrite = T)

    for(aaa in c("15_25","25_35","35_45","45_100")) {
      file.copy(paste0("FILEPATH",aaa,".csv"),
                paste0("FILEPATH",aaa,"_",run_date,".csv"),
                overwrite = T)
    }   
  }
  if (start<=4) {
    file.copy("FILEPATH/surv_draws.dta",
              paste0("FILEPATH/surv_draws_",run_date,".dta"),
              overwrite = T)
    for(fff in c("survival_curve_preds","survival_uncertainty","median_survival")) {
      file.copy(paste0("FILEPATH",fff,".csv"),
                paste0("FILEPATH",fff,"_",run_date,".csv"),
                overwrite = T) 
    }

    for(aaa in c("15_25","25_35","35_45","45_100")) {
      for(type in c("mortality","progression","survival_predictions")) {
        file.copy(paste0("FILEPATH",aaa,"_sample_",type,".csv"),
                  paste0("FILEPATH",aaa,"_sample_",type,"_",run_date,".csv"),
                  overwrite = T)
      }
    }
  }
  if (start<=5) {
    for(fff in c("re_age_specific_parameter_bounds","coeff_var_table","mortality_compiled","progression_compiled")) {
      file.copy(paste0("FILEPATH",fff,".csv"),
                paste0("FILEPATH",fff,"_",run_date,".csv"),
                overwrite = T) 
    }
    file.copy("FILEPATH/compare_pars_ihme_unaids.csv",
              paste0("FILEPATH",run_date,".csv"),
              overwrite = T)
    
    ## Archive Draws: ONLY IF YOU CAN ACCESS CLUSTERTMP
#       if (Sys.info()[1] == "Linux") {
#         dir.create(paste0(draw_save_dir,"FILEPATH",run_date))
#         dir.create(paste0(draw_save_dir,"FILEPATH",run_date))
#         
#         do.call(function(x) file.copy(paste0(spectrum_dir,"FILEPATH",x,"_mortality_par_draws.csv"),
#                                       paste0(draw_save_dir,"FILEPATH",run_date,"/",x,"_mortality_par_draws.csv"),
#                                       overwrite = T), 
#                 list(loc_list)) 
#         do.call(function(x) file.copy(paste0(spectrum_dir,"FILEPATH",x,"_progression_par_draws.csv"),
#                                       paste0(draw_save_dir,"FILEPATH",run_date,"/",x,"_progression_par_draws.csv"),
#                                       overwrite = T), 
#                 list(loc_list))
#       }
#     
    
  }
  if (start<=6) file.copy("FILEPATH/compare_models.csv",
                          paste0("FILEPATH/compare_models_",run_date,".csv"),
                          overwrite = T)
  # 
  # if (start<=7) {
  #   for(fff in c("survival","survival_stats")) {
  #    file.copy(paste0("FILEPATH",fff,".csv"),
  #             paste0("FILEPATH",fff,"_",run_date,".csv"),
  #             overwrite = T)
  #   }
  #   file.copy("FILEPATH/median_survival.csv",
  #             paste0("FILEPATH/median_survival_",run_date,".csv"),
  #             overwrite = T)
  # }
}




