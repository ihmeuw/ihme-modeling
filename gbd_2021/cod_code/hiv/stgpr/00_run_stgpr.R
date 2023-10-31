## Grant Nguyen
## Run a version of Space-Time GPR for HIV data to create a smooth time-series to input to the HIV process
## One use of this data is to split national Spectrum results into subnational units based on CoD data
## See HIV repo: launch_subnat_split.do and split_subnationals.do


###############################################################################################################
## Set Up

## Set up settings
rm(list=ls()); library(foreign)

if (Sys.info()[1]=="Windows") {
  root <- "J:" 
  user <- Sys.getenv("USERNAME")
} else {
  root <- "/home/j"
  user <- Sys.getenv("USER")
}
source(paste0(root, "/Project/Mortality/shared/functions/get_locations.r"))

## Set start and end options
start <-1  # Options: 1,2,3,4,5
end <- 5
test <- F # Test submission of everything 
file_del <- T

run_comment <- "RUNNAME" # Name this whatever parameter combination or whatever that you are testing
run_date <- Sys.Date()
run_date <- gsub("-","",run_date)
run_name <- paste0(run_date,"_",run_comment)


## Add new qsub function
source(paste0(root,"/Project/Mortality/shared/functions/qsub.R"))

## where data will be saved
out_dir <- "FILEPATH"
code_dir <- "FILEPATH"



###############################################################################################################
## Remove Existing Files
if (file_del == T) {
  if (start <= 1) {
    file.remove(paste0(out_dir,"/linear_predictions.csv"))
    file.remove(paste0(out_dir,"/params.csv"))
  }
  if (start <= 2 & end >=2) {
    system(paste0("rm ",out_dir,"/st/*.csv"))
  }
  if (start <= 3 & end >=3) {
    file.remove(paste0(out_dir,"/forgpr.csv"))
  }
}

###############################################################################################################
## Submit Jobs
setwd(code_dir)

## Get data, change ST and GPR parameters, and run the preliminary linear model
if (start <= 1) {
  qsub("st_01",paste0(code_dir,"/01_linear_model.do"), slots = 2, submit = !test, proj="proj_hiv",log = T)
  
}

## Submit the space-time jobs (runs a submission script that then launches parallelized jbos by country)
if (start <= 2 & end >=2) {
  qsub("st_02",paste0(code_dir,"/02_submit_st.py"), slots = 2, hold = ifelse(start < 2,"st_01","fakejob"), submit = !test, proj="proj_hiv", log = T)
  
  ## Wait for param file to write before using the locations there to check for ST output
  counter <- 0
  while(counter == 0) {
    if(file.exists(paste0(out_dir,"/params.csv"))) {
      counter <- 1
    } else {
      Sys.sleep(60)
    }
  }
  
  ## Check for ST output
  params <- read.csv(paste0(out_dir,"/params.csv"))
  expected_files <- nrow(params)
  counter <- 0
  time_counter <- 0
  while(counter == 0) {
    n_files <- list.files(paste0(out_dir,"/st"))
    if(length(n_files) == expected_files) {
      counter <- 1
      print("Jobs have finished, moving on to location-specific GPR")
    } else {
      print(paste0("Have ",length(n_files),", expecting ",expected_files," ",Sys.time()))
      time_counter <- time_counter + 1
      Sys.sleep(60)
      #if(time_counter > 180) stop("Jobs are taking over 3 hours -- stopping execution") # Should take around 35-40 minutes total
    }
  }
}

## Prepare location-specific data for GPR
if (start <= 3 & end >=3) {
  qsub("st_03",paste0(code_dir,"/03_prep_gpr.py"), slots = 2, hold = ifelse(start < 3,"st_02","fakejob"), submit = !test, proj="proj_hiv", log = T)
}

## Run GPR
if (start <= 4 & end >=4) {
  qsub("st_04",paste0(code_dir,"/04_run_gpr.py"), slots = 2,  hold = ifelse(start < 4,"st_03","fakejob"),pass=list(run_name), submit = !test, proj="proj_hiv", log = T)
  
  counter <- F
  while(counter == F) {
    counter <- file.exists(paste0(out_dir,"/gpr_results_",run_name,".csv"))
    Sys.sleep(60) # Wait a minute before re-checking
    print(paste0("Waiting for final file at ",Sys.time()))
  }
  print("Final file has been written")
  
  ## Constrain ENN and LNN results to 0
  results <- read.csv(paste0(out_dir,"/gpr_results_",run_name,".csv"))
  results$gpr_mean[results$age_group_id %in% c(2,3)] <- 0
  results$gpr_lower[results$age_group_id %in% c(2,3)] <- 0
  results$gpr_upper[results$age_group_id %in% c(2,3)] <- 0
  results$gpr_var[results$age_group_id %in% c(2,3)] <- 0
  results$st_prediction[results$age_group_id %in% c(2,3)] <- 0
  results$ln_dr_predicted[results$age_group_id %in% c(2,3)] <- 0
  write.csv(results,paste0(out_dir,"/gpr_results_",run_name,".csv"))

  ## Prep Spectrum GPR
  qsub("spec_gpr", paste0(code_dir,"/prep_spectrum_gpr_results.R"), slots = 2, submit = !test, proj="proj_hiv", log = T) 
  qsub("cor_age",paste0(code_dir,"/save_correct_age.do"), slots = 2, submit = !test, proj="proj_hiv",pass=list(run_name),log = T)
}

## Prepare location-specific data for GPR
if (start <= 5 & end >=5) {
  
  file.remove(paste0(out_dir,"/results_for_graphs.csv"))
  qsub("str_05",paste0(code_dir,"/05_graph_results.R"), slots = 4,hold = ifelse(start < 5,"cor_age","fakejob"), submit = !test, proj="proj_hiv", log = T)
  assertable::check_files(c("results_for_graphs.csv"), folder = out_dir, continual = T) 
  Sys.sleep(30)
  results_graph <- read.csv(paste0(out_dir,"/results_for_graphs.csv")) 
  locs <- unique(results_graph$ihme_loc_id)
  #locs <- c("NLD","CAN")
  
  
  ##remove male/female graphs 
  graph_dir <- "FILEPATH"
  file.remove(paste0(graph_dir,"/male/",locs,".pdf"))
  file.remove(paste0(graph_dir,"/female/",locs,".pdf"))
  
  for (lll in locs){ 
    qsub(paste0("graph_stgpr_",lll),paste0(code_dir,"/loc_graph.R"), slots = 2,pass=list(lll), submit = !test, proj="proj_hiv", log = T)
    
  }
  assertable::check_files(paste0(locs,".pdf"), folder = paste0(graph_dir,"/female"), continual = T)
  Sys.sleep(30)
  files <- gsub(",", "", toString(paste0(graph_dir,"/female/",locs,".pdf")))
  file.remove(paste0(graph_dir,"/female_",run_comment,".pdf"))
  append_pdf(files, paste0(graph_dir,"/female_",run_comment,".pdf"))
  
  assertable::check_files(paste0(locs,".pdf"), folder = paste0(graph_dir,"/male"), continual = T) 
  Sys.sleep(30)
  files <- gsub(",", "", toString(paste0(graph_dir,"/male/",locs,".pdf")))
  file.remove(paste0(graph_dir,"/male",run_comment,".pdf"))
  append_pdf(files, paste0(graph_dir,"/male",run_comment,".pdf"))
  
}