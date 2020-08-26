## NAME
## Run a version of Space-Time GPR for HIV data to create a smooth time-series to input to the HIV process
## One use of this data is to split national Spectrum results into subnational units based on CoD data
## See HIV repo: launch_subnat_split.do and split_subnationals.do


###############################################################################################################
## Set Up

## Set up settings
rm(list=ls()); library(foreign)

if (Sys.info()[1]=="Windows") {
  root <- "ADDRESS" 
  user <- Sys.getenv("USERNAME")
} else {
  root <- "ADDRESS"
  user <- Sys.getenv("USER")
}

library(mortdb, lib = "FILEPATH")


## Set start and end options
start <-1  # Options: 1,2,3,4,5
end <- 5
test <- F # Test submission of everything 
file_del <- F

r.shell <- "FILEPATH"
s.shell = "FILEPATH"
p.shell = "FILEPATH"

# Enter your id below
user <- "USER"

sge.output.dir <- "FILEPATH"


run_comment <- "test_20" # Name this whatever parameter combination or whatever that you are testing
run_date <- Sys.Date()
run_date <- gsub("-","",run_date)
run_name <- paste0(run_date,"_",run_comment)
run_proj <- "-P proj_hiv "


## Add new file check function
source(paste0(root,"FILEPATH")) 

## functions 
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")
library(ltcore, lib = "FILEPATH")

## where data will be saved
out_dir <- "FILEPATH"
code_dir <- "FILEPATH"
file_dir <- "FILEPATH"



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

  qsub(jobname = "st_01", code = paste0(code_dir, "/01_linear_model_ver2.do"), cores = 2, mem = 10, 
         proj = "proj_hiv", submit = F, intel = F, shell = s.shell, archive_node = T, log = T, queue = 'all')
  
}

## Submit the space-time jobs (runs a submission script that then launches parallelized jbos by country)
setwd(code_dir)
if (start <= 2 & end >=2) {
  ## Wait for param file to write before using the locations there to check for ST output
  counter <- 0
  while(counter == 0) {
    if(file.exists(paste0(file_dir, "/params.csv"))) {
      counter <- 1
    } else {
      Sys.sleep(60)
    }
  }
  params <- fread(paste0(file_dir, "/params.csv"))
 
  ## Check for ST output
  ## Uncomment if you want to skip running the submitter
  ##  instead will run run_st.py for every location 
  for (l in unique(params$location_id)){
    #l <- 44709
    jname <- paste0("st_02_", l)
    lparams = params[params$location_id == l,]
    lambdaa = lparams$lambda[1]
    omega = lparams$omega[1]
    zeta = lparams$zeta[1]
    qsub(jobname = jname, code = paste0(code_dir, "/run_st.py"), pass = list(l, lambdaa, omega, zeta), 
         cores = 5, mem = 50, proj = "proj_hiv", submit = T, intel = F, shell = p.shell, archive_node = T, log = T, queue = 'long')

    
  }

  # qsub(jobname = "st_02", code = paste0(code_dir, "/02_submit_st.py"), cores = 2, mem = 10,
  #      proj = "proj_hiv", submit = T, intel = F, shell = p.shell, archive_node = T, log = T)
  # 
  n_files <- list.files(paste0(out_dir))
  n_files <- data.table(gsub(".csv","", n_files, fixed = TRUE))
  miss_locs <- params[!(params$location_id %in% n_files$V1),]
  #lin_new <- fread(paste0(out_dir, "/linear_predictions.csv"))
  lin_new <- fread(paste0("FILEPATH"))
  lin_loc <- data.table(unique(lin_new$location_id))
  lin_miss <- miss_locs[(miss_locs$location_id %in% lin_loc$V1),]
  write.csv(miss_locs, paste0(code_dir, "/miss_locs.csv"), row.names = FALSE)
  
  expected_files <- nrow(params)
  counter <- 0
  time_counter <- 0
  while(counter == 0) {
    n_files <- list.files(paste0(out_dir))
    if(length(n_files) == expected_files) {
      counter <- 1
      print("Jobs have finished, moving on to location-specific GPR")
    } else {
      print(paste0("Have ",length(n_files),", expecting ",expected_files," ",Sys.time()))
      time_counter <- time_counter + 1
      Sys.sleep(60)
      if(time_counter > 180) stop("Jobs are taking over 3 hours -- stopping execution") # Should take around 35-40 minutes total
    }
  }
}

## Prepare location-specific data for GPR
if (start <= 3 & end >=3) {
  qsub(jobname = "st_03", code = paste0(code_dir, "/03_prep_gpr.py"), cores = 4, mem = 5,
       proj = "proj_hiv", submit = T, intel = F, shell = p.shell, archive_node = T, log = T)
  
}

gpr <- fread(paste0(file_dir,"/forgpr.csv"))

## Run GPR
if (start <= 4 & end >=4) {
  qsub("st_04",paste0(code_dir,"/04_run_gpr.py"), cores = 4, mem = 5, pass=list(run_name), 
       submit = T, proj="proj_hiv", intel = F, shell = p.shell, archive_node = T, log = T)
  
  counter <- F
  while(counter == F) {
    counter <- file.exists(paste0(file_dir, "/gpr_results_run_date.csv"))
    Sys.sleep(60) # Wait a minute before re-checking
    print(paste0("Waiting for final file at ",Sys.time()))
  }
  print("Final file has been written")
  
  
  ## Constrain ENN and LNN results to 0
  results <- fread(paste0(file_dir, "/gpr_results_run_date.csv"))
  #results <- read.csv(paste0(out_dir,"/gpr_results_run_date.csv"))
  results$gpr_mean[results$age_group_id %in% c(2,3)] <- 0
  results$gpr_lower[results$age_group_id %in% c(2,3)] <- 0
  results$gpr_upper[results$age_group_id %in% c(2,3)] <- 0
  results$gpr_var[results$age_group_id %in% c(2,3)] <- 0
  results$st_prediction[results$age_group_id %in% c(2,3)] <- 0
  results$ln_dr_predicted[results$age_group_id %in% c(2,3)] <- 0
  write.csv(results,paste0(file_dir,"/gpr_results_",run_name,".csv"))
  
  file.remove(paste0(out_dir,"/gpr_results.csv"))
  file.copy(paste0(file_dir, "/gpr_results_20191209_test_20.csv"),paste0(file_dir, "/gpr_results.csv"))
  file.rename(paste0(file_dir,"gpr_results_run_date.csv"),paste0("FILEPATH"))
  file.copy(paste0(out_dir,"/params.csv"),paste0("FILEPATH"))
  
  ## Prep Spectrum GPR
  qsub("spec_gpr", paste0(code_dir,"/prep_spectrum_gpr_results.R"), cores = 4, mem = 5,
       submit = T, proj="proj_hiv", intel = F, shell = r.shell, archive_node = T, log = T) 
  
  qsub("cor_age",paste0(code_dir,"/save_correct_age.do"), cores = 4, mem = 5, submit = T, 
       proj="proj_hiv",pass=list(run_name), intel = F, shell = s.shell, archive_node = T, log = T)
}
######## TEST ##############
subnat_z <- fread(paste0(file_dir,"/spectrum_gpr_results_20191209_test_20.csv"))
locations <- get_locations()
small_loc <- locations[, c("location_id", "ihme_loc_id")]
subnat_z <- merge(subnat_z, small_loc, by = "location_id")
DT <- subnat_z[,sum(gpr_mean), by = .(location_id, ihme_loc_id)]
DT_z <- DT[V1 == 0,]


## Prepare location-specific data for GPR
if (start <= 5 & end >=5) {
  qsub("str_05", paste0(code_dir,"/05_graph_results.R"), cores = 4, mem = 5,
       submit = T, proj="proj_hiv", intel = F, shell = r.shell, archive_node = T, log = T) 
  
  #file.remove(paste0(out_dir,"/results_for_graphs.csv"))
  assertable::check_files(c("results_for_graphs.csv"), folder = file_dir, continual = T) 
  Sys.sleep(30)
  results_graph <- read.csv(paste0(file_dir, "/results_for_graphs.csv")) 
  locs <- unique(results_graph$ihme_loc_id)
  #locs <- c("NLD","CAN")
  
  
  ##remove male/female graphs 
  graph_dir <- "FILEPATH"
  
  file.remove(paste0(graph_dir,"/male/",locs,".pdf"))
  file.remove(paste0(graph_dir,"/female/",locs,".pdf")) 
  
  for (lll in locs){ 
   # lll <- "CHN_515"
    jname <- paste0("graph_stgpr_",lll)
    qsub(jobname = jname, paste0(code_dir,"/loc_graph.R"), cores = 4, mem = 5, pass = list(lll),
         submit = T, proj="proj_hiv", intel = F, shell = r.shell, archive_node = T, log = T) 
    
  }
  assertable::check_files(paste0(locs,".pdf"), folder = paste0(graph_dir,"/female"), continual = T)
  Sys.sleep(30)
  files <- gsub(",", "", toString(paste0(graph_dir,"/female/china/",locs,".pdf")))
  #file.remove(paste0(graph_dir,"/female_",run_comment,".pdf"))
  append_pdf(files, paste0(graph_dir,"/female/china/female_china.pdf"))
  
  assertable::check_files(paste0(locs,".pdf"), folder = paste0(graph_dir,"/male"), continual = T) 
  Sys.sleep(30) 
  files <- gsub(",", "", toString(paste0(graph_dir,"/male/china/",locs,".pdf")))
  #file.remove(paste0(graph_dir,"/male",run_comment,".pdf"))
  append_pdf(files, paste0(graph_dir,"/male/china/male_china.pdf"))

}


######  
params <- read.csv(paste0(out_dir,"/params.csv"))
cod_data <- fread(paste0(file_dir, "/cod_pull.csv"))
chn_data <- read.csv(paste0(file_dir, "/prepped_hiv_data.csv"))

shanxi <- fread(paste0(file_dir,"/515.csv"))
write.csv(shanxi, paste0(file_dir,"/shanxi_st.csv"))
linear_pred_shanxi <- data.table(linear_pred)
linear_pred_shanxi <- linear_pred_shanxi[location_id == 515,]
write.csv(linear_pred_shanxi, paste0(file_dir,"/shanxi_linear_pred.csv"))

forgpr <- fread(paste0(file_dir,"/forgpr.csv"))
forgpr_shanxi <- forgpr[location_id == 515,]
gpr_results <- fread(paste0(file_dir,"/gpr_results_20190718_190415_orca.csv"))
gpr_shanxi <- gpr_results[location_id == 515,]
write.csv(gpr_shanxi, paste0(file_dir,"/gpr_shanxi.csv"))

source("FILEPATH")
cod_data <- data.frame(get_cod_data(cause_id = 298, gbd_round_id = 6), stringsAsFactors = FALSE)
write.csv(cod_data, paste0(file_dir,"/cod_data_gbd5.csv"))

source("FILEPATH")
locs <- data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 5))
chn_locs <- locs[parent_id == 6,]
cod_data_chn <- cod_data[(cod_data$location_id %in% chn_locs$location_id),]
write.csv(cod_data_chn, paste0(file_dir,"/cod_china_gbd5.csv"))
