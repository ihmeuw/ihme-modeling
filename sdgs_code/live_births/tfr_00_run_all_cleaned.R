################################################################################
## 00_run_all
## Purpose: run all of the fertility modeling process
################################################################################

# Run this on the cluster by running the file /00_run_all.R
# source("00_run_all.R")

  rm(list=ls())

  library(data.table)
  root = "FILEPATH"

# set parameters 


  test <- F     # if T,  no jobs are submitted
  start <- 1      # code piece to start at
  end <- 4           # the last piece of code to be run

  user <- Sys.getenv("USER") # DUSERt for linux user grab. "USERNAME" for Windows
  code_dir <- paste0("FILEPATH")

  mod_id <- 61
#-------------------------------------------------------------------------------

########################
## Define qsub function
########################

  qsub <- function(jobname, code, hold=NULL, pass=NULL, slots=1, email = F, submit=F, log=T) { 
    # choose appropriate shell script 
    if(grepl(".R", code, fixed=TRUE)) shell <- "r_shell.sh" else if(grepl(".py", code, fixed=T)) shell <- "python_shell.sh" else shell <- "stata_shell.sh" 
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
                 if(log==F) " -e FILEPATH ",  # don't log (if there will be many log files)
                 if(log==T) paste0(" -e FILEPATH "),
                 " -P proj_covariates", 
                 if (slots>1) slot.string,
                 if (email == T) " -m e",
                 if (!is.null(hold)) hold.string, 
                 " -N ",jobname, " ",
                 shell, " ",
                 "\"",code,"\"", " ",
                 if (!is.null(pass)) pass.string, 
                 sep="")
    # submit the command to the system
    if (submit) {
      system(sub) 
    } else {
      cat(paste("\n", sub, "\n\n "))
      cat(paste0(jobname, "\n"))
      flush.console()
    } 
    
  } 
  

#########################
## Data Density Function
#########################

  data_density <- function(dt){

      density <- dt[!is.na(tfr), unique(year), by = .(ihme_loc_id, region_id)]
      density <- density[, .N, by = .(ihme_loc_id, region_id)]
      density <- merge(density, dt[,.(ihme_loc_id, region_id)], by = c("ihme_loc_id", "region_id"), all.y = T)
      
      density[is.na(N), N := 0]
      density[, dens := N/67]
      density <- density[, .(dens = mean(dens)), by = .(ihme_loc_id, region_id)]

      return(density)
  }
  
#-------------------------------------------------------------------------------

########################################
## delete previous files 
########################################
if(test == F){

    
  if (start <= 3 & end >=3){
    files <- list.files(path = "FILEPATH", pattern="gpr", full.names=T)
    file.remove(files)
  }
  

}

########################################
## submit jobs 
########################################


###################
### 01_prep
###################

if (start <= 1 & end >=1) {qsub(jobname="fertility_01_prep", code = paste0(code_dir,"01_prep.R"), 
                      hold=NULL, slots=4, pass = list(mod_id), submit=!test)}


###################
## 02_fit_prediction_model 
###################


if (start <= 2 & end >=2) {qsub(jobname="fertility_02_fit_prediction_model", code = paste0(code_dir,"02_fit_prediction_model.R"), 
                      hold= "fertility_01_prep", slots=4, pass = list(mod_id), submit=!test)}


##############
#### 03_fit_gpr  Run GPR for each country
##############

jobids <- NULL

if (start <= 3 & end >=3){
    
    # get location data 
    while(!file.exists("FILEPATH")){
        Sys.sleep(30)
    } 
    

    source("FILEPATH") #function to get locations
    codes <- fread("FILEPATH")

    codes <- data_density(codes)

    # removes GPR files before running the new GPR process
    gpr_files <- list.files("FILEPATH", full.names=T)
    unlink(gpr_files, recursive = FALSE, force = FALSE)
    jobids <- NULL
    count <- 0
    
    for (rr in sort(unique(codes$region_id))) {
      for (cc in sort(unique(codes$ihme_loc_id[codes$region_id == rr]))) {

                 if (codes[ihme_loc_id == cc, dens > .75]) {

                     amp <- 1
                     scale <- 5
                     print(paste(amp, scale, sep = ", "))
                     
                 } else {
                     
                     amp <- 1
                     scale <- 10 
                     print(paste(amp, scale, sep = ", "))
                 }
          
                  count <- count + 1
                  qsub(paste("fertility_gpr",cc, amp, scale, sep="_"), code = paste0(code_dir, "03_fit_gpr.py"),
                       hold = paste(c("fertility_01_prep", "fertility_02_fit_prediction_model"), collapse=","), 
                       slots = 6, 
                       pass = list(rr, cc, mod_id, amp, scale), 
                       submit=!test)
                  jobids[count] <- paste("fertility_gpr",cc, amp, scale, sep="_") 

      }
    }
}


###############
## 04_graphing_results
###############


if (start <= 4 & end >=4) {qsub(jobname="fertility_04_graphing_results", code = paste0(code_dir,"linear_gpr_vetting.R"), 
                                hold=paste(append(c("fertility_01_prep", "fertility_02_fit_prediction_model"),
                                       jobids), collapse=","), slots=5, pass = list(mod_id), submit=!test)}





