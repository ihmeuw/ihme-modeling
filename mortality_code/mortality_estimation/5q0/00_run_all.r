################################################################################
## Description: Submit shells scripts to run all code necessary to produce 
##              estimates of child mortality. This code must be run on the 
##              cluster and assumes that you have already done everything up
##              through compiling the data (i.e. producing 'raw.5q0_adjusted.txt')
################################################################################


############
## Settings
############
rm(list=ls()); library(foreign); library(data.table); library(haven)

if (Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- paste0("FILEPATH", username, "/FILEPATH/child-mortality/")
  source("FILEPATH/shared/functions/get_locations.r")
} else {
  username <- Sys.getenv("USER")
  root <- "FILEPATH"
  code_dir <- paste0("FILEPATH", "/child-mortality/")
  setwd(code_dir)
  source("FILEPATH/shared/functions/get_locations.r")
}

  test <- F               # if T, no files are deleted and no jobs are submitted.
  start <- 1            # code piece to start (1 to run whole thing)
  end <- 13

  errout <- T          # If T, it will create error and output files on the cluster.
                        # Only do this if the number of HIV simulations is less than 100 (to be safe).
  delete_errout <- F
    
  save_prerake <- 1   # decide whether or not to save sims of preraked subnationals
  
  
  
  # save locs in flat file
  # In all of these get_locations calls, add a row with Old Andhra PrUSER
  
  codes <- get_locations(level="estimate", gbd_type = "ap_old")
  ap_old <- codes[codes$location_id==44849,]
  write.csv(codes, paste0(root, "FILEPATH/locations.csv"), row.names = F)
  
  codes_2015 <- get_locations(gbd_year=2015)
  codes_2015 <- rbind(codes_2015, ap_old)
  write.csv(codes_2015, paste0(root, "FILEPATH/locations2015.csv"), row.names = F)
  
  # save locs in flat file (saving separate 2016 because sometimes called explicitely)
  codes_2016 <- get_locations(gbd_year=2016, level="estimate", gbd_type = "ap_old")
  write.csv(codes_2016, paste0(root, "FILEPATHy/locations2016.csv"), row.names = F)
  
  codes_estimate <- get_locations(level="estimate", gbd_type = "ap_old")
  write.csv(codes_estimate, paste0(root, "FILEPATH/locations_est.csv"), row.names = F)
  
  
  codes <- codes[codes$level_all != 0 | codes$ihme_loc_id=="IND_44849",]
  codes <- codes[!duplicated(codes$ihme_loc_id),c("ihme_loc_id","region_name")]
  names(codes) <- c("ihme_loc_id","gbd_region")
  codes <- codes[order(codes$gbd_region, codes$ihme_loc_id),]
  codes$gbd_region <- gsub(" ", "_", codes$gbd_region)

  hivsims = 0 # deprecated but needed to for other code to run
############
## Define qsub function
############

qsub <- function(jobname, code, hold=NULL, pass=NULL, submit=F) { 
    # choose appropriate shell script 
    if(grepl(".r", code, fixed = T) | grepl(".R",code,fixed = T)) shell <- "r_shell.sh" else if(grepl(".py", code, fixed=T)) shell <- "python_shell.sh" else shell <- "stata_shell.sh" 
    # set up jobs to hold for 
    if (!is.null(hold)) { 
        hold.string <- paste(" -hold_jid \"", hold, "\"", sep="")
    } 
    # set up arguments to pass in 
    if (!is.null(pass)) { 
        pass.string <- ""
        for (ii in pass) pass.string <- paste(pass.string, " \"", ii, "\"", sep="")
    }
    #For 
    parallel <- NULL
    if(grepl(".py",code,fixed = T)) {
        parallel <- " -pe multi_slot 9"
    } else {
        parallel <- " -pe multi_slot 8"
    }
    # construct the command
    sub <- paste("FILEPATH/qsub -cwd", ifelse(errout," -o FILEPATH/errors"," -o FILEPATH -e FILEPATH"), 
    		 if (!is.null(hold)) hold.string,
    		 if (!is.null(parallel)) parallel,
    		 " -N ", jobname, " ",
    		 " -P proj_mortenvelope ",
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

#################
## First, make sure that the error and output files on the cluster are clear (only if we are saving error and outputs)
#################
if (delete_errout) {
    if (errout) {
        ## delete all error and output files
        system("find FILEPATH -type f -exec rm -f {} \\;")
        
        ## wait until they are all deleted
        while (!length(list.files(paste("FILEPATH/errors",sep=""))) == 0) {
            while (!length(list.files("FILEPATH/outputs")) == 0) Sys.sleep(10)
        }
    }
}
  
############
## Delete all current output files
############

    if (!test) {
        setwd("FILEPATH/data/")
        if (start<=1) file.remove("prediction_input_data.txt")
        if (start<=2) {
            file.remove("prediction_model_results_all_stages_GBD2015.txt")
        }
        if (start<=3) {
            file.remove("gpr_5q0_input_GBD2015.txt") 
        }
        
        if (start <=8) for(ff in dir("FILEPATH/GBD2015", pattern="gpr", full.names=T, include.dirs=F)) {file.remove(ff)}
        if (start<=9) file.remove("prediction_model_results_all_stages.txt")
        if (start<=10) file.remove("gpr_5q0_input.txt")	
        if (start <=11) for(ff in dir("FILEPATH/final", pattern="gpr", full.names=T, include.dirs=F)) {file.remove(ff)}
        
        setwd("FILEPATH/results")
            
    }
    
    setwd(code_dir)

###########################
#Run everything 250 times with different hiv draws
##########################

## testing
if (test) nhiv <- 5

## if only one run - no hiv sims
nhiv <- 1

runids <- NULL
for(rnum in 1:nhiv){

############
## Format the data; Run the prediction model
############
    setwd(code_dir)
    if (start<=1) qsub(paste0("m01_",rnum), "01_format_covariates_for_prediction_models.r", pass = list(rnum,hivsims,username), submit=!test)
    if (start<=2 & end >= 2) qsub(paste0("m02_",rnum), "02_fit_prediction_model.r", hold = paste0("m01_",rnum), pass = list(rnum,hivsims,username), submit=!test)
    setwd(code_dir)
    if (start<=3 & end >= 3) qsub(paste0("m03_",rnum), "03_calculate_data_variance.do", hold = paste0("m02_",rnum), pass = paste(rnum,as.numeric(hivsims),sep = "-"), submit=!test)

}

#
##############
#### Run GPR for each country
##############

#GPR seed in 08 is 123456
jobids <- NULL
count <- 0
for (rr in sort(unique(codes$gbd_region))) {
    for (cc in sort(unique(codes$ihme_loc_id[codes$gbd_region == rr]))) {
        count <- count + 1
        if (start<=8 & end >= 8) qsub(paste("m08",rnum, cc, sep="_"), "08_fit_gpr.py", paste("m03_",rnum, sep = ""), list(rr, cc, rnum, hivsims, username), submit=!test)
        jobids[count] <- paste("m08",rnum, cc, sep="_")
    }
}

if (start<=9 & end >= 9) qsub(paste("m09_",rnum, sep = ""), "09_fit_prediction_model_new_locs.r", paste("m03_",rnum, sep = ""), pass = list(rnum,hivsims,username), submit=!test)
if (start<=10 & end >= 10) qsub(paste("m10_",rnum, sep = ""), "10_calculate_data_variance_new_locs.do", paste("m09_",rnum, sep = ""), pass = paste(rnum,as.numeric(hivsims),sep = "-"), submit=!test)

#GPR seed in 11 is 123456


jobids <- NULL
count <- 0
for (rr in sort(unique(codes$gbd_region))) {
    for (cc in sort(unique(codes$ihme_loc_id[codes$gbd_region == rr]))) {
        count <- count + 1
        if (start<=11 & end >= 11) qsub(paste("m11",rnum, cc, sep="_"), "11_fit_gpr_new_locs.py", paste("m10_",rnum, sep = ""), list(rr, cc, rnum, hivsims, username), submit=!test)
        jobids[count] <- paste("m11",rnum, cc, sep="_")
    }
}
  
################
#### Create pause so dependencies are easier for compile code
################

if (start <= 8.25 & end >= 8.25 & hivsims) qsub(paste("pause", rnum, sep="_"), "pause.r", paste(jobids, collapse=","), submit=!test)
runids[rnum] <- paste("pause", rnum, sep="_")


#########################################################################################################

############
## Compile GPR results
############

# get list of locations with subnationals
locs <- as.data.table(get_locations())
locs <- locs[level_1==1 & level_2==0,]
parents <- unique(locs$ihme_loc_id)

## get list of gbd2015 locations
gbd2015 <- data.table(get_locations(gbd_year = 2015))  
gbd2015 <- gbd2015[level_all != 0] # 2015 locs



jobids2 <- NULL
count <- 0

if(start<=12 & end>=12){
 
  qsub("m12aa_copy_files", "12aa_copy_files.R", hold=ifelse(hivsims, paste(names, collapse=","),paste(jobids, collapse = ",")), pass=NULL, submit=!test)
  
  setwd(code_dir)
  for(loc in parents){
    count <- count + 1
    id <- locs[ihme_loc_id==loc,]$location_id
    qsub(paste0("m12a_", loc), "12a_rake_gpr_results.R", hold="m12aa_copy_files", pass=list(username, id, loc), submit=!test)
    jobids2[count] <- paste0("m12a_", loc)
  }
}


if (start<=12 & end >=12) qsub("m12b", "12b_compile_gpr_results.R", hold=paste(jobids2, collapse = ","), pass=list(save_prerake,username), submit=!test)


###################
###### Graph results
###################
#
if (start<=13 & end >=13) qsub("m13", "13_graph_5q0_compstage.r", "m12b", submit=!test)

