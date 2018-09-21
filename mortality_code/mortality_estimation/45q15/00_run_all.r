################################################################################
## Description: Submit shells scripts to run all code necessary to produce 
##              estimates of adult mortality. This code must be run on the 
##              cluster and assumes that you have already done everything up
##              through compiling the data 
################################################################################

############
## Settings
############
  
  rm(list=ls()); library(foreign); library(data.table)

  test <- F             # if T, no files are deleted and no jobs are submitted


  start <- 1  
  end <- 9    # piece of code to end at
 
  param.selection <- F  
  hiv.uncert <- 1       ## 1 or 0 logical for whether we're running with draws of HIV
  hiv.update <- 0      ## Do you need to update the values of the hiv sims? Affects 01b
  num.holdouts <- 100
  hivdraws <- 250         ## How many draws to run, must be a multiple of 25 (for job submission)
  if (hiv.uncert == 0) hivdraws <- 1
  hivscalars <- T  # use scalars for HIV (only T on the first run of the loop e.g. use on delta 1, but not on delta 2)
  #these options are for the HIV sims - but the code isn't set up to do parameter selection and HIV sims at the same time, which would be way too many jobs

  if (Sys.info()[1] == "Linux") root <- "filepath" else root <- "filepath"
  user <- Sys.getenv("USER") # DUSERt for linux user grab. "USERNAME" for Windows
  code_dir <- paste0("filepath")
  data_dir_uncert <- paste0("filepath")
  data_dir <- paste0("filepath")
  source(paste0( "filepath"))

  ## get countries we want to produce estimates for
  codes <- get_locations(gbd_type = "ap_old", level = "estimate")
  old_ap <- codes[codes$ihme_loc_id == "IND_44849",]
  write.csv(codes, paste0( "filepath"))
  codes <- codes[codes$location_id == 44849 | codes$level_all == 1,] # Eliminate those that are estimates but that are handled uniquely (ex: IND six minor territories)
  codes$region_name <- gsub(" ", "_", gsub(" / ", "_", gsub(", ", "_", gsub("-", "_" , codes$region_name))))
  codes <- merge(codes, data.frame(sex=c("male", "female")))
  codes <- codes[order(codes$region_name, codes$ihme_loc_id, codes$sex),]
  

# 2015 locations plus Kenya provinces, to be used in 02_fit_prediction_model
locations_2015_ken <- get_locations(level = "estimate", gbd_year = 2015)
kenya_prov <- as.data.table(get_locations())
kenya_prov <- kenya_prov[substr(ihme_loc_id, 1, 3) == "KEN" & level == 4,]
locations_2015_ken <- rbind(locations_2015_ken, kenya_prov, use.names = T)
locations_2015_ken <- rbind(locations_2015_ken, old_ap, use.names = T)
write.csv(locations_2015_ken, paste0("filepath"))

############
## Define qsub function
############

  qsub <- function(jobname, code, hold=NULL, pass=NULL, slots=1, submit=F, log=T) { 
    # choose appropriate shell script 
    if(grepl(".r", code, fixed=T) | grepl(".R", code, fixed=T)) shell <- "filepath" else if(grepl(".py", code, fixed=T)) shell <- "filepath" else shell <- "filepath" 
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
                 if(log==F) "filepath",  # don't log (if there will be many log files)
                 if(log==T) paste0("filepath"),
                 if (slots>1) slot.string, 
                 if (!is.null(hold)) hold.string, 
                 " -N ", jobname, " ",
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
 
  
   setwd(code_dir)

############
## Format the data; Run the prediction model; Define the holdouts 
############
  if (start<=1 & end >=1) {
    qsub("am01a", paste0("filepath"), slots=5,submit=!test)
    qsub("am01b", paste0("filepath"), hold = "am01a",pass=list(hiv.uncert,hiv.update,hivscalars), slots=5,submit=!test)
  }
  
  if (start<=2 & end >=2) {
    for (i in 1:(hivdraws)) {


      qsub(paste("am02",i,sep="_"), paste0("filepath"), hold = "am01b",pass=list(i,hiv.uncert), slots=5, submit=!test)
    }
  }
 
###########
## Run the second stage
###########
count <- 0
if (start<=7 & end >=7) {
  holdgpr <- NULL
  ## first launch step that saves first stage coefficients, doing here so holds present
  qsub(paste0("hivcoeff"), paste0("filepath"), hold=ifelse(param.selection, "am06b", paste("am02",c(1:250),sep="_",collapse=",")),slots = 1, submit=!test)
  
  
  for (i in 1:(hivdraws)) {
    count <- count + 1
    qsub(paste("am07a",i, sep="_"), paste0("filepath"), hold=ifelse(param.selection, "am06b", paste("am02",i,sep="_")),pass=list(i,hiv.uncert),slots = 2, submit=!test)
    holdgpr[count] <- paste("am07a",i,sep="_")
  }
}  


############
## Run GPR for each country-sex
#note: make sure chosen draw files have been made for new locations
############

  jobids <- NULL
  count <- 0 
  if(hivdraws == 1) {
    hhh <- 1
  } else {
    hhh <- hivdraws/25
  }
  
if (start<=7.5 & end >=7.5) {
for (cc in sort(unique(codes$ihme_loc_id))) {
    for (i in 1:hhh) {
       
      count <- count + 1
      qsub(paste("am07b", cc,i, sep="_"), paste0("filepath"), hold=ifelse(start < 7.5,paste(holdgpr,collapse=","),"fakejob"),  pass = list(cc,hiv.uncert,i),slots=3, 
           submit=!test,log=T) 
      jobids[count] <- paste("am07b", cc,i, sep="_")
     
    }
  }
}
 
############ 
## Compile GPR results
############

## if doing HIV sims, compile those gpr files first
if (hiv.uncert == 1) {
  jobidsh <- NULL
  count <- 0 
  if (start<=7.75 & end >=7.75) {
     for (cc in sort(unique(codes$ihme_loc_id))) {

        count <- count + 1
        qsub(paste("am07c", cc, sep="_"), paste0("filepath"), hold=ifelse(start > 7.5,"fake_jobs",paste(jobids[grepl(cc,jobids)],collapse=",")), pass= 
             list(cc,hivdraws/25), slots=10,submit=!test) 
        jobidsh[count] <- paste("am07c", cc, sep="_")
          
      }
  }
}

  
if (start<=8 & end >=8) {
  count <- 0
  jobids2 <- NULL

  #Calling the raking code
  #Should only be called on parent locations
  parents <- codes[codes$level_1==1 & codes$level_2==0,]
  for(cc in unique(parents$ihme_loc_id)) {
    count <- count + 1
    qsub(paste0("am08_",cc), paste0("filepath"), hold = ifelse(start > 7.9,"no_holds",ifelse(hiv.uncert==1,paste(jobidsh,collapse=","), paste(jobids, collapse=","))), 
         slots=4, pass = list(cc, hiv.uncert), submit=!test)
    jobids2[count] <- paste0("am08_",cc)
    }
  }

    qsub("am08b", paste0("filepath"), slots=2, hold = paste(jobids2, collapse=","), pass=list(hiv.uncert),submit=!test)
  }
