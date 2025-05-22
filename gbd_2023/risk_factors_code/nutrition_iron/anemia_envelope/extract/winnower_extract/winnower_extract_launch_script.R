#################################
#
# Winnower Extraction Script
#
# Note that UbCov and Winnower are used interchangeably in this script, but given that the ubcov_id variable is still used in the codebook, it will be referenced the same way in this pipeline
#
#################################

# SOURCE R-SCRIPTS --------------------------------------------------------

source("~/repos/extraction_templates/winnower_extraction_template/winnower_sifter_functions.R") #loads in helper functions for checking extraction progress and prepping extraction tasks

# CUSTOMIZE VARIABLES FOR GLOBAL SETUP/REFERENCING  -----------------------

#where you want to save parameter maps to. this can be somewhere on your home drive or in a team shared folder
extraction_code_dir <- "~/repos/extraction_templates/winnower_extraction_template/" 

#the codebook/topic name (should match the Winnmill name for your custom codebook EXACTLY)
codebook_name <- "anemia" 

# CUSTOMIZE .DTA EXTRACTION SURVEY OUTPUT PATHS ---------------------------

# (make sure they match the paths defined in your ~/.winnower.ini file)

#where extracted .dta files originating from the J Drive will end up:
j_winnower_path <- 'FILEPATH'

#where extracted .dta files that were sourced from the L drive will end up
l_winnower_path <- "FILEPATH"
#NOTE - only initialize the l_winnower_path if you have been added to the "UbCov Sandbox" 
#for your outputs on the L Drive. You may need to submit a help desk ticket to gain access.


# CUSTOMIZE OUTPUT/ERROR LOG FILE PATHS & SLURM JOB NAME ------------------

# NOTE - (these will get written during the Winnower extraction process)

#the extraction job name (example: "anemia_winnower_extract"). This is the job name that will be sent to slurm.
job_name <- paste0(codebook_name,"_winnower_extract") # <-- feel free to edit

#the directory to save output logs to
winnower_log_output_dir <- file.path("FILEPATH", Sys.getenv("USER"), "output/") # <-- feel free to edit

#the directory to save error logs to
winnower_log_errors_dir <- file.path("FILEPATH", Sys.getenv("USER"), "errors/") # <-- feel free to edit


# SET RUN STATUS FLAG -----------------------------------------------------

##################
#BEFORE STARTING, SET THIS FLAG:
# - to TRUE if you are starting a new extraction run
# - to FALSE if you are continuing to work with the current extraction
# (this is because UbCov IDs are ever-changing and a static codebook is needed to reference the current IDs used in the extraction pipeline for later calculations)
# It is good practice to have this flag set to TRUE. Only set to FALSE if you require the UbCov IDs from the previous extraction for post processing purposes
new_run <- T
remove_old_files <- T

# INITIALIZE CODEBOOK -----------------------------------------------------

if(new_run){
  codebook <- format_cb(topic = codebook_name) #your custom codebook
  codebook <- as.data.table(codebook)
  write.csv(
    x = codebook, 
    file = paste0(
      extraction_code_dir,
      codebook_name,"_codebook.csv"
    ),
    row.names = F
  )
  
  if(remove_old_files){
    # clear out the directories where past files were saved
    system(paste0("rm -r ",winnower_log_errors_dir,"/",job_name,".e*"))
    system(paste0("rm -r ",winnower_log_output_dir,"/",job_name,".o*"))
    system(paste0("rm -r ",j_winnower_path,"/*.dta"))
  }
}else{
  codebook <- fread(paste0(
    extraction_code_dir,
    codebook_name,
    "_codebook.csv"
  ))
}


# Step 1 - Check for potential file output duplicates ---------------------

###########
#
# With duplicate files, go into codebook to confirm that they are duplicates, and if not, which survey is the one that should be kept. 
#
# If there are any duplicates, you'll need to rename output of one survey and re-extract the duplicate survey. 
#
############

duplicate_files <- check_4_winnower_duplicates(codebook)


# Step 2 - Get list of UbCov IDs to extract -------------------------------

###########
#
# This function will return a list of UbCov IDs that still need to be extracted based on file paths provided above for Winnower outputs
#
############

ubcov_ID_vec <- get_ubcov_ids(
  codebook,
  j_path = j_winnower_path,
  l_path = l_winnower_path
) #will add L-Drive path once given access

write.csv(
  x = data.frame(ubcov_ID_vec=ubcov_ID_vec),
  row.names = F,
  file = paste0(extraction_code_dir,"ubcov_ids.csv")
) #this will act as a parameter map for the array job extraction template to reference ubcov ids


# Step 3 - Extract UbCov IDs ----------------------------------------------

###########
#
# These functions will run a the run_extract process on the UbCov IDs designated above. You can rerun this step as many times as needed
#
# NOTE - before starting this step, make sure you have winnower installed and that your .winnower.ini file has been setup in your home directory
#
############

#define input parameters - CUSTOMIZE THESE BEFORE EXTRACTING!!
mem <- 10 #the amount of memory needed 
threads <- 4 #thread count
max_concurrently_running <- 100 #the amount of jobs to run in parallel (try to keep extraction load < 1 TB)
run_time <- 20 # time in minutes
partition <- "long.q"
array_string <- paste0("1-", length(ubcov_ID_vec), "%", max_concurrently_running)

#Step 3.1 - run the extraction process

nch::submit_job(
  script = launch_script,
  script_args = c(extraction_code_dir, codebook_name, job_name, max_concurrently_running),
  job_name = job_name,
  memory = mem,
  ncpus = threads,
  time = run_time,
  array = array_string,
  partition = partition,
  output = file.path(winnower_log_output_dir,"%x.o%j"),
  error = file.path(winnower_log_errors_dir, "%x.e%j"),
  archive = T
)

###############
#
# once extraction is completed, run steps 3.2 - 3.4 below
#
###############

#####
#Step 3.2 - get ubcov ids that errored out
bad_ubcov_ids <- find_errors(
  output_path = winnower_log_errors_dir,
  job_name = job_name,
  cb = codebook,
  j_path = j_winnower_path
)

#create a dataframe of just the surveys that encountered an error (this is just for reference to help in the debugging process)
error_codebook <- compile_error_codebook(
  cb = codebook,
  bad_id_vec = bad_ubcov_ids
)

#now you can print all of the output from the files that were logged during the extraction process to help identify the error that needs to be debugged
print_error_log(
  output_dir = winnower_log_errors_dir,
  job_name = job_name,
  bad_id_vec = bad_ubcov_ids,
  out_log_file = paste0(extraction_code_dir,"/",job_name,"_full_error_log.txt")
)

#####
#Step 3.3 (optional) - run local extracts on each ubcov ID in the "bad_ubcov_ids" vec to identify the error (or run one at a time)
#you can turn full_debug=True if you want to see all winnower output in console window
#Note - to run this in your RStudio session, you'll need a session with at least 4G of ram, 1 thread, and to be on a sarchive node (to access the J drive)
#THIS MAY NOT PROVIDE ENOUGH OUTPUT TO DEBUG. If needed, you may need to run these bad UbCov IDs on the cluster. Refer to readME to learn more on how to do that!
run_local_extract(codebook_name = codebook_name,
                  ubcov_ids = 123456,
                  full_debug = F)

#####
#Step 3.4 - obtain new list of ubcov ids to run, where you'll choose either:
# 1) an ubcov vector where you can obtain all ubcov ids that haven't been extracted (that also haven't errored out)
ubcov_ID_vec <- get_ubcov_ids(df = codebook,
                              j_path = j_winnower_path,
                              l_path = l_winnower_path,
                              bad_id_vec = bad_ubcov_ids)

# 2) OR try to re-extract all bad_ubcov_ids if errors have been identified
ubcov_ID_vec <- bad_ubcov_ids

# 3) OR a user defined ubcov ID vec (for example - find the ubcov IDs that errored out due to memory constraints and supply those with more memory in the submit jobs above)
ubcov_ID_vec <- c()

#and then write the ubcov IDs out to the csv parameter map
write.csv(data.frame(ubcov_ID_vec),
          row.names = F,
          file = paste0(extraction_code_dir,"ubcov_ids.csv")) #this will act as a parameter map for the array job extraction template to reference ubcov ids

#now rerun steps 3.1-3.4 and repeat process until all ubcov ids that needed to be extracted have been!

###########
# 
# Step 4 - Review Winnower Outputs
#
# After extracting all of the necessary surveys, check outputs of Winnower extractions to ensure everything was extracted properly.
#
############

