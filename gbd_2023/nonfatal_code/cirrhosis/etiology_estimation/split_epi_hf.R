# # ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  h_root <- '~/'
} else { 
  j_root <- 'J:/'
  h_root <- 'H:/'
}

# require functions
#require(data.table)
#require(openxlsx)

# source functions
source("FILEPATH/split_epi_model.R")

# read in map of ME to measure id 

# pass args 
args<-commandArgs(trailingOnly = TRUE)
output_version<-args[1]
source_me<-as.numeric(args[2])
rel_id<-args[3]

#-------------------------------------------------------------------------------------------------------------

# ---SPLIT----------------------------------------------------------------------------------------------------
# set options for split 
target_mes <- c(25886, 25888, 25884, 25890, 25892) #for heart failure 
prop_mes<-c(1920, 1921, 1922, 24358, 24359) # cirrhosis proportion mes 

# specify output directory 
out_dir <- paste0("FILEAPTH", output_version)
dir.create(out_dir,recursive = TRUE)

split_epi_model(source_meid=source_me,
                target_meids=target_mes,
                prop_meids=prop_mes,
                split_measure_ids = 5, 
                output_dir = out_dir,
                release_id = rel_id)

#-------------------------------------------------------------------------------------------------------------