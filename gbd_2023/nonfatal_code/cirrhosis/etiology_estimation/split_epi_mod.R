# ---HEADER--------------------------------------------------------------------------------------------------
# Project: Split decompensated cirrhosis envelope, decompensated cirrhosis without heart failure, and compensated cirrhosis into 5 etiologies (alcohol, HBV, HCV, NASH, and other causes)
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  h_root <- '~/'
} else { 
  j_root <- 'J:/'
  h_root <- 'H:/'
}


# source functions
source("FILEPATH/split_epi_model.R")

# read in map of ME to measure id 

# pass args 
args<-commandArgs(trailingOnly = TRUE)
out_dir<-args[1]
source_me<-as.numeric(args[2])
rel_id<-args[3]

#-------------------------------------------------------------------------------------------------------------

# ---SPLIT----------------------------------------------------------------------------------------------------
# set options for split 

#target_mes<- c(2892, 2893, 2891, 2894, 19682 ) #for decompensated envelope
#target_mes <- c(11655, 11656, 11657, 11658, 19681) #for compensated
target_mes<- c(26964, 26965, 26966, 26967, 26968) #for decompensated without heart failure

prop_mes<-c(1920, 1921, 1922, 24359, 24358)

# specify output directory 
dir.create(out_dir,recursive = TRUE)

#Measure ID should be 5 and 6 for compensated only. 
#The decompensated and decompensated without HF should only be prevalence, similar to previous rounds
split_epi_model(source_meid=source_me,
                target_meids=target_mes,
                prop_meids=prop_mes,
                #split_measure_ids = c(5,6), 
                split_measure_ids = 5,
                output_dir = out_dir,
                release_id = rel_id)

#-------------------------------------------------------------------------------------------------------------