# ---HEADER--------------------------------------------------------------------------------------------------
# Project: Split cirrhosis model by etiology proportions
#------------------------------------------------------------------------------------------------------------

# require functions
require(data.table)
require(openxlsx)

# source functions
source("FILEPATH/split_epi_model.R")

# read in map of ME to measure id 

# pass args 
args<-commandArgs(trailingOnly = TRUE)
output_version<-args[1]
source_me<-as.numeric(args[2])
ds<-args[3]

#-------------------------------------------------------------------------------------------------------------

# ---SPLIT----------------------------------------------------------------------------------------------------
# set options for split 
target_mes <- OBJECT #for decompensated 
target_mes <- OBJECT #for compensated
prop_mes <-OBJECT

# specify output directory 
outdir <- FILEPATH
dir.create(out_dir,recursive = TRUE)

split_epi_model(source_meid=source_me,
                target_meids=target_mes,
                prop_meids=prop_mes,
                output_dir = out_dir,
                gbd_round_id = 6, 
                decomp_step = ds)
#-------------------------------------------------------------------------------------------------------------