# ---HEADER--------------------------------------------------------------------------------------------------
# Project: Split cirrhosis cod model by etiology proportions
#------------------------------------------------------------------------------------------------------------

# source functions
cirrhosis_repo<- FILEPATH
source("FILEPATH/split_cod_model.R")

# pass args 
args<-commandArgs(trailingOnly = TRUE)
out_dir<-args[1]
ds<-args[2]

system("echo $PATH")
#-------------------------------------------------------------------------------------------------------------

# ---SPLIT----------------------------------------------------------------------------------------------------
split_cod_model(source_cause_id=OBJECT,
                target_cause_ids=OBJECT,
                target_meids=OBJECT,
                output_dir = "FILEPATH",
                gbd_round_id = 6, 
                decomp_step = OBJECT)
#-------------------------------------------------------------------------------------------------------------