####################################################################################################
## STGPR_SENDOFF.R
## ST-GPR final data prep and model sendoff for utilization envelope
####################################################################################################

rm(list=ls())

library(data.table)
library(dplyr)
library(Hmisc)
library(openxlsx)

## ------------------------------------------------------------------------------------------
## PREP DATA
## Read in file
to_model <- as.data.table(read.csv("FILEPATH"))

cleanup <- function(df){
  df$val <- df$mean
  df[standard_error >= 1, standard_error := 0.999]
  df$variance <- df$standard_error^2
  df[sex == "Male", sex_id := 1]
  df[sex == "Female", sex_id := 2]
  df$year_id <- ceiling((df$year_start + df$year_end)/2)
  df <- df[,c("nid","location_id","year_id","age_group_id","sex_id","measure","val","variance","sample_size","is_outlier",
              "seq","crosswalk_parent_seq","sex","age_start","age_end","note_modeler")]
  df[(location_id == 4770 & age_group_id %in% c(32, 235) & val > 5), is_outlier := 1]
  return(df)
}

to_model <- cleanup(to_model)
for_model <- to_model %>% distinct(nid, location_id, year_id, age_group_id, sex_id, 
                                                    measure, val, variance, sample_size, is_outlier, sex, age_start, age_end, .keep_all = TRUE)



## ------------------------------------------------------------------------------------------
## SAVE FIlE 
write.csv(for_model, "FILEPLATH")


## ------------------------------------------------------------------------------------------

## RUN ST-GPR
central_root <- "FILEPATH"
setwd(central_root)
source("FILEPATH/register.R")
source("FILELPATH/sendoff.R")

run_id_3 <- register_stgpr_model("FILEPATH", model_index_id = 3)
stgpr_sendoff(run_id_3, "proj_hospital")
