###################################################################################################################

rm(list=ls())


##load in shared functions
pacman::p_load(data.table, ggplot2, dplyr, readr, tidyverse, openxlsx, tidyr, plyr, readxl)
source(paste0(FILEPATH, "/get_location_metadata.R"))
source(paste0(FILEPATH, "/save_crosswalk_version.R"))

#set parameters 
acause <- CAUSE_NAME
cause_id <- CAUSE_ID
bundle <- BUNDLE_ID
nid <- nid
sex <- SEX_ID


loc_table <- get_location_metadata(gbd_round_id = 7, location_set_id=35)
loc_table <- loc_table[most_detailed == 1, ]
location_ids <- loc_table[,location_id]
location_ids<- sort(location_ids)

date <- gsub("-", "_", Sys.Date())
filepath <- FILEPATH
dir.create(filepath, showWarnings = F)


#############################################################################################################################################
## QSUB for parallelization
#############################################################################################################################################


##qsub and send jobs for each cause, location

for (loc_id in location_ids) {
  # qsub arguments:
  threads <- 2
  memory <- "5G"
  time <- "00:10:00"
  proj <- "-P proj_rgud "
  sge_output_dir <- " -o FILEPATH "
  sge_error_dir <- " -e FILEPATH "
  shell <-  " ADDRESS -i /ADDRESS " 
  rscript <- "FILEPATH"
  jobname <- paste0(acause, "_", loc_id)
  
  command <- paste0("qsub -q all.q ", proj, sge_output_dir, sge_error_dir,
                    " -l m_mem_free=", memory, 
                    " -l fthread=", threads, 
                    " -N ", jobname, " -l h_rt=", 
                    time,  " -l archive=True ",
                    shell, " -s ", rscript, " ", acause, " ",cause_id, " ", bundle, " ", loc_id, " ", sex)
  
  system(command)   
}




#############################################################################################################################################
##LOAD FILES AND OUTPUT ONE COMBINED FILE FOR EACH VIRUS
#############################################################################################################################################
#load all csmr files and append them together
dir <- FILEPATH


file.list.all <- list.files(dir, full.names = F)

dt <- fread(paste0(dir, file.list.all[1]))

for(i in 2:length(file.list.all)) {
  file.dt <- fread(paste0(dir, file.list.all[i]))
  dt <- rbind(dt, file.dt)
}


#update seq column to be new, consecutive range
dt$seq <- NA
dt$crosswalk_parent_seq <- NA
dt$nid <- nid

#############################################################################################################################################
##APPEND CSMR DATA WITH NONFATAL DATA
#############################################################################################################################################

dt1 <- subset(dt, lower==upper) #
dt2 <- subset(dt, lower!=upper)

master <- as.data.table(read.xlsx(FILEPATH))
df_master <-subset(master, measure=="prevalence" | measure=="incidence")
append <- rbind.fill(df_master, dt2)

output_filepathlit <- FILEPATH
write.xlsx(append, output_filepathlit, sheetName = "extraction", col.names=TRUE)


######APPEND CSMR DATA WITH OTHER INPUT DATA

########################################################################################
#Upload MAD-outliered new data 
description2 <- DESCRIPTION
result <- save_crosswalk_version( BUNDLE_VERSION_ID, output_filepathlit, description=description2)


