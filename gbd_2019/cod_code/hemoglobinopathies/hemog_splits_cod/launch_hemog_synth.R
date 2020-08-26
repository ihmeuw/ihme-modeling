rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- "USERNAME"
}

library("stringr")
library("tidyverse")
#################

source(paste0("FILEPATH"))


next_script <- paste0("FILEPATH")


#template_data <- get_model_results('cod', model_version_id = 620900, decomp_step = 'step4')
#locs <- unique(template_data$location_id)

#Next section for missing jobs
#test_step4 <- Sys.glob('/ihme/scratch/users/jab0412/hemog/decomp4/data_rich/618/2/*')
#s1 <- sapply(strsplit(test_step4, split='/', fixed=TRUE), function(x) (x[11]))
#s2 <- sapply(strsplit(s1, split='_', fixed=TRUE), function(x) (x[1]))
#s3 <- unique(s2)

#This part to combine dr and regular data in one folder
#locs <- locs[!locs %in% s3]

locs <- get_location_metadata(35, gbd_round_id = 7)

locs <- locs %>%
  filter(most_detailed == 1) %>%
  select(location_id) 

template_data <- get_model_results('cod', model_version_id = 620903, decomp_step = 'step4')
locs <- unique(template_data$location_id)

#locs <- locs$location_id[!locs$location_id %in% dr_locs]

for (loc in locs){
  job_name <- paste0("data_rich_hemog_sub_", loc)
  
  system(paste( "qsub -P proj_custom_models -l m_mem_free=5G -l fthread=1 -q long.q  -N ", job_name, "FILEPATH", "-s", next_script, loc))
}



