###########################################################
### Project: ubCov
### Purpose: Collapse ubcov extraction output
###########################################################
###################
### Setting up ####
###################
rm(list=ls())

## Load Functions
ubcov_central <- "FILEPATH"
setwd(ubcov_central)
source("FILEPATH")

######################################################################################################################

## Settings

topic <- "wash" ## wash or census
config.path <- "FILEPATH"
parallel <- F ## Run in parallel?
slots <- ifelse(topic == "wash", 1, 50) ## How many slots per job (used in mclapply) | Set to 1 if running on desktop
logs <- "FILEPATH"
cluster_proj <- 'ADDRESS'

### Launch collapse
df <- collapse.launch(topic=topic, file.list=NA, config=NULL, config.path=config.path, parallel=parallel, fthreads=2, m_mem_free=5, h_rt="00:10:00", 
                       logs=logs, cluster_project=cluster_proj, central.root="FILEPATH", write_file=TRUE)