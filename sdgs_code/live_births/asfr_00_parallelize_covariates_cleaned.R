################################################################################
## Purpose: Submit Script to Create ASFR Covariates at Draw Level - Draw Level Age Group Extension Occurs Independently
################################################################################

################################################################################
### Setup
################################################################################

rm(list=ls())

# load packages 
if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, magrittr) # load packages and install if not installed
 

username <- ifelse(Sys.info()[1]=="Windows","[username]",Sys.getenv("USER"))

j <- ifelse(Sys.info()[1]=="Windows","J:","/home/j")
h <- ifelse(Sys.info()[1]=="Windows","H:",paste0("/homes/", username))

################################################################################
### Arguments 
################################################################################
locsetid <- 21
mod_id <- 12
cores <- 6 #for parallelization in rake script

test <- F     # if T,  no jobs are submitted
start <- 4      # code piece to start at
end <- 4           # the last piece of code to be run


code_dir <- "FILEPATH"

################################################################################
### Functions 
################################################################################
setwd("FILEPATH")
source("FILEPATH")
setwd("FILEPATH")
source("FILEPATH")
setwd("FILEPATH")


################################################################################
### Data 
################################################################################

locs <- get_location_metadata(location_set_id = locsetid)[level >= 3, location_id]

################################################################################
### Code 
################################################################################

#######################
## Step 1: Merge UN WPP (National) Age Patterns with those from Locations We Model Ourselves
#######################

if (start <= 1 & end >= 1) {
    
    qsub(jobname = "fertility_combine_asfr",
         code = paste0(code_dir, "01_combine_asfr.R"), 
         hold = NULL,
         slots = 1, 
         pass = list(mod_id),
         submit = !test
    )
    
}

#######################
## Step 2: Create ASFR draws by scaling ASFR to TFR by location_id
#######################
count_02 <- 0
job_ids <- NULL

if (start <= 2 & end >= 2) {
    
    
    
    for (loc_id in locs) {
        
        count_02 <- count_02 + 1
        
        qsub(jobname = paste0("fertility_scale_asfr_tfr_", loc_id),
             code = paste0(code_dir, "01_scale_unpop_by_draw.R"), 
             hold = "fertility_combine_asfr",
             slots = 2, 
             pass = list(mod_id, loc_id),
             submit = !test
        )
        
        job_ids[count_02] <- paste0("fertility_scale_asfr_tfr_", loc_id)
        
    }

}  

#########################
## Step 3: Rake ASFR Draws (only matters for subnationals)
#########################

if (start <= 3 & end <= 3) {
    
    qsub(jobname = "fertility_rake_asfr",
         code = paste0(code_dir, "01_rake_asfr.R"), 
         hold = paste(append(c("fertility_combine_asfr"), job_ids), collapse=","),
         slots = cores, 
         pass = list(mod_id, cores),
         submit = !test
    )
    
}

#########################
## Step 4: Calculate Births by Location
#########################
count_04 <- 0
job_ids_04 <- NULL

if (start <= 4 & end >= 4) {
    
    
    
    for (loc_id in locs) {
        
        count_04 <- count_04 + 1
        
        qsub(jobname = paste0("fertility_births_draws_", loc_id),
             code = paste0(code_dir, "01_births_draws.R"), 
             hold = paste(append(c("fertility_combine_asfr", "fertility_rake_asfr"), job_ids), collapse=","),
             slots = 2, 
             pass = list(mod_id, loc_id),
             submit = !test
        )
        
        job_ids_04[count_04] <- paste0("fertility_births_draws_", loc_id)
        
    }
    
}  

##########################

##########################


################################################################################ 
### End
################################################################################