rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- paste0("USERNAME")
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')

source(paste0("FILEPATH"))

#bundle_id = 211
#Takes in a bundle, returns a data.table with age subsetted to <70 years and all "cv_marketscan_*" columns 
#reduced to only one cv_marketscan column, "cv_marketscan"

bundle_subset <- function(bundle_id){
  bun_data <- fread(paste0("FILEPATH"))
  bun_cols <- colnames(bun_data)
  cv_cols <- bun_cols[grep('cv_marketscan_[a-z0-9]', bun_cols)]
  list_non_cv <- setdiff(bun_cols, cv_cols)
  bun_data_age_cv_subset <- subset(bun_data, age_start<70.0, select=list_non_cv)
  return(bun_data_age_cv_subset)
}

#Set arguments to be submitted in submission script
bundles <- c()

measures <- c("prevalence", "incidence")

#Script submit function, submits crosswalks by bundle and measure
submit_bundle_crosswalk <- function(bundles, measures){
  #Job specifications
  username <- "jab0412"
  m_mem_free <- "2G"
  fthread <- 1
  shell <- " FILEPATH"
  script <- " FILEPATH"
  for (bundle in bundles){
    for (measure in measures){
      job_name <- paste0("-N", "bundle_", bundle, "_", measure)
      job <- paste0("qsub -l m_mem_free=", m_mem_free, " -l fthread=", fthread, " ",
                    job_name, " -P proj_custom_models -o FILEPATH", username, shell, script, " ", bundle, " ", measure)
      
      system(job); print(job_name)
    }
  }
}

  
