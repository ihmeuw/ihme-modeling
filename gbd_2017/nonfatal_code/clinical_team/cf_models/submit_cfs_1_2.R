
rm(list = ls())
library(ggplot2)
library(data.table)
library(lme4)
library(boot)
library(RMySQL)
library(slackr)
library(mortcore, lib = "filepath")
library(parallel)
library(magrittr)

source('filepath')
source('filepath')
source('filepath')
source('filepath')


## Submit script with qsub
prep_data <- fread('filepath')

## Set True/False for make_draws
lapply(unique(prep_data$bundle_id), function(bundle){

  qsub()
})

## CF2 submit
#lapply(unique(prep_data$bundle_id), function(bundle){
## When not using draws can use like no slots

files_cf1 <- list.files('filepath')
files_cf1 <- gsub('filepath')
files_cf1 <- gsub('.csv', '', files)

files_cf2 <- list.files('filepath')
files_cf2 <- gsub('filepath')
files_cf2 <- gsub('.csv', '', files)

files_cf3 <- list.files('filepath')
files_cf3 <- gsub('filepath', '', files)
files_cf3 <- gsub('.csv', '', files)

small <- files[sapply(files, file.size) < 6000000]

small <- gsub('filepath')
small <- gsub('.csv', '', small)

lapply(unique(prep_data$bundle_id), function(bundle){  
  qsub()
})
