###########################################################
### Author: 
### Date: 10/20/17
### Project: Education Dementia meta-analysis  
### Purpose: GBD 2017 Nonfatal Estimation
###########################################################

## SET-UP
rm(list=ls())

if (Sys.info()[1] == "Linux"){
  j_root <- "/home/j/"
  h_root <- "~/"
} else if (Sys.info()[1] == "Windows"){
  j_root <- "J:/"
  h_root <- "H:/"
}

library(pacman, lib.loc = FIELPATH)
pacman::p_load(data.table, ggplot2, readr, openxlsx)
library(nlme, lib.loc = paste0(j_root, FILEPATH))
library(metafor, lib.loc = paste0(j_root, FILEPATH))

## SET OBJECTS
data_dir <- paste0(j_root, FILEPATH)
doc_dir <- paste0(j_root, FILEPATH)
date <- gsub("-", "_", Sys.Date())

## GET DATA
dt <- as.data.table(read.xlsx(paste0(data_dir, "education_rrdata.xlsx")))

## MANIPULATE 3-YEAR DATA AND CALC STANDARD ERROR
dt[author == "Yamada", `:=` (or = or^(1/3), lower = lower^(1/3), upper = upper^(1/3))]
dt[, se := ((log(upper)-log(or))/1.96 + (log(or)-log(lower))/1.96)/2]

## META-ANALYSIS
model <- rma.uni(data = dt, yi = or, sei = se)

## RESULTS
pdf(paste0(doc_dir, "educationdementia.pdf"))
forest(model, showweights = T, slab = paste0(dt$author, " : ", dt$country, " ", dt$year), 
       xlab = "Relative Risk of Dementia per Additional Year of Education", refline = "NA")
dev.off()
