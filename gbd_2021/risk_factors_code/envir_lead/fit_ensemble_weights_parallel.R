#####################################################################################
## Project: RF: Lead Exposure
## Purpose: Fit ensemble weights on lead microdata
#####################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Darwin") j_drive <- "FILEPATH"
if (Sys.info()["sysname"] == "Linux") j_drive <- "FILEPATH"

## load packages
library(data.table)
library(foreach)
library(iterators)
library(doParallel)
library(dplyr)
library(rio)
library(dfoptim)
library(fitdistrplus)
library(RColorBrewer)
library(ggplot2)
library(actuar)
library(grid)
library(lme4)
library(mvtnorm)
library(zipfR)
library(dfoptim, lib.loc = "FILEPATH")

## functions (must run on cluster)
source("FILEPATH/eKS_parallel.R")
source("FILEPATH/ihmeDistList.R")

set.seed(52506)


arg <- c("lead")

## DEBUG = 1
if (exists("DEBUG")=="TRUE") {
  dl <- "lead"
} else dl <- as.character(arg[1])

print(paste0("Risk: ",dl))

d <- gsub("lead_", "", dl)

dlist <- c(classA, classB, classM)

## all demographics for saving weights
base <- expand.grid(location_id = 1, year_id = c(seq(1990,2015,5),2019,2020:2022), sex_id = c(1,2),
                    age_group_id = c(2,3,6:20,30:32,34,235,238,388,389), N = 1)

## load lead data 
lead <- fread(file.path(j_drive,'FILEPATH/compiled_lead_microdata.csv'))
lead[,data := data + 0.001]
Data <- as.vector(lead$data)

## Fit ensemble distribution
FIT <- eKS(Data = Data, distlist = dlist)

M <- mean(Data,na.rm=T)
VAR <- var(Data,na.rm=T)
XMIN <<- min(Data,na.rm=T)
XMAX <<- max(Data,na.rm=T)

## Get density function
Edensity <- get_edensity(FIT$best_weights,min=XMIN,max=XMAX,mean=M,variance=VAR,distlist=dlist)

## Plot fit function
pdf(paste0(j_drive,"FILEPATH/eKS_fit.pdf"))
plot_fit(Data,FIT$best_weights,Edensity,paste0(d," best fits"),distlist = dlist)
dev.off()

## Plot KS
pdf(paste0(j_drive,"FILEPATH/eKS_KS.pdf"))
plot_KS_fit(Data,FIT$best_weights,mean=M,variance=VAR,xmin=XMIN,xmax=XMAX,dlist=dlist)
dev.off()

## Write best weights file
W = data.table(FIT$best_weights)
W <- W %>% dplyr::mutate(N = 1)
OUT <- left_join(base,W)
OUT <- OUT %>% dplyr::select(-N)

write.csv(OUT,paste0(j_drive,"FILEPATH/envir_lead_blood.csv"),row.names = F)

print("DONE!")

## END

