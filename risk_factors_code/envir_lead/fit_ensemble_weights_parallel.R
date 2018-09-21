
## Generate ensemble weights for blood lead

rm(list=ls())

h<-paste0("FILEPATH")

h_lib<-paste0(h, "FILEPATH")

#library(haven, lib.loc=kc_lib)
library(data.table, lib.loc=h_lib)
library(foreach, lib.loc=h_lib)
library(iterators, lib.loc=h_lib)
library(doParallel, lib.loc=h_lib)
library(dplyr, lib.loc=h_lib)
library(rio, lib.loc=h_lib)
library(dfoptim, lib.loc=h_lib)
library(fitdistrplus, lib.loc=h_lib)
library(RColorBrewer, lib.loc=h_lib)
library(ggplot2, lib.loc=h_lib)
library(actuar, lib.loc=h_lib)
library(grid, lib.loc=h_lib)
library(lme4, lib.loc=h_lib)
library(mvtnorm, lib.loc=h_lib)
library(zipfR, lib.loc=h_lib)

if (Sys.info()["sysname"] == "Darwin") j_drive <- "FILEPATH"
if (Sys.info()["sysname"] == "Linux") j_drive <- "FILEPATH"

## functions
source(paste0(j_drive,"FILEPATH"))
source(paste0(j_drive,"FILEPATH"))

set.seed(52506)

arg <- c("lead")

## DEBUG = 1
if (exists("DEBUG")=="TRUE") {
  dl <- "lead"
} else dl <- as.character(arg[1])

print(paste0("Risk: ",dl))

d <- gsub("lead_", "", dl)

dlist <- c(classA, classB, classM)

## load BMD template to save weights to for central PAF comp
base <- import(paste0(j_drive,"FILEPATH"))
base <- base %>% dplyr::select(location_id,year_id,sex_id,age_group_id)
base <- base %>% dplyr::mutate(N = 1)

## load lead data 
lead <- fread(file.path(j_drive,'FILEPATH'))
lead[,bll := bll + 0.001]
Data <- as.vector(lead$bll)

## Fit ensemble distribution
FIT <- eKS(Data = Data ,distlist = dlist)

M <- mean(Data,na.rm=T)
VAR <- var(Data,na.rm=T)
XMIN <<- min(Data,na.rm=T)
XMAX <<- max(Data,na.rm=T)

## Get density function
Edensity <- get_edensity(FIT$best_weights,min=XMIN,max=XMAX,mean=M,variance=VAR,distlist=dlist)

## Plot fit function
pdf(paste0(j_drive,"FILEPATH",dl,".pdf"))
plot_fit(Data,FIT$best_weights,Edensity,paste0(d," best fits"),distlist = dlist)
dev.off()

## Plot KS
pdf(paste0(j_drive,"FILEPATH",dl,".pdf"))
plot_KS_fit(Data,FIT$best_weights,mean=M,variance=VAR,xmin=XMIN,xmax=XMAX,dlist=dlist)
dev.off()

## Write best weights file
W = data.table(FIT$best_weights)
W <- W %>% dplyr::mutate(N = 1)
OUT <- left_join(base,W)
OUT <- OUT %>% dplyr::select(-N)

write.csv(OUT,paste0(j_drive,"FILEPATH",dl,".csv"),row.names = F)

print("DONE!")



