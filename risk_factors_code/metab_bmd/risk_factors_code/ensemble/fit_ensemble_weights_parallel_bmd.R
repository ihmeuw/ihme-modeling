## May 1, 2017
## Fit bmd by KS ensemble local run

## June 28, 2017
## Restrict the distribution weight choices
## Rerun updated optimization function

rm(list = ls())
if (Sys.info()["sysname"] == "Darwin") j_drive <- "FILEPATH"
if (Sys.info()["sysname"] == "Linux") j_drive <- "FILEPATH"
if (Sys.info()["sysname"] == "Windows") j_drive <- "FILEPATH"  ### add to run locally

library(data.table)
library(dplyr)
library(rio)
library(Matrix)  
library(lme4)
library(mvtnorm)
library(actuar)
library(zipfR)

## functions
source(paste0(j_drive,"FILEPATH/eKS_parallel.R"))  #run with updated optimization function
source(paste0(j_drive,"FILEPATH/ihmeDistList_bmd.R")) # run with restricted distribution list

set.seed(52506)

dlist <- c(classA, classB, classM)

nhanes <- read.csv("FILEPATH.csv")  ### add bmd data
nhanes.m <- subset(nhanes,nhanes$sex==1)
nhanes.f <- subset(nhanes,nhanes$sex==2)
nhanes.m.y <- subset(nhanes.m,nhanes.m$age<70)
nhanes.m.o <- subset(nhanes.m,nhanes.m$age>69)
nhanes.f.y <- subset(nhanes.f,nhanes.f$age<70)
nhanes.f.o <- subset(nhanes.f,nhanes.f$age>69)

Data.m.y <- as.vector(nhanes.m.y$bmd)  
Data.m.o <- as.vector(nhanes.m.o$bmd)
Data.f.y <- as.vector(nhanes.f.y$bmd)
Data.f.o <- as.vector(nhanes.f.o$bmd)

## Fit ensemble distribution
FIT.m.y <- eKS(Data = Data.m.y, distlist = dlist)
FIT.m.o <- eKS(Data = Data.m.o, distlist = dlist)
FIT.f.y <- eKS(Data = Data.f.y, distlist = dlist)
FIT.f.o <- eKS(Data = Data.f.o, distlist = dlist)

## Plot KS
pdf(paste0(j_drive,"FILEPATH.pdf"))
plot_KS_fit(Data.m.y,FIT.m.y$best_weights,mean=mean(Data.m.y,na.rm=T),variance=var(Data.m.y,na.rm=T),xmin=min(Data.m.y,na.rm=T),xmax=max(Data.m.y,na.rm=T),dlist=dlist)
dev.off()
pdf(paste0(j_drive,"FILEPATH.pdf"))
plot_KS_fit(Data.m.o,FIT.m.o$best_weights,mean=mean(Data.m.o,na.rm=T),variance=var(Data.m.o,na.rm=T),xmin=min(Data.m.o,na.rm=T),xmax=max(Data.m.o,na.rm=T),dlist=dlist)
dev.off()
pdf(paste0(j_drive,"FILEPATH.pdf"))
plot_KS_fit(Data.f.y,FIT.f.y$best_weights,mean=mean(Data.f.y,na.rm=T),variance=var(Data.f.y,na.rm=T),xmin=min(Data.f.y,na.rm=T),xmax=max(Data.f.y,na.rm=T),dlist=dlist)
dev.off()
pdf(paste0(j_drive,"FILEPATH.pdf"))
plot_KS_fit(Data.f.o,FIT.f.o$best_weights,mean=mean(Data.f.o,na.rm=T),variance=var(Data.f.o,na.rm=T),xmin=min(Data.f.o,na.rm=T),xmax=max(Data.f.o,na.rm=T),dlist=dlist)
dev.off()

## Write best weights file
W.m.y = data.table(FIT.m.y$best_weights)
W.m.o = data.table(FIT.m.o$best_weights)
W.f.y = data.table(FIT.f.y$best_weights)
W.f.o = data.table(FIT.f.o$best_weights)

fwrite(W.m.y,paste0(j_drive,"FILEPATH.csv"))
fwrite(W.m.o,paste0(j_drive,"FILEPATH.csv"))
fwrite(W.f.y,paste0(j_drive,"FILEPATH.csv"))
fwrite(W.f.o,paste0(j_drive,"FILEPATH.csv"))
print("DONE!")

FIT.m.y <- read.csv(paste0(j_drive,"FILEPATH.csv"))
FIT.m.o <- read.csv(paste0(j_drive,"FILEPATH.csv"))
FIT.f.y <- read.csv(paste0(j_drive,"FILEPATH.csv"))
FIT.f.o <- read.csv(paste0(j_drive,"FILEPATH.csv"))

## Get density function
Edensity.m.y <- get_edensity(FIT.m.y,min=min(Data.m.y,na.rm=T),max=max(Data.m.y,na.rm=T),mean=mean(Data.m.y,na.rm=T),variance=var(Data.m.y,na.rm=T),distlist=dlist)
Edensity.m.o <- get_edensity(FIT.m.o,min=min(Data.m.o,na.rm=T),max=max(Data.m.o,na.rm=T),mean=mean(Data.m.o,na.rm=T),variance=var(Data.m.o,na.rm=T),distlist=dlist)
Edensity.f.y <- get_edensity(FIT.f.y,min=min(Data.f.y,na.rm=T),max=max(Data.f.y,na.rm=T),mean=mean(Data.f.y,na.rm=T),variance=var(Data.f.y,na.rm=T),distlist=dlist)
Edensity.f.o <- get_edensity(FIT.f.o,min=min(Data.f.o,na.rm=T),max=max(Data.f.o,na.rm=T),mean=mean(Data.f.o,na.rm=T),variance=var(Data.f.o,na.rm=T),distlist=dlist)

## Plot fit function
pdf(paste0(j_drive,"FILEPATH.pdf"))
plot_fit(Data.m.y,FIT.m.y,Edensity.m.y,"BMD male 40-69 best fits",distlist = dlist)
dev.off()
pdf(paste0(j_drive,"FILEPATH.pdf"))
plot_fit(Data.m.o,FIT.m.o,Edensity.m.o,"BMD male 70-80 best fits",distlist = dlist)
dev.off()
pdf(paste0(j_drive,"FILEPATH.pdf"))
plot_fit(Data.f.y,FIT.f.y,Edensity.f.y,"BMD female 40-69 best fits",distlist = dlist)
dev.off()
pdf(paste0(j_drive,"FILEPATH.pdf"))
plot_fit(Data.f.o,FIT.f.o,Edensity.f.o,"BMD female 70-80 best fits",distlist = dlist)
dev.off()