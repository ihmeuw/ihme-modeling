################
################
## 
## Goal: Use Copula to model bivariate joint distribution of birthweight and gestational age, given marginal distributions of birthweight and gestational age. 
## Simulate existing US microdata of joint birthweight and gestational age data. 
## Use best simulation parameters to model bivariate distribution of birthweight and gestational age in location/years were there is no joint distribution data   
##
## Implementation of Copula using this methodology requires: 
##    1) Selection of Copula family
##    2) Marginal distributions of birthweight and gestational age (distribution family needs to be selected for both marginals - can be different distribution family for each marginal)
##     
## 
##  Selection of Copula family is decided using the VineCopula R Package, which uses the bivariate data to choose the best fitting the Copula
##
################
################



#############################

rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- "J:/"
  h <-"H:/"
  
  
} else {
  j<- "/home/j/"
  h<-"/homes/USER/"
  
}

library(copula)
library(VineCopula)
library(data.table)
library(fitdistrplus)
library(psych)

set.seed(500)

## Load Joint Vector sample of Microdata

dt <- readRDS("FILEPATH")

plot(bw, gestweek, pch='.', main = "Birthweight vs Gestational Week Scatter")
abline(lm(gestweek~bw), col = 'red', lwd=1)
cor(bw, gestweek,method='spearman')


## To decrease compuation time, take a random sample of size 100,000

sample_size <- 100000
dt_sample <- dt[sample(nrow(dt), sample_size), ] # Random sample




#############################################################

## Select appropriate copula family using the BiCopSelect function from the VineCopula R package
## This function chooses the best fit Copula family and parameters given the joint gestational age and birthweight microdata
## 
## The appropriate copula family selected by the VineCopula R package is "Survival Joe" or "Survival BB8"



u <- pobs(as.matrix(cbind(bw_sample,gestweek_sample)))[,1]
v <- pobs(as.matrix(cbind(bw_sample,gestweek_sample)))[,2]
selectedCopula <- BiCopSelect(u,v,familyset = 20, method = "mle")   ## Here the BiCopula is being set to choose a Survival BB8 Copula, and then fitting the paramaters. 
## Change familyset to = 16 for a Survival BB8 copula, or to = NA if you want the function to pick from all the families
## However, setting to = NA takes some time to run, so I've set to 20, which is a good fit for the data
## See VineCopula documentation for more copula families
## Copula selected can be rotated -- see "Survival" or "180" copulas

summary(selectedCopula)


## Save parameters from output here. Note that for some copula families (such as surivival Joe), there is only 1 parameter.
param <- selectedCopula$par
param2 <- selectedCopula$par2


## Check "Unit Cube" correlation


u <- rCopula(sample_size, surBB8Copula(param=c(param, param2)))
plot(u[,1],u[,2], pch='.',col='blue', main = "Random samples from the Copula in unit cube")
cor(u,method='spearman') ## Correlation = 0.422

