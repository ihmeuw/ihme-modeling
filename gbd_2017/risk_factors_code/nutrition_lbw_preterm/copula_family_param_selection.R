rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  j<- FILEPATH
  h <- FILEPATH
  
  
} else {
  j<- FILEPATH
  h<- FILEPATH
  
}

library(copula)
library(VineCopula)
library(data.table)
library(fitdistrplus)
library(psych)

set.seed(500)

dt <- readRDS("FILEPATH")

plot(bw, gestweek, pch='.', main = "Birthweight vs Gestational Week Scatter")
abline(lm(gestweek~bw), col = 'red', lwd=1)
cor(bw, gestweek,method='spearman')

sample_size <- 100000
dt_sample <- dt[sample(nrow(dt), sample_size), ] 

u <- pobs(as.matrix(cbind(bw_sample,gestweek_sample)))[,1]
v <- pobs(as.matrix(cbind(bw_sample,gestweek_sample)))[,2]
selectedCopula <- BiCopSelect(u,v,familyset = 20, method = "mle") 

summary(selectedCopula)

param <- selectedCopula$par
param2 <- selectedCopula$par2

u <- rCopula(sample_size, surBB8Copula(param=c(param, param2)))
plot(u[,1],u[,2], pch='.',col='blue', main = "Random samples from the Copula in unit cube")
cor(u,method='spearman')

