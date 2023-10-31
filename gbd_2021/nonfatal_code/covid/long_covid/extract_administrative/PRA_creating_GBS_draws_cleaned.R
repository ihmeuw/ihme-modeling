## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Creating draw-level data of GBS proportions using PRA data
## Author: 
## Date 4/12/2021
## --------------------------------------------------------------------- ----
#Create draws for PRA data
pra <- read.csv('FILEPATH/prepped_data_050721.csv')
pra_a <- pra[pra$study_id=='PRA' & pra$sex=="Both"&pra$age_start==0 & pra$age_end==99 & pra$symptom_cluster=="GBS",]
pra_a$hospital<-c(0,1,0)
pra_a$icu <-c(0,0,1)

#norm_dist_com <- rnorm(1000, mean = pra_a$mean[1], sd = pra_a$standard_error[1]*pra_a$sample_size[1])
#norm_dist_hos <- rnorm(1000, mean = pra_a$mean[2], sd = pra_a$standard_error[2]*pra_a$sample_size[2])
#norm_dist_icu <- rnorm(1000, mean = pra_a$mean[3], sd = pra_a$standard_error[3]*pra_a$sample_size[3])

binomial_com <- rbinom(1000, pra_a$sample_size[1], pra_a$mean[1])/pra_a$sample_size[1]
binomial_hos <- rbinom(1000, pra_a$sample_size[2], pra_a$mean[2])/pra_a$sample_size[2]
binomial_icu <- rbinom(1000, pra_a$sample_size[3], pra_a$mean[3])/pra_a$sample_size[3]

#?rbinom
data <- rbind(binomial_com, binomial_hos,binomial_icu )
data_final <- as.data.frame(cbind(pra_a$hospital, pra_a$icu, data))

suffix <-seq(1:999)
prefix <- "draw_"
v1 <- paste(prefix, suffix, sep="")

names(data_final)[4:1002]<-v1
names(data_final)[names(data_final)=='V1']<-"hospital"
names(data_final)[names(data_final)=='V2']<-"icu"
names(data_final)[names(data_final)=='V3']<-"draw_0"

setwd("FILEPATH")
write.csv(data_final, paste0("PRA_proportion", ".csv"))

