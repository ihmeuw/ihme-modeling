## Intimate Partner Violence, HIV PAF calculation, relative risk meta-analysis

# setup
library(data.table)
library(stats)
library(metafor, lib.loc = "FILEPATH")
source('FILEPATH/get_epi_data.R')

# get data
rr <- get_epi_data(2621)
rr <- rr[cause=='hiv',]

# calculate SE
rr[,log_mean:=log(mean)]
rr[,log_lower:=log(lower)]
rr[,log_upper:=log(upper)]
rr[,diff:=log_upper-log_mean]
rr[,standard_error:=diff/1.96]

# run mixed effects meta-regression
model <- rma.uni(data=rr, yi=log_mean, sei=standard_error)
m <- as.numeric(model$beta)
se <- as.numeric(model$se)

# expand to draws
rr_draws <- data.table(draw_num=c(0:999),rr=rnorm(n=1000,mean=m,sd=se))

# un-log transform
rr_draws[,rr:=exp(rr)]

# save to input folder for paf calculation
write.csv(rr_draws,paste0(main_dir,"/input/rr_draws.csv"),row.names=F)

# END