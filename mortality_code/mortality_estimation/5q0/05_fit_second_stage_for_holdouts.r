################################################################################
## Description: Runs the second stage prediction model for a given holdout
################################################################################

  rm(list=ls())
  library(plyr)
 
  if (Sys.info()[1] == "Linux") root <- "FILEPATH" else root <- "FILEPATH"
 
  ## Set local working directory (toggles by GIT user) 
  user <- commandArgs()[5] 
  code_dir <- "FILEPATH"
 
  setwd("FILEPATH")
  source("FILEPATH/space_time.r")

  if (Sys.info()[1] == "Linux"){
     setwd("FILEPATH")
  }else{
     setwd("FILEPATH")
  }

## load data
  test = F
  ## for testing use specific rr and ho
  if(test == T) {
    rr <- "High-income_Asia_Pacific"
    ho <- 1
  } else {
    rr <- commandArgs()[3] 
    ho <- as.numeric(commandArgs()[4])
  }


  data <- read.csv(paste("input_", rr, ".txt", sep=""), header=T, stringsAsFactors=F)
  data$include <- (data[,paste("ho", ho, sep="")] == 0)
  data <- data[,!grepl("ho",names(data))]

## fit second stage model

#keep only not-held-out data and one residual per year
stdata <- ddply(data[data$include,], .(iso3,year),
  function(x){
      data.frame(gbd_region = x$gbd_region[1],
      iso3 = x$iso3[1],
      year = x$year[1],
      vr = max(x$vr),
      resid = mean(x$resid))
    })

# fit space-time for both national and subnational
  reg.sub <- unique(stdata$gbd_region[grepl("X(.{2})",stdata$iso3)])
  preds.nat <- resid_space_time(stdata[!grepl("X(.{2})",stdata$iso3),])
  if (length(reg.sub) > 0) {
    preds.sub <- resid_space_time(stdata[stdata$gbd_region %in% reg.sub & !(stdata$iso3 %in% c("IND","GBR","CHN","MEX")),])
    preds.sub <- preds.sub[grepl("X(.{2})",preds.sub$iso3),]
    preds <- rbind(preds.sub,preds.nat)
  } else { 
    preds <- preds.nat
  }

  data <- merge(data, preds, by=c("iso3", "year"))
  data$pred.2.resid <- inv.logit(data$pred.2.resid)

  
  data$pred.2.final <- inv.logit(logit(data$pred.2.resid) + logit(data$pred1b))

 
## save output
  if (Sys.info()[1] == "Linux"){
     setwd("FILEPATH")
  }else{
     setwd("FILEPATH")
  }

  write.csv(data, file=paste("prediction_model_results_all_stages_", rr, "_", ho, ".txt", sep=""), row.names=F)
   