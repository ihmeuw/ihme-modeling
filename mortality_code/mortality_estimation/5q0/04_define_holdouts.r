################################################################################
## Description: Defines holdouts for each region
################################################################################

  rm(list=ls())

  if (Sys.info()[1] == "Linux") root <- "FILEPATH" else root <- "FILEPATH"
  setwd("FILEPATH")
  
  num.holdouts <- 100

## load data; identify each row; make an indicator for whether or not data is knocked out
  data <- read.csv(file="FILEPATH/gpr_5q0_input.txt", header=T, stringsAsFactors=F)
  data <- data[,names(data)[!grepl("pred2", names(data))]]
  
  if (Sys.info()[1] == "Linux"){
     setwd("FILEPATH")
  }else{
     setwd("FILEPATH")
  }
## loop through regions & holdouts

  for (rr in sort(unique(data$gbd_region))) { 
    cat(paste("\n", rr, "\n  ", sep="")); flush.console()
    region.data <- data[data$gbd_region == rr,]
    ho <- matrix(0, nrow=nrow(region.data), ncol=num.holdouts)
    for (hh in 1:num.holdouts) { 
      cat(paste(hh, if (hh%%10==0) "\n   " else " ", sep="")); flush.console()
      
## select block length to drop most recent data
      max <- max(region.data$year)
      set.seed(436+hh)
      length <- sample(10:20, 1)
      knockout <- (region.data$year >= (max - length) & region.data$data == 1) 
      ho[knockout,hh] <- 1
      
## select random blocks to drop 
      for (ii in 1:length(unique(region.data$iso3))) {
        set.seed(293+hh+ii)
        country <- sample(unique(region.data$iso3),1)
        set.seed(921+hh+ii)
        length <- sample(5:10,1)
        set.seed(318+hh+ii)
        mid <- sample(1950:2011,1)+0.5
        knockout <- (region.data$year >= (mid - length) & region.data$year <= (mid + length) & region.data$iso3 == country & region.data$data == 1)
        ho[knockout,hh] <- 1
      } 
    } 
## save knocked-out & region-specific data file
      colnames(ho) <- paste("ho", 1:num.holdouts, sep="")
      save <- cbind(region.data, ho)
      write.csv(save, file = paste("input_", rr, ".txt", sep=""), row.names = F)
  } 

              
