#################################################################################################################################
### 0_Source.R - This file contains the source code and functions used to produce estimates of exposure                       ###
###              to ambient outdoor fine particular matter (PM2.5) for the GBD2017 Study                                      ###
#################################################################################################################################
###  * LocalMonitorMethod - Applies new method using common local monitor types to convert PM10 values                        ###
###  * joint.samp.inla.downscaling - A function to generate joint predictions from INLA downscaling models                    ###
###  * joint.samp.inla.downscaling2 - A function to generate joint predictions from INLA downscaling models with groups       ###
###  * PredictPM25 - Function to predict missing PM25 values                                                                  ###
###  * StandardMethod - Applies the method of Brauer et al. 2015 to predict missing PM2.5 values                              ###
###  * summaryExposures - A function to summarise predictions into exposures                                                  ###
###  * summaryPred - A function to summarise joint predictions (i.e. marginalise)                                             ###
#################################################################################################################################

#################################
### Loading required packages ###
#################################
require(INLA)
require(raster)
require(ggplot2)
require(plyr)
require(Metrics)
require(RColorBrewer)
require(rworldmap)
require(sp)
require(rgdal)
require(spdep)
require(spatstat)
require(reshape)
require(reshape2)
require(caTools)
require(fields)
require(mgcv)
require(prevR)
require(doParallel)
require(knitr)
require(markdown)
require(rmarkdown)
require(pander)
require(haven)
require(readxl)
require(foreign)
require(ggthemes)
require(matrixStats)

#######################################################
### PredictPM25 - Function to predict missing PM25  ###
###               values                            ###
#######################################################
### Input: Database - data frame with columns       ###
###           Longitude, Latitude, PM25, PM10,      ###
###           CountryName and MonitorType           ###
###        method - prediction method, currently    ###
###           "standard" (50km radius -> Country-   ###
###           Monitor -> Country -> Default)        ###
###        default - ratio to use when no country-  ###
###           level data can be used for prediction ###
### Output: PM2.5 predictions, prediction type, and ###
###         list of rows of the database predicted  ###
#######################################################
PredictPM25 <- function(Database, method = "standard", default = 0.5, ...){
  if (method == "standard"){
    PredFn <- StandardMethod
  }
  if (method == "newlocal"){
    PredFn <- LocalMonitorMethod
  }
  # First select those from the database that can be used to predict
  Both <- Database[which(!is.na(Database$PM25) & !is.na(Database$PM10)),]
  # Select those that have PM10 measurements, but not PM25
  PredIndex <- which(is.na(Database$PM25) & !is.na(Database$PM10))
  PM10only <- Database[PredIndex,]
  PM25Preds <- PredFn(Both, PM10only, default, ...)
  return(list(PM25Pred = PM25Preds$Pred, PM25Method = PM25Preds$Method, PredIndex = PredIndex))
}

#######################################################
### StandardMethod - Applies the method of Brauer   ###
###    et al. 2015 to predict missing PM2.5 values  ###
#######################################################
### Input: TrainData - data frame where both PM2.5  ###
###           and PM10 are known                    ###
###        PredictData - data frame with missing    ###
###           PM2.5, known PM10                     ###
###        default - ratio to use when no country-  ###
###           level data for prediction             ###
### Output: PM2.5 predictions, prediction type      ###
#######################################################
StandardMethod <- function(TrainData, PredictData, default = 0.5){
  KnownRatios <- TrainData$PM25 / TrainData$PM10
  TrainLonLat <- cbind(TrainData$Longitude, TrainData$Latitude)
  PredLonLat <- cbind(PredictData$Longitude, PredictData$Latitude)
  t <- dim(TrainData)[1]
  v <- dim(PredictData)[1]
  Pred <- Method <- numeric(v)
  for (i in 1:v){
    dists <- rdist.earth(PredLonLat[c(i,1),], TrainLonLat, miles = FALSE)[1,]
    CloseLocations <- as.numeric(which(dists <= 50))
    LocalRatios <- KnownRatios[CloseLocations]
    Remove <- which(LocalRatios < 0.2 | LocalRatios > 0.8)
    if (length(Remove) > 0){
      CloseLocations <- CloseLocations[-Remove]
      LocalRatios <- LocalRatios[-Remove]
    }
    if (length(CloseLocations) > 0){
      Pred[i] <- mean(LocalRatios) * PredictData$PM10[i]
      Method[i] <- "Local"
    }
    else {
      Country <- PredictData$CountryName[i]
      Monitor <- PredictData$MonitorType[i]
      CountryMonitorLocations <- which(TrainData$CountryName == Country & TrainData$MonitorType == Monitor)
      CountryMonitorRatios <- KnownRatios[CountryMonitorLocations]
      Remove <- which(CountryMonitorRatios < 0.2 | CountryMonitorRatios > 0.8)
      if (length(Remove) > 0){
        CountryMonitorLocations <- CountryMonitorLocations[-Remove]
        CountryMonitorRatios <- CountryMonitorRatios[-Remove]
      }
      if (length(CountryMonitorLocations) > 0 & !Monitor == "Unknown" & !Monitor == ""){
        Pred[i] <- mean(CountryMonitorRatios) * PredictData$PM10[i]
        Method[i] <- "CountryMonitor"
      }
      else {
        CountryLocations <- which(TrainData$CountryName == Country)
        CountryRatios <- KnownRatios[CountryLocations]
        Remove <- which(CountryRatios < 0.2 | CountryRatios > 0.8)
        if (length(Remove) > 0){
          CountryLocations <- CountryLocations[-Remove]
          CountryRatios <- CountryRatios[-Remove]
        }
        if (length(CountryLocations) > 0){
          Pred[i] <- mean(CountryRatios) * PredictData$PM10[i]
          Method[i] <- "Country"
        }
        else {
          Pred[i] <- default * PredictData$PM10[i]
          Method[i] <- "Default"
        }
      }
    }
  }
  return(list(Pred = Pred, Method = Method))
}

#######################################################
### LocalMonitorMethod - Applies new method using   ###
### common local monitor types to convert PM10      ###
### values                                          ###
#######################################################
### Input: TrainData - data frame where both PM2.5  ###
###           and PM10 are known                    ###
###        PredictData - data frame with missing    ###
###           PM2.5, known PM10                     ###
###        default - ratio to use when no country-  ###
###           level data for prediction             ###
###        minvalue - discard ratios below this val ###
###        maxvalue - discard ratios above this val ###
### Output: PM2.5 predictions, prediction type      ###
#######################################################
LocalMonitorMethod <- function(TrainData, PredictData, default = 0.5, minvalue = 0.2, maxvalue = 0.8){
  KnownRatios <- TrainData$PM25 / TrainData$PM10
  if (default == "median"){
    WHORegions <- c("AFRO","AMRO","EMRO","EURO","SEARO","WPRO")
    DefaultConv <- numeric(6)
    names(DefaultConv) <- WHORegions
    for (i in 1:6){
      ByRegion <- TrainData[which(TrainData$WHORegion == WHORegions[i]),]
      DefaultConv[i] <- median(ByRegion$PM25 / ByRegion$PM10, na.rm  = TRUE)
    }
  }
  else {
    DefaultConv <- rep(default, 6)
    names(DefaultConv) <- c("AFRO","AMRO","EMRO","EURO","SEARO","WPRO")
  }
  TrainLonLat <- cbind(TrainData$Longitude, TrainData$Latitude)
  PredLonLat <- cbind(PredictData$Longitude, PredictData$Latitude)
  t <- dim(TrainData)[1]
  v <- dim(PredictData)[1]
  Pred <- Method <- numeric(v)
  for (i in 1:v){
    dists <- rdist.earth(PredLonLat[c(i,1),], TrainLonLat, miles = FALSE)[1,]
    Monitor <- PredictData$MonitorType[i]
    Country <- PredictData$CountryName[i]
    CloseLocations <- as.numeric(which(dists <= 50))
    CloseLocationsFull <- TrainData[CloseLocations,]
    CloseMonitorLocations <- CloseLocations[which(CloseLocationsFull$MonitorType == Monitor & CloseLocationsFull$CountryName == Country)]
    LocalRatios <- KnownRatios[CloseMonitorLocations]
    Remove <- which(LocalRatios < minvalue | LocalRatios > maxvalue)
    if (length(Remove) > 0){
      CloseMonitorLocations <- CloseMonitorLocations[-Remove]
      LocalRatios <- LocalRatios[-Remove]
    }
    if (length(CloseMonitorLocations) > 4 & !Monitor == "Unknown"){
      Pred[i] <- mean(LocalRatios) * PredictData$PM10[i]
      Method[i] <- "LocalMonitor"
    }
    else {
      LocalRatios <- KnownRatios[CloseLocations]
      DifferentCountry <- which(!CloseLocationsFull$CountryName == Country)
      if (length(DifferentCountry) > 0){
        CloseLocations <- CloseLocations[-DifferentCountry] # remove those relating to different country
        LocalRatios <- LocalRatios[-DifferentCountry]
      }
      Remove <- which(LocalRatios < minvalue | LocalRatios > maxvalue)
      if (length(Remove) > 0){
        CloseLocations <- CloseLocations[-Remove]
        LocalRatios <- LocalRatios[-Remove]
      }
      if (length(CloseLocations) > 0){
        Pred[i] <- mean(LocalRatios) * PredictData$PM10[i]
        Method[i] <- "Local"
      }
      else {
        Country <- PredictData$CountryName[i]
        Monitor <- PredictData$MonitorType[i]
        CountryMonitorLocations <- which(TrainData$CountryName == Country & TrainData$MonitorType == Monitor)
        CountryMonitorRatios <- KnownRatios[CountryMonitorLocations]
        Remove <- which(CountryMonitorRatios < minvalue | CountryMonitorRatios > maxvalue)
        if (length(Remove) > 0){
          CountryMonitorLocations <- CountryMonitorLocations[-Remove]
          CountryMonitorRatios <- CountryMonitorRatios[-Remove]
        }
        if (length(CountryMonitorLocations) > 0 & !Monitor == "Unknown" & !Monitor == ""){
          Pred[i] <- mean(CountryMonitorRatios) * PredictData$PM10[i]
          Method[i] <- "CountryMonitor"
        }
        else {
          CountryLocations <- which(TrainData$CountryName == Country)
          CountryRatios <- KnownRatios[CountryLocations]
          Remove <- which(CountryRatios < minvalue | CountryRatios > maxvalue)
          if (length(Remove) > 0){
            CountryLocations <- CountryLocations[-Remove]
            CountryRatios <- CountryRatios[-Remove]
          }
          if (length(CountryLocations) > 0){
            Pred[i] <- mean(CountryRatios) * PredictData$PM10[i]
            Method[i] <- "Country"
          }
          else {
            Region <- PredictData$WHORegion[i]
            RegionConv <- as.numeric(DefaultConv[which(names(DefaultConv) == Region)])
            Pred[i] <- RegionConv * PredictData$PM10[i]
            Method[i] <- "Default"
          }
        }
      }
    }
  }
  return(list(Pred = Pred, Method = Method))
}


#######################################################
### joint.samp.inla.downscaling - A function to ge- ###
### nerate joint predictions from INLA downscaling  ###
### models                                          ###
#######################################################
### Input: dat - Dataset containing locations/vari- ###
###              ables needed for the predictions   ###
###        N - Number of samples to generate        ###
###        samp - Samples from the parameter field  ###
###            If NULL, function will generate this ###
###        INLAOut - Output from inla.model function###
###        A - A matrix needed to map SPDE to pred- ###
###            iction locations                     ###
###        spat.slope - Variable in which to place  ###
###             the spatially varying slope.        ###
###        keep - Variables to keep from dat in     ###
###               output dataset                    ###
### Output: Joint samples from a downscaling model  ###
#######################################################
joint.samp.inla.downscaling <- function(dat,
                                        N,
                                        samp = NULL,
                                        INLAOut,
                                        A = A,
                                        prefix = 'pred_',
                                        spat.slope,
                                        keep = NULL){
  # Creating Samples if none specified
  if (is.null(samp)){
    samp <- inla.posterior.sample(n = N, INLAOut$INLAObj)
  }
  # Number of samples
  N <- length(samp)
  # Fixed Effects
  fixed <- rownames(INLAOut$INLAObj$summary.fixed)
  # Random Effects
  random <- names(INLAOut$INLAObj$summary.random)
  random <- random[!grepl('index',random)]
  # Looping for each fixed effect
  for (i in 1:N){
    # Empty column for prediction
    dat[,paste(prefix, i, sep = '')] <- as.numeric(0)
    # Adding a suffix if necessary 
    if (sum(rownames(samp[[i]]$latent) == fixed[1]) == 0){suffix <- ':1'}
    else {suffix <- ''}
    # Loop for each fixed effect 
    for (j in 1:length(fixed)){
      # Intercept
      if (fixed[j] %in% c('(Intercept)','intercept')) {
        dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] + samp[[i]]$latent[paste(fixed[j], suffix, sep = ''),]
      }
      # Interaction terms
      else if (grepl(':',fixed[j])){
        dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] +
          samp[[i]]$latent[paste(fixed[j], suffix, sep = ''),] * dat[,strsplit(fixed[j],':')[[1]][1]] * dat[,strsplit(fixed[j],':')[[1]][2]]
      }
      # Other variables
      else {
        dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] +  samp[[i]]$latent[paste(fixed[j], suffix, sep = ''),] * dat[,fixed[j]]
      }
    }
  }
  for (i in 1:N){
    check <- rep(0, length(random))
    # Adding intercept if missing
    if (!('intercept' %in% fixed)) {fixed <- c('intercept', fixed)}
    # Looping for each random effect
    for (k in 1:length(random)){
      # Shell dataset for samples
      tmp <- data.frame(Val = rep(0, length(unique(c(INLAOut$INLAObj$summary.random[[random[k]]]$ID, dat[, random[k]])))))
      rownames(tmp) <- sort(unique(c(INLAOut$INLAObj$summary.random[[random[k]]]$ID, dat[,random[k]])))
      # Extracting samples
      tmp2 <- samp[[i]]$latent[grep(random[k],rownames(samp[[i]]$latent)), ]
      # RElabelling if missing in samples (INLA bug)
      names(tmp2) <- INLAOut$INLAObj$summary.random[[random[k]]]$ID
      # Placing values in the prediction frame
      tmp[names(tmp2),'Val'] <- as.numeric(tmp2)
      # Loop for each fixed effect
      for (l in 1:length(fixed)){
        if(grepl(l, random[k])){
          if (fixed[l] %in% c('(Intercept)','intercept')){
            dat[,paste(prefix, i, sep = '')] <-
              dat[,paste(prefix, i, sep = '')] + tmp[dat[,random[k]],]
          }
          else {
            dat[,paste(prefix, i, sep = '')] <-
              dat[,paste(prefix, i, sep = '')] + tmp[dat[,random[k]],] * dat[,fixed[l]]
          }
          check[k]<-1
          break
        }
      }
      if (check[k] == 0){
        dat[,paste(prefix, i, sep = '')] <-
          dat[,paste(prefix, i, sep = '')] + tmp[dat[,random[k]],]
        check[k]<-1
      }
    }
    # Adding spatial random effects
    dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] +
      as.numeric(A %*% samp[[i]]$latent[grep('index1:',rownames(samp[[i]]$latent)),]) + # Spatial Intercept
      as.numeric(A %*% samp[[i]]$latent[grep('index2:',rownames(samp[[i]]$latent)),]) * dat[,deparse(substitute(spat.slope))] # Spatial slope for CTM
    # Printing index
    if (i %% 10 == 0 | N <= 10){print(paste('Predictions done:', i))}
  }
  # Returning data
  return(dat[c(keep,paste(prefix, 1:N, sep = ''))])
}







#######################################################
### joint.samp.inla.downscaling2 - A function to    ###
### generate joint predictions from INLA downscal-  ###
### ing models with groups                          ###
#######################################################
### Input: dat - Dataset containing locations/vari- ###
###              ables needed for the predictions   ###
###        N - Number of samples to generate        ###
###        timevar - Randomeffect that contains has ###
###            been grouped by time                 ###
###        N.Years - Number of Years in each group  ###
###        t - timepoint to be predicted            ###
###        samp - Samples from the parameter field  ###
###            If NULL, function will generate this ###
###        INLAOut - Output from inla.model function###
###        A - A matrix needed to map SPDE to pred- ###
###            iction locations                     ###
###        spat.slope - Variable in which to place  ###
###             the spatially varying slope.        ###
###        keep - Variables to keep from dat in     ###
###               output dataset                    ###
### Output: Joint samples from a downscaling model  ###
#######################################################
joint.samp.inla.downscaling2 <- function(dat,
                                         N,
                                         timevar = NULL,
                                         N.Years = NULL,
                                         t = NULL,
                                         samp = NULL,
                                         INLAOut,
                                         A = A,
                                         prefix = 'pred_',
                                         spat.slope,
                                         keep = NULL){
  # Creating Samples if none specified
  if (is.null(samp)){
    samp <- inla.posterior.sample(n = N, INLAOut$INLAObj)
  }
  # Number of samples
  N <- length(samp)
  # Fixed Effects
  fixed <- rownames(INLAOut$INLAObj$summary.fixed)
  #if ('intercept' %in% fixed) {fixed <- c('intercept',fixed)}
  # Random Effects
  random <- names(INLAOut$INLAObj$summary.random)
  random <- random[!grepl('index',random)]
  # If some are grouped by time, then remove from main run
  if (is.null(timevar) == FALSE){
    random <- random[!(random %in% timevar)]
  }
  # Looping for each fixed effect
  for (i in 1:N){
    # Empty column for prediction
    dat[,paste(prefix, i, sep = '')] <- as.numeric(0)
    # Adding a suffix if necessary 
    if (sum(rownames(samp[[i]]$latent) == fixed[1]) == 0){suffix <- ':1'}
    else {suffix <- ''}
    # Loop for each fixed effect 
    for (j in 1:length(fixed)){
      # Intercept
      if (fixed[j] %in% c('(Intercept)','intercept')) {
        dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] + samp[[i]]$latent[paste(fixed[j], suffix, sep = ''),]
      }
      # Interaction terms
      else if (grepl(':',fixed[j])){
        dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] +
          samp[[i]]$latent[paste(fixed[j], suffix, sep = ''),] * dat[,strsplit(fixed[j],':')[[1]][1]] * dat[,strsplit(fixed[j],':')[[1]][2]]
      }
      # Other variables
      else {
        dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] +  samp[[i]]$latent[paste(fixed[j], suffix, sep = ''),] * dat[,fixed[j]]
      }
    }
  }
  # Looping for each fixed effect
  for (i in 1:N){
    check <- rep(0, length(random))
    # Adding intercept if missing
    if (!('intercept' %in% fixed)) {fixed <- c('intercept', fixed)}
    # Looping for each random effect
    for (k in 1:length(random)){
      # Shell dataset for samples
      tmp <- data.frame(Val = rep(0, length(unique(c(INLAOut$INLAObj$summary.random[[random[k]]]$ID, dat[, random[k]])))))
      rownames(tmp) <- sort(unique(c(INLAOut$INLAObj$summary.random[[random[k]]]$ID, dat[,random[k]])))
      # Extracting samples
      tmp2 <- samp[[i]]$latent[grep(random[k],rownames(samp[[i]]$latent)), ]
      # RElabelling if missing in samples (INLA bug)
      names(tmp2) <- INLAOut$INLAObj$summary.random[[random[k]]]$ID
      # Placing values in the prediction frame
      tmp[names(tmp2),'Val'] <- as.numeric(tmp2)
      # Loop for each fixed effect
      for (l in 1:length(fixed)){
        if(grepl(l, random[k])){
          if (fixed[l] %in% c('(Intercept)','intercept')){
            dat[,paste(prefix, i, sep = '')] <-
              dat[,paste(prefix, i, sep = '')] + tmp[dat[,random[k]],]
          }
          else {
            dat[,paste(prefix, i, sep = '')] <-
              dat[,paste(prefix, i, sep = '')] + tmp[dat[,random[k]],] * dat[,fixed[l]]
          }
          check[k]<-1
          break
        }
      }
      if (check[k] == 0){
        dat[,paste(prefix, i, sep = '')] <-
          dat[,paste(prefix, i, sep = '')] + tmp[dat[,random[k]],]
        check[k]<-1
      }
    }
    if (is.null(timevar) == FALSE){
      # Adding Temporal random effects
      for (k in 1:length(timevar)){
        # Extracting sample
        tmp1 <- samp[[i]]$latent[grep(paste(timevar[k],':',sep = ''),rownames(samp[[i]]$latent)),]
        names(tmp1) <- INLAOut$INLAObj$summary.random[[timevar[k]]]$ID
        # Extracting number of random effects
        N.RE <- length(tmp1) / N.Years
        names(tmp1) <- rep(names(tmp1)[1:N.RE], N.Years)
        tmp1 <- tmp1[((t-1)*N.RE):(t*N.RE - 1) + 1]
        # Converting to dataframe
        tmp1 <- data.frame(tmp1)
        names(tmp1) <- 'Val'
        # Creating a shell to place samples in
        tmp2 <- data.frame(Val = rep(0, length(unique(dat[,timevar[k]]))))
        rownames(tmp2) <- unique(dat[,timevar[k]])
        tmp2[row.names(tmp1),'Val'] <- tmp1$Val
        for (l in 1:length(fixed)){
          if(grepl(l, k)){
            if (fixed[l] %in% c('(Intercept)','intercept')){
              dat[,paste(prefix, i, sep = '')] <-
                dat[,paste(prefix, i, sep = '')] + tmp2[dat[,timevar[k]],]
            }
            else {
              dat[,paste(prefix, i, sep = '')] <-
                dat[,paste(prefix, i, sep = '')] + tmp2[dat[,timevar[k]],] * dat[,fixed[l]]
            }
            break
          }
        }
      }
    }
    # Adding spatial random effects
    dat[,paste(prefix, i, sep = '')] <- dat[,paste(prefix, i, sep = '')] +
      as.numeric(A %*% samp[[i]]$latent[grep('index1:',rownames(samp[[i]]$latent)),]) + # Spatial Intercept
      as.numeric(A %*% samp[[i]]$latent[grep('index2:',rownames(samp[[i]]$latent)),]) * dat[,deparse(substitute(logSAT))] # Spatial slope for CTM
    # Printing index
    if (i %% 10 == 0 | N <= 10){print(paste('Predictions done:', i))}
  }
  return(dat[c(keep,paste(prefix, 1:N, sep = ''))])
}



#######################################################
### summaryPred - A function to summarise joint     ###
###               predictions (i.e. marginalise)    ###
#######################################################
### Input: dat - Dataframe containing predictions   ###
###        prefix - Prefix for prediction colums    ###
###        keep - Variables to keep from dat in     ###
###               output dataset                    ###
### Output: Dataframe with summarised predictions   ###
#######################################################
summaryPred <- function(dat, 
                        prefix,
                        keep,
                        nCluster = detectCores()){
  # Splitting 
  dat$Proc <- cut(1:nrow(dat), 
                  breaks = quantile(1:nrow(dat), 
                                    probs = seq(0,1, length.out = (nCluster + 1))), 
                  labels = 1:nCluster, 
                  include.lowest = TRUE)
  # Parallel loop for each 
  out <- foreach(i=1:nCluster, .combine=rbind) %dopar% {
    # Only keeping allocated chunk 
    tmp <- subset(dat, Proc == i)
    # Summarising predictions
    tmp$Mean    <- apply(tmp[,grep(prefix,names(tmp))], 1, function(x) {mean(x, na.rm=TRUE)})
    tmp$Median  <- apply(tmp[,grep(prefix,names(tmp))], 1, function(x) {median(x, na.rm=TRUE)})
    tmp$Lower95 <- apply(tmp[,grep(prefix,names(tmp))], 1, function(x) {quantile(x, prob = 0.025, na.rm=TRUE)})
    tmp$Upper95 <- apply(tmp[,grep(prefix,names(tmp))], 1, function(x) {quantile(x, prob = 0.975, na.rm=TRUE)})
    tmp$StdDev  <- apply(tmp[,grep(prefix,names(tmp))], 1, function(x) {sd(x, na.rm=TRUE)})
    # Returning object 
    return(tmp)
  }
  # Only keeping relevant columns 
  out <- out[,c(keep,'Mean','Median','Lower95','Upper95','StdDev')]
  # Returning summaried predictions 
  return(out)
}


#######################################################
### summaryExposures - A function to summarise pre- ###
###                    dictions into exposures      ###
#######################################################
### Input: dat - Dataframe containing predictions   ###
###        byvar - Variable to summarise prediction ###
###               by. If NULL grid is summarised    ###
###        weights - Variable to weight predictions ###
###             by. If NULL, results are unweighted ###
###        prefix - Prefix for prediction colums    ###
### Output: Dataframe with summarised predictions   ###
#######################################################
summaryExposures <- function(dat, 
                        prefix,
                        weights = NULL,
                        byvar = NULL){
  # Extracting variable to summarise by
  dat$byvar <- dat[,byvar]
  if (is.null(weights)){
    # Equal weighting if none specified 
    dat$weights <- 1
  }
  else {
    # Extracting variable to weight by
    dat$weights <- dat[,weights]
  }
  # Number of predictions 
  N <- sum(grepl(prefix, names(dat)))
  # Function to merge dataset together in parallel loop 
  merge.by.byvar <- function(a, b) {
    merge(a, b,
          by = c('byvar'),
          all.x = TRUE,
          all.y = TRUE)
  }
  ######################################
  ### Creating summaries of the mean ###
  ######################################
  # Parallel loop for each 
  out1 <- foreach(i=1:N, .combine=merge.by.byvar) %dopar% {
	  # Loading packages
	  require(plyr)
	  require(spatstat)
	  require(Metrics)
    # Extracting correct sample
    dat$samp <- dat[,paste(prefix, i, sep = '')]
    # Summarising by byvar
    tmp <- ddply(dat,
                 .(byvar),
                 summarise,
                 samp = weighted.mean(samp, weights, na.rm = TRUE))
    # Altering column names 
    names(tmp) <- c('byvar', paste(prefix, i, sep = ''))
    # Returning object
    return(tmp)
  }
  # Keeping samples for testing later
  outsamp <- out1
  # Summarising predictions
  out1$LowerCI <- apply(out1[,grep(prefix,names(out1))], 1, function(x) {quantile(x, prob = 0.025, na.rm=TRUE)})
  out1$UpperCI <- apply(out1[,grep(prefix,names(out1))], 1, function(x) {quantile(x, prob = 0.975, na.rm=TRUE)})
  out1$StdDevCI <- apply(out1[,grep(prefix,names(out1))], 1, function(x) {sd(x, na.rm=TRUE)})
  # Only keeping relevant columns 
  out1 <- out1[,c('byvar','LowerCI','UpperCI', "StdDevCI")]
  ######################################
  ### Creating summaries of the mean ###
  ######################################
  # Number of byvar 
  M <- length(sort(unique(dat$byvar)))
  # List of unique byvar
  listtmp <- sort(unique(dat$byvar))
  # Parallel loop for each 
  out2 <- foreach(i=1:M, .combine = rbind) %dopar% {
	  # Loading packages
	  require(plyr)
	  require(spatstat)
	  require(Metrics)
    # Extracting first by variable 
    tmp <- listtmp[i]
    # Weighted Mean
    mn <- weighted.mean(as.matrix(dat[which(dat$byvar == tmp),
                                      grep(prefix,names(dat))]), 
                        as.matrix(dat[which(dat$byvar == tmp),rep('weights',N)]))
    # Weighted Median
    med <- weighted.median(as.matrix(dat[which(dat$byvar == tmp),
                                         grep(prefix,names(dat))]), 
                           as.matrix(dat[which(dat$byvar == tmp),rep('weights',N)]))
    # Weighted Median
    var <-  sum(as.matrix(dat[which(dat$byvar == tmp),rep('weights',N)]) * (as.matrix(dat[which(dat$byvar == tmp),grep(prefix,names(dat))]) - mn)^2)
    var <- var / (((N-1)/N) * sum(as.matrix(dat[which(dat$byvar == tmp),rep('weights',N)])))
    # Weighted LowerPI
    low <- weighted.quantile(as.matrix(dat[which(dat$byvar == tmp),
                                           grep(prefix,names(dat))]), 
                             as.matrix(dat[which(dat$byvar == tmp),rep('weights',N)]),
                             probs = 0.025)
    # Weighted UpperPI
    upp <- weighted.quantile(as.matrix(dat[which(dat$byvar == tmp),
                                           grep(prefix,names(dat))]), 
                             as.matrix(dat[which(dat$byvar == tmp),rep('weights',N)]),
                             probs = 0.975)
    # Outputting as a dataframe
    tmp <- data.frame(byvar = tmp,
                      Mean = mn,
                      Median = med,
                      LowerPI = low,
                      UpperPI = upp,
                      StdDevPI = sqrt(var))
    # REturning object
    return(tmp)
  }
  ##########################
  ### Outputting dataset ###
  ##########################
  out <- merge(out1, 
               out2, 
               by = 'byvar')
  out <- list(outsamp = outsamp,
              outsumm = out)
  # Returning summaried predictions 
  return(out)
}

#######################################################
### inla.spat.downscaling - A function to run a     ###
###               spatial downscaling model in INLA ###
#######################################################
### Input: dat - Dataframe containing all variables ###
###             used in the analysis.               ###
###        Y - Response variable.                   ###
###        spat.slope - Variable in which to place  ###
###             the spatially varying slope.        ###
###        effects - A string of effects to include ###
###             in the model. Separated by '+'. Non ###
###             linear effects can be added in the  ###
###             normal way in INLA by using the f() ###
###             function. Need to ensure all varia- ###
###             bles used are in dat data.frame.    ###
###        mesh - Mesh used in downscaling model    ###
###        mode - Argument to pass information on   ###
###             the mode to the initial Newton-     ###
###             Raphson optimiser.                  ###
###        hyperpriors - A list of PC priors for    ###
###             model hyperparameters (requires     ###
###             a series of vectors - prior.range1, ### 
###             prior.sigma1, prior.range2,         ###
###             prior.sigma2. 1 and 2 refer to the  ###
###             processes on the intercept and slope###
###             respectively.                       ###
###        config - Store internal grids from nume- ###
###             rical integration (T/F)             ###
###        verbose - Print INLA working (T/F)       ###
### Output: List containing all model fit details   ###
#######################################################
inla.spat.downscaling <- function(dat,
                                  Y,
                                  spat.slope,
                                  effects,
                                  mesh, 
                                  mode = list(),
                                  hyperpriors,
                                  verbose = FALSE,
                                  config = FALSE){
  # SPDE object for the intercept with PC priors 
  spde.int <- inla.spde2.pcmatern(mesh,
                                  alpha = 2,
                                  prior.range = hyperpriors$prior.range1, 
                                  prior.sigma = hyperpriors$prior.sigma1,
                                  constr = TRUE)
  # Creating a list of indeces for the SPDE on the intercept 
  spde.int.index <- inla.spde.make.index(name = "index1",
                                         n.spde = spde.int$n.spde)
  # SPDE object for the slope with PC priors 
  spde.slp <- inla.spde2.pcmatern(mesh,
                                  alpha = 2,
                                  prior.range = hyperpriors$prior.range2, 
                                  prior.sigma = hyperpriors$prior.sigma2,
                                  constr = TRUE)
  # Creating a lits of indeces for the SPDE on the slope 
  spde.slp.index <- inla.spde.make.index(name = "index2",
                                         n.spde = spde.slp$n.spde)
  # Number of data points in the mesh 
  n.data <- nrow(dat)
  # Creating the A Matrix
  A.int = inla.spde.make.A(mesh,
                           loc = dat)
  # Creating the A Matrix
  A.slp = inla.spde.make.A(mesh,
                           loc = dat,
                           weights = dat@data[,deparse(substitute(spat.slope))])
  # Build the INLA stack for modelling
  stack <- inla.stack(data = list(Y=dat@data[,deparse(substitute(Y))]),
                      A = list(1,A.int,A.slp),
                      effects = list(list(intercept=rep(1,n.data),
                                          dat@data),
                                     (spde.int.index),
                                     (spde.slp.index)),
                      tag='est')
  # Model formula
  formula <-  as.formula(paste('Y ~ - 1 +',paste(deparse(substitute(effects)), collapse = ''),'+ f(index1,model=spde.int) + f(index2,model=spde.slp)',sep=''))
  # # Fitting INLA object
  result <- inla(formula,
                 data = inla.stack.data(stack),
                 control.family = list(hyper = list(prec = list(prior = 'pc.prec',
                                                                param = hyperpriors$prior.prec))),
                 control.predictor = list(A=inla.stack.A(stack),compute=TRUE),
                 control.compute = list(config=config,dic=TRUE),
                 control.mode = mode,
                 control.inla = list(strategy = "gaussian", int.strategy = "eb"),
                 verbose = verbose)
  # Returning Model fit
  return(list(INLAObj   = result,
              INLAStack = stack,
              INLAMesh  = mesh))
}

