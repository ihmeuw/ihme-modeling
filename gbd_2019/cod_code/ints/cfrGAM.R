
###############################################################################################################################################################

# SET UP FUNCTIONS & ODBC CONNECTIONS #
  library(scam)
  library(mgcv)
  library(RODBC)
  library(dplyr)
  library(data.table)
  library(zoo)
  library(matrixStats)
  library(tidyr)

  source("FILEPATH/get_covariate_estimates.R")
  source("FILEPATH/get_ids.R")
  source("FILEPATH/get_demographics.R")

  shared <- odbcConnect(ADDRESS)
  
  step <- STEP

###############################################################################################################################################################  
  
# PULL IN SDI & AGE GROUP METADATA, & CREATE PREDCITION DATA FRAME #  
  
  # Get SDI
  sdi <- data.frame(get_covariate_estimates(covariate_id=881, decomp_step=step))
  sdi <- sdi[sdi$year_id>=1980, c("location_id", "year_id", "mean_value")]
  colnames(sdi)[3] <- "sdi"
  
  malaria <- data.frame(get_covariate_estimates(covariate_id=1072, decomp_step=step))
  malaria <- malaria[malaria$year_id>=1980, c("location_id", "year_id", "mean_value")]
  colnames(malaria)[3] <- "malaria"

  # Get age metadata and calculate age midpoint for each group
  ages <- paste(as.character(c(2:20, 30:32, 235)), collapse = ",")
  ageMeta <- sqlQuery(shared, paste("SELECT age_group_id, age_group_years_start, age_group_years_end FROM age_group WHERE age_group_id IN (", ages, ")"))
  agePred <- data.frame(cbind(ageMeta$age_group_id, rowMeans(cbind(ageMeta$age_group_years_start, ageMeta$age_group_years_end))))
  names(agePred) <- c("age_group_id", "ageMid")
  
  
  # Create prediction data frame for final estimates by location, year, and age 
  forPred = merge(sdi, agePred, all=TRUE)
  forPred = merge(forPred, rbind(0,1), all = TRUE)
  names(forPred)[6] <- "estPrHiv"
  forPred$sdi[forPred$sdi<0.2] <- 0.2
  forPred$sdi[forPred$sdi>0.9] <- 0.9
                               
  # Create prediction data frame to compute a simultaneous interval for a penalised spline 
  critPred <- merge(seq(from = 0.1, to = 0.9, by = 0.1), c(0.1, 0.5, 1, 2.5, seq(from = 5, to = 100, by = 5)), all=TRUE)
  critPred <- merge(critPred, rbind(0,1),  all=TRUE)
  names(critPred) <- c("sdi", "ageMid", "estPrHiv")
  
  
  
###############################################################################################################################################################
  
# PULL IN CFR DATA AND CLEAN FOR MODELLING #  
  cfr <- read.csv(FILEPATH)
  cfr <- cfr[is.na(cfr$mean)==FALSE & is.na(cfr$exclude),]
  cfr$logitMean <- log((cfr$mean + 0.001) / (1 - cfr$mean + 0.001))
  cfr$weights <- cfr$sample_size / mean(cfr$sample_size)
  cfr$nid <- factor(cfr$nid) 
  
  toEst <- cfr$cv_hiv_mixed==1 & is.na(cfr$cv_hiv_proportion)
  have  <- cfr$cv_hiv_mixed==1 & !is.na(cfr$cv_hiv_proportion)
    
  cfr$estPrHiv[have] <- cfr$cv_hiv_proportion[have]
  
  cfr$estPrHiv2 <- cfr$estPrHiv
  cfr$estPrHiv2[toEst] <- (cfr$HIV_prev_pct[toEst] * (20-1)) / (1 + cfr$HIV_prev_pct[toEst] * (20-1)) 


 
  
###############################################################################################################################################################  

# RUN GAM MODEL AND MAKE PREDICTIONS #
 forPred <- forPred[,c(1:6)]
 rm(gPred, toPlot)

 
 # Calculate degrees of freedom adjustment to account for clustered data   
 dfMod <- gam(cbind(cases, sample_size-cases) ~ s(ageMid, bs='ps', sp=150) + estPrHiv + sdi + s(nid, bs="re"), data=cfr, weights=cfr$weights, family=binomial())
 dfa <- (length(unique(cfr$nid))/(length(unique(cfr$nid)) - 1)) * (length(cfr$nid) - 1)/dfMod$df.residual

 # Run model for prediction
 g <- gam(cbind(cases, sample_size-cases) ~ s(ageMid, bs='ps', sp=150) + estPrHiv + sdi, data=cfr, weights=cfr$weights, family=binomial())
 
 
 # Make predicitions for all locations, years, and age groups
 gPred <- predict.gam(g, forPred, se.fit = TRUE)
 forPred$logitPred <- as.numeric(gPred$fit)
 forPred$logitPredSe <- as.numeric(gPred$se.fit * dfa)
 
 # Smooth standard errors across age groups to correct discontinuities
 forPred <- forPred[order(forPred$location_id, forPred$year_id, forPred$estPrHiv, forPred$ageMid), ]
 forPred$logitPredSeSm <- rollmean(forPred$logitPredSe, k=5, na.pad = TRUE, partial = TRUE)
 forPred$logitPredSeSm[forPred$age_group_id<=4 | forPred$age_group_id>=31] <- forPred$logitPredSe[forPred$age_group_id<=4 | forPred$age_group_id>=31]

  
 # Compute simultaneous interval for a penalised spline
 rmvn <- function(n, mu, sig) { 
   L <- mroot(sig)
   m <- ncol(L)
   t(mu + L %*% matrix(rnorm(m*n), m, n))
   }
 
 Vb <- vcov(g)
 pred  <- predict(g, critPred, se.fit = TRUE)
 se.fit <- pred$se.fit
 
 set.seed(42)
 N <- 10000
 
 BUdiff <- rmvn(N, mu = rep(0, nrow(Vb)), sig = Vb)
 
 Cg <- predict(g, critPred, type = "lpmatrix")
 simDev <- Cg %*% t(BUdiff)
 
 absDev <- abs(sweep(simDev, 1, se.fit, FUN = "/"))
 masd <- apply(absDev, 2L, max)
 crit <- quantile(masd, prob = 0.95, type = 8)
 

 
###############################################################################################################################################################   

 
 
 write.table(rnorm(n=nDraws, mean=0, sd=crit/qnorm(0.975)), file = FILEPATH, eol = " ", quote = FALSE, row.names = FALSE, col.names = FALSE)

 out <- forPred[,c("location_id", "year_id", "age_group_id", "estPrHiv", "logitPred", "logitPredSeSm")]
 
 write.csv(forPred[,c("location_id", "year_id", "age_group_id", "estPrHiv", "logitPred", "logitPredSeSm")], file = FILEPATH, row.names = FALSE)
 
 