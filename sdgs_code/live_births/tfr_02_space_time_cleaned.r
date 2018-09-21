################################################################################
## Description: Defines the space-time model (2nd stage prediction model) for GBD 2016 Fertility
## Adapted from GBD 2016 Mortality team
################################################################################
library(data.table)


logit <- function(x) log(x/(1-x))

inv.logit <- function(x) exp(x)/(1+exp(x))

resid_space_time <- function(data, iso3=NULL, lambda=.9, zeta=.99, min_year=1950, max_year=2015, lambda_by_density = T, lambda_by_year = F, custom = F) {
  ## set up data frame to hold results
  test <- F
  if(test){
    data=tfr
    iso3=NULL
    lambda=.6
    zeta=.99
    min_year=1950
    max_year=2016
    lambda_by_density = T
    lambda_by_year = F
    custom = F
    
  }
  preds <- NULL
  count <- 0
  
  
  ## calculate the data density
  
  data_density <- function(dt){
      

      density <- dt[!is.na(resid), unique(year)] %>% length
      print(density)
      denom <- 67
      print(denom)
      prop <- density/denom
      print(prop)
      return(prop)
  }
  
  
  #calculate the proprtion of years covered to select time weights
  prop_years_covered <- function(dt) {
      
      covered <- dt[!is.na(resid) & year >= 1980, year] %>% unique %>% length
      
      prop <- 100*covered/((max_year - 1980) +1)# +1 allows this to count range of years 1980-2016 inclusive
      
      return(prop)
  }
  
  ## smooth on mean residual per location year so as not to overweight years that have multiple data points (adapted from 5q0)
  
  data <- data[, list(resid = mean(resid)), by = list(ihme_loc_id, iso3, year, vital, data_rich)]
  
  
  ## loop through countries
  if (is.null(iso3)) iso3 <- sort(unique(data$iso3))
  for (rr in iso3) {
    cat(paste(rr, "\n")); flush.console()
    region.data <- data[data$iso3 == rr & !is.na(data$resid),]
      
  ## loop through subnationals
      countries <- sort(unique(data$ihme_loc_id[data$iso3==rr]))
      for (cc in countries) {
        in.country <- (region.data$ihme_loc_id == cc)
        other.resids <- (sum(!in.country)>0)
        
        ## data density
        d_dens <- data_density(data[ihme_loc_id == cc])
        print(d_dens)
        # for countries with data, find the first and last year of that data, for countries without data, find the first and last year of data in the region 
        if (sum(in.country)>0) in.sample <- range(region.data$year[in.country]) else in.sample <- range(region.data$year)
        
         has_vital <- any(data[ihme_loc_id == cc, vital])
        data_complete <- any(data[ihme_loc_id == cc, data_rich]) #amy rather than all because rows with covariates but without data have not been attirubted data rich or VR status
        
        if (lambda_by_density) {
            
            #cutoffs established by looking at distribution of data density--special case for Brazil and its subnationals
          
          if (d_dens > .75) {
                
                lambda2 <- .9
                if (cc == "BRA") {labmda2 <- .3}
                zeta <- .99
                print(paste("High-density", cc, zeta, lambda2, sep = ","))
                
            } else if (d_dens > .35) {
                
                lambda2 <- .9
                if (cc %like% "BRA_") {lambda2 <- .5}
                zeta <- .9
                print(paste("Medium density", cc, zeta, lambda2, sep = ","))
                
            } else {
               
                if (cc %like% "BRA_") {lambda2 <- .5}
                lambda2 <- .9
                zeta <- .7
                print(paste("Low Density", cc, zeta, lambda2, sep = ",")) 
                
            }

            
        } else {
            
            if (data_complete) {
                
                lambda2 <- .9
                zeta <- .99
                print(paste("High-quality", cc, zeta, lambda2, sep = ","))
                
            } else if (custom == T) {
                
                lambda2 <- lambda
                zeta <- zeta
                print(paste("VR Adjustment", cc, zeta, lambda2, sep = ","))
              
            } else {
                lambda2 <- .9
                zeta <- .95
                print(paste("Low-quality", cc, zeta, lambda2, sep = ","))  
            }
            
            
        }


    ## loop through years
        
        
        first_data_year <- ifelse(lambda_by_year, region.data$year[in.country] %>% min(., na.rm = T), NA)
        
        
        for (yy in min_year:max_year) {
          
          count <- count + 1
          year <- yy


          if (lambda_by_year& (first_data_year != Inf) & yy < first_data_year & lambda2 < 1){
              
              n <- first_data_year - yy
              
              x <- first_data_year - 1950
              
              lambda3 <- lambda2 + (.9 - lambda2)*(n/x)
              
              print(paste0("lambda: ", lambda3, ", year:", yy))
              
          } else {lambda3 <- lambda2}
          
          
          # calculate time weights
          t <- abs(region.data$year - year)
          w <- (1-(t/(1+max(t)))^lambda3)^3
          
          # calculate space weights
          if (zeta < 1){
            if (other.resids) w[in.country] <- (zeta/(1-zeta))*(sum(w[!in.country])/sum(w[in.country]))*w[in.country]
          } else w[!in.country] <- 0

          # fit variant 1: linear local regression
          model.data <- data.frame(resid=region.data$resid, year=region.data$year, dd=as.numeric(in.country), w=w)
          if (sum(in.country)==0) {
            linear <- predict(lm(resid ~ year, weights=w, data=model.data),
                              newdata=data.frame(year=year))
          } else {

            linear <- predict(lm(resid ~ year + dd, weights=w, data=model.data),
                              newdata=data.frame(year=year, dd=1))
          }

          # fit variant 2: fixed effects local regression
          constant <- region.data$resid %*% (w/sum(w))

          # combine variants
          combined <- linear*d_dens + constant*(1-d_dens)
          preds[[count]] <- data.frame(ihme_loc_id=cc, fake_region_name =rr, year= yy, 
                                       pred.2.resid=combined,
                                       linear =linear*d_dens,
                                       constant = constant*(1-d_dens),
                                       weight=w[count], lambda = lambda2, zeta = zeta, stringsAsFactors=F)


        } # close year loop
      } # close country loop
  } # close region loop

  preds <- do.call("rbind", preds)
  
  return(preds)
  
} # close function

loess_resid <- function(data) {
  for (cc in unique(data$ihme_loc_id)) {
    ii <- (data$ihme_loc_id == cc)
    data$pred.2.final[ii] <- predict(loess(pred.2.raw ~ year, span=.3, data=data[ii,]))
  }
  data
}
