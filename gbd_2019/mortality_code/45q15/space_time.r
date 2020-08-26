################################################################################
## Description: Defines the space-time model (2nd stage prediction model)
################################################################################

logit <- function(x) log(x/(1-x))

inv.logit <- function(x) exp(x)/(1+exp(x))

logit10 <- function(x) log(x/(1-x),base=10)

inv.logit10 <- function(x) (10^(x))/(1+(10^(x)))

resid_space_time <- function(data, region=NULL, lambda=0.5, zeta=0.99, min_year=1950, max_year, param_data=NULL) {
  ## set up data frame to hold results
  preds <- NULL
  count <- 0
  
  ## calculate the data density
  data_density_numerator <- unlist(lapply(unique(data$ihme_loc_id), function(x) sum(data$ihme_loc_id == x & !is.na(data$resid) & !is.na(data$vr) & data$vr == 1)))
  names(data_density_numerator) <- unique(data$ihme_loc_id)
  region_iso3s <- lapply(unique(data$region_name), function(x) unique(data$ihme_loc_id[data$region_name == x]))
  names(region_iso3s) <- unique(data$region_name)
  
  if(!is.null(param_data)){
    params <- param_data
  }
  
  ## loop through regions
  #region="Central_Sub_Saharan_Africa"
  if (is.null(region)) region <- sort(unique(data$region_name[!is.na(data$resid)])) # Don't loop over a region if they don't have any data/residuals to smooth (ex: subnational Kenya)
  for (rr in region) {
    #rr="North_Korea"
    cat(paste(rr, "\n")); flush.console()
    
    ## loop through sex
    for (ss in c("male", "female")) {
      # ss="male"
      region.data <- data[data$region_name == rr & data$sex == ss & !is.na(data$resid),]
      
      ## loop through country
      countries <- sort(unique(data$ihme_loc_id[data$region_name==rr]))
      for (cc in countries) {
        to_keep <- unique(data$keep[data$ihme_loc_id==cc & data$region_name==rr])
        
        # Get location-specific lambda and zeta values from parameter file
        if (!is.null(param_data)) {
          lambda = params[params$ihme_loc_id == cc & params$sex == ss,]$lambda
          zeta = params[params$ihme_loc_id == cc & params$sex == ss,]$zeta
        }
        
        print(paste("zeta is ", zeta, "for country", cc))
        flush.console()
        
        print(paste("lambda is ", lambda, "for country", cc))
        flush.console()
        
        in.country <- (region.data$ihme_loc_id == cc)
        other.resids <- (sum(!in.country)>0)
        
        # for countries with data, find the first and last year of that data, for countries without data, find the first and last year of data in the region
        if (sum(in.country)>0) in.sample <- range(region.data$year[in.country]) else in.sample <- range(region.data$year)
        
        ## loop through years
        for (yy in min_year:max_year) {
          count <- count + 1
          year <- yy + 0.5
          if (year<=in.sample[1]) year <- in.sample[1]
          if (year>=in.sample[2]) year <- in.sample[2]
          
          # calculate time weights
          t <- abs(region.data$year - year)
          w <- (1-(t/(1+max(t)))^lambda)^3
          
          # calculate space weights
          if (other.resids) w[in.country] <- (zeta/(1-zeta))*(sum(w[!in.country])/sum(w[in.country]))*w[in.country]
          
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
          if(max(data_density_numerator[region_iso3s[[rr]]])!=0){
            data_density <- data_density_numerator[cc]/max(data_density_numerator[region_iso3s[[rr]]])
          }else{
            data_density=0
          }
          combined <- linear*data_density + constant*(1-data_density)
          preds[[count]] <- data.frame(ihme_loc_id=cc, sex=ss, year=(yy+0.5), pred.2.resid=combined, keep=to_keep,  stringsAsFactors=F) #ADD KEEP
          
        } # close year loop
      } # close country loop
    } # close sex loop
  } # close region loop
  preds <- do.call("rbind", preds)
} # close function

loess_resid <- function(data) {
  for (cc in unique(data$ihme_loc_id)) {
    for (ss in unique(data$sex)) {
      ii <- (data$ihme_loc_id == cc & data$sex == ss)
      data$pred.2.final[ii] <- predict(loess(pred.2.raw ~ year, span=0.3, data=data[ii,]))
    }
  }
}