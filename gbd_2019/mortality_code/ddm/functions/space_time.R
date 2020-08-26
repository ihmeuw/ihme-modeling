################################################################################
## Description: Defines the space-time model (2nd stage prediction model)
################################################################################

logit <- function(x) log(x/(1-x))

inv.logit <- function(x) exp(x)/(1+exp(x))
  
resid_space_time <- function(data, region=NULL, lambda=NULL, zeta=0.8, min_year=1950, max_year=2017) {
  lambda_names <- names(lambda)

  ## set up data frame to hold results
  preds <- NULL
  count <- 0
  
  ## loop through regions
  if (is.null(region)) region <- sort(unique(data$super_region_name))
  for (rr in region) {
    cat(paste(rr, "\n")); flush.console()
    region.data <- data[data$super_region_name == rr & !is.na(data$resid),]
    countries <- sort(unique(data$iso3_sex_source[data$super_region_name == rr]))
    
## loop through country-source type
    for (cc in countries) {
      print(cc)
      underscore_cc <- gsub("&&", "_", cc)
      if(underscore_cc %in% lambda_names) sel_lambda <- lambda[[underscore_cc]]
      else sel_lambda <- lambda[["default"]]

      in.country <- (region.data$iso3_sex_source == cc)
      other.resids <- (sum(!in.country)>0)

  ## loop through years
      for (yy in min_year:max_year) {
        count <- count + 1

        # calculate time weights
        t <- abs(region.data$year - yy)
        w <- (1-(t/(1+max(t)))^sel_lambda)^3

        # calculate space weights
        if (other.resids) w[in.country] <- (zeta/(1-zeta))*(sum(w[!in.country])/sum(w[in.country]))*w[in.country]

        # find weighted average 
        constant <- region.data$resid %*% (w/sum(w))
        preds[[count]] <- data.frame(iso3_sex_source=cc, year=yy, pred.resid=constant, stringsAsFactors=F)

      } # close year loop
    } # close country-source type loop
  } # close region loop

  preds <- do.call("rbind", preds)
} # close function

loess_resid <- function(data) {
  for (cc in unique(data$iso3_sex_source)) {
    ii <- (data$iso3_sex_source == cc)
    data$pred.2.final[ii] <- predict(loess(pred.2.raw ~ year, span=0.3, data=data[ii,]))
  }
  data
}