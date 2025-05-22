################################################################################
## Description: Defines the space-time model (2nd stage prediction model)
################################################################################

logit <- function(x) {
  return(log(x/(1-x)))
}

inv.logit <- function(x) {
  return(exp(x) / (1+exp(x)))
}

resid_space_time <- function(data, region = NULL, min_year = , max_year = , params, st_loess = ) {
  # Set up variables to hold results
  preds <- NULL
  count <- 0

  # Get the number of input data points for a country
  data$location_id <- as.numeric(data$location_id)
  data_density_numerator <- unlist(lapply(unique(data$location_id), function(x) sum(data$location_id == x & !is.na(data$resid))))
  names(data_density_numerator) <- unique(data$location_id)
  # Get the total number of years for a population
  data_density_denominator <- unlist(lapply(unique(data$location_id), function(x) sum(data$location_id == x)))
  # Calculate the density (input data points / possible data points)
  data_density <- data_density_numerator / data_density_denominator


  # If no region is specified in the function call, run all regions
  if (is.null(region)) {
    region <- sort(unique(data$region_name))
  }

  # Loop over region
  for (rr in region) {
    cat(paste(rr, "\n")); flush.console()
    # Get all the data for the region
    region_data <- data[data$region_name == rr & !is.na(data$resid),]
    if(nrow(region_data)>0){

      # Loop over each location in the region
      countries <- sort(unique(data$location_id[data$region_name==rr]))
      for (cc in countries) {
        to_keep <- unique(data$keep[data$location_id==cc & data$region_name==rr])
        in_country <- (region_data$location_id == cc)
        other_resids <- (sum(!in_country)>0)

        # Get lambda and zeta values
        lambda <- unique(params[params$location_id==cc & params$best==1,]$lambda)
        zeta <- unique(params[params$location_id==cc & params$best==1,]$zeta)

        lambda2 <- lambda
        print(cc)
        print(paste0("lambda is ", lambda2))
        print(paste0("zeta is ", zeta))

        # Loop through years
        for (yy in min_year:max_year) {
          count <- count + 1
          year <- yy + 0.5

          # Calculate time weights
          t <- abs(region_data$year - year)
          w <- (1 - (t / (1 + max(t)))^lambda2)^3

          # Calculate space weights
          if (zeta < 1) {
            if (other_resids) {
              w[in_country] <- (zeta / (1 - zeta)) * (sum(w[!in_country]) / sum(w[in_country])) * w[in_country]
            }
          } else {
            w[!in_country] <- 0
          }

          # Fit variant 1: linear local regression
          model_data <- data.frame(resid=region_data$resid, year=region_data$year, dd=as.numeric(in_country), w=w)
          if (sum(in_country)==0) {
            linear <- predict(lm(resid ~ year, weights=w, data=model_data),
                              newdata=data.frame(year=year))
          } else {
            linear <- predict(lm(resid ~ year + dd, weights=w, data=model_data),
                              newdata=data.frame(year=year, dd=1))
          }

          # Fit variant 2: fixed effects local regression
          constant <- region_data$resid %*% (w / sum(w))

          # combine variants
          if (st_loess) {
            combined <- linear * data_density[[toString(cc)]] + constant * (1 - data_density[[toString(cc)]])
          } else {
            combined <- constant * (1 - data_density[[toString(cc)]])
          }

          preds[[count]] <- data.frame(location_id=cc, year=(yy+.5),
                                       pred.2.resid=combined, weight=w[count],
                                       keep=to_keep, stringsAsFactors=F)
        }
      }

    }
  }

  # Combine all predictions
  preds <- do.call("rbind", preds)

  return(preds)
}

loess_resid <- function(data) {
  for (cc in unique(data$location_id)) {
    ii <- (data$location_id == cc)
    data$pred.2.final[ii] <- predict(loess(pred.2.raw ~ year, span=.3, data=data[ii,]))
  }
  data
}
