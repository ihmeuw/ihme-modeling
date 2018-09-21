################################################################################
## Description: Defines the space-time model (2nd stage prediction model)
################################################################################

logit <- function(x) log(x/(1-x))

inv.logit <- function(x) exp(x)/(1+exp(x))

logit10 <- function(x) log(x/(1-x),base=10)

inv.logit10 <- function(x) (10^(x))/(1+(10^(x)))
  
resid_space_time <- function(data, region=NULL, lambda=0.5, zeta=0.99, min_year=1950, max_year=2016, post_param_selection=F) {
  ## set up data frame to hold results
  preds <- NULL
  count <- 0
  
  root <- ifelse(Sys.info()[1]=="Windows","filepath","filepath")
  
  ## calculate the data density
  data_density_numerator <- unlist(lapply(unique(data$ihme_loc_id), function(x) sum(data$ihme_loc_id == x & !is.na(data$resid) & !is.na(data$vr) & data$vr == 1)))
  names(data_density_numerator) <- unique(data$ihme_loc_id)
  region_iso3s <- lapply(unique(data$region_name), function(x) unique(data$ihme_loc_id[data$region_name == x]))    
  names(region_iso3s) <- unique(data$region_name) 
  
  ## bring in pops for lambda determination
  root <- ifelse(Sys.info()[1]=="Windows","filepath","filepath")
  pop20mill <- read.csv(paste0("filepath"))
  large_pops <- unique(pop20mill$ihme_loc_id)
  
  if(post_param_selection==T){
  params <- read.csv(paste0("filepath"))
  }
  
  ## loop through regions
  if (is.null(region)) region <- sort(unique(data$region_name[!is.na(data$resid)])) # Don't loop over a region if they don't have any data/residuals to smooth (ex: subnational Kenya)
  for (rr in region) {

     cat(paste(rr, "\n")); flush.console()
    
  ## loop through sex
    for (ss in c("male", "female")) {
      region.data <- data[data$region_name == rr & data$sex == ss & !is.na(data$resid),]
      
  ## loop through country
      countries <- sort(unique(data$ihme_loc_id[data$region_name==rr]))
      for (cc in countries) {

       to_keep <- unique(data$keep[data$ihme_loc_id==cc & data$region_name==rr])
        if(post_param_selection==T){
          type = unique(region.data[region.data$ihme_loc_id==cc,]$type)

          # if there is no data use no data parameters
          if(nrow(region.data[region.data$ihme_loc_id==cc,])==0){
            type="no data"
          }

          lambda=params[params$type==type & params$best==1,]$lambda
          
          years_data = length(unique(region.data$year[region.data$ihme_loc_id==cc]))
          if(years_data >= 40){
            zeta = 0.99
          } else if(years_data <40 & years_data >= 30){
            zeta = 0.9
          } else if(years_data <30 & years_data >= 20){
            zeta = 0.8
          } else if(years_data <20 & years_data >= 10){
            zeta = 0.7
          } else{
            zeta = 0.6
          }
          if(cc == "SWE_4944") {
            zeta = 0.99
          }
          if(cc == "SRB"){
            zeta = 0.8
            lambda = 0.5
          }
          if(cc == "CHN_518"){
            lambda = 0.4
          }
          
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
#           if (year<=in.sample[1] & subnational == F) year <- in.sample[1]
#           if (year>=in.sample[2] & subnational == F) year <- in.sample[2]
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
          preds[[count]] <- data.frame(ihme_loc_id=cc, sex=ss, year=(yy+0.5), pred.2.resid=combined, keep=to_keep,  stringsAsFactors=F) 

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
