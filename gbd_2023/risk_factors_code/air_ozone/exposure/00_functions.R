# OZONE HELPER FUNCTIONS

# this function is used to estimate pollution for islands or small countries that dont have large enough borders to pick up a grid
# the way it works is by walking around the border using 1 degree of padding (+.5 degree everytime it fails) until we pick up more than 10 grids
# we use the average of these grids as the estimate
estimateIslands <- function(country,
                            borders,
                            location_id.list) {
  
  # Get a rectangle around the island in question
  poly <- borders@polygons[[which(borders$location_id == country)]]
  
  # Begin with 5 degrees of padding in every direction
  distance.to.look <- 1
  looping <- TRUE
  
  #define the true extent of the island polygon
  min.x <- 1000
  min.y <- 1000
  max.x <- -1000
  max.y <- -1000
  
  for (iii in 1:length(poly@Polygons)) {
    max.x <- max(max.x, max(poly@Polygons[[iii]]@coords[,1]))
    max.y <- max(max.y, max(poly@Polygons[[iii]]@coords[,2]))
    min.x <- min(min.x, min(poly@Polygons[[iii]]@coords[,1]))
    min.y <- min(min.y, min(poly@Polygons[[iii]]@coords[,2]))
  }
  
  # this loop will continue to add a half degree of padding in every direction as long as we are unable to find 10 surrounding grids
  while (looping) {
    
    #print loop status
    cat(location_id.list[location_id==country, location_name],
        "-trying w/ degrees of padding:",
        distance.to.look,
        "\n"); flush.console
    
    #add the padding to your island polygon
    padded.min.x <- min.x - distance.to.look
    padded.min.y <- min.y - distance.to.look
    padded.max.x <- max.x + distance.to.look
    padded.max.y <- max.y + distance.to.look
    
    # find out how many grids fall within the current (padded) extent
    temp <- grid[which(grid$Longitude <= padded.max.x
                            & grid$Longitude >= padded.min.x
                            & grid$Latitude <= padded.max.y
                            & grid$Latitude >= padded.min.y), ]
    # drop missing grids
    temp <- na.omit(temp)
    
    # add a half degree to the distance in case we end up needing to reloop
    distance.to.look <- distance.to.look + .5
    
    # inform the loop whether we have discovered more than 10 grids nearby using the current padding
    looping <- !(nrow(temp) > 10)
    
    # output loop status and if we were successful how many grids were found
    loop.output <- ifelse(looping==TRUE, "FAILED", paste0("SUCCESS, pixels found #", nrow(temp)))
    cat(loop.output, "\n"); flush.console()
    
  }
  
  # Prep to be added on to the dataset
  temp$location_id <- country
  temp$location_name <- as.character(location_id.list[location_id==country,.(location_name)])
  temp$Longitude <- round(mean(max.x, min.x) * 20) / 20 # All real grids are at .05 units of latitude/longitude.
  temp$Latitude <- round(mean(max.y, min.y) * 20) / 20
  temp$Weight <- 1
  
  temp <- as.data.table(temp)
  
  return(temp)
  
}


# this function is used to forecast pollution (since we only have it up to 2011 at this time)
# we fit splines to the data from 1990-2011 and then use them to predict 2012-2015
# TODO current issue is that i cant predict any grids that have a missing observation from 1990-2011
# spline can only be fit with 4 obvs, so because of that i just decided to skip any grids that have missing data
# the missing data is usually in the earlier years
# future fix could be to use a different method to pred these but right now i dont think it is worth it
splinePred <- function(dt,
                       this.grid,
                       pred.vars,
                       start.year,
                       end.year) {
  
  #cat("~",this.grid); flush.console() #toggle for troubleshooting/monitoring loop status
  
  pred.length <- (end.year - start.year) + 1
  
  pred.dt <- dt[grid==this.grid & year %in% c(1990, 2000, 2010)] # these are the only values that haven't already been predicted using splines (IE real data)
  
  forecast <- dt[grid==this.grid, -c("year", pred.vars), with=F]
  forecast[1:pred.length, "year" := start.year:end.year]
  
  forecast[1:pred.length,
           c(pred.vars) := lapply(pred.vars,
                                  function(var)
                                    ifelse(rep(any(is.na(pred.dt[, var, with=F])), #test if any obv is NA, spline needs 4+ obvs to fit
                                               pred.length), # note i had to add the rep*pred.length because ifelse returns things in shape of test
                                           NA, #if so, return NA for the pred
                                           predict(lm(get(var) ~ ns(year), data=pred.dt),
                                                   newdata=data.frame(year=year)))),
           with=F]
  
  return(na.omit(forecast))
  
}


#EXTRAPOLATE at gridcell level. This function takes in a dataset, isolates to each gridcell, runs a linear model to calculate the average rate of change overtime and extrapolates into the future
lm_extrap <- function(i,dt,in_years,out_years,pred_vars){
  
  this_id <- unique(map$id)[i]
  
  extrap_dt <- copy(dt[id==this_id & year_id %in% in_years])
  
  pred_dt <- data.table()
  for(year in out_years){
    row <- copy(extrap_dt[1])
    row[,year_id:=year]
    pred_dt <- rbind(pred_dt,row)
  }
  
  for(var in pred_vars){
    mod <- lm(log(get(var))~year_id, data=extrap_dt)
    pred_dt[,c(var):=predict(mod,newdata=pred_dt) %>% exp]
  }
  
  return(pred_dt)
  
  cat(paste("Finished with grid dcell,", i, "of", length(unique(map$id))))
  
}

