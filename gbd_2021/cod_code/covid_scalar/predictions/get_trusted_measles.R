##################################################
## Project: CVPDs
## Script purpose: Get trusted measles estimates from WHO data
## Date: April 2021
## Author: username
##################################################
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}
library(data.table)
library(plyr)
# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("FILEPATH"), source))

get_trusted_measles <- function(years, gbd_round_id, decomp_step, time, se = F, draws = F, n_draws = NULL, 
                                measles_path = paste0("FILEPATH"),
                                fill_partial_year_zero = F){
  # get location data
  hierarchy <- get_location_metadata(35, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  
  dt <- as.data.table(readxl::read_xlsx(measles_path, sheet = "WEB"))
  setnames(dt, c("ISO3", "Year"), c("ihme_loc_id", "year_id"))
  dt <- dt[year_id %in% years]
  dt[, `:=` (Region = NULL, Country = NULL)]
  my_data <- melt(dt, id.vars = c("ihme_loc_id", "year_id"), value.name = "cases", variable.name = "month")
  my_data[, `:=` (cases = as.numeric(cases))]
  my_data <- merge(my_data, hierarchy[,.(location_id, ihme_loc_id)], by = "ihme_loc_id")
  elimination_locs <- c("BTN", "LKA", "MDV", "CHN_354", "CHN_361", "TLS", "BHR", "KHM", "PRK", "OMN", "IRN", "JOR", "CHN") # elimination locations outside of trusted CN superregions. 
  trusted_srs <- c(64, 31, 103)
  # set up trusted_locs
  trusted_locs <- hierarchy[super_region_id %in% trusted_srs | ihme_loc_id %in% elimination_locs, location_id]
  trusted_cn <- my_data[location_id %in% trusted_locs]
  
  if(fill_partial_year_zero){
    # Fill NAs in years that only have sporadic NAs with Os
    trusted_cn[, sumNA := sum(is.na(cases)), by = c("location_id", "year_id")]
    trusted_cn[sumNA > 0 & sumNA < 12 & is.na(cases), cases := 0]
  }
  
  # for locations with data in 2020 and no data in 2021, this will put NAs for cases in 2021 and it will fill with loc ratio
  trusted_cn <- trusted_cn[!is.na(cases)]
  
  # aggregate to annual if desired
  if (time == "annual"){
    trusted_cn <- trusted_cn[, .(cases = sum(cases)), .(location_id, ihme_loc_id, year_id)]
  } else if (time == "monthly"){
    trusted_cn[, month:= as.integer(match(month, month.name))]
  }
  adjusted_notifications <- copy(trusted_cn)
  # get draws if desired
  if (se|draws) {
    # get the standard error from the sample size (u5 population)
    pop_u5 <- get_population(age_group_id = 1, gbd_round_id = gbd_round_id, status = "best",
                             decomp_step = decomp_step, sex_id = 3, location_id = unique(trusted_cn$location_id), year_id = years)
    adjusted_notifications <- merge(trusted_cn, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
    adjusted_notifications <- merge(adjusted_notifications, hierarchy[, .(location_id, region_id, super_region_id)], by="location_id", all.x=TRUE)
    invisible(adjusted_notifications[, inc_rate := cases / population])
    adjusted_notifications[, SE := sqrt( (inc_rate * (1 - inc_rate)) / population)]
    # calculate region and super-region average error for country-years with 0 cases reported
    notif_error_reg <- ddply(adjusted_notifications, c("region_id", "year_id"), summarise, reg_SE=mean(SE, na.rm=TRUE))
    notif_error_sr <- ddply(adjusted_notifications, c("super_region_id", "year_id"), summarise, sr_SE=mean(SE, na.rm=TRUE))
    # add on region and super-region mean error, replace with these errors if country-level error is zero
    adjusted_notifications <- merge(adjusted_notifications, notif_error_reg, by=c("region_id", "year_id"), all.x=TRUE)
    adjusted_notifications <- merge(adjusted_notifications, notif_error_sr, by=c("super_region_id", "year_id"), all.x=TRUE)
    invisible(adjusted_notifications[SE==0 | is.na(SE), SE := reg_SE])
    invisible(adjusted_notifications[SE==0 | is.na(SE), SE := sr_SE])
    # if region and super region error is still zero, calculate non-zero SE
    adjusted_notifications[SE==0, SE := sqrt( ((1 / population) * inc_rate * (1 - inc_rate)) + ((1 / (4 * (population ^ 2))) * ((qnorm(0.975)) ^ 2)) ) ]
    # bring SE back into case space
    adjusted_notifications[, SE := SE*population]
    if(draws){
      if(is.null(n_draws))n_draws <- 1000
      # Generate draws of observed cases using binomial distribution
      set.seed(2)
      inc_draws <- rbinom(n=n_draws * length(adjusted_notifications$inc_rate), prob=adjusted_notifications$inc_rate, size=as.integer(adjusted_notifications$pop)) %>% matrix(., ncol=n_draws, byrow=FALSE)
      setDT(adjusted_notifications)
      colnames(inc_draws) <- paste0("inc_draw_", 0:(n_draws-1))
      adjusted_notifications <- cbind(adjusted_notifications, inc_draws)
    } 
  }
  return(adjusted_notifications)
}