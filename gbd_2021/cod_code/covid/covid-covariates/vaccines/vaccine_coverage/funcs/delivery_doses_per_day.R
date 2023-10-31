
##########################################################
## Estimate vaccine doses that can be delivered by location. 
##########################################################

delivery_doses_per_day <- function(us_doses_per_day, model_inputs_path, hierarchy){
  
  ## What is the level (max doses delivered per day)
  doses_level <- us_doses_per_day
  
  ## Get population (total population)
  population <- model_inputs_data$load_all_populations(model_inputs_path)
  population <- population[age_group_id == 22 & sex_id == 3]
  
  ## Get hierarchy
  hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  population <- merge(population, hierarchy[,c("location_id","location_name","level","parent_id","most_detailed")], by = c("location_id"))
  
  ## What is the doses per day per capita? Standardized to United States  
  doses_rate <- doses_level / population[location_id==102]$population
  
  ## Which covariate should we use?
  covariate <- gbd_data$get_sdi()
  
  ## Apply GBD covariate value to missing COVID subnationals
  cov_fill <- rbind(hierarchy[parent_id %in% c(101, 81, 92, 570)], 
                    hierarchy[location_id == 60896]) # Dadra and Nagar Haveli and Daman and Diu
  cov_fill <- cov_fill[, c("location_name","location_id","parent_id")]
  cov_fill <- merge(cov_fill, covariate, by.x="parent_id", by.y="location_id")
  cov_fill[, c("parent_id","location_name.y") := NULL]
  setnames(cov_fill, "location_name.x", "location_name")
  
  covariate <- rbind(covariate, cov_fill)
  
  setdiff(hierarchy[most_detailed == 1]$location_id, covariate$location_id) 
  
  population <- merge(population, covariate[,c("location_id","mean_value")], by="location_id", all.x=T)
  #  population[is.na(mean_value)]$location_name
  
  ## Now determine the rate based on the pinned US level
  us_level <- population[location_id == 102]$mean_value
  population$rate <- doses_rate
  population[, us_ratio := mean_value / us_level]
  
  population[, delivery_rate := rate * us_ratio]
  population[, delivery_count := delivery_rate * population]
  
  return(population)
}
