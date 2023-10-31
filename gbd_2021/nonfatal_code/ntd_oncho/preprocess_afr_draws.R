# NTDS: Onchocerciasis
# Purpose: function to process oncho EG draws
# Notes: code taken from legacy stata script 

preprocess_afr_draws <- function(data, out_dir){
  
  outcomes <- c("wormcases", "mfcases", "blindcases", "VIcases", "OSDcases1acute",
                "OSDcases1chron", "OSDcases2acute", "OSDcases2chron", "OSDcases3acute", "OSDcases3chron")
  
  data[location_name == "Guinea Bissau", location_name := "Guinea-Bissau"]
  data[grep('voire', location_name), location_name := "Côte d'Ivoire"]
  data[location_name == "Eq.Guinea", location_name := "Equatorial Guinea"]
  data[location_name == "RDC", location_name := "Democratic Republic of the Congo"]
  data[location_name == "CAR", location_name := "Central African Republic"]
  data[location_name == "Tanzania", location_name := "United Republic of Tanzania"]
  
  
  locs <- get_location_metadata(35, gbd_round_id = gbd_round_id, decomp_step=decomp_step)
  locs <- locs[level == 3, .(location_name, ihme_loc_id, location_id, level)]
  
  data <- merge(data, locs, by = "location_name", all.x = TRUE)
  setnames(data, c("year", "sex"), c("year_id", "sex_id"))
  
  # need to create age group ids and 2, 3, 4, 30, 31, 32, 235
  # 2 3 4 0; drop 28
  # split 28 equally to 30 31 32 235, drop 21
  data[, age_group_id := age/5 + 5] 
  data[age == 1, age_group_id := 34] 
  
  # age group ids 2, 3, 388, 389 are 0
  zero_ages <- data[age == 0,] # change to 2, 3, 388, 389, or 238 ; sopy or else aliased
  zero_ages_2 <- copy(zero_ages[, age_group_id := 2])
  zero_ages_3 <- copy(zero_ages[, age_group_id := 3]) 
  zero_ages_388 <- copy(zero_ages[, age_group_id := 388])
  zero_ages_389 <- copy(zero_ages[, age_group_id := 389])
  zero_ages_238 <- copy(zero_ages[, age_group_id := 238])
  
  data <- data[age != 0]
  data <- rbind(data, zero_ages_2, zero_ages_3, zero_ages_388, zero_ages_389, zero_ages_238)
  
  # breakout 21 to 30 31 32 235, dividing by 4 doesnt work, take global proportions (case - space)
  old_ages <- data[age_group_id == 21]
  
  old_ages_30  <- copy(old_ages[, age_group_id := 30])
  old_ages_31  <- copy(old_ages[, age_group_id := 31]) 
  old_ages_32  <- copy(old_ages[, age_group_id := 32]) 
  old_ages_235 <- copy(old_ages[, age_group_id := 235]) 
  old_ages <- rbind(old_ages_30, old_ages_31, old_ages_32, old_ages_235)
  
  old_population <- get_population(location_id = 166, sex_id = 3, age_group_id = c(30, 31, 32, 235), year_id = 2000, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  old_population[, total_pop := sum(population)]
  old_population[, prop := population / total_pop]
  old_population <- old_population[, .(age_group_id, prop)]
  
  old_ages <- merge(old_ages, old_population)
  
  for (outcome in outcomes){
    old_ages[, eval(outcome) := get(outcome) * prop]
  }
  
  old_ages[, prop := NULL]
  data <- rbind(data, old_ages)
  data <- data[age_group_id != 21]
  
  # create vi_mod and vi_severe cases
  # Visual impairment was then split into moderate and severe vision impairment by first multiplying the visual impairment estimates by a random value 
  # (from a normal distribution with mean 0.84 and standard deviation 0.0031) to generate moderate vision impairment, and then subtracting the resulting 
  # estimates from visual impairment to obtain estimates of severe vision impairment.
  
  prop <- data.table(prop_mod = rnorm(n = 1000, mean = 0.84, sd = 0.0031), draw = 0:999)
  data <- merge(data, prop, by = "draw")
  data[, vi_mod_cases := VIcases * prop_mod]
  data[, vi_sev_cases := VIcases * (1 - prop_mod)]
  
  data[, level:= NULL]
  data[, VIcases:= NULL]
  data[, prop_mod:= NULL]
  data[, age:= NULL]
  
  
  # update w/ vi_mod and vi_sev w/o VIcases
  outcomes <- c("wormcases", "mfcases", "blindcases",  "OSDcases1acute",
                "OSDcases1chron", "OSDcases2acute", "OSDcases2chron", "OSDcases3acute", "OSDcases3chron", "vi_mod_cases",
                "vi_sev_cases")
  
  
  # change to prevalence space by dividing by population
  data[blindcases == Inf, blindcases := 0]
  
  population <- get_population(location_id = unique(data$location_id), sex_id = c(1,2), age_group_id = "all", year_id = c(1990, 1995, 2000, 2005, 2010, 2013), gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  data <- merge(data, population, by = c("location_id", "sex_id", "age_group_id", "year_id"), all.x = TRUE)
  if(nrow(data[is.na(population)]) > 0){ stop("population did not merge correctly") }
  
  for (outcome in outcomes){
    data[, eval(outcome) := get(outcome) / population]
  }
  
  data[, population:= NULL]
  
  # prepped as was
  data <- melt(data, measure.vars = outcomes, variable.name = "outvar", value.name = "prevalences")
  
  cat("\n Starting to write out\n")
  
  for( loc_id in unique(data[, location_id]) ){
    data1 <- copy(data[location_id == loc_id])
    data1 <- dcast(data1, ... ~ draw, value.var = "prevalences")
    setnames(data1, as.character(0:999), paste0("prevalence", 0:999))
    data1[, gbdyear := 1]
    data1[, OKyear := 1]
    data1[, outvar := tolower(outvar)]
    
    file_path <- paste0(out_dir, '/', loc_id, ".csv")
    fwrite(data1, file_path)
    message(sprintf('Wrote %d rows of data to %s', nrow(data1), file_path))
  }
  
}