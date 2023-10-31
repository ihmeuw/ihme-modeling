
process_gavi_allocations <- function(data_root, 
                                     gavi_dose_scenario, 
                                     location_hierarchy){

##------------------------------------------------------------------
## 1.3 billion dose scenario

  gavi <- model_inputs_data$load_gavi_data(data_root, "less")
  
## Dan Hogan says divide J&J by 2
  gavi[, dec_jj_doses_1.3b := dec_jj_doses_1.3b / 2]

# AstraZeneca (Serum Institute of India, right?)
  az <- data.table(melt(gavi[, c("iso3","country_name","april_az_doses","june_az_doses","dec_az_doses_1.3b")], id.vars = c("iso3","country_name")))
  az[, date := fifelse(variable %like% "april", as.Date("2021-02-01"),
                      fifelse(variable %like% "june", as.Date("2021-04-01"), as.Date("2021-07-01")))]
  az[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  
  az$company <- "Serum Institute of India AZD"

# Pfizer
  pz <- data.table(melt(gavi[, c("iso3","country_name","april_pfizer_doses","june_pfizer_doses","dec_pfizer_doses_1.3b")], 
                        id.vars = c("iso3","country_name")))
  pz[, date := fifelse(variable %like% "april", as.Date("2021-04-01"),
                       fifelse(variable %like% "june", as.Date("2021-06-01"), as.Date("2021-10-01")))]
  pz[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  
  pz$company <- "Pfizer/BioNTech"
  
# Johnson and Johnson
  jj <- data.table(melt(gavi[, c("iso3","country_name","dec_jj_doses_1.3b")], id.vars = c("iso3","country_name")))
  jj[, date := fifelse(variable %like% "april", as.Date("2021-02-01"),
                       fifelse(variable %like% "june", as.Date("2021-04-01"), as.Date("2021-07-01")))]
  jj[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  
  jj$company <- "Janssen"
  
  gavi_out <- rbind(az, pz, jj)
  gavi_out$gavi_scenario <- "1.3 billion"

##--------------------------------------------------------------
# 1.8 billion scenario

  gavi <- model_inputs_data$load_gavi_data(data_root, "more")
  
  ## Dan Hogan says divide J&J by 2
  gavi[, dec_jj_doses_1.8b := dec_jj_doses_1.8b / 2]
  
  # AstraZeneca (Serum Institute of India, right?)
  az <- data.table(melt(gavi[, c("iso3","country_name","april_az_doses","june_az_doses","dec_az_doses_1.8b")], id.vars = c("iso3","country_name")))
  az[, date := fifelse(variable %like% "april", as.Date("2021-02-01"),
                       fifelse(variable %like% "june", as.Date("2021-04-01"), as.Date("2021-07-01")))]
  az[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  
  az$company <- "Serum Institute of India AZD"
  
  # Pfizer
  pz <- data.table(melt(gavi[, c("iso3","country_name","april_pfizer_doses","june_pfizer_doses","dec_pfizer_doses_1.8b")], 
                        id.vars = c("iso3","country_name")))
  pz[, date := fifelse(variable %like% "april", as.Date("2021-02-01"),
                       fifelse(variable %like% "june", as.Date("2021-04-01"), as.Date("2021-07-01")))]
  pz[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  
  pz$company <- "Pfizer/BioNTech"
  
  # Johnson and Johnson
  jj <- data.table(melt(gavi[, c("iso3","country_name","dec_jj_doses_1.8b")], id.vars = c("iso3","country_name")))
  jj[, date := fifelse(variable %like% "april", as.Date("2021-02-01"),
                       fifelse(variable %like% "june", as.Date("2021-04-01"), as.Date("2021-07-01")))]
  jj[, quarter_doses := value - shift(value, fill = 0), by= "country_name"]
  
  jj$company <- "Janssen"
  
  gavi_scenario_2 <- rbind(az, pz, jj)
  gavi_scenario_2$gavi_scenario <- "1.8 billion" 
  
  gavi_out <- rbind(gavi_out, gavi_scenario_2)
  
  ## Lose Kosovo, what to do with that?
  gavi_out <- merge(gavi_out, location_hierarchy[,c("ihme_loc_id","location_id","location_name")],
                    by.x = "iso3", by.y = "ihme_loc_id")
  
  gavi_out[, c("country_name","variable","value") := NULL]
  
  split1 <- gavi_out[date == "2021-07-01"]
  split1[, date := as.Date("2021-10-01")]
  split1[, quarter_doses := quarter_doses / 2]
  
  gavi_out[date == "2021-07-01", quarter_doses := quarter_doses / 2]
  
  gavi_out <- rbind(gavi_out, split1)
  gavi_out[date == "2021-02-01", date := as.Date("2021-03-01")]
  
  if(gavi_dose_scenario == "more"){
    gavi_out <- gavi_out[gavi_scenario == "1.8 billion"]
  } else {
    gavi_out <- gavi_out[gavi_scenario == "1.3 billion"]
  }
  
  gavi_out[date == "2021-06-01", date := as.Date("2021-07-01")]
  gavi_out[, named_doses := quarter_doses]
  # Drop if Pfizer == 0
  gavi_out <- gavi_out[!(company == "Pfizer/BioNTech" & named_doses == 0)]
  
  ## Remove future doses of AZ https://www.nytimes.com/live/2021/05/20/world/covid-vaccine-coronavirus-mask/the-global-vaccination-effort-already-in-dire-shape-faces-more-setbacks
  gavi_out <- gavi_out[!(date > "2021-05-25" & company == "Serum Institute of India AZD")]

  return(gavi_out)
}

  