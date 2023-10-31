
pre_stockpile_modifications <- function(pre_stockpile_purchase_candidates, location_hierarchy){
  ## Not sure why this is happening...
  pre_stockpile_purchase_candidates[trial_stage == "phase3" & 
                                      date == "2021-01-01", 
                                    c("date") := as.Date(expected_ready)]
  
  ## Update Moderna date (available on December 12)
  pre_stockpile_purchase_candidates[company == "Moderna" & 
                                      date == as.Date("2020-12-15"), 
                                    date := as.Date("2020-12-22")]
  
  # Update Russian and Chinese vaccines to be in use in those countries (but not elsewhere that has purchased)
  # Bahrain and Serbia both have Gamelaya starting in January. 
  
  pre_stockpile_purchase_candidates[location_id %in% c(62, location_hierarchy[parent_id == 6, location_id], 140, 53) &
                                      candidate %in% unique(pre_stockpile_purchase_candidates[trial_stage == "limited_use", candidate]), 
                                    expected_ready := as.Date("2020-12-15")]
  pre_stockpile_purchase_candidates[location_id %in% c(62, location_hierarchy[parent_id == 6, location_id], 140, 53) &
                                      candidate %in% unique(pre_stockpile_purchase_candidates[trial_stage == "limited_use", candidate]), 
                                    probability := 1]
    
  ## Remove Pfizer in African Union from 2020
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[!(location == "African Union" & 
                                                                             company == "Pfizer/BioNTech" & 
                                                                             date < "2021-04-01")]
  ## Remove Janssen and Serum Institute India AZD from African Union until April 1
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[!(location == "African Union" & 
                                                                             company %in% c("Janssen","Serum Institute of India AZD") & 
                                                                             date < "2021-04-01")]
  ## Remove Pfizer in Covax from 2020
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[!(location == "COVAX" & 
                                                                             company == "Pfizer/BioNTech" & 
                                                                             date < "2021-01-01")]
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[!(location == "COVAX AMC" & 
                                                                             company == "Pfizer/BioNTech" & 
                                                                             date < "2021-01-01")]
    
  ## Remove J&J in COVAX until Q2 2021 (https://www.gavi.org/sites/default/files/covid/covax/COVAX%20Supply%20Forecast.pdf) 
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[!(location == "COVAX" & 
                                                                             company == "Janssen" & 
                                                                             date < "2021-04-01")]
    
  ## Remove Novavax in COVAX (https://www.nytimes.com/live/2021/05/20/world/covid-vaccine-coronavirus-mask/the-global-vaccination-effort-already-in-dire-shape-faces-more-setbacks)
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[!(location == "COVAX AMC" & 
                                                                             company == "Serum Institute of India NVX")]
    
  ## Delay Novavax outside of COVAX until Q3 2021 (https://www.washingtonpost.com/health/2021/05/10/novavax-coronavirus-vaccine/)
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[!(location != "COVAX" & 
                                                                             company == "Novavax" & 
                                                                             date < "2021-07-01")]
    
  ## Remove AstraZeneca entirely from the US
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[!(location == "United States of America" & 
                                                                             company == "AstraZeneca")]

  # Denmark never started using AZ again https://www.nytimes.com/2021/04/14/world/europe/EU-Pfizer-AstraZeneca-Covid.html
  # Or Johnson and Johnson
  pre_stockpile_purchase_candidates[company == "AstraZeneca" & 
                                      location_id == 78 & 
                                      date >= "2021-04-01", 
                                    location_secured_daily_doses := 0]
  pre_stockpile_purchase_candidates[company == "Janssen" & 
                                      location_id == 78 & 
                                      date >= "2021-04-01", 
                                    location_secured_daily_doses := 0]
    
  # No more AZ in Norway (https://www.bloomberg.com/news/articles/2021-05-12/norway-permanently-removes-astrazeneca-from-vaccine-program)
  pre_stockpile_purchase_candidates[company == "AstraZeneca" & 
                                      location_id == 90 & 
                                      date >= "2021-05-01", 
                                    location_secured_daily_doses := 0]
    
  return(pre_stockpile_purchase_candidates)  
}

make_stockpile <- function(pre_stockpile_purchase_candidates, location_hierarchy){
  # Find doses by quarter, set to 0 if "expected date" < start of quarter?
  pre_stockpile_purchase_candidates[, quarterly_probable_doses := quarter_capacity * probability]
  pre_stockpile_purchase_candidates[, quarterly_doses := ifelse(date > expected_ready, quarterly_probable_doses, 0)]
  pre_stockpile_purchase_candidates[, cumulative_quarter := cumsum(quarterly_doses), by = c("candidate","company")]
  
  ## Find doses by day, assuming a start_date between quarters will have capacity days_remaining_quarter / quarter_capacity
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[order(company, date, location_id)]
  
  # Date when vaccine is available (either start of the quarter or expected first day)
  pre_stockpile_purchase_candidates[, effective_date := fifelse(as.Date(date) >= as.Date(expected_ready), as.Date(date), as.Date(expected_ready))]
  pre_stockpile_purchase_candidates[, effective_days := shift(effective_date - shift(effective_date), n = -1), by=c("company","location_id","location")]
  pre_stockpile_purchase_candidates[, effective_days := ifelse(effective_date == "2020-12-15", 17, effective_days)]
  pre_stockpile_purchase_candidates[, effective_days := ifelse(effective_date == "2020-12-22", 10, effective_days)]
  pre_stockpile_purchase_candidates[, effective_days := ifelse(is.na(effective_days), as.Date("2022-01-01") - as.Date("2021-10-01"), effective_days)]
  
  pre_stockpile_purchase_candidates[, quarter_days := as.numeric(quarter_days)]  
  pre_stockpile_purchase_candidates[, quarter_days := ifelse(quarter == "Q4-2020", 17, quarter_days)]
  pre_stockpile_purchase_candidates[, quarter_days := ifelse(effective_date == "2020-12-22", 10, quarter_days)]
  pre_stockpile_purchase_candidates[, effective_quarter := effective_days / quarter_days]
  
  pre_stockpile_purchase_candidates[, daily_probable_doses := quarterly_probable_doses / quarter_days * effective_quarter]
  pre_stockpile_purchase_candidates[, daily_probable_doses := ifelse(daily_probable_doses == "Inf", 0, daily_probable_doses)]
  
  # Drop if no daily doses
  pre_stockpile_purchase_candidates <- pre_stockpile_purchase_candidates[effective_days > 0]
  pre_stockpile_purchase_candidates[date < effective_date, date := effective_date]
  
  # Create a stockpile of vaccine
  pre_stockpile_purchase_candidates[, stockpile_start := min(date), by = c("location_id","candidate")]
  pre_stockpile_purchase_candidates[, stockpile := ifelse(stockpile_start == date, quarterly_probable_doses / 30, 0)]
  
  # No stockpile for Russian, Chinese vaccines (in country)
  # Add Bahrain here?
  
  pre_stockpile_purchase_candidates[location_id %in% c(location_hierarchy[parent_id == 6, location_id], 140, 53) &
                                      candidate %in% unique(pre_stockpile_purchase_candidates[trial_stage == "limited_use", candidate]), 
                                    stockpile := 0]
  # Don't over vaccinate for Russian, Chinese vaccines
  pre_stockpile_purchase_candidates[location_id %in% c(62, location_hierarchy[parent_id == 6, location_id], 140, 53) & 
                                      date == "2020-12-15" &
                                      candidate %in% unique(pre_stockpile_purchase_candidates[trial_stage == "limited_use", candidate]), 
                                    daily_probable_doses := quarterly_probable_doses / 90]
  
  return(pre_stockpile_purchase_candidates)
}


##--------------------------------------------------------------
## Changes after stockpile 
post_stockpile_modifications <- function(purchase_candidates){
  
  # Change "stockpile" to our known number of doses by Jan 1 for Pfizer, Moderna
    purchase_candidates[, stockpile := ifelse(candidate == "BNT-162" & date == "2020-12-15", 32000000 / 17, stockpile)]
    purchase_candidates[, stockpile := ifelse(company == "Moderna" & date == "2020-12-22", 13000000 / 10, stockpile)]
    purchase_candidates[company == "Pfizer/BioNTech" & date == "2020-12-15", daily_probable_doses := stockpile]
    purchase_candidates[company == "Moderna" & date == "2020-12-22", daily_probable_doses := stockpile]
  
  # Find proportion of annual capacity in each purchasing block
    purchase_candidates[, proportion_all_purchase := secured_doses / total_secured_company]
    purchase_candidates[, proportion_annual_capacity := secured_doses / annual_capacity]
  
  ## Move Novavax to July 15
    purchase_candidates[company == "Novavax" & date == "2021-07-01",
                        c("date","expected_ready","stockpile_start","effective_date") := as.Date("2021-07-15")]

  # Issues with Novavax stockpile  
    purchase_candidates[company == "Novavax", location_stockpile := 0.1 * purchase_block_pop * stockpile]
    
  # Decrease the size of the Janssen, AstraZeneca stockpile for COVAX (https://www.gavi.org/sites/default/files/covid/covax/COVAX%20Supply%20Forecast.pdf)
    purchase_candidates[company == "Janssen" & location == "COVAX", location_stockpile := 0.01 * purchase_block_pop * stockpile]
    purchase_candidates[company == "AstraZeneca" & location == "COVAX", location_stockpile := 0.1 * purchase_block_pop * stockpile]
    purchase_candidates[company == "Serum Institute of India NVX" & location == "COVAX", location_stockpile := 0.1 * purchase_block_pop * stockpile]
    
  # Decrease the size of the AstraZeneca vaccine stockpile in the EU: https://www.reuters.com/article/health-coronavirus-eu-astrazeneca/exclusive-eu-told-to-expect-no-astrazeneca-vaccines-from-us-in-near-future-sources-idUSL8N2L9384 
    purchase_candidates[company == "AstraZeneca" & location == "European Commission", location_stockpile := 0.25 * location_stockpile]
    
  
  return(purchase_candidates)
}  
##----------------------------------------------------------------------
## Make changes from CSV (gold standard)
flat_file_modifications <- function(purchase_candidates,
                                    dose_update_sheet,
                                    gavi_candidates=NULL
                                    ){
  
  dose_update_sheet[, date := as.Date(date, "%m/%d/%Y")]
  dose_update_sheet[, updated_date := as.Date(updated_date, "%m/%d/%Y")]
  
  date_change_updates <- dose_update_sheet[flag_date_change == 1]
  dose_change_updates <- dose_update_sheet[flag_date_change == 0]
  
  ## Make these changes from a CSV
  purchase_candidates[date < "2021-01-01" & quarter_end_date == "2021-04-01", stockpile := 0]
  purchase_candidates[date < "2021-01-01" & quarter_end_date == "2021-04-01", effective_date := as.Date("2021-01-01")]
  purchase_candidates[date < "2021-01-01" & quarter_end_date == "2021-04-01", effective_days := 90]
  purchase_candidates[date < "2021-01-01" & quarter_end_date == "2021-04-01", date := as.Date("2021-01-01")]
  
  
  # Update introduction dates
  purchase_candidates <- merge(purchase_candidates, date_change_updates[,c("location","company","date","updated_date","updated_effective_days")],
                               by = c("location","company","date"), all.x = T)
  purchase_candidates[!is.na(updated_date),
                      c("date","expected_ready","stockpile_start","effective_date") := updated_date]
  # Update available doses
  purchase_candidates <- merge(purchase_candidates, dose_change_updates[,c("location","company","date",
                                                                           "source_doses", "updated_stockpile", "updated_daily")],
                               by = c("location","company","date"), all.x = T)
  
  purchase_candidates[!is.na(updated_stockpile),
                      c("stockpile","daily_probable_doses") := updated_stockpile]
  purchase_candidates[!is.na(updated_daily),
                      daily_probable_doses := updated_daily]
  purchase_candidates[!is.na(updated_daily),
                      proportion_annual_capacity := 1]
  purchase_candidates[!is.na(source_doses), proportion_all_purchase := 1]
  
  # Has to happen for EU
  purchase_candidates[!is.na(updated_effective_days), effective_days := updated_effective_days]
  purchase_candidates[!is.na(updated_effective_days) & location == "European Commission", stockpile := stockpile / effective_days]
  
  # Start Johnson and Johnson on April 1 in the EU: https://www.nytimes.com/2021/03/11/world/eu-johnson-johnson-covid-vaccine.html
  purchase_candidates <- purchase_candidates[!(location == "European Commission" & company == "Janssen" & date == "2021-03-01")]
  purchase_candidates[location == "European Commission" & company == "Janssen", 
                      c("expected_ready","stockpile_start") := as.Date("2021-04-01")]
  
  if (!is.null(gavi_candidates)) {
  # No J&J in March   
  purchase_candidates <- purchase_candidates[!(location_id %in% unique(gavi_candidates$location_id) & location == "COVAX AMC" &
                                                 date == "2021-03-01" & company == "Janssen")]
  }
  
  
  return(purchase_candidates)
}
