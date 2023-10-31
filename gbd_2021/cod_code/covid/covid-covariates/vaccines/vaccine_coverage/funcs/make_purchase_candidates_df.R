
make_purchase_candidates <- function(data_root, output_root, total_population) {
  
## Read in files that have information from Linksbridge
  eu_locs <- gbd_data$get_european_union_hierarchy()
  au_locs <- gbd_data$get_african_union_hierarchy()
  
  stage_probability <- model_inputs_data$load_purchase_candidates_data(data_root, "stage_probability")
  manu_capacity <- model_inputs_data$load_purchase_candidates_data(data_root, "manufacturing_capacity")
  secured_doses <- model_inputs_data$load_purchase_candidates_data(data_root, "secured_doses")

  
  secured_doses_manufacturer <- model_inputs_data$load_purchase_candidates_data(data_root, "secured_doses_manufacturer")
  secured_doses_manufacturer <- secured_doses_manufacturer[,c("location","location_id","manufacturer","secured_doses","company","optioned_doses","source")]
  secured_doses_manufacturer <- secured_doses_manufacturer[secured_doses > 0]

# Try using Linksbridge direct download for quarter capacity
  quarter_capacity <- model_inputs_data$load_purchase_candidates_data(data_root, "quarter_capacity")
  candidate_stages <- model_inputs_data$load_purchase_candidates_data(data_root, "candidate_stages")
  
# Read this from versioned folder for reproducibility
  expected_effectiveness <- vaccine_data$load_vaccine_efficacy(output_root)
  
  approved_vaccines <- expected_effectiveness$merge_name
  approved_vaccines <- approved_vaccines[!(approved_vaccines %in% c("mRNA Vaccine","Other"))]
  
  expected_effectiveness[, c("company", "candidate","vaccine_type") := NULL]

# Clean up a few things to merge them
  candidate_stages[, merge_name := ifelse(vaccine_developer %in% c("Moderna","Janssen","Novavax","CNBG Beijing", "CNBG Wuhan"), 
                                          vaccine_developer,
                                          ifelse(candidate %in% c("AZD1222","CoronaVac","BNT-162","Sputnik V","Covaxin"), candidate, 
                                                 ifelse(vaccine_developer %like% "Tianjin", "Tianjin CanSino", vaccine_type)))]
  candidate_stages <- merge(candidate_stages, expected_effectiveness, by="merge_name")

##-------------------------------------------------------------------------
# What should we do about Russia? 
# For now, assume that non-secured doses of Sputnik V (Gamelaya Research Institute are secured by Russia)
  gamaleya <- manu_capacity[company == "Gamaleya Research Institute"]
  sputnik_secured <- secured_doses_manufacturer[company == "Gamaleya Research Institute", lapply(.SD, function(x) sum(x, na.rm=T)), .SDcols = "secured_doses"]
  
  #### Testing this assumption here: 
  # Russia to increase Gamaleya capacity (260*3 million doses at least)
  # https://www.marketwatch.com/story/russia-looks-to-china-to-help-produce-its-sputnik-v-covid-19-vaccine-01620023486
  russian_secured <- gamaleya$capacity*2 - sputnik_secured$secured_doses
  ####
  
  secured_doses_manufacturer <- rbind(secured_doses_manufacturer,
                                      data.table(location = "Russian Federation", 
                                                 location_id = 62,
                                                 manufacturer = "Gamaleya Research Institute", 
                                                 secured_doses = russian_secured,
                                                 company = "Gamaleya Research Institute", 
                                                 optioned_doses = NA,
                                                 source = "Assumption"))

# Set missing optioned_doses to 0
  secured_doses_manufacturer[is.na(optioned_doses), optioned_doses := 0]

## Quick gg for the bar chart.
  secured_doses_manufacturer[, total := sum(secured_doses), by="location"]
  secured_doses_manufacturer[, plot_loc := reorder(location, -total)]
  
  date_map <- data.table(quarter = unique(quarter_capacity$quarter), 
                         date = as.Date(c("2021-10-01","2021-07-01","2021-04-01","2021-01-01", "2020-12-15")),
                         quarter_end_date = as.Date(c("2022-01-01","2021-10-01","2021-07-01","2021-04-01","2020-12-31")))
  
  quarter_capacity <- merge(quarter_capacity, date_map, by="quarter")
  quarter_capacity[, quarter_capacity := ifelse(is.na(quarter_capacity), 0, quarter_capacity)]
  quarter_capacity <- quarter_capacity[order(date)]
  quarter_capacity[, cumulative := cumsum(quarter_capacity), by="quarter"]

# Add Dr. Reddy's, assume they can provide 100 million doses over 4 quarters;
  dr_quarter <- data.table(quarter = unique(quarter_capacity$quarter), manufacturer = "Dr. Reddy's",
                           quarter_capacity = c(0, 25000000, 25000000, 25000000, 25000000),
                           date = unique(quarter_capacity$date),
                           quarter_end_date = unique(quarter_capacity$quarter_end_date),
                           cumulative = NA)
  quarter_capacity <- rbind(quarter_capacity, dr_quarter)
  
  quarter_capacity[, quarter_days := quarter_end_date - date]

# Create a cumulative annual capacity by manufacturer to compare with stated overall annual capacity
  quarter_capacity[date != "2020-10-01", annual_capacity_from_quarters := sum(quarter_capacity), by="manufacturer"]

## Do some work to fill missing in candidate_stages
  candidate_stages[, candidate := ifelse(vaccine_developer=="Sanofi", "mRNA Sanofi", as.character(candidate))]
  candidate_stages[, candidate := ifelse(vaccine_developer=="GSK & Sanofi", "Sanofi/GSK Vaccine", as.character(candidate))]
  candidate_stages[, candidate := ifelse(vaccine_developer=="CNBG Wuhan", "Inactivated Vaccine - CNBG Wuhan", as.character(candidate))]
  candidate_stages[, candidate := ifelse(vaccine_developer=="Bristol University & VABIOTECH", "Protein Subunit Vaccine - Vabiotech", as.character(candidate))]
  candidate_stages[, candidate := ifelse(vaccine_developer %like% "Kazakh", "Protein Subunit Vaccine - Kazakh", as.character(candidate))]
  candidate_stages[, candidate := ifelse(vaccine_developer %like% "IVAC,", "Inactivated Vaccine - IVAC", as.character(candidate))]
  candidate_stages[, candidate := ifelse(vaccine_developer %like% "UMN Pharma", "Protein Subunit Vaccine - UMN", as.character(candidate))]
  candidate_stages[, candidate := ifelse(vaccine_developer == "Tianjin CanSino", "Ad5-nCoV", as.character(candidate))]
  candidate_stages[, candidate := ifelse(vaccine_developer == "Takeda", "NVX CoV-2373", as.character(candidate))]

# Change candidates to help merge
  setnames(manu_capacity, c("vaccine_type"), c("candidate"))
  manu_capacity[, candidate := ifelse(company == "Sanofi/Translate Bio", "mRNA Sanofi", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "Tianjin CanSino", "Ad5-nCoV", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "Petrovax", "Ad5-nCoV", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "Takeda", "NVX CoV-2373", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "CNBG Beijing", "BBIBP-CorV", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "CNBG Wuhan", "Inactivated Vaccine - CNBG Wuhan", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "CSL", "UQ-CSL V451", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "Inovio", "INO-4800", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "Sinovac", "CoronaVac", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "VABIOTECH", "Protein Subunit Vaccine - Vabiotech", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "Kazakhstan Institute", "Protein Subunit Vaccine - Kazakh", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "IVAC", "Inactivated Vaccine - IVAC", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "UMN Pharma", "Protein Subunit Vaccine - UMN", as.character(candidate))]
  manu_capacity[, candidate := ifelse(company == "Chula Vaccine Research Center", "ChulaCov19", as.character(candidate))]

# Add BioKantai
  biokantai_dt <- data.table(company = "BioKantai", 
                             capacity = sum(quarter_capacity[manufacturer == "BioKantai" & date != "2021-01-01"]$quarter_capacity),
                             candidate = "AZD1222")
  manu_capacity <- rbind(manu_capacity, biokantai_dt)

# Add capacity for Takeda (https://www.reuters.com/article/us-takeda-moderna-vaccine/japans-takeda-to-import-50-mln-doses-of-modernas-covid-19-vaccine-raises-profit-forecast-idUSKBN27E0OO)
  add_moderna_takeda <- data.table(company = "Takeda", capacity = 50000000, candidate = "mRNA-1273")
  manu_capacity <- rbind(manu_capacity, add_moderna_takeda)
  
# Add Bharat Biotech 
  add_bharat <- data.table(company = "Bharat Biotech", capacity = 1125000000, candidate = "Covaxin")
  manu_capacity <- rbind(manu_capacity, add_bharat)

# Change company name to help merge
  manu_capacity[, company := ifelse(company == "Sanofi/GSK", "GSK & Sanofi", as.character(company))]
  
  manu_capacity[, possible_doses := as.numeric(capacity)]
  capacity_by_candidate <-  manu_capacity[, lapply(.SD, function(x) sum(x)), by="candidate", .SDcols = "possible_doses"]

##------------------------------------------------------------------------
## First step is to estimate likelihood that a candidate gets licensed
  candidate_stages[, trial_stage := ifelse(discovery == 1, "preclinical",
                                           ifelse(preclinical == 1, "preclinical",
                                                  ifelse(phase_I == 1, "phase1",
                                                         ifelse(phase_I_II == 1, "phase2",
                                                                ifelse(phase_II == 1, "phase2", 
                                                                       ifelse(phase_III == 1, "phase3", "limited_use"))))))]
  candidate_stages[, approved := ifelse(merge_name %in% approved_vaccines, 1, 0)]

# Merge all candidates with known capacity (by candidate)
  candidates <- candidate_stages[, c("candidate","vaccine_developer","trial_stage","efficacy","variant_efficacy",
                                     "prop_protected_only","prop_protected_not_infectious","wastage","approved")]
  candidates <- merge(candidates, stage_probability, by = "trial_stage")
  candidates <- merge(candidates, manu_capacity, by="candidate", all = T)
  
  candidates[, date_start := Sys.Date()]
  candidates[, expected_ready := as.Date(timing_expected_start, "%m/%d/%Y")]
  
  candidates[, earliest_ready := expected_ready - 90]
  candidates[, latest_ready := expected_ready + 90]
  candidates[, potential_doses_2021 := possible_doses * as.numeric((as.Date("2021-12-31") - expected_ready) / 365)] # Find proportion of 2021 remaining, multiply by doses

##--------------------------------------------------------------------------------------
# Manual updates to vaccine candidates
  candidates[, probability := ifelse(approved == 1, 1, probability)]

# Update phase 3 vaccines
  candidates[trial_stage == "phase3", expected_ready := as.Date("2021-09-01")] # This keeps getting pushed back...

# Update Bharat
  candidates[company == "Bharat Biotech", expected_ready := as.Date("2021-04-01")]
  
# Update Janssen
  candidates[company == "Janssen", expected_ready := as.Date("2021-03-01")]
  candidates[company == "Janssen", probability := 1]

# Update the Pfizer vaccine
  candidates[candidate == "BNT-162", probability := 1]
  candidates[candidate == "BNT-162", expected_ready := as.Date("2020-12-15")]

# Update the Moderna vaccine
  candidates[company == "Moderna", probability := 1]
  candidates[company == "Moderna", expected_ready := as.Date("2020-12-22")]
  
# Update the Sinovac vaccine
  candidates[company == "Sinovac", probability := 1]
  candidates[company == "Sinovac", expected_ready := as.Date("2021-02-01")]

# Update the AstraZeneca vaccine
  candidates[candidate == "AZD1222", probability := 1]
  candidates[candidate == "AZD1222", expected_ready := as.Date("2021-02-01")]

# Update the CSL vaccine (sad) [https://www.theage.com.au/politics/federal/australian-covid-vaccine-terminated-due-to-hiv-false-positives-20201210-p56mju.html]
  candidates[company == "CSL", probability := 0]

# Update the Merck candidate vaccines (sad again) https://www.nbcnews.com/health/health-news/merck-discontinues-two-covid-19-vaccine-candidates-n1255503
  candidates[vaccine_developer %like% "Merck", probability := 0]

# Update the Sanofi/GSK Vaccine https://www.reuters.com/article/health-coronavirus-vaccines-sanofi-idUSKBN28L0II 
  candidates[company == "GSK & Sanofi", expected_ready := as.Date("2021-12-01")]

# Change the name of Serum Institute of India to have separate vaccines by developer
  candidates[, company := ifelse(company == "Serum Institute of India" & candidate == "AZD1222", "Serum Institute of India AZD",
                               ifelse(company == "Serum Institute of India" & candidate == "NVX CoV-2373", "Serum Institute of India NVX",
                                      as.character(company)))]

## Purpose of all that is to get this value. 
  candidates[, probable_doses := probability * possible_doses]

##-----------------------------------------------------------------------------
  candidates <- candidates[!is.na(probable_doses)]
  
  setdiff(unique(candidates$company), unique(manu_capacity$company))
  setdiff(unique(manu_capacity$company), unique(candidates$company))

## Merge candidates with known manufacturer with manufacturing capacity by quarter
# Serum Institute of India is the only place I can see with multiple vaccine candidates (2), so divide capacity by two for merging
  quarter_capacity[, quarter_capacity := ifelse(manufacturer == "Serum Institute of India", quarter_capacity / 2, quarter_capacity)]
  quarter_capacity[, cumulative := ifelse(manufacturer == "Serum Institute of India", cumulative / 2, cumulative)]
  srm_india1 <- quarter_capacity[manufacturer == "Serum Institute of India"]
  srm_india1$manufacturer <- "Serum Institute of India AZD"
  srm_india2 <- quarter_capacity[manufacturer == "Serum Institute of India"]
  srm_india2$manufacturer <- "Serum Institute of India NVX"
  quarter_capacity <- rbind(quarter_capacity, srm_india1, srm_india2)
  quarter_capacity[, date := as.Date(date)]
  
  can_do <- merge(candidates, quarter_capacity, by.x = "company", by.y  = "manufacturer", all = T)
  
  message(paste0("Unmatched by quarterly capacity: ", paste(unique(can_do[is.na(quarter_capacity), company]), collapse = ", ")))
  message(paste0("Unmatched by manufacturer/candidate: ", paste(unique(can_do[is.na(vaccine_developer), company]), collapse = ", ")))
  
  candidates <- merge(candidates, quarter_capacity, by.x = "company", by.y = "manufacturer")
  setnames(candidates, "capacity", "annual_capacity")
  
##------------------------------------------------------------------------------------------------   
## Great! Now by manufacturer and candidate, we have probability and number of doses by quarter.
##------------------------------------------------------------------------------------------------  
  ## Now, split the global into the counts pre-purchased where known, split the rest.
  # Use the one with manufacturer
  secured_doses <- copy(secured_doses_manufacturer)
  
  # What the REDACTED is the European Commission?
  # It is the Executive Branch of the European Union
  secured_doses[, location_id := ifelse(location == "European Commission", -5,
                                        ifelse(location == "African Union", -32,
                                               ifelse(location == "COVAX", -2,
                                                      ifelse(location == "COVAX AMC", -8, 
                                                             ifelse(location == "Hong Kong SAR and Macao", -3, location_id)))))]
  secured_doses[, secured_id := location_id]
  secured_doses[, location_id := NULL]
  secured_doses[, total_secured_company := sum(secured_doses), by = c("company")]
  
  # What are COVAX countries? https://www.gavi.org/sites/default/files/covid/pr/COVAX_CA_COIP_List_COVAX_PR_29-10.pdf
  covax_locs <- model_inputs_data$load_covax_locations(data_root)
  gavi_locs <- covax_locs[!is.na(gavi_eligible)] # Remove Gavi locations (different block)
  covax_locs <- covax_locs[is.na(gavi_eligible)] # Remove Gavi locations (different block)
  
  # Create a dummy hierarchy
  location_hierarchy <- gbd_data$get_covid_modeling_hierarchy()
  
  covid_modeling_hierarchy <- location_hierarchy[level >= 3, c("location_id","location_name","parent_id","region_name","level","most_detailed", "path_to_top_parent")]
  
  covid_modeling_hierarchy$in_china <- children_of_parents(6, covid_modeling_hierarchy, include_parent = T)
  covid_modeling_hierarchy <- covid_modeling_hierarchy[!(in_china & most_detailed==0),]
  covid_modeling_hierarchy$in_india <- children_of_parents(163, covid_modeling_hierarchy, include_parent = T)
  covid_modeling_hierarchy <- covid_modeling_hierarchy[!(in_india & most_detailed==0),]
  covid_modeling_hierarchy[which(in_china), parent_id := 6] # assign ALL children of china the parent id of 6 to include in purchasing block
  covid_modeling_hierarchy[which(in_india), parent_id := 163] # same for India 
  covid_modeling_hierarchy <- covid_modeling_hierarchy[,!c("path_to_top_parent", "in_china", "in_india")]
  
  
  covid_modeling_hierarchy <- covid_modeling_hierarchy[!(level == 3 & most_detailed == 0)]
  covid_modeling_hierarchy <- rbind(covid_modeling_hierarchy, location_hierarchy[location_id %in% c(11,71), #, 81, 86, 92
                                            c("location_id","location_name","parent_id","region_name","level","most_detailed")]) # Add back on Australia, Indonesia
  covid_modeling_hierarchy <- merge(covid_modeling_hierarchy, total_population[, c("population","location_id")], by="location_id")
  
  
# Purchase block of EU
  eu_block <- covid_modeling_hierarchy[location_id %in% eu_locs$location_id]
  # Add subnationals, remove parent
  eu_block <- rbind(eu_block, covid_modeling_hierarchy[parent_id %in% eu_locs$location_id]) # Add subnationals in Germany, Italy, Spain
  eu_block <- rbind(eu_block, covid_modeling_hierarchy[location_id == 349]) # Add Greenland
  eu_block[, purchase_block_pop := population / sum(population)]
  eu_block[, secured_id := -5]
  eu_block <- merge(eu_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of African Union
  au_block <- covid_modeling_hierarchy[location_id %in% au_locs$location_id]
  au_block[, purchase_block_pop := population / sum(population)]
  au_block[, secured_id := -32]
  au_block <- merge(au_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of COVAX (Committed locations)
  # Should be split to locations such that up to 20% receive vaccine before anywhere else. 
  covax_block <- covid_modeling_hierarchy[location_id %in% covax_locs$location_id] # Most detailed nationals
  covax_subs <- covid_modeling_hierarchy[parent_id %in% covax_locs$location_id] # Subnationals among COVAX parents (Brazil, Canada, China, Germany, Italy, Mexico, Spain, UK)
  covax_block <- rbind(covax_block, covax_subs)
  covax_block[, purchase_block_pop := population / sum(population)]
  covax_block[, secured_id := -2]
  covax_block <- merge(covax_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of Gavi locations
  gavi_block <- covid_modeling_hierarchy[location_id %in% gavi_locs$location_id] # Most detailed nationals
  gavi_subs <- covid_modeling_hierarchy[parent_id %in% gavi_locs$location_id] # Subnationals among gavi parents (Brazil, Canada, China, Germany, Italy, Mexico, Spain, UK)
  gavi_block <- rbind(gavi_block, gavi_subs)
  gavi_block[, purchase_block_pop := population / sum(population)]
  gavi_block[, secured_id := -8]
  gavi_block <- merge(gavi_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of Latin America
  lam_block <- covid_modeling_hierarchy[parent_id %in% c(104, 120, 124, 134, 130)] # Per Linksbridge, this DOES NOT include Brazil (135)
  lam_block[, purchase_block_pop := population / sum(population)]
  lam_block[, secured_id := 103]
  lam_block <- merge(lam_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of Brazil
  bra_block <- covid_modeling_hierarchy[parent_id == 135] #
  bra_block[, purchase_block_pop := population / sum(population)]
  bra_block[, secured_id := 135]
  bra_block <- merge(bra_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of Mexico
  mex_block <- covid_modeling_hierarchy[parent_id == 130] 
  mex_block[, purchase_block_pop := population / sum(population)]
  mex_block[, secured_id := 130]
  mex_block <- merge(mex_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of USA (should small island countries be added?)
  usa_block <- covid_modeling_hierarchy[parent_id == 102 | parent_id == 570]
  usa_block <- usa_block[location_id != 570]
  usa_block <- rbind(usa_block, covid_modeling_hierarchy[location_id %in% c(24, 25, 298, 380, 385, 422, 351)])
  usa_block[, purchase_block_pop := population / sum(population)]
  usa_block[, secured_id := 102]
  usa_block <- merge(usa_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of Canada
  can_block <- covid_modeling_hierarchy[parent_id == 101]
  can_block[, purchase_block_pop := population / sum(population)]
  can_block[, secured_id := 101]
  can_block <- merge(can_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of United Kingdom  
  uk_block <- covid_modeling_hierarchy[parent_id == 95]
  uk_block[, purchase_block_pop := population / sum(population)]
  uk_block[, secured_id := 95]
  uk_block <- merge(uk_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of Germany  
  ger_block <- covid_modeling_hierarchy[parent_id == 81]
  ger_block[, purchase_block_pop := population / sum(population)]
  ger_block[, secured_id := 81]
  ger_block <- merge(ger_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of India  
  ind_block <- covid_modeling_hierarchy[parent_id == 163]
  ind_block[, purchase_block_pop := population / sum(population)]
  ind_block[, secured_id := 163]
  ind_block <- merge(ind_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of Pakistan  
  pak_block <- covid_modeling_hierarchy[parent_id == 165]
  pak_block[, purchase_block_pop := population / sum(population)]
  pak_block[, secured_id := 165]
  pak_block <- merge(pak_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of China  
  chn_block <- covid_modeling_hierarchy[parent_id == 6]
  chn_block[, purchase_block_pop := population / sum(population)]
  chn_block[, secured_id := 6]
  chn_block <- merge(chn_block, secured_doses, by="secured_id", allow.cartesian = T)
# Purchase block of Hong Kong/Macao 
  hkm_block <- rbind(covid_modeling_hierarchy[location_name %like% "Hong Kong"], covid_modeling_hierarchy[location_name %like% "Macao"])
  hkm_block[, purchase_block_pop := population / sum(population)]
  hkm_block[, secured_id := -3]
  hkm_block <- merge(hkm_block, secured_doses, by="secured_id", allow.cartesian = T)  
# Country-specific purchases
  country_block <- merge(covid_modeling_hierarchy, secured_doses, by.x="location_id", by.y="secured_id", allow.cartesian = T)
  country_block[, purchase_block_pop := 1]
  country_block[, secured_id := location_id]
  
# Collapsed
  purchase_blocks <- rbind(eu_block, au_block, covax_block, 
                           gavi_block, 
                           lam_block, 
                           bra_block, mex_block,
                           usa_block, can_block, uk_block, 
                           ger_block, ind_block, pak_block,
                           chn_block, hkm_block, country_block)
  
  
# Merge with candidates
  purchase_candidates <- merge(purchase_blocks, candidates, by = "company", all = T, allow.cartesian = T)
# That many:many merge makes me nervous...
  purchase_candidates <- unique(purchase_candidates)
  
  purchase_candidates <- purchase_candidates[, c(
    "date", "location_id", "company", "manufacturer", "expected_ready", "purchase_block_pop",
    "location", "candidate", "trial_stage", "annual_capacity", "quarter", "quarter_capacity",
    "quarter_days", "prop_protected_not_infectious", "possible_doses", "probable_doses",
    "efficacy", "variant_efficacy", "wastage", "probability", "secured_doses", 
    "total_secured_company", "quarter_end_date")]
  return(list(
    purchase_candidates = purchase_candidates, 
    covid_modeling_hierarchy = covid_modeling_hierarchy, 
    eu_block = eu_block))
  
}
