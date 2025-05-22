# Fit and predict a uCFR model for outside of Africa
library(tidyverse)
library(terra)
library(INLA)
library(INLAutils)

#1. Setup -----
# Input data
cod_table_path <- 'FILEPATH'
incidence_reals_folder <-  'FILEPATH'
act_folder <- 'FILEPATH'
act_pattern <- 'efffectivetreatment.*\\.tif$'
pop_table_path <-  'FILEPATH'
incidence_table_path <-  'FILEPATH'

# Covariates
inc_template_raster <- 'FILEPATH'
viirs_path <- 'FILEPATH'
accessibility_path <- 'FILEPATH'
adults_path <- 'FILEPATH'
infants_path <- 'FILEPATH'
sickle_path <- 'FILEPATH'
envelope_path <- 'FILEPATH'

# Config file
config_path <-'FILEPATH'
ihme_admin_units <- 'FILEPATH'

# Year definitions
start.year <- 2000
end.year   <- 2024
year.list  <- seq(start.year, end.year)

# Number of draws
inc.realizations.per.year  <- 100

# Read in metadata
config <- read.csv(config_path)

# Read in cod data.
cod <- read.csv(cod_table_path)
cod <- cod %>%
  filter(cfr < 0.2, year <= 2015, unt_inc_correct_scale > 1e-5)

# Spde setup
outline.hull <- inla.nonconvex.hull(as.matrix(distinct(cod[, c('longitude', 'latitude')])), 
                                     convex = -0.05, 
                                     concave = -0.05,
                                     resolution = 300)

mesh <- inla.mesh.2d(cod[, c('longitude', 'latitude')], 
                     boundary = outline.hull,
                     max.edge = c(2,60), 
                     cutoff = 2, 
                     min.angle = 21, 
                     offset = c(7, 30))

Aest <- inla.spde.make.A(mesh = mesh, loc = as.matrix(cod[, c('longitude', 'latitude')]))

# Define penalised complexity priors for random field. 
spde <- inla.spde2.pcmatern(mesh = mesh, alpha = 2, prior.range = c(20, 0.01), prior.sigma = c(0.4, 0.01))

# Create ihme super region ids
cod <- config %>% 
  dplyr::select(IHME_Super_Region_ID, IHME_location_id) %>% 
  left_join(cod, ., by = c('location_id' = 'IHME_location_id')) %>% 
  mutate(IHME_Super_Region_ID = ifelse(IHME_Super_Region_ID == 64, 31, IHME_Super_Region_ID)) %>% 
  mutate(region_i = as.numeric(factor(IHME_Super_Region_ID))) %>%
  mutate(region_fac = factor(IHME_Super_Region_ID))

# Get data ready for INLA model
covs <- cod %>% 
  mutate(location_factor = factor(location_id)) %>% 
  dplyr::select(accessibility, viirs, adults, infants, year, location_factor, 
                sickle_As, region_i, log_mortality_rate, region_fac)

stk.env <- inla.stack(tag = 'estimation', 
                      data = list(logit_cfr = cod$logit_cfr),
                      A = list(Aest, 1),  
                      effects = list(space = seq_len(spde$n.spde), 
                                     cbind(b0 = 1, covs)))


# Define priors
hyper.rw2 <- list(prec = list(prior="pc.prec", param = c(0.03, 0.01)))
hyper.iid <- list(prec = list(prior="pc.prec", param = c(0.8, 0.01)))
fixed <- list(mean = list(viirs = -1.5, accessibility = 0.6, adults = -2, 
                          infants = 2, log_mortality = 4, incidence = 0, sickle_As = -6, 
                          region_fac166 = -6, region_fac4 = -4, region_fac31 = -4, region_fac103 = -4, region_fac158 = -4),
              prec = list(viirs = 1, accessibility = 1, adults = 1.5, 
                          infants = 1.5, log_mortality = 1, incidence = 0.001, sickle_As = 0.7, 
                          region_fac166 = 1, region_fac4 = 1, region_fac31 = 1, region_fac103 = 1, region_fac158 = 1))

#2. Fit Model -----
form <- logit_cfr ~ 0 + region_fac + 
  accessibility + adults + infants + log_mortality_rate + sickle_As +
  f(year, model="rw2", hyper = hyper.rw2, scale.model = TRUE, replicate = region_i) + 
  f(location_factor, model = 'iid', hyper = hyper.iid)


for(i in seq_len(5)){
  
  if(!exists('best.model.cfr')){
    message(paste0('Model fittings attempt:', i))
    try(
      best.model.cfr <- inla(form,
                             data = inla.stack.data(stk.env), 
                             control.predictor = list(A = inla.stack.A(stk.env)), 
                             control.compute = list(waic = TRUE, config = TRUE),
                             control.fixed = fixed),
      silent = FALSE)
  }
}



#3. Predict realisations -----
# Realisation parameter draws
parameters.samples <- inla.posterior.sample(inc.realizations.per.year, best.model.cfr, num.thread = 50)
cc <- best.model.cfr$misc$configs$contents
nyears <- best.model.cfr$summary.random$year$ID %>% unique %>% length

# Read in covariates for prediction
template <- rast(inc_template_raster) 

access <- accessibility_path %>% rast %>% crop(template) %>% mask(template)
access <- log1p(access)

sickle <- sickle_path %>% rast %>% crop(template) %>% mask(template)
As <- sickle * (1 - sickle)

adults <- list.files(adults_path, pattern = '.*adults_.*.tif$', full.names = TRUE) %>% 
  rast %>%
  crop(template) %>% 
  mask(template)


infants <-  list.files(infants_path, pattern = '.*infants_.*.tif$', full.names = TRUE) %>% 
  rast %>%
  crop(template) %>% 
  mask(template)


log_mortality <-  list.files(envelope_path, pattern = '.*.tif$', full.names = TRUE) %>% 
  rast %>%
  crop(template) %>% 
  mask(template) %>%
  log
names(log_mortality) <- list.files(envelope_path, pattern = '.*.tif$') %>% gsub('\\.tif$', '', .)

eff_treat <-  list.files(act_folder, pattern = act_pattern, full.names = TRUE) %>% 
  rast() %>% 
  crop(template) %>% 
  mask(template) 

no_eff_treat <- 1 - eff_treat

# Setup file vectors & indices for dynamics covariates
incidence_files <- list.files(incidence_reals_folder)

fake_year_list <- year.list 
fake_year_list[fake_year_list>(end.year-1)] <- (end.year-1)

adults_ii <- sapply(fake_year_list, function(x) grep(x, names(adults)))
infants_ii <- sapply(fake_year_list, function(x) grep(x, names(infants)))
mortality_ii <- sapply(fake_year_list, function(x) grep(x, names(log_mortality)))

eff_treatment_ii <- as.numeric(sapply(year.list, function(x) grep(x, names(eff_treat))))
eff_treatment_ii[is.na(eff_treatment_ii)] <- max(as.numeric(eff_treatment_ii), na.rm = TRUE)

# Make indices for random effects
year_id <- cc$tag == 'year'
space_id <- cc$tag == 'space'
intercept_id <- grep('region_fac', cc$tag)
intercept_region_id <- grep('region_fac', cc$tag, value = TRUE) %>% gsub('region_fac', '', .) %>% as.numeric

modelled_year_list <- seq(min(unique(cod$year)), max(unique(cod$year)))

# Extract coordinates for each pixel to be predicted and the matching covariate values
prediction.points <- 
  as.data.frame(template, xy=TRUE)  %>% 
  rename(lon = x, lat = y) %>% 
  select(lon, lat)

# Admin units (location ids)
admin                             <- rast(ihme_admin_units)
prediction.points$location_id     <- terra::extract(admin,cbind(prediction.points$lon,prediction.points$lat))[,1]

# Temporally static covs
prediction.points$sickle <- terra::extract(As, cbind(prediction.points$lon,prediction.points$lat))[,1]
prediction.points$access <- terra::extract(access, cbind(prediction.points$lon,prediction.points$lat))[,1]

# Add super region ID 
prediction.points <- left_join(prediction.points,
                               config[c('IHME_location_id', 'IHME_Super_Region_ID')],
                               by = c('location_id' = 'IHME_location_id'))


# Limit to surveillance locations
prediction.points <- filter(prediction.points, 
                            location_id %in% config$IHME_location_id[config$MAP_Pf_Model_Method == 'Surveillance'])

# Get the intercept for each super region
intercepttable <- data.frame(intercept = p$latent[cc$start[intercept_id]], 
                             IHME_Super_Region_ID = intercept_region_id)


prediction.points <- left_join(prediction.points, intercepttable)

# For each year predict the uCFR for n realisations, calculate the number of deaths and aggregate to admin units
for(i in 1:length(year.list)){
  year <- year.list[i]
  print(year)
  # Match prediction year to the closest modelled year
  if(year %in% modelled_year_list){
    year_id_temporal <- year
  } else if(year > max(modelled_year_list)){
    year_id_temporal <- max(modelled_year_list)
  } else if( year < min(modelled_year_list)){
    year_id_temporal <- min(modelled_year_list)
  } else if(year-1 %in% modelled_year_list){
    year_id_temporal <-  year-1
  } else if(year+1 %in% modelled_year_list){
    year_id_temporal <-  year+1
  }
  
  # Year id for fixed effects.
  year_ii_covs <- (year == year.list)
  
  # Get fixed effects params
  fixed_covs_ii <- cc$tag %in% c('b0', 'accessibility', 'adults', 'infants', 'log_mortality_rate', 'sickle_As')
  intercept_ii <- grepl('region_fac', cc$tag)
  fixed_ii <- fixed_covs_ii | intercept_ii
  
  fxd <- as.list(best.model.cfr$summary.fixed$mean)
  names(fxd) <- cc$tag[fixed_ii]
  
  # Extract year specific covariate values
  prediction.points$adults <- terra::extract(adults[[adults_ii[i]]], cbind(prediction.points$lon,prediction.points$lat))[,1]
  prediction.points$infants <- terra::extract(infants[[infants_ii[i]]], cbind(prediction.points$lon,prediction.points$lat))[,1]
  prediction.points$log_mortality <- terra::extract(log_mortality[[mortality_ii[i]]], cbind(prediction.points$lon,prediction.points$lat))[,1]
  prediction.points$no_eff_treat<- terra::extract(no_eff_treat[[eff_treatment_ii[i]]], cbind(prediction.points$lon,prediction.points$lat))[,1]
  
  # For each realisation
  for(r in 1:inc.realizations.per.year){
    print(r)
    p <- parameters.samples[[r]]
    
    # Get the temporal/regional interaction effect
    subtable <- data.frame(value = p$latent[seq(cc$start[year_id], cc$start[year_id] + cc$length[year_id] - 1)], 
                           IHME_Super_Region_ID = rep(levels(factor(cod$IHME_Super_Region_ID)), each = nyears),
                           year_id = best.model.cfr$summary.random$year$ID)
    
    subtable_thisyear <- subtable %>% filter(year_id == year_id_temporal) %>% dplyr::select(IHME_Super_Region_ID, value) %>% 
      mutate(IHME_Super_Region_ID = as.numeric(IHME_Super_Region_ID))
    
    prediction.points <- left_join(prediction.points, subtable_thisyear)

    # Read in incidence realisation
    inc_r_path <- paste0(incidence_reals_folder, grep(paste0(year, '\\.'), incidence_files, value = TRUE)[r])
    inc_r <- rast(inc_r_path) %>% crop(template) %>% mask(template)
    
    prediction.points$incidence <- terra::extract(inc_r, cbind(prediction.points$lon,prediction.points$lat))[,1]
    
    # Predict logit uCFR
    prediction.points$logit_cfr_tmp <- 
      prediction.points$intercept +
      fxd$accessibility * prediction.points$access + 
      fxd$adults * prediction.points$adults + 
      fxd$infants * prediction.points$infants+
      fxd$log_mortality_rate * prediction.points$log_mortality +
      fxd$sickle_As * prediction.points$sickle + 
      prediction.points$value
    
    
    # Inverse logit transform
    prediction.points$cfr_tmp <-  1 / (1 + exp(-1 * prediction.points$logit_cfr_tmp))
    
    # Calc deaths
    prediction.points$deaths <- prediction.points$cfr_tmp * prediction.points$incidence * prediction.points$no_eff_treat
    
    # Clean up (join will be repeated next realisation)
    prediction.points$value <-  NULL
    
    # Aggregate pixels up to the admin unit
    if(!exists('admin.death.draws')){
      admin.death.draws <- group_by(prediction.points, location_id) %>% summarise(deaths = sum(deaths, na.rm=T)) %>% rename(draw_1 = deaths)
    } else{
      admin.death.draws[, paste0('draw_', r)] <- 
        group_by(prediction.points, location_id) %>% summarise(deaths = sum(deaths, na.rm=T))%>% select(deaths)
    }
  }
  
  # Combine into one data frame with all years
  year.death.draws <- admin.death.draws %>% 
    mutate(year = year) %>% 
    relocate(year, .after = location_id)
  
  if(i==1){
    all_years_draws <- year.death.draws
  } else{
    all_years_draws <- bind_rows(all_years_draws, year.death.draws)
  }
  
}

# Final cleaning
config <- read_csv(config_path) %>% 
  filter(MAP_Pf_Model_Method == 'Surveillance') %>% 
  select(location_id = IHME_location_id, location_name = IHME_Location_Name_Short, ISO3)

deaths_draws <- inner_join(config, all_years_draws) %>% 
  mutate(sex_id = 3, sex = 'both', age_group_id = 'All Ages') %>% 
  relocate(sex_id, .before = draw_1) %>% 
  relocate(sex, .before = draw_1) %>% 
  relocate(age_group_id, .before = draw_1)

# 4. Project back to 1980 ----
# Load in population and incidence estimates
pop <- read.csv(pop_table_path)
load(incidence_table_path)

combined.table <- filter(combined.table, ihme_age_group_id == 'All_Ages',
                         ihme_location_id %in% deaths_draws$location_id) %>% 
  arrange(ihme_location_id, year)

combined.table <- combined.table[, c(1:(inc.realizations.per.year+8))] 

# Convert incidence rate to counts
pop <- filter(pop, 
              ihme_id %in% deaths_draws$location_id,
              age_bin == 'All_Ages') %>% 
  arrange(ihme_id, year)

table(pop$ihme_id == combined.table$ihme_location_id)
table(pop$year == combined.table$year)

combined.table[, grepl('draw', names(combined.table))] <- combined.table[, grepl('draw', names(combined.table))] * pop$pop_at_risk_pf

# Sort the draws so we dont end up with extreme CFRs
data.only                                           <- combined.table[,grep('draw_',names(combined.table))]
sorted.block                                        <- apply(data.only, 1, sort)
combined.table[,grep('draw_',names(combined.table))] <- t(sorted.block)

data.only                                          <- deaths_draws[,grep('draw_',names(deaths_draws))]
sorted.block                                       <- apply(data.only, 1, sort)
deaths_draws[,grep('draw_',names(deaths_draws))]   <- t(sorted.block)

rm(data.only, sorted.block)

combined.table <- arrange(combined.table, ihme_location_id, year)
deaths_draws <- arrange(deaths_draws, location_id, year)

# Calculate the CFR for 2000 for each location
inc_2000 <- filter(combined.table, year == start.year)
deaths_2000 <- filter(deaths_draws, year == start.year) %>% 
  arrange(location_id)

cfr_2000 <- inc_2000
cfr_2000[, grepl('draw', names(cfr_2000))] <- 
  deaths_2000[, grepl('draw', names(deaths_2000))]/inc_2000[, grepl('draw', names(inc_2000))]
  
cfr_2000 <- mutate(cfr_2000,
                   across(starts_with('draw'), \(x) ifelse(is.na(x), 0, x)),
                   across(starts_with('draw'), \(x) ifelse(x>.2, .2, x)))

# Duplicate this for the number of years requiring imputation
cfr_block <- do.call("rbind", 
                     replicate((start.year-total.start.year), cfr_2000, simplify = FALSE)) 

cfr_block <- arrange(cfr_block, ihme_location_id, year)
old_deaths <- combined.table %>% filter(year <start.year)
table(cfr_block$ihme_location_id == old_deaths$ihme_location_id)

# Apply the CFR to old incidence counts
old_deaths[,grepl('draw', names(old_deaths))] <- old_deaths[,grepl('draw', names(old_deaths))] * cfr_block[,grepl('draw', names(cfr_block))]

# Clean up
old_deaths <-  mutate(old_deaths,
                      sex_id = 3, sex = 'both') %>% 
  select(location_id = ihme_location_id, 
                      location_name = admin_unit_name,
                      ISO3 = iso3,
                      year, sex_id, sex, age_group_id =ihme_age_group_id, starts_with('draw')) 

names(deaths_draws)[(grepl('draw', names(deaths_draws)))] <- paste0('draw_', 0:99)
deaths_draws <- rbind(old_deaths, deaths_draws)

# Save final draw estimates
save(deaths_draws, file = 'FILENAME')
