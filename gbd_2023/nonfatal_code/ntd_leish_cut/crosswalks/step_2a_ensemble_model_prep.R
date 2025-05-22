#########################################################################################
### Author: Cathleen Keller
### Date: 03/02/2023
### Project: ST-GPR custom linear stage
### Purpose: Test combinations of priors for ensemble model selection
#########################################################################################

### Define paths
data_root <- 'FILEPATH'
params_dir <- paste0(data_root, "/FILEPATH")

### Boilerplate
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_location_metadata.R")
source("/FILEPATH/get_covariate_estimates.R")
source("/FILEPATH/get_bundle_version.R")
library(lme4)
library(AICcmodavg)
pacman::p_load(data.table, dplyr,tidyr,stringr)

# Set constants
bundle_version_id <- ID 
release_id <- ADDRESS
years <- 1980:2024
cov_ids <- c(IDS)  #list of covariates to consider

#========================================
## Read in country-year-specific case data 
#  for endemic countries
#========================================

data <- get_bundle_version(bundle_version_id = bundle_version_id)
data$age_group_id <- 22
data$sex_id <- 3
data <- subset(data, year_id > 1979)

loc_meta <- get_location_metadata(location_set_id = 22, release_id = release_id)[level %in% 3:6]
loc_meta <- loc_meta[, c('location_id', 'location_name', 'ihme_loc_id', 'region_id', 'region_name')]

### Merge population & location data
locs <- unique(loc_meta$location_id)

pops <- get_population(location_id=locs,
                       age_group_id=22, 
                       sex_id=3,
                       year_id=years,
                       release_id = release_id)

# data-only loc-years for testing priors; data_expand includes rows for all loc-years -> need for full ensemble model
data$subset <- 1
data <- as.data.table(merge(data, pops, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE, all.y = FALSE))

data[, sample_size := population]
data <- data[, c('location_id','year_id','age_group_id','sex_id','underlying_nid','nid','field_citation_value','sex','sample_size','orig_cases',
                 'val','variance','subset')]

data <- left_join(data, loc_meta, by = c('location_id'))

data_expand <- as.data.table(merge(data, pops, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.x=TRUE, all.y = TRUE))
data_expand <- data_expand[, -c('run_id','ihme_loc_id','location_name','region_id','region_name')]

#========================================
## Pull covariate metadata
#========================================

cov_ids <- cov_ids
cov_dt <- NULL
for(covariate in cov_ids){
  cov_data <- as.data.table(get_covariate_estimates(release_id = release_id, covariate_id = covariate, location_id = 'all', year_id = years))
  cov_dt <- rbind(cov_dt, cov_data)
  
}

# Convert long to wide
covs <- dcast(cov_dt, location_id + year_id ~ covariate_name_short, value.var = "mean_value")
setnames(covs, "sanitation_prop", "sanitation")
setnames(covs, "pop_dens_under_150_psqkm_pct", "pop_dens")
setnames(covs, "universal_health_coverage", "uhc")

# Merge covariate values onto dataset
df <- as.data.table(left_join(data, covs, by = c("location_id", "year_id")))
df$log_ss <- log(df$sample_size)

# Create expanded dataset for full ensemble (step 2b)
data_expand <- left_join(data_expand, covs, by = c('location_id','year_id'))
data_expand <- left_join(data_expand, loc_meta, by = 'location_id')
data_expand$cases_ur <- data_expand$val/100000 * data_expand$sample_size     #need to convert to rate per 1
data_expand$sample_size <- data_expand$population
data_expand$log_ss <- log(data_expand$sample_size)

write.csv(data_expand, paste0(params_dir, '/FILEPATH'), row.names = FALSE)
#========================================
## DATA PREP FOR REGRESSION MODELS
#======================================== 

cov_names <- unique(names(covs))
predictors <- cov_names[! cov_names %in% c('location_id','year_id')]

mix <- ' + offset(log_ss) + (1|location_name) + (1|region_name)'

#========================================
## CREATE LIST OF MODELS
#========================================
list.of.models <- lapply(seq_along((predictors)), function(n) {
  
  LHS <- 'cases_ur'
  RHS <- apply(X = combn(predictors, n), MARGIN = 2, paste, collapse = " + ")
  
  paste(LHS, paste0(RHS, mix), sep = "  ~  ")
})

## Convert to a vector
vector.of.models <- unlist(list.of.models)

#=======================================
## TEST PRIORS (DIRECTIONALITY COVARIATES)
#========================================

# Read in data and priors
df <- fread(paste0(params_dir, '/FILEPATH'))
df <- subset(df, subset == 1) #only keep rows with data for testing

prior_sign <- fread(paste0(params_dir, '/FILEPATH'))

final_df <- NULL
for (m in 1:length(vector.of.models)){
  
  print(paste0('Model ', m))
  
  #run model
  model <- glmer.nb(vector.of.models[m], data = df)

  #pull coefficients into a dataframe
  coef_table <- summary(model)$coefficients %>% 
    as.data.table()
  
  #pull names of predictors to be tested
  cov_list <- attr(terms(formula(model)), which = "term.labels")
  rand_eff <- c('1 | location_name', '1 | region_name')
  cov_list <- cov_list[!(cov_list %in% rand_eff)]
  cov_list2 <- c('intercept', cov_list)
  
  coef_table$predictor <- cov_list2
  coef_table <- coef_table[-1, ] #drop intercept, not needed for testing prior
  
  #test directionality 
  coef_prior <- left_join(coef_table, prior_sign, by = 'predictor')
  coef_prior$test <- ifelse(coef_prior$Estimate > 0 & coef_prior$prior_sign > 0 | coef_prior$Estimate < 0 & coef_prior$prior_sign < 0, 1, 0)
  
  test <- ifelse(sum(coef_prior$test) == length(coef_prior$predictor), 'keep', 'drop')
  
  model <- vector.of.models[m]
  model_status <- test
  
  df_test <- data.frame(model, model_status)  
  
  final_df <- rbind(df_test, final_df)
}  

write.csv(final_df, paste0(params_dir, '/FILEPATH'), row.names = FALSE)

