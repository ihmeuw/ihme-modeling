# Purpose: Saves named .RData file of underreporting bootstrap models to interms
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"

# Source relevant libraries
library(data.table)
library(stringr)
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source(paste0(code_root, '/FILEPATH/processing.R'))

# set-up run directory
#gen_rundir(data_root = data_root, acause = 'CAUSE', message = 'MESSAGE')
run_file <- fread(paste0(data_root, "/FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")
draws_dir <- paste0(run_dir, "/draws/")
interms_dir <- paste0(run_dir, "/interms/")

release_id <- ADDRESS

### ======================= MAIN ======================= ###

study_dems <- readRDS(paste0(data_root, '/FILEPATH.rds'))
year_ids <- study_dems$year_id

haqi    <- get_covariate_estimates(ADDRESS, release_id = release_id)
sdi     <- get_covariate_estimates(ADDRESS, release_id = release_id)
ilogit  <- function(x)1/(1+exp(-x))
underreport_raw <- fread(paste0(params_dir, "/FILEPATH"))

# outliered where underreporting percentage < 0.15
underreport_raw <- underreport_raw[(observed_cases/true_cases > .15)]

# gr - fix by adding up to level 3 locations
# will need to edit if grs change 
leish_endemic_vl <- fread(paste0(data_root, "/FILEPATH"))
# need to add Brazil and Bihar rows 
leish_endemic_vl_row_bih <- copy(leish_endemic_vl[location_id == ID])
leish_endemic_vl_row_bih[, location_id := ID]
leish_endemic_vl_row_bra <- copy(leish_endemic_vl[location_id == ID])
leish_endemic_vl_row_bra[, location_id := ID]
leish_endemic_vl <- rbind(leish_endemic_vl_row_bih, leish_endemic_vl_row_bra, leish_endemic_vl)

leish_endemic_cl <- fread(paste0(data_root, "/FILEPATH"))
leish_endemic_cl[, year_id := year_start]
leish_endemic_vl[, year_id := year_start]
leish_endemic_vl[, mean_value := value_endemicity]
leish_endemic_cl[, mean_value := value_endemicity]

#'[ Create Underreporting Model]

n_reps            <- 1000
underreport_list  <- list()
year_string       <- NA
proportion_string <- NA
proportion_error  <- NA
higher_bound      <- NA
lower_bound       <- NA
proportion_sample <- NA
std_dev           <- NA
random_location   <- NA

#create a string of unique locations, as want to include effect of including/excluding

uniqlo <- unique(underreport_raw$loc_id)

for (k in 1:n_reps){

  for (j in 1:nrow(underreport_raw)){
    
    if(length(underreport_raw$year_start[j]:underreport_raw$year_end[j])==1){
      year_string[j] <- underreport_raw$year_start[j]
    }else{
      year_string[j] <- sample(underreport_raw$year_start[j]:underreport_raw$year_end[j],1)
      #randomly sample between start and end to pick a year if study spans multiple
    }
    
    proportion_string[j] <- underreport_raw$observed_cases[j]/underreport_raw$true_cases[j]
    proportion_error[j]  <- sqrt((proportion_string[j] * (1 - proportion_string[j]))/underreport_raw$true_cases[j])
    # sqrt((p * 1 - p)) / true)
    
    higher_bound[j]      <- proportion_string[j] + (1.96*proportion_error[j])
    lower_bound[j]       <- proportion_string[j] - (1.96*proportion_error[j])
    std_dev[j]           <- proportion_error[j] * (sqrt(underreport_raw$true_cases[j]))
    proportion_sample[j] <- rnorm(1,proportion_string[j], proportion_error[j])
    
  }
  
  # of which there will be one thousand
  underreport_list[[k]]  <- data.frame(location_name=underreport_raw$location_name,
                                    year_start=year_string,
                                    loc_id=underreport_raw$loc_id,
                                    proportion=proportion_string,
                                    prop_low=lower_bound,
                                    prop_upper=higher_bound,
                                    std_dev=std_dev,
                                    proportion_sample=proportion_sample,
                                    pathogen=underreport_raw$pathogen,
                                    observed_cases=underreport_raw$observed_cases,
                                    true_cases=underreport_raw$true_cases)
                                
  random_location[k]    <- sample(uniqlo, 1)
  underreport_list[[k]] <- subset(underreport_list[[k]], underreport_list[[k]]$loc_id!=random_location[k])
  #remove a random location each time
}

#subset each into a training and a test dataset
#determine 90/10 fraction
fraction          <- 0.9

underreport_train <- list()
underreport_test  <- list()
sampling_frame    <- list()
omission_frame    <- list()

for (i in 1:n_reps){
  reference_string    <- seq(1, nrow(underreport_list[[i]]),1)
  sampling_frame[[i]] <- sample(reference_string,round(nrow(underreport_list[[i]])*fraction))
  omission_frame[[i]] <- setdiff(reference_string, sampling_frame[[i]])
  underreport_train[[i]] <- underreport_list[[i]][sampling_frame[[i]],]
  underreport_test[[i]]  <- underreport_list[[i]][omission_frame[[i]],]
}


mod       <- list()
pred_test <- list()

for (k in 1:n_reps){
  
  underreport <- underreport_train[[k]]
  pulled_year <- pulled_haqi <- pulled_sdi <- pulled_leish_e <- rep(NA,length(underreport[,1]))
  
  for (i in 1:length(underreport[,1])){
    pulled_year[i] <- underreport$year_start[i]
    
    pulled_haqi[i] <- haqi[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value
    
    pulled_sdi[i] <- sdi[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value
    
    if (underreport$pathogen[i]=='cl'){
      pulled_leish_e[i] <- leish_endemic_cl[location_id==underreport$loc_id[i] & year_id == pulled_year[i]]$mean_value
    }
    if (underreport$pathogen[i]=='vl'){
      pulled_leish_e[i] <- leish_endemic_vl[location_id==underreport$loc_id[i] & year_id ==pulled_year[i]]$mean_value
    }
  }
  
  
  pulled_data <- data.frame(observed = underreport$observed_cases,
                            true = underreport$true_cases,
                            pathogen = as.factor(underreport$pathogen),
                            year = pulled_year,
                            haqi = pulled_haqi,
                            sdi = pulled_sdi,
                            leish_endemic = pulled_leish_e)
  
  mod[[k]] <- glm(observed/true ~ pathogen + year + sdi, data = pulled_data, weights=true, family = binomial())

  LocToPull <- ID
  pred_year <- min(year_ids):max(year_ids)
  pred_path <- rep(levels(pulled_data$pathogen)[2], length(pred_year))
  pred_sdi <- sdi[location_id == LocToPull & year_id >= min(pred_year) & year_id <= max(pred_year)]$mean_value

  pred_test[[k]] <- predict(mod[[k]],data.frame(year = pred_year, pathogen = pred_path,  sdi = pred_sdi),se=TRUE)
}

#create a named .RData file tagged with experiment_name

save(mod,
     underreport_train,
     underreport_test,
     underreport_raw,
     file = paste0(interms_dir, "/FILEPATH.Rdata"))

# write out diagnostics

path <- c()
year <- c()
sdi <- c()
intercept <- c()

for (i in 1:1000){
  draw <- mod[[i]]
  coef <- coef(draw)
  intercept <- c(intercept, coef[1])
  path <- c(path, coef[2])
  year <- c(year, coef[3])
  sdi <- c(sdi, coef[4])
}

coefs <- data.table('term' = c('intercept', 'vlpathogen', 'sdi', 'year'),
                          'mean' = c(mean(intercept), mean(path), mean(sdi), mean(year)),
                          'variance' = c(var(intercept), var(path), var(sdi), var(year)))

fwrite(coefs, paste0(interms_dir, '/FILEPATH.csv'))