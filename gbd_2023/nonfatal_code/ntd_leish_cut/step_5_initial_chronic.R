#######################################################################
# Description:                                                        #
# Estimating starting point for prevalence estimation of chronic      #                                                          
# sequela - using 1-year age groups                                   #
#                                                                     #
#######################################################################

### ========================= BOILERPLATE ========================= ###
rm(list = ls())
# Load packages/central functions
library(dplyr)
library(tidyr)
library(ggplot2)
library(data.table)
source('/FILEPATH/get_covariate_estimates.R')
source('/FILEPATH/get_population.R')
source('/FILEPATH/get_location_metadata.R')
source('/FILEPATH/get_age_metadata.R')
source('/FILEPATH/get_age_spans.R')

param_map <- fread("FILEPATH")
task_id <- as.integer(Sys.getenv("ADDRESS"))
i <- param_map[task_id, location_id]

code_root <- "FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
}

# Set run dir
run_file <- fread(paste0(data_root, "/FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")
draws_dir    <- paste0(run_dir, "/draws/")
interms_dir    <- paste0(run_dir, "/interms/")

# Define constants
release_id <- release_id

### ========================= MAIN EXECUTION ========================= ###

###' [1) Pull in UHC data and generate draws of mean with uncertainty
uhc_data <- fread(paste0(params_dir, "/FILEPATH"))
uhc_data_loc <- uhc_data[location_id == i & year_id == 1990 ,]
uhc_dt <- uhc_data_loc %>%
  slice(rep(1:n(), each = 51))
uhc_dt$year_id <- 1940:1990

###' [2) Create age dataframe of 1-yr bins within GBD age groups 
gbd_ages <- get_age_metadata(age_group_set_id = 19)$age_group_id
ages_1yr_spans <- fread(paste0(params_dir, "/FILEPATH"))
setnames(ages_1yr_spans, 'age_group_years_start', 'age_start')

###' [3) Pull in incidence draws
inc_df <- fread(paste0(interms_dir, "/FILEPATH"))
inc_df <- inc_df[inc_df$year_id %in% 1980:1990,]
  
data <- as.data.table(inc_df)
  
###' [4) Create array, loop through draws and years and backfill initial values to estimate prevalence
  n_draws <- 1000
  draw_col_names <- paste0("draw_", 0:(n_draws-1))
  draw_cols_uhc <- paste0("draw_uhc_", 0:(n_draws-1)) 
  ages <- unique(ages_1yr_spans$age_start)
  n_ages <- as.numeric(length(ages))
  years <- 1890:1990
  n_years <- as.numeric(length(years))
  sexes <- c(1,2)
  n_sex <- as.numeric(length(sexes))
  
  ar <- array(dim = c(3, n_years, n_sex, n_ages, n_draws),
              dimnames = list(c("uhc", "inc", "r_c"),
                              years,
                              sexes,
                              ages,
                              paste0("draw_", 0:(n_draws-1))))
  #define constants
  uhc_start_year <- 1940
  
  # fill in initial values  
  # NOTES: filling in age using 1-yr age bins, will need to convert back to age_group_id after
  for (yr in years){
    if (yr < uhc_start_year){
      ar["uhc", as.character(yr),,,] <- 0
      
    } else if (yr >= uhc_start_year) {
      ar["uhc", as.character(yr),,,] <- as.numeric(uhc_dt[year_id == 1990, draw_cols_uhc, with = F])
      
    }
  } 
  
  for (yr in years){
    for (sex in sexes){
      for (a in ages) {
        
        if (a < 0.5){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- 0
        
        } else if (yr < 1980 & a > 0.5 & a < 1.0){

          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 389, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a > 0.5 & a < 1.0){

          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 389, draw_col_names, with = F])
          
        } else if (yr < 1980 & a == 1){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 238, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a == 1){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 238, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 2:4){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 34, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 2:4){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 34, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 5:9){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 6, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 5:9){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 6, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 10:14){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 7, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 10:14){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 7, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 15:19){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 8, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 15:19){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 8, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 20:24){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 9, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 20:24){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 9, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 25:29){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 10, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 25:29){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 10, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 30:34){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 11, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 30:34){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 11, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 35:39){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 12, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 35:39){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 12, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 40:44){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 13, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 40:44){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 13, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 45:49){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 14, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 45:49){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 14, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 50:54){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 15, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 50:54){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 15, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 55:59){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 16, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 55:59){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 16, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 60:64){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 17, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 60:64){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 17, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 65:69){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 18, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 65:69){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 18, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 70:74){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 19, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 70:74){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 19, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 75:79){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 20, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 75:79){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 20, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 80:84){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 30, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 80:84){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 30, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 85:89){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 31, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 85:89){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 31, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 90:94){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 32, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 90:94){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 32, draw_col_names, with = F])
          
        } else if (yr < 1980 & a %in% 95:99){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == 1980 & sex_id == sex & age_group_id == 235, draw_col_names, with = F])
          
        } else if (yr %in% 1980:1990 & a %in% 95:99){
          ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 235, draw_col_names, with = F])
          
        }
      }
    }
  }
  
  #setting all r_c to zero for ease of coding  
  for (yr in years){
    ar["r_c", "1890",,,] <- 0
  } 
  
  ## Estimate chronic sequelae
  for (yr in 1891:1990){
    for (sex in sexes) {
      for (a in ages) {
        
        if (a < 0.5){
          ar["r_c", as.character(yr), as.character(sex), as.character(a),] <- 0
          
        } else if (a > 0.5 & a < 1.0){

          ar["r_c", as.character(yr), as.character(sex), as.character(a),] <- ar["inc", as.character(yr), as.character(sex), as.character(a),] * (1 - ar["uhc", as.character(yr), as.character(sex), as.character(a),]) * 0.476
          
        }else if (a == 1){
          ar["r_c", as.character(yr), as.character(sex), as.character(a),] <- ar["inc", as.character(yr), as.character(sex), as.character(a),] * (1 - ar["uhc", as.character(yr), as.character(sex), as.character(a),]) * 0.476 +
            ar["r_c", as.character(yr - 1), as.character(sex), "0.50136986",]
          
        }else if (a >=2){
          ar["r_c", as.character(yr), as.character(sex), as.character(a),] <- ar["inc", as.character(yr), as.character(sex), as.character(a),] * (1 - ar["uhc", as.character(yr), as.character(sex), as.character(a),]) * 0.476 +   #chronic of current year/age from new infections
            ar["r_c", as.character(yr - 1), as.character(sex), as.character(a - 1),]
        }
      }
    }
  }  
  
  ## Data management
  dt <- as.data.table(ar)
  setnames(dt, old = c('V1','V2','V3','V4','V5'), new = c('measure','year_id','sex_id','age_start','draws'))
  
  dt <- dcast(dt, measure + year_id + sex_id + age_start ~ draws, value.var = "value")
  
  dt_chronic <- filter(dt, measure == 'r_c')
  dt_chronic$age_start <- as.double(dt_chronic$age_start)
  
  write.csv(dt_chronic, file = (paste0(interms_dir, "/FILEPATH")), row.names = F)
  cat("\n Writing", i) 

