##########################################################################
# Description:                                                           #
# Cohort estimation - pull in estimated incidence and combine with       #                                                                                                                           
##########################################################################

### ========================= BOILERPLATE ========================= ###
rm(list = ls())

# Load packages/central functions
library(dplyr)
library(tidyr)
library(ggplot2)
library(data.table)
library(fuzzyjoin)
source('/FILEPATH/get_covariate_estimates.R')
source('/FILEPATH/get_population.R')
source('/FILEPATH/get_age_spans.R')
source('/FILEPATH/get_age_metadata.R')
source('/FILEPATH/get_population.R')

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

# Create directory for prevalence
dir.create(paste0(interms_dir, "FILEPATH/"))

# Define constants
release_id <- release_id
### ========================= MAIN EXECUTION ========================= ###

###' [1) Pull in UHC data
uhc_data_cl <- fread(paste0(params_dir, "/FILEPATH"))

###' [2) Pull in age bins for indexing
ages_1yr_spans <- fread(paste0(params_dir, "/FILEPATH"))
ages_gbd_bins <- fread(paste0(params_dir, "/FILEPATH")) 

uhc_dt <- uhc_data_cl[location_id == i, ]
chronic_dt <- fread(paste0(interms_dir, "FILEPATH"))
  
###' [3) Pull in incidence draws and merge with UHC draws
inc_df <- fread(paste0(interms_dir, "/FILEPATH"))
inc_df <- inc_df[inc_df$year_id > 1989, ]
  
data <- as.data.table(inc_df)

###' [4) Create array, loop through draws and years to estimate prevalence of chronic sequelae
  n_draws <- 1000
  draw_col_names <- paste0("draw_", 0:(n_draws-1))
  draw_cols_uhc <- paste0("draw_uhc_", 0:(n_draws-1))
  ages <- unique(ages_1yr_spans$age_group_years_start)
  n_ages <- as.numeric(length(ages))
  years <- 1990:2024
  n_years <- as.numeric(length(years))
  sexes <- c(1,2)
  n_sex <- as.numeric(length(sexes))
  
  ar <- array(dim = c(3, n_years, n_sex, n_ages, n_draws),
              dimnames = list(c("uhc", "inc", "r_c"),
                              years,
                              sexes,
                              ages,
                              paste0("draw_", 0:(n_draws-1))))
  
  # fill in initial values  
  for (yr in years){
    if (yr < 2020){
      ar["uhc", as.character(yr),,,] <- as.numeric(uhc_dt[year_id == yr, draw_cols_uhc, with = F])
      
    } else if (yr > 2019) {
      ar["uhc", as.character(yr),,,] <- as.numeric(uhc_dt[year_id == 2019, draw_cols_uhc, with = F])
      
    }
  } 
  
for (yr in years){
  for (sex in sexes){
    for (a in ages) {
      
      if (a < 0.5){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- 0
        
  #    } else if (a == 0.50136986){
      } else if (a > 0.5 & a < 1.0){

        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 389, draw_col_names, with = F])
        
      } else if (a == 1){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 238, draw_col_names, with = F])
        
      } else if (a %in% 2:4){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 34, draw_col_names, with = F])
        
      } else if (a %in% 5:9){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 6, draw_col_names, with = F])
        
      } else if (a %in% 10:14){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 7, draw_col_names, with = F])

      } else if (a %in% 15:19){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 8, draw_col_names, with = F])

      } else if (a %in% 20:24){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 9, draw_col_names, with = F])

      } else if (a %in% 25:29){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 10, draw_col_names, with = F])
        
      } else if (a %in% 30:34){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 11, draw_col_names, with = F])

      } else if (a %in% 35:39){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 12, draw_col_names, with = F])

      } else if (a %in% 40:44){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 13, draw_col_names, with = F])
        
      } else if (a %in% 45:49){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 14, draw_col_names, with = F])
  
      } else if (a %in% 50:54){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 15, draw_col_names, with = F])

      } else if (a %in% 55:59){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 16, draw_col_names, with = F])
        
      } else if (a %in% 60:64){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 17, draw_col_names, with = F])

      } else if (a %in% 65:69){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 18, draw_col_names, with = F])

      } else if (a %in% 70:74){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 19, draw_col_names, with = F])

      } else if (a %in% 75:79){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 20, draw_col_names, with = F])
        
      } else if (a %in% 80:84){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 30, draw_col_names, with = F])

      } else if (a %in% 85:89){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 31, draw_col_names, with = F])
        
      } else if (a %in% 90:94){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 32, draw_col_names, with = F])
        
      } else if (a %in% 95:99){
        ar["inc", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(data[year_id == yr & sex_id == sex & age_group_id == 235, draw_col_names, with = F])

      }  
    }
  }
}

  for (yr in 1990){
    for (sex in sexes){
      for (a in ages){
        
        if (a < 0.5){
        ar["r_c", as.character(yr), as.character(sex), as.character(a),] <- 0
        
        } else if (a > 0.5){
        ar["r_c", as.character(yr), as.character(sex), as.character(a),] <- as.numeric(chronic_dt[year_id == yr & sex_id == sex & age_start == a, draw_col_names, with = F])
        }
      }
    } 
  }
  
for (yr in 1991:2024){
  for (sex in sexes) {
    for (a in ages) {
      
      if (a < 0.5){
        ar["r_c", as.character(yr), as.character(sex), as.character(a),] <- 0
        
      } else if (a == 0.50136986){
#      } else if (a > 0.5 & a < 1.0){

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

  ## Estimate prevalence

  dt <- as.data.table(ar)
  setnames(dt, old = c('V1','V2','V3','V4','V5'), new = c('measure','year_id','sex_id','age_group_years_start','draws'))
  
  dt <- dcast(dt, measure + year_id + sex_id + age_group_years_start ~ draws, value.var = "value")
  dt <- as.data.table(dt)
  
  dt_inc <- dt[measure == 'inc',]
  dt_inc[, measure:=NULL]
  dt_inc$age_group_years_start <- as.numeric(dt_inc$age_group_years_start)
  setnames(dt_inc, old = paste0("draw_", 0:999), new = paste0("draw_inc_", 0:999))
  
  dt_uhc <- dt[measure == 'uhc',]
  dt_uhc[, measure:=NULL]
  dt_uhc$age_group_years_start <- as.numeric(dt_uhc$age_group_years_start)
  setnames(dt_uhc, old = paste0("draw_", 0:999), new = paste0("draw_uhc_", 0:999))
  
  dt_chronic <- dt[measure == 'r_c',]
  dt_chronic[, measure:=NULL]
  dt_chronic$age_group_years_start <- as.numeric(dt_chronic$age_group_years_start)
  setnames(dt_chronic, old = paste0("draw_", 0:999), new = paste0("draw_rc_", 0:999))

  dt_final <- merge(dt_inc, dt_chronic, by = c('year_id', 'sex_id', 'age_group_years_start'))
  dt_final <- merge(dt_final, dt_uhc, by = c('year_id', 'sex_id', 'age_group_years_start'))
  
  #calculate final prevalence
  dt_final[, paste0("draw_",0:999) := lapply(0:999, function(x)
    get(paste0("draw_rc_", x)) + (get(paste0("draw_inc_", x)) - (get(paste0("draw_inc_", x)) * (1 - get(paste0("draw_uhc_", x))) * 0.476)) / 2)]
  
  #data management - aggregate to GBD ages - delete unnecessary columns
  dt_final2 <- left_join(dt_final, ages_1yr_spans, by = 'age_group_years_start')
  dt_final2[, age_group_years_start := ifelse(age_group_years_start > 95, 95, age_group_years_start)]
  dt_final2[, age_group_years_end := ifelse(age_group_years_start == 95, 96, age_group_years_end)]
  
  final_gbd_bins <- fuzzy_left_join(dt_final2, ages_gbd_bins, by = c('age_group_years_start', 'age_group_years_end'),
                                      match_fun = list(`>=`, `<=`)) 
  
  cols_to_drop <- c(paste0("draw_inc_", 0:999), paste0("draw_rc_", 0:999), paste0("draw_uhc_", 0:999), "age_group_id.x", "age_group_years_start.x", "age_group_years_start.y", "age_group_years_end.x", "age_group_years_end.y")
  final_gbd_bins <- final_gbd_bins[ ,!(names(final_gbd_bins) %in% cols_to_drop)]
  setnames(final_gbd_bins, "age_group_id.y", "age_group_id")

  mean_draws <- as.data.table(final_gbd_bins)
  mean_draws <- mean_draws[, lapply(.SD, mean), by = c("age_group_id", "sex_id", "year_id"), .SDcols = paste0("draw_", 0:999)]
  
  mean_draws$location_id <- i
  mean_draws$measure_id <-  5
  
  write.csv(mean_draws, file = (paste0(interms_dir, "FILEPATH")), row.names = F)
  cat("\n Writing", i) 
