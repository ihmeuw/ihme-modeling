#############################################
# Calculate PAFs
#############################################
  
  library(data.table)  #For easy slicing/dicing of dataframes
  library(plyr)        #For fast and clear joins
  
  #Source central functions in GBD
  setwd("FILEPATH")
  source("get_demographics_template.R")
  source("get_location_metadata.R")
  source("get_draws.R")
  
  #Set up directories and code settings for paf calculation.
  
  version <- 3
  debug   <- FALSE
  
  directory <- "FILEPATH"
  
  code_directory     <- paste0(directory, "FILEPATH")
  exposure_directory <- paste0(directory, "FILEPATH")
  paf_directory      <- paste0(directory, "FILEPATH", version, "/")
  rr_directory       <- paste0(directory, "FILEPATH")
  
  dir.create(paste0(paf_directory, "epi_upload/"), showWarnings = FALSE, recursive = TRUE)
  
  #Set up settings for cluster submission
  
  project         <- " -P proj_ensemble "
  shell           <- paste0(code_directory, "r_shell.sh")
  errors          <- "FILEPATH"
  cluster_cores   <- 2
  
  #Set up arguments to pass to calculation
  
  sexes       <- c(1,2)
  ages        <- c(8:20, 30:32, 235)
  years       <- c(1990, 1995, 2000, 2005, 2010, 2017)
  locations   <- get_location_metadata(location_set_id = 35, gbd_round_id = 5)[, location_id]
  causes      <- 688
  #c(297,322,411,420,423,429,441,444,447,450,493,495,496,498,500,524,535,545,587,688,696,718,724)
  
  draws       <- 999     # Accepts values from 0-999
  
  #Build folders if they don't exist
  
  #Parallelize jobs by location & cause
  
  for (location in locations){
    for (cause in causes){
      for (sex in sexes){
        
        #Build qsub
        name <- paste0("alcohol_paf_",location,"_",sex, "_", cause)
        
        script <- paste0(code_directory,"paf_alcohol_calculation.R")
        
        arguments <- paste(debug, 
                           version, 
                           code_directory,
                           exposure_directory,
                           paf_directory,
                           rr_directory,
                           location,
                           paste(years, collapse=","), 
                           sex,
                           paste(ages, collapse=","),
                           paste(cause),
                           draws)
        
        qsub <- paste0("qsub -N ", name, 
                       project,
                       " -pe multi_slot ", cluster_cores,
                       " -l mem_free=", 2*cluster_cores,
                       " -e ", errors, " -o ", errors)
        
        #Run qsub
        if (debug == FALSE){
          system(paste(qsub, shell, script, arguments))
        } else{
          system(paste(qsub, shell, script, arguments))
        }
      }
    }
  }
  
  arg <- commandArgs()[-(1:3)]
  
  #Source packages
  library(plyr)
  library(data.table)
  library(dplyr)
  source("FILEPATH/paf_functions.R")
  
  #Read in arguments
  
  debug  <- arg[1]
  
  if (debug == "FALSE"){
    version             <- as.numeric(arg[2])
    code_directory      <- arg[3]
    exposure_directory  <- arg[4]
    paf_directory       <- arg[5]
    rr_directory        <- arg[6]
    
    location            <- as.numeric(arg[7])
    years               <- as.numeric(unlist(strsplit(arg[8][[1]], ",")))
    sex                 <- as.numeric(unlist(strsplit(arg[9][[1]], ",")))
    ages                <- as.numeric(unlist(strsplit(arg[10][[1]], ",")))
    cause               <- as.numeric(arg[11])
    draws               <- arg[12]
    
  } else{
    
    location            <- 66
    years               <- c(1990, 1995, 2000, 2005, 2010, 2017)
    sex                 <- c(1)
    ages                <- c(7:21, 31:33, 235)
    cause               <- 587
    draws               <- 999
  }
  
  cat(format(Sys.time(), "%a %b %d %X %Y"))
  
  #Read in exposures & change draw name to integer
  exposure <- fread(sprintf("%s/alc_exp_%s.csv", exposure_directory, location)) %>%
    .[, draw := as.numeric(gsub("draw_", "", draw))] %>%
    .[(age_group_id %in% ages) & (year_id %in% years) & (sex_id == sex) & (draw <= draws),]
  
  #Temporary - calculate drink_gday sd based on rate of mean drink_gday to sd of drink_day
  exposure <- exposure[, sd_rate := mean(.SD$drink_gday)/sd(.SD$drink_gday), 
                       by=c("location_id", "sex_id", "year_id", "age_group_id")] %>%
    .[, drink_gday_se := drink_gday/sd_rate]
  
  #For each cause, calculate paf and save
  
  #For liver cancer & cirrhosis, set PAF = 1 and exit
  if (cause %in% c(420, 524)){
    
    exposure <- exposure[, paf := 1] %>%
      .[, tmrel := 1] %>%
      .[, attribute := 0] %>%
      .[, cause_id := cause]
    
    write.csv(exposure, paste0(paf_directory, location,  "_", cause, "_", sex, ".csv"), row.names=F)
    
    cat(format(Sys.time(), "%a %b %d %X %Y"))
    cat("Finished!")
    quit()
  }
  
  #Use sex_specific RR if IHD, Stroke, or Diabetes. Otherwise, use both-sex RR
  if (cause %in% c(493, 495, 496, 587)){
    relative_risk <- fread(paste0(rr_directory, "/rr_", cause, "_", sex, ".csv"))
  } else{
    relative_risk <- fread(paste0(rr_directory, "/rr_", cause, ".csv"))
  }
  
  #For each location, sex, year, age, & draw, calculate attributable risk. Pass to function only selected columns of
  #the subset dataframe.
  
  exposure[, attribute := attributable_risk(.BY$draw, .SD, relative_risk), 
           by=c("location_id", "sex_id", "year_id", "age_group_id", "draw"), 
           .SDcols = c("draw", "current_drinkers","drink_gday", "drink_gday_se")]
  
  exposure[, tmrel := 1]
  
  #Using the calculated attributable risk, calculate PAF
  exposure[, paf := (abstainers+attribute-tmrel)/(abstainers+attribute)]
  exposure[, cause_id := cause]
  
  #For MVA, adjust for fatal harm caused to others
  if (cause == 688){
    exposure <- mva_adjust(exposure)
  }
  
  write.csv(exposure, paste0(paf_directory, location,  "_", cause, "_", sex, ".csv"), row.names=F)
  
  cat(format(Sys.time(), "%a %b %d %X %Y"))
  cat("Finished!")
  
  library(data.table)
  
  attributable_risk <- function(d, exposure, relative_risk, l=0, u=150){
    
    #Read in exposure dataframe, relative risk curves, and set lower and upper bounds for integral.
    #Exposure dataframe requires a column identifying mean population "dose" (in this case, g/day), the standard error of that
    #measure, as well as the percentage of the population exposure (in this case, current_drinkers).
    
    #Peg relative risk draw to draw of exposure passed.
    rr <- relative_risk[draw == d,]
    
    #Calculate individual distribution, multiply by relative risk curve. Integrate over this plane to get total attributable
    #risk within the population
    
    attribute <- function(dose){
      individual_distribution(dose, exposure$drink_gday, exposure$drink_gday_se) * construct_rr(dose, rr)
    }
    
    #Multiply risk by percentage of current drinkers
    attribute <- exposure$current_drinkers * integrate(attribute, lower=l, upper=u)$value
    
    #Integral isn't working for small amounts of drinking (<1 g/day). Assume the risk is 1.
    if (exposure$drink_gday < 1){
      attribute <- exposure$current_drinkers
    }
    
    return(attribute)
  }
  
  construct_rr <- function(dose, relative_risk){
    
    #Returns a complete set of relative risk exposures for a given draw. 
    
    #From spline points estimated, interpolate between to construct curve at non-integers.
    rr <- approx(relative_risk$exposure, relative_risk$rr, xout=dose)$y
    
    return(rr)
  }
  
  construct_tmrel <- function(d, tmrel, relative_risk){
    
    t <- tmrel[draw == d,]
    rr <- relative_risk[draw == d,]
    
    t <- construct_rr(t$tmrel, rr)
    return(t)
    
  }
  
  individual_distribution <- function(dose, exposure_mean, exposure_se, distribution="gamma", l=0, u=150){
    
    #Gamma is the default, can calculate custom distribution from weights if desired.
    if (distribution == "gamma"){
      
      alpha <- exposure_mean^2/((exposure_se^2))
      beta <- exposure_se^2/exposure_mean
      
      #Since Gamma is exponential, we need a normalizing constant to scale total area under the curve = 1. If mean is small,
      #just set normalizing constant = 1
      
      normalizing_constant <- 1
      
      if (exposure_mean >= 1){
        normalizing_constant <- integrate(dgamma, shape=alpha, scale=beta, lower=l, upper=u)$value
      }
      
      result <- (dgamma(dose, shape = alpha, scale = beta))/normalizing_constant
      
      return(result)
    }
    
    if (distribution == "custom"){
      return(NULL)
    }
  }
  