#########################################################################
# Description:                                                          #
# Generate shape parameters of underreporting factors. (1) Bring in UR  #                
# factors, estimate beta parameters, generate draws                     #
# NOTE: will need to update to 500 draws next cycle                     #
#########################################################################

### ========================= BOILERPLATE ========================= ###

rm(list=ls())

j <- 'FILEPATH'

# Load packages/central functions
library(openxlsx)
library(data.table)
library(ggplot2)
library(dplyr)

### ========================= MAIN EXECUTION ========================= ###
# Bring in country-specific underreporting factors as assigned by Alvar et al (PLoS Negl Trop Dis 2012).
# N.B the figures in the excel source file are color-coded to indicate whether they were originally assigned
# by Alvar or Luc Coffeng (previous NTD researchers), because Alvar et al had not assigned any value to a 
# specific country.

dt <- read.xlsx(paste0(j, "FILEPATH"))
dt <- dt[, c("iso3", "gbd_analytical_region_name", "cl_underreporting_hi", "cl_underreporting_lo")]

## CONVERT UNDERREPORTING FACTORS TO PROPORTION REPORTED ###
dt$prop_hi <- 1/dt$cl_underreporting_lo
dt$prop_lo <- 1/dt$cl_underreporting_hi

dt <- select(dt, -c("cl_underreporting_lo", "cl_underreporting_hi"))

## ESTIMATE PARAMETERS FOR A BETA DISTRIBUTION THAT CORRESPOND TO THE UPPER AND LOWER BOUNDS ###
## LOAD FUNCION TO SOLVE FOR BETA PARAMETERS (USED IN NL FUNCTION BELOW) ###
# Generate shape parameters for a beta distributions with 2.5 and 97.5 percentiles equal to one divided
# by upper and lower underreporting factors for CL, respectively. One divided by the underreporting factor
# represents the fraction of the true total that is reported in the data. N.B.: lower and upper bounds for
# underreporting factors may not have the same value! 

# Find a beta distribution such that the 2.5th and 97.5th percentiles match some known upper and lower bounds
# to use this, we will pass our parameters in as a vector (easier for optim)
# alpha and beta are the alpha and beta parameters for the beta function
# lower and upper are your targets for the 2.5th and 97.5th percentiles

evaluate_beta <- function(params, lower_target, upper_target) {
  
  alpha <- params[1]
  beta <- params[2]
  
  # Get the positions of the 2.5th and 97.5th percentiles of the cumulative distribution function
  beta_lims <- qbeta(c(0.025, 0.975), alpha, beta)
  
  # Calculate (absolute value) how far away each is from the target
  dist_from_lower <- abs(lower_target-beta_lims[1])
  dist_from_upper <- abs(upper_target-beta_lims[2])
  
  # Return of the sum of these (which we will minimize)
  return(dist_from_lower + dist_from_upper)
}

dt <- as.data.table(dt)

uf_temp <- lapply(unique(dt$iso3), function(loc) {
  
  solution <- optim(par = c(2,2),
                    fn = evaluate_beta,
                    lower_target = dt[iso3 == loc]$prop_lo,
                    upper_target = dt[iso3 == loc]$prop_hi)
  dt_temp <- dt[dt$iso3==loc,]
  dt_temp$alpha <- solution$par[1]
  dt_temp$beta <- solution$par[2]
  return(dt_temp)
  
})

uf_temp <- rbindlist(uf_temp)

## Randomly generate 1000 draws from beta distribution with derived alpha,beta shape parameters

# subset to level 3 locations
uf_df <- subset(uf_temp, nchar(as.character(iso3)) <= 3)
uf_df <- uf_df[order(uf_df$iso3),]

for (loc in unique(uf_df$iso3)){
  
  pred_alpha <- uf_df[iso3==loc]$alpha
  pred_beta <- uf_df[iso3==loc]$beta
  region <- uf_df[iso3==loc]$gbd_analytical_region_name
  
  for (i in 0:999) {
    if(loc=="SSD"){
      uf_df[iso3==loc, paste0("uf_", i)] <- 1/(rbeta(1, pred_alpha, pred_beta))*3
      
    } else if(region == "Western Europe") {
      uf_df[iso3==loc, paste0("uf_", i)] <- 1
      
    } else {
      uf_df[iso3==loc, paste0("uf_", i)] <- 1/(rbeta(1, pred_alpha, pred_beta))
    }
  }
  cat("\n Writing", loc) 
}

## Output file
write.csv(uf_df, paste0(params_dir, "/FILEPATH"), row.names = FALSE)