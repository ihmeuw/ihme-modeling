
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
  
  #To be added later
  if (distribution == "custom"){
    return(NULL)
  }
}

mva_adjust <- function(df){
  
  source('FILEPATH')
  source('FILEPATH')
  
  library(zoo)
  
  df <- df[, .(location_id, year_id, sex_id, age_group_id, cause_id, draw, paf)]
  
  #Add empty rows for missing age groups (Below age 15)
  new_ages <- expand.grid(location_id = location,
                          year_id = unique(df$year_id), 
                          sex_id = unique(df$sex_id),
                          draw   = unique(df$draw),
                          age_group_id = c(1, 6, 7), #2,3,4,5 
                          cause_id = 688,
                          paf = 0)
  
  df <- rbind(df, new_ages)
  
  #Read in avg fatalities by age and percent victims by age
  
  fatalities <- fread('FILEPATH')
  victims    <- fread('FILEPATH')


  mva_dalys <- interpolate(gbd_round_id = 6,
                           decomp_step = "step4",
                          gbd_id_type = 'cause_id',
                          gbd_id = 688,
                          source = 'dalynator',
                          location_id = location,
                          measure_id = 2,
                          status = 'best',
                          reporting_year_start = 1990,
                          reporting_year_end = 2019
                         )
  
  mva_dalys <- copy(mva_dalys) %>%
    .[metric_id==1, ] %>%
    .[, c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", c(paste0("draw_", 0:999))), with = F] %>%
    melt(., id.vars = c("location_id", "year_id", "sex_id", "age_group_id", "cause_id"), 
         value.name = "dalys", 
         variable.name ='draw') %>%
    .[, draw := as.integer(gsub("draw_", "", draw))] %>%
    data.table
  
  #Join PAF results with MVA, add on average fatalities and calculate excess DALYs.
  df <- join(df, mva_dalys, by=c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", "draw"), type = 'left') %>%
    join(., fatalities, by=c("age_group_id", "sex_id")) %>%
    .[is.na(avg_fatalities), avg_fatalities := 1] %>%
    .[, attributable_dalys := paf*dalys] %>%
    .[, excess_dalys := attributable_dalys*(avg_fatalities)]
  
  #Reapportion fatalities to average victims by drunk driver's age/sex
  reapportion <- df[, .(year_id, sex_id, age_group_id, draw, excess_dalys)] %>%
    join(., victims, by=c("sex_id", "age_group_id"), type="inner") %>%
    data.table %>%
    .[, new_dalys := excess_dalys * pct_deaths] %>%
    .[, new_dalys := sum(.SD$new_dalys), 
      by = c("victim_sex_id", "victim_age_group_id", "year_id", "draw")] %>%
    .[, .(year_id, victim_sex_id, victim_age_group_id, draw, new_dalys)] %>%
    unique %>%
    setnames(., c("victim_sex_id", "victim_age_group_id"), c("sex_id", "age_group_id"))
  
  #Set as new PAF
  df <- join(df, reapportion, by = c("year_id", "sex_id", "age_group_id", "draw"), type="left") %>%
    data.table %>%
    .[, new_paf := new_dalys/dalys] %>%
    .[, paf := new_paf]
  
  return(df)
}
