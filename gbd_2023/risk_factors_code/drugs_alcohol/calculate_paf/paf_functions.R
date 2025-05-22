

library(data.table)

attributable_risk <- function(d, a, exposure, relative_risk, l=0, u=100){
  
  
  #Peg relative risk draw to draw of exposure passed.
  rr <- relative_risk[draw == d & age_group_id == a,]
  
  #Calculate individual distribution, multiply by relative risk curve. Integrate over this plane to get total attributable
  #risk within the population
  
  attribute_f <- function(dose){
    individual_distribution(dose, exposure$drink_gday, exposure$drink_gday_se, u = u) * construct_rr(dose, rr)
  }
  
  #Multiply risk by percentage of current drinkers
  if (cause %in% c(297,545,521,444,411,543)){
    r_acc <- 0.005 
  } else {
    r_acc <- .Machine$double.eps^0.25 
  }
  
  attribute <- exposure$current_drinkers * integrate(attribute_f, lower=l, upper=u, rel.tol = r_acc)$value
  
 
  return(attribute)
}

construct_rr <- function(dose, relative_risk){
  
  
  rr <- approx(relative_risk$exposure, relative_risk$rr, xout=dose)$y
  
  return(rr)
}

construct_tmrel <- function(d, tmrel, relative_risk){
  
  t <- tmrel[draw == d,]
  rr <- relative_risk[draw == d,]
  
  t <- construct_rr(t$tmrel, rr)
  return(t)
  
}

individual_distribution <- function(dose, exposure_mean, exposure_se, distribution="gamma", l=0, u=100){
  
  if (distribution == "gamma"){
    
    alpha <- exposure_mean^2/((exposure_se^2))
    beta <- exposure_se^2/exposure_mean # beta is the scale parameter here. scale=var/mean


    
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

mva_adjust <- function(df, release_id, version_id, n_draws=500, location=NA){
  
  source('FILEPATH')
  source('FILEPATH')
  
  library(zoo)
  
  
  
  df <- df[, .(location_id, year_id, sex_id, age_group_id, cause_id, draw, paf)]
  
  new_ages <- expand.grid(location_id = location,
                          year_id = unique(df$year_id), 
                          sex_id = unique(df$sex_id),
                          draw   = unique(df$draw),
                          age_group_id = c(1, 6, 7), #2,3,4,5 
                          cause_id = 688,
                          paf = 0) %>% data.table
  
  df <- rbindlist(list(df, new_ages), use.names = T)
  
  
  
  fatalities <- fread('FILEPATH')
  victims    <- fread('FILEPATH')
  

  
  loc_orig <- NA
  
  if(location %in% c(60908, 94364, 95069)){
    loc_orig <- location
    location <- 179
  }
  
  mva_dalys <- get_draws(gbd_id_type = 'cause_id',
                         gbd_id = 688,
                         source = 'dalynator',
                         location_id = location,
                         metric_id = 1, # numbers
                         measure_id = 2, # dalys
                         n_draws = n_draws,
                         downsample = T,
                         version_id = version_id,
                         release_id = release_id)

  year_max <- max(mva_dalys[,year_id])
  if (years <= year_max){
    mva_dalys <- mva_dalys[year_id == years]
  } else {
    mva_dalys <- mva_dalys[year_id == year_max]
    mva_dalys$year_id <- years
  }

  
  mva_dalys <- copy(mva_dalys) %>%
    .[metric_id==1, ] %>%
    .[, c("location_id", "year_id", "sex_id", "age_group_id", "cause_id", c(paste0("draw_", seq(0,n_draws-1,1)))), with = F] %>% 
    melt(., id.vars = c("location_id", "year_id", "sex_id", "age_group_id", "cause_id"), 
         value.name = "dalys", 
         variable.name ='draw') %>%
    .[, draw := as.integer(gsub("draw_", "", draw))] %>%
    data.table
  
  if(loc_orig %in% c(60908, 94364, 95069)){
    location <- loc_orig
    mva_dalys[, location_id := loc_orig]
  }
  
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
