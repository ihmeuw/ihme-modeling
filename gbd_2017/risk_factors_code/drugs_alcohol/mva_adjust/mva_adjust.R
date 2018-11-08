#############################################
# Adjust for MVA
#############################################

mva_adjust <- function(df){
    
    source("FILEPATH/get_draws.R")
    library(zoo)
    
    df <- df[, .(location_id, year_id, sex_id, age_group_id, cause_id, draw, paf)]
    
    #Add empty rows for missing age groups (Below age 15)
    new_ages <- expand.grid(location_id = location,
                            year_id = unique(df$year_id), 
                            sex_id = unique(df$sex_id),
                            draw   = unique(df$draw),
                            age_group_id = c(1, 6, 7),
                            cause_id = 688,
                            paf = 0)
    
    df <- rbind(df, new_ages)
    
    #Read in avg fatalities by age and percent victims by age
    
    fatalities <- fread("FILEPATH/mva_avg_fatalities.csv")
    victims    <- fread("FILEPATH/mva_victims.csv")
    
    #Get DALY draws for MVA and format to match paf dataframe
    mva_dalys <- get_draws('cause_id', 
                           688, 
                           'dalynator', 
                           location_id = location,
                           metric_id = 1,
                           measure_id = 2,
                           version_id = 77)
    
    mva_dalys <- copy(mva_dalys) %>%
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
  