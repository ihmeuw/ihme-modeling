
arguments <- commandArgs()[-(1:5)]

#Source packages
library(plyr)
library(data.table)
library(dplyr)

#Read arguments and make necessary variables

param_map <- fread('FILEPATH')
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))

location   <- param_map[task_id, location]
paf_directory <- arguments[1]

years         <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
sexes         <- c(1, 2)

setwd(paf_directory)
files <- list.files(paf_directory)
files <- files[grep(paste0("^", location, "_"), files)]

cause_map <- fread('FILEPATH')


#Read in all causes & sexes, collapse into single dataframe, then add in child causes
#Then reshape wide, and save in the particular format required for save_results

compiler <- function(f){
  
  df <- fread(f) %>%
    .[, .(location_id, year_id, sex_id, age_group_id, cause_id, draw, paf)] %>%
    .[!is.na(paf), ]
  
  return(df)
  
}

#For causes where we only estimate for the parent, remap to child causes
cause_mapper <- function(index){
  
  c_old <- cause_map[index]$cause_id_old
  c_new <- cause_map[index]$cause_id_new
  
  df <- compiled[cause_id == c_old,] %>%
    .[, cause_id := c_new]
  
  #Issue with redistribution of MVA injuries using FARS in some old age groups;
  #Depending on exposure levels/CoD, below may not kick in but added here as a safety net.
  #Real solution would be to include injury redistribution using sources from countries besides the US
  
  if (c_old == 688){
    
    hold <- copy(df) %>%
      .[age_group_id == 30,] %>%
      .[, age_group_id := NULL] %>%
      setnames(., "paf", "new_paf")
    
    df <- join(df, hold, by=c("location_id", "year_id", "sex_id", "cause_id", "draw")) %>%
      data.table %>%
      .[age_group_id %in% c(31, 32, 235), paf := new_paf] %>%
      .[, new_paf := NULL] %>%
      .[paf >= 0.3, paf := 0.3]
    
  }
  return(df)
}

#Read in all cause files for given location/years
compiled <- ldply(files, compiler) %>%
  data.table

#Remap causes estimated at higher levels
cause_map_id <- seq(1, dim(cause_map)[1])

add_causes <- ldply(cause_map_id, cause_mapper)

compiled <- compiled[(!cause_id %in% unique(cause_map$cause_id_old))]
compiled <- rbind(compiled, add_causes)

#Write each cause in save_results format
compiled[, draw := paste0("paf_", draw)]
compiled <- dcast(compiled, location_id + year_id + sex_id + age_group_id + cause_id ~ draw, value.var='paf')

for (sex in sexes){
  for (year in years){
    
    df <- compiled[(sex_id == sex & year_id == year)]
    df <- df[, names(df)[!names(df) %in% c("location_id", "sex_id")], with=FALSE]
    
    write.csv(df, 'FILEPATH', row.names = F) #yld
    write.csv(df, 'FILEPATH', row.names = F) #yll
  }
}

print(paste0(location, "_", year, "_", sex," Done!"))
