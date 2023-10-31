source("FILEPATH/get_draws.R")
location_list <- read.csv("FILEPATH/Location_list_GBD2016.csv") 
for(year in c(1990, 1995, 2000, 2005, 2010, 2015)){
suicide_deaths <- get_draws(gbd_id_field='cause_id', gbd_id=718, source = 'codem', location_ids=unique(location_list$location_id), year_ids=year, gbd_round_id=3)
write.csv(suicide_deaths, file=paste0("FILEPATH/suicide_deaths_GBD2016_",year,".csv"))
}
