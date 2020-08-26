library(plyr)
library(data.table)
library(dplyr)

#Read in needed inputs
loc        <- as.numeric(commandArgs(trailingOnly = T)[1])

source('FILEPATH')
source('FILEPATH')
include_unrecorded <- F

gbd_num <- 6
locs <- get_location_metadata(location_set_id = 22, gbd_round_id = gbd_num)

main_dir   <- 'FILEPATH'
final_dir  <- 'FILEPATH'

# bind together the draws of tourist totals:
tourist_draws <- 'FILEPATH'
tourist_totals <- paste0(tourist_draws,list.files(tourist_draws)) %>%
lapply(fread, na.strings = c("", "NA", ".")) %>%
  rbindlist(fill=T)
fwrite(tourist_totals,'FILEPATH')


# read the ST-GPR draws:
filelist <- list.files('FILEPATH',full.names = T)
system.time(df <- lapply(filelist, fread) %>% rbindlist())
write.csv(df,'FILEPATH')

# population
template_pops <- get_population(location_id = unique(locs$location_id), sex_id = 3, age_group_id = 22, decomp_step = "iterative", year_id = seq(1970,2019,1))
template <- merge(template_pops,locs[,.(location_id,ihme_loc_id,location_name)])
setnames(template,old=c("population"),new=c("pop_scaled"))


tourist_proportions <- fread('FILEPATH') 
tourist_totals      <- fread('FILEPATH')
trip_duration       <- fread('FILEPATH') 
alc_lpc             <- fread('FILEPATH')


# edit the tourism proportions because the wrong name was used for Russia
# this has been fixed in the Python code, so this is just a quick fix for now
tourist_proportions$visiting_country <- gsub("Russia","Russian Federation",tourist_proportions$visiting_country)
tourist_proportions$location_name <- gsub("Russia","Russian Federation",tourist_proportions$location_name)

# and same for length of stay file
trip_duration$location_name <- gsub("Russia","Russian Federation",trip_duration$location_name)

loc_name <- template[location_id==loc, unique(location_name)]

# rename some of the tourist proportion columns
setnames(tourist_proportions, old = c("location_name","visiting_country"), new = c("host","location_name"))
tourist_proportions_fix <- merge(tourist_proportions, locs, by = "location_name") # get location IDs for the visiting countries; all of those that were dropped in this merge are not locations that we measure in GBD
tourist_proportions_fix$V1 <- NULL
tourist_proportions_fix$tourist_proportion <- NULL
setnames(tourist_proportions_fix, old = c("host","location_name","location_id"), new = c("location_name","visiting_country","location_id_visitor"))
tourist_proportions <- copy(tourist_proportions_fix)

#Get alcohol lpc and tourist totals from gpr. Reshape long and hold onto needed columns, discarding rest. Add on ids.
alc_lpc <-  join(alc_lpc, template, by=c("location_id", "year_id"), type="left") %>%
  melt(., id.vars=c("location_id", "location_name", "year_id", "pop_scaled"), 
       measure.vars=paste0("draw_", 0:999), value.name='alc_lpc', variable.name="draw") %>%
  .[!is.na(location_name)]

tourist_totals <- melt(tourist_totals, id.vars = c("location_id", "year_id"), measure.vars = paste0("draw_", 0:999),
                       value.name='total_tourists', variable.name='draw')

# convert to the correct units for tourist totals:
tourist_totals[,total_tourists := total_tourists*10000]

tourist_proportions <- setnames(tourist_proportions, "mean", "tourist_proportion") %>%
  .[(location_name == loc_name | location_id_visitor == loc),] %>%
  unique %>%
  .[!is.na(location_id_visitor)]

trip_duration <- join(trip_duration, template, by=c("location_name", "year_id"), type = "left") %>%
  .[, .(location_name, location_id, year_id, length_of_stay)] %>%
  .[!is.na(location_id)]

population <- template[location_id == loc, .(location_id, year_id, pop_scaled)]

#Separate proportions into consumption that locals consume while abroad and consumption tourists consume while visiting locally
abroad   <- tourist_proportions[location_id_visitor == loc, ] %>%
  join(., template, by=c("location_name", "year_id"), type = "left") %>%
  .[, .(location_name, location_id, year_id, visiting_country, location_id_visitor, tourist_proportion)] %>%
  .[, year_id := as.numeric(year_id)] %>%
  .[!is.na(location_id)]

domestic <- tourist_proportions[location_name == loc_name, ] %>%
  join(., template, by=c("location_name", "year_id"), type = "left") %>%
  .[, .(location_name, location_id, year_id, visiting_country, location_id_visitor, tourist_proportion)] %>%
  .[, year_id := as.numeric(year_id)]

#Calculate average length of stay and fill in for missing observations. Convert to fraction of year
average_duration <- trip_duration[, mean(length_of_stay, na.rm=TRUE)] # this is average length of stay over all countries, over all years
trip_duration    <- trip_duration[is.na(length_of_stay), length_of_stay := average_duration]

#Construct additive and subtractive measure of tourists for given location:
tourist_consumption <- function(h, y, v, tourist_prop, tourist_total, lpc, duration){
  
  #For a given host country, visiting country, and year, calculate 1000 draws of tourist consumption
  
  tourist_prop   <- tourist_prop[J(h, v, y), tourist_proportion]
  tourist_total  <- tourist_total[J(h, y), total_tourists] # this is the number of tourists in a country of interest
  lpc            <- lpc[J(v,y), alc_lpc]
  duration       <- trip_duration[J(v, y), length_of_stay]
  
  consumption <- lpc * tourist_prop * tourist_total * (duration/365)
  tourist_consumption <- data.table(host = h, visitor = v, year_id = y, 
                                    draw = paste0("draw_", 0:999), tourist_consumption = consumption)
  
  return(tourist_consumption)
}

setkey(abroad, location_id, location_id_visitor, year_id)
setkey(domestic, location_id, location_id_visitor, year_id)
setkey(tourist_totals, location_id, year_id)
setkey(alc_lpc, location_id, year_id)
setkey(trip_duration, location_id, year_id)

args <- unique(abroad[, .(location_id, year_id)])

# tourist proportions should be the proportion of tourists from the country of interest in each country that they visited
# then multiply the proportion of tourists from the country of interest by the number of tourists in that country and then by the average amount consumed by people in the country of interest
tourist_consumption_abroad <- mdply(cbind(h = as.numeric(args$location_id), y = as.numeric(args$year_id)), 
                                    tourist_consumption, v = loc, tourist_prop = abroad, tourist_total = tourist_totals, 
                                    duration = trip_duration, lpc = alc_lpc) %>% 
  as.data.table %>%
  .[, sum(.SD$tourist_consumption, na.rm=T), by=c('year_id', 'draw')] %>%
  setnames(., "V1", "tourist_consumption") %>%
  join(., population, by="year_id", type="left") %>%
  .[, tourist_consumption := tourist_consumption/pop_scaled]

args <- unique(domestic[, .(location_id_visitor, year_id)])
tourist_consumption_domestic <-  mdply(cbind(v = as.numeric(args$location_id), y = as.numeric(args$year_id)), 
                                       tourist_consumption, h = loc, tourist_prop = domestic, tourist_total = tourist_totals, 
                                       duration = trip_duration, lpc = alc_lpc) %>% 
  as.data.table %>%
  .[, sum(.SD$tourist_consumption, na.rm=T), by=c('year_id', 'draw')] %>%
  setnames(., "V1", "tourist_consumption") %>%
  join(., population, by="year_id", type="left") %>%
  .[, tourist_consumption := tourist_consumption/pop_scaled]

net_tourism <- tourist_consumption_abroad$tourist_consumption - tourist_consumption_domestic$tourist_consumption

net <- expand.grid(location_id = loc, location_name = loc_name, year_id = unique(tourist_consumption_domestic$year_id),
                   draw = unique(tourist_consumption_domestic$draw)) %>% cbind(., net_tourism) %>% data.table

write.csv(net, paste0(final_dir, "net_adjustment_", loc, ".csv"), row.names=F)


# run this once all of the individual tourism files have been written
filelist <- list.files(final_dir,full.names = T)
system.time(df <- lapply(filelist, fread) %>% rbindlist())
write.csv(df,'FILEPATH')
