


### MICRODATA STEPS-BASED UNRECORDED RATES

### Prep
library(readstata13)
library(tidyverse)
library(data.table)
library(boot)
source('FILEPATH')

locs <- get_location_metadata(22, release_id=16)

path <- 'FILEPATH'

files <- grep("STEP", list.files(path, full.names=T, recursive=T), value=T)
vars <- c("ihme_loc_id", "nid", "survey_name", "year_end", "year_start", "sex_id", "age_year", "drinker", "gday", "drinks_all", "drinks_unrec")

### Create STEPS dataset
df <- rbindlist(lapply(files, function(x) fread(x) %>% dplyr::select(any_of(vars))), fill=T)
df <- df %>% mutate(ihme_loc_id=ifelse(substr(ihme_loc_id, 1, 3)=="IND", "IND", ihme_loc_id)) # Fix India subnat names
df <- left_join(df, locs[, c("ihme_loc_id", "location_name", "region_name", "super_region_name")], by="ihme_loc_id") %>% 
  filter(drinker==1, !nid==516132, !ihme_loc_id=="ABW")
with(df %>% filter(is.na(drinks_all), is.na(drinks_unrec)), unique(nid)) # Check locations missing data
df <- drop_na(df, c(drinks_all, drinks_unrec))

### Read in old/prior rates for comparison
rates21 <- fread('FILEPATH') %>% mutate(round="GBD21")
rates23 <- fread('FILEPATH') %>% mutate(round="GBD23")
pastrates <- rbind(rates21, rates23) %>% left_join(locs[, c("location_id", "ihme_loc_id", "region_name", "super_region_name")]) %>% 
  mutate(unrecorded=ifelse(unrecorded>5, NA, unrecorded))
past_locs <- pastrates %>% group_by(ihme_loc_id, round) %>% dplyr::mutate(rate=round(mean(unrecorded, na.rm=T), 2)) %>% ungroup() %>% dplyr::select(ihme_loc_id, round, rate) %>% distinct() %>% pivot_wider(names_from=round, values_from=rate)
past_regions <- pastrates %>% group_by(region_name, round) %>% dplyr::mutate(rate=round(mean(unrecorded, na.rm=T), 2)) %>% ungroup() %>% dplyr::select(region_name, round, rate) %>% distinct() %>% pivot_wider(names_from=round, values_from=rate)
past_super_regions <- pastrates %>% group_by(super_region_name, round) %>% dplyr::mutate(rate=round(mean(unrecorded, na.rm=T), 2)) %>% ungroup() %>% dplyr::select(super_region_name, round, rate) %>% distinct() %>% pivot_wider(names_from=round, values_from=rate)

### Calculate unrecorded ratios by country/region/super-region
# country <- df %>% group_by(ihme_loc_id, sex_id) %>% 
country <- df %>% group_by(ihme_loc_id) %>%
  dplyr::summarize(n=n(), total=sum(drinks_all, na.rm=T), unrecorded=sum(drinks_unrec, na.rm=T)) %>% 
  mutate(recorded=total-unrecorded,
         total_cap=round(total/n, 3),
         recorded_cap=round(recorded/n, 3),
         unrecorded_cap=round(unrecorded/n, 3),
         diff_cap=round((recorded-unrecorded)/n, 3),
         prop=round(unrecorded/recorded, 3),
         ratio=1+prop,
         sim=round(recorded*ratio, 0),
         total_cap_day=round(total/(n*7), 2)) %>% 
  # sex=ifelse(sex_id==1, "Male", "Female")) %>%
  filter(total>10) %>% 
  left_join(locs[, c("ihme_loc_id", "super_region_name", "location_name")], by="ihme_loc_id") %>% 
  left_join(past_locs, by="ihme_loc_id")

# region <- df %>% group_by(region_name, sex_id) %>%
region <- df %>% group_by(region_name) %>%
  dplyr::summarize(total=sum(drinks_all, na.rm=T), unrecorded=sum(drinks_unrec, na.rm=T)) %>% 
  mutate(recorded=total-unrecorded,
         prop=round(unrecorded/recorded, 3),
         ratio=1+prop,
         # sex=ifelse(sex_id==1, "Male", "Female"),
         sim=round(recorded*ratio, 0)) %>% 
  arrange(desc(ratio)) %>% 
  dplyr::mutate(order=row_number()) %>% 
  left_join(past_regions, by="region_name")

# super <- df %>% group_by(super_region_name, sex_id) %>%
super <- df %>% group_by(super_region_name) %>%
  dplyr::summarize(total=sum(drinks_all, na.rm=T), unrecorded=sum(drinks_unrec, na.rm=T)) %>% 
  mutate(recorded=total-unrecorded,
         prop=round(unrecorded/recorded, 3),
         ratio=1+prop,
         # sex=ifelse(sex_id==1, "Male", "Female"),
         sim=round(recorded*ratio, 0)) %>% 
  arrange(desc(ratio)) %>% 
  dplyr::mutate(order=row_number()) %>% 
  left_join(past_super_regions, by="super_region_name")

### Bootstrap region and super-region estimates to draws for use in unrecorded adjustment
# Bootstrapped function
ratfun <- function(data, index){
  all <- sum(data[["drinks_all"]][index], na.rm=T)
  unrec <- sum(data[["drinks_unrec"]][index], na.rm=T)
  rec <- all-unrec
  ratio <- all/rec
  return(ratio)
}

# Region draws 
regionlist <- unlist(c(unique(region$region_name)))
region_draws <- data.frame(matrix(ncol=1002, nrow=0))
names(region_draws) <- c("region_name", "ratio", paste0("draw_", seq(0, 999)))
set.seed(1996)
for (x in regionlist){
  rdis <- boot(df[region_name==x,], ratfun, R=1000)
  newrow <- c(paste(x), rdis$t0, rdis$t)
  region_draws[nrow(region_draws)+1,] <- newrow
}
fwrite(region_draws, 'FILEPATH')

# Super-region draws
superlist <- unlist(c(unique(super$super_region_name)))
super_region_draws <- data.frame(matrix(ncol=1002, nrow=0))
names(super_region_draws) <- c("super_region_name", "ratio", paste0("draw_", seq(0, 999)))
set.seed(1996)
for (x in superlist){
  rdis <- boot(df[super_region_name==x,], ratz, R=1000)
  newrow <- c(paste(x), rdis$t0, rdis$t)
  super_region_draws[nrow(super_region_draws)+1,] <- newrow
}
fwrite(super_region_draws, 'FILEPATH')


#using region draws for all countries in the region. Filling in other locs with SR averages.
region_draws <- fread('FILEPATH')
region_draws <- melt(region_draws, id.vars = c("region"),variable.name = 'draw') %>% setnames(c('value'),c('unrecorded_reg'))
region_draws <- region_draws[draw!='ratio']
region_draws <- merge(region_draws,unique(locs[level>=3,.(region_name,super_region_name)]),by=c('region_name'),all.x=T,allow.cartesian = T)
super_region_draws <- fread('FILEPATH')
super_region_draws <- melt(super_region_draws, id.vars = c("superregion"),variable.name = 'draw') %>% setnames(c('value'),c('unrecorded_sr'))
super_region_draws <- super_region_draws[draw!='ratio']

unrecorded_draws <- merge(region_draws,super_region_draws,by=c('draw','super_region_name'),all=T)
#make template to expand estimates
template <- as.data.table(expand.grid(location_id=unique(locs[level>=3]$location_id),
                                      draw=paste0('draw_',0:999)))
template <- merge(template,unique(locs[level>=3,.(location_id,region_name,super_region_name)]),by=c('location_id'),all.x=T)
unrecorded_draws <- merge(unrecorded_draws,template,by=c('draw','region_name','super_region_name'),all=T)
unrecorded_draws[,unrecorded_sr := mean(unrecorded_sr,na.rm=T),by=.(draw,super_region_name)]
unrecorded_draws[,unrecorded := unrecorded_reg]
unrecorded_draws[is.na(unrecorded),unrecorded := unrecorded_sr]
unrecorded_draws <- unrecorded_draws[!(is.na(unrecorded))]
#copy Latin america region to HIC, its the best approximation
temp <- super_region_draws[super_region_name=='Latin America and Caribbean']
temp$super_region_name <- 'High-income'
temp <- merge(temp,template[super_region_name=='High-income'],by=c('super_region_name','draw'),all=T,allow.cartesian = T)
setnames(temp,'unrecorded_sr','unrecorded')

unrecorded_draws <- rbind(unrecorded_draws,temp,fill=T) %>% .[,.(location_id,draw,unrecorded)] %>% unique()
#version ready to be read into 5_finalize_adjustment
fwrite(unrecorded_draws,paste0('FILEPATH'))
  

