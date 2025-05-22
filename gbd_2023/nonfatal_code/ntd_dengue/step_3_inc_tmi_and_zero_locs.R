################################################################################
##### Dengue correcting low incidence based on CSMR and zeroing out non-endemic 
##### locations. This used to be separated into two scripts.

################################################################################
rm(list = ls())

library(dplyr)
library(data.table)
library(tidyverse)

source("FILEPATH/get_location_metadata.R")
source('FILEPATH/get_demographics.R')
source('FILEPATH/get_crosswalk_version.R')
source('FILEPATH/get_demographics.R')
source('FILEPATH/get_population.R')
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/save_results_epi.R")

mean_ui <- function(x){
  y <- dplyr::select(x, starts_with("draw"))
  y$mean <- apply(y, 1, mean)
  y$lower <- apply(y, 1, function(x) quantile(x, c(.025)))
  y$upper <- apply(y, 1, function(x) quantile(x, c(.975)))
  
  w <- y[, c('mean', 'lower', 'upper')]
  
  z <- dplyr::select(x, -contains("draw"))
  
  v <- cbind(z, w)
  v <- as.data.table(v)
  return(v)
}

################################################################################
# set paths and vars
release_id <- ADDRESS
date <- Sys.Date()
stgpr_mv <- ADDRESS
run_date <- "ADDRESS"
codf_mv <- ADDRESS
codm_mv <- ADDRESS
codf_shk <- ADDRESS
codm_shk <- ADDRESS
cw_id <- ADDRESS
model_years <- c(ADDRESS)
save_epi <- TRUE
desc<-paste0("ADDRESS")

path <- paste0("FILEPATH")
input_path <- paste0(path, "FILEPATH")
outpath <- paste0(path, "FILEPATH")
dir.create(outpath, recursive = T, showWarnings = FALSE)

################################################################################
##read in and save out files from stgpr_only to new outpath
files <- list.files(path = input_path, pattern = "FILEPATH", full.names = TRUE)
temp <- lapply(files, fread, sep=",")
df <- rbindlist(temp)
rm(temp)
x <- 1
for(i in unique(df$location_id)){
  message(paste0("Location_id: ", i, "; number: ", x, "/843"))
  #pull in single country and spit out
  sub  <- df[df$location_id == i,]

  write.csv(sub,(paste0(outpath, "FILEPATH")), row.names = FALSE)
  x <- x+1
}
rm(df, sub)

################################################################################
## read in results
files <- list.files(path = input_path, pattern = "FILEPATH", full.names = TRUE)
temp <- lapply(files, fread, sep=",")
dengue_gbd23_age_sp <- rbindlist(temp)
location_list <- unique(dengue_gbd23_age_sp$location_id)

# summarize for checks
dengue_gbd23_age_sp <- mean_ui(dengue_gbd23_age_sp)

# pull in CoD estimates and shocks
dengue_m <- get_model_results('cod', ADDRESS,  sex_id=1,  location_id = location_list, 
                              release_id=release_id, measure_id=1, model_version_id = codm_mv)
dengue_f<-get_model_results('cod', ADDRESS,   sex_id=2,  location_id = location_list,   
                            release_id=release_id, measure_id=1, model_version_id = codf_mv)

dengue_m_shk <- get_model_results('cod', ADDRESS,   sex_id=1,  location_id = location_list,  
                                  release_id=release_id, measure_id=1, model_version_id = codm_shk)
dengue_f_shk<-get_model_results('cod', ADDRESS,  sex_id=2,  location_id = location_list,   
                                release_id=release_id, measure_id=1, model_version_id = codf_shk)
dengue_m_shk[,population := 0]
dengue_f_shk[,population := 0]

dengue_both <- rbind(dengue_m, dengue_f,dengue_m_shk , dengue_f_shk, fill=T)
dengue_both <- dengue_both[, lapply(.SD, sum), by = c("location_id", "year_id", "age_group_id", "sex_id"),
                           .SDcols = c("mean_death", "population")]

dengue_both[, csmr := mean_death/population]

loc_meta <- get_location_metadata(location_set_id = ADDRESS, release_id = release_id)

# merge the results to CoD estimates
dengue_gbd23_age_sp_cfr <- merge(dengue_gbd23_age_sp, dengue_both, by = c("location_id", "year_id", "age_group_id", "sex_id"), all.x = T)

# calculate CFR and deaths
dengue_gbd23_age_sp_cfr[, cfr := csmr/mean]
dengue_gbd23_age_sp_cfr[, count_deaths := csmr * population]
dengue_gbd23_age_sp_cfr <- merge(dengue_gbd23_age_sp_cfr, loc_meta, by = "location_id")

# check instances where CFR == Inf
cfr_inf <- subset(dengue_gbd23_age_sp_cfr, cfr == Inf)
table(cfr_inf$location_name)

# Conduct CSMR checks
dengue_gbd23_age_sp_cfr2 <- dengue_gbd23_age_sp_cfr[mean>0,]
dengue_gbd23_age_sp_cfr3 <- subset(dengue_gbd23_age_sp_cfr2, cfr >1)
dengue_gbd23_age_sp_cfr3all <- subset(dengue_gbd23_age_sp_cfr3, age_group_id ==22)

dengue_gbd23_age_sp_cfr2a <- subset(dengue_gbd23_age_sp_cfr2, cfr <1)

dengue_gbd23_age_sp_cfr4 <- subset(dengue_gbd23_age_sp_cfr3, count_deaths> 1)

age_cfr <- copy(dengue_gbd23_age_sp_cfr2a)
age_cfr2 <- age_cfr %>%
  group_by(age_group_id) %>%
  summarize(age_cfr95 = quantile(cfr, probs = 0.95, na.rm = TRUE))

dengue_gbd23_age_sp_cfr2_loc <- merge(dengue_gbd23_age_sp_cfr2, age_cfr2, by = "age_group_id", all.x=T)
dengue_gbd23_age_sp_cfr2_loc2 <- subset(dengue_gbd23_age_sp_cfr2_loc, cfr > age_cfr95 )
dengue_gbd23_age_sp_cfr2_loc2md <- subset(dengue_gbd23_age_sp_cfr2_loc2, most_detailed ==1)

# check results
table_loc <- as.data.table(table(dengue_gbd23_age_sp_cfr2_loc2$location_name))
table_loc2 <- as.data.table(table(dengue_gbd23_age_sp_cfr2_loc2md$ihme_loc_id))

# get a list of location, years, and ages
loc_list_adj <- unique(dengue_gbd23_age_sp_cfr2_loc2md$location_id)
year_list_adj <- unique(dengue_gbd23_age_sp_cfr2_loc2md$year_id)
age_list_adj <- unique(dengue_gbd23_age_sp_cfr2_loc2md$age_group_id)

################################################################################
# now pull CoD and shock draws for applying the adjustment
death_draws_m <- get_draws("cause_id", ADDRESS, "ADDRESS", location_id=loc_list_adj, 
                           release_id = release_id, version_id=codm_mv,
                           year_id =model_years)
death_draws_f <- get_draws("cause_id", ADDRESS, "ADDRESS", location_id=loc_list_adj, 
                           release_id = release_id, version_id=codf_mv,
                           year_id =model_years)

death_draws_mshk <- get_draws("cause_id", ADDRESS, "ADDRESS", location_id=loc_list_adj, 
                              release_id = release_id, version_id=codm_shk, year_id =model_years)
death_draws_fshk <- get_draws("cause_id", ADDRESS, "ADDRESS", location_id=loc_list_adj, 
                              release_id = release_id, version_id=codf_shk, year_id =model_years)

death_draws <- rbind(death_draws_m,death_draws_f, death_draws_mshk, death_draws_fshk, fill=T )

death_draws2 <- death_draws[, lapply(.SD, sum), by = c("location_id", "year_id", "age_group_id","sex_id"),
                            .SDcols = c(paste0('draw_', 0:999))]

age_list_adj <- unique(death_draws2$age_group_id)

setnames(death_draws2, paste0('draw_', 0:999), paste0('death_draw_', 0:999))

# get population
pop <- get_population(release_id =release_id, location_id = loc_list_adj, 
                      age_group_id = age_list_adj, sex_id =c(1,2), year_id = model_years)

# merge pop and death draws to calculate CFR
death_draws2a <- merge(death_draws2, pop, by = c('location_id', 'age_group_id', 'sex_id', 'year_id'), all.x=T)

death_draws2a[, paste0("csmr_draw_", 0:999) := lapply(0:999, function(x) get(paste0("death_draw_", x)) / population)]

death_draws3 <- merge(death_draws2a,age_cfr2, by = 'age_group_id', all.x=T )

# check by age
table(death_draws3$age_cfr95, death_draws3$age_group_id)
zna <- subset(death_draws3, is.na(age_cfr95))

# set NAs to 0
death_draws3[is.na(age_cfr95), age_cfr95 := 0]

###  theorical minimum incidence based on 95th percentile from age-specific
# estimate theorical minimum incidence as csmr / age_cfr_max by age
death_draws3[, paste0("tmi_draw_", 0:999) := lapply(0:999, function(x) get(paste0("csmr_draw_", x)) / age_cfr95)]

# check NAs
zna <- subset(death_draws3, is.na(tmi_draw_1))

drawcol <- paste0("tmi_draw_", 0:999)
death_draws3f <- subset(death_draws3, select= c('location_id', 'sex_id', 'year_id', 'age_group_id' , 'age_cfr95',  drawcol ))

# check results
table_locage <- as.data.table(table(dengue_gbd23_age_sp_cfr2_loc2$location_name,dengue_gbd23_age_sp_cfr2_loc2$location_id, dengue_gbd23_age_sp_cfr2_loc2$age_group_id, dengue_gbd23_age_sp_cfr2_loc2$year_id))
table_locage <- subset(table_locage, N >0)

################################################################################
# Loop through the locations to adjust
for(loc in loc_list_adj){
  message(paste0("For loc ", loc))
  inc_draws <- fread(paste0(input_path, 'FILEPATH'))
  death_draw_loc <- subset(death_draws3f, location_id == loc)

  df_merged <- merge(inc_draws, death_draw_loc, by =c('location_id', 'sex_id', 'year_id', 'age_group_id'), all.x=T )


  for(d in 0:999){
    df_merged[get(paste0("draw_", d)) < get(paste0("tmi_draw_", d)) , paste0("draw_", d) := get(paste0("tmi_draw_", d)) /age_cfr95]
  }

  df_merged[, (drawcol ) := NULL]
  df_merged[, age_cfr95  := NULL]

  fwrite(df_merged,paste0(outpath, 'FILEPATH'))
}
################################################################################
# now handle the other locations that need to be 0
zero_locs <- fread(path, "FILEPATH")
zero_locs <- zero_locs[zero_locs$flag ==1,]

# this is getting endemic locs
d_geo<-read.csv(sprintf(paste0(path, "FILEPATH")), stringsAsFactors = FALSE)
d_geo<-d_geo[d_geo$most_detailed==1,]
d_geo<-d_geo[d_geo$year_start==2019,]
d_geo<-d_geo[d_geo$value_endemicity==1,]

# all locations
d_locs<-get_location_metadata(release_id = release_id, location_set_id = ADDRESS)
d_locs<-d_locs[d_locs$is_estimate==1,]
location_list<-unique(d_locs$location_id)

# list of unique dengue non-endemic locations
unique_d_locations<-unique(d_geo$location_id)
ne_locs<-location_list[! location_list %in% unique_d_locations]

draw.cols <- paste0("draw_",0:999)
ne_locs<-location_list[! location_list %in% unique_d_locations]

# write out non-endemic files
x <- 1
mod1<-read.csv((paste0(outpath, "FILEPATH")))
for(i in ne_locs){
  message(paste0(i,"; ",x, "/", length(ne_locs)))
  # pull in single country and write out to copy over with zeros for non-endemic settings
  draws2<-setDT(mod1)
  draws2$location_id<-i
  draws2[, id := .I]
  # set all draws  to 0
  draws2[, (draw.cols) := 0, by=id]

  draws2$id <- NULL

  fwrite(draws2,(paste0(outpath,"FILEPATH")))
  x <- x+1
}

# now handle the other locations that need to be 0
x <- 1
for(i in unique(zero_locs$location_id)){
  message(paste0(i,"; ",x, "/", length(unique(zero_locs$location_id))))
  # pull in single country and write out to copy over with zeros for non-endemic settings
  draws2<-setDT(mod1)
  draws2$location_id<-i
  draws2[, id := .I]
  # set all draws  to 0
  draws2[, (draw.cols) := 0, by=id]

  draws2$id <- NULL

  fwrite(draws2,(paste0(outpath, i,"ADDRESS")))
  x <- x+1
  }
################################################################################
## after running, need to save
if (save_epi == TRUE){
  
  save_results_epi(input_dir =outpath,
                   input_file_pattern = "ADDRESS",
                   modelable_entity_id = ADDRESS,
                   description = desc,
                   measure_id = ADDRESS,
                   release_id = release_id,
                   bundle_id=ADDRESS,
                   crosswalk_version_id=cw_id,
                   year_id = model_years,
                   mark_best = TRUE
  )
}
