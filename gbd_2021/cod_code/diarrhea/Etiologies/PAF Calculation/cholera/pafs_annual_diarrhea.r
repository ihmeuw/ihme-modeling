##############################
### ANNUAL PAFs
##############################


print(commandArgs()[1])
location <- commandArgs(trailingOnly=TRUE)[1]

library(plyr)
library(boot)
source("/FILEPATH/get_draws.R")
source("/FILEPATH/get_population.R")
source("/FILEPATH/get_age_metadata.R")
age_map <- get_age_metadata(age_group_set_id = 19)
age_group_id <- age_map$age_group_id

sex_id <- c(1,2)
year_id <- c(1990, 1995, 2000, 2005, 2010, 2015, 2019, 2020, 2021, 2022)


source("/FILEPATH/interpolate.R")
source("/FILEPATH/get_location_metadata.R")
locs <- as.data.table(get_location_metadata(location_set_id = 9, gbd_round_id = 7))

keep <- "all"
eti <- read.csv("/FILEPATH/eti_rr_me_ids.csv")
eti_meta <- read.csv("/FILEPATH/eti_rr_me_ids.csv")

if(keep == "all"){
  info <- eti
} else {
  info <- subset(eti, model_source==keep)
}

gbd_round_id <- 7
sex_id <- c(1,2)


## YLL

for(v in c(5)){
  m <- eti_meta$modelable_entity_id[v] 
  r <- eti_meta$rei_id[v]
  name <- as.character(eti_meta$name_colloquial[v])
  eti_dir <- eti_meta$rei[v]

  print(paste0("Interpolating years for ", name))

  df <- interpolate(gbd_id_type="rei_id"
                    , gbd_id=r, source='paf'
                    , measure_id=4
                    , location_id=location
                    , sex_id=sex_id
                    , gbd_round_id=7
                    , decomp_step='iterative'
                    , reporting_year_start=1990
                    , reporting_year_end=2022
  )

  df$cause_id <- 302
  df$rei_id <- r
  df$modelable_entity_id <- m

  write.csv(df, paste0("/FILEPATH/paf_annual_yll_location.csv"), row.names=F)

  print(paste0("Saved for ", name,"!"))

}


## YLD

for(v in c(5)){
  m <- eti_meta$modelable_entity_id[v]
  r <- eti_meta$rei_id[v]
  name <- as.character(eti_meta$name_colloquial[v])
  eti_dir <- eti_meta$rei[v]

  print(paste0("Interpolating years for ", name))

  df <- interpolate(gbd_id_type="rei_id"
                    , gbd_id=r, source='paf'
                    , measure_id=3
                    , location_id=location
                    , sex_id=sex_id
                    , gbd_round_id=7
                    , decomp_step='iterative'
                    , reporting_year_start=1990
                    , reporting_year_end=2022
  )

  df$cause_id <- 302
  df$rei_id <- r
  df$modelable_entity_id <- m

  write.csv(df, paste0("/FILEPATH/paf_annual_yld_location.csv"), row.names=F)

  print(paste0("Saved for ", name,"!"))

}

