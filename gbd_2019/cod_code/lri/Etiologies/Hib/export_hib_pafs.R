##############################################################
## This file is simple. The only thing it does is to
## expand the Hib PAF draw file and export as CSVs by location
## to be uploaded using save_results ##
##############################################################
library(plyr)
library(data.table)

## Estimation years ##
years <- c(1990,1995,2000,2005,2010,2015,2017,2019)

## Load in age information ##
ages <- read.csv("filepath")
ages <-  subset(ages, !is.na(order) & age_group_id!=33)
ages <- ages[,c("age_group_id","age_group_name")]

## Load in draws ##
hib_paf <- read.csv("filepath")
hib_paf <- subset(hib_paf, year_id %in% years)
# drop some duplicate column names
hib_paf <- hib_paf[, -which(names(hib_paf) %in% c("age_group_id","sex_id","mean_coverage","mean_paf","std_paf","paf_lower","paf_upper"))]

## Now loop through location_ids, expand for all ages
# and both sexes, make sure age exclusions
# are applied, and save as CSVs ##
source("/filepath/get_location_metadata.R")
locs <- get_location_metadata(location_set_id=9)
locs <- subset(locs, is_estimate==1 & most_detailed==1)
loc_loop <- locs$location_id
n <- 1
for(l in loc_loop){
  print(paste0("On location ", n, " of ",length(loc_loop)))
  df <- expand.grid(age_group_id = ages$age_group_id, year_id=years, location_id=l, sex_id=c(1,2))
  df <- join(df, hib_paf, by=c("location_id","year_id"))

  for(i in 1:1000){
    # If in neonatal period, set to 0
    df[df$age_group_id < 4, paste0("draw_",i)] <- 0

    # If over 5 years, set to 0
    df[df$age_group_id > 5, paste0("draw_",i)] <- 0
  }

  df$cause_id <- 322
  df$rei_id <- 189
  df$modelable_entity_id <- 1266
  setnames(df, paste0("draw_",1:1000), paste0("draw_",0:999))

  write.csv(df, 'filepath')

  n <- n + 1
}
