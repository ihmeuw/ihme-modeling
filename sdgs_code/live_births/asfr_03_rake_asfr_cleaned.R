#############################
## Rakes ASFR Down Location Hierarchy
############################

### Settings ###

rm(list=ls())


if (!require("pacman")) install.packages("pacman")
pacman::p_load(data.table, magrittr, parallel) 

mod_id <- commandArgs()[3]
cores <- as.numeric(commandArgs()[4])


root <- "FILEPATH"
dir <- "FILEPATH"
setwd(dir)

tfr_draws_dir <- "FILEPATH"
asfr_scaled_draws_dir <- "FILEPATH"
asfr_raked_draws_dir <- "FILEPATH"
if(!dir.exists("FILEPATH")) dir.create("FILEPATH")


source("FILEPATH") # load get_locations
codes <- get_locations(2016) %>% as.data.table
merge.codes <- codes[, .(ihme_loc_id, location_id)]

setwd("FILEPATH")
source("FILEPATH")
source("FILEPATH")

aggregate_ids <- 6 


simple_rake <- function(df, lvl) {
    
    draws <- grep("[[:digit:]]", names(df), value = T)
    
    parent <- copy(df)
    parent[, parent_mean := rowMeans(parent[, (draws), with = F])]
    parent_sum <- parent[, .(parent_total = sum(population*parent_mean)), by = .(location_id, year_id, age_group_id)]
    setnames(parent_sum, "location_id", "parent_id")
    df <- merge(df, parent_sum, by = c("parent_id", "year_id", "age_group_id"), all.x = T)
    
    df[, child_mean := rowMeans(df[, (draws), with = F])]
    df[level == lvl, child_total := sum(population*child_mean), by = .(parent_id, year_id, age_group_id)]
    df[level == lvl, rake_factor := parent_total/child_total]
    df[level == lvl & !is.na(rake_factor), (draws):= lapply(.SD, function(x) rake_factor*x), .SDcols = draws] #taking care of locs that need to be aggregated
    df[, c("parent_total", "child_mean", "child_total", "rake_factor") := NULL]
    
    return(df)
}

simple_aggregate <- function(df, agg_id) {
    
    par_id <- codes[location_id == agg_id, parent_id]
    draws <- grep("[[:digit:]]", names(df), value = T)
    
    parent_series <- df[parent_id == agg_id, lapply(.SD, function(x) sum(x*population)/sum(population)), by = .(year_id, age_group_id, parent_id), .SDcols = draws]
    setnames(parent_series, "parent_id", "location_id")
    
    df <- rbindlist(list(parent_series, df[, c("location_id", "year_id", "age_group_id", draws), with = F]), use.names = T)
    
    
}

## Read and Combine Data for All locations
setwd(paste0(asfr_scaled_draws_dir, "/", mod_id))
combined <- list.files(getwd()) %>% mclapply(., fread, mc.cores = cores) %>% rbindlist

combined <- combined[!location_id %in% aggregate_ids]

draws <- grep("[[:digit:]]", names(combined), value = T)

for (lvl in c(4,5,6)) {
    
    combined <- simple_rake(combined, lvl)
}

for (id in aggregate_ids) {
    
    combined <- simple_aggregate(combined, id)
}


## Clean and Save
final <- combined[, c("location_id", "year_id", "age_group_id", draws), with = F]

#Final Check
stopifnot(nrow(final) == 354095)

mclapply(codes[, unique(location_id)], function(x) write.csv(final[location_id == x], "FILEPATH", row.names = F))



