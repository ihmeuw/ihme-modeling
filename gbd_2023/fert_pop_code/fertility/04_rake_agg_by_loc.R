##############################
## Purpose: Raking
## Details: Rake subnational to national for specified locations
##          Aggregate subnational to national for specified locations
###############################

library(data.table)
library(readr)
library(mortdb)
library(mortcore)
rm(list=ls())

if (interactive()){
  user <- Sys.getenv('USER')
  version_id <- 'Run id'
  gbd_year <- 2023
  year_start <- 1950
  year_end <- 2024
  loop <- 1
  model_age <- 20
  parent <- 'input location'
} else {
  user <- Sys.getenv('USER')
  parser <- argparse::ArgumentParser()
  parser$add_argument('--version_id', type = 'character')
  parser$add_argument('--gbd_year', type = 'integer')
  parser$add_argument('--year_start', type = 'integer')
  parser$add_argument('--year_end', type = 'integer')
  parser$add_argument('--loop', type = 'integer')
  parser$add_argument('--model_age', type = 'integer')
  parser$add_argument('--parent', type = 'character')
  args <- parser$parse_args()
  version_id <- args$version_id
  gbd_year <- args$gbd_year
  year_start <- args$year_start
  year_end <- args$year_end
  loop <- args$loop
  model_age <- args$model_age
  parent <- args$parent
}

input_dir <- 'FILEPATH'
gpr_dir <- 'FILEPATH'
output_dir <- 'FILEPATH'
loop1_dir <- 'FILEPATH'

## Read in inputs
loc_map <- fread(paste0(loop1_dir, 'loc_map.csv'))
age_map <- fread(paste0(loop1_dir, 'age_map.csv'))
pop <- fread(paste0(loop1_dir, 'input_pop.csv'))[sex_id == 2]
model_locs <- fread(paste0(loop1_dir, 'model_locs.csv'))

## Assign which location to rake and which to aggregate 
parent_locs <- unique(loc_map[grepl('_', ihme_loc_id), gsub('_.*', '', ihme_loc_id)])
agg_locs <- c("UKR")
rake_locs <- parent_locs[!parent_locs %in% agg_locs]

## Get parent & child data
children <- model_locs[grepl(parent, ihme_loc_id), ihme_loc_id]
children <- children[!children %in%  c('CHN_354', 'CHN_361', '')]
child_data <- assertable::import_files(paste0(gpr_dir, 'gpr_', children, '_', model_age, '_sim.csv'), FUN=fread)
child_data[, year_id := floor(year)]
child_data[, age := model_age]
child_data[, age_group_id := age_map[age_group_years_start == model_age & age_group_id %in% 7:15, age_group_id]]

child_data <- merge(child_data, pop, by=c('age_group_id', 'ihme_loc_id', 'year_id'), all.x=T)

## Rake in births space
child_data[, births := fert * pop]

## Rake or aggregate
if (parent == 'CHN') {
  raked <- mortcore::scale_results(child_data, id_vars = c('location_id', 'year_id', 'age_group_id', 'sim'), value_var = 'births', 
                                   location_set_id = 21, gbd_year = gbd_year, exclude_parent = 'CHN')
} else if (parent == 'KEN') {
  locs_44797 <- c('KEN_44797', loc_map[parent_id == 44797, ihme_loc_id])
  raked <- mortcore::scale_results(child_data, id_vars = c('location_id', 'year_id', 'age_group_id', 'sim'), value_var = 'births', 
                                   location_set_id = 21, gbd_year = gbd_year)
  raked_44797 <- mortcore::scale_results(child_data[ihme_loc_id %in% locs_44797],
                                         id_vars = c('location_id', 'year_id', 'age_group_id', 'sim'), value_var = 'births', 
                                         location_set_id = 21, gbd_year = gbd_year, exclude_parent = 'KEN')
  raked <- rbind(raked[!ihme_loc_id %in% locs_44797], raked_44797)
  
} else if (parent == "GBR") {
  
  print("GBR: mix of agg and raking")
  
  child_data_pre74 <- child_data[year_id < 1974]
  child_data_pre74 <- merge(
    child_data_pre74, 
    model_locs[, .(location_id, level)], 
    all.x = TRUE, 
    by = "location_id"
  )
  
  # replace GBR national
  child_data_pre74 <- child_data_pre74[level == 4][, .(location_id, age_group_id, sim, year_id, births)]
  
  agg_pre74 <- hierarchyUtils::agg(
    dt = child_data_pre74,
    id_cols = c("location_id", "age_group_id", "sim", "year_id"),
    value_cols = "births",
    col_stem = "location_id",
    col_type = "categorical",
    mapping = data.table(
      child = model_locs[grepl("GBR", ihme_loc_id) & level == 4, location_id],
      parent = 95
    )
  )
  setnames(agg_pre74, "births", "agg_births")
  child_data <- merge(
    child_data,
    agg_pre74,
    by = c("location_id", "age_group_id", "sim", "year_id"),
    all.x = TRUE
  )
  child_data[!is.na(agg_births), births := agg_births]
  child_data[, agg_births := NULL]
  
  # rake with updated national
  raked <- mortcore::scale_results(
    child_data, 
    id_vars = c('location_id', 'year_id', 'age_group_id', 'sim'), 
    value_var = 'births', 
    location_set_id = 21, gbd_year = gbd_year
  )
  
} else if (parent %in% rake_locs) {
  raked <- mortcore::scale_results(child_data, id_vars = c('location_id', 'year_id', 'age_group_id', 'sim'), value_var = 'births', 
                                   location_set_id = 21, gbd_year = gbd_year)
} else if (parent %in% agg_locs) {
  raked <- mortcore::agg_results(child_data[, c('location_id', 'year_id', 'age_group_id', 'sim', 'births')], 
                                 id_vars = c('location_id', 'year_id', 'age_group_id', 'sim'), value_vars = 'births', 
                                 loc_scalars = F, agg_sdi = F, end_agg_level = 3, tree_only = T, gbd_year = gbd_year)
  
  child_data[, births := NULL]
  raked <- merge(raked, child_data, 
                 by = c('location_id', 'year_id', 'age_group_id', 'sim'),
                 all.x = TRUE)
  
} else {
  stop('Not a parent location')
}

raked[, fert := births/pop]
raked[, age := model_age]
raked[, year := year_id + 0.5]

## Collapse draws
summary <- copy(raked)

setkey(summary, ihme_loc_id, year, age)
summary <- summary[,.(mean = mean(fert),lower=quantile(fert,probs=c(.025)),upper = quantile(fert,probs=c(.975))),by=key(summary)]

## Write draw files
for(loc in unique(raked$ihme_loc_id)){
  temp <- raked[ihme_loc_id == loc, .(ihme_loc_id, sim, year, fert, age)]
  readr::write_csv(temp, paste0(output_dir, "gpr_", loc, "_", model_age, "_sim.csv"))
}

## Write summary level files
for(loc in unique(summary$ihme_loc_id)){
  temp <- summary[ihme_loc_id == loc]
  readr::write_csv(temp, paste0(output_dir, "gpr_", loc, "_", model_age, ".csv"))
}


