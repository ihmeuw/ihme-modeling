## Purpose: Fill and backcast missing dates
## ------------------------------------------

# Load libraries
library(dplyr)
library(zoo)
library(forecast)
library(splines)
library('tseries')
library(msm)
library(ggplot2)
library(data.table)
library('ihme.covid', lib.loc = 'FILEPATH')
source("FILEPATH/get_population.R")
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)

# Set permissions
Sys.umask('0002')

# Define args if interactive. Parse args otherwise.
if (interactive()){
  code_dir <- "FILEPATH"
} else {
  parser <- argparse::ArgumentParser()
  parser$add_argument("--code_dir", type = "character", help = "Root of repository.")

  args <- parser$parse_args(get_args())
  for (key in names(args)) {
    assign(key, args[[key]])
  }
  
  print("Arguments passed were:")
  for (arg in names(args)){
    print(paste0(arg, ": ", get(arg)))
  }
  
}

# Define additional args
plot_bool <- FALSE # if you want to plot along the way

# Define dirs
output_dir <- "FILEPATH" 
today <- (Sys.Date()) %>% gsub('-', '_', .)
run_index <- ihme.covid::get_latest_output_date_index(output_dir, today)
output_subdir <- file.path(output_dir, sprintf("%s.%02i", today, run_index))
metadata_path <- file.path(output_subdir, "04_fill_and_backcast.metadata.yaml")
message(paste0("Saving outpus here: ", output_subdir))

# Load location sets
locs <- fread(file.path(output_subdir, "locs.csv"))
locs_for_pub <- fread(file.path(output_subdir, "locs_for_pub.csv"))

source(paste0(code_dir,"FILEPATH/custom_functions.R"))

# Load data
updated_avg_post_outliering_path <- paste0(output_subdir,"/prepped_final_moving_avg.csv")
raw_data <- fread(updated_avg_post_outliering_path)
raw_data[, date:= as.Date(date)]

all_gpr <- fread(file.path(output_subdir, "all_gpr.csv"))
all_gpr[, date:= as.Date(date)]

map_years_ref <- fread(file.path(output_subdir, "map_years_ref.csv"))
map_years_ref[, date:= as.Date(date)]

gpr_versions <- fread(file.path(output_dir, "gpr_versions.csv"))
old_versions <- gpr_versions$gpr_ids
old_versions <- old_versions[length(old_versions):(length(old_versions)-3)]


# Backcast and fill missing data
all_forecasted <- forecast_loop(all_gpr)
  
# make regional averages
locs_missing <- setdiff(locs$ihme_loc_id,all_forecasted$ihme_loc_id)
locs_missing
locs_gbd <- get_location_metadata(22,gbd_round_id=7, decomp_step='iterative')
  
# first deal with the subnationals
sub_missing_fill <- data.table()
temp_subnat <- data.table()
for(l in locs_missing[locs_missing %like% "_"]){
  nat_level <- strsplit(l,"_")[[1]][1]
    
  temp_subnat <- all_forecasted[ihme_loc_id == nat_level]
  temp_subnat[,ihme_loc_id := l]
  sub_missing_fill <- rbind(sub_missing_fill,temp_subnat)
}
sub_missing_fill[, version:=paste(version, "(national)")]
  
# add onto dataset
all_forecasted <- rbind(all_forecasted,sub_missing_fill)
  
# then deal with the national level; assumes that this is a GBD location
all_forecasted <- merge(all_forecasted,locs_gbd[,.(ihme_loc_id,region_id,level, parent_id)],by="ihme_loc_id",all.x=T)
locs_missing_dt <- as.data.table(locs_missing)
locs_missing_dt <- merge(locs_missing_dt,locs_gbd[,.(ihme_loc_id,level,region_id)],by.x="locs_missing",by.y="ihme_loc_id",all.x=T)
locs_missing_dt_nat <- locs_missing_dt[level == 3 & !is.na(level)]

nat_filled <- data.table()
for(i in 1:nrow(locs_missing_dt_nat)){
  # for china, take the pop-weighted avg of the provincial mobility
  if(locs_missing_dt_nat[i, locs_missing]=='CHN'){
    #produce a list of population size for each province of china
    china_subnats <- locs_gbd[parent_id==44533|location_id == 354, .(ihme_loc_id, location_id)]
    china_pops <- get_population(gbd_round_id = 7, decomp_step="iterative",
                                 age_group_id = 22, sex_id = 3,
                                 year_id = 2019,
                                 location_id = china_subnats$location_id)
    china_pops <- china_pops[, .(ihme_loc_id = paste0('CHN_',location_id), population)]

    other_nat_reg <- all_forecasted[parent_id==44533|ihme_loc_id == 'CHN_354',] #subset dt to chinese subnats
    other_nat_reg <- merge(other_nat_reg, china_pops, by='ihme_loc_id') #tack on population
    
    other_nat_reg <- other_nat_reg[, province_mean := weighted.mean(mean, population), by="year_id"] #calc pop-weighted avg
    other_nat_reg <- unique(other_nat_reg[!is.na(province_mean),.(province_mean, year_id)]) #one row per year_id
    setnames(other_nat_reg,"province_mean", "mean")
    other_nat_reg[, ihme_loc_id := 'CHN']
    other_nat_reg[, version:="(subnational)"]
    other_nat_reg <- other_nat_reg[,.(mean, year_id, ihme_loc_id, version)]
    nat_filled <- rbind(nat_filled,other_nat_reg)
  } else{ # for all other countries, use the regional average
    nat_loc <- locs_missing_dt_nat[i,locs_missing]
    other_nat_reg <- all_forecasted[region_id == locs_missing_dt_nat[i,region_id] & level == 3]
    
    # if the region is Central Asia (region_id 32), don't include Mongolia in the calculation
    if(locs_missing_dt_nat[i]$region_id==32){
      other_nat_reg <- other_nat_reg[ihme_loc_id != 'MNG']
    }
    
    # only average to minimum max date of any country in the region
    max_dates_reg <- other_nat_reg[,max_year := max(year_id), by=ihme_loc_id]
    max_year_reg <- min(max_dates_reg$max_year)
  
    other_nat_reg[year_id <= max_year_reg, region_mean := mean(mean),by="year_id"]
    other_nat_reg <- unique(other_nat_reg[!is.na(region_mean),.(region_mean,year_id)])
    setnames(other_nat_reg,"region_mean","mean")
    other_nat_reg[,ihme_loc_id := nat_loc]
    other_nat_reg[, version:="(regional)"]
    nat_filled <- rbind(nat_filled,other_nat_reg)
  }
    
}
  
# combine averages
all_forecasted <- rbind(all_forecasted,nat_filled,fill=T)

all_forecasted <- merge(all_forecasted,map_years_ref,by="year_id")
all_forecasted[,date := as.Date(date)]
all_gpr_for <- merge(all_gpr,all_forecasted,by=c("ihme_loc_id","year_id","date"),all=T)

forecast_results_path <- paste0(output_subdir,"/forecasted_results.csv")
  
## check if the dataset is square
all_data_permutations_forecast <- as.data.table(expand.grid(ihme_loc_id = unique(all_gpr_for$ihme_loc_id),
                                                              year_id = seq(min(map_years_ref$year_id),max(map_years_ref$year_id),by=1)))
all_data_permutations_forecast$complete <- 1
all_gpr_for_test <- merge(all_gpr_for[!is.na(mean)],all_data_permutations_forecast,by=c("ihme_loc_id","year_id"),all=T)
  
if(nrow(all_gpr_for_test[is.na(complete)]) > 0){
  stop("DATASET IS NOT SQUARE")
}
  
all_gpr_for$location_id <- NULL
all_gpr_for <- merge(all_gpr_for,unique(locs[,.(ihme_loc_id,location_id)]),by="ihme_loc_id",all.x=T)
write.csv(all_gpr_for, forecast_results_path, row.names=F)
  

# GPR comparison plot
if(plot_bool){
  
  # prep the data file
  old_data_compare <- data.table()
  for(old_version in old_versions){
    tmp_old_data_compare <- fread(paste0(output_dir,"/",old_version,"/forecasted_results.csv"))
    tmp_old_data_compare[,date := as.Date(date)]
    colnames(tmp_old_data_compare) <- make.unique(names(tmp_old_data_compare))
    tmp_old_data_compare[, version:=old_version]
    old_data_compare <- rbind(old_data_compare, tmp_old_data_compare, fill = T)
  }
  
  # make the plot
  forecast_plot_path <- paste0(output_subdir,"/forecasted_data_plots_v_", paste(old_versions, collapse = "_"),".pdf")
  pdf(forecast_plot_path,width=12,height=8)
  
  for(l in unique(all_gpr_for[ihme_loc_id %in% locs_for_pub$ihme_loc_id,ihme_loc_id])){
    message(l)
      
    p <- ggplot(all_gpr_for[ihme_loc_id == l])+geom_point(aes(date,val),alpha=0.5)+
      geom_line(aes(date,mean,color="NEW"))+
      theme_bw()+
      ggtitle(unique(locs[ihme_loc_id == l, location_name]))
      
    if(nrow(raw_data[ihme_loc_id == l]) > 0){
      p <- p + geom_point(data=raw_data[ihme_loc_id == l],aes(date,val,color="imputed/raw data"),shape=4)
    }
      
    if(nrow(old_data_compare[ihme_loc_id == l]) > 0){
      p <- p + geom_line(data=old_data_compare[ihme_loc_id == l],aes(date,mean,color=version),linetype="dashed")
    }
      
    print(p)
      
  }
  
  dev.off()
}
  
# missing locs
setnames(locs_missing_dt, "locs_missing", "ihme_loc_id")
locs_mising_dt <- merge(locs_missing_dt, locs[,.(ihme_loc_id, location_name, location_id)], by="ihme_loc_id", all.x=T)
write.csv(locs_missing_dt, file.path(output_subdir, "imputed_locations.csv"), row.names = F)


yaml::write_yaml(
  list(
    script = "04_fill_and_backcast.R",
    output_dir = output_subdir,
    input_files = ihme.covid::get.input.files()
  ),
  file = metadata_path
)
