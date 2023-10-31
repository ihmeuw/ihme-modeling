## Purpose: Combine historic (multi-source) and current (Google-only) time series
## -------------------------------------------------------------------------------

# load libraries
library(data.table)
library(ggplot2)
library(dplyr)
library('ihme.covid', lib.loc = 'FILEPATH')
R.utils::sourceDirectory('functions', modifiedOnly = FALSE)
source("FILEPATH/get_population.R")
source("FILEPATH/get_location_metadata.R")

# specify splice date (shouldn't change)
cutoff_date <- as.Date('2021-04-01') #don't use or impute safegraph, descartes, or fb after this date

# specify locations needing a splice date other than 4/1/21
special_splice_locs <- c('BRA_4771', 'HUN', 'ESP_60357')
special_splice_dates <- c('2021-01-01', '2021-03-01', '2021-02-01')

# define dirs  
root <- "FILEPATH"
output_dir <- ihme.covid::get_latest_output_dir(root = root)
message(paste0("Saving outputs here: ", output_dir))

# load location sets
locs <- fread(file.path(output_dir, "locs.csv"))
locs_for_pub <- fread(file.path(output_dir, "locs_for_pub.csv"))

# load the compare versions for plotting
gpr_versions <- fread(file.path(root, "gpr_versions.csv"))
old_versions <- gpr_versions$gpr_ids
old_versions <- old_versions[length(old_versions):(length(old_versions)-3)]

# Load the data
gpr_dt <- fread(paste0(output_dir,"/forecasted_results.csv"))
hist_dt <- fread(paste0(root, "/smooth_mobility_historic.csv"))

# clean up
gpr_dt <- gpr_dt[, date:=as.Date(date)]
gpr_dt <- gpr_dt[, .(ihme_loc_id, location_id, date, mean, val)]
hist_dt <- hist_dt[, date:=as.Date(date)]

# address locations with missing values of mean on the last day of the historic time series
special_locs <- unique(hist_dt[date=='2021-03-31' & is.na(mean), ihme_loc_id])
special_locs <- special_locs[!grepl('CHN', special_locs)]
special_locs <- special_locs[special_locs != "ITA_35498"]
hist_dt <- hist_dt[ihme_loc_id %in% special_locs & is.na(mean), mean := gpr_mean]
hist_dt <- hist_dt[, .(ihme_loc_id, location_id, date, mean, val)]

# Correct Washington DC identifying information
dc_hist <- hist_dt[ihme_loc_id == 'USA_531',]
dc_hist[,ihme_loc_id := 'USA_891']
dc_hist[,location_id := 891]
hist_dt <- rbind(hist_dt, dc_hist)

# list of locations that used to have Safegraph, Descartes, FB, Baidu
locs_to_splice <- unique(hist_dt$ihme_loc_id)

# remove the special splice locs from this list
locs_to_splice <- setdiff(locs_to_splice, special_splice_locs)

# remove the special splice locs from hist_dt
hist_new <- hist_dt[! ihme_loc_id %in% special_splice_locs]

# remove China provinces
chn_prov <- unique(hist_dt[grepl('CHN', ihme_loc_id)]$ihme_loc_id)
locs_to_splice <- setdiff(locs_to_splice, chn_prov)
hist_new <- hist_new[! ihme_loc_id %in% chn_prov]

# store the recent time series for splice locs
new_dt <- gpr_dt[date>='2021-04-01' & ihme_loc_id %in% locs_to_splice]

# drop full time series for splice locs
gpr_dt <- gpr_dt[! ihme_loc_id %in% locs_to_splice]

# combine data files to make a single time series for spliced locs
spliced_dt <- rbind(hist_new, new_dt)
spliced_dt <- spliced_dt[order(location_id, date)]

## intercept shift

# calculate the amount of shift required for each location
shift_dt <- spliced_dt[date=='2021-03-31' | date=='2021-04-01']
shift_dt <- dcast(shift_dt, ihme_loc_id + location_id ~ date, value.var = "mean")
shift_dt <- shift_dt[, shift := `2021-03-31` - `2021-04-01`]

# add the correction size back to the data
spliced_dt <- merge(spliced_dt, shift_dt[,.(ihme_loc_id, shift)], by='ihme_loc_id')

# apply the shift
spliced_dt <- spliced_dt[date>='2021-04-01', mean := mean + shift]
spliced_dt$shift <- NULL

# finally, add the spliced locations back to the data file
full_dt <- rbind(gpr_dt, spliced_dt)

# Address locations needing custom splice dates
extra_splice <- function(loc_id, splice_date){
  # save the recent time series
  new_tmp <- full_dt[ihme_loc_id==loc_id & date>=as.Date(splice_date)]
  
  # remove all data for the location of interest
  full_dt <- full_dt[ihme_loc_id != loc_id]
  
  # save the historic time series
  hist_tmp <- hist_dt[ihme_loc_id==loc_id & date<as.Date(splice_date)]
  
  # combine past and future
  spliced_tmp <- rbind(hist_tmp, new_tmp)
  spliced_tmp <- spliced_tmp[order(location_id, date)]
  
  # intercept shift
  shift_tmp <- spliced_tmp[date==as.Date(splice_date) - 1 | date==as.Date(splice_date)]
  shift <- shift_tmp[1]$mean - shift_tmp[2]$mean
  spliced_tmp <- spliced_tmp[date>=as.Date(splice_date), mean := mean + shift]
  
  # finally, add the location back to the data file
  full_dt <- rbind(full_dt, spliced_tmp)
  return(full_dt)
}

for (i in 1:length(special_splice_locs)){
  loc <- special_splice_locs[i]
  s.date <- special_splice_dates[i]
  print(paste(loc, s.date))
  full_dt <- extra_splice(loc, s.date)
} 


# fix Provincia autonoma di Bolzano (35498)
# this is the only location that had FB as it's only source
# drop the truncated FB time series and impute the whole time series
# using the national average
pops <- fread('FILEPATH/all_populations.csv')
pops <- pops[age_group_id == 22 & sex_id == 3 & year_id == 2019, .(location_id, population)]

italy_locs <- locs[parent_id==86 & ihme_loc_id!='ITA_35498', ihme_loc_id]
italy <- full_dt[ihme_loc_id %in% italy_locs]
italy <- merge(italy, pops, by="location_id", all.x=T)
italy <- italy[, weighted.mean(mean, population), by="date"] #calc pop-weighted avg
setnames(italy, 'V1', 'mean')
italy$ihme_loc_id <- 'ITA_35498'
italy$location_id <- 35498
italy$val <- NA
italy <- italy[, .(ihme_loc_id, location_id, date, mean, val)]

# drop existing rows for Provincia Bolzano and splice in the new rows
full_dt <- full_dt[ihme_loc_id != 'ITA_35498']
full_dt <- rbind(full_dt, italy)

# aggregate China to "China w/o" Macao IF modeling, or just "China" if not modeling any subnats
# also, fix the national imputation to use the provincial data

china_nat_dt <- full_dt[grepl('CHN', ihme_loc_id)]

if(44533 %in% locs$location_id){
  
  china_dt <- full_dt[grepl('CHN', ihme_loc_id)]
  
  #remove hong kong and macao from dataset as they are modeled seperately
  china_dt <- china_dt[!(location_id %in% c(354, 361, 6)),]
  
  # get populations for china provinces
  china_subnats <- unique(china_dt$location_id)
  china_pops <- get_population(gbd_round_id = 7, decomp_step="iterative",
                               age_group_id = 22, sex_id = 3,
                               year_id = 2019,
                               location_id = china_subnats)
  china_pops <- china_pops[, .(ihme_loc_id = paste0('CHN_',location_id), population)]
  
  # merge on location info to china data
  china_dt <- merge(china_dt, china_pops, by='ihme_loc_id') 
  
  china_dt_agg <- china_dt[, list(mean = weighted.mean(mean, population),
                                        val = weighted.mean(val, population)), by=c("date")] #calc pop-weighted avg
  
  china_dt_agg[,`:=`(ihme_loc_id = 'CHN_44533', location_id = 44533)]
  
  full_dt <- rbind(full_dt, china_dt_agg)
  rm(china_dt)
  rm(china_dt_agg)
}

macao_dt <- full_dt[location_id == 6,]
macao_dt[,`:=`(ihme_loc_id = 'CHN_361', location_id = 361)]


full_dt <- full_dt[location_id != 361,]
full_dt <- rbind(full_dt, macao_dt)

# output final result
fwrite(full_dt, paste0(output_dir,'/smoothed_mobility.csv'))


## Re-make the gpr compare plots post-intercept shift

# read in the fit from previous version(s)
old_data_compare <- data.table()

for(old_version in old_versions){
  
  if(old_version=='2021_05_25.03'){
    tmp_old_data_compare <- fread(paste0(root,"/",old_version,"/forecasted_results.csv"))
  }else{
    tmp_old_data_compare <- fread(paste0(root,"/",old_version,"/smoothed_mobility.csv"))
  }
  tmp_old_data_compare[,date := as.Date(date)]
  tmp_old_data_compare <- tmp_old_data_compare[, .(ihme_loc_id, location_id, date, mean, val)]
  tmp_old_data_compare[, version:=old_version]
  
  old_data_compare <- rbind(old_data_compare, tmp_old_data_compare, fill = T)
  
}

# ready to plot
forecast_plot_path <- paste0(output_dir,"/gpr_compare_v_", paste(old_versions, collapse = "_"),".pdf")

pdf(forecast_plot_path,width=12,height=8)
for(l in unique(full_dt[ihme_loc_id %in% locs_for_pub$ihme_loc_id,ihme_loc_id])){
  message(l)
  
  p <- ggplot(full_dt[ihme_loc_id == l]) +
    geom_point(aes(date,val),alpha=0.5)+
    geom_line(aes(date,mean,color="NEW"))+
    theme_bw()+
    ggtitle(unique(locs[ihme_loc_id == l, location_name]))
  
  if(nrow(old_data_compare[ihme_loc_id == l]) > 0){
    p <- p + geom_line(data=old_data_compare[ihme_loc_id == l],aes(date,mean,color=version),linetype="dashed")
    
  }
  
  print(p)

}
dev.off()

