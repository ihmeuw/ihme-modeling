#Prep IFR-age by each location needed for gbd outputs

prep_ifr_gbd_locs <- function(age_rates_dir, hierarchy, output_dir){
  
  nursing_home_locs <- fread(paste0(age_rates_dir, '/nursing_home_location_ids.csv'))$location_id
  
  #ifr preds
  preds_5yr <- fread(file.path(age_rates_dir, "ifr_preds_5yr_3set.csv"))
  hi <- preds_5yr[custom_hierarchy=='high_income']
  nh <- preds_5yr[custom_hierarchy=='nursing_homes']
  oth <- preds_5yr[custom_hierarchy=='all_other']
  
  #add hierarchy info to loc meta
  df_locmeta <- hierarchy
  df_locmeta[location_id %in% nursing_home_locs, custom_hierarchy:='nursing_homes']
  df_locmeta[super_region_name=='High-income' & !location_id %in% nursing_home_locs, custom_hierarchy:='high_income']
  df_locmeta[super_region_name!='High-income' & !location_id %in% nursing_home_locs, custom_hierarchy:='all_other']
  
  #fill in respective ifr preds
  all_preds_5yr <- data.table()
  for (l in unique(df_locmeta[level>=3]$location_id)){
    print(l)
    ch <- df_locmeta[location_id==l]$custom_hierarchy
    preds <- preds_5yr[custom_hierarchy==ch]
    preds[, location_id:=paste0(l)]
    all_preds_5yr <- rbind(all_preds_5yr, preds)
  }
  
  #add global 
  preds_5yr_global <- fread(file.path(age_rates_dir, 'ifr_preds_5yr_global.csv'))
  preds_5yr_global$location_id <- 1
  preds_5yr_global$custom_hierarchy <- 'global'
  
  all_preds_5yr <- rbind(all_preds_5yr, preds_5yr_global)
  write.csv(all_preds_5yr, paste0(output_dir, '/ifr_preds_5yr_byloc.csv'), row.names=F)
  
}

