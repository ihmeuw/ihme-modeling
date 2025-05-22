library(data.table)

bundle_map <- fread(file.path(getwd(), 'model_prep/param_maps/bundle_map.csv'))

elmo_df <- data.table()
for(i in bundle_map$id){
  df <- ihme::get_elmo_ids(bundle_id = i)
  for(c in colnames(df)){
    df[[c]] <- as.character(df[[c]])
  }
  elmo_df <- rbindlist(list(elmo_df, df), use.names = TRUE, fill = TRUE)
}

copy_elmo <- copy(elmo_df)

elmo_df$bundle_version_date <- as.Date(elmo_df$bundle_version_date)
elmo_df <- elmo_df[bundle_version_date > as.Date("2024-01-01") & is.na(crosswalk_version_id)]

source("FILEPATH/prune_version.R")

for(i in unique(elmo_df$bundle_id)){
  temp_df <- elmo_df[bundle_id == i]
  prune_version(bundle_id = i, bundle_version_id = temp_df$bundle_version_id)  
}
