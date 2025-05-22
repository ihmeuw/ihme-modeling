#############################################################
# Purpose:Prep CF inputs for modeling -
#     -Combine data sources
#     -Aggregate to the appropriate level
#     -Other modeling prep
#############################################################

# Get settings / controls ####
source("~/00a_prep_setup.R")

# Set up write folder ####
setwd("FILEPATH")
write_folder = paste0("FILEPATH") 
if (!dir.exists(write_folder)) dir.create(write_folder)

read_folder = paste0("FILEPATH")
read_folder2 = paste0("FILEPATH")

# read in the data that was already prepped up to this point:
df_list = lapply(list('twn', 'pol', 'ms', 'hosp'), function(x){
  files = list.files(paste0(read_folder, x, '/'))
  files = files[!grepl('fileloading.csv', files)]
  data = lapply(files, function(file){
    fread(paste0(read_folder, x, '/', file))
  })
  data = rbindlist(data)
  return(data)
})
df_list2 = lapply(list('cms', 'hosp'), function(x){
  files = list.files(paste0(read_folder2, x, '/'))
  files = files[!grepl('fileloading.csv', files)]
  data = lapply(files, function(file){
    fread(paste0(read_folder2, x, '/', file))
  })
  data = rbindlist(data)
  return(data)
})

df1 = rbindlist(df_list, use.names = TRUE, fill = TRUE)
df2 = rbindlist(df_list2, use.names = TRUE, fill = TRUE)
df = rbind(df1, df2)

setnames(df, 'val', 'count')

# Neonatal collapser function ####
df = neonatal_collapser(df)

# Apply age-sex restrictions ####
# This is needed again after neonatal_collapser function at current
rests = get_bundle_restrictions() %>% as.data.table()
rests = melt(rests, id.vars = c('bundle_id','yld_age_start','yld_age_end','map_version'), variable.name = 'sex', value.name = 'sex_present')
rests[sex == 'male', sex_id := 1][sex == 'female', sex_id := 2]
df = merge(df, rests, by = c('bundle_id','sex_id'))
check = df[sex_present == 0, ]
df = df[sex_present != 0]
check2 = df[age_start < yld_age_start]
df = df[age_start >= yld_age_start]

df[, rm := 0]
df[(yld_age_end >= 1) & (age_start > yld_age_end), rm := 1] 
check3 = df[rm == 1,]
df = df[rm != 1]
# Special handling of age max for Taiwan <1 age data ####
df[(yld_age_end < 1) & age_start >= 1, rm := 1] 
check4 = df[rm == 1,]
df = df[rm != 1]
df[, rm := NULL]
df[age_midpoint == 110, age_midpoint := 97.5]

# save documentation of data removed
if (!dir.exists(paste0(write_folder, "FILEPATH"))) dir.create(paste0(write_folder, "FILEPATH"))
write.csv(check, paste0(write_folder, "FILEPATH"), row.names = FALSE)
write.csv(check2, paste0(write_folder, "FILEPATH"), row.names = FALSE)
write.csv(check3, paste0(write_folder, "FILEPATH"), row.names = FALSE)

# Create source location column ####
if(agg_over_subnational==FALSE){
  df[, s_loc := paste0(source,location_id)]
}

df[, c('yld_age_start', 'yld_age_end', 'map_version', 'sex_present'):=NULL]

# Save CF inputs ####
fwrite(df, paste0(write_folder, "FILEPATH"))