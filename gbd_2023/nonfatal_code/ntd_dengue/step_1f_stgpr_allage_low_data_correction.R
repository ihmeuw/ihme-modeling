################################################################################
### for GBD 2023 dengue. For final stgpr model we decided to run it twice,
### one with the outbreak covariate and one without
### for the final results, we pull both and for locations where we have <=5
### data points (grouping by location and year) we want to use the results from
### the model without the dengue outbreak covariate. All other locations will
### use results from the model including the outbreak covariate
### NOTE: we're not replacing only some years, we're replacing the full set of
### estimates
### This step takes place right before the final age-splitting.
################################################################################
# clear
rm(list=ls())

# load libraries and source functions
library(dplyr)
library(data.table)
library(ggplot2)
library(foreign)
library(reshape2)
library(reshape)
library(gtools)
library(tidyr)
library(readxl)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_crosswalk_version.R")
source('FILEPATH/api/public.R')
source("FILEPATH/get_population.R")
source("FILEPATH/get_model_results.R")

# other functions
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
}

################################################################################
# set params
release_id <- ADDRESS
cw_version <- ADDRESS
mv1 <- ADDRESS # model version for the final stgpr model run WITH the outbreak cov
mv2 <- ADDRESS # model version for the final stgpr model run WITHOUT the outbreak cov
run_date <- ADDRESS

# filepaths
path <- paste0("FILEPATH")
param_path <- paste0(path, "FILEPATH")
outpath <- paste0(path,"FILEPATH")
dir.create(outpath, recursive = T, showWarnings = F)

inpath1 <- paste0("FILEPATH")
inpath2 <- paste0("FILEPATH")
################################################################################
# pull useful reference files
locs <- get_location_metadata(release_id = release_id, location_set_id = 35)
loc_sub <- locs[,.(location_id, level, parent_id, ihme_loc_id, most_detailed)]

nats <- loc_sub[loc_sub$level ==3,]
subs <- loc_sub[loc_sub$most_detailed == 1 & !(loc_sub$level ==3),]
loc_sub <- rbind(nats, subs)

loc_sub$parent_id <- ifelse((loc_sub$ihme_loc_id %like% "KEN"), 180, loc_sub$parent_id)
loc_sub$ihme_loc_id <- NULL
locs <- locs[,.(location_id, location_name)]

ages <- get_age_metadata(release_id = release_id)

######
end_locs <- fread(paste0(j, 'Project/NTDS/ntd_grs/ntd_grs/data/grs_long/release_id_',release_id,'/gr_ntd_dengue_lgr.csv'))
end_locs <- end_locs[ ,end_overall:=sum(value_endemicity), by=c("location_id", "location_name")]

end_locs <- end_locs[,.(location_id, super_region_id, super_region_name, location_name, ihme_loc_id, level,end_overall)]

end_locs <- end_locs %>% group_by(location_id, location_name) %>% unique()
end_locs <- as.data.table(end_locs)
endem_locs <- end_locs[end_locs$end_overall >0,]
################################################################################
#### begin selection
# pull the final cw version
bundle <- get_crosswalk_version(cw_version)

# get number of data points by location and year 
bundle <- subset(bundle, is_outlier ==0)
#bundle <- merge(bundle, locs, by = c("location_id", "location_name"), all.x=T)
#bundle <- subset(bundle, level >=3)
bundle[, n:=1]
bundle[sex=="Male", sex_id :=1]
bundle[sex=="Female", sex_id :=2]

# need to get a list of endemic locations where we don't have any data
zero_data <- endem_locs[!(endem_locs$location_id %in% bundle$location_id),]
zero_data <- left_join(zero_data, loc_sub, by = c("level", "location_id"))
zero_data <- zero_data[!(is.na(zero_data$parent_id)),]
zero_data <- zero_data[zero_data$level >= 3,]
zero_sub <- zero_data[zero_data$level >3,]

bundle_sum <- bundle[,n_studies := sum(n), by = c("location_id", "sex")]

# subset to locations where there are less than 5 data points
low_data_bundle <- subset(bundle_sum, n_studies <=5)

# for subnats where the national loc has >5 data sources, don't count them as low data
low_data_bundle <- left_join(low_data_bundle, loc_sub, by = "location_id")

low_subnats <- low_data_bundle[low_data_bundle$level >3,]

dont_replace <- low_subnats[!(low_subnats$parent_id %in% low_data_bundle$location_id),]

dont_replace_zero_sub <- zero_sub[!(zero_sub$parent_id %in% low_data_bundle$location_id),]
dont_replace_zero_sub <- dont_replace_zero_sub[!(dont_replace_zero_sub$parent_id %in% zero_data$location_id),]

low_data_bundle <- low_data_bundle[!(low_data_bundle$location_id %in% dont_replace$location_id),]
zero_data <- zero_data[!(zero_data$location_id %in% dont_replace_zero_sub$location_id),]
zero_data <- zero_data[!(zero_data$location_id %in% dont_replace$location_id),]

low_data_bundle <- low_data_bundle[!(low_data_bundle$ihme_loc_id %like% "IND"),]
zero_data <- zero_data[!(zero_data$ihme_loc_id %like% "IND"),]

write.csv(low_data_bundle, paste0("FILEPATH"), row.names = FALSE)
write.csv(zero_data, paste0("FILEPATH"), row.names = FALSE)

low_data_locs <- unique(low_data_bundle$location_id)
zero_data_locs <- unique(zero_data$location_id)

locs_to_replace <- c(low_data_locs, zero_data_locs)

# save out a file listing all locations that were corrected, date run, etc.
params <- rbind(c("Date and time script was run:", as.character(Sys.time())),
                c("release_id:", release_id),
                c("Bundle cw version pulled:", cw_version),
                c("stgpr model with outbreak cov:", mv1),
                c("stgpr model without outbreak cov:", mv2),
                c("List of locations being adjusted: ", locs_to_replace))
write.table(params, file = paste0("FILEPATH"), row.names=FALSE,col.names=FALSE, quote=FALSE)

# first save out mv1 (with outbreak cov) to a new folder (don't want to overwrite original files)
files <- list.files(FILEPATH, pattern = ".csv", full.names = TRUE)
temp <- lapply(files, fread, sep=",")
df <- rbindlist(temp,use.names=TRUE)
rm(temp)

i <- 1
for (x in unique(df$location_id)){
  message(paste0("Now processing: ", x, "; ", i, "/", length(unique(df$location_id))))
  
  sub <- df[df$location_id==x,]
  
  # save out
  fwrite(sub, paste0("FILEPATH"))
  
  i <- i+1
}
rm(df,sub)

i <- 1
for (x in unique(locs_to_replace)){
  message(paste0("Now processing: ", x, "; ", i, "/", length(locs_to_replace)))
  
  df <- fread(paste0("FILEPATH"))

  # save out
  fwrite(df, paste0("FILEPATH"))

  i <- i+1
}

message("End of script")
################################################################################