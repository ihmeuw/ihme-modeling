## Aggregate from location-specific, granular ages to all-location, age- and sex-aggregated results
rm(list=ls())
library(rhdf5); library(data.table)


## Setup filepaths
if (Sys.info()[1]=="Windows") {
  root <- "filepath" 
  user <- Sys.getenv("USERNAME")
  
  type <- "BHS"
} else {
  root <- "filepath"
  user <- Sys.getenv("USER")

  type <- commandArgs()[3]
  year <- as.integer(commandArgs()[4])
  new_upload_version <- commandArgs()[5]
  print(type)
  print(year)
  print(new_upload_version)


  
  parent_dir <- paste0("filepath")
  if(type == "hiv_free") {
  
  } else if(type == "with_hiv") {

  }

}

require("RMySQL")

locations <- data.table(get_locations(level="all"))
locations <- locations[,list(location_id,ihme_loc_id)]

lowest_table <- data.table(get_locations(level="lowest"))
lowest <- lowest_table[,list(location_id)]

aggs <- unique(locations[!location_id %in% unique(lowest[,location_id]),location_id])
sdi_aggs <- data.table(get_locations(gbd_type="sdi", gbd_year = 2015))
sdi_aggs <- unique(sdi_aggs[level==0,location_id])
all_aggs <- c(aggs,sdi_aggs) # This should have all location aggregates that we want to keep

## Provide ID variables, value variables, and dataset -- then aggregate
## Choose the lowest level countries to aggregate
countries <- lowest_table$ihme_loc_id

##Read in data from country-specific hdf files for the specified year
file.paths <- paste0(filepath) 
data <- as.data.table(rbindlist(lapply(file.paths,load_hdf,by_val=year)))

data[,sim:=paste0("env_",sim)]
if(type == "with_hiv") env_varname <- "mx_avg_whiv"
if(type == "hiv_free") env_varname <- "mx_hiv_free"
data <- dcast.data.table(data,location_id+year_id+sex_id+age_group_id+pop~sim,value.var=env_varname)
value_vars <- c(rep(paste0("env_",0:999)),"pop")
id_vars <- c("location_id","year_id","sex_id","age_group_id")

##write out central comp files before agg results
##also make sure we're getting the right locations 
myconn <- dbConnect(RMySQL::MySQL(), host="", username="USERNAME", password="PASSWORD") 
sql_command <- paste0("")
cc_locs <- dbGetQuery(myconn, sql_command)
dbDisconnect(myconn)
locations <- data.table(get_locations(level="all"))
locations <- locations[,list(location_id,ihme_loc_id)]
lowest_locs <- merge(locations, cc_locs, by = c("location_id"))

if (length(setdiff(cc_locs$location_id, data$location_id)) != 0){
  stop(paste0("Lowest locations are not correct ", setdiff(cc_locs, data$location_id)))
}
#Central comp requested hdfs indexed by "draws"
draw_filepath <- paste0(filepath)
file.remove(draw_filepath)
h5createFile(draw_filepath)  
save_hdf(data = data, filepath = draw_filepath, level = 0, groupname = "draws")
H5close()



#aggregate
data <- agg_results(data,id_vars = id_vars, value_vars = value_vars, age_aggs = "gbd_compare", agg_sex = T, loc_scalars=T, agg_sdi=T)

## Remove pop from draws
data[,pop := NULL]

write_summary <- function(data, id_vars){
  ## Collapse to summary statistics
  enve_mean <- apply(data[,.SD,.SDcols=c(rep(paste0("env_",0:999)))],1,mean)
  enve_lower <- apply(data[,.SD,.SDcols=c(rep(paste0("env_",0:999)))],1,quantile,probs=.025,na.rm=T)
  enve_upper <- apply(data[,.SD,.SDcols=c(rep(paste0("env_",0:999)))],1,quantile,probs=.975,na.rm=T)
  data <- data[,.SD,.SDcols=id_vars]
  data <- cbind(data,enve_mean,enve_lower,enve_upper)

  write.csv(data, file = paste0(filepath
#Write out summary file before we melt the data
write_summary(data, id_vars)

#Saving the hdf long by draw
data <- melt(data, id = c("location_id","year_id","sex_id","age_group_id"), variable.name="draw",value.name=env_varname)
data[,draw:=gsub("env_","",draw)]
## Save data to year-specific hdf file, indexed by country
save_locs <- unique(data[,location_id])
filepath <- paste0(filepath)
file.remove(filepath)
h5createFile(filepath)  
lapply(save_locs, save_hdf, data=data, filepath=filepath, by_var="location_id", level = 0)
H5close()




