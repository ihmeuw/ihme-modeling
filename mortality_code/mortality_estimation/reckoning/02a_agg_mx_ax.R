## Aggregate from location-specific, granular ages to all-location, age- and sex-aggregated results

#Parallel by year
rm(list=ls())
library(data.table)

## Setup filepaths
if (Sys.info()[1]=="Windows") {
  root <- "filepath" 
  user <- Sys.getenv("USERNAME")
  
  type <- "hiv_free"
} else {
  root <- "filepath"
  user <- Sys.getenv("USER")
  
  type <- commandArgs()[3]
  year <- as.integer(commandArgs()[4])
  new_upload_version <- commandArgs()[5]

}
print(type)
print(year)


location_table <- data.table(get_locations(level="lowest"))
est_locations <- data.table(get_locations(level="estimate"))
est_locations <- est_locations[is_estimate == 1,]
loc_names <- location_table[,list(ihme_loc_id)]

parent_locs <- est_locations[location_id %in% unique(location_table[,parent_id]),ihme_loc_id] # Get all parent locations but not non-estimate countries (England, China national)
parent_locs <- c(parent_locs,"IND") ## Add India National here
parent_locs <- parent_locs[!(parent_locs %in% c("ZAF"))] # Take out ZAF (ZAF not re-agg'ed)
locations <- location_table[,list(location_id)]

region_locs <- data.table(get_locations(level="all"))
region_locs <- unique(region_locs[level<=2,location_id])

age_map <- data.table(get_age_map())
age_map <- age_map[age_group_id %in% c(5,28),list(age_group_id)]
age_map[age_group_id==5,age:=1]
age_map[age_group_id==28,age:=0]

## Bring in data, convert mx to deaths, weight ax by deaths
# Bring in year indices from location-specific files
filepaths <- paste0(filepath)
data <- rbindlist(lapply(filepaths,load_hdf,by_val=year))
data[,location_id:=as.integer(location_id)]

## Bring in population for all of the locations of interest
population <- data.table(get_population(status = "recent", location_id = unique(data$location_id), year_id = year, sex_id = c(1:3), age_group_id = c(unique(data$age_group_id), 235)))
setnames(population, c("population"), c("pop"))
population <- population[ ,list(location_id,year_id,sex_id,age_group_id,pop)]

## For all population groups, fill it in with 95+ population
lt_granular_95_groups <- c(33,44,45,148)

over_95_pops <- population[age_group_id==235,]
over_95_pops[,age_group_id:=NULL]
map <- data.table(expand.grid(age_group_id=lt_granular_95_groups,sex_id=unique(over_95_pops[,sex_id]),
                              location_id=unique(over_95_pops[,location_id]),year_id=unique(over_95_pops[,year_id])))
over_95_pops <- merge(over_95_pops,map,by=c("sex_id","year_id","location_id"))
population <- population[!age_group_id %in% c(235,lt_granular_95_groups),]
population <- rbindlist(list(population,over_95_pops),use.names=T)

if(type == "with_hiv") {
  print("Bringing in national-level u5 mx/ax draws")
  natl_locs <- data.table(get_locations(level="all"))
  natl_locs <- natl_locs[ihme_loc_id %in% parent_locs,list(location_id,ihme_loc_id)]

  get_mx_ax <- function(country) {
    data <- data.table(fread(paste0(filepath)))
    data <- data[(age %in% c(0,1)) ,list(age,sex,draw,year,mx,ax)]
    data[,location_id:=unique(natl_locs[ihme_loc_id==country,location_id])]
  }
  
  gen_natl_mx_ax <- function() {
    natl_mx_ax <- data.table(rbindlist(lapply(parent_locs,function(x) get_mx_ax(x))))
    natl_mx_ax <- merge(natl_mx_ax,age_map,by=c("age"))
    natl_mx_ax[,age:=NULL]
    setnames(natl_mx_ax,"year","year_id")
    setnames(natl_mx_ax,"sex","sex_id")
  } 
  
  u5_mx_ax <- gen_natl_mx_ax()
  data <- rbindlist(list(data,u5_mx_ax),use.names=T)
  data[,sex_id := as.integer(sex_id)]
  # age_group_id, sex_id, year_id, draw, location_id, mx, ax
}

data <- merge(data,population,by=c("year_id","sex_id","location_id","age_group_id"))
data[,mx:=mx*pop]
data[,ax:=ax*mx]

## Provide ID variables and value variables, then aggregate
## Aggregation skips locations if they already exist in the dataset, so if it's with-HIV,
## we run under-5 separately to retain the national-level mx/ax
## And then run 5+ age group separately to avoid this issue
value_vars <- c("mx","ax","pop")
id_vars <- c("location_id","year_id","sex_id","age_group_id","draw")
if(type == "with_hiv") {
  data_u5 <- agg_results(data[age_group_id %in% c(5,28),],id_vars = id_vars, value_vars = value_vars, agg_sex = F, loc_scalars=T, agg_sdi=T)
  data_o5 <- agg_results(data[!(age_group_id %in% c(5,28)),],id_vars = id_vars, value_vars = value_vars, agg_sex = F, loc_scalars=T, agg_sdi=T)
  data <- rbindlist(list(data_u5,data_o5),use.names=T)
} else {
    data <- agg_results(data,id_vars = id_vars, value_vars = value_vars, agg_sex = F, loc_scalars=T, agg_sdi=T)
}

## For locations without both sexes (e.g. regions and above where we don't produce scalars), we need to agg again because they are missing 
region_data <- data[location_id %in% c(region_locs),]
setkey(region_data,location_id,year_id,age_group_id,draw)
region_data <- region_data[,list(ax=sum(ax),mx=sum(mx),pop=sum(pop)),by=key(region_data)]
region_data[,sex_id:=3]
data <- rbindlist(list(data,region_data),use.names=T)

data[,ax:=ax/mx]
data[,mx:=mx/pop]
data[,pop:=NULL]

# Write year specific hdf indexed by location_id
locations <- unique(data$location_id)

filepath <- paste0(filepath)
file.remove(filepath)
h5createFile(filepath)
lapply(locations, save_hdf, data=data, filepath=filepath, by_var="location_id")

## Create summary mean lifetable based off of the mx/ax results
  format_for_lt <- function(data) {
    data[,qx:=0]
    data[,id:=paste0(location_id,"_",draw)]
    setnames(data,"year_id","year")
    
    return(data)
  }
  
  mx_ax_compiled <- format_for_lt(data)

## Summarize regions as a whole, add on summary file from earlier, and save combined_agg results file
  summarize_lt <- function(data) {
    varnames <- c("ax","mx")
    data <- data.table(data)[,lapply(.SD,mean),.SDcols=varnames,
                             by=c("location_id","age_group_id","sex_id","year")]
    data[,id:=location_id]
    data[,qx:=0]
    
    # Rerun lifetable function to recalculate life expectancy and other values based on the mean lifetable
    data <- lifetable(data,cap_qx=1)
    data$id <- NULL
    return(data)
  }
  mx_ax_compiled$age_group_id <- as.integer(mx_ax_compiled$age_group_id)
  mx_ax_compiled <- summarize_lt(mx_ax_compiled)
  filepath <- paste0(filepath)
  write.csv(mx_ax_compiled, filepath, row.names = F)

