#'####################################`INTRO`##########################################
#' @purpose: creates proportions for each subcause by age, sex, & super-region. 
#' Submits split-loc-year for each location to be adjusted for each age,sex, SR specific proportions
#'####################################`INTRO`##########################################

if (Sys.info()['sysname'] == 'Linux') {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
user <- Sys.info()["user"]
date <- gsub("-", "_", Sys.Date())

# Set up environment ------------------------------------------------------
source("FILEPATH")
library("haven")
library("data.table")

#SOURCE THINGS
source_shared_functions(functions = c("get_location_metadata", "get_cod_data"))

#ARGS & DIRS
parent_cause_id <- 393
cause_ids <- c(394, 395, 396, 399)

root_dir <- "FILEPATH"
out_dir <- "FILEPATH"

#CREATE DIRECTORIES
dir.create(out_dir, recursive = TRUE) 
directories <- cause_ids
invisible(lapply(directories, function(inner_dir) { dir.create(paste0(out_dir, inner_dir)) }))

create_props <- F

if (create_props == T){
  df <- data.table()
  
  for (cause in cause_ids){
    cod_data <- get_cod_data(cause_id = cause, cause_set_id = 4, age_group_id = c(7:20, 30:32, 235))
    print(dim(cod_data))
    df <- rbind(df, cod_data)
    print(dim(df))
  }
  
  print(unique(df$cause_id))
  names(df)
  nrow(df) 
  
  sub_causes <- df[data_type == "Vital Registration", c("cause_id", "location_id", "location_name", "age_group_id", "sex", "year", "study_deaths", "sample_size")]
  setnames(sub_causes, old = c("sex", "year"), new = c("sex_id", "year_id"))
  
  locations <- get_location_metadata(location_set_id = 35)
  locs <- locations[ ,c("location_id", "location_name", "super_region_name")]
  causes <-  merge(sub_causes, locs, by = c("location_id", "location_name"), all.x = TRUE)
  
  #NEW--------------------------------------------------------------------------------------------------------------
  #set groups by super region
  group1 <- c("Central Europe, Eastern Europe, and Central Asia", "High-income")
  sr_agg <- causes[super_region_name %in% group1, sr_group := "CECAH"]
  sr_agg[!(super_region_name %in% group1), sr_group := "ALMA"]
  
  sr_agg[cause_id == 395 & sex_id ==1, study_deaths := 0]
  sr_agg[ , `:=` (study_deaths_collapsed = sum(study_deaths), sample_size_collapsed = sum(sample_size)), by = c("cause_id", "sr_group", "age_group_id", "sex_id")]
  
  #set up calcs
  cause_props <- unique(sr_agg, by = c("cause_id", "sr_group", "age_group_id", "sex_id"))
  cause_props <- cause_props[ ,c("sr_group","super_region_name", "cause_id", "age_group_id", "sex_id","study_deaths_collapsed", "sample_size_collapsed")]
  cause_props <- cause_props[age_group_id >= 7]
  nrow(cause_props)
  
  # calc props
  cause_props[ ,prop := study_deaths_collapsed/sample_size_collapsed]
  cause_props[ ,scale_group := sum(prop), by = c("age_group_id", "sex_id", "sr_group")]
  length(unique(cause_props$scale_group))
  cause_props[ ,scaled := prop/scale_group]
  
  #format for plotting
  age_table_code <- "FILEPATH"
  age_table <- source(age_table_code)
  ages
  ages[ , age_name := paste0(age_start, "_", age_end, " yrs")]
  ages
  causes <- data.table(cause_id = c(394, 395, 396, 399), cause_name = c("syphilis", "chlamydia", "gonorrhea", "other"))
  
  prop <- merge(cause_props,ages,by="age_group_id", all.x = TRUE)
  prop <- merge(prop,causes,by="cause_id",all.x = TRUE)
  prop[sex_id == 1, sex_name := "Male"]
  prop[is.na(sex_name), sex_name := "Female"]
  
  write.csv(x = prop, file = "FILEPATH")
  
  #plot proportions by age/sex/sr_group
  #each age,sex,sr specific packet should sum to 1 across the 4 subcauses. 
  pdf("FILEPATH", width = 20, height = 4, paper = "USr") #pdf printed in landscape
  
  ggplot(data = prop[age_group_id >= 7 & !(age_group_id %in% c(22, 27)) & sex_name == "Male"], mapping = aes(x=age_name, y=scaled, fill=cause_name)) + geom_bar(stat='identity') + facet_wrap(~sr_group) + theme(axis.text.x=element_text(angle=45, hjust=1)) + theme(
    legend.title = element_text(color = "black", size = 6),
    legend.text = element_text(color = "black", size = 4)) + ggtitle(" Males: Adult STI proportional split Step3")
  
  ggplot(data = prop[age_group_id >= 7 & !(age_group_id %in% c(22, 27)) & sex_name == "Female"], mapping = aes(x=age_name, y=scaled, fill=cause_name)) + geom_bar(stat='identity') + facet_wrap(~sr_group) + theme(axis.text.x=element_text(angle=45, hjust=1)) + theme(
    legend.title = element_text(color = "black", size = 6),
    legend.text = element_text(color = "black", size = 4)) + ggtitle("Females: Adult STI proportional split Step3")
  
  dev.off()
}



## apply to each location and save results
#read in props
props <- data.table(read.csv(paste0(out_dir,"props_agg.csv")))
prop_data <- props[ ,c("super_region_name","sr_group", "age_group_id", "sex_id", "scaled", "cause_id")]
age_ids <- c(7:20, 30:32, 235)

#identify which sr group the location falls into
group1 <- c("Central Europe, Eastern Europe, and Central Asia", "High-income")
loc_meta <- data.table(get_location_metadata(location_set_id = 35, release_id = release)) #model results
loc_meta[super_region_name %in% group1, sr_group := "CECAH"]
loc_meta[is.na(sr_group), sr_group := "ALMA"]
super_group <- loc_meta[location_id == loc_id, sr_group]

for (year in 1980:end_year){
  cat(paste("Get draws for", year))
  #defaults to bested model
  year_draws <- get_draws(gbd_id_type = "cause_id", gbd_id = parent_cause_id, location_id = loc_id, year_id = year, age_group_id = age_ids, sex_id = c(1,2), source = "codem", release_id=release)
  year_draws[ ,sr_group := super_group]
  year_draws$cause_id <- NULL
  
  cat(paste0("Merging draws & proportions!"))
  merge <- merge(year_draws, prop_data, by = c("age_group_id", "sex_id", "sr_group"), coord_cartesian = TRUE)
  
  cat("Pre-prop application dimensions:", dim(merge))
  #multiply draws by their respective scaling factors
  vars <- c(paste0("draw_", seq(0,999, by=1)))
  apply_props <- merge[, (vars) := lapply(.SD, function(x) x*scaled), .SDcols=vars , by = c("age_group_id", "sr_group", "sex_id", "cause_id")]
  cat("Post-prop dimensions:", dim(apply_props), ". Should be the same as pre-dimensions!")
  
  for (cause in unique(apply_props$cause_id)){
    for (sex in unique(apply_props$sex_id)){
      
      save_loc <- apply_props[cause_id == cause & sex_id == sex, ]
      save_loc[ ,c("sex_id", "super_region_name", "sex_name", "year_id", "metric_id", "measure_id", "scaled", "cause_id", "envelope", "location_id", "pop") :=NULL]
      
      cat(paste0("Save as ", loc_id, "_", year, "_", sex, ".csv" ))
      write.csv(x = save_loc, file = paste0(out_dir, cause, "/", loc_id, "_", year, "_", sex, ".csv"), row.names = FALSE)
    }
  }
  
  
}





