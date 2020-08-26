#'####################################`INTRO`##########################################
#' @purpose: Apply proportions by super-region to split the parent model into subcause for each location
#' @outputs: deaths by subcause as csv at `FILEPATH`
#'
#'####################################`INTRO`##########################################

library(data.table)

functions_dir <- FILEPATH
functs <- c("get_location_metadata", "get_draws")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R")))) 

# Read in parameters ------------------------------------------------------
cat(paste0("Reading In Arguments..."))
args <- commandArgs(trailingOnly = TRUE)
print(args)

out_dir <- args[1]
parent_cause_id <- args[2]
loc_id   <- args[3]
decomp <- args[4]

cat("here are the args for this loc", paste(out_dir, parent_cause_id, loc_id, decomp))

#read in props
props <- data.table(read.csv(FILEPATH))
prop_data <- props[ ,c("super_region_name","sr_group", "age_group_id", "sex_id", "scaled", "cause_id")]
age_ids <- c(7:20, 30:32, 235)

#identify which super-region group each location falls into
group1 <- c("Central Europe, Eastern Europe, and Central Asia", "High-income")
loc_meta <- data.table(get_location_metadata(location_set_id = 35, decomp_step = decomp)) 
loc_meta[super_region_name %in% group1, sr_group := "CECAH"]
loc_meta[is.na(sr_group), sr_group := "ALMA"]
super_group <- loc_meta[location_id == loc_id, sr_group]

for (year in 1980:2019){
  cat(paste("Get draws for", year))
  year_draws <- get_draws(gbd_id_type = "cause_id", gbd_id = parent_cause_id, location_id = loc_id, year_id = year, age_group_id = age_ids, source = "codem", decomp_step = decomp, sex_id = c(1,2))
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
      write.csv(x = save_loc, file = FILEPATH, row.names = FALSE)
    }
  }
  
  
}

