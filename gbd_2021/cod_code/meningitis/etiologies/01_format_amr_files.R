#' @author username
#' @date 2022/07/14
#' @description rearrange and reformat files received from AMR team

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- paste0("FILEPATH")
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
}

## Load packages
pacman::p_load(data.table, ggplot2, dplyr, parallel, argparse, pbapply)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--meid", help = "modelable entity id for upload", default = NULL, type = "integer")
parser$add_argument("--etiology", help = "pathogen for upload", default = NULL, type = "character")
parser$add_argument("--cause_id", help = "cause_id of results being read from AMR", default = NULL, type = "integer")
parser$add_argument("--in_dir", help = "in directory for AMR saved results", default = NULL, type = "character")
parser$add_argument("--out_dir", help = "out directory for formatted results", default = NULL, type = "character")
parser$add_argument("--code_dir", help = "repository directory, has a csv with dimensions/IDs", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step4', type = "character")
parser$add_argument("--gbd_round_id", help = "specify gbd round", default = 7L, type = "integer")
parser$add_argument("--desc", help = "upload description", default = as.character(gsub("-", "_", Sys.Date())), type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

## Source Functions
invisible(sapply(list.files("FILEPATH"), source))


## Helper functions
expand_subnational <- function(dt, hierarchy){
  loc_estimate <- hierarchy[most_detailed == 1]
  l <- strsplit(as.character(loc_estimate$path_to_top_parent), ',')
  df1new <- data.frame(country_id = as.integer(unlist(l)), 
                       path_to_top_parent = rep(loc_estimate$path_to_top_parent, lengths(l)),
                       location_id = rep(loc_estimate$location_id, lengths(l)))
  dt <- merge(dt, df1new, by.x = "location_id", by.y = "country_id", all.x = TRUE, allow.cartesian = T)
  dt[, location_id := NULL]
  setnames(dt, "location_id.y", "location_id")
  return(dt)
}

## Loop through both causes
for (cause_id in c(322,332)){
  
  # TEMP VERSION ADDED: get input directory
  if(cause_id == 322){
    file_path <- paste0("FILEPATH")
  } else if (cause_id == 332){
    file_path <- paste0("FILEPATH")
  }
  
  ## Load in data
  amr_results_neonatal_list <- lapply(list.files(file.path("FILEPATH"), full.names = TRUE), function(file){
    if (file %like% "_draws.csv"){
      dt <- fread(file)
      year <- substr(file, (nchar(file) - 13), (nchar(file) - 10))
      print(paste("Reading for", year))
      dt$year_id <- as.integer(year)
      dt <- dt[age_group_id == 42]
      return(dt)
    }
  })
  amr_results <- rbindlist(amr_results_neonatal_list) 
  
  amr_results_pn_plus_list <- lapply(list.files(file.path(file_path, 
                                                          if(cause_id == 322){
                                                            paste0("post_neonatal_plus")
                                                          } else if (cause_id == 332){
                                                          paste0("non_neonatal")
                                                          }), full.names = TRUE), function(file){
    if (file %like% "_draws.csv"){
      dt <- fread(file)
      year <- substr(file, (nchar(file) - 13), (nchar(file) - 10))
      print(paste("Reading for", year))
      dt$year_id <- as.integer(year)
      dt <- dt[age_group_id != 42]
      return(dt)
    }
  })
  amr_results <- rbind(amr_results, rbindlist(amr_results_pn_plus_list)) 
  
  rm(amr_results_neonatal_list); rm(amr_results_pn_plus_list)
  
  # Keep only the relevant pathogen
  p <- copy(etiology)
  amr_results <- amr_results[pathogen == p]
  if(nrow(amr_results) == 0) next()
  
  ## Expand AMR results out to the GBD age groups
  ages <- get_ids("age_group")
  ages_amr <- ages[age_group_id %in% amr_results$age_group_id]
  # Use Custom age map.
  custom_age_map <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)
  custom_age_map <- custom_age_map %>%  mutate(custom_age_map = if_else(age_group_years_start<28/365, "Neonatal",
                                                                        if_else(age_group_years_start<5, "Post Neonatal to 5",
                                                                                if_else(age_group_years_start < 49, "5 to 49",
                                                                                        if_else(age_group_years_start < 70, "50-69 years", "70+ years")))))
  custom_age_map[,c("age_group_years_start", "age_group_years_end", "age_group_weight_value", "most_detailed", "age_group_name") := NULL]
  custom_age_map <- merge(custom_age_map, ages_amr, by.x = "custom_age_map", by.y = "age_group_name")
  amr_results_expanded <- merge(amr_results, custom_age_map, by.x = "age_group_id", by.y = "age_group_id.y", allow.cartesian = TRUE)
  amr_results_expanded$age_group_id <- NULL
  setnames(amr_results_expanded, "age_group_id.x", "age_group_id")
  
  # Duplicate to both sex
  amr_results_expanded <- rbind(copy(amr_results_expanded)[,sex_id := 1], copy(amr_results_expanded)[,sex_id := 2])
  
  # Add cause id column
  amr_results_expanded[infectious_syndrome == "respiratory_infectious", cause_id := 322]
  amr_results_expanded[infectious_syndrome == "cns_infectious", cause_id := 332]
  
  # Delete columns unneeded for upload
  amr_results_expanded[,c("hosp", "infectious_syndrome") := NULL]
  
  ## Expand AMR results out to the GBD locations
  hierarchy <- get_location_metadata(35, gbd_round_id = 7, decomp_step = "iterative")
  amr_results_expanded <- expand_subnational(amr_results_expanded, hierarchy)
  

  # Rewrite for upload
  # Special case for virus
  if(p %like% "virus") p <- paste0("virus_", cause_id)
  out_eti <- file.path(out_dir, "original", p)
  dir.create(out_eti, recursive = TRUE, showWarnings = FALSE)
  mclapply(unique(amr_results_expanded$location_id), function(l){
    # read in any previous files
    if (file.exists(paste0(out_eti, "/", l, ".csv"))) {
      amr_prev <- fread(paste0(out_eti, "/", l, ".csv"))
      # keep only OTHER causes
      cid <- copy(cause_id)
      amr_prev <- amr_prev[cause_id != cid]
    } else amr_prev <- data.table()
    # subset current AMR info to the location of interest
    amr_temp <- amr_results_expanded[location_id == l]
    amr_temp[measure_id == 1, measure_id := 4]
    amr_temp[measure_id == 6, measure_id := 3]
    # combine with previously uploaded material
    if(nrow(amr_prev)>0) amr_temp <- rbind(amr_prev, amr_temp)
    fwrite(amr_temp, paste0(out_eti, "/", l, ".csv"), row.names=F)
  }, mc.cores = 4)
  
  # Clear memory for next iteration
  rm(amr_results); rm(amr_results_expanded)
  gc()
}

# Done writing the unsqueezed version