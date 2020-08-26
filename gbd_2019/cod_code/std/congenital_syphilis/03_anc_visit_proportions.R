#####################################INTRO##################################################
#' Purpose: Calculate the ANC visit proportions using ANC 1+, 2+, and 4+
#'          1) Read in draws
#'          2) Manipulate to get 0 - 1 visits (untreated), 2 - 3 (inadequate late). Ensure 
#'             draws for less visits are higher than more visits 
#'          3) Save by location
#'
#' OUTPUTS: FILEPATH for each location
#'
#####################################INTRO##################################################

library(dplyr)
library(data.table)

# Retrieve draw-level ANC covariate data ----------------------------------

message(paste0(Sys.time(), "Retrieve draw-level ANC covariate data"))
base <- FILEPATH


combine_draws_for_anc <- function(anc, base_dir = base) {
  message(paste(Sys.time(), "Combining draws for ANC", anc))
  filepath <- FILEPATH
  files <- list.files(filepath, pattern = paste0("\\.csv$"))
  
  
  draws <- rbindlist(parallel::mclapply(FILEPATH, fread))
  long  <- melt(draws, id.vars = c("location_id","year_id"), measure.vars = patterns("draw_"),
                value.name = paste0("anc_", anc), variable.name = "draw_num")
}

anc_1 <- combine_draws_for_anc(1)
anc_2 <- combine_draws_for_anc(2)
anc_4 <- combine_draws_for_anc(4)


# Join ANCs together ------------------------------------------------------
message(paste(Sys.time(), "Merging ANC 1, 2, and 4 together"))
anc_all <- merge(anc_1, anc_2, by = c("year_id", "location_id", "draw_num")) %>% 
  merge(anc_4, by = c("year_id", "location_id", "draw_num"))


test/treat should not exceed 1, hence the capping 
anc_all[anc_1 > 1, anc_1 := 1]
anc_all[anc_2 > 1, anc_2 := 1]
anc_all[anc_4 > 1, anc_4 := 1]

anc_all[anc_2 > anc_1, anc_2 := anc_1]
anc_all[anc_4 > anc_2, anc_4 := anc_2]

# Calculate 0 - 1 and 2 - 3 visit(s) populations ------------------------------

anc_all_pops <- anc_all %>% 
  mutate(draw_0_1 = 1 - anc_2,      # Proportion w/ 0 - 1 ANC visits = 1 - proportion with 2+ visits
         draw_2_3 = anc_2 - anc_4,  # Proportion with 2 - 3 ANC visits = 2+ visits - 4+ visits (see above)
         draw_4   = anc_4) %>%      # Proporion with 4+ ANC visits, renamed             
  select(location_id, year_id, draw_num, draw_0_1, draw_2_3, draw_4) # put draw_2_3 between draw_0_1 and draw_4

message(paste(Sys.time(), "Saving the long form of the ANC population draws as a .csv"))
readr::write_csv(anc_all_pops, path = FILEPATH)


# Save as a csv for each location -----------------------------------------


# specify a data.table (dt), location (vectorizable), and an out_dir. 
location_data <- get_location_metadata(location_set_id = 35)
locations <- unique(location_data[level >= 3, location_id])

save_location_subset <- function(dt, location, out_dir, verbose = FALSE) {
  if (verbose) {
    message(paste0("Saving for location id ", location))
  }
  
  location_subset <- copy(dt[location_id == location])
  readr::write_csv(location_subset, FILEPATH)
}

anc_all_pops <- as.data.table(anc_all_pops)

message(paste(Sys.time(), "Saving the ANC population draws by location"))
invisible(parallel::mclapply(locations, function(loc) save_location_subset(dt = anc_all_pops, location = loc, out_dir = out_dir, verbose  = T)))












