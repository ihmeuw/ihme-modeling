#####################################INTRO##################################################
#' Author: 
#' 12/28/17 - Edited 3/13/18
#' Purpose: Calculate the ANC visit proportions using ANC 1+, 2+, and 4+
#'          1) Read in draws
#'          2) Manipulate to get 0 - 1 visits (untreated), 2 - 3 (inadequate late). Ensure 
#'             draws for less visits are higher than more visits 
#'          3) Save by location
#'
#' OUTPUTS: FILEPATH/{location_id}.csv for each location
#'
#####################################INTRO##################################################

library(dplyr)


# Retrieve draw-level ANC covariate data ----------------------------------

message(paste0(Sys.time(), " Retrieve draw-level ANC covariate data"))

base <- "FILEPATH"

# Reads in all the separate csvs (currently saved by location),
# combines them all together, and melts long
combine_draws_for_anc <- function(anc, base_dir = base) {
  message(paste(Sys.time(), "Combining draws for ANC", anc))
  filepath <- paste0(base_dir, anc, "/")
  files <- list.files(filepath, pattern = paste0("\\.csv$"))
  
  
  draws <- rbindlist(parallel::mclapply(paste0(filepath, files), fread))
  long  <- melt(draws, id.vars = c("location_id","year_id"), measure.vars = patterns("draw_"),
                value.name = paste0("anc_", anc), variable.name = "draw_num")
}

anc_1 <- combine_draws_for_anc(1)
anc_2 <- combine_draws_for_anc(2)
anc_4 <- combine_draws_for_anc(4)


# Join ANCs together ------------------------------------------------------


message(paste(Sys.time(), "Merging ANC 1, 2, and 4 togther"))

anc_all <- merge(anc_1, anc_2, by = c("year_id", "location_id", "draw_num")) %>% 
  merge(anc_4,                 by = c("year_id", "location_id", "draw_num"))


# Currently, draws can be greater than 1
anc_all[anc_1 > 1, anc_1 := 1]
anc_all[anc_2 > 1, anc_2 := 1]
anc_all[anc_4 > 1, anc_4 := 1]

# When ANC 2+ draw X is greater than ANC 1+ draw X (not physically possible), replace 2 with value for ANC 1+ draw X
# as we trust the ANC 1 draw data more. repeat for 4+ greater than 2+
anc_all[anc_2 > anc_1, anc_2 := anc_1]
anc_all[anc_4 > anc_2, anc_4 := anc_2]

# Calculate 0 - 1 and 2 - 3 visit(s) populations ------------------------------

# Proportion w/ 0  - 1 ANC visits = 1 - proportion with 2+ visits
# prop w/ 2 - 3 visits = prop w/ 2+ visits - prop w/ 4+ visits
#   Ex: mean_2_3 = those with (2, 3, 4, 5, 6+) - (4, 5, 6+)
#   with simplyfies to 2, 3
# This requires no squeezing to 1 total; by definition of how we derive 0 - 1, 2 - 3, and 4+, all draws sum to 1
#   Ex: draw_0_1   = 1      - draw_2
#       draw_2_3 = draw_2 - draw_4         , substitute in draw_1 from line above:
#    => draw_2_3 = 1      - draw_0_1 - draw_4, rearrange terms
#    => draw_0_1 + draw_2_3 + draw_4 = 1
anc_all_pops <- anc_all %>% 
  mutate(draw_0_1 = 1 - anc_2,      # Proportion w/ 0 - 1 ANC visits = 1 - proportion with 2+ visits
         draw_2_3 = anc_2 - anc_4,  # Proportion with 2 - 3 ANC visits = 2+ visits - 4+ visits (see above)
         draw_4   = anc_4) %>%      # Proporion with 4+ ANC visits, renamed             
  select(location_id, year_id, draw_num, draw_0_1, draw_2_3, draw_4) # put draw_2_3 between draw_0_1 and draw_4

message(paste(Sys.time(), "Saving the long form of the ANC population draws as a .csv"))
readr::write_csv(anc_all_pops, path = paste0(out_dir, "EX/anc_visit_props_long.csv"))


# Save as a csv for each location -----------------------------------------
# This cuts down on file size and facilitates parallelization

# remove old files
system(paste0("rm -f ", out_dir, "/EX/*.csv"))

# specify a data.table (dt), location (vectorizable), and an out_dir. 
# Verbose is optional if you want live prongress on which locations have been saved 
location_data <- get_location_metadata(location_set_id = 35)
locations <- unique(location_data[level >= 3, location_id])

save_location_subset <- function(dt, location, out_dir, verbose = FALSE) {
  if (verbose) {
    message(paste0("Saving for location id ", location))
  }
  
  location_subset <- copy(dt[location_id == location])
  readr::write_csv(location_subset, paste0(out_dir, "EX/", location, ".csv"))
}

anc_all_pops <- as.data.table(anc_all_pops)

message(paste(Sys.time(), "Saving the ANC population draws by location"))
invisible(parallel::mclapply(locations, function(loc) save_location_subset(dt = anc_all_pops, location = loc, out_dir = out_dir, verbose  = T)))












