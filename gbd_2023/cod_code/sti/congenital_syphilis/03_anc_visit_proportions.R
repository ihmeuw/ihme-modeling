#####################################INTRO##################################################
#' Purpose: Calculate the ANC visit proportions using ANC 1+, 2+, and 4+
#'          1) Read in draws
#'          2) Manipulate to get 0 - 1 visits (untreated), 2 - 3 (inadequate late). 
#'          3) Save by location
#'
#' OUTPUTS: anc_visits/{location_id}.csv for each location
#####################################INTRO##################################################

args <- commandArgs(trailingOnly = TRUE)
out_dir <- as.character(args[1])
loc_id <- as.numeric(args[3])

source("FILEPATH")
source_shared_functions(functions = c("get_location_metadata", "get_draws"))
library(dplyr)
library(data.table)

# Retrieve draw-level ANC covariate data ----------------------------------
years <- 1980:2023

message(paste(Sys.time(), "Retrieve draw-level ANC coverage data"))
anc_1 <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 10647, source = "stgpr",
                         location_id = loc_id, age_group_id = 22, sex_id = 3, release_id=release)
anc_2 <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 25042, source = "stgpr",
                       location_id = loc_id, age_group_id = 22, sex_id = 3, release_id=release)
anc_4 <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = 25043, source = "stgpr",
                       location_id = loc_id, age_group_id = 22, sex_id = 3, release_id=release)

#specify dts
anc_1[ ,anc_1 := "anc_1"]
anc_2[ ,anc_2 := "anc_2"]
anc_4[ ,anc_4 := "anc_4"]

## add checkpoint here - if saved on less than 1000 draws, exit script
if (ncol(anc_1) < 1008 | ncol(anc_2) < 1008 | ncol(anc_4) < 1008) {
  stop("One or more of the ANC covariates has less than 1000 draws. Exiting script.")
}
  
#ANC 1 WORK ---------------------
#melt each dt long 
anc1_long <- melt(anc_1, id.vars = c("location_id", "year_id"), measure.vars = patterns("draw_"), value.name = "anc_1", variable.name = "draw_num")

#ANC 2 WORK -------------------------------
#melt each dt long 
anc2_long <- melt(anc_2, id.vars = c("location_id", "year_id"), measure.vars = patterns("draw_"), value.name = "anc_2", variable.name = "draw_num")
  
#ANC 4 WORK ----------------------
#melt each dt long 
anc4_long <- melt(anc_4, id.vars = c("location_id", "year_id"), measure.vars = patterns("draw_"), value.name = "anc_4", variable.name = "draw_num")

# Join ANCs together ------------------------------------------------------
message(paste(Sys.time(), "Merging ANC 1, 2, and 4 together"))
  
#nested merge 
anc_all <- merge(anc1_long, anc2_long, by = c("year_id", "location_id", "draw_num")) %>% 
  merge(anc4_long, by = c("year_id", "location_id", "draw_num"))
  
head(anc_all)
#Each "draw" is a y-a-s-l, with a different proportion based on 1,2, or 4 visits.
#Adding across the same location in anc 1,2 and 4 and so on for each location, the proportions for each category of test/treat should not exceed 1, hence the capping 

# anc_all %>% filter(anc_4 > anc_1) %>% View
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
#   NOTE: ~3% of draws for 1 - 3 ANC visits will be 0 since we set some draw 4s to the value of draw 1s if the former were greater
# This requires no squeezing to 1 total; by definition of how we derive 0 - 1, 2 - 3, and 4+, all draws sum to 1
#   Ex: draw_0_1   = 1      - draw_2
#       draw_2_3 = draw_2 - draw_4         , substitute in draw_1 from line above:
#    => draw_2_3 = 1      - draw_0_1 - draw_4, rearrange terms
#    => draw_0_1 + draw_2_3 + draw_4 = 1
anc_all_pops <- anc_all %>% 
  mutate(draw_0_1 = 1 - anc_2,      # Proportion w/ 0 - 1 ANC visits = 1 - proportion with 2+ visits
         draw_2_3 = anc_2 - anc_4,  # Proportion with 2 - 3 ANC visits = 2+ visits - 4+ visits (see above)
         draw_4   = anc_4)      # Proporion with 4+ ANC visits, renamed       
anc_all_pops <- anc_all_pops[ ,c("location_id", "year_id", "draw_num", "draw_0_1", "draw_2_3", "draw_4")] # put draw_2_3 between draw_0_1 and draw_4
   
  
head(anc_all_pops)

message(paste0("Saving for location id ", loc_id))
  
location_subset <- copy(anc_all_pops)
readr::write_csv(location_subset, "FILEPATH")
  






