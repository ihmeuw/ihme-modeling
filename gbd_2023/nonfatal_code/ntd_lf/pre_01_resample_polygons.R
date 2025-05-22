##############
# This script resamples polygon data into point data using the lbd.mbg resample_polygon function
##############

###---- Step 1: Setup
# Get current user name
user <- Sys.info()[["user"]]
run_date = sprintf(paste0(as.character(Sys.Date()),'_', user))

# Set up directory
root_dir <- "FILENAME"
data_dir <- "FILENAME"
run_dir <- "FILENAME" # current model run
param_dir <- "FILENAME"# training data or other files
out_dir <- "FILENAME" # saved xwalk model objects
lu_dir <- "FILENAME" # save intermediate data files 

# load lbd.mbg package and other packages
library(lbd.loader, lib.loc = "FILENAME")
library(lbd.mbg, lib.loc =lbd.loader::pkg_loc("lbd.mbg"))
library(rgeos)
library(rgdal)
library(sf)

# Set up indicators
ig <- "lf"
indic <- indicator <- "had_lf_poly"

# Import the dataset for resampling
lf_data_for_resampling <- fread("FILENAME")
  
###---- Step 2: Resampling polygons using the lbd.mbg function
df_poly_data <- resample_polygons(
  data = lf_data_for_resampling,
  indic = indicator,
  density = as.numeric(0.001), # Integration points generated at a density of 1 per 1,000 grid cells
  perpixel = TRUE,
  prob = TRUE,
  use_1k_popraster = TRUE,
  pull_poly_method = "fast",
  gaul_list = get_adm0_codes("all", shapefile_version = "current"),
  seed = NULL,
  shapefile_version = "current",
)

###---- Step 5: Join re-sampled polygons to point data

# Bind together
lf_pts <- lf_data_for_resampling %>% 
  filter(point == 1) %>%
  mutate(pseudocluster = FALSE,
         weight = 1) 
  
pts_polys_comb <- rbind.fill(lf_pts, df_poly_data) %>%
  # the name of the column that contains the cases needs to correspond with our "indicator" variable
  dplyr::rename(had_lf_w_resamp = had_lf_poly)

# Check to ensure no data was lost during the resampling process
new_id <- length(unique(pts_polys_comb$V1))  
orig_id <- length(unique(lf_data_for_resampling$V1))
if(new_id != orig_id){
  message(paste0("The resampled dataset has ", new_id, " unique IDs, but the original set has ", orig_id, " unique IDs. Please review this discrepency"))
} else {
  message(paste0("The number of unique IDs in the resampled dataset matches the original dataset. Saving in the following folder for mbg modeling: ", ))
}

# Save
write.csv(pts_polys_comb, "FILENAME")
