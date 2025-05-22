
# source libraries --------------------------------------------------------

library(data.table)
library(rgdal)
library(haven)

# source elevation adjustment functions -----------------------------------

source(file.path(getwd(), "extract/brinda/adjust_scripts/apply_elevation_adjustment.R"))
source(file.path(getwd(), "extract/brinda/adjust_scripts/assign_anemia_severity.R"))

# load in map file and admin elevation location ---------------------------

task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
command_args <- commandArgs(trailingOnly = TRUE)
google_cb <- fread(command_args[1])
coords_df <- fread(file.path(getwd(), "extract/brinda/param_maps/google_admin_coords.csv"))

Sys.sleep(task_id * 3) # sleep so the shape file can load in properly

world <- readOGR("FILEPATH/lbd_standard_admin_2.shp")
# reshape it to be the same dimensions as the raster files to be loaded in
world <- spTransform(
  world, 
  CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")
)
admin_df <- as.data.table(world@data)

# assign elevation --------------------------------------------------------

assign_elevation <- function(input_df){
  df <- copy(input_df)
  
  for(r in seq_len(nrow(df))){
    admin_2_code <- get_admin2_polygon(
      longitutde = df$long[r], 
      latitude = df$lat[r]
    )
    if(!(is.null(admin_2_code))){
      admin_elevation <- get_admin_elevation(
        admin_2_code = admin_2_code,
        year_id = unique(df$year_end)
      )
      df$cluster_altitude[r] <- admin_elevation
      df$cluster_altitude_unit[r] <- "m"
    }
  }
  keep_cols <- c("admin_name", "cluster_altitude", "cluster_altitude_unit")
  df <- df[, keep_cols, with = FALSE]
  return(df)
}

get_admin2_polygon <- function(longitutde, latitude){
  deez_coords <- SpatialPoints(data.frame(x = longitutde, y = latitude))
  proj4string(deez_coords) <- "+proj=longlat +datum=WGS84 +no_defs"
  admin_area <- rbindlist(sp::over(deez_coords, world, returnList = TRUE))
  if(nrow(admin_area) == 1 && !(all(is.na(admin_area)))){
    return(admin_area$ADM2_CODE[1])
  }
  return(NULL)
}

get_admin_elevation <- function(admin_2_code, year_id){
  admin_2_file_path <- "FILEPATH"
  admin_2_file_path <- file.path(
    admin_2_file_path,
    paste0(
      "admin2_elevation_means_",
      year_id,
      ".csv"
    )
  )
  df <- fread(admin_2_file_path)
  index <- match(admin_2_code, df$ADM2_CODE)
  return(df$weighted_mean_elevation[index])
}

# main google function ----------------------------------------------------

main_google_function <- function(codebook, lat_long_df, row_num){
  admin_coords <- lat_long_df[ubcov_id == codebook$cb_basic_id[row_num]]
  admin_coords <- assign_elevation(input_df = admin_coords)
  
  df <- setDT(read_dta(codebook$extracted_file_name[row_num]))
  df <- merge.data.table(
    x = df,
    y = admin_coords,
    by.x = unique(admin_coords$admin_level),
    by.y = "admin_name",
    all.x = TRUE
  )

  df <- apply_elevation_adjustment(input_df = df)
  df <- assign_anemia_severity(input_df = df)

  write_dta(
    data = df,
    path = codebook$extracted_file_name[row_num]
  )
}

# call main function ------------------------------------------------------

main_google_function(
  codebook = google_cb,
  lat_long_df = coords_df,
  row_num = task_id
)
