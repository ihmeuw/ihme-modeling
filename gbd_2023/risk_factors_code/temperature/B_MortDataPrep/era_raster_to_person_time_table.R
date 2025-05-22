# This code converts temperature and population rasters to flat files with
# person-days of exposure to given temperatures. It is the worker script of a
# parallelized run, with each job handling a specific combination of GBD
# location and year.
#
# The overall process is....
#  1) Read in the GBD locations shapefile, and rasters for population,
#     temperature, and temperature zone;
#  2) Get all spatial inputs on the same projection, resolution, alignment,
#     and extent
#  3) Extract the pixels from each raster that fall within the target location
#  4) Create draws of the number of person-days within the target location and
#     year that were spent within each combination of daily temperature and
#     temperature zone.
#
#
# The code was created during the GBD 2021 cycle when we changed the approach to
# PAF calculation to increase speed and efficiency. Updates from the original
# version have been modest and mostly involve changes to file paths in response
# to the FILEPATH migration, and some improvements in commenting, and readability.
# The RGDAL library was retired in late 2023 or early 2024, which broke this
# pipeline, so further updates were made in Feb 2024 to transition to the sf library.
#
# Libraries: raster, data.table, zoo, parallel, Rfast, sf
# Input data: ERA5 temperature rasters, World Pop population rasters,
#             GBD shapefiles
# Outputs: this saves a csv with draws of person-days exposure to each
#          combination of daily temperature and temperature zone, for the target
#          location and year to be used in PAF and SEV calculation



## SET UP THE ENVIRONMENT ------------------------------------------------------

# Start with a clean slate
rm(list = ls())


# Record the starting time to measure the duration of the run
start_time <- Sys.time()

# Load the required libraries
library(Rcpp)
library(raster)
library(data.table)
library(zoo)
library(parallel)
library(Rfast)
library(sf)


# Determine if this is an interactive job (i.e. development) or a batch job
interactive = commandArgs()[2] == '--interactive'

# If this is a batch job, then the arguments will be passed in from the command
# line; otherwise, hard code the desired arguments for testing and development

# If launched from cmd line use args
if (!interactive) {
  args <- commandArgs(trailingOnly = T)
  loc_id   <- as.integer(args[1])
  year     <- as.integer(args[2])
  release  <- as.integer(args[3])
  n_draws  <- as.integer(args[4])
  threads  <- as.integer(args[5])
  out_dir  <- args[6]
  job_name <- args[7]
  debug    <- FALSE

# otherwise, this is an interactive testing/development run & hardcode arguments
} else {
  loc_id   <- 207
  year     <- 2023
  release  <- 16
  n_draws  <- 100
  threads  <- 8
  out_dir  <- 'FILEPATH'
  job_name <- paste0('melt_', loc_id, '_', year)
  debug    <- FALSE # make true for debugging mode that will retain data
                    # to help debugging and development
}

# Message out so arguments will be visible in the log for debugging
message(paste0('Location   = ', loc_id))
message(paste0('Year       = ', year))
message(paste0('Output dir = ', out_dir))
message(paste0('Job name   = ', job_name))
message(paste0('Draws      = ', n_draws))


# Establish directories for the input data and shared functions
POP_VERSION <- '2024_01_29'
POP_DIR <- file.path('FILEPATH', POP_VERSION, '1y')
SHARED_FUN_DIR <- 'FILEPATH'
SHP_ROOT <- 'FILEPATH'
TEAM_DIR <- 'FILEPATH'

# The shapefile directories are named based on the release name, rather than id,
# so we need to get the release name from the release id
source(file.path(SHARED_FUN_DIR, 'get_ids.R'))
release_name <- get_ids('release')[release_id == release, release_name]


# Build out paths to sub-directories
era_dir <- file.path(TEAM_DIR, 'ERA5')
inputs_dir <- file.path(TEAM_DIR, paste0('release_', release),
                        'temperature', 'inputs')
shp_dir <- file.path(SHP_ROOT, gsub(' ', '_', release_name),
                     'master', 'shapefiles')


# Draws need to be numbered starting at 0, so the max draw is n_draws - 1
max_draw <- n_draws - 1

## LOAD THE SHAPEFILE ----------------------------------------------------------
# Read in the shapefile #
shp_version <- paste0(gsub(' ', '', release_name), '_analysis_final')
shape <- read_sf(dsn = shp_dir, layer = shp_version)
message('Shapefile loaded')

# Subset shapefile to target location, unless location is global (loc_id 1)
if (loc_id == 1) {
  sub_shape <- shape
} else {
  sub_shape <- shape[shape$loc_id == loc_id, ]
}
message('Shapefile subsetted')

## DETERMINE THE RASTERS TO USE ------------------------------------------------
# Find files with closest available year of temperature data
temp_files <- list.files(file.path(era_dir, 'temp'),
                         'era5_temp_daily_[0-9]{4}.gri')
temp_years <- as.integer(regmatches(temp_files,
                                    regexpr('[0-9]{4}', temp_files)))
temp_file <-  temp_files[which.min(abs(temp_years - year))]
temp_year <-  temp_years[which.min(abs(temp_years - year))]

temp_sd_file <- paste0('spread_daily_', temp_year, '.gri')

# Find file with closest available year of population data
pop_files <- list.files(POP_DIR, '.tif$')
pop_years <- as.integer(regmatches(pop_files, regexpr('[0-9]{4}', pop_files)))
pop_file  <- pop_files[which.min(abs(pop_years - year))]


# Check the date of the temperature file to ensure that it was created after the
# year it represents. It's possible that we have a placeholder temperature file
# in place while waiting for the final temperature estimates to become available
# for the most recent year(s). We need to know if that's the case to avoid errors
# and bugs. Here we'll check the date of each temperature file to ensure that it
# was created after the year it represents (and therefore unlikely to be a
# placeholder file)
temp_file_mtime <- file.info(file.path(era_dir, 'temp', temp_file))$mtime
earliest_possible <- as.POSIXct(paste0(temp_year + 1, '/01/01'))

if (temp_file_mtime < earliest_possible) {
  warning('Temperature file created before the time it represents!')
} else {
  message('Temperature file modified after end of year it represents')
}

## LOAD AND PROCESS THE RASTERS ------------------------------------------------
# Read in the population, daily temp, temp SD, and temp zone rasters
pop <- raster(file.path(POP_DIR, pop_file))

daily_temp <- brick(file.path(era_dir, 'temp', temp_file), overwrite = T,
                    format = 'raster')

sd <- brick(file.path(era_dir, 'temp_spread', temp_sd_file), overwrite = T,
            format = 'raster')

temp_zone <- raster(file.path(inputs_dir, 'temperature_zones.grd'))

message('Rasters loaded')

# Ensure that all rasters have the same projection
projection <- '+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0'
crs(pop) <- projection
crs(daily_temp) <- projection
crs(sd) <- projection
crs(temp_zone) <- projection
message('Rasters reprojected')

# Ensure that all rasters have the same extent and resolution
pop <- raster::resample(pop, daily_temp, method = 'bilinear')
sd <- raster::resample(sd, daily_temp, method = 'bilinear')
temp_zone <- raster::resample(temp_zone, daily_temp, method = 'bilinear')
message('Rasters resampled')


## DEFINE PIXEL EXTRACTION FUNCTION --------------------------------------------
extract_target_pixels <- function(x, shape, melt = F,
                                  mask = T, crop = T) {

  # x is the character name of the object -- getting the object itself here
  tmp <- eval(parse(text = x))

  # Crop and mask the raster, if requested
  if (crop == T) tmp <- raster::crop(tmp, shape)
  if (mask == T) tmp <- raster::mask(tmp, shape)

  # Convert raster to data table
  tmp <- as.data.table(rasterToPoints(tmp, spatial = T))

  # Melt the data table if requested (converts daily rasters to long format)
  if (melt == T) {
    tmp <- melt(data = tmp, id.vars = c('x', 'y'))
    setnames(tmp, 'variable', 'layer')
    setnames(tmp, 'value', x)
  } else {
    names(tmp)[1] <- x
  }

  # Round the x and y coord to avoid floating point merge errors
  tmp[, c('x', 'y') := lapply(.SD, round, digits = 2), .SDcols = c('x', 'y')]
  return(tmp)
}

message('Extract function defined')


## DETERMINE WHICH PIXELS TO EXTRACT -------------------------------------------
# For very small locations (e.g. islands) we may lose all pixels if we mask to
# the exact location polygon, for these locations we'll buffer the polygon until
# we pick up pixels with populations

if (loc_id == 1) {
  crop <- mask <- F
} else {
  crop <- mask <- T

  while (TRUE) {
    test_pixels <- raster::mask(crop(pop, sub_shape), sub_shape)
    test_vals <- as.numeric(values(test_pixels))
    message('Test values extracted')

    if (sum(test_vals, na.rm = T)<1) {
      message('Zero population found in the polygon. Buffering.')
      sub_shape <- st_buffer(sub_shape, dist = 100)
    } else {
      break
    }
  }

  rm(test_vals, test_pixels)
}

message('Pixel test completed')

## EXTRACT DATA FROM RASTERS AND CREATE DATA TABLES ----------------------------
raster_names <- c('pop', 'daily_temp', 'sd', 'temp_zone')
extracted <- lapply(raster_names, function(x) {
  if (x %in% c('daily_temp', 'sd')) melt <- T
  else melt <- F

  return(extract_target_pixels(x, sub_shape, melt = melt, mask = mask))
})

names(extracted) <- raster_names


# Clean and find unique values of temperature zone
# Subtract 273.15 to convert Kelvin to Celsius
extracted$temp_zone[, temp_zone  := as.integer(round(temp_zone - 273.15,
                                                     digits = 0))]
zones <- unique(extracted$temp_zone$temp_zone)

## MERGE COMPONENTS ------------------------------------------------------------
full <- merge(extracted$pop, extracted$temp_zone,  by = c('x', 'y'), all = T)
full <- merge(full, extracted$daily_temp, by = c('x', 'y'), all = T)
full <- merge(full, extracted$sd, by = c('x', 'y', 'layer'), all = T)
message('Components merged')

# Unless we're in debugging mode, drop the component data tables to save memory
if (debug == F) rm(extracted)

# Drop any pixels with zero or missing populations, since they can't contribute
# to person-time
full <- full[pop > 0 & is.na(pop) == F, ]

# If we're in debugging mode make a backup copy of the data before proceeding
if (debug == T) full_bkup <- copy(full)


## CONVERT TEMP VARIABLES AND CREATE TEMPERATURE DRAWS -------------------------

# Convert degrees Kelvin to degrees Celsius and multiply by 10 to make integers
# of each tenth degree for more reliable merges later
full[, daily_temp_cat := 10 * (daily_temp - 273.1)]
full[, sd := 10 * sd]

full <- full[, .(temp_zone, daily_temp_cat, sd, pop)]
setkeyv(full, c('temp_zone', 'daily_temp_cat'))

### CREATE DRAWS AND COLLAPSE POPULATION BY DAILY TEMP AND TEMP ZONE ###
collapse_start <- Sys.time()

collapse_draws <- function(draw) {
  draw_tmp <- copy(full)[, daily_temp_cat := as.integer(round(sd
                                                            * Rnorm(.N, 0, 1)
                                                            + daily_temp_cat))]
  draw_tmp <- draw_tmp[, lapply(.SD, sum), by = .(temp_zone, daily_temp_cat),
                       .SDcols = 'pop']
  draw_tmp[, draw := as.integer(draw)]

  return(draw_tmp)
  }

draws <- rbindlist(mclapply(0:max_draw, collapse_draws, mc.cores = threads))

collapse_end <- Sys.time()
message('Reshape to long & collapse completed for all zones in ',
        round(difftime(collapse_end, collapse_start, units = 'secs'), 1),
        ' seconds')


# Save the output #
write.csv(draws, file = file.path(out_dir, paste0(job_name, '.csv')),
          row.names = F)

message('File exported')
message('Job took ', round(difftime(Sys.time(), start_time, units = 'mins'), 1),
        ' minutes to complete')