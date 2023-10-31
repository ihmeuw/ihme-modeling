#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#                   Custom subnational GBD logit raking function                      #
#                                  AUTHORS
#                                18 Feb 2021                                          #
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#


#' Modified from MBG calculate_raking_factors and apply_raking_factors functions
#' @param rake_targets List of raking targets with location and year
#' @param gbd_loc_id gbd location id that matches name in rake_targets
#' @param cell_pred draws from subnational location
#' @param nyears number of years in data
#' @param year_list list of years in data
#' @param population vector of population previously pulled using get_population function
#' @param year_list integer vector of years
#' @param save_vec default `NULL`. If null, uses names from subnational_rake_output. Otherwise takes a character vector with the names of objects in subnational_rake_output to save.
#' @param save_draws_as character string default `RDS`. additional supported arguments `rds` and `Rdata`. The file type to save raked draws. If using `RDS` or `rds`, the file extension will be .RDS or .rds accordingly.
#'
#' @return raked "cell_pred"

gbd_raking_level4 <- function(rake_targets, gbd_loc_id, cell_pred, nyears, year_list, age_group_ids){
  
  lyv = c('name','year', 'ages', 'value')
  rake_targets = copy(rake_targets[, lyv, with = F])
  setnames(rake_targets, lyv, c('loc', 'year', 'age', 'target'))
  
  sr_locs <- c(gbd_loc_id)
  cyas = setDT(expand.grid(loc = sr_locs, year = year_list, age = age_group_ids))
  start_row = nrow(cyas)
  cyas = merge(cyas, rake_targets, by= c('loc','year', 'age'), all.x= T)
  
  raker=data.table(loc_means = rowMeans(cell_pred),
                   loc = rep.int(gbd_loc_id, nyears), 
                   year = as.vector(cell_pred$year_id), 
                   age = as.vector(cell_pred$age_group_id), 
                   weight = as.vector(cell_pred$population))
  
  cell_pred_no_labels <- copy(cell_pred)
  cell_pred_no_labels <- cell_pred_no_labels[, c('location_id', 'year_id', 'age_group_id', 'sex_id', 'population'):=NULL]
  
  raker[,cell_pred_id := .I]
  
  #specify by_vars
  byvars = c('loc', 'year', 'age')
  
  #remove NA weight values from raker
  pre = dim(raker)[1]
  post = dim(raker)[1]
  
  if(pre != post){
    warning(paste0(pre - post, ' (', scales::percent((pre - post)/pre), ') pixels (over the whole cube) were removed because of NA weight_brick or pixel values'))
  }
  
  #for each country year, find the logit raking factor
  #redefine cys
  cyas = unique(raker[,.(loc, year, age)])
  rf = lapply(seq(nrow(cyas)), function(x) {
    
    #year and location
    theloc = cyas[x,loc]
    theyear = cyas[x,year]
    theage = cyas[x,age]
    
    message(theloc, " in ", theyear, " age group: ", theage)
    
    if (nrow(rake_targets[loc == theloc & year == theyear & age==theage]) == 0) {
      if(if_no_gbd == "return_na"){
        return(NA)
      } else {
        return(0)
      }
    } else if (rake_targets[loc == theloc & year == theyear & age==theage, .(target)] == 0 & zero_heuristic == T) {
      # catch true zeroes (i.e. pre-introduction of an intervention) and return -9999. This will speed things up & will replace later with 0
      return(-9999)
    } else {
      ret <- try(LogitFindK(gbdval     = rake_targets[loc == theloc & year == theyear & age==theage,.(target)],
                            pixelval   = cell_pred_no_labels[raker[loc == theloc & year == theyear & age==theage, cell_pred_id],], #pass the cell pred rows (without ids attached, or else it will break!) that corrospond to this country year age
                            weightval  = raker[loc == theloc & year == theyear & age==theage, weight],
                            MaxJump    = MaxJump,
                            MaxIter    = MaxIter,
                            FunTol     = FunTol,
                            approx_0_1 = approx_0_1))
      return(ret)
    }
  })
  
  cyas[,raking_factor := unlist(rf)]
  cyas = merge(cyas, rake_targets, all.x = T, by =c('loc', 'year', 'age'))
  
  #calculate the starting point post hoc for standardized reporting
  rak = raker[, list(px_mean = sum(loc_means * weight, na.rm = T), sumweight = sum(weight, na.rm = T)), by = byvars]
  rak[, start_point := px_mean/sumweight]
  rak = merge(rak, cyas, all.x = T, by = c('loc','year', 'age'))
  
  #raking factors at the cyas level 
  rak = rak[, .(loc, year, age, start_point, target, raking_factor)]
  rake_dt <- rak
  
  message('apply raking factors')
  ############## now apply raking factors
  cpdim = dim(cell_pred)
  thelocs = unique(cell_pred$location_id)
  thelocs = thelocs[!is.na(thelocs)]
  dt = setDT(expand.grid(loc = thelocs, year = year_list, age = age_group_ids))
  
  #merge on the raking factors and rake
  rake_dt = merge(dt, rake_dt, all.x = T, by = c('year', 'age'))
  rake_dt[, loc:=loc.x]
  rake_dt[, c('loc.x', 'loc.y'):=NULL]
  raked_cell_pred <- merge(cell_pred, rake_dt, all=T, by.x=c('location_id', 'year_id', 'age_group_id'), by.y=c('loc', 'year', 'age')) %>% draws_to_long
  raked_cell_pred[, raked_value:=invlogit(logit(value)+raking_factor)]
  
  return(raked_cell_pred)
  
}


#' @title Standard Raking Function
#'
#' @author AUTHOR
#'
#' @description A function used to rake mbg ouputs to GBD estimates. Can rake to either national level or subnational level (where available). Supports linear and logit raking. Optionally outputs summary rasters of raked cell pred object.
#'
#' @param cell_pred Cell pred object to be raked.
#' @param rake_to Df with `name`, `year`, and `mean` columns. values in name must match up with values in `field`.
#' @param reg character string - Region used to produce cell pred object.
#' @param year_list integer vector - vector of years
#' @param pop_measure character string - population measure (can be found in config of model run)
#' @param rake_method character string - must be either `linear` or `logit`
#' @param rake_subnational boolean default `T`. If true, uses the subnational raking shapefile, otherwise uses the adm0 shapefile. Make sure that values in `rake_to` contain the codes for level of raking chosen.
#' @param crosswalk Boolean default `T`, for models run before the new gaul shapefiles ready on 7/6/18, the cell_pred needs to be crosswalked to match the subnational raking raster. Introduces NAs into raster where the new simple raster has values but the original does not.
#' @param shapefile_path character string -Path to shapefile that will be used for raking. Preset to subnational raking shapefile, don't change.
#' @param field character string - Field in shapefile that has admin identifiers that match with rake_to. Preset to ihme_loc_ids at the lowest level in the shapefile. Don't change.
#' @param zero_heuristic Boolean default `F`.  If logit raking, this will automatically set rf = -9999 for any country-year with a target value of 0.  This produces a raked value of 0 for all pixels.  Raking to a target of zero in logit space is very time-consuming and the algorithm
#'                       can only approach zero asymptotically.  For situations where the target is truly zero (most useful for, say, an intervention pre-introduction) this will both speed up the process and ensure that zeros are returned.
#' @param approx_0_1 Boolean default `F`. If logit raking, any values of zero will be replaced with 1e-10 and values of 1 will be replaced with (1-(1e-10)).  Otherwise, logit transformations will fail in `NewFindK`. Useful if some areas have very low or high predicted values in `cell_pred`,
#'                   such that some draws are either 0 or 1 (or extremely close to these values).
#' @param simple_raster default `NULL`, option to pass in simple raster to function if it's been loaded already. NOTE: if the pop raster is not being passed in as well, the simple_polygon needs to be supplied as well. There is a check to ensure this.
#' @param simple_polygon default `NULL`, option to pass in simple polygon if its been loaded already. This is necessary if the simple raster is being passed in, but the pop raster is not. The covariate loading function requires a simple polygon.
#' @param pop_raster default `NULL`, option to pass in pop raster if its been loaded already.
#'
#' Additional Parameters (...)
#' @param if_no_gbd default `return_na`, other option `return_unraked`. If return_na, any location-years without gbd estimates will return NA raking factors. if return_unraked, will any location-years without gbd estimates will return 1 for linear raking, 0 for logit raking.
#' @param MaxJump default `10`. Maximum size of a jump to the answer (for logit raking).
#' @param MaxIter default `80`. Number of jumps towards the solution (for logit raking)
#' @param FunTol default `1e-5`. Maximum allowed difference between the raking target and raked results (for logit raking)
#' @param iterate default `F`. If logit raking for a location-year fails, try again with `MaxJump` and `MaxIter` times 10. If that fails, try again times 100. For circumstances where raking target is very far from estimate and raking does not converge.
#' @param modeling_shapefile_version string identifying version of of shapefile used in modeling
#' @param raking_shapefile_version string identifying version of of shapefile to use in raking
#' @param if_no_gbd default `return_na`, other option `return_unraked`. If return_na, any location-years without gbd estimates will return NA raking factors. if return_unraked, will any location-years without gbd estimates will return 1 for linear raking, 0 for logit raking.
#' @param custom_raking_shapefile SPDF object -shapefile that will be used for raking. Used for passing in custom raking shapefile for choosing subnational countries to rake to. See `make_custom_raking_shapefile()`
#' @param countries_not_to_subnat_rake as it sounds. Used for constructing raking raster. Default: NULL
#'
#' @return Returns a named list with a raked cell pred object, simple raster used for raking, raking factors, and (optional) rasters of mean, lower, upper, and cirange for years in year list
#'
#' @export
rake_cell_pred <- function(cell_pred,
                           rake_to,
                           reg,
                           year_list,
                           pop_measure,
                           rake_method = "linear",
                           rake_subnational = T,
                           crosswalk = F,
                           shapefile_path = get_admin_shapefile(admin_level = 0, raking = T, version = modeling_shapefile_version),
                           field = "loc_id",
                           zero_heuristic = F,
                           approx_0_1 = F,
                           simple_raster = NULL,
                           simple_polygon = NULL,
                           pop_raster = NULL,
                           modeling_shapefile_version = "current",
                           raking_shapefile_version = "current",
                           if_no_gbd = "return_na",
                           custom_raking_shapefile = NULL,
                           countries_not_to_subnat_rake = NULL,
                           ...) {
  if (!rake_method %in% c("linear", "logit")) {
    stop("rake_method must be either linear or logit")
  }
  
  if (!is.null(simple_raster) & is.null(simple_polygon) & is.null(pop_raster)) {
    stop("If you want to pass in a simple raster directly, you must also pass in either a simple_polygon or pop raster")
  }
  
  if (length(year_list) == 1) {
    interval_mo <- 12
  } else {
    # Calculate interval month, number of months between years in year list
    year_diff <- diff(year_list)
    if (length(unique(year_diff)) != 1) {
      stop("Please use annual or 5-year intervals exclusively in year_list")
    } else {
      interval_mo <- year_diff[[1]] * 12
    }
  }
  
  message("\n        Starting Raking for Region: ", reg)
  message("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  message("     Loading Necessary Shapefiles and Rasters")
  message("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
  
  ## load objects used in modeling
  if (is.null(simple_raster)) {
    ## get simple polygon and simple raster used to produce cell pred
    message("Loading simple polygon")
    simple_polygon <- load_simple_polygon(
      gaul_list = get_adm0_codes(reg,
                                 shapefile_version = modeling_shapefile_version
      ),
      buffer = 0.4,
      shapefile_version = modeling_shapefile_version
    )
    subset_shape <- simple_polygon[["subset_shape"]]
    simple_polygon <- simple_polygon[["spoly_spdf"]]
    
    message("Loading simple raster\n")
    raster_list <- build_simple_raster_pop(subset_shape)
    simple_raster <- raster_list[["simple_raster"]]
  } else {
    message("Using supplied simple raster")
  }
  
  ## now we load objects to be used in raking
  gaul_list <- get_adm0_codes(reg, shapefile_version = raking_shapefile_version)
  
  # if not raking subnationally, use the admin0 shapefile
  if (rake_subnational == F) {
    message("You have chosen not to rake subnationally; make sure you are using national level raking factors from GBD\n")
    message("Loading national raking shapefile")
    shapefile_path <- get_admin_shapefile(admin_level = 0, raking = F, version = raking_shapefile_version)
    
    location_metadata <- get_location_code_mapping(shapefile_version = raking_shapefile_version)
    location_metadata <- location_metadata[, c("GAUL_CODE", "loc_id")]
    
    shapefile <- readOGR(shapefile_path, stringsAsFactors = FALSE, GDAL1_integer64_policy = TRUE)
    shapefile <- shapefile[shapefile$ADM0_CODE %in% gaul_list, ]
    
    # merge on loc_ids for making simple raster
    shapefile <- sp::merge(shapefile, location_metadata, by.x = "ADM0_CODE", by.y = "GAUL_CODE", all.x = T)
    shapefile@data[is.na(shapefile@data$loc_id), "loc_id"] <- -1
  } else {
    # loading in subnational shapefile and subsetting for speed
    if (is.null(custom_raking_shapefile)) {
      message("Loading subnational raking shapefile")
      shapefile <- readOGR(shapefile_path, stringsAsFactors = FALSE, GDAL1_integer64_policy = TRUE)
    } else {
      message("Using custom raking shapefile")
      shapefile <- custom_raking_shapefile
    }
    gaul_list <- get_adm0_codes(reg, shapefile_version = raking_shapefile_version)
    shapefile <- shapefile[shapefile$ADM0_CODE %in% gaul_list, ]
    shapefile@data[, field] <- as.numeric(as.character(shapefile@data[, field]))
  }
  
  # get simple raster from new gbd shapefile
  message("Loading raking raster\n")
  new_simple_polygon <- load_simple_polygon(
    gaul_list = NULL, ## doesn't matter since custom_shapefile specified
    buffer = 0.4, custom_shapefile = shapefile
  )
  new_subset_shape <- new_simple_polygon[["subset_shape"]]
  new_simple_polygon <- new_simple_polygon[["spoly_spdf"]]
  
  message("Loading simple raster\n")
  raking_link_table <- build_raking_link_table(shapefile_version = raking_shapefile_version, force_adm0 = !rake_subnational, countries_not_to_subnat_rake = countries_not_to_subnat_rake)
  new_raster_list <- build_simple_raster_pop(new_subset_shape, field = field, link_table = raking_link_table)
  new_simple_raster <- new_raster_list[["simple_raster"]]
  
  # get extents of original and simple raster to line up - extend and crop just in case
  new_simple_raster <- extend(new_simple_raster, simple_raster, values = NA)
  new_simple_raster <- crop(new_simple_raster, extent(simple_raster))
  new_simple_raster <- mask(new_simple_raster, simple_raster)
  
  # pulling values from raster into a list for comparison
  simple_extract <- raster::extract(simple_raster, extent(simple_raster))
  new_simple_extract <- raster::extract(new_simple_raster, extent(new_simple_raster))
  simple_extract <- na.omit(simple_extract)
  new_simple_extract <- na.omit(new_simple_extract)
  
  # test that simple raster matches cell pred
  if ((length(simple_extract) * length(year_list)) != nrow(cell_pred)) {
    stop(paste0("the simple raster for region: ", reg, " does not match the cell pred object - make sure that the code for the simple raster has not changed since running the function."))
  }
  
  # force crosswalk if unfortunate misshape happens:
  if (((length(new_simple_extract) * length(year_list)) != nrow(cell_pred)) | (length(cellIdx(simple_raster)) != length(cellIdx(new_simple_raster)))) {
    warning("cellIdx not aligned. Forcing crosswalk to TRUE because this will fail moving onwards. Please use fractional raking :(")
    crosswalk <- TRUE
  }
  
  # see documentaion for details on crosswalk
  if (crosswalk) {
    message("crosswalk = T; now altering cell pred to match raking raster")
    message("Warning: This can drop or introduce NA rows into cell pred, see")
    message("crosswalk_cell_pred_add_NA() for more information \n")
    cell_pred <- crosswalk_cell_pred_add_NA(simple_raster, new_simple_raster, cell_pred, year_list)
    
    # make sure crosswalk worked
    if ((length(new_simple_extract) * length(year_list)) != nrow(cell_pred)) {
      stop(paste0("the simple raster for region: ", reg, " does not match the cell pred object - Problem with Crosswalk"))
    }
  } else {
    # make sure the number of non-na pixels in simple raster and raking raster are the same
    if (length(simple_extract) != length(new_simple_extract)) {
      stop(paste0("simple raster for ", reg, " has a different number of non-na pixels than the raking raster - use crosswalk = T in subnational_rake function call"))
    }
  }
  
  if (is.null(pop_raster)) {
    message("Loading population raster")
    ## Pull 2000-2015 annual population brick using new covariates function
    if (class(year_list) == "character") year_list <- eval(parse(text = year_list))
    pop_raster_annual <- load_worldpop_covariate(template_raster = simple_polygon,
                                                 pop_measure = pop_measure,
                                                 pop_release = pop_release,
                                                 start_year = min(year_list),
                                                 end_year = max(year_list),
                                                 interval = as.numeric(interval_mo))[[1]]
  } else {
    message("Using supplied population raster")
    pop_raster_annual <- pop_raster
  }
  
  ## extend and crop pop raster to ensure it matches the raking raster
  pop_raster_annual <- raster::extend(pop_raster_annual, new_simple_raster, values = NA)
  pop_raster_annual <- raster::crop(pop_raster_annual, extent(new_simple_raster))
  pop_raster_annual <- raster::setExtent(pop_raster_annual, new_simple_raster)
  pop_raster_annual <- raster::mask(pop_raster_annual, new_simple_raster)
  
  ## check to ensure the pop raster matches the raking raster in extent and resolution
  if (extent(pop_raster_annual) != extent(new_simple_raster)) {
    stop("population raster extent does not match simple raster")
  }
  if (any(res(pop_raster_annual) != res(new_simple_raster))) {
    stop("population raster resolution does not match simple raster")
  }
  
  message("\n     Finished loading shapefiles and rasters")
  message("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  message("     Calculating and Applying Raking Factors")
  message("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
  
  # calculate raking factors
  message("rake_method ", rake_method, " was specified\n")
  message("Calculating raking factors\n")
  rf <- calculate_raking_factors(cell_pred,
                                 rake_to,
                                 lyv = c("name", "year", "mean"),
                                 year_list,
                                 simple_raster = new_simple_raster,
                                 weight_brick = pop_raster_annual,
                                 rake_method = rake_method,
                                 zero_heuristic = zero_heuristic,
                                 approx_0_1 = approx_0_1,
                                 if_no_gbd = if_no_gbd,
                                 ...
  )
  
  # apply raking factors
  message("Raking cell pred object")
  raked_cell_pred <- apply_raking_factors(cell_pred,
                                          simple_raster = new_simple_raster,
                                          rake_dt = rf,
                                          rake_method = rake_method,
                                          force_simp_ras_dt_match = F
  )
  
  
  message("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  message("     Raking for Region: ", reg, " Complete")
  message("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
  
  outputlist <- list(
    "raked_cell_pred" = raked_cell_pred,
    "new_simple_raster" = new_simple_raster,
    "simple_raster" = simple_raster,
    "raking_factors" = rf
  )
  
  return(outputlist)
}


#' Makes summary rasters from cell pred
#'
#' @description wrapper function for make_cell_pred_summary
#'
#' @param cell_pred cell pred object
#' @param simple_raster admin raster matching the cell pred object
#' @param summary_measures charater vector - functions to summarize by. see default below
#' @param raked boolean default `T` - adds "raked" or "unraked" to name of output
#'
#' @return a named list with a raster brick for each summary measure
#'
make_summary_rasters <- function(cell_pred,
                                 simple_raster,
                                 summary_measures = c('mean','cirange','lower','upper'),
                                 raked = T) {
  
  message('\nMaking summary rasters from cell pred')
  outputlist <-
    lapply(summary_measures, function(x) {
      message("   ", x, ":")
      make_cell_pred_summary(draw_level_cell_pred = cell_pred,
                             mask                 = simple_raster,
                             return_as_raster     = TRUE,
                             summary_stat         = x)
    })
  names(outputlist) <- paste0(summary_measures, ifelse(raked, "_raked", "_unraked"), "_raster")
  
  message("\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  message("           Summary Rasters Complete" )
  message("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
  
  return(outputlist)
}

#' Save Outputs from  Raking Function
#'
#' @param rake_output Output list from subnational raking function
#' @param outdir character string - Directory for files to be saved to
#' @param indicator charater string - Name of indicator being modeled
#' @param age_group character string - Name of age group
#' @param prefix Character string to be added to start of file name to avoid overwriting non-custom files in folder
#' @param reg character string - Name of region that was modeled
#' @param year_list integer vector of years
#' @param save_vec default `NULL`. If null, uses names from subnational_rake_output. Otherwise takes a character vector with the names of objects in subnational_rake_output to save.
#' @param save_draws_as character string default `RDS`. additional supported arguments `rds` and `Rdata`. The file type to save raked draws. If using `RDS` or `rds`, the file extension will be .RDS or .rds accordingly.
#'
#' @return NULL
#'
#' @examples    save_custom_raking_outputs(custom_rake_output,
#'                                         outdir = sprintf('<<< FILEPATH REDACTED >>>>'),
#'                                         indicator,
#'                                         age_group,
#'                                         prefix = "custom_india",
#'                                         reg = "south_asia")
#'
save_raking_outputs <- function(rake_output,
                                outdir,
                                indicator,
                                prefix,
                                reg,
                                year_list,
                                save_vec = NULL,
                                save_draws_as = 'RDS') {
  
  ol <- rake_output
  
  if (is.null(save_vec)) {
    save_vec <- names(ol)
  }
  
  if (prefix != "") {
    prefix <- paste0(prefix, "_")
  }
  
  if("raked_cell_pred" %in% save_vec) {
    raked_cell_pred <- ol[["raked_cell_pred"]]
    if (save_draws_as %in% c("rds", "RDS")) {
      saveRDS(raked_cell_pred, file=sprintf('%s/%s%s_raked_cell_draws_eb_bin0_%s_0.%s',outdir, prefix, indicator, reg, save_draws_as))
    } else if (save_draws_as == "RData") {
      save(raked_cell_pred, file=sprintf('%s/%s%s_raked_cell_draws_eb_bin0_%s_0.RData',outdir, prefix, indicator, reg))
    }
  }
  if("new_simple_raster" %in% save_vec) {
    simple_raster <- ol[["new_simple_raster"]]
    writeRaster(simple_raster, file= sprintf('%s/%s%s_%s_simple_raster', outdir, prefix, indicator,reg), format = "GTiff", overwrite=T)
  }
  if("raking_factors" %in% save_vec) {
    raking_factors <- ol[["raking_factors"]]
    write.csv(raking_factors, file=sprintf('%s/%s%s_%s_rf.csv',outdir, prefix, indicator, reg))
  }
  
  #get all rasters in save vec
  rasters <- ol[grep("raster$", names(ol))]
  #remove simple raster, which is saved separately
  rasters <- rasters[names(rasters) != "new_simple_raster"]
  #loop through rasters and save them all using their list names
  for(ras in names(rasters)){
    if(ras %in% save_vec) {
      ras_name <- substring(ras, 1, nchar(ras) - 7)
      writeRaster(rasters[[ras]], file= sprintf('%s/%s%s_%s_%s_%i_%i', outdir, prefix, indicator, ras_name, reg, min(year_list), max(year_list)), format = "GTiff", overwrite=T)
    }
  }
}

#' Make Custom Raking Shapefile
#'
#' @title Make Custom Raking Shapefile
#'
#' @description A function used to generate a new shapefile where adm1 polygons in `countries` are replaced with their corresponding adm0 polygon
#'
#' @param countries character vector of iso3 codes to change from subnational to national. Any subnational not in this list (out of "ETH", "KEN", "CHN", "ZAF", "BRA", "IND", "IDN", "IRN", "MEX", "NGA", "PAK", "PHL") will stay as ADM1
#' @param raking_shapefile_version standard shapefile version date (YYYY_MM_DD)
#'
#' @export
#'
#' @return Returns a new raking shapefile with adm1 polygons switched out with adm0 polygons for the specified country. stops if invalid country iso3s are passed in.
#'
make_custom_raking_shapefile <- function(countries,
                                         raking_shapefile_version) {
  
  #if countries is in condensed form (i.e. "CHN+BRA") separate into character vector
  if (all(grepl("+", countries, fixed = T))) {
    countries <- strsplit(countries, "[+]", fixed = FALSE, perl = FALSE, useBytes = FALSE)[[1]]
  }
  
  #get mapping of gadm codes to ihme loc ids
  stage_list <- fread('<<< FILEPATH REDACTED >>>>')
  
  #check that countries recieved valid subnational iso3s
  subnational_countries <- c("ETH", "KEN", "CHN", "ZAF", "BRA", "IND", "IDN", "IRN", "MEX", "NGA", "PAK", "PHL")
  if (!all(toupper(countries) %in% subnational_countries)){
    stop("Invalid ISO3 passed to countries, must be one or several of the following: ", paste(subnational_countries, collapse = ", "))
  }
  
  message("Making custom raking shapefile with subnationals: ", paste(subnational_countries[!(subnational_countries %in% countries)], collapse = ", "))
  
  #subset subnational countries to the ones requested
  subnational_countries  <- subnational_countries[!(subnational_countries %in% countries)]
  #read in raking and adm0 shapefiles
  raking_shp <- readOGR(get_admin_shapefile(admin_level = 0, raking = T, version = raking_shapefile_version), stringsAsFactors = F, verbose = F)
  adm0_shp <- readOGR(get_admin_shapefile(admin_level = 0, raking = F, version = raking_shapefile_version), stringsAsFactors = F, verbose = F)
  
  #check that all expected subnationals are present in the shapfile
  subnational_country_codes <- data.table("ADM0_CODE" = as.numeric(unique(raking_shp@data[raking_shp@data$ad_level == 1,]$ADM0_CODE)))
  subnational_iso3s <- merge(stage_list[, c("iso3", "gadm_geoid")], subnational_country_codes, by.x = "gadm_geoid", by.y = "ADM0_CODE")
  if (!all(subnational_countries %in% subnational_iso3s$iso3)) {
    message("The following countries are not present subnationally in the raking shapefile and will be used nationally: ", paste(subnational_countries[!subnational_countries %in% subnational_iso3s$iso3], collapse = ", "))
    message("All subnationals are included in admin shapefiles from 4/2019 or later")
  }
  
  #get all subnational country codes that were not passed in to countries -
  country_codes <- get_adm0_codes(countries, shapefile_version = raking_shapefile_version)
  
  #remove countries from raking shapefile, keep only those countries in adm0 shapefile
  raking_shp <- raking_shp[!(raking_shp$ADM0_CODE %in% country_codes),]
  adm0_shp <- adm0_shp[adm0_shp$ADM0_CODE %in% country_codes,]
  
  #add on fields to adm0 shapefile in order to merge to raking shapefile
  adm0_shp@data$ad_level <- 0
  adm0_shp@data$ADM1_CODE <- 0
  adm0_shp@data$ADM1_NAME <- "NA"
  adm0_shp@data <- merge(adm0_shp@data, stage_list[,c("gadm_geoid", "loc_id")], by.x = "ADM0_CODE", by.y = "gadm_geoid", all.x = T)
  
  #merge shapefiles and return
  new_shp <- rbind(raking_shp, adm0_shp, makeUniqueIDs = TRUE)
  
  return(new_shp)
}

#' Crosswalk Cell Pred Object
#'
#' This version of the crosswalk adds NAs to the cell pred where there are pixels in the new simple
#' raster but not in the old. Reversible.
#'
#' @param simple_raster Simple raster used to create the cell pred object (created by build_simple_raster_pop).
#' @param new_simple_raster Simple raster to be crosswalked to (created by build_simple_raster_pop).
#' @param cell_pred Cell pred object that matches with simple raster.
#' @param year_list List of years
#'
#' @return Returns crosswalked cell pred object
#' @export
#'
#' @examples new_pred_object <- crosswalk_cell_pred(simple_raster, new_simple_raster, cell_pred, years)
#' @note This function loops through each pixel in the rasters and compares them. pixels where both are NA or both have values are kept the same. Pixels where the original raster has a value and the new is na have the row in the cell pred object deleted. For pixels where the original is na and the new has a value, an NA row is added to the cell pred object.
#'
crosswalk_cell_pred_add_NA <- function(simple_raster, new_simple_raster, cell_pred, year_list) {
  #number of years in year list
  years <- length(year_list)
  #get a list with number of rows equal to one year of cell pred object
  rows <- c(1:(nrow(cell_pred) / years))
  #initialize vector to track row indices to drop
  rows_to_drop <- c()
  #initialize vector to track row ids to add
  rows_to_add <- c()
  #initialize to count times where pixel in new raster is set to NA
  new_pixels_set_NA <- 0
  #intitialize to track cell_pred index
  cell_pred_pointer <- 1
  #convert rasters to list for speed purposes
  simple_raster_list <- raster::extract(simple_raster, extent(simple_raster))
  new_simple_raster_list <- raster::extract(new_simple_raster, extent(new_simple_raster))
  #loop through raster and compare values for each pixel
  for(i in 1:length(simple_raster_list)) {
    #if both rasters are NA, do nothing
    if(is.na(simple_raster_list[i]) & is.na(new_simple_raster_list[i])) {
      next
      #if both rasters are not NA, advance cell_pred_pointer
    } else if (!is.na(simple_raster_list[i]) & !is.na(new_simple_raster_list[i])) {
      cell_pred_pointer <- cell_pred_pointer + 1
      #if the original raster is not na and the new raster is na, add row index for deletion from cell pred
    } else if(!is.na(simple_raster_list[i]) & is.na(new_simple_raster_list[i])) {
      rows_to_drop <- c(rows_to_drop, cell_pred_pointer)
      cell_pred_pointer <- cell_pred_pointer + 1
      #if the original raster is na and the new raster is not na, set pixel in new raster to NA
    } else if (is.na(simple_raster_list[i]) & !is.na(new_simple_raster_list[i])) {
      rows_to_add <- c(rows_to_add, cell_pred_pointer)
    }
  }
  #get boolean vector for dropping
  drop_logical <- rows %in% rows_to_drop
  #duplicate vector for each year in the
  drop_logical <- rep(drop_logical, times=years)
  
  #add id column to cell_pred
  rows_to_add_id <- 1:nrow(cell_pred)
  cell_pred <- cbind(rows_to_add_id, cell_pred)
  
  #drop rows in cell pred
  cell_pred <- cell_pred[!drop_logical,]
  
  if(length(rows_to_add) > 0) {
    #calculate row positions for new rows to be added in
    rows_to_add <- rows_to_add - 1 + 0.00001
    
    if(length(rows_to_add) == 1){
      rows_to_add[1] <- rows_to_add[1] + 0.00001
    } else {
      for(i in 2:length(rows_to_add)){
        while(rows_to_add[i] <= rows_to_add[i-1]){
          rows_to_add[i] <- rows_to_add[i] + 0.00001
        }
      }
    }
    
    #duplicate row positions for multiple years
    dup <- rows_to_add
    for(i in 2:years) {
      new <- dup + (length(rows) * (i - 1))
      rows_to_add <- c(rows_to_add, new)
    }
    
    #construct NA matrix
    add_matrix <- matrix(nrow=length(rows_to_add), ncol= (ncol(cell_pred) - 1))
    add_matrix <- cbind(rows_to_add, add_matrix)
    
    #add NA matrix to cell pred, sort by row id and drop row id column
    cell_pred <- rbind(cell_pred, add_matrix)
    cell_pred <- cell_pred[order(cell_pred[,1]),]
  }
  
  cell_pred <- cell_pred[, 2:ncol(cell_pred)]
  return(cell_pred)
}

#' Crosswalk Cell Pred Object
#'
#' This version of the crosswalk edits the new simple raster to match the cell pred by making any
#' pixel not in the original simple raster na. Nonreversible.
#'
#' @param simple_raster Simple raster used to create the cell pred object (created by build_simple_raster_pop).
#' @param new_simple_raster Simple raster to be crosswalked to (created by build_simple_raster_pop).
#' @param cell_pred Cell pred object that matches with simple raster.
#' @param year_list List of years
#'
#' @return Returns crosswalked cell pred object and altered new simple raster
#' @export
#'
#' @examples new_pred_object <- crosswalk_cell_pred(simple_raster, new_simple_raster, cell_pred, years)
#' @note This function loops through each pixel in the rasters and compares them. pixels where both are NA or both have values are kept the same. Pixels where the original raster has a value and the new is na have the row in the cell pred object deleted. For pixels where the original is na and the new has a value, that pixel in the new raster is changed to an NA because there is no corresponding value in the cell pred object for that pixel.
#'
crosswalk_cell_pred_alter_raster <- function(simple_raster, new_simple_raster, cell_pred, year_list) {
  #number of years in year list
  years <- length(year_list)
  #get a list with number of rows equal to one year of cell pred object
  rows <- c(1:(nrow(cell_pred) / years))
  #initialize vector to track row indices to drop
  rows_to_drop <- c()
  #initialize to count times where pixel in new raster is set to NA
  new_pixels_set_NA <- 0
  #intitialize to track cell_pred index
  cell_pred_pointer <- 1
  #convert rasters to list for speed purposes
  simple_raster_list <- raster::extract(simple_raster, extent(simple_raster))
  new_simple_raster_list <- raster::extract(new_simple_raster, extent(new_simple_raster))
  #loop through raster and compare values for each pixel
  for(i in 1:length(simple_raster_list)) {
    #if both rasters are NA, do nothing
    if(is.na(simple_raster_list[i]) & is.na(new_simple_raster_list[i])) {
      next
      #if both rasters are not NA, advance cell_pred_pointer
    } else if (!is.na(simple_raster_list[i]) & !is.na(new_simple_raster_list[i])) {
      cell_pred_pointer <- cell_pred_pointer + 1
      #if the original raster is not na and the new raster is na, add row index for deletion from cell pred
    } else if(!is.na(simple_raster_list[i]) & is.na(new_simple_raster_list[i])) {
      rows_to_drop <- c(rows_to_drop, cell_pred_pointer)
      cell_pred_pointer <- cell_pred_pointer + 1
      #if the original raster is na and the new raster is not na, set pixel in new raster to NA
    } else if (is.na(simple_raster_list[i]) & !is.na(new_simple_raster_list[i])) {
      new_simple_raster[i] <- NA
      new_pixels_set_NA <- new_pixels_set_NA + 1
    }
  }
  #get boolean vector for dropping
  drop_logical <- rows %in% rows_to_drop
  #duplicate vector for each year in the
  drop_logical <- rep(drop_logical, times=years)
  
  #drop rows in cell pred
  cell_pred <- cell_pred[!drop_logical,]
  
  #return new cell_pred and simple_raster
  return(list(cell_pred, new_simple_raster))
}


#' a function to logit rake MBG results
#'
#' @param cell_pred matrix. draws from an mbg model in the cell pred form
#' @param rake_targets data.table with at least 3 columns (specified by lyv) with gaul code, year and val to rake towards
#' @param lyv character vector of length 3. Denotes the gaul_code column, year column and value column-- in that order
#' @param year_list numeric vector. Vector of years implied by the cell pred.
#' @param simple_raster raster. Should be the rasterized version of a shapefile or otherwise denoting the gaul/admin codes specified in rake targets
#' @param weight_brick rasterbrick. Raster brick where the values are the spatial weightings (usually population).
#' @param rake_method character. One of 'linear' or 'logit'. Possibly more in the future
#' @param if_no_gbd default `return_na`, other option `return_unraked`. If return_na, any location-years without gbd estimates will return NA raking factors. if return_unraked, will any location-years without gbd estimates will return 1 for linear raking, 0 for logit raking.
#' @param MaxJump default `10`. Maximum size of a jump to the answer for logit raking.
#' @param MaxIter default `80`. Number of jumps towards the solution
#' @param FunTol default `1e-5`. Maximum allowed difference between the raking target and raked results
#' @param iterate default `F`. If logit raking for a location-year fails, try again with `MaxJump` and `MaxIter` times 10. If that fails, try again times 100. For circumstances where raking target is very far from estimate and raking does not converge.
#' @param zero_heuristic default `F`.  If logit raking, this will automatically set rf = -9999 for any country-year with a target value of 0.  This produces a raked value of 0 for all pixels.  Raking to a target of zero in logit space is very time-consuming and the algorithm
#'                       can only approach zero asymptotically.  For situations where the target is truly zero (most useful for, say, an intervention pre-introduction) this will both speed up the process and ensure that zeros are returned.
#' @param approx_0_1 default `F`. If logit raking, any values of zero will be replaced with 1e-10 and values of 1 will be replaced with (1-(1e-10)).  Otherwise, logit transformations will fail in `NewFindK`. Useful if some areas have very low or high predicted values in `cell_pred`,
#'                   such that some draws are either 0 or 1
#'
#' @usage a = calculate_raking_factors(cell_pred, aaa[[1]], year_list = 2000:2015, simple_raster = simple_raster, weight_brick = pop_raster_annual, rake_method = 'linear')
#' @usage b = calculate_raking_factors(cell_pred, aaa[[1]], year_list = 2000:2015, simple_raster = simple_raster, weight_brick = pop_raster_annual, rake_method = 'logit')
#' @import data.table
calculate_raking_factors = function(cell_pred,
                                    rake_targets,
                                    lyv = c('name','year', 'mean'),
                                    year_list,
                                    simple_raster,
                                    weight_brick,
                                    rake_method = c('logit', 'linear'),
                                    if_no_gbd = "return_na",
                                    MaxJump = 10,
                                    MaxIter = 80,
                                    FunTol = 1e-5,
                                    iterate = F,
                                    zero_heuristic = F,
                                    approx_0_1 = F){
  #check to make sure rake targets is a data table
  setDT(rake_targets)
  
  #check to make sure rake targets has valid columns
  if(!all(lyv %in% names(rake_targets))){
    stop('rake_targets does not contain all the columns specified by lyv')
  }
  
  #scoping
  rake_targets = copy(rake_targets[, lyv, with = F])
  setnames(rake_targets, lyv, c('loc', 'year', 'target'))
  
  #check to make sure all country years are represented
  sr_locs = unique(simple_raster[])
  sr_locs = sr_locs[!is.na(sr_locs)]
  cys = setDT(expand.grid(loc = sr_locs, year = year_list))
  start_row = nrow(cys)
  cys = merge(cys, rake_targets, by= c('loc','year'), all.x= T)
  
  #print missing GBD targets
  if(any(is.na(cys$target))){
    missing_targets <- cys[is.na(target),]
    for(i in 1:nrow(missing_targets)){
      message("no GBD raking targets for ihme_loc_id - ", missing_targets$loc[i], ", year - ", missing_targets$year[i])
    }
  }
  
  #check to make sure weight_brick has the same number of years as year list
  if(dim(weight_brick)[3] != length(year_list)){
    stop('year_list implies a different number of time steps than the weight brick')
  }
  
  #check to make sure simple raster, weight_brick, and cell pred all have the proper dimensions
  stopifnot(dim(simple_raster)[1:2] == dim(weight_brick)[1:2])
  
  #check to make sure cell pred is an accurate subset of simple raster and weight brick
  if(!dim(cell_pred)[1] / length(cellIdx(simple_raster)) == dim(weight_brick)[3]){
    stop('cell_pred and simple_raster dimensions are not aligned')
  }
  
  #match rake_method
  rake_method = match.arg(rake_method)
  
  ##assuming checks pass, calculate a data table from which raking factors can be calculated
  sri = cellIdx(simple_raster)
  ngoodpixels = length(sri)
  
  #figure out which pixels over time have good values
  sri_years = unlist(lapply(seq(year_list)-1, function(x) sri + (x * ncell(simple_raster))))
  nyears = length(year_list)
  
  #first format the weight_brick
  raker = data.table(pixel_means = rowMeans(cell_pred),
                     pixel_xyt_id = sri_years,
                     pixel_xy_id  = sri,
                     loc = rep.int(simple_raster[sri], nyears),
                     year = as.vector(outer(Y = year_list, X = rep.int(1, ngoodpixels))),
                     weight = weight_brick[][sri_years])
  raker[,cell_pred_id := .I]
  
  #specify by_vars
  byvars = c('loc', 'year')
  
  #remove NA weight values from raker
  pre = dim(raker)[1]
  raker = raker[!is.na(weight) & !is.na(pixel_means), ]
  post = dim(raker)[1]
  
  if(pre != post){
    warning(paste0(pre - post, ' (', scales::percent((pre - post)/pre), ') pixels (over the whole cube) were removed because of NA weight_brick or pixel values'))
  }
  
  if(rake_method=='linear'){
    #collapse by country year to get the linear adjustment
    rak = raker[, list(px_mean = sum(pixel_means * weight, na.rm = T), sumweight = sum(weight, na.rm = T)), by = byvars]
    rak[, start_point := px_mean/sumweight]
    #merge on the target
    rak = merge(rak, as.data.frame(rake_targets), all.x = T)
    rak[, raking_factor := target/start_point]
    
    if(if_no_gbd == "return_unraked"){
      rak[is.na(raking_factor), raking_factor := 1]
    }
    
  } else if(rake_method == 'logit'){
    
    #for each country year, find the logit raking factor
    #redefine cys
    cys = unique(raker[,.(loc, year)])
    rf = lapply(seq(nrow(cys)), function(x) {
      
      #year and location
      theloc = cys[x,loc]
      theyear = cys[x,year]
      
      if (nrow(rake_targets[loc == theloc & year == theyear]) == 0) {
        if(if_no_gbd == "return_na"){
          return(NA)
        } else {
          return(0)
        }
      } else if (rake_targets[loc == theloc & year == theyear, .(target)] == 0 & zero_heuristic == T) {
        # catch true zeroes (i.e. pre-introduction of an intervention) and return -9999. This will speed things up & will replace later with 0
        return(-9999)
      } else {
        ret <- try(LogitFindK(gbdval     = rake_targets[loc == theloc & year == theyear,.(target)],
                              pixelval   = cell_pred[raker[loc == theloc & year == theyear, cell_pred_id],], #pass the cell pred rows that corrospond to this country year
                              weightval  = raker[loc == theloc & year == theyear, weight],
                              MaxJump    = MaxJump,
                              MaxIter    = MaxIter,
                              FunTol     = FunTol,
                              approx_0_1 = approx_0_1))
        
        if(iterate) {
          # Iterate over larger values of MaxJump and NumIter if needed
          if (is.null(ret) | "try-error" %in% class(ret)) {
            message(paste0("Your GBD and MBG estimates are quite far apart for location ", theloc, " | year ", theyear))
            message("Increasing MaxJump and NumIter by 10-fold, but you should investigate this...")
            
            ret <- try(LogitFindK(gbdval     = rake_targets[loc == theloc & year == theyear,.(target)],
                                  pixelval   = cell_pred[raker[loc == theloc & year == theyear, cell_pred_id],], #pass the cell pred rows that corrospond to this country year
                                  weightval  = raker[loc == theloc & year == theyear, weight],
                                  MaxJump    = MaxJump*10,
                                  MaxIter    = MaxIter*10,
                                  FunTol     = FunTol,
                                  approx_0_1 = approx_0_1))
          }
          
          # If we get this far, the estimates are generally *really* far apart
          if (is.null(ret) | "try-error" %in% class(ret)) {
            message(paste0("Your GBD and MBG estimates are REALLY far apart for location ", theloc, " | year ", theyear))
            message("Increasing MaxJump and NumIter by 100-fold, but you should investigate this...")
            
            ret <- try(LogitFindK(gbdval     = rake_targets[loc == theloc & year == theyear,.(target)],
                                  pixelval   = cell_pred[raker[loc == theloc & year == theyear, cell_pred_id],], #pass the cell pred rows that corrospond to this country year
                                  weightval  = raker[loc == theloc & year == theyear, weight],
                                  MaxJump    = MaxJump*100,
                                  MaxIter    = MaxIter*100,
                                  FunTol     = FunTol,
                                  approx_0_1 = approx_0_1))
          }
          
          # Throw error if previous two didn't work
          if (is.null(ret) | "try-error" %in% class(ret)) {
            stop(paste0("Your GBD and MBG estimates are WAY TOO far apart for location ", theloc, " | year ", theyear, " - stopping."))
          }
        }
        
        return(ret)
      }
    })
    
    cys[,raking_factor := unlist(rf)]
    cys = merge(cys, rake_targets, all.x = T, by =c('loc', 'year'))
    
    #calculate the starting point post hoc for standardized reporting
    rak = raker[, list(px_mean = sum(pixel_means * weight, na.rm = T), sumweight = sum(weight, na.rm = T)), by = byvars]
    rak[, start_point := px_mean/sumweight]
    
    rak = merge(rak, cys, all.x = T, by = c('loc','year'))
  }
  
  #raking factors at the cys level
  rak = rak[, .(loc, year, start_point, target, raking_factor)]
  
  return(rak)
  
}

#' A function to apply raking factors to a cell pred
#'
#' @param cell_pred Matrix. Cell_pred object
#' @param simple_raster Raster. Should be the rasterized version of a shapefile or otherwise denoting the gaul/admin codes specified in rake_dt
#' @param rake_dt data.table Data.table output of calculate_raking_factors. Or at the very least, a three column data.table with columns for loc, year and raking_factor.
#' @param rake_method character string. Determines whether logit raking or linear scaling occurs
#' @param force_simp_ras_dt_match logical. Determines whether the function should break if it detects a difference between number of country years models and number of country years
#'                                         supplied to rake.
apply_raking_factors = function(cell_pred,
                                simple_raster,
                                rake_dt,
                                rake_method,
                                force_simp_ras_dt_match = T){
  
  cpdim = dim(cell_pred)
  
  #check to make sure simple raster and cell_pred play nice
  nyears = dim(cell_pred)[1] %% length(cellIdx(simple_raster)) != 0
  if(nyears){
    stop('good cells in simple raster is not a multiple of cell pred rows')
  }
  nyears = dim(cell_pred)[1] / length(cellIdx(simple_raster))
  thelocs = unique(as.vector(simple_raster))
  thelocs = thelocs[!is.na(thelocs)]
  
  if(force_simp_ras_dt_match){
    if(nrow(rake_dt) != (length(thelocs) * nyears)){
      stop('Combination of inputs suggest a disconnect between rake_dt and cell pred')
    }
  }
  
  #create comparison to cell pred to fill in the raking factors
  dt = rbindlist(lapply(unique(rake_dt[,year]), function(x) data.table(cell_xy_id = cellIdx(simple_raster),
                                                                       loc =simple_raster[][cellIdx(simple_raster)],
                                                                       year = x)))
  dt[, id:= .I]
  
  #merge on the raking factors
  rake_dt = merge(dt, rake_dt, all.x = T, by = c('loc','year'))
  
  #enfore prespecified order
  setorder(rake_dt, id)
  
  if(rake_method == "linear"){
    cell_pred = cell_pred * rake_dt[,raking_factor]
  }else{
    cell_pred = invlogit(logit(cell_pred) + rake_dt[,raking_factor])
  }
  
  return(cell_pred)
}


LogitFindK <- function(gbdval, pixelval, weightval, MaxIter = 40, MaxJump = 10, FunTol = 1e-5, approx_0_1){
  
  # Logit raking won't work with any values of 0 or 1 in cell_pred
  # Adjust values slightly to avoid -Inf or Inf in NewPixelVal
  if (approx_0_1) {
    pixelval[pixelval == 0] <- 1e-10
    pixelval[pixelval == 1] <- 1-(1e-10)
  }
  
  NumIter <- ceiling(-log2(FunTol / MaxJump))
  
  if(NumIter > MaxIter){
    stop(paste("Maximum number of iterations is less than projected iterations required:", NumIter / MaxIter))
  }
  
  CurrentError <- EvalDiff(gbdval, pixelval, weightval)
  if (CurrentError > 0){
    Range <- c(0, MaxJump)
  } else {
    Range <- c(-MaxJump, 0)
  }
  
  a <- Range[1]
  b <- Range[2]
  F_a <- EvalDiff(gbdval, NewPixelVal(a, pixelval), weightval)
  F_b <- EvalDiff(gbdval, NewPixelVal(b, pixelval), weightval)
  
  if (F_a * F_b > 0){
    stop("Your estimates are WAY too far away from GBD")
  } else {
    i <- 1
    c <- (a + b) / 2
    F_c <- EvalDiff(gbdval, NewPixelVal(c, pixelval), weightval)
    Success <- (abs(F_c) <= FunTol)
    while (!Success & i < NumIter){
      if (sign(F_c) == sign(F_a)){
        a <- c
        F_a <- F_c
      } else {
        b <- c
        F_b <- F_c
      }
      c <- (a + b) / 2
      F_c <- EvalDiff(gbdval, NewPixelVal(c, pixelval), weightval)
      Success <- (abs(F_c) <= FunTol)
      i <- i + 1
    }
    if (Success){
      return(c)
    } else {
      return(sprintf("Need more iterations, current output: K = %g, F(K) = %g",c, F_c))
    }
  }
}


NewPixelVal <- function(K, vals){
  return(ilogit(logit(vals)+K))
}


#vals: matrix
#weightval: vector
NewEst <- function(vals, weightval){
  
  vals = vals * weightval
  vals = apply(vals, 2, sum)
  vals = vals / sum(weightval)
  
  return(mean(vals))
}


EvalDiff <- function(gbdval, vals, weightval){
  return(gbdval - NewEst(vals, weightval))
}


logit <- function(x) {
  log(x/(1-x))
}


ilogit <- function(x) {
  exp(x)/(1+exp(x))
}


#overwrite cellIdx function to reduce outward dependancies
cellIdx <- function(x) which(!is.na(getValues(x[[1]])))
