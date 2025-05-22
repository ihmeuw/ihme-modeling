# Purpose: prepares data for stacking
  ## (1) import prevalence data
  ## (2) create simple polygon and raster
  ## (3) create covariate rasters
  ## (4) conduct a VIF analysis 
  ## (5) replace any NA covariate values with non-NA value of the nearest land pixel 

############ setup pipeline, paths, parameters, etc. ############ 
user <- Sys.info()["user"]
cause <- "ntd_lf" 
source("FILEPATH")

# setup preambles
## loads pipeline objects saved in the run_date/objects/node_id folder 
inputs <- if (interactive()) { # otherwise NULL
  list(
    run_date = run_date,
    indicator = indicator,
    indicator_group = indicator_group,
    node_id = 1,
    node_name = "j01_data_prep",
    dag_path = sprintf("FILEPATH"),
    loopvar_index = 1)}
pipeline_preamble(inputs = inputs)

# setup input paths
repo_dir <- "FILEPATH"
custom_fun_dir <- "FILEPATH"

# setup output paths
output_dir <- "FILEPATH"
cov_dir <- "FILEPATH"

# Populate global variables tied to loopvar_index.
assign("reg", pipeline$loopvars[loopvar_index, region])
assign("age", pipeline$loopvars[loopvar_index, age])
assign("holdout", pipeline$loopvars[loopvar_index, holdout])
assign("seed", pipeline$config_list$seed)

# Prep MBG inputs/Load Data -----------------------------------------------
## if skipping to INLA, then just quit job
if (as.logical(pipeline$config_list$skiptoinla)) {
  print("Skipping to INLA")

  ## Save out environment
  mbg_save_nodeenv(
    node = nodename,
    ig = pipeline$indicator_group,
    indic = pipeline$indicator,
    rd = pipeline$run_date,
    reg = reg,
    age = age,
    holdout = holdout,
    objs = ls()
  )

  ## Create output file and remove err file ##
  mbg_job_marker(type = "end", tmpdir = "~/mbgdir")

  q("no")
}

## Load simple polygon template to model over
gaul_list <- get_adm0_codes(
  reg,
  shapefile_version = pipeline$config_list$modeling_shapefile_version
)

use_sub_nats <- pipeline$config_list$use_sub_nats
use_custom_space <- pipeline$config_list$use_custom_space# assign year_list object
year_list <- pipeline$config_list$year_list

## load simple polygon based on whether using countries or sub-nationals
if (use_sub_nats == TRUE){
  source(paste0(custom_fun_dir,"load_simple_polygon_ADM1.R"))
  simple_polygon_list <- load_simple_polygon_ADM1(
    gaul_list = gaul_list, 
    buffer = 1, 
    tolerance = 0.4,
    shapefile_version = pipeline$config_list$modeling_shapefile_version, 
    use_IND_states = F, 
    model_just_IND = T)
} else { 
  if (use_custom_space == TRUE){
    simple_polygon_list <- load_simple_polygon(
      gaul_list = gaul_list, 
      buffer = 1, 
      tolerance = 0.4, 
      shapefile_version = pipeline$config_list$modeling_shapefile_version, 
      custom_shapefile_path = pipeline$config_list$custom_shapefile_path)
  } else {
    simple_polygon_list <- load_simple_polygon(
      gaul_list = gaul_list, 
      buffer = 1, 
      tolerance = 0.4,
      shapefile_version = pipeline$config_list$modeling_shapefile_version) 
  }
}

subset_shape <- simple_polygon_list[[1]]
simple_polygon <- simple_polygon_list[[2]]

## Load input data based on stratification and holdout, OR pull in data as normal and run with the whole dataset if holdout == 0
if (holdout != 0) {
  message(paste0(
    "Holdout != 0 so loading holdout data only from holdout ", holdout
  ))
  message(
    "Please be sure you have a list object called stratum_ho in your environment."
  )
  if (age != 0) {
    df <- as.data.table(
      pipline$stratum_ho[[paste("region", reg, "_age", age, sep = "__")]]
    )
  } else {
    df <- as.data.table(
      pipeline$stratum_ho[[paste("region", reg, sep = "__")]]
    )
  }
  df <- df[fold != holdout, ]
  df$first_entry <- 1
  df$agg_weight <- 1
} else {
  message("Holdout == 0 so loading in full dataset using load_input_data()")
  with_globals(
    new = list(
      run_date = pipeline$run_date,
      modeling_shapefile_version = pipeline$config_list$modeling_shapefile_version
    ),
    df <- load_input_data(
      indicator = gsub(paste0("_age", age), "", pipeline$indicator),
      indicator_group = pipeline$indicator_group,
      agebin = age,
      removeyemen = FALSE,
      date = pipeline$run_date,
      pathaddin = pipeline$get_path_addin(loopvar_index),
      years = pipeline$config_list$yearload,
      withtag = as.logical(pipeline$config_list$withtag),
      datatag = pipeline$config_list$datatag,
      use_share = as.logical(pipeline$config_list$use_share),
      yl = pipeline$config_list$year_list,
      poly_ag = pipeline$config_list$poly_ag,
      # converts text value e.g., "NULL" into proper scalar e.g., NULL
      zcol_ag = pipeline$eval_conf("zcol_ag"),
      region = reg
    )
  )
  df <- process_input_data(
    df,
    pop_release                = pipeline$config_list$pop_release,
    interval_mo                = pipeline$config_list$interval_mo,
    modeling_shapefile_version = pipeline$config_list$modeling_shapefile_version,
    poly_ag                    = as.logical(pipeline$config_list$poly_ag),
    zcol                       = pipeline$config_list$zcol,
    zcol_ag                    = pipeline$eval_conf("zcol_ag"),
    zcol_ag_id                 = pipeline$eval_conf("zcol_ag_id"),
    z_map                      = pipeline$eval_conf("z_map"),
    z_ag_mat                   = pipeline$eval_conf("z_ag_mat")
  )
}

# create simple_raster, a single layer 5x5 km raster, from sub-setted shapefile
if (use_custom_space == TRUE){
  source(paste0(custom_fun_dir,"build_simple_raster_custom.R"))
  simple_raster <- build_simple_raster(extent_template = subset_shape,
                                       shapefile_version = pipeline$config_list$modeling_shapefile_version,
                                       region = reg,
                                       raster_agg_factor = as.integer(pipeline$config_list$raster_agg_factor))
} else {
  simple_raster <- build_simple_raster(extent_template = subset_shape,
                                       shapefile_version = pipeline$config_list$modeling_shapefile_version,
                                       region = reg,
                                       raster_agg_factor = as.integer(pipeline$config_list$raster_agg_factor))
}

## only for countries with sub-nationals
if (use_sub_nats == TRUE) {
  
  simple_raster2 <- suppressMessages(
    build_simple_raster(extent_template = simple_raster,
                        shapefile_version = pipeline$config_list$modeling_shapefile_version,
                        reg = reg,
                        raster_agg_factor = as.integer(pipeline$config_list$raster_agg_factor),
                        field = "ADM1_CODE"))
  
  plot(simple_raster2) ## view simple_raster2
  
  # get admin_0 codes for entire region
  region_adm0s <- get_adm0_codes(
    reg,
    shapefile_version = pipeline$config_list$modeling_shapefile_version
  )
  
  if ("all" %in% pipeline$config_list$subnat_country_to_get) {
    countries_to_get_subnat_res <- region_adm0s
  } else {
    re_country_adm0s <- get_adm0_codes(
      pipeline$config_list$subnat_country_to_get,
      shapefile_version = pipeline$config_list$modeling_shapefile_version
    )
    countries_to_get_subnat_res <- intersect(re_country_adm0s, region_adm0s)
    
    simple_mask <- copy(simple_raster)
    countries_to_remove <- na.omit(setdiff(unique(simple_mask), countries_to_get_subnat_res))
    simple_mask[simple_mask %in% countries_to_remove] <- NA
    
    simple_raster2 <- raster::mask(simple_raster2, simple_mask)
  }
  
  subnat_full_shp <- readRDS(get_admin_shapefile(
    admin_level = 1,
    raking = FALSE,
    suffix = ".rds",
    version = pipeline$config_list$modeling_shapefile_version
  ))
  
  subnat_shapefile <- raster::subset(
    subnat_full_shp,
    ADM0_CODE %in% countries_to_get_subnat_res
  )
  
  ## merge shapefile with input data
  adm1_subset_lox <- sp::over(sp::SpatialPoints(
    df[, .(long = longitude, lat = latitude)],
    sp::CRS(proj4string(subnat_shapefile))
  ), subnat_shapefile)
  df[, subnat_re_ADM1_CODE := as.numeric(as.character(adm1_subset_lox$ADM1_CODE))]
  df[, subnat_re_ADM0_CODE := as.numeric(as.character(adm1_subset_lox$ADM0_CODE))]
  
  for (i in 1:length(unique(na.omit(df$subnat_re_ADM0_CODE)))) {
    df[subnat_re_ADM0_CODE == unique(na.omit(df$subnat_re_ADM0_CODE))[i], (paste0("SUBNAT", i)) := subnat_re_ADM1_CODE]
  }
} else {
  simple_raster2 <- NULL
}

if (pipeline$eval_conf("test")) {
  test_pct <- as.numeric(pipeline$config_list$test_pct)
  
  message(paste0(
    "Test option was set on and the test_pct argument was found at ",
    test_pct,
    "% \n\n                 ... keeping only ",
    round(nrow(df) * (test_pct / 100), 0), " random rows of data."
  ))
  set.seed(seed)
  increment_seed(seed)
  df <- df[sample(nrow(df), round(nrow(df) * (test_pct / 100), 0)), ]
  
  message("Also, making it so we only take 100 draws")
  samples <- 100
}

df[[pipeline$indicator]] <- df[[gsub(paste0("_age", age), "", pipeline$indicator)]]

if (pipeline$config_list$other_weight != "") {
  message(paste0(
    "Multiplying weight and ",
    pipeline$config_list$other_weight
  ))
  df[["weight"]] <- df[["weight"]] * df[[other_weight]]
}

if (
  pipeline$config_list$indicator_family == "binomial" & any(
    df[, get(pipeline$indicator)] / df$N > 1
  )) {
  stop("You have binomial data where k > N. Check your data before proceeding")
}
if (any(df[["weight"]] %in% c(Inf, -Inf) | any(is.na(df[["weight"]])))) {
  stop(
    "You have illegal weights (NA,Inf,-Inf). Check your data before proceeding"
  )
}

# Pull Covariates ---------------------------------------------------------
if (pipeline$config_list$yearload == "annual") {
  period_map <-
    make_period_map(
      modeling_periods = c(
        min(
          pipeline$config_list$year_list
        ):max(
          pipeline$config_list$year_list
        )
      )
    )
}
if (pipeline$config_list$yearload == "five-year") {
  period_map <-
    make_period_map(
      modeling_periods = seq(
        min(
          pipeline$config_list$year_list
        ), max(
          pipeline$config_list$year_list
        ),
        by = 5
      )
    )
}

cov_layers <- gbd_cov_layers <- NULL

## pull all standard covariate bricks/layers
if (nrow(pipeline$fixed_effects_config) > 0) {
  message("Grabbing raster covariate layers")
  loader <- MbgStandardCovariateLoader$new(
    start_year = min(pipeline$config_list$year_list),
    end_year = max(pipeline$config_list$year_list),
    interval = as.numeric(pipeline$config_list$interval_mo),
    covariate_config = pipeline$fixed_effects_config
  )
  cov_layers <- loader$get_covariates(simple_polygon)
}

## Pull country-level gbd covariates
if (nchar(pipeline$config_list$gbd_fixed_effects) > 0) {
  message("Grabbing GBD covariates")
  
  with_globals(
    new = list(modeling_shapefile_version = pipeline$config_list$modeling_shapefile_version),
    gbd_cov_layers <- load_gbd_covariates(
      covs = trim(
        strsplit(pipeline$config_list$gbd_fixed_effects, "\\+")[[1]]
      ),
      measures = trim(
        strsplit(
          pipeline$config_list$gbd_fixed_effects_measures, "\\+"
        )[[1]]
      ),
      year_ids = pipeline$config_list$year_list,
      age_ids = pipeline$config_list$gbd_fixed_effects_age,
      template = cov_layers[[1]][[1]],
      modeling_shapefile_version = pipeline$config_list$modeling_shapefile_version,
      simple_polygon = simple_polygon,
      interval_mo = pipeline$config_list$interval_mo,
      year_list = pipeline$config_list$year_list
    )
  )
}

## combine all covariates
all_cov_layers <- c(cov_layers, gbd_cov_layers)

## aggregate covariate raster
if (as.integer(pipeline$config_list$raster_agg_factor) > 1) { 
  extent_template <- empty_world_raster()
  extent_template <- raster::aggregate(extent_template, fact = as.integer(pipeline$config_list$raster_agg_factor))
  extent_template <- raster::crop(extent_template, extent(simple_polygon))

  for (idx in 1:length(all_cov_layers)) {
    all_cov_layers[[idx]] <- downsample_covariate_raster(cov_raster = all_cov_layers[[idx]],
                                                         simple_raster = extent_template,
                                                         raster_agg_factor = pipeline$config_list$raster_agg_factor)
  }
  rm(idx)
}


## create fixed effects equation from the cov layers
all_fixed_effects <- paste(names(all_cov_layers), collapse = " + ")

## make stacker-specific formulas where applicable
all_fixed_effects_brt <- all_fixed_effects

## set up country-level fixed effects based on config file parameters
if (pipeline$config_list$use_child_country_fes == TRUE | pipeline$config_list$use_inla_country_fes == TRUE) {
  message("Setting up country fixed effects")
  fe_gaul_list <- unique(c(
    gaul_convert(unique(df[, country]),
                 shapefile_version =
                   pipeline$config_list$modeling_shapefile_version
    ),
    gaul_list
  ))
  fe_template <- cov_layers[[1]][[1]]
  simple_polygon_list <- load_simple_polygon(
    gaul_list = fe_gaul_list,
    buffer = 0.4,
    subset_only = TRUE,
    shapefile_version = pipeline$config_list$modeling_shapefile_version
  )
  fe_subset_shape <- simple_polygon_list[[1]]
  admin_code_raster <- rasterize_check_coverage(
    fe_subset_shape, fe_template,
    field = "ADM0_CODE",
    link_table = pipeline$config_list$modeling_shapefile_version
  )
  admin_code_raster <- setNames(admin_code_raster, "gaul_code")
  admin_code_raster <- create_categorical_raster(admin_code_raster)
  
  ## update covlayers and add country fixed effects to the formula object
  all_cov_layers <- update_cov_layers(all_cov_layers, admin_code_raster)
  all_fixed_effects_cfes <- paste(all_fixed_effects,
                                  paste(names(admin_code_raster)[1:length(names(admin_code_raster))],
                                        collapse = " + "
                                  ),
                                  sep = " + "
  )
  
  ## update specific stacker formulas
  all_fixed_effects_brt <- all_fixed_effects_cfes
} else {
  admin_code_raster <- NULL
}

## Add these to the fixed effects if we want them in stacking
if (pipeline$config_list$use_child_country_fes == TRUE) {
  gaul_fes <- paste(
    names(admin_code_raster)[2:length(names(admin_code_raster))],
    collapse = " + "
  )
  all_fixed_effects <- paste(all_fixed_effects, gaul_fes, sep = " + ")
}

# create visuals
cov_rast_dir <- "FILEPATH"
# if directory does not already exist, create it
if (!dir.exists(file.path(cov_rast_dir))){
  dir.create(cov_rast_dir)
}

# save .grd files of the covariate rasters
if (pipeline$config_list$cov_geotiffs == TRUE) {
  cov_names <- names(all_cov_layers)
  for (i in 1:length(all_cov_layers)) {
    name <- cov_names[[i]]
    layer <- all_cov_layers[[i]]
    lay_class <- class(layer)
    
    if (lay_class == "RasterLayer"){
      writeRaster(layer,"FILEPATH", format="GTiff", overwrite=TRUE, options=c('TFW=YES'))
    }
    if (lay_class == "RasterBrick"){
      writeRaster(layer, "FILEPATH", format="raster", overwrite=TRUE, options=c("INTERLEAVE=BAND","COMPRESS=LZW")) 
    }
  } 
}

# save pdfs of the covariate rasters
if (pipeline$config_list$cov_raster_output == TRUE) {
  pdf(paste0("FILEPATH"),
      width = 12, height = 7)
  for (i in names(all_cov_layers)) {
    print(plot(all_cov_layers[[i]], main = i, maxnl=33))
  } 
  dev.off()
}

# check for any NA variables in the dataset
## may be due to coastal points that fall in the water
if(pipeline$config_list$check_nas == T){
    message("Checking if there are any NA covariate values present in the dataset.")
    
    gaul_cols <- grep("^gaul_", names(df), value = TRUE)
    
    # Get the unique country-code combinations
    gaul <- df %>% 
      select(country, gaul_cols) %>% 
      unique %>% 
      na.omit()
    
    # Fill in any missing GAUL code values
    df <- dplyr::rows_patch(df, gaul, by = "country")
    
    # Set the distance threshold in meters
    threshold <- 10000
    
    # Check the number of rows with NA
    nas_present <- cs_covs[[1]] %>% 
      select(all_of(the_covs)) %>% 
      filter(apply(is.na(.), 1, any))
    
    # If NA values are present 
    if(nrow(nas_present) > 0) {
      message(paste0(nrow(nas_present)), " rows have at least one NA covariate value.")
      message(paste0("Attempting to replace NA values with nearest non-NA covariate value within a ", threshold, " meter threshold."))
      
      # Load the function
      source(paste0("/homes/", user, "/FILEPATH"))
      # Run the function
      df_fixed <- nudge_coordinates(temp_data = df, # Data frame that has both coordinates and covariate values
                                          the_covs = the_covs, # Character vector of cov names
                                          covs_cs_df = covs_cs_df, 
                                          threshold = threshold, # Default is 10000m
                                          save_fixed_coordinate = T)
              
      ## store the centre scaling mapping
      covs_cs_df <- df_fixed$covs_cs_df
      
      ## Save the updated data
      df <- df_fixed$temp_data
      
      # Make sure the function worked
      nas_present_new <- df %>% 
        select(all_of(the_covs)) %>% 
        filter(apply(is.na(.), 1, any))
      
        if(nrow(nas_present_new) > 0) {
          message(paste0(nrow(nas_present_new)), " rows still have NA values. Please review the data further to resolve.")
        }
      
    } else {
      message("No NA covariate values are present.")
    }
}

# VIF analysis ------------------------------------------------------------
if (pipeline$config_list$vif == TRUE) {
  source("FILEPATH")
  
  ###### Perform stepwise covariate removal using VIF
  #### Set basic parameters
  threshold_min <- 2
  threshold_max <- 10
  threshold_step <- 1
  threshold <- c(seq(as.numeric(threshold_min), as.numeric(threshold_max), by = as.numeric(threshold_step)))
  
  message(paste0("VIF analysis for the ", reg, " region"))
  
  # set up table
  col_names <- c(the_covs[1:length(cov_cols)], "region", "vif_threshold", "num_covs_selected", "data_run_date")
  
  selection_results <- setNames(data.table(matrix(
    nrow = 0,
    ncol = length(col_names)
  )), col_names)
  
  vif_results <- data.table()
  
  # run vif selection analysis
  for (i in threshold) {
    
    # report threshold
    message(paste0("Threshold: ", i))
    
    # run stepwise vif selection function
    vif <- stepwise_vif_selection(
      thresh = i,
      covariate_data = cov_data,
      reg = reg,
      trace = F)
    
    vif_results <- rbind(vif_results, vif)
    
  }
  
  # save selection results
  write.csv(vif_results, "FILEPATH")
  message("Selection complete. Results saved.")
  
} else {
  print("Skipping VIF analysis")
}

############ postamble ############ 
pipeline_postamble()
q("no")