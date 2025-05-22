# Purpose: post-estimation fractional raking and aggregation 
## when aggregating to admin units, will assign overlapping pixels based on fraction in each unit

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
    node_id = 6,
    node_name = "j06_MBG_postest",
    dag_path = sprintf("FILEPATH"),
    loopvar_index = 1)}
pipeline_preamble(inputs = inputs)

# create filepath object for summaries of results by admin-level 
admin_dir <- "FILEPATH"
if (!(dir.exists(file.path(admin_dir)))){
  dir.create(admin_dir, recursive = T, showWarnings = F) 
}

# Populate global variables tied to loopvar_index.
assign("reg", pipeline$loopvars[loopvar_index, region])
assign("age", pipeline$loopvars[loopvar_index, age])
assign("holdout", pipeline$loopvars[loopvar_index, holdout])
assign("seed", pipeline$config_list$seed)

# load libraries
library("viridis")
library("ggnewscale")
library("spdep")

# setup input paths
repo_dir <- "FILEPATH"
custom_fun_dir <- "FILEPATH"

# setup output paths
output_dir <- "FILEPATH"
admin_dir <- "FILEPATH"
agg_dir <- "FILEPATH"
prob_dir <- "FILEPATH"
moran_dir <- "FILEPATH"
box_dir <- "FILEPATH"

if(pipeline$indicator == "int_testing" & 
   endsWith(pipeline$run_date, "step")) {
  pipeline_temp_outputdir <- copy(pipeline$outputdir)
  pipeline$outputdir <- gsub("_([^_]+)_step", "", pipeline$outputdir)
}

pipeline$config_list$countries_not_to_rake <- reg

stratum <- reg <- pipeline$loopvars[loopvar_index, region] ## TODO For now just assign stratum to reg (will need to modify the below for strata beyond reg)
if (!exists("age")) age <- pipeline$loopvars[loopvar_index, age]
if (!exists("holdout")) holdout <- pipeline$loopvars[loopvar_index, holdout]
path_addin <- if(age != 0 | holdout != 0) paste0("_bin", age, "_", holdout) else ""

interval_mo <- 12

# Load objects from convenience temp file
temp_dir <- file.path("FILEPATH")
if(dir.exists(temp_dir)) {
  load(file.path("FILEPATH"))
}

# Get the necessary variables out from the config object into global env
summstats <- eval(parse(text = pipeline$config_list$summstats))

# Print some settings to console
print(paste0("Indicator: ", pipeline$indicator))
print(paste0("Indicator group: ", pipeline$indicator_group))
print(paste0("Run date:", pipeline$run_date))
print(paste0("Stratum: ", stratum))
print(paste0("Pop measure: ", pipeline$config_list$pop_measure))
print(paste0("Pop release: ", pipeline$config_list$pop_release))
print(paste0("Summary stats: ", paste0(summstats, collapse = ", ")))
print(paste0("Metric space                       : ", pipeline$config_list$metric_space))
print(paste0("Subnational raking                 : ", pipeline$config_list$subnational_raking))
print(paste0("Countries not to rake at all       : ", pipeline$config_list$countries_not_to_rake))
print(paste0("Countries not to rake subnationally: ", pipeline$config_list$countries_not_to_subnat_rake))

######################## prepare rasters, etc. ######################## 
# load cell_draws from prediction script
message("Loading Data...")
load(file.path("FILEPATH"))

if(pipeline$indicator == "int_testing" & 
   endsWith(pipeline$run_date, "step")) {
  pipeline$outputdir <- pipeline_temp_outputdir
}

if (!exists("cell_pred")) {
  message(filename_rds)
  stop("Unable to load cell_pred object! Check to make sure that the relevant object exists.")
}

# Rake estimates
if (pipeline$config_list$rake_countries) {
  if (!exists("gbd")) {
    stop("rake_countries was specified as T in config, gbd raking targets must be provided.")
  }

  ## determine if a crosswalk is needed
  if (pipeline$config_list$modeling_shapefile_version == pipeline$config_list$raking_shapefile_version){
    crosswalk <- F
  }else{
    crosswalk <- T
  }

  # Assume linear raking unless specified as logit
  if (pipeline$config_list$rake_transform == "logit") rake_method <- "logit" else rake_method <- "linear"

  ##### prep rasters and input data for raking
  print("Getting simple and prepped rasters")
  with_globals(new = list(modeling_shapefile_version = pipeline$config_list$modeling_shapefile_version,
                          raking_shapefile_version = pipeline$config_list$raking_shapefile_version), 
               raster_outputs <- prep_shapes_for_raking(
                 reg = reg,
                 modeling_shapefile_version = pipeline$config_list$modeling_shapefile_version,
                 raking_shapefile_version = pipeline$config_list$raking_shapefile_version,
                 field = "loc_id",
                 raster_agg_factor = as.integer(pipeline$config_list$raster_agg_factor),
                 rake_subnational = as.logical(pipeline$config_list$subnational_raking),
                 countries_not_to_subnat_rake = pipeline$config_list$countries_not_to_subnat_rake)
               )

  simple_raster <- raster_outputs[["simple_raster"]]
  new_simple_raster <- raster_outputs[["new_simple_raster"]]
  simple_polygon <- raster_outputs[["simple_polygon"]]
  pixel_id <- raster_outputs[["pixel_id"]]

  # use fractional raking to convert the pixel-estimates to admin-level estimates (mean and confidence intervals for an admin-year) 
  if (pipeline$config_list$metric_space == "rates") {
    print("Get GBD populations")
    with_globals(new = list(raking_shapefile_version = pipeline$config_list$raking_shapefile_version), 
                 gbd_pops <- prep_gbd_pops_for_fraxrake(pop_measure = pipeline$config_list$pop_measure,
                                                        reg = reg,
                                                        year_list = pipeline$config_list$year_list,
                                                        gbd_round_id = ADDRESS, decomp_step = "ADDRESS")
                 )

    print("Using the rates raking and aggregation functions:")

    ## create all the fractional rake factors
    with_globals(new = list(modeling_shapefile_version = pipeline$config_list$modeling_shapefile_version,
                            pop_release = pipeline$config_list$pop_release), 
                 fractional_rake_rates(
                   cell_pred = cell_pred,
                   simple_raster = simple_raster,
                   simple_polygon = simple_polygon,
                   pixel_id = pixel_id,
                   shapefile_version = pipeline$config_list$raking_shapefile_version,
                   reg = reg,
                   pop_measure = pipeline$config_list$pop_measure,
                   year_list = pipeline$config_list$year_list,
                   interval_mo = interval_mo,
                   rake_subnational = pipeline$config_list$subnational_raking, 
                   age_group = age_group,
                   sex_id = sex_id,
                   sharedir = pipeline$sharedir,
                   run_date = pipeline$run_date,
                   indicator = pipeline$indicator,
                   gbd = gbd,
                   rake_method = rake_method,
                   gbd_pops = gbd_pops,
                   countries_not_to_rake = pipeline$config_list$countries_not_to_rake,
                   countries_not_to_subnat_rake = pipeline$config_list$countries_not_to_subnat_rake,
                   raster_agg_factor = as.integer(pipeline$config_list$raster_agg_factor),
                   path_addin = path_addin
                 ))

    ## create the fractional aggregation file using the raking factors from fractional_rake_rates()
    with_globals(new = list(modeling_shapefile_version = pipeline$config_list$modeling_shapefile_version,
                            pop_release = pipeline$config_list$pop_release), 
                 outputs <- fractional_agg_rates(
                   cell_pred = cell_pred,
                   simple_raster = simple_raster,
                   simple_polygon = simple_polygon,
                   pixel_id = pixel_id,
                   shapefile_version = pipeline$config_list$raking_shapefile_version,
                   reg = reg,
                   pop_measure = pipeline$config_list$pop_measure,
                   year_list = pipeline$config_list$year_list,
                   interval_mo = interval_mo,
                   rake_subnational = pipeline$config_list$subnational_raking, 
                   sharedir = pipeline$sharedir,
                   run_date = pipeline$run_date,
                   indicator = pipeline$indicator,
                   main_dir = pipeline$outputdir,
                   rake_method = rake_method,
                   age = age,
                   holdout = holdout,
                   countries_not_to_subnat_rake = pipeline$config_list$countries_not_to_subnat_rake,
                   raster_agg_factor = as.integer(pipeline$config_list$raster_agg_factor),
                   return_objects = TRUE
                 ))
                 
    ## get the necessary outputs and rename the columns
    rf <- data.table(outputs[["rf"]])[, .(loc = location_id, year, start_point = mbg_prev, target = gbd_prev, raking_factor = rf)]
    raked_cell_pred <- outputs[["raked_cell_pred"]]

    raked_simple_raster <- new_simple_raster

  } else if (pipeline$config_list$metric_space == "counts") {
    print("Using the counts raking and aggregation functions:")

    ## rake counts
    with_globals(new = list(pop_release = pipeline$config_list$pop_release), 
                 outputs <- fractionally_rake_counts(
                   count_cell_pred = data.table(cell_pred),
                   rake_to = gbd,
                   reg = reg,
                   year_list = pipeline$config_list$year_list,
                   rake_subnational = pipeline$config_list$subnational_raking, 
                   countries_not_to_subnat_rake = pipeline$config_list$countries_not_to_subnat_rake,
                   countries_not_to_rake = pipeline$config_list$countries_not_to_rake,
                   simple_raster = simple_raster,
                   modeling_shapefile_version = pipeline$config_list$modeling_shapefile_version,
                   raking_shapefile_version = pipeline$config_list$raking_shapefile_version,
                   raster_agg_factor = pipeline$config_list$raster_agg_factor
                 ))

    ## Get the necessary outputs
    rf <- outputs$raking_factors
    raked_cell_pred <- outputs$raked_cell_pred
    raked_simple_raster <- new_simple_raster

    ## Save out the aggregate files
    with_globals(new = list(run_date = pipeline$run_date,
                            main_dir = pipeline$outputdir), 
      raked_frax_counts_save(output_list = outputs, sharedir = pipeline$sharedir,
                             indicator = pipeline$indicator,
                             age = age, reg = reg, holdout = holdout))
  }
} else {
  rf <- NULL
  raked_cell_pred <- NULL

  # Define simple raster for mask
  simple_polygon_list <- load_simple_polygon(
    gaul_list = get_adm0_codes(reg,
      shapefile_version = pipeline$config_list$modeling_shapefile_version
    ),
    buffer = 0.4, subset_only = FALSE,
    shapefile_version = pipeline$config_list$modeling_shapefile_version
  )
  subset_shape <- simple_polygon_list[[1]]
  simple_polygon <- simple_polygon_list[[2]]
  simple_raster <- build_simple_raster(extent_template = subset_shape,
                                       shapefile_version = pipeline$config_list$modeling_shapefile_version,
                                       region = reg,
                                       raster_agg_factor = pipeline$config_list$raster_agg_factor)
  rm(simple_polygon_list)
}

######################## save aggregation results ######################## 
message("saving results...")

## save raking factors
source(paste0(custom_fun_dir,"save_post_est.R"))
save_post_est(rf, filetype = "csv", filename = "FILEPATH")
save(raked_cell_pred, file = "FILEPATH")

# make and save summaries
save_cell_pred_summary <- function(summstat, raked, ...) {
  message(paste0("Making summmary raster for: ", summstat, " (", raked, ")"))
  
  if (raked == "unraked") {
    cpred <- "cell_pred"
    mask_raster <- "simple_raster"
  }
  if (raked == "raked") {
    cpred <- "raked_cell_pred"
    mask_raster <- "raked_simple_raster"
  }
  if (raked == "raked_c") {
    cpred <- "raked_cell_pred_c"
    load("FILEPATH")
    mask_raster <- "raked_simple_raster"
  }
  ras <- make_cell_pred_summary(
    draw_level_cell_pred = get(cpred),
    mask = get(mask_raster),
    return_as_raster = TRUE,
    summary_stat = summstat,
    ...
  )
  
  save_post_est(ras, 
                filetype = 'raster',
                filename = "FILEPATH")
}

message("Done making and saving summaries...")

if (!as.logical(pipeline$config_list$rake_countries)) {
  rake_list <- c("unraked")
} else {
  rake_list <- c("unraked", "raked")
}

summ_list <- expand.grid(summstats[summstats != "p_below"], rake_list)

message("Created summ_list")

lapply(1:nrow(summ_list), function(i) {
  summstat <- as.character(summ_list[i, 1])
  raked <- as.character(summ_list[i, 2])
  save_cell_pred_summary(summstat, raked)
})

for (r in rake_list) {
  if ("p_below" %in% summstats) {
    save_cell_pred_summary(summstat = "p_below", raked = r, value = 0.8, equal_to = F)
  }
}

message("done saving results...")

######################## postamble ######################## 
# the pipeline_postamble saves all of the objects within R’s global environment as individual rda files in the Objects folder's corresponding node_id folder. The next script in the Dag looks up the previous node_id and loads in all of those rda files back into the global environment
pipeline_postamble()
q("no")
