# Purpose: prep data for fitting with MBG by; extract covariates for INLA (child model prediction), prep rasters, and create space & time mesh for INLA 

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
    node_id = 3,
    node_name = "j03_prep_for_INLA",
    dag_path = sprintf("FILEPATH"),
    loopvar_index = 1)}
pipeline_preamble(inputs = inputs)

# Populate global variables tied to loopvar_index.
assign("reg", pipeline$loopvars[loopvar_index, region])
assign("age", pipeline$loopvars[loopvar_index, age])
assign("holdout", pipeline$loopvars[loopvar_index, holdout])
assign("seed", pipeline$config_list$seed)

######################## final pre-MBG processing ######################## 
## If skipping to INLA, then just quit job
  ## not applicable to demo
if (as.logical(pipeline$config_list$skiptoinla)) {
  print("Skipping to INLA")

  ## Save out environment
  mbg_save_nodeenv(
    node = nodename,
    ig = indicator_group,
    indic = indicator,
    rd = run_date,
    reg = reg,
    age = age,
    holdout = holdout,
    objs = ls()
  )

  ## Create output file and remove err file ##
  mbg_job_marker(type = "end", tmpdir = "~/mbgdir")

  q("no")
}

## set the fixed effects to use in INLA based on config arguments (the fixed effects for INLA are the child model results from the stacking script)
all_fixed_effects <- build_fixed_effects_logic(
  pipeline = pipeline,
  admin_codes = admin_code_raster
)

## copy things back over to df
df <- copy(the_data)

## remove the covariate columns used for stacking so there are no name conflicts when they get added back in
df <- df[, paste0(the_covs) := rep(NULL, length(the_covs))]

## double-check that gaul codes get dropped before extracting
df <- df[, grep(
  "gaul_code_*", names(df),
  value = T
) := rep(
  NULL, length(grep("gaul_code_*", names(df), value = T))
)]

## create a full raster list to carry though to next steps
if (pipeline$config_list$use_stacking_covs) {
  cov_list <- c(unlist(stacked_rasters), unlist(all_cov_layers))
  child_mod_ras <- cov_list[child_model_names]
} else {
  cov_list <- unlist(all_cov_layers)
  child_model_names <- ""
}


## stacking covs are used the oos-stackers
##
## For predict_mbg, non-center-scaled covs are pulled from cov_list (either
## stackers or raw) and center-scaled within the function.  So both fit and
## predict take place on center-scaled covs

if (as.logical(pipeline$config_list$use_raw_covs) == TRUE) {
  centre_scale_covs <- FALSE
} else {
  centre_scale_covs <- TRUE
}

######################## last stages of prepping ######################## 
just_covs <- extract_covariates(df, cov_list,
  return_only_results = T,
  centre_scale = centre_scale_covs,
  period_var = "year",
  period_map = period_map
)
if (centre_scale_covs == TRUE) {
  just_covs <- just_covs$covs
}
just_covs <- just_covs[, year := NULL]
just_covs <- just_covs[, period_id := NULL]
df <- cbind(df, just_covs)

# create a period column
if (is.null(period_map)) {
  period_map <- make_period_map(c(2000, 2005, 2010, 2015))
}
df[, period := NULL]
setnames(period_map, "data_period", "year")
setnames(period_map, "period_id", "period")
df <- merge(df, period_map, by = "year", sort = F)

for (l in 1:length(cov_list)) {
  cov_list[[l]] <- crop(cov_list[[l]], extent(simple_raster))
  cov_list[[l]] <- setExtent(cov_list[[l]], simple_raster)
  cov_list[[l]] <- raster::mask(cov_list[[l]], simple_raster)
}
rm(l)

set.seed(seed)
increment_seed(seed)

# the INLA model is fit using a stochastic partial differential equations (SPDE) approach, meaning that we work with a sparse precision (inverse of covariance) matrix, 
# rather than a dense covariance matrix, which improves computational efficiency. 
# SPDE approach is done by approximating the continuous surface over a mesh.
  # hyperparameters (range and variance) are controlled by spde_prior.
  # config option mesh_t_knots is used when running a space x time model using INLA to specify the locations of the B-spline (basis degree 1) knots in time.

## build spatial mesh over modeling area
mesh_s <- build_space_mesh(
  d = df,
  simple = simple_polygon,
  max_edge = pipeline$config_list$mesh_s_max_edge,
  mesh_offset = pipeline$config_list$mesh_s_offset,
  s2mesh = pipeline$config_list$use_s2_mesh,

  ## s2params MUST be in stringed vector format. Yuck.
  s2params = pipeline$config[V1 == "s2_mesh_params", V2]
)

## build temporal mesh (standard for now)
if (length(pipeline$config_list$year_list) == 1) {
  mesh_t <- NULL
} else {
  mesh_t <- build_time_mesh(
    periods = eval(
      parse(
        text = pipeline$config_list$mesh_t_knots
      )
    )
  )
}

######################## Postamble ######################## 
# the pipeline_postamble saves all of the objects within R’s global environment as individual rda files in the Objects folder's corresponding node_id folder. The next script in the Dag looks up the previous node_id and loads in all of those rda files back into the global environment
pipeline_postamble()
q("no")
