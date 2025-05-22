# Purpose: use MBG fitting results to generate MBG predictions

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
    node_id = 5,
    node_name = "j05_MBG_predict",
    dag_path = sprintf("FILEPATH"),
    loopvar_index = 1)}
pipeline_preamble(inputs = inputs)

# setup pipeline values
library("viridis")
library("ggnewscale", lib = "FILEPATH")

# setup input paths
repo_dir <- "FILEPATH"
custom_fun_dir <- "FILEPATH"

# setup output paths
output_dir <- "FILEPATH"
raster_dir <- "FILEPATH"

path_addin <- pipeline$get_path_addin(loopvar_index)

######################## predictions ######################## 
# generate pixel-level predictions with predict_mbg()
# Populate global variables tied to loopvar_index
assign("reg", pipeline$loopvars[loopvar_index, region])
assign("age", pipeline$loopvars[loopvar_index, age])
assign("holdout", pipeline$loopvars[loopvar_index, holdout])
assign("seed", pipeline$config_list$seed)

######################## predictions ######################## 
# generate pixel-level predictions with predict_mbg()
## Run predict_mbg on chunks of 50 samples (to avoid memory issues)
## predict_mbg() calculates prediction of the mean or probability surface on a 5x5 km grid based on the stacking predictions at this resolution
message("Making predictions in 50 draw chunks.")
  
## create list of child models
child_model_names <- pipeline$config_list$stacked_fixed_effects %>%
  gsub(" ", "", .) %>%
  strsplit(., "+", fixed = T) %>%
  unlist()
  
draws_dir <- "FILEPATH"
  
if(!dir.exists(file.path(draws_dir))){
  dir.create(draws_dir, recursive = T, showWarnings = F)
}

# number of draws to predict
samples <- 1000
samples <- as.numeric(samples)

# number of draws to predict/write in each loop
max_chunk <- 50
max_chunk <- as.numeric(max_chunk)

# number of loops in which chunks of draws will be written
chunks <- rep(max_chunk, samples %/% max_chunk)
if (samples %% max_chunk > 0) chunks <- c(chunks, samples %% max_chunk)

# run and save predictions by chunk
for (i in 1:length(chunks)) {
  # only run the prediction for this chunk if the file doesn't already exist
  if (file.exists("FILEPATH")) {
    message(paste0(draws_dir," 'FILEPATH' already exists; skipping."))
    next
  } else {
    samp <- chunks[i]
    chunk <- predict_mbg(
      res_fit = model_fit,
      cs_df = cs_df,
      mesh_s = mesh_s,
      mesh_t = mesh_t,
      cov_list = cov_list,
      samples = samp,
      simple_raster = simple_raster,
      fixed_effects = all_fixed_effects,
      stacker_names = child_model_names, 
      transform = pipeline$config_list$transform,
      coefs.sum1 = pipeline$config_list$coefs_sum1,
      yl = pipeline$config_list$year_list,
      nperiod = length(pipeline$config_list$year_list),
      pred_gp = pipeline$config_list$use_gp,
      shapefile_version = pipeline$config_list$modeling_shapefile_version,
      seed = seed,
      no_nugget_predict = pipeline$config_list$no_nugget_predict,
      use_space_only_gp = as.logical(pipeline$config_list$use_space_only_gp),
      use_time_only_gmrf = as.logical(pipeline$config_list$use_time_only_gmrf),
      use_timebyctry_res = as.logical(pipeline$config_list$use_timebyctry_res),
      ind = pipeline$indicator,
      indg = pipeline$indicator_group,
      rd = pipeline$run_date,
      region = reg,
      simple_raster_subnats = simple_raster2,
      subnat_country_to_get = pipeline$config_list$subnat_country_to_get
    )[[3]]
      
    message(paste0("Writing predictions to 'FILEPATH'"))
    saveRDS(chunk, "FILEPATH")
      
  }  
}

# read-in the predictions and recreate the pm object
pm <- lapply(1:length(chunks), function(i) {readRDS("FILEPATH")})

############ process predictions and produce output ############ 
# if z dimension has more than one level, then save each z as a different indicator
if (length(pipeline$config_list$z_list) > 1) {
    
   # reorder pm list, right now its z within each chunk--rbind all z's together
   for (z in pipeline$config_list$z_list) { 
    if (length(chunks) > 1) {
       for (ch in 2:length(chunks)) {
        pm[[1]][[z]] <- cbind(pm[[1]][[z]], pm[[ch]][[z]])
       }
     }
   }
   pm <- pm[[1]] # pm is now a list of cell_preds by z
    
    
   # loop over z and save as an indicator each one
   orig_indic <- indicator 
    
   message("Wrapping up MBG model predictions")
    
  for (z in z_list) {
     cptmp <- pm[[z]]
     
    indicator <- sprintf("%s_%s%i", orig_indic, zcol, z) # new indicator name
    pathaddin <- paste0("_bin", z, "_", reg, "_", holdout) # new pathaddin
    outputdir <- sprintf(output_dir) # new outputdir
    dir.create(outputdir)
    message(sprintf("New indicator: %s", indicator))
      
    # make a raster of the mean pixel-estimate for each year modeled
    mean_ras <- insertRaster(
      simple_raster,
      matrix(rowMeans(cptmp),
             ncol = max(period_map$period)
      )
    )
    sd_ras <- insertRaster(
      simple_raster,
      matrix(rowSds(cptmp), ncol = max(period_map$period))
    )
      
    # save z specific objects
    writeRaster(
      mean_ras,
      file = paste0(output_dir, "FILEPATH"),
      overwrite = TRUE
    )
    
    save(
      cptmp,
      file = paste0(
        output_dir, "FILEPATH"
      ),
      compress = TRUE
    )
      
    pdf(paste0(output_dir, "FILEPATH"))
    plot(mean_ras, main = "mean", maxpixel = 1e6)
    plot(sd_ras, main = "sd", maxpixel = 1e6)
    dev.off()
    
    rm(cptmp)
  }
    
  ## Reset the constants
  indicator <- orig_indic
    
  # save training data
  write.csv(
    df,
    file = paste0(output_dir, "FILEPATH"
    ),
    row.names = FALSE
  )
    
  message("done saving indicator-specific outputs by z")
} else {
  ## Make cell_pred, which contains predictions
  ## Each row corresponds to a pixel-year, each column is a prediction draw
  cell_pred <- do.call(cbind, pm)
  mean_ras <- insertRaster(
    simple_raster,
    matrix(rowMeans(cell_pred),
           ncol = max(period_map$period)
    )
  )
  source("FILEPATH")
  message("Wrapping up")
  with_globals(
    new = list(
      indicator = pipeline$indicator,
      indicator_group = pipeline$indicator_group
    ),
    save_mbg_preds(
      config = pipeline$config,
      time_stamp = TRUE,
      run_date = pipeline$run_date,
      mean_ras = mean_ras,
      sd_ras = NULL,
      res_fit = model_fit,
      cell_pred = cell_pred,
      df = df,
      pathaddin = path_addin
    )
  )
}

# save .grd raster files
prediction_dir <- "FILEPATH"
if (!dir.exists(file.path(prediction_dir))){
  dir.create(prediction_dir)
}

if (pipeline$config_list$inla_geotiffs == TRUE){
  writeRaster(mean_ras, 
              filename="FILEPATH", 
              format="raster",
              overwrite=TRUE, 
              options=c("INTERLEAVE=BAND","COMPRESS=LZW")) 
}

######################## postamble ######################## 
# the pipeline_postamble saves all of the objects within R’s global environment as individual rda files in the Objects folder's corresponding node_id folder. The next script in the Dag looks up the previous node_id and loads in all of those rda files back into the global environment
pipeline_postamble()
q("no")
