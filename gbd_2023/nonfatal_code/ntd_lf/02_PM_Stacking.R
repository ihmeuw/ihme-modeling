# Purpose: extract covariate values for each data point, run/stack child models, and bind stacking results into single data.frame

############ setup pipeline, paths, parameters, etc. ############
user <- Sys.info()["user"]
cause <- "ntd_lf"
source("FILEPATH"))

# setup preambles
## loads pipeline objects saved in the run_date/objects/node_id folder 
inputs <- if (interactive()) { # otherwise NULL
  list(
    run_date = run_date,
    indicator = indicator,
    indicator_group = indicator_group,
    node_id = 2,
    node_name = "j02_stacking",
    dag_path = sprintf("FILEPATH"),
    loopvar_index = 1)}
pipeline_preamble(inputs = inputs)

# setup input paths
repo_dir <- "FILEPATH"
custom_fun_dir <- "FILEPATH"

# setup output paths
output_dir <- "FILEPATH"
stacker_dir <- "FILEPATH"

# Populate global variables tied to loopvar_index.
assign("reg", pipeline$loopvars[loopvar_index, region])
assign("age", pipeline$loopvars[loopvar_index, age])
assign("holdout", pipeline$loopvars[loopvar_index, holdout])
assign("seed", pipeline$config_list$seed)

# if ran VIF in the previous step, remove the colinear variables from the dataset
if(pipeline$config_list$vif == T){
  
  # bring in VIF results
  vif_results <- read.csv("FILEPATH")
  
  # fixed effects names
  fes <- pipeline$fixed_effects_config$covariate
  
  # ad-hoc fix for West Africa
  if(reg == "wssa"){fes <- fes[fes != "map_pv_incidence" & fes != "map_pv_prevalence"]}
  
  # select the variables to omit
  omit_vars <- vif_results %>% 
    filter(vif_threshold == threshold_vif) %>% 
    dplyr::select(all_of(fes)) 
  na_columns <- sapply(omit_vars, function(col) any(is.na(col)))
  omit_vars <- omit_vars[, na_columns]
  
  # select the variables to drop
  retain_vars <- vif_results %>% 
    filter(vif_threshold == threshold_vif) %>% 
    dplyr::select(all_of(fes)) 
  non_na_columns <- sapply(retain_vars, function(col) any(!is.na(col)))
  retain_vars <- retain_vars[, non_na_columns]
  
  # save the col names
  omit_vars <- colnames(omit_vars)
  retain_vars <- colnames(retain_vars)
  
  ## create fixed effects equation from the cov layers
  all_fixed_effects <- retain_vars
  all_fixed_effects_brt <- c(retain_vars, names(admin_code_raster)[1:length(names(admin_code_raster))])
  
  # ensure the data.frame reflects the covariate set
  the_data <- the_data %>% dplyr::select(-c(omit_vars))

}

# Stacking ----------------------------------------------------------------

## If skipping to INLA, then just quit job
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
  
  mbg_job_marker(type = "end", tmpdir = "~/mbgdir")
  
  q("no")
}


tic("Stacking - all") 

## create list of child models
child_model_names <- pipeline$config_list$stacked_fixed_effects %>%
  gsub(" ", "", .) %>%
  strsplit(., "+", fixed = T) %>%
  unlist()
message(paste0(
  "Child stackers included are: ",
  paste(child_model_names,
        collapse = " // "
  )
))

the_covs <- format_covariates(all_fixed_effects) 

## copy the dataset to avoid unintended namespace conflicts
the_data <- copy(df)

## only use data where we know what age group or point
ag_data <- the_data[the_data$agg_weight != 1, ]
the_data <- the_data[the_data$agg_weight == 1, ]

set.seed(seed)
increment_seed(seed)

## shuffle the data into 5 folds
## for each cild model, a series of sub-child models is run for cross-validation purposes
## the folds are used to divide the data for cross-validation
the_data <- the_data[base::sample(nrow(the_data)), ]
the_data[, fold_id := cut(
  seq(1, nrow(the_data)),
  breaks = as.numeric(
    pipeline$config_list$n_stack_folds
  ),
  labels = FALSE
)]

## add a row id column
the_data[, a_rowid := seq(1:nrow(the_data))]

## extract covariates to the data points and subset data where covariate value is missing
cs_covs <- extract_covariates(the_data,
                              all_cov_layers,
                              id_col = "a_rowid",
                              return_only_results = TRUE,
                              centre_scale = TRUE,
                              period_var = "year",
                              period_map = period_map
)

# check if any of the variables do NOT vary across the data
covchecklist <- check_for_cov_issues(
  cc = cs_covs,
  afe = all_fixed_effects,
  afeb = all_fixed_effects_brt,
  fe = pipeline$config_list$fixed_effects,
  check_pixelcount = pipeline$config_list$check_cov_pixelcount,
  check_pixelcount_thresh = ifelse(
    "pixelcount_thresh" %in% names(pipeline$config_list),
    pipeline$config_list$pixelcount_thresh, 0.95
  )
)
for (n in names(covchecklist)) {
  assign(n, covchecklist[[n]])
}

## Check for data where covariate extraction failed
rows_missing_covs <- nrow(the_data) - nrow(cs_covs[[1]])
if (rows_missing_covs > 0) {
  pct_missing_covs <- round((rows_missing_covs / nrow(the_data)) * 100, 2)
  warning(
    paste0(
      rows_missing_covs, " out of ", nrow(the_data), " rows of data ",
      "(", pct_missing_covs, "%) do not have corresponding ",
      "covariate values and will be dropped from child models..."
    )
  )
  if (rows_missing_covs / nrow(the_data) > 0.1) {
    stop(
      paste0(
        "Something has gone quite wrong: more than 10% of your data ",
        " does not have corresponding covariates.  You should investigate ",
        "this before proceeding."
      )
    )
  }
}

## merge input data with extracted covariate data by row id
the_data <- merge(the_data, cs_covs[[1]], by = "a_rowid", all.x = F, all.y = F)

## store the centre scaling mapping
covs_cs_df <- cs_covs[[2]]

## drop rows with NA covariate values
the_data <- na.omit(the_data, c(pipeline$indicator, "N", the_covs))

if (nrow(the_data) == 0) {
  stop(paste0(
    "You have an empty df, make sure one of your",
    " covariates was not NA everywhere."
  ))
}

## running stackers:
## each child model is fit separately on all covariates and outcome of interest
## for each child model, a series of sub-child models is fit for cross-validation based on the number of folds
if (pipeline$config_list$use_stacking_covs) {
  message("Fitting Stackers")
  with_globals(
    new = list(
      gbm_tc = as.numeric(pipeline$config_list$gbm_tc),
      gbm_lr = as.numeric(pipeline$config_list$gbm_lr),
      gbm_bf = as.numeric(pipeline$config_list$gbm_bf)
    ),
    # Run the child stacker models
    child_model_run <- run_child_stackers(
      models = child_model_names,
      input_data = the_data,
      indicator = pipeline$indicator,
      indicator_family = pipeline$config_list$indicator_family,
      covariates = all_fixed_effects,
      covariates_brt = all_fixed_effects_brt,
      outputdir = pipeline$outputdir,
      reg = reg
    )
  )
  
  # Bind the list of predictions into a data frame
  child_mods_df <- do.call(cbind, lapply(child_model_run, function(x) x[[1]]))

  ## combine the child models with the_data
  the_data <- cbind(the_data, child_mods_df)

  ## Rename the child model objects into a named list
  child_model_objs <- setNames(
    lapply(child_model_run, function(x) x[[2]]),
    child_model_names
  )

  ## return the stacked rasters
  stacked_rasters <- make_stack_rasters(
    covariate_layers = all_cov_layers, # raster layers and bricks
    period = min(period_map[, period_id]):max(period_map[, period_id]),
    child_models = child_model_objs,
    indicator_family = pipeline$config_list$indicator_family,
    centre_scale_df = covs_cs_df,
    rd = pipeline$run_date,
    re = reg,
    ind_gp = pipeline$indicator_group,
    ho = as.numeric(holdout),
    ind = pipeline$indicator
  )
  
  message("Stacking is complete")
}

## add aggregate data back in, with stacking predictions from the full model
if (nrow(ag_data) > 0) {
  ag_data[, a_rowid := 1:.N + max(the_data$a_rowid)]
  if(pipeline$config_list$use_stacking_covs) {
    ag_stackers <- extract_covariates(ag_data,
                                      stacked_rasters,
                                      id_col              = "a_rowid",
                                      return_only_results = TRUE,
                                      centre_scale        = FALSE,
                                      period_var          = "year",
                                      period_map          = period_map)
    ag_stackers <- ag_stackers[, c("a_rowid", child_model_names, child_model_names), with = F]
    
    stacker_names <- c(paste0(child_model_names, "_full_pred"), paste0(child_model_names, "_cv_pred"))
    setnames(ag_stackers, c("a_rowid", stacker_names))
    
    ag_data <- merge(ag_data, ag_stackers, by = "a_rowid")
    
    if(any(is.na(ag_data[, ..stacker_names]))) {
      stop("There are NAs in predictions from stackers for aggregated data.
               Please contact the core code team if you encounter this problem.")
    }
  } else {
    ag_covs <- extract_covariates(ag_data,
                                  all_cov_layers,
                                  id_col              = "a_rowid",
                                  return_only_results = TRUE,
                                  centre_scale        = FALSE,
                                  period_var          = 'year',
                                  period_map          = period_map)
    cov_names <- names(all_cov_layers)
    cs_ag_covs <- centreScale(ag_covs[, ..cov_names], df = covs_cs_df)
    ag_covs <- cbind(ag_covs[,- ..cov_names], cs_ag_covs)
    
    ag_data <- merge(ag_data, ag_covs, by = "a_rowid")
    
    if(as.logical(pipeline$config_list$use_raw_covs)) {
      if(any(is.na(ag_data[, ..cov_names]))) {
        stop("There are NAs in covariates for aggregated data.
               Please contact the core code team if you encounter this problem.")
      }
    }
  }
  
  the_data <- rbind(the_data, ag_data, fill = T)
}

# save .grd files of the stacker rasters
stacker_dir <- paste0(output_dir, "/stacker_rasters/")
if (!dir.exists(file.path(stacker_dir))){
  dir.create(stacker_dir)
}

if (pipeline$config_list$stacker_geotiffs == TRUE){
  stacked_names <- names(stacked_rasters)
  for (i in 1:length(stacked_rasters)) {
    name <- stacked_names[[i]]
    layer <- stacked_rasters[[i]]
    
    writeRaster(layer, 
                filename="FILEPATH", 
                format="raster",
                overwrite=TRUE, 
                options=c("INTERLEAVE=BAND","COMPRESS=LZW")) 
    
  }  
}

# create stacker raster maps of 1) input data and 2) raster result by child model + region + year
if (pipeline$config_list$stack_raster_output == TRUE) {
  for (i in seq_along(stacked_rasters)) {
    model <- stacked_rasters[[i]]
    model_name <- names(stacked_rasters[i])
    year_list <- pipeline$config_list$year_list
    
    pdf("FILEPATH")
    
    for (year in 1:length(year_list)) {
      raster_object <- model[[year]] 
      yr <- year_list[year]
      # convert raster to data.frame (using spdf as intermediary) for use in ggplot
      df <- as.data.frame(as(raster_object, "SpatialPixelsDataFrame"))
      colnames(df) <- c("value", "x", "y")
      data <- the_data %>% filter(year == yr) 
      
      # convert the relevant shapefile to an SF object
      shp <- st_as_sf(subset_shape)
      points_sf <- st_as_sf(data, coords = c("longitude", "latitude"), crs = st_crs(shp))
      
      # create plot for predicted prevalence
      p1 <- ggplot() +  
        geom_sf(data=shp, fill="grey", color="grey") +
        geom_tile(data=df, aes(x=x, y=y, fill=value), alpha=0.8) + 
        viridis::scale_fill_viridis() +
        theme_empty()
      
      # create plot showing the data points
      p2 <- ggplot() +
        geom_sf(data=shp, fill="grey", color="grey") +
        geom_sf(data=points_sf, aes(color=lf_prev), alpha=0.8) +
        viridis::scale_fill_viridis() +
        labs(fill="Prevalence") +  # Adjust the label as needed
        theme_empty()
      
      # Arrange the two plots side by side
      p <- ggarrange(p1, p2,
                     ncol=2, 
                     nrow=1, 
                     common.legend = TRUE, 
                     legend="right")
      
      p <- annotate_figure(p, top = text_grob(paste0("Pixel predictions from ", model_name," for ", reg, " in ", yr),
                                              color = "black", 
                                              face = "bold", 
                                              size = 20))
      
      print(p)
    }
    dev.off()
  }
}

############ postamble ############ 
# the pipeline_postamble saves all of the objects within R’s global environment as individual rda files in the Objects folder's corresponding node_id folder. The next script in the Dag looks up the previous node_id and loads in all of those rda files back into the global environment
pipeline_postamble()
q("no")
