prep_to_adjust <- function(to_adjust,
                           alternate_defs,
                           reference_def,
                           release_id = release, 
                           logit_transform
                           ){
  
  to_adjust[, data_id := 1:nrow(to_adjust)]
  to_adjust[standard_error == 1, standard_error := 0.99999]
  
  # Merge with loc_meta to account for some blank super regions
  loc_meta <- get_location_metadata(location_set_id = 9, release_id = release)
  loc_cols_replace <- c("super_region_name", "region_name", "ihme_loc_id")
  loc_meta <- loc_meta[, (c(loc_cols_replace, "location_id")), with = F]
  to_adjust[, (loc_cols_replace) := NULL]
  to_adjust <- merge(to_adjust, loc_meta, by = "location_id")
  to_adjust[, ihme_loc_abv := substr(ihme_loc_id,1,3)]
  
  ## Make a covariate for "orig_dorms" column
  defs <- c(alternate_defs, reference_def)
  ## set the definition = 0 if it is NA
  for (col in alternate_defs) to_adjust[is.na(get(col)), (col) := 0]
  ## see if each row is uniquely defined
  to_adjust$num_defs <- rowSums(to_adjust[, ..defs])
  multiple_defs <- ifelse(max(to_adjust$num_defs) > 1, T, F)
  to_adjust[, (alternate_defs) := lapply(.SD,as.logical), .SDcols = alternate_defs]
  to_adjust[, newCol := toString(alternate_defs[unlist(.SD)]), by = 1:nrow(to_adjust), .SDcols = alternate_defs]

  if(multiple_defs){
    dorm_separator <- "AND"
    for (def in defs) to_adjust[get(def) == 1 & num_defs == 1, definition := paste0(def)]
    to_adjust[, (alternate_defs) := lapply(.SD,as.logical), .SDcols = alternate_defs]
    to_adjust[, definition := toString(alternate_defs[unlist(.SD)]), by = 1:nrow(to_adjust), .SDcols = alternate_defs]
    to_adjust[, definition := gsub(" ","",definition)]
    to_adjust[, definition := gsub(",",dorm_separator,definition)]
    to_adjust[, definition := str_replace(definition,", ",dorm_separator)]
    to_adjust[, (alternate_defs) := lapply(.SD,as.integer), .SDcols = alternate_defs] #convert back to numeric. 
    if (nrow(to_adjust[is.na(definition)]) > 0) stop("Definitions haven't been labeled appropriately")
  } else {
    dorm_separator <- NULL
    for (def in defs) to_adjust[get(def) == 1, definition := paste0(def)]
    if (nrow(to_adjust[is.na(definition)]) > 0) stop("Definitions haven't been labeled appropriately")
  }
  
  ## Mark rows with a zero mean in original data, we will adjust only SE for these
  to_adjust[, original_zero_mean := ifelse(mean == 0, 1, 0)]
  ## offset mean
  fix_ones <- logit_transform
  to_adjust <- rm_zeros(to_adjust,
                        offset = T,
                        drop_zeros = F,
                        fix_ones = fix_ones)
  
  ## Take the original standard error into log- or logit-space using the delta method
  if(logit_transform) {
    step1 <- cw$utils$linear_to_logit(mean = array(to_adjust$mean), sd = array(to_adjust$standard_error))
    to_adjust[, "logit_mean" := step1[[1]]]
    to_adjust[, "logit_se" := step1[[2]]]
  } else {
    step1 <- cw$utils$linear_to_log(mean = array(to_adjust$mean), sd = array(to_adjust$standard_error))
    to_adjust[, "log_mean" := step1[[1]]]
    to_adjust[, "log_se" := step1[[2]]]
  }
  
  
}