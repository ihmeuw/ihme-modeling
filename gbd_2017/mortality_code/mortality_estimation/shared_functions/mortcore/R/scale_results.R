#' Scale results from subnationals to nationals (or other geographies)
#' 
#' Given results for aggregate locations, add up and scale the locations underneath them to add up to the aggregate location
#' Carry out scaling to start at the closest levels first, then progressively to lower levels of the hierarchy.
#' For example, scale level 4 locations to level 3 locations, then level 5 to level 4, etc.
#' 
#' @param data data.table or data.frame, with the following requirements:
#' \itemize{
#'   \item No NA values in id_vars or value_var
#'   \item Contains a variable called location_id
#'   \item Cannot contain parent_id, scaling_factor, or level variables
#'   \item Must be square across id_vars -- one unique observation for each unique combination of id variables
#' }
#' @param id_vars character vector, names of variables in the dataset that uniquely identify all observations in data.
#' \itemize{
#'   \item id_vars must NOT include location-specific IDs except for location_id (e.g. do not include ihme_loc_id, location_name, etc.)
#'   \item If age_group_id or sex_id is included, make sure that you are not trying to aggregate both an aggregate group as well as granular groups. e.g. do NOT try to scale "All Ages" alongside granular ages -- they won't add up at the end due to how scaling is applied
#' }
#' @param location_set_id numeric, the location_set_id to use to generate a hierarchy to scale results across.
#' @param gbd_round_id numeric, the gbd_round_id with which to pull the location_set_id.
#' @param value_var character, name of variable that is being scaled. Must be numeric, not factor or character. If an integer, will be coerced to double to allow for precise scaling.
#' @param exclude_tree character or numeric vector, the ihme_loc_ids or location_ids of all parent locations under which you want to exclude the entire tree from scaling. e.g. "GBR" would make sure that none of the locations under GBR or any of their children (and so on so forth) would be scaled
#' @param exclude_parent character or numeric vector, the ihme_loc_ids or location_ids of all parent locations whose children you don't want to scale (but whose grandchildren you are ok with scaling). For example, "CHN" would not scale CHN_44533 and HKG/MAC to CHN, but all the other CHN subnationals would still be scaled to CHN_44533
#' @param parent_start numeric, default 3. Level of parents to begin scaling from. Starts with level 4 to level 3 scaling by default. If setting parent_start < 3, make sure that regions do not already have population scalars applied to them (as with GBD populations and death numbers)
#' @param parent_end numeric, the level of parents to end with. Ends with level 6 to level 5 by default.
#' @param exception_gbr_1981 logical, whether to scale only England and Wales to the UK minus Scotland and Northern Ireland in pre-1981 years. Only applicable for 5q0 and 45q15. Requires either year or year_id in dataset (NOT BOTH).
#' @param scale_former_ussr logical, whether to scale former USSR locations by USSR estimates (only scales up to 1992)
#' @param scale_former_yugoslavia logical, whether to scale former Yugoslavia locations by Yugoslavia estimates (only scales up to 1992)
#' 
#' @export
#' @return data.frame or data.table with the same rows and observations as input dataset, but with value_var scaled up to the parent value. Additional variables beyond id_vars and value_var will be preserved with original values. Note: The order of the dataset may change.
#' @import data.table
#' @import mortdb

#' @examples 
#' \dontrun{
#' if (Sys.info()[1]=="Windows") root <- "J:" else root <- "/home/j"
#' source(paste0(root, "/temp/central_comp/libraries/current/r/get_location_metadata.R"))
#' loc_ids <- locations$location_id
#' sexes <- c(1,2)
#' years <- c(1980:1990)
#' 
#' test <- data.table(expand.grid(location_id=loc_ids, sex_id=sexes, year_id=years))
#' test[, population := 1000]
#' test[, other_var := 90]
#' scale_results(test, id_vars=c("location_id", "sex_id", "year_id"), value_var="population", parent_start=3, gbd_round_id=4)
#' }

scale_results <- function(data, id_vars="", value_var="", location_set_id=21, gbd_round_id=5, exclude_parent=NULL, exclude_tree=NULL,
                          parent_start=3, parent_end=5, assert_ids=T, exception_gbr_1981 = F, scale_former_ussr = F, scale_former_yugoslavia = F) {

    ######################################################################################################
    ## Function setup
  
    ## Enforce data as a data.table
    if(!is.data.table(data)) {
        dt_indic <- F
        setDT(data)
    } else {
        dt_indic <- T
    }

    ## Bring in metadata etc.
    ## Remove get_location_metadata because increased vulnerability of cluster job crashes etc.
    # source(paste0(root, "/temp/central_comp/libraries/current/r/get_location_metadata.R"))
    # locations <- get_location_metadata(location_set_id, gbd_round_id=gbd_round_id)
    if(location_set_id %in% c(21, 82)) {
        if(location_set_id == 21) loc_gbd_type <- "mortality"
        if(location_set_id == 82) loc_gbd_type <- "ap_old"
        locations <- data.table::setDT(get_locations(level = "all", gbd_type = loc_gbd_type))
    }

    ## Set target levels for the function
        target_levels <- c(parent_start:parent_end)

    ## Set the variables to collapse over
        collapse_vars <- id_vars[id_vars != "location_id"]


    ######################################################################################################
    ## Exclude locations from scaling
    ## For exclude_tree, exclude both the location and all subnationals below it, using the path_to_top_parent variable as the way to keep them out (if multi-level e.g. India)
      if(is.null(exclude_tree)) {
        locations[, exclude := 0]
      } else {
        if(is.character(exclude_tree)) {
          locations[ihme_loc_id %in% exclude_tree, exclude := 1]
        } else {
          locations[location_id %in% exclude_tree, exclude := 1]
        }
        exclude_ids <- unique(locations[exclude==1, location_id])

        # Exclude all places that have the specified location in their tree (doesn't work for topmost in tree, but you wouldn't want to run scale_results if you didn't want to scale anything in the entire tree)
        for(id in exclude_ids) {
          locations[grepl(paste0(",",id,","), path_to_top_parent), exclude := 1]
        }
        
        locations[is.na(exclude), exclude := 0]
      }
        
    ## For exclude_parent, exclude the parent location only but none of the levels underneath it
      if(!is.null(exclude_parent)) {
        if(is.character(exclude_parent)) {
          locations[ihme_loc_id %in% exclude_parent, exclude := 1]
        } else {
          locations[location_id %in% exclude_parent, exclude := 1]
        }
      }


    ######################################################################################################
    ## Assertions on function arguments, for unique IDs, and no NA values
    
    if(length(colnames(data)) < length(c(value_var,id_vars))) {
      stop(paste0("Not all id_vars or value_var exist in the dataset"))
    }
    
    if(!"location_id" %in% colnames(data)) {
      stop("Need to have location_id to aggregate locations")
    }

    if(("ihme_loc_id" %in% id_vars) | ("location_name" %in% id_vars) | length(colnames(data)[grepl("country",id_vars)]) >0 ) {
      stop("Cannot have location identifiers other than location_id (e.g. ihme_loc_id, location_name, or country*)")
    }

    if("scaling_factor" %in% colnames(data) | "level" %in% colnames(data) | "parent_id" %in% colnames(data)) {
        stop("Cannot have variables named scaling_factor, level, or parent_id in data")
    }
        
    if(is.integer(data[[value_var]])) {
      warning(paste0("Coercing ", value_var, "from integer to double to allow for precise scaling"))
      data[, (value_var) := lapply(.SD, as.double), .SDcols=value_var]
    }
        
    if(is.factor(data[[value_var]]) | is.character(data[[value_var]])) {
      stop(paste0(value_var, "is a factor or character variable -- convert to numeric before scaling"))
    }

    ## Throw warning if parent_start < 3
    if(parent_start < 3) {
        warning(paste0("You have chosen to scale up to level ", parent_start, 
            ". Trying to scale to region/global may run into regional scalar issues if using population or deaths as a denominator. ",
            "Make sure you are not trying to scale from country-to-region if the region denominator has regional scalars applied to them already (e.g. anything using standard GBD populations)."))
    }

    ## Throw warning if sex_id 1, 2, AND 3 exist, or all ages exist alongside other ages (non-two-way-scaling problem)
    if("sex_id" %in% id_vars) {
        if(3 %in% unique(data$sex_id) & (1 %in% unique(data$sex_id) | 2 %in% unique(data$sex_id))) {
            stop(paste0("You are attempting to scale both-sexes and male and/or female at the same time. ",
                        "Scaling by location will make the sex-specific results not add up to the both-sexes results."))
        }
    }

    if("age_group_id" %in% id_vars) {
        if(22 %in% unique(data$age_group_id) & (length(unique(data$age_group_id)) > 1)) {
            stop(paste0("You are attempting to scale all ages and another age group at the same time. ",
                        "Scaling by location will make the age-specific results not add up to the all-age results."))
        }
    }

    ## Check that all locations in the data frame actually exist in the specified location_set_id
    data_loc_ids <- unique(data$location_id)
    expected_locs <- unique(locations$location_id)
    if (scale_former_ussr) {
      expected_locs <- c(expected_locs, 420)
    }
    if (scale_former_yugoslavia) {
      expected_locs <- c(expected_locs, 320)
    }
    missing_locs <- unique(data_loc_ids[!(data_loc_ids %in% expected_locs)])
    if(length(missing_locs) > 0) {
        stop(paste0("Your dataset includes the following location_ids which don't exist in location_set_id ",
                    location_set_id, ": ",
                    paste(missing_locs, collapse=" ")))
    }

    ## Check that all required variables for GBR exception exist if this is run
    if(exception_gbr_1981 == T) {
        year_var <- id_vars[id_vars == "year" | id_vars == "year_id"]
        if(length(year_var) > 1) stop("You have multiple id_vars that contain the string year -- include only year OR year_id if you want to run GBR 1981 exception")
        if(length(year_var) == 0) stop("To run GBR 1981 exception, you need a variable year or year_id in the dataset")
        if(!is.numeric(data[[year_var]])) stop(paste0(year_var, " must be numeric for GBR 1981 exception"))
    }

    ## Check for unique combinations of id_vars
    if(assert_ids == T) {
      id_list <- list()
      for(var in id_vars) {
        id_list[[var]] <- unique(data[[var]])
      }
      setkeyv(data, id_vars)
      assertable::assert_ids(data, id_vars=id_list, quiet=T)
    }

    ## Make sure no ID variables or value variables are NA
    assertable::assert_values(data, c(id_vars, value_var), "not_na", quiet=T)


    ######################################################################################################
    ## Define scaling function, and then run it recursively down the location hierarchy
    data <- merge(data, locations[, list(location_id, parent_id, level)], by="location_id", all.x=T)

    scale_children_to_parent <- function(scale_data, child_ids, old_parent_id, start_scale_year = NULL, end_scale_year = NULL) {
      orig_row_count <- nrow(scale_data)
      child_data <- scale_data[location_id %in% child_ids] # New Andhra Pradesh and Telangana
      parent_data <- scale_data[location_id == old_parent_id, .SD, .SDcols=c(collapse_vars, value_var)]
      
      year_var <- id_vars[id_vars == "year" | id_vars == "year_id"]
      data_has_years <- length(year_var) == 1
      if (data_has_years) {
        if (is.null(end_scale_year)) {
          end_scale_year <- max(scale_data[[year_var]])
        }
        
        if (is.null(start_scale_year)) {
          start_scale_year <- min(scale_data[[year_var]])
        }
        
        child_data <- child_data[get(year_var) >= start_scale_year & get(year_var) <= end_scale_year]
        parent_data <- parent_data[get(year_var) >= start_scale_year & get(year_var) <= end_scale_year]
      }
      
      setnames(parent_data, c(value_var), c("parent_value"))
      child_agg <- child_data[, lapply(.SD,sum), .SDcols=value_var, by=collapse_vars]
      setnames(child_agg, value_var, "child_value")
      parent_data <- merge(child_agg, parent_data, by=collapse_vars, all=T)
      if(nrow(child_agg) != nrow(parent_data)) stop(paste0("Something went wrong when merging children aggregates and parents on level ", start_level))
      
      parent_data[, scaling_factor := parent_value/child_value]
      assertable::assert_values(parent_data, "scaling_factor", "not_na", quiet=T)
      parent_data[, c("parent_value", "child_value") := NULL]
      
      child_data <- merge(child_data, parent_data, by=collapse_vars)
      child_data[, (value_var) := get(value_var) * scaling_factor]
      child_data[, scaling_factor := NULL]
      
      assertable::assert_values(child_data, value_var, "not_na", quiet=T)
      if (data_has_years) {
        scale_data <- scale_data[get(year_var) < start_scale_year | get(year_var) > end_scale_year | !(location_id %in% child_ids)]
      } else {
        scale_data <- scale_data[!(location_id %in% child_ids)]
      }
      scale_data <- rbindlist(list(scale_data, child_data), use.names=T, fill=T)
      if (orig_row_count != nrow(scale_data)) stop("Some data lost during merge")
      rm(child_data, parent_data)
      return(scale_data)
    }
    
    ## If location_set_id is 82 and Telangana exists in the dataset, implement custom scaling of new AP and Telangana to old AP
    if(location_set_id == 82 & 4871 %in% data_loc_ids) {
      if(!4841 %in% unique(data$location_id) | !44849 %in% unique(data$location_id)) stop("If using location_set_id 82, need to have Old AP, New AP, and Telangana to implement scaling")
      print("Location Set ID 82 includes Old/New Andhra Pradesh and Telangana. Scaling New AP and Telangana to Old AP.")
      data <- scale_children_to_parent(scale_data = data, child_ids = c(4871, 4841), old_parent_id = 44849)
    }
    
    ## If USSR and Yugoslavia subnationals exist, add USSR/Yugoslavia scaling
    ussr_yug_end_year <- 1992 #scale data until end_year
    if (scale_former_ussr) {
      ussr_child_locs <- c(33, 34, 35, 36, 37, 39, 40, 41, 57, 58, 59, 60, 61, 62, 63)
      if (!all(ussr_child_locs %in% unique(data_loc_ids))) {
        stop(paste0("Missing some USSR child locations. Check for the following location_ids: ", paste(ussr_child_locs, collapse = ", ")))
      }
      data <- scale_children_to_parent(scale_data = data, child_ids = ussr_child_locs, old_parent_id = 420, end_scale_year = 1992)
    }
    
    if (scale_former_yugoslavia) {
      yugoslavia_child_locs <- c(44, 46, 49, 50, 53, 55)
      if (!all(yugoslavia_child_locs %in% unique(data_loc_ids))) {
        stop(paste0("Missing some Yugoslavia child locations. Check for the following location_ids: ", paste(yugoslavia_child_locs, collapse = ", ")))
      }
      data <- scale_children_to_parent(scale_data = data, child_ids = yugoslavia_child_locs, old_parent_id = 320, end_scale_year = 1992)
    }
    
    ## Run scale_level: also relies on the locations data.table, and definitions of value_var and collapse_vars (not sure whether I need to/should pass-through here)
    scale_level <- function(dt, start_level, end_level) {
        levelplus <- start_level + 1
        
        # Pull child locations, get the parents of those, then pull the children that we expect from there
        child_list <- unique(locations[level == levelplus & location_id %in% data_loc_ids, location_id])
        parent_list <- unique(locations[location_id %in% child_list, parent_id])
        parent_list <- unique(locations[location_id %in% parent_list & exclude != 1 & level == start_level, location_id])
        
        # Remove children whose parents we're skipping from scaling, and create a comparison child_locs list
        child_list <- unique(locations[location_id %in% child_list & parent_id %in% parent_list, location_id])
        child_locs <- locations[parent_id %in% parent_list, location_id]

        if(location_set_id == 82) {
          child_locs <- child_locs[child_locs != 44849] # Remove Old Andhra Pradesh from the list of eligible children if it exists (will use New AP and Telangana instead)
          child_list <- child_list[child_list != 44849]
        }

        if(!identical(sort(child_list), sort(child_locs))) {
            miss_child_locs <- child_locs[!(child_locs %in% unique(child_data$location_id))]
            stop(paste0("Missing the following level ", levelplus, " child locations whose parent location exists. Specify exclude_parent or exclude_tree to skip scaling from children to the parent, or use a different location_set_id.",
                        paste(miss_child_locs, collapse=" ")))
        }

        child_data <- dt[location_id %in% child_locs,]

        if(nrow(child_data) > 0) {
            parent_data <- dt[location_id %in% parent_list, .SD, .SDcols=c(collapse_vars, value_var, "location_id")] 
            if(length(unique(parent_data$location_id)) != length(parent_list)) {
              miss_parent_locs <- parent_list[!(parent_list %in% unique(parent_data$location_id))]
              stop(paste0("Missing the following level ", start_level, " parent locations whose child locations exist. Specify exclude_parent or exclude_tree to skip scaling from children to the parent, or use a different location_set_id.",
                          paste(miss_parent_locs, collapse=" ")))
            }
            setnames(parent_data, c(value_var, "location_id"), c("parent_value", "parent_id"))
            setkeyv(parent_data, c(collapse_vars, "parent_id"))

            ## Perform raking of England and Wales to UK minus Scotland and Northern Ireland (if specified)
            if(exception_gbr_1981 == T & 95 %in% parent_list) {
                eng_wales <- child_data[get(year_var) <= 1980 & location_id %in% c(4749, 4636)]
                scot_ni <- child_data[get(year_var) <= 1980 & location_id %in% c(433, 434)]
                gbr_data <- parent_data[get(year_var) <= 1980 & parent_id == 95]
                child_rows <- nrow(eng_wales) + nrow(scot_ni)

                ## Subtract Scotland and Northern Ireland values from GBR
                agg_scot_ni <- scot_ni[, lapply(.SD,sum), .SDcols=value_var, by=c(collapse_vars, "parent_id")]
                setnames(agg_scot_ni, value_var, "child_value")
                gbr_data <- merge(gbr_data, agg_scot_ni, by=c(collapse_vars, "parent_id"), all=T)
                if(nrow(gbr_data) != nrow(agg_scot_ni)) stop(paste0("Something went wrong when merging Scot/NI to GBR"))
                gbr_data[, parent_value := parent_value - child_value]
                gbr_data[, child_value := NULL]
                if(nrow(gbr_data[parent_value < 0]) > 0) stop("GBR minus Scotland and Northern Ireland has resulted in negative values. Check that Scotland and NI do not exceed GBR if using exception_gbr_1981 = T")

                ## Scale England and Wales to the remainder
                agg_eng_wales <- eng_wales[, lapply(.SD,sum), .SDcols=value_var, by=c(collapse_vars, "parent_id")]
                setnames(agg_eng_wales, value_var, "child_value")
                gbr_data <- merge(gbr_data, agg_eng_wales, by=c(collapse_vars, "parent_id"), all=T)
                if(nrow(gbr_data) != nrow(agg_eng_wales)) stop(paste0("Something went wrong when merging Scot/NI to GBR"))
                gbr_data[, scaling_factor := parent_value/child_value]
                gbr_data[, c("parent_value", "child_value") := NULL]

                eng_wales <- merge(eng_wales, gbr_data, by=c(collapse_vars, "parent_id"))
                eng_wales[, (value_var) := get(value_var) * scaling_factor]
                eng_wales[, scaling_factor := NULL]
                
                assertable::assert_values(eng_wales, value_var, "not_na", quiet=T)

                ## Remove all GBR child and parent locations from aggregation
                if(child_rows != nrow(eng_wales) + nrow(scot_ni)) stop("Something went wrong with GBR 1981 exception and number of matching observations")
                child_data <- child_data[get(year_var) >= 1981 | !(location_id %in% c(4749, 4636, 433, 434))]
                parent_data <- parent_data[get(year_var) >= 1981 | parent_id != 95]
            }

            ## Do scaling here
            orig_child_rows <- nrow(child_data)
            child_data <- merge(child_data, parent_data, by=c(collapse_vars, "parent_id"), all=T)
            if(orig_child_rows != nrow(child_data)) stop("Child-parent merge produced more rows than expected -- check that id vars are unique and parents all have children")
            child_data[, (value_var) := get(value_var) * parent_value / sum(get(value_var)), by = c(collapse_vars, "parent_id")]
            child_data[, parent_value := NULL]

            assertable::assert_values(child_data, value_var, "not_na", quiet=T)

            dt <- dt[!location_id %in% child_locs]
            dt <- rbindlist(list(dt, child_data), use.names=T, fill=T)
            if(exception_gbr_1981 == T & 95 %in% parent_list) dt <- rbindlist(list(dt, eng_wales, scot_ni), use.names=T)
        } else {
          warning(paste0("No locations available to scale to parent level ",start_level))
        }
        
        ## Here, call the function within itself to RECURSE until you reach the final end_level
        if(start_level != end_level) {
          new_start <- start_level + 1
          scale_level(dt, new_start, end_level)
        } else {
          return(dt)
        }
    }

    ## Prepare data for scaling function by setting keys and getting the number of rows in the original dataset
    setkeyv(data, c(collapse_vars, "parent_id"))
    old_rows <- nrow(data)
    if(parent_start < parent_end) data <- scale_level(dt=data, start_level=parent_start, end_level=parent_end)
    
    if(nrow(data) != old_rows) stop("The scaling does not return the same number of results -- something has gone wrong!")
    data[, c("parent_id", "level") := NULL]
    
    if(dt_indic == F) {
        data <- data.frame(data)
    }
    return(data)
}


