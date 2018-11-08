#' Aggregate results from lowest levels to highest levels of locations, to create a complete set of locations
#'
#' Given a data.table with data to aggregate, aggregate results into aggregate location/sex/age combinations
#' 
#' @param data a data.table containing only id_vars and value_vars
#' \itemize{
#'   \item This data.table must include at least ALL locations at the start_agg_level (default is level = 6, which can be found by get_locations(level="lowest"))
#'   \item Variables: location_id, value_vars (note: please list location_id as one of the id_vars)
#'   \item If age_aggs option is specified, age_group_id must be present
#'   \item If agg_sex option is specified, sex_id must be present
#'   \item All variables in the dataset must be specified as either id_vars or value_vars
#' }
#' @param age_aggs If you want to aggregate to GBD Compare aggregate ages, age_aggs="gbd_compare"
#'   If you want to aggregate to age groups used for vetting, age_aggs="vetting"
#' @param agg_sex: T if you want to create both sexes
#' @param loc_scalars When you aggregate to region/super-region/global, do you want to apply official GBD scalars? 
#' \itemize{
#'   \item NOTE: We use 95+ scalars for all over-95 granular age groups if they exist
#'   \item NOTE: We use ENN scalars for age under-1 if it exists -- currently, under-1 and ENN/PNN/LNN are all the same (NOT GUARANTEED TO STAY THIS WAY) dataset
#' }
#' @param agg_hierarchy If you want to aggregate up from locations -- this is usually the main purpose of the code
#' \itemize{
#'   \item NOTE: If a parent location already exists in the dataset, \strong{THE CODE WILL NOT OVERWRITE THE PARENT DATA}. So if you have, say, England in the dataset and all the England subnationals, this will skip aggregating England and use the pre-existing numbers for it
#' }
#' @param start_agg_level The location level at which you want to start the aggregations (e.g. set to 3 if you only want to start aggregations from the country level on up to region/super/global, rather than lowest-level)
#' @param end_agg_level The location level at which you want to stop the aggregations (e.g. set to 3 if you only want to create up to the country-level, but not region/super/global)
#' @param agg_sdi Generate SDI aggregates based off of lowest-level locations
#' @param tree_only Character, ihme_loc_id of the head of a tree which you want to aggregate. Allows you to only aggregate a single country rather than the entire hierarchy.
#'  e.g. "ZAF" would only aggregate from ZAF subnationals to ZAF nationals, but not throw errors about other countries being missing
#' @param gbd_year numeric, GBD year to use (2010, 2013, 2015, 2016, 2017)
#'  
#' @export
#' @return data.table with id_vars and value_vars, along with new observations for location, age, and sex aggregates
#' @import data.table
#' @import haven
#' @import mortdb


agg_results <- function(data,id_vars,value_vars,age_aggs="",agg_sex = F,loc_scalars=T,agg_hierarchy=T,start_agg_level=6,end_agg_level=0,agg_sdi=F,tree_only="",gbd_year=2017) {
    
    ## Enforce data as a data.table, and all variables are present that should be there
    data <- data.table(data)
    
    if(length(colnames(data)[!colnames(data) %in% c(value_vars,id_vars)]) != 0) {
      stop(paste0("These variables are not specified in either id_vars or value_vars: ", paste(colnames(data)[!colnames(data) %in% c(value_vars,id_vars)], collapse = " ")))
    }
    
    if(length(colnames(data)) < length(c(value_vars,id_vars))) {
      stop(paste0("Not all id_vars or value_vars exist in the dataset"))
    }
    
    if(!"location_id" %in% colnames(data)) {
      stop("Need to have location_id to aggregate locations")
    }
    
    if(("ihme_loc_id" %in% colnames(data)) | ("location_name" %in% colnames(data)) | length(colnames(data)[grepl("country",colnames(data))]) >0 ) {
      stop("Cannot have location identifiers other than location_id (e.g. ihme_loc_id, location_name, or country*)")
    }

    if(agg_sex == T & (("sex" %in% id_vars) | ("sex_name" %in% id_vars))) {
      stop("Cannot have sex identifiers other than sex_id in your id_vars if you're trying to agg over sex")
    }

    if(age_aggs != "") {
      stopifnot("age_group_id" %in% id_vars)
      if(("age" %in% id_vars) | ("age_group_name" %in% id_vars)) {
        stop("Cannot have age identifiers other than age_group_id in your id_vars if you're trying to agg over age")
      }
    }

    if(loc_scalars == T & agg_hierarchy == T) {
      if(!("year_id" %in% id_vars) | !("sex_id" %in% colnames(data)) | !("age_group_id" %in% colnames(data))) {
        stop("Need year_id, age_group_id, and sex_id to merge on location scalars")
      }
      if(("parent_id" %in% id_vars) | ("level" %in% id_vars) | ("scaling_factor" %in% id_vars)) {
        stop("Cannot have parent_id, level, or scaling_factor in your dataset if you want to use location scalars")
      }
    }

  ## Bring in regional scalars if we want to use them
    if(loc_scalars == T & agg_hierarchy == T) {
      # pop_scalars <- data.table(haven::read_dta(paste0(root,"/Project/Mortality/Population/analyses/pop_scalars/results/gbd_scalars.dta")))
      pop_scalars <- get_mort_outputs("population scalar", "estimate", run_id = "best", gbd_year = gbd_year)
      pop_scalars <- pop_scalars[, .(location_id, year_id, scaling_factor = mean)]
      setkeyv(pop_scalars, c("location_id","year_id"))
    }
  
  ## Specify variables to collapse by for the age, sex, and location aggregates
    age_collapse_vars <- id_vars[!id_vars %in% "age_group_id"]
    sex_collapse_vars <- id_vars[!id_vars %in% "sex_id"]
    loc_collapse_vars <- c(id_vars[!id_vars %in% "location_id"],"parent_id")

  
  ## Aggregate from level 5 locations to level 4 (India state/urbanicity to India state), etc. etc. up to Global
    if(agg_hierarchy==T) {
      ## Merge on parent_id and level to the dataset
      locations <- data.table(get_locations(level="all", gbd_year=gbd_year))
      data <- merge(data,locations[, list(location_id, parent_id, level)],by="location_id")
      
      if(tree_only != "") {
        print(paste0("Aggregating hierarchy for ", tree_only, " children and below only"))
        tree_top <- locations[ihme_loc_id == tree_only, location_id]
        locations <- locations[grepl(paste0(",", tree_top, ","), path_to_top_parent) | location_id == tree_top, ]
      }

      setkeyv(data, loc_collapse_vars)

      ## Apply aggregations, rolling from bottom-up, skipping locations that already exist, and applying regional scalars
      for(agg_level in start_agg_level:end_agg_level) {
        loc_list <- locations[level == agg_level & !(location_id %in% unique(data[,location_id])),location_id] # Aggregate to parent only if the parent doesn't already exist in the datset
        if(tree_only != "" & length(loc_list) == 0) next # This should only happen if tree_top is specified, skip to the next level

        ## Check that all the children actually exist in the source dataset, to prevent a parent location being an incomplete sum of all of its children
        child_list <- locations[level==(agg_level + 1) & parent_id %in% loc_list,location_id]

        child_exist <- unique(data[parent_id %in% loc_list,location_id])
        missing_list <- child_list[!(child_list %in% child_exist)]
        if(length(missing_list) != 0) {
          print(missing_list)
          stop(paste0("The above child locations are missing, cannot aggregate"))
        }
        agg <- data[parent_id %in% loc_list, lapply(.SD,sum), .SDcols=value_vars, by=loc_collapse_vars]
        setnames(agg,"parent_id","location_id")
        
        ## If we want to apply regional scalars to the data, do so here when the agg_level = 2 (region-level). Will then be carried on to levels 1 and 0
        if(loc_scalars == T & agg_level == 2) {
          mult_scalars <- function(x) return(x * agg[['scaling_factor']])
          agg <- merge(agg, pop_scalars, by = c("location_id","year_id"))
          agg[,(value_vars) := lapply(.SD, mult_scalars), .SDcols = value_vars]
          agg[,scaling_factor := NULL]
        }
        
        agg <- merge(agg,locations[, list(location_id, parent_id, level)],by="location_id") # Get parent_id for next aggregation

        data <- rbindlist(list(data,agg),use.names=T)
      }
      data[,c("parent_id","level") := NULL]
    }
  
  ## Aggregate to SDI bins if desired
    if(agg_sdi==T) { ## This will aggregate lowest-level locations to SDI locations (hierarchy based on 2015 SDI bins), WITHOUT any scalars etc.
      locations <- data.table(get_locations(gbd_type="sdi", gbd_year = gbd_year))
      locations <- locations[,list(location_id,parent_id,level)]
      data <- merge(data,locations,by="location_id",all.x=T)
      
      loc_list <- locations[level == 0,location_id] # Aggregate to parent SDI categories
      
      ## Check that all the children actually exist in the source dataset, to prevent a parent location being an incomplete sum of all of its children
      child_list <- locations[level==1,location_id]
      child_exist <- unique(data[parent_id %in% loc_list,location_id])
      missing_list <- child_list[!(child_list %in% child_exist)]
      if(length(missing_list) != 0) {
        print(missing_list)
        stop(paste0("The above child locations are missing, cannot aggregate"))
      }

      setkeyv(data, loc_collapse_vars)
      
      agg <- data[parent_id %in% loc_list, lapply(.SD,sum), .SDcols = value_vars, by = loc_collapse_vars]
      setnames(agg, "parent_id", "location_id")
      data[,c("parent_id","level"):=NULL]
      data <- rbindlist(list(data,agg), use.names=T)
    }
  
  ## Add age aggregates for gbd compare tool and for under-1/all ages if needed
    if(age_aggs == "gbd_compare") { 
      req_ages <- c(c(2:20), c(30:32), 235)
      if(length(req_ages[!req_ages %in% unique(data[,age_group_id])]) != 0) {
        stop(paste0("These age_group_ids are not present in the dataset: ", paste(req_ages[!req_ages %in% unique(data[,age_group_id])], collapse = " ")))
      }

      setkeyv(data, age_collapse_vars)
      
      ## Under-1
      under_1 <- data[age_group_id >= 2 & age_group_id <= 4,lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      under_1[,age_group_id:=28]
      
      ## All Ages
      all_ages <- data[,lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      all_ages[,age_group_id:=22]
      
      ## GBD Compare: Under-5, 5-14, 15-49, 50-69, 70+, 80+ (not GBD Compare but needed for vetting etc.)
      under_5 <- data[age_group_id >= 2 & age_group_id <= 5,lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      under_5[,age_group_id := 1]
      
      d_5_14 <- data[age_group_id >= 6 & age_group_id <= 7,lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      d_5_14[,age_group_id := 23]
      
      d_15_49 <- data[age_group_id >= 8 & age_group_id <= 14,lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      d_15_49[,age_group_id := 24]
      
      d_50_69 <- data[age_group_id >= 15 & age_group_id <= 18,lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      d_50_69[,age_group_id := 25]
      
      d_70plus <- data[age_group_id %in% c(19, 20, 30, 31, 32, 235),lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      d_70plus[,age_group_id := 26]
      
      d_80plus <- data[age_group_id %in% c(30, 31, 32, 235),lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      d_80plus[,age_group_id := 21]
      
      d_0_19 <- data[age_group_id %in% c(2,3,4,5,6,7,8),lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      d_0_19[,age_group_id := 158]
      
      d_10_24 <- data[age_group_id %in% c(7,8,9),lapply(.SD,sum),.SDcols=value_vars,by=age_collapse_vars]
      d_10_24[,age_group_id := 159]
      
      ## Append all together
      data <- rbindlist(list(data, under_1, all_ages,under_5, d_5_14, d_15_49, d_50_69, d_70plus, d_80plus, d_0_19, d_10_24), use.names = T)
    } else if (age_aggs == "vetting") {
      # check that all age_group_ids required for aggregating to the desired age groups exist
      req_ages <- c(c(2:20), c(30:32), 235)
      if (length(req_ages[!req_ages %in% unique(data[, age_group_id])]) != 0) {
        stop(paste0("These age_group_ids are not present in the dataset: ", paste(req_ages[!req_ages %in% unique(data[,age_group_id])], collapse = " ")))
      }

      setkeyv(data, age_collapse_vars)
      
      under_1 <- data[age_group_id >= 2 & age_group_id <= 4, lapply(.SD, sum), .SDcols = value_vars, by = age_collapse_vars]
      under_1[, age_group_id := 28]
      
      all_ages <- data[, lapply(.SD, sum), .SDcols = value_vars, by = age_collapse_vars]
      all_ages[, age_group_id := 22]
      
      d_80plus <- data[age_group_id %in% c(30, 31, 32, 235), lapply(.SD, sum), .SDcols = value_vars, by = age_collapse_vars]
      d_80plus[, age_group_id := 21]
      
      # append
      data <- rbindlist(list(data, under_1, all_ages, d_80plus), use.names = T)
    }
    
  ## Aggregate to both sexes if needed
    if(agg_sex == T) {
      stopifnot("sex_id" %in% id_vars)
      req_sexes <- c(1:2)
      if(length(req_sexes[!req_sexes %in% unique(data[,sex_id])]) != 0) {
        stop(paste0("These sex_ids are not present in the dataset: ", paste(req_sexes[!req_sexes %in% unique(data[,sex_id])], collapse = " ")))
      }

      setkeyv(data, sex_collapse_vars)
      
      both_sexes <- data[, lapply(.SD,sum), .SDcols=value_vars, by = sex_collapse_vars]
      both_sexes[, sex_id := 3]
      data <- rbindlist(list(data, both_sexes), use.names = T)
    }
  
  return(data)
}



