#' Create modeling hierarchy for space-time smoothing
#'
#' Create modeling hierarchy for space-time smoothing, for use by 5q0, 45q15, and other processes
#'
#' @param prk_own_region logical, whether to process North Korea as its own region
#' @param old_ap logical, whether to add a new region containing old Andhra Pradesh results
#' @param gbd_year: numeric, corresponding to year of GBD to pull location hierarchy for, e.g. 2015, 2016, 2017
#' @export
#' @return data.table with full space-time hierarchy, as determined by region_name
#' @import data.table
#'
#' @examples
#' \dontrun{
#' get_spacetime_loc_hierarchy()
#' }

get_spacetime_loc_hierarchy <- function(prk_own_region=T, old_ap=T, gbd_round_id = 5){
  # Get locations from the database
  locs <- get_location_metadata(location_set_id = 82, gbd_round_id = gbd_round_id)

  locs[location_id == 4749, is_estimate := 1]
  locs <- locs[location_id != 6]
  locs[location_id %in% c(354, 361, 44533), level := 3]
  locs[parent_id == 44533,
       path_to_top_parent := gsub(",6,", ",44533,", path_to_top_parent)]

  data <- list()

  for(countries in unique(locs$location_id[locs$level == 3 & locs$is_estimate == 1])) {
    temp <- copy(locs)
    region_i <- locs$region_id[locs$location_id == countries]
    temp <- temp[grepl(paste0(",", countries), path_to_top_parent) & region_id == region_i]
    for(levels in unique(temp$level[temp$level > 3])){
      index <- paste0(countries,"_" ,levels)
      subnat_temp <- copy(temp)
      subnat_temp <- subnat_temp[level==levels]
      subnat_temp[,keep:=1]

      if(!countries %in% subnat_temp$parent_id & countries == 95){
        to_append <- copy(temp)
        parent <- subnat_temp$path_to_top_parent
        parent <- parent[1]
        parent <- strsplit(parent, ",")
        parent <- unlist(parent)
        parent <- parent[3]
        to_append <- to_append[level == 4 & !grepl(paste0(",", parent), path_to_top_parent)]
        to_append[,keep := 0]
        subnat_temp <- rbind(to_append, subnat_temp)
      }

      id <- locs$region_id[locs$location_id == countries]
      region <- copy(locs)
      region <- region[region_id == id & !location_id %in% temp$location_id & location_id != id & level == 3]
      subnat_temp <- rbind(subnat_temp, region, fill = T, use.names = T)
      subnat_temp <- subnat_temp[,.(location_id, ihme_loc_id, region_name, keep)]
      subnat_temp[, region_name := paste0(region_name, "_", index)]
      data[[index]] <- copy(subnat_temp)
    }
  }

  for(regions in unique(locs$region_id[!is.na(locs$region_id)])){
    temp <- copy(locs)
    temp <- temp[region_id == regions & level == 3]
    temp <- temp[,.(location_id, ihme_loc_id, region_name)]
    temp[,keep := 1]
    data[[paste0(regions)]] <- copy(temp)
  }

  data <- rbindlist(data, fill = T, use.names = T)
  data[is.na(keep), keep := 0]

  data[ihme_loc_id == "PRK" & region_name == "East Asia_44533_5", keep := 1]
  data[ihme_loc_id == "PRK" & region_name == "East Asia", keep := 0]

  if(prk_own_region==T){
    data <- data[ihme_loc_id!="PRK"]
    prk <- data.table(location_id = 7, ihme_loc_id = "PRK", region_name = "East Asia_PRK", keep = 1)
    data <- rbind(data, prk)
  }

  if(old_ap==T){
    ind <- data[region_name == "South Asia_163_4"]
    ind <- ind[!(ihme_loc_id %in% c("IND_4841"))] 
    ind[ihme_loc_id == "IND_4871", ihme_loc_id := "IND_44849"]
    ind[location_id == 4871, location_id := 44849]
    ind[location_id == 44849, keep := 1]
    ind[,region_name := "South_Asia_with_Old_Andhra_Pradesh"]

    ## appending it on
    data <- rbind(ind, data, use.names=T)

    data[(region_name == "South Asia_163_4" & ihme_loc_id != "IND_4841" & ihme_loc_id != "IND_4871"), keep := 0]
  }

  return(data)
}
