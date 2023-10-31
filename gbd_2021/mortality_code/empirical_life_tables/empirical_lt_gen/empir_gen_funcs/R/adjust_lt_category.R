#' Adjust life table category (universal, location-specific, outlier) based on provided designations.
#' Designations may come from machine vision or manual review.
#'
#' @param dt data.table with empirical life tables
#' @param review data.table with id variables, plus life_table_category_id representing desired category
#' @param id_vars vector of id variables, not including age
#' @param loc_list vector of location_ids for which we will add back in excluded ELTs as loc-specific and smooth width 7
#'
#' @return data.table with modified life table category id
#' @export
#'
#' @import data.table


adjust_lt_category <- function(dt, review, id_vars, loc_list){
  
    by_vars <- id_vars[id_vars != "smooth_width"]
  
    # merge on review decisions
    setnames(review, "life_table_category_id", "new_lt_cat")
    dt <- merge(dt, review, by = id_vars, all.x = T)
    
    # always take review's decision unless outlier_type_id = 18 (45q15/5q0 outlier)
    dt[!is.na(new_lt_cat) & outlier_type_id != 18, life_table_category_id := new_lt_cat]
    
    # for some locations, we keep LTs as loc-specific to improve fit
    dt[location_id %in% loc_list, temp := min(life_table_category_id), by = by_vars]
    dt[temp == 5 & smooth_width == 7, life_table_category_id := 2]
    dt[, temp := NULL]
    
    # re-select smallest smooth_width, take care of keeping loc-specific if
    #   lower smooth-width than lowest non-outlier universal
    dt[life_table_category_id == 1, min_smooth := as.integer(min(smooth_width, na.rm = T)), by = by_vars]
    dt[, min_smooth := as.integer(min(min_smooth, na.rm = T)), by = by_vars]
    dt[smooth_width > min_smooth & life_table_category_id %in% c(1,2), life_table_category_id := 5]
    dt[, min_smooth := NULL]
    dt[life_table_category_id == 2, min_smooth := min(smooth_width, na.rm = T), by = by_vars]
    dt[smooth_width > min_smooth & life_table_category_id == 2, life_table_category_id := 5]
    
    # adjust outlier_type_id (id 13 = manual outlier)
    dt[life_table_category_id == 5 & outlier_type_id == 1, outlier_type_id := 13]
    dt[life_table_category_id %in% c(1,2), outlier_type_id := 1]
    
    return(dt)
  
}