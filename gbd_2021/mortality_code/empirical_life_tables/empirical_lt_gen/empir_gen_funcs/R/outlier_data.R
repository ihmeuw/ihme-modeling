#' Outliers from 5q0 and 45q15 modeling, return data.table of outliers to be used by cascade_select_lts
#'
#' @param run_id_5q0_estimate integer, run ID of 5q0 estimates to pull underlying data and outliers from
#' @param run_id_45q15_estimate integer, run ID of 5q0 estimates to pull underlying data and pull outliers from
#' @param gbd_year integer, gbd year (ex: 2017, 2019, 2020, etc.)
#'
#' @return data.table with ihme_loc_id, year, sex, source_type, child_adult=1, based on 5q0 and 45q15 outliers
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

outlier_data <- function(run_id_5q0_estimate, run_id_45q15_estimate, gbd_year) {
    
        id_vars <- c("ihme_loc_id", "year_id", "sex_id", "source_name")

    ## 5q0 outliers ==========================================================================
        
        run_id_5q0_data <- get_proc_lineage("5q0", "estimate",
                                            run_id = run_id_5q0_estimate,
                                            gbd_year = gbd_year)
        run_id_5q0_data <- unique(run_id_5q0_data[parent_process_id == 5 & exclude == 0, parent_run_id])
        
        outliers_5q0 <- suppressWarnings(get_mort_outputs("5q0", "data",
                                                          run_id = run_id_5q0_data,
                                                          outlier_run_id = run_id_5q0_estimate,
                                                          gbd_year = gbd_year))
        outliers_5q0 <- unique(outliers_5q0[outlier == 1, .SD, .SDcols = id_vars])
        outliers_5q0[, sex_id := 1]
        outliers_5q0_female <- copy(outliers_5q0)
        outliers_5q0_female[, sex_id := 2]
        outliers_5q0 <- rbindlist(list(outliers_5q0, outliers_5q0_female), use.names = T)
        outliers_5q0[, outlier_type_id := 18]
        
    ## 45q15 outliers ========================================================================

        run_id_45q15_data <- get_proc_lineage("45q15", "estimate",
                                              run_id = run_id_45q15_estimate,
                                              gbd_year = gbd_year)
        run_id_45q15_data <- unique(run_id_45q15_data[parent_process_id == 2 & exclude == 0, parent_run_id])

        outliers_45q15 <- get_mort_outputs("45q15", "data",
                                           run_id = run_id_45q15_data,
                                           outlier_run_id = run_id_45q15_estimate,
                                           gbd_year = gbd_year)
        outliers_45q15 <- unique(outliers_45q15[outlier == 1, .SD, .SDcols = id_vars])
        outliers_45q15[, outlier_type_id := 18]

    ## Combine  ==========================================================================
        
        outliers <- rbindlist(list(outliers_5q0, outliers_45q15), use.names = T)
        
        # Drop any duplicates (if outliered both both 5q0 and 45q15)
        outliers <- unique(outliers, by = id_vars)
        
        # Modify variables
        outliers[sex_id == 1, sex := "male"]
        outliers[sex_id == 2, sex := "female"]
        outliers[, sex_id := NULL]
        setnames(outliers, c("year_id", "source_name"), c("year", "source_type"))
        outliers[, child_adult := 1]

        # Return list of 45q15 and 5q0 outliers
        return(outliers)
}
