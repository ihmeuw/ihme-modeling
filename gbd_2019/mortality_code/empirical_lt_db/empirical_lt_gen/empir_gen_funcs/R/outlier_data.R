#' Outlier selected datapoints
#'
#' Outlier datapoints if they are marked as outliers in 5q0 or 45q15, or if they are manually selected as an outlier.
#'
#' @param dt data.table with variables: ihme_loc_id, year, source_type, sex, age, age_length, dx, ax, qx
#' @param outlier_sheet data.table with variables: ihme_loc_id, sex, source_type, specific_year, year_start, year_end, apply_outliering.
#'                      Each row must have a value for ihme_loc_id, sex, and source_type. Each row must either have a value for specific year OR (year start and/or end)
#' @param completeness_outliers data.table with variables: ihme_loc_id, sex, source_type, year to which to apply outliering based on adult completeness cutoffs
#' @param run_id_5q0_estimate integer, run ID of 5q0 estimates to pull underlying data and outliers from
#' @param run_id_45q15_estimate integer, run ID of 5q0 estimates to pull underlying data and pull outliers from
#' @param drop_outliers logical, whether to drop the outliers or simply generate a variable named outlier that will be 1 or NA. Default: T
#'
#' @return data.table with all outliered country/year/sex combinations dropped
#' @export
#'
#' @examples
#' @import data.table
#' @import assertable

outlier_data <- function(dt, outlier_sheet, completeness_outliers, run_id_5q0_estimate, run_id_45q15_estimate, drop_outliers = T) {
    
    ## preserve any input from smooth_id != 1
    if("smooth_width" %in% names(dt)){
      not_smooth_1 <- dt[smooth_width!=1]
      dt <- dt[smooth_width==1]
    }
    
    ## Run assertions on outlier sheet
    assert_colnames(outlier_sheet, c("ihme_loc_id", "sex", "source_type", "specific_year", "year_start", "year_end"), only_colnames = F)
    
    if(nrow(outlier_sheet[!is.na(specific_year) & (!is.na(year_start) | !is.na(year_end))]) > 0) {
        stop("Cannot have rows in outlier_sheet that have both a specific_year value and either year_start or year_end")
    }

    if(nrow(outlier_sheet[is.na(sex) | (!sex %in% c("male", "female"))]) > 0) stop("Invalid values for sex -- must be male or female")
    assert_values(outlier_sheet, c("ihme_loc_id", "sex", "source_type"), "not_na", quiet = T)

    dt <- copy(dt)
    unique_data_ids <- c("ihme_loc_id", "year_id", "sex_id", "source_name")
    outlier_sheet_ids <- c(unique_data_ids[! unique_data_ids %in% c("year_id", "sex_id", "source_name")], "sex", "source_type")
    final_unique_ids <- c("ihme_loc_id", "year", "sex", "source_type")

    ## Format outliers from the completeness process
    completeness_outliers[, outlier_type_id := 19]

    ## Import and format outliers from the 5q0 and 45q15 modeling processes
    run_id_5q0_data <- get_proc_lineage("5q0", "estimate", run_id = run_id_5q0_estimate)
    run_id_5q0_data <- unique(run_id_5q0_data[parent_process_id == 5 & exclude == 0, parent_run_id])
    run_id_45q15_data <- get_proc_lineage("45q15", "estimate", run_id = run_id_45q15_estimate)
    run_id_45q15_data <- unique(run_id_45q15_data[parent_process_id == 2 & exclude == 0, parent_run_id])

    outliers_5q0 <- suppressWarnings(get_mort_outputs("5q0", "data", run_id = run_id_5q0_data, outlier_run_id = run_id_5q0_estimate))
    outliers_45q15 <- get_mort_outputs("45q15", "data", run_id = run_id_45q15_data, outlier_run_id = run_id_45q15_estimate)

    outliers_5q0 <- unique(outliers_5q0[outlier == 1, .SD, .SDcols = unique_data_ids])
    outliers_5q0[, sex_id := 1]
    outliers_5q0_female <- copy(outliers_5q0)
    outliers_5q0_female[, sex_id := 2]
    outliers_5q0 <- rbindlist(list(outliers_5q0, outliers_5q0_female), use.names = T)
    outliers_5q0[, outlier_type_id := 18]

    outliers_45q15 <- unique(outliers_45q15[outlier == 1, .SD, .SDcols = unique_data_ids])
    outliers_45q15[, outlier_type_id := 18]

    model_outliers <- rbindlist(list(outliers_5q0, outliers_45q15), use.names = T)
    model_outliers[sex_id == 1, sex := "male"]
    model_outliers[sex_id == 2, sex := "female"]
    model_outliers[, sex_id := NULL]

    setnames(model_outliers, c("year_id", "source_name"), c("year", "source_type"))

    final_outliers <- rbindlist(list(model_outliers, completeness_outliers), use.names = T)

    outlier_sheet <- outlier_sheet[apply_outliering == 1]

    if(nrow(outlier_sheet) > 0) {
        ## If one of the range variables exists but the other is NA, assume that the outliering is intended to extend for the full time-series
        outlier_sheet[!is.na(year_end) & is.na(year_start), year_start := 1900]
        outlier_sheet[!is.na(year_start) & is.na(year_end), year_end := 2030]

        if(nrow(outlier_sheet[year_start > year_end & !is.na(year_start) & !is.na(year_end)]) > 0) {
            stop("Cannot have year_start be larger than year_end")
        }

        ## Create distinct rows for places where you have the range of years
        continuous_outliers <- outlier_sheet[!is.na(year_start) & !is.na(year_end)]
        if(nrow(continuous_outliers) > 0) {
            continuous_outliers <- continuous_outliers[, list(max_year_end = max(year_end)), by = c(outlier_sheet_ids, "year_start")]
            continuous_outliers <- continuous_outliers[, list(min_year_start = min(year_start)), by = c(outlier_sheet_ids, "max_year_end")]
            continuous_outliers <- continuous_outliers[, .(year = min_year_start:max_year_end), by = c(outlier_sheet_ids, "min_year_start", "max_year_end")]
            continuous_outliers <- unique(continuous_outliers[, .SD, .SDcols = c(outlier_sheet_ids, "year")])
            continuous_outliers[, outlier_type_id := 13]

            final_outliers <- rbindlist(list(final_outliers, continuous_outliers), use.names = T)
        }

        ## Extract data where there are precise years specified to drop
        outlier_sheet <- outlier_sheet[!is.na(specific_year), .SD, .SDcols = c(outlier_sheet_ids, "specific_year")]
        setnames(outlier_sheet, "specific_year", "year")
        outlier_sheet[, outlier_type_id := 13]

        final_outliers <- rbindlist(list(final_outliers, outlier_sheet), use.names = T)
    }

    ## Drop any overlaps between different outliering sources
    final_outliers <- unique(final_outliers, by=final_unique_ids)

    dt <- merge(dt, final_outliers, by = final_unique_ids, all.x = T)

    nrow_drops <- nrow(unique(dt[!is.na(outlier_type_id), .SD, .SDcols = final_unique_ids]))
    message("Dropping ", nrow_drops, " lifetables from empirical death data")

    outliered_data <- dt[!is.na(outlier_type_id)]
    outliered_data[, smooth_width:=1]

    kept_data <- dt[is.na(outlier_type_id)]
    kept_data[, outlier_type_id := NULL]
    kept_data[, smooth_width:=1]
    
    ## preserve any input from smooth_id != 1
    ## use a different process to select LTs w/ smoothness >1
    if("smooth_width" %in% names(dt)){
      kept_data <- rbind(kept_data, not_smooth_1)
    }

    return(list(kept_data = kept_data, outliered_data = outliered_data))
}
