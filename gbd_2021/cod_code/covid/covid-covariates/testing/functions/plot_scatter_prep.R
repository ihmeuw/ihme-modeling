#' .plot_scatter_prep
#' 
#' Prepare testing data for scatterplots.  Plots test_pc, current run vs. prior
#' run on 6 arbitrary days in time.  Plots current vs. previous scatters in
#' daily and cumulative space. Checks missingness in test_pc variable, and sets
#' cumulative_test_pc == 0 if it finds any (to prompt further investigation).
#'
#' @param previous_best_version [character] YYYY_MM_DD.VV version for last production testing data set
#' Require: columns: location_id, date, test_pc
#' @param output_dt_new [data.table] data table of current, new testing outputs
#' Require: columns: location_id, date, test_pc
#' @param hierarchy [data.table] Covid covariates hierarchy (location set id = 115)
#' Require columns: location_id, location_name
#'
#' @return [data.table] data table of covid hierarchy locations, old and new, for a subset of comparison dates
.plot_scatter_prep <- function (previous_best_version,
                                output_dt_new,
                                hierarchy) {
  
  require(data.table)
  
  # load reference data
  output_dt_ref <- fread(
    file.path(
      OUTPUT_ROOT, 
      previous_best_version, 
      "forecast_raked_test_pc_simple.csv")
  )
  
  # New and reference 
  dt_list <- list("New" = output_dt_new,
                  "Ref" = output_dt_ref)
  
  # function to operate on one data.table
  calc_cumulative <- function(dataset){
    dataset <- dataset[location_id %in% unique(hierarchy$location_id)]
    dataset[, date := as.Date(date)]
    
    # add cumulative column, checking for missingness
    dataset <- 
      rbindlist(
        
        lapply(
          split(dataset, by = "location_id"), function (x) {
            
            tryCatch(
              {
                if(any(is.na(x$test_pc))) {warning("Missing test_pc value")}
                x[, cumulative_test_pc := cumsum(test_pc)]
              }, 
              warning = function(w) {
                message("WARNING : plot_scatter_prep : ", unique(x$location_id), " : ", unique(x$location_name), " : ", conditionMessage(w))
              },
              error = function (e) {
                message(unique(x$location_id))
              }
            )
          }
        )
      )
    
    return(dataset)
    
  }
  
  # Apply to both new and ref datasets
  prepped_dt_list <- lapply(dt_list, calc_cumulative)
  
  
  plot_dt_all <- merge(prepped_dt_list$Ref, 
                       prepped_dt_list$New, 
                       by = c("location_id","date"), 
                       all.x = T, all.y = T,
                       suffixes = c("_ref", "_new"))

  if(any(c(is.na(plot_dt_all$cumulative_test_pc_new), is.na(plot_dt_all$cumulative_test_pc_ref)))) 
    {warning("If locations are missing in `test_pc` data, setting test_pc & cumulative = 0")}
  
  plot_dt_all[which(is.na(plot_dt_all$cumulative_test_pc_new)), cumulative_test_pc_new := 0]
  plot_dt_all[which(is.na(plot_dt_all$cumulative_test_pc_ref)), cumulative_test_pc_ref := 0]
  plot_dt_all[which(is.na(plot_dt_all$test_pc_new)), test_pc_new := 0]
  plot_dt_all[which(is.na(plot_dt_all$test_pc_ref)), test_pc_ref := 0]
  # Fill names
  plot_dt_all <- merge(hierarchy[,.(location_id, location_name)], plot_dt_all, all.x = T, by = "location_id")
  
  # keep only columns and dates needed for plotting
  plot_dt_all <- plot_dt_all[ , .(location_id, location_name, date, 
                                  test_pc_new, test_pc_ref, 
                                  cumulative_test_pc_new, cumulative_test_pc_ref)]
    plot_dt_all <- subset(plot_dt_all, 
                        date %in% c(Sys.Date(), 
                                    as.Date("2020-08-01"), 
                                    as.Date("2020-10-01"),
                                    as.Date("2021-01-01"),
                                    as.Date("2021-10-01"), 
                                    as.Date("2022-03-01")
                        )
  )
  
  return(plot_dt_all)

}


