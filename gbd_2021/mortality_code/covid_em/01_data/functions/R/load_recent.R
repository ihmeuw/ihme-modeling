
#' @title Download recent data
#'
#' @description Assumes file structure of,
#'   identifies most recent date, and returns aggregated file.
#'
#' @param ihme_loc_list \[`character()`\]\cr
#'   ihme_loc_id values
#' @param dir_list \[`character()`\]\cr
#'   Base directories to search in
#' @param source_prioritization \[`data.table()`\]\cr
#'   Table with columns "ihme_loc_id" and "source", where the rows represent
#'   the location-source pairings to be included for locations with more
#'   than one source. Default empty data.table and all location-sources
#'   identified by `ihme_loc_list` and `dir_list` will be included.

load_recent <- function(ihme_loc_list,
                        dir_list,
                        source_prioritization = data.table(ihme_loc_id = c(),
                                                           source = c())) {

  # loop over location
  dt <- rbindlist(lapply(ihme_loc_list, function(l) {

    # loop over directory
    dt_l <- rbindlist(lapply(dir_list, function(dir) {

      sources <- list.dirs(paste0(), full.names = F, recursive = F)

      # source prioritization
      if (l %in% source_prioritization$ihme_loc_id) {
        keep_source <- source_prioritization[ihme_loc_id == l, source]
        sources <- sources[sources %in% keep_source]
      }

      # loop over sources
      dt_dir <- rbindlist(lapply(sources, function(s) {

        # find most recent dated folder for this source
        dates <- list.dirs(
          paste0(), full.names = F, recursive = F
        )
        date <- sort(dates, decreasing = T)[1]

        # import all csv files in this detailed directory
        dt_s <- invisible(assertable::import_files(
          filenames = list.files(
            paste0(),
            full.names = T, pattern = ".csv"
          )
        ))
        dt_s[, date_prepped := date]

        # make sure column class types are uniform for rbind
        if ("date_reported" %in% names(dt_s)) {
          dt_s[, date_reported := as.character(date_reported)]
        }

        # drop empty rows
        dt_s <- dt_s[!is.na(date_prepped)]

        return(dt_s)
      }), fill = T, use.names = T)
      return(dt_dir)
    }), fill = T, use.names = T)

    if('DOH' %in% unique(dt_l$source)){
      if(l == "USA_570"){
        dt_l <- dt_l[!(source=='NVSS' & !year_start %in% c(2016:2018))]
      }else{
        dt_l <- dt_l[!(year_start==2018 & source == 'NVSS')]
      }
    }

    return(dt_l)
  }), fill = T, use.names = T)
  return(dt)
}
