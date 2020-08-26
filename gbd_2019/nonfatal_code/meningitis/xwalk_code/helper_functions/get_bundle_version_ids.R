#' @author
#' @date 2019/11/05
#' @description This script creates a SQL query to the epi databased to retrieve your
#' bundle version ids and associated crosswalk version ids. 
#' 
#' @example
#' get_bundle_version_ids(bundle_id = c(614, 13))
#' get_bundle_version_ids(cause_id = 368)
#' get_bundle_version_ids(bundle_id = 37, crosswalk_id = 13694)
library(RMySQL)
library(dplyr)
library(data.table)
query <- function(query, conn_def) {
  odbc <- ini::read.ini("filepath/.odbc.ini")
  conn <- dbConnect(RMySQL::MySQL(),
                    host = odbc[[conn_def]]$server,
                    username = odbc[[conn_def]]$user,
                    password = odbc[[conn_def]]$password)
  dt <- dbGetQuery(conn, query) %>% data.table
  dbDisconnect(conn)
  return(dt)
}
get_bundle_version_ids <- function(cause_id = NA, bundle_id = NA, crosswalk_id = NA) {
  host <- "epi"
  
  if (any(is.na(cause_id)) & any(is.na(bundle_id) & any(is.na(crosswalk_id)))) {
    stop("Please supply either a cause_id, a bundle_id, or both.")
  }
  
  if (!any(is.na(cause_id))) {
    cause_filter <- paste0("b.cause_id in (", paste(cause_id, collapse = ", "), ")")
  } else {
    cause_filter <- ""
  }
  
  if (!any(is.na(bundle_id))) {
    bundle_filter <- paste0("bv.bundle_id in (", paste(bundle_id, collapse = ", "), ")")
  } else {
    bundle_filter <- ""
  }
  
  if (!any(is.na(cause_id)) & !any(is.na(bundle_id))) {
    and <- " AND "
  } else {
    and <- ""
  }
  
  if (!any(is.na(crosswalk_id))) {
    crosswalk_filter <- paste0("c.crosswalk_version_id in (", paste(crosswalk_id, collapse = ", "), ")")
  } else {
    crosswalk_filter <- ""
  }
  
  if (!any(is.na(bundle_id)) & !any(is.na(crosswalk_id))) {
    and2 <- " AND "
  } else {
    and2 <- ""
  }
  
  q <- paste(
    "SELECT b.cause_id, bv.bundle_id, b.bundle_name, bv.bundle_version_id,", 
    "bv.clinical_refresh_id, bv.date_inserted, c.crosswalk_version_id",
    "FROM bundle_version.bundle_version bv",
    "LEFT JOIN bundle.bundle b", 
    "ON bv.bundle_id = b.bundle_id",
    "LEFT JOIN crosswalk_version.crosswalk_version c", 
    "ON c.bundle_version_id = bv.bundle_version_id",
    "WHERE", cause_filter, and, bundle_filter, and2, crosswalk_filter
  )
  print(q)
  return(query(q, host))
}
