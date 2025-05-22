# Libraries
USER <- Sys.info()[7]

library(data.table)
library(stringr)
library(RMySQL)
library(tidyr)
library(boot)
source('~/db_utilities.R')
lapply(dbListConnections( dbDriver( drv = "MySQL")), dbDisconnect)

####### Functions #######
loadBundles <- function(bundle_ids) {
  db_con = fread(paste0("FILEPATH"))

  # Get list of ids and names
  con <- dbConnect(dbDriver("MySQL"),
                   username = db_con$username,
                   password = db_con$pass,
                   host = db_con$host)
  df <- dbGetQuery(con, sprintf("QUERY",
                                paste0(bundle_ids, collapse = ",")))
  df <- data.table(df)
  df <- df[is.na(sort_order), sort_order := 9999]
  df <- df[order(sort_order)][, sort_order := NULL]
  dbDisconnect(con)
  return(df)
}

