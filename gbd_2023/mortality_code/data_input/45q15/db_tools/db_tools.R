# library(ini) gets an error with singluarity image, is loaded in by mortdb anyway
library(RMySQL)

get_engine <- function(conn_def) {
  # Get the odbc.ini file from the J drive first, if not, then home directory
  j_drive_ini <- paste0("/home/j/WORK/02_mortality/shared/.odbc.ini")
  if (file.exists(j_drive_ini)) {
    odbc <- ini::read.ini(j_drive_ini)
  } else {
    odbc <- ini::read.ini("~/.odbc.ini")
  }

  # Make sure the connection is in the odbc file
  assertthat::assert_that(conn_def %in% names(odbc))

  # Convert relevent keys into lowercase
  for (x in c('SERVER', 'USER', 'PASSWORD')) {
    if (x %in% names(odbc[[conn_def]])) {
      odbc[[conn_def]][tolower(x)] <- odbc[[conn_def]][x]
    }
  }

  # Create the connection
  mysqlengine <- RMySQL::dbConnect(RMySQL::MySQL(),
                                   host = odbc[[conn_def]]$server,
                                   username = odbc[[conn_def]]$user,
                                   password = odbc[[conn_def]]$password)

  #
  return(mysqlengine)
}

query <- function(query, mysqlengine = NULL, conn_def = NULL) {
  #Query using a connection definition name or a `MySQLEngine` object
  #
  #  Args:
  #      query (str): SQL query string to execute
  #      engine (:obj:`db_tools.mysqlapis.MySQLEngine`, optional): MySQLEngine
  #          object to use to execute query
  #      conn_def (:obj:`str`, optional): connection definition to use from
  #          `config.conn_defs`. Connection definitions are populated from a set
  #          of known base connections to ihme databases as well as from
  #          importing any known odbc.ini files.
  #      cached (:obj:`bool`, optional): whether to cache the query results on
  #          the mysql_engine object used to execute the specified query.
  #
  #  Returns:
  #      pandas.DataFrame: pandas DataFrame of query results.

  assertthat::assert_that(!is.null(mysqlengine) || !is.null(conn_def))

  if (is.null(mysqlengine)) {
    mysqlengine <- get_engine(conn_def)
  }

  # Make call
  output <- data.table::data.table(DBI::dbGetQuery(mysqlengine, query))

  # Disconnect
  RMySQL::dbDisconnect(mysqlengine)

  return(output)
}
