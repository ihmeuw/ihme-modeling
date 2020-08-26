
SetGlobalDirectories <- function() {
  source(paste0(h, "FILEPATH/get_paths.R"))

  db.dataloc <<- fread(paste0(get_path("5q0_process_inputs"),
                              "/compile/data_location_table.csv"))

  source_table_query <- paste0("SQL")
  myconn <- db_init()
  db.source_table <<- setDT(DBI::dbGetQuery(myconn, source_table_query))
  DBI::dbDisconnect(myconn)
}


DbLoad <- function(source.name, input.source.type, db.source, db.dataloc,
                   as.table=TRUE) {
  # Load a file by filename from the source and data_location tables.
  # Args:
  #   source.name : the filename of the file you want to Load (corresponds to
  #                 'filename' field in db.source)
  #   db.source : source_table.csv. Contains sources by filename and type
  #   db.dataloc : data_location_table.csv. Contains data locations by type
  # Returns:
  #  the file as a data.frame

  # Error handling
  if (db.source[filename == source.name, .N] == 0) {
    stop(paste(source.name), " is not included in the source database.")
  }

  if (Sys.info()[1] == "Windows") {
    source.path <- db.dataloc[source_type == input.source.type, win_path]
  } else {
    source.path <- db.dataloc[source_type == input.source.type, lin_path]
  }

  source.file <- paste(source.path, "/", source.name, sep="")
  source.data <- EzRead(source.file, as.table)
  source.data[, filename := source.name]
  source.data[, source.type := input.source.type]

  # Normalize and underlying_NID NID column capitalization
  nid_col <- names(source.data)[tolower(names(source.data)) == "nid"]
  underlying_nid_col <- names(source.data)[tolower(names(source.data)) == "underlying_nid"]

  if(length(nid_col) == 0) {
    source.data[, nid := NA]
    nid_col <- "nid"
  }

  if(length(underlying_nid_col) == 0) {
    source.data[, underlying_nid := NA]
    underlying_nid_col <- "underlying_nid"
  }

  setnames(source.data, c(nid_col, underlying_nid_col), c("NID", "underlying_NID"))

  # Convert factors to character values
  if (input.source.type %in% c("AGG", "SSC")){
    factor_variables <- sapply(source.data, is.factor)
    lapply(names(factor_variables[factor_variables == TRUE]), function(x) source.data[, (x) := as.character(get(x))])
  }

  return(source.data)
}


EzRead <- function(file, as.table=TRUE) {
  # Read wrapper so you don't have to call separate for .csv or .dta data
  # Uses data.frame, optionally can be turned on for data.table
  require(tools)
  require(data.table)

  file.ext <- file_ext(file)
  file.data <- NA

  if (file.ext == "dta") {
    file.data <- read.dta13(file, convert.underscore=TRUE)
    if (as.table == TRUE) {
      setDT(file.data)
    }
  } else if (file.ext == "csv") {
    if (as.table == TRUE) {
      file.data <- fread(file)
    } else
      file.data <- read.csv(file)
  } else
    stop(paste(file.ext, " is not a valid file type. Please use csv or dta."))
  return(file.data)
}


FillMissingCols <- function(col.names, dt) {
  # Adds any columns missing in data from list of col.names as NA
  col.names <- setdiff(col.names, as.vector(names(dt)))
  for (col in col.names) {
    dt[, (col) := NA]
  }
  return(dt)
}


AppendData <- function(data, add.data) {
  # Append data
  cols.req <- c("ihme.loc.id", "iso3", "NID", "underlying_NID", "filename", "compiling.entity",
                "in.direct", "source.type", "sd.q5", "log10.sd.q5")

  add.data <- FillMissingCols(cols.req, add.data)

  # only keep variables we want in the final dataset
  final_cols <- c("iso3", "ihme.loc.id", "t", "q5", "source", "source.date",
                  "in.direct", "compiling.entity", "data.age", "sd.q5",
                  "log10.sd.q5", "NID", "underlying_NID", "filename", "source.type")

  if(typeof(add.data$source.date) %in% c("double", "float")) {
    add.data[round(source.date, digits = 3) == round(source.date, digits = 0), source.date := as.double(as.integer(source.date))]
  }

  data <- rbindlist(list(data[, .SD, .SDcols = final_cols],
                         add.data[, .SD, .SDcols = final_cols]),
                    use.names = T)
  return(data)
}
