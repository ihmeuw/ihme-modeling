library(tools)
library(data.table)
library(readstata13)

SetGlobalDirectories <- function() {

  # pop and births
  pop.dir <<- "FILEPATH"
  pop.file <<- "d09_denominators.dta"
  births.dir <<- "FILEPATH"
  births.file <<- "births_gbd2016.dta"
  db.source <<- "FILEPATH/source_table.csv"
  db.dataloc <<- "FILEPATH/data_location_table.csv"
  db.appendorder <<- "FILEPATH/combine_order_table.csv"
  }


DbLoad <- function(source.name, source.type, db.source, db.dataloc,
                   as.table=FALSE) {
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
    source.path <- db.dataloc[source.type == source_type, win_path]
  } else {
    source.path <- db.dataloc[source.type == source_type, lin_path]
  }
  
  source.file <- paste(source.path, "/", source.name, sep="")
  source.data <- EzRead(source.file, as.table)
  source.data[["filename"]] <- source.name
  source.data[["source.type"]] <- source.type
  return(source.data)
}


EzRead <- function(file, as.table=FALSE) {
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


FillMissingCols <- function(col.names, data) {
  # Adds any columns missing in data from list of col.names as NA
  col.names <- setdiff(col.names, as.vector(names(data)))
  for (col in col.names) {
    data[[col]] <- NA
  }
  return(data)
}


AppendData <- function(data, add.data) {
  setDT(data)
  setDT(add.data)
  # Append data
  cols.req <- c("ihme.loc.id", "iso3", "NID", "underlying_NID", "filename", "compiling.entity",
                "in.direct", "source.type", "sd.q5", "log10.sd.q5")
  # Check if need to add cbh variables
  if (add.data[1, (in.direct != "direct" |
                  (in.direct == "direct" & data.age != "new")
                  | is.na(in.direct))]) {
  # add in variables that are specific to CBH data variance
    add.data[["sd.q5"]] <- add.data[["log10.sd.q5"]] <- NA
  }
  add.data <- FillMissingCols(cols.req, add.data)
  # only keep variables in the final dataset
  names <- c("iso3", "ihme.loc.id", "t", "q5", "source", "source.date",
             "in.direct", "compiling.entity", "data.age", "sd.q5",
             "log10.sd.q5", "NID", "underlying_NID", "filename", "source.type")

  if(typeof(add.data$source.date) %in% c("double", "float")) add.data[round(source.date, digits = 3) == round(source.date, digits = 0), source.date := as.character(as.integer(source.date))]
  add.data <- add.data[, names, with=FALSE]
  data <- data[, names, with=FALSE]
  data <- rbind(data, add.data)
  setDF(data)
  return(data)
}
