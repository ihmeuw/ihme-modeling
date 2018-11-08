#' Save and load HDF files from a data.table
#'
#' Take a data.table and save or load it into a HDF file grouped into different data types. If by_var and by_val are not specified, save_hdf saves all the data in a group called "data" or a name controlled by the groupname option. If by_val is specified, each chunk of the data will be saved in a group with the value of by_val that was used to chunk the data (for example, by year). load_hdf only uses the filepath and by_val arguments, to attempt to load data from a given H5 file with a group with the name specified in by_val.
#' 
#' @param data data.table containing containing by_var (if specified)
#' @param filepath character, path to file to be written
#' @param by_var character, name of variable in data which contains valuse of which to save variables
#' @param by_val character or numeric, value of which to subset data by, using by_var. Only usable if by_var is also specified.
#' @param level integer (0-9), level of compression to use when saving the HDF file.
#' @param groupname character, only usable if by_var and by_val are NOT specified. Overrides the default group name of "data" to save the data into the HDF file.
#' @return data.table with id_vars and value_vars, along with new observations for location, age, and sex aggregates
#' @import data.table
#' @seealso \url{http://bioconductor.org/packages/release/bioc/html/rhdf5.html}
#' @export
#' save_hdf load_hdf
#'
#' @examples 
#' \dontrun{
#' years <- c(1970:2016)
#' data <- data.table(year_id = years, mean = 1)
#' filepath <- paste0(data_dir,"/results.h5")
#' 
#' file.remove(filepath)
#' rhdf5::h5createFile(filepath)
#' lapply(years, save_hdf, data=data, filepath=filepath, by_var="year_id", level=2)
#' rhdf5::H5close()
#' 
#' rbindlist(lapply(years, load_hdf, filepath = filepath))
#' }
#' 
#' @name hdf_utils
NULL

#' @rdname hdf_utils
save_hdf <- function(data,filepath=NA,by_var=NA,by_val=NA,level=0, groupname = NA) {
    if(is.na(filepath) | !grepl(".h5",filepath)) stop("Must specify filepath ending in .h5")
    if(!data.table::is.data.table(data)) stop("Data must be in a data.table")
    if(!by_var %in% data.table::key(data)) warning("Your specified by_val is not a key on your data -- consider implementing this to save time during subsetting")

    if(!is.na(by_var) & !is.na(by_val)) {
        data <- data[data[,get(by_var)] == by_val,]
        groupname <- paste0(by_val)
    } else if((is.na(by_var) & !is.na(by_val)) | !is.na(by_var) & is.na(by_val)) {
        stop("Cannot specify either by_val or by_var without specifying the other")
    } else if(is.na(groupname)){
        # If no by_var or by_val specified, save the dataset in one called "data"
        groupname <- "data"
    }

    tryCatch({
        rhdf5::h5write(data,file=filepath, name=groupname, level=level) 
    }, error = function(e) {
        print("Re-trying file with 10 second rest")
        Sys.sleep(10)
        rhdf5::h5write(data,file=filepath, name=groupname, level=level) 
    })
}

#' @rdname hdf_utils
load_hdf <- function(filepath,by_val="data") {
  library(rhdf5)
  if(file.exists(filepath)){
    tryCatch({
      rhdf5::h5read(paste0(filepath), name=paste0(by_val))
    }, error = function(e) {
        print(paste0("Sleeping then re-attempting import of ",filepath," ",by_val))
        Sys.sleep(10)
        rhdf5::h5read(paste0(filepath), name=paste0(by_val))
    })
  }
}
