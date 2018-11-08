####################
## source("FILEPATH/utility/.R")
########################


try_stata<-function(stata_file, ...){
  require(haven)
  require(readstata13)
  tryCatch(
    
    ## try reading in w/ haven
    {
      output<-as.data.table(haven::read_dta(stata_file, ...))
      return(output)
    },
    
    ## try reading w/ readstat13
    error = function(e) {
      message("haven::read_data() failed with following error:")
      message(" '", e, "'")
      message("Trying readstata13::read.dta13()..")
      output<-as.data.table(readstata13::read.dta13(stata_file, ...))
      message("File read successfully")
      return(output)
    })
}