#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Function for reading in excel files
# Inputs:  dataset
#***********************************************************************************************************************


#----FUNCTION-----------------------------------------------------------------------------------------------------------
source("/share/code/coverage/functions/load_packages.R")
load_packages("readxl")

read_excel <- function(...) {
  
  quiet_read <- purrr::quietly(readxl::read_excel)
  out <- quiet_read(...)
  
  if(length(c(out[["warnings"]], out[["messages"]]))==0)
    return(out[["result"]])
  
  else readxl::read_excel(...)
  
}
#***********************************************************************************************************************