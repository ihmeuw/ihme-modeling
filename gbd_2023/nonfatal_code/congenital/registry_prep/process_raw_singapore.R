
rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library('openxlsx')
library('data.table')


singapore <- as.data.table(read.xlsx(paste0("FILEPATH")))
singapore[, mean := cases/sample_size]

write.xlsx(singapore, paste0("FILEPATH"),
           row.names = FALSE, sheetName = "extraction")




