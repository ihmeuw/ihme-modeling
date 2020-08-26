# Ira's SQL query pulls bundle labels/betas and the stata script saves it as an xlsx
# I wrote a quick workaround when I should have just changed stata to save as a csv
# but since i haven't done that yet, I'm saving this script


rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "/home/j/"
  h <- "/homes/chikeda/"
}
library("openxlsx")
#################

datadir <- paste0(h, "GBD_2019/congenital_work/190308_Xwalks/study_labels/")

datanames <- dir(datadir, pattern = ".xlsx")
datanames <- gsub(".xlsx", "", datanames)

for (me in datanames){
  print(paste0(datadir, me, ".xlsx"))
  data <- read.xlsx(paste0(datadir, me, ".xlsx"))
  
  write.csv(data, paste0(datadir, "csv/", me, ".csv"), row.names = FALSE)
}
