# ---HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Create proportions to split ISIC based on concordance tables
# Map the flows between major groups across ISIC versions in order to crossawlk between versions
#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# load packages
library(Cairo, lib.loc = "FILEPATH")
pacman::p_load(data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr)
library(xlsx, lib.loc = "FILEPATH")

# set working directories
home.dir <- "FILEPATH"
  setwd(home.dir)

#set values for project
  options(bitmapType="cairo")

##in##
data.dir <- file.path(home.dir, "FILEPATH")
doc.dir <- file.path(home.dir, "FILEPATH")
  isic.map <- file.path(doc.dir, "FILEPATH")

##out##
out.dir <- file.path(home.dir, "FILEPATH")
graph.dir <- file.path(home.dir, "FILEPATH")
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- "FILEPATH"
ubcov.function.dir <- "FILEPATH"
# this pulls the general misc helper functions
file.path(central.function.dir, "FUNCTION") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "FUNCTION") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FUNCTION") %>% source
# central functions
file.path('FUNCTION') %>% source
#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#riverplot
makeRivPlot <- function(data1, data2, var1, var2, var3) {

  require(dplyr)          # Needed for the count function
  require(riverplot)      # Does all the real work
  require(RColorBrewer)   # To assign nice colours

  #browser()

  #pull out all your node IDs
  names <- mapply(function(dt, var) unique(dt[, var, with=F]),
                  dt=list(data1, data1, data2),
                  var=c(var1, var2, var3))

  labels <- mapply(function(dt, var) unique(dt[, str_replace(var,
                                                             "major_",
                                                             "major_label_short_isic_"), with=F]),
                   dt=list(data1, data1, data2),
                   var=c(var1, var2, var3))

  labels <- mapply(function(list1, list2) paste0(list1, ":\n", list2), names, labels)

  labels <- lapply(labels, function(var) str_replace(var, pattern=";", replacement=";\n"))
  labels <- lapply(labels, function(var) str_replace(var, pattern="and", replacement="\nand"))

  #create the edge object by subsetting your dts to the relevant variable combos and then collapsing with count
  dt1 <- data1[, c(var1, var2), with=F]
  dt2 <- data2[, c(var2, var3), with=F]
  edges  <- rbind(dt1, dt2, use.names=F) %>% count
    colnames(edges) <- c("N1", "N2", "Value")

  #function to make a color palette with lots of colors from ColorBrewer
  manyColors <- function(var1) {
    n <- length(var1)
    qual_col_pals = brewer.pal.info[brewer.pal.info$category == 'qual',]
    col_vector = unlist(mapply(brewer.pal, qual_col_pals$maxcolors, rownames(qual_col_pals)))
    sample(col_vector, n) %>% return
  }

  #create node object
  nodes <- data.frame(ID = names %>% unlist,
                      x = c(rep(1, times = length(names[[1]])),
                            rep(2, times = length(names[[2]])),
                            rep(3, times = length(names[[3]]))), #x coord defined as 1/2/3 based on ISIC version
                      col = paste0(lapply(names, manyColors) %>% unlist, 75),
                      labels = labels %>% unlist, #labels and IDs are equal in this usecase
                      stringsAsFactors=FALSE)

  #make riverplot object
  river <- makeRiver(nodes, edges) %>% return #return river object for manipulation
}
#***********************************************************************************************************************

#***********************************************************************************************************************

# ---ISICv2 to ISICv3---------------------------------------------------------------------------------------------------
#read in the ISICv2->3 concordance table and analyze the flows between major groups
dt <- file.path(data.dir, 'FILEPATH') %>% fread
setnames(dt, c('ISIC2', 'partialISIC2', 'ISIC3', 'partialISIC3'), c('code_isic_2', 'partial_2', 'code_isic_3', 'partial_3'))

#read in the map to major groups (sheet3=rev3, sheet4=rev2)
isic_3_map <- read.xlsx(isic.map, sheetIndex = 3) %>% as.data.table
  setnames(isic_3_map, names(isic_3_map), paste0(names(isic_3_map), "_isic_3"))
isic_2_map <- read.xlsx(isic.map, sheetIndex = 4) %>% as.data.table
  setnames(isic_2_map, names(isic_2_map), paste0(names(isic_2_map), "_isic_2"))

#extract minor groups as the first 2 digits to merge on major groups
dt[, minor_isic_3 := substr(code_isic_3, start = 1, stop = 2)]
dt[, minor_isic_2 := substr(code_isic_2, start = 1, stop = 2) %>% as.numeric]
all_2_3 <- merge(dt, isic_3_map, by='minor_isic_3')
all_2_3 <- merge(all_2_3, isic_2_map, by='minor_isic_2')
all_2_3[, major_2 := as.factor(paste0("2_", major_isic_2))] #factor for riverplot
all_2_3[, major_3 := as.factor(paste0("3_", major_isic_3))]

#create proportions to split v 2 major groups into v 3 major groups
collapse <- all_2_3[, list(code_isic_2, code_isic_3, major_isic_3, major_label_short_isic_3,
                           major_isic_2, major_label_short_isic_2)]
collapse <- collapse[, .N, by=c("major_isic_3", "major_label_short_isic_3", "major_isic_2", "major_label_short_isic_2")]
collapse[, denom_wt := sum(N), by='major_isic_3']
collapse[, denom_prop := sum(N), by='major_isic_2']
collapse[, weight := N/denom_wt]
collapse[, prop := N/denom_prop]

write.csv(collapse, file.path(out.dir, "FILEPATH"),row.names=F)

#***********************************************************************************************************************

# ---ISICv3.1 to ISICv4-------------------------------------------------------------------------------------------------
#read in the ISICv3.1->4 concordance table and analyze the flows between major groups
dt <- file.path(data.dir, 'FILEPATH') %>% fread
setnames(dt, c('ISIC31code', 'partialISIC31', 'ISIC4code', 'partialISIC4'), c('code_isic_3', 'partial_3', 'code_isic_4', 'partial_4'))

#read in the map to major groups (sheet2=rev3.1, sheet1=rev4)
isic_3_map <- read.xlsx(isic.map, sheetIndex = 2) %>% as.data.table
setnames(isic_3_map, names(isic_3_map), paste0(names(isic_3_map), "_isic_3"))
isic_4_map <- read.xlsx(isic.map, sheetIndex = 1) %>% as.data.table
setnames(isic_4_map, names(isic_4_map), paste0(names(isic_4_map), "_isic_4"))

#extract minor groups as the first 2 digits to merge on major groups
dt[, minor_isic_3 := substr(code_isic_3, start = 1, stop = 2)]
dt[, minor_isic_4 := substr(code_isic_4, start = 1, stop = 2)]
all_3_4 <- merge(dt, isic_3_map, by='minor_isic_3')
all_3_4 <- merge(all_3_4, isic_4_map, by='minor_isic_4')
all_3_4[, major_3 := as.factor(paste0("3_", major_isic_3))] #factor for riverplot
all_3_4[, major_4 := as.factor(paste0("4_", major_isic_4))] #factor for riverplot

#create proportions to split v 4 major groups into v 3 major groups
collapse <- all_3_4[, list(code_isic_3, code_isic_4, major_isic_3, major_label_short_isic_3,
                           major_isic_4, major_label_short_isic_4)]
collapse <- collapse[, .N, by=c("major_isic_3", "major_label_short_isic_3", "major_isic_4", "major_label_short_isic_4")]
collapse[, denom_wt := sum(N), by='major_isic_3']
collapse[, denom_prop := sum(N), by='major_isic_4']
collapse[, weight := N/denom_wt]
collapse[, prop := N/denom_prop]

write.csv(collapse, file.path(out.dir, "FILEPATH"),row.names=F)
