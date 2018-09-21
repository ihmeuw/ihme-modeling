# ---HEADER-------------------------------------------------------------------------------------------------------------

# Project: OCC - OCC
# Purpose: Create proportions to split ISCO based on concordance tables
# Map the flows between major groups across ISCO versions in order to crossawlk between versions

#********************************************************************************************************************************

# ---CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# set toggles


# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
  # necessary to set this option in order to read in a non-english character shapefile on a linux system (cluster)
  Sys.setlocale(category = "LC_ALL", locale = "C")
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  
}

# load packages
pacman::p_load(Cairo, data.table, gridExtra, ggplot2, lme4, magrittr, parallel, stringr, tidyr, readxl)

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

#set values for project
options(bitmapType="cairo")

##in##
data.dir <- file.path(home.dir, "FILEPATH")
doc.dir <- file.path(home.dir, 'FILEPATH')
isco.map <- file.path(doc.dir, 'ISCO_MAJOR_SUB_MAJOR.xlsx')

##out##
out.dir <- file.path(home.dir, "FILEPATH")
graph.dir <- file.path(home.dir, "FILEPATH")
#***********************************************************************************************************************

# ---FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
ubcov.function.dir <- file.path(j_root, 'FILEPATH')
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# other tools created by covs team for querying db (personal version)
file.path(central.function.dir, "db_tools.R") %>% source
# other tools created by covs team for querying db
file.path(ubcov.function.dir, "FILEPATH") %>% source
# central functions
file.path('FILEPATH') %>% source
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
                                                             "major_label_isco_"), with=F]),
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

# ---ISCO08 to ISCO88---------------------------------------------------------------------------------------------------
#read in the concordance table and analyze the flows between major groups
dt <- file.path(data.dir, 'ISCO_2008-ISCO_1988.csv') %>% fread
setnames(dt, c("ISCO-08 Title", "ISCO- 08 Code", "ISCO-88 code",  "V4", "ISCO-88 Title"),
         c('title_isic_08', 'code_isco_08', 'code_isco_88', 'partial', 'title_isic_88'))
#use tidyr to fill the isc08 values which are not repeated by rows (only gives the unique header)
dt <- fill(dt, code_isco_08, .direction="down")

#read in the map to major groups (sheet1=08, sheet2=88)
isco_08_map <- read_xlsx(isco.map, sheet = 1) %>% as.data.table
setnames(isco_08_map, names(isco_08_map), paste0(names(isco_08_map), "_isco_08"))
isco_88_map <- read_xlsx(isco.map, sheet = 2) %>% as.data.table
setnames(isco_88_map, names(isco_88_map), paste0(names(isco_88_map), "_isco_88"))

#extract minor groups as the first 2 digits to merge on major groups
dt[, minor_isco_08 := substr(code_isco_08, start = 1, stop = 2)]
dt[, minor_isco_88 := substr(code_isco_88, start = 1, stop = 2)]
all.08 <- merge(dt, isco_88_map, by.x='minor_isco_88', by.y='occ_sub_major_isco_88')
all.08 <- merge(all.08, isco_08_map, by.x='minor_isco_08', by.y='occ_sub_major_isco_08')
all.08[, major_3 := as.factor(paste0("3_", occ_major_isco_88))] #factor for riverplot
all.08[, major_4 := as.factor(paste0("4_", occ_major_isco_08))]

#create proportions to split v 2 major groups into v 3 major groups
collapse <- all.08[, list(code_isco_88, code_isco_08, major_3, major_4)]
collapse <- collapse[, .N, by=c("major_3", "major_4")]
collapse[, denom_wt := sum(N), by='major_3']
collapse[, denom_prop := sum(N), by='major_4']
collapse[, weight := N/denom_wt]
collapse[, prop := N/denom_prop]

fwrite(collapse, file=file.path(out.dir, "ISCO_08_TO_ISCO_88_LVL_1_CW.csv"))

#***********************************************************************************************************************

# ---ISCO1968 to ISCO1988-------------------------------------------------------------------------------------------------
#read in the appropriate concordance table and analyze the flows between major groups
#read in the concordance table and analyze the flows between major groups
dt <- fread(file.path(data.dir, 'ISCO_1968-ISCO_1988.csv'), sep=" ", header=F)
dt[, id := 1]
dt <- melt(dt, id.vars = "id")
dt[, c('code_isco_68', 'code_isco_88') := tstrsplit(value, "_", fixed=T)]
dt <- dt[, list(code_isco_68, code_isco_88)]

#read in the map to major groups (sheet1=08, sheet2=88, sheet3=68)
isco_68_map <- read_xlsx(isco.map, sheet = 3) %>% as.data.table
setnames(isco_68_map, names(isco_68_map), paste0(names(isco_68_map), "_isco_68"))
isco_88_map <- read_xlsx(isco.map, sheet = 2) %>% as.data.table
setnames(isco_88_map, names(isco_88_map), paste0(names(isco_88_map), "_isco_88"))

#quick cleanup on the 68 map for confusing category labels
isco_68_map[, occ_major_isco_68_agg := as.character(occ_major_isco_68)]
isco_68_map[occ_major_isco_68 %in% c(0, 1), occ_major_isco_68_agg := "0_1"]
isco_68_map[occ_major_isco_68 %in% c(7:9), occ_major_isco_68_agg := "7_9"]

#extract minor groups as the first 2 digits to merge on major groups
dt[, minor_isco_68 := substr(code_isco_68, start = 1, stop = 2)]
dt[, minor_isco_88 := substr(code_isco_88, start = 1, stop = 2)]
all.68 <- merge(dt, isco_88_map, by.x='minor_isco_88', by.y='occ_sub_major_isco_88')
all.68 <- merge(all.68, isco_68_map, by.x='minor_isco_68', by.y='occ_sub_major_isco_68')
all.68[, major_2 := as.factor(paste0("2_", occ_major_isco_68_agg))] #factor for riverplot
all.68[, major_3 := as.factor(paste0("3_", occ_major_isco_88))]

#create proportions to split v 2 major groups into v 3 major groups
collapse <- all.68[, list(code_isco_88, code_isco_68, major_3, major_2)]
collapse <- collapse[, .N, by=c("major_3", "major_2")]
collapse[, denom_wt := sum(N), by='major_3']
collapse[, denom_prop := sum(N), by='major_2']
collapse[, weight := N/denom_wt]
collapse[, prop := N/denom_prop]

fwrite(collapse, file=file.path(out.dir, "ISCO_68_TO_ISCO_88_LVL_1_CW.csv"))

#***********************************************************************************************************************

# ---RIVER--------------------------------------------------------------------------------------------------------------
#prep
setnames(all.08, c('occ_major_label_isco_08', 'occ_major_label_isco_88'),
         c('major_label_isco_4', 'major_label_isco_3'))
setnames(all.68, c('occ_major_label_isco_68', 'occ_major_label_isco_88'),
         c('major_label_isco_2', 'major_label_isco_3'))


#create riverplot
river <- makeRivPlot(all.68, all.08, "major_2", "major_3", "major_4")

#print the plot object and save as a bitmap
bmp(file.path(graph.dir, "isco_riverplot.bmp"), #toggle if only showing final PM2.5 for GBD2013
    width = 16,
    height = 12,
    units = 'in',
    type='cairo', #used to improve anti-aliasing
    res=300)

plot(river, srt=0, node_margin = .15, plot_area=.99, yscale=.95) #srt=0 sets the label rotation to be 0 degrees (dUSERt=90)
title(main='Visualizing Concordance Between Major Groups of ISCO 1968, 1988, 2008') #add title

dev.off()
#***********************************************************************************************************************