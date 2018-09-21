#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: RF: air_pm
# Purpose: Take the global gridded shapefile and cut it up into different countries/subnationals using shapefiles
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- commandArgs()[-(1:3)]# First args are for unix use only
  if (length(arg)==0) {
    arg <- c("GLOBAL", "log_space", "23", 1000, 80) #toggle targetted run
  }
  
  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- c("PNG", "log_space", "16", 100, 10)
  
}

#set the seed to ensure reproducibility and preserve covariance across parallelized countries
set.seed(42) 

#set parameters based on arguments from master
this.country <- arg[1]
draw.method <- arg[2]
grid.version <- arg[3]
draws.required <- as.numeric(arg[4])
cores.provided <- as.numeric(arg[5])

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)

# load packages, install if missing
pacman::p_load(data.table, fst, ggplot2, parallel, magrittr, matrixStats)
#***********************************************************************************************************************

#----FUNCTIONS----------------------------------------------------------------------------------------------------------
##function lib##
#Air EXP functions#
exp.function.dir <- file.path(h_root, 'FILEPATH')
file.path(exp.function.dir, "assign_tools.R") %>% source

#general functions#
central.function.dir <- file.path(h_root, "FILEPATH")
# this pulls the general misc helper functions
file.path(central.function.dir, "misc.R") %>% source
# this pulls the current locations list
file.path(central.function.dir, "get_locations.R") %>% source
#***********************************************************************************************************************

#----IN/OUT-------------------------------------------------------------------------------------------------------------
# Set directories and load files
# Get the list of most detailed GBD locations
location_id.list <- data.table(get_locations()) 

# where to output the split gridded files
out.dir <-  file.path("FILEPATH", grid.version)
exp.dir <- file.path('FILEPATH', grid.version)

# file that will be created by the previous assign codeblock
pollution <- file.path(exp.dir, "all_grids.fst") %>%
  read.fst(as.data.table=TRUE)
#***********************************************************************************************************************

#----DRAW---->SAVE------------------------------------------------------------------------------------------------------
#subset pollution file to current country (unless producing global gridded file)
if (this.country!="GLOBAL" & this.country != "EU") {
  pollution <- pollution[location_id==this.country]
} else if (this.country == "EU") {
  pollution <- pollution[ihme_loc_id %in% c('AUT', 'BEL', 'BGR', 'HRV', 'CYP', 'CZE', 'DNK', 'EST', 'FIN', 'FRA', 'DEU',
                                            'GRC', 'HUN', 'IRL', 'ITA', 'LVA', 'LTU', 'LUX', 'MLT', 'NLD', 'POL', 'PRT',
                                            "ROU", 'SVK', 'SVN', 'ESP', 'SWE', 'GBR')]
}
# generate 1000 draws by grid and then save a csv of this country
setkeyv(pollution, c("long", "lat", "year"))

# create a list of draw names based on the required number of draws for this run
draw.colnames <- c(paste0("draw_", 1:draws.required))

# convert relevant vars to numerics 
vars <- c('log_mean', 'log_sd', 'pop', 'lower95', 'upper95', 'mean', 'median', 'year')
pollution[, (vars) := lapply(.SD, as.numeric), .SDcols=vars]

#ensure there are no duplicate grids
pollution <- unique(pollution, by=c('long', 'lat', 'year'))

# add index
pollution[, index := seq_len(.N)]

#divide into a list where each piece is a chunk of the dt (#cores x ~equal pieces in total)
chunks <- split(pollution, as.numeric(as.factor(pollution$index)) %% (cores.provided*10))

#break DT into 1000 grid chunks in order to parallelize this calculation
chunkWrapper <- function(name,
                         this.chunk,
                         transformation=draw.method,
                         test.toggle=FALSE, #toggle to create mean/CI to test the draws have been done correctly
                         ...) {
  
  if (cores.provided == 1) {
    message(name) #print loop status, or give quarterly milestones if parallel
  } else if (cores.provided > 1 & name == length(chunks)/4) {
    message("@ 25%", "[", name, "]")
  } else if (cores.provided > 1 &  name == length(chunks)/2) {
    message("@ 50%", "[", name, "]")
  } else if (cores.provided > 1 & name == length(chunks)/4*3) {
    message("@ 75%", "[", name, "]")
  }
  
  if (transformation == "log_space") {
    
    #create draws of exposure based on provided uncertainty -- log space 
    this.chunk[, draw.colnames := rnorm(draws.required, mean=log_mean, sd=log_sd) %>%
                 exp %>%
                 as.list,
               by="index", with=F]
    
  } else if (transformation == "normal_space") {
    
    this.chunk[, draw.colnames := rnorm(draws.required, mean=median, sd=sd) %>%
                 as.list,
               by="index", with=F]
    
  }
  
  if (test.toggle == TRUE) {
    
    #test work
    this.chunk[, "lower_calc" := quantile(.SD, c(.025)), .SDcols=draw.colnames, by="index"]
    this.chunk[, "mean_calc" := rowMeans(.SD), .SDcols=draw.colnames, by="index"]
    this.chunk[, "median_calc" := as.matrix(.SD) %>% rowMedians, .SDcols=draw.colnames, by="index"]
    this.chunk[, "upper_calc" := quantile(.SD, c(.975)), .SDcols=draw.colnames, by="index"]
    
    this.chunk[, "diff_lower" := lower95 - lower_calc]
    this.chunk[, "diff_median" := median - median_calc]
    this.chunk[, "diff_mean" := mean - mean_calc]
    this.chunk[, "diff_upper" := upper95 - upper_calc]
    
  }
  
  return(this.chunk)
  
}

exp <- mcmapply(chunkWrapper, names(chunks), chunks, mc.cores=(cores.provided), SIMPLIFY=FALSE) %>% rbindlist

# #create a summary table for diagnostics
# summary.table <- sapply(exp[, c("diff_lower", "diff_median", "diff_upper"), with=F], summary) %>% as.data.table(keep.rownames=T)
# summary.table[, iso3 := this.country]
# summary.table[, draw_method := draw.method]
#
# #write this table
# write.csv(summary.table,
#           file.path(out.dir, "summary", paste0(this.country, ".csv")),
#           row.names=F)

#output each country to feed into PAF calculation in parallel
write.fst(exp,
          path=file.path(exp.dir, paste0(this.country, ".fst")))
# save(exp,
#      file=file.path(exp.dir, paste0(this.country, ".Rdata")))
#***********************************************************************************************************************