#----HEADER-------------------------------------------------------------------------------------------------------------
# Project: air_pm
# Purpose: Save gridded PM2.5 values by country year for PAF calculation
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
  arg <- tail(commandArgs(), n=5) # First args are for unix use only
  if (length(arg)!=5) {
    arg <- c("354", 2011, "33", 1000, 5) #toggle targetted run 
  }
 

  
} else {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  arg <- c("177", 2017, "28", 1000, 1) #country,draw.method,grid.version,draws.required,cores.provided
  
}

#Packages:
lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","fst","matrixStats","magrittr","Hmisc")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}


#set the seed to ensure reproducibility and preserve covariance across parallelized countries
set.seed(42) 

#set parameters based on arguments from master
this.country <- arg[1]
this.year <- arg[2]
grid.version <- arg[3]
draws.required <- as.numeric(arg[4])
cores.provided <- as.numeric(arg[5])

print(arg)

draw.colnames<-paste0("draw_",1:draws.required)
out.colnames <- paste0("draw_",0:(draws.required-1))

# set working directories
home.dir <- file.path(j_root, "FILEPATH")
setwd(home.dir)


#***********************************************************************************************************************


#----IN/OUT-------------------------------------------------------------------------------------------------------------
# Set directories and load files

# where to output the split gridded files
exp.dir <- file.path('FILEPATH', grid.version)
out.exp.tmp <-  file.path("FILEPATH", grid.version)
dir.create(file.path(out.exp.tmp, "final_draws"), recursive=T, showWarnings=F)
out.exp.dir <- file.path(home.dir, 'FILEPATH', grid.version)
dir.create(file.path(out.exp.dir, "summary"), recursive=T, showWarnings=F)

# file that will be created by 03_gen_draws step
pollution <- paste0(exp.dir,"FILEPATH/all_grids_",this.year,".fst") %>%
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

# Sort
setkeyv(pollution, c("longitude", "latitude"))

# ensure there are no duplicate grids
pollution <- unique(pollution, by=c('longitude', 'latitude'))

#output each country to feed into PAF calculation in parallel
write.fst(pollution,
          path=file.path(exp.dir,"draws", paste0(this.country,"_",this.year, ".fst")))

#---------------------SUMMARY OBJECTS-------------------------------------------------------------------------------------
# First make a summary that is gridded
# add index
pollution[, index := seq_len(.N)]

#create summary table of mean, median, lower and upper for each grid cell
summary <- copy(pollution)
summary[, "lower":= quantile(.SD, c(.025)), .SDcols=draw.colnames, by="index"]
summary[, "mean" := rowMeans(.SD), .SDcols=draw.colnames, by="index"]
summary[, "median" := as.matrix(.SD) %>% rowMedians, .SDcols=draw.colnames, by="index"]
summary[, "upper" := quantile(.SD, c(.975)), .SDcols=draw.colnames, by="index"]

# Only keep relevant columns
summary <- summary[,.(location_id,location_name,longitude,latitude,lower,mean,median,upper)]
summary <- summary[,year_id:=this.year]

#save as CSV
write.csv(summary, file.path(exp.dir,"summary",paste0(this.country,"_",this.year, ".csv")), row.names=F)


#Now Create Pop-weighted location values
# calculate population weighted draws
exp<-copy(pollution)

# create weighted pop based on proportion of grid covered by border file
exp[,w_pop:=weight*pop]


#take weighted sample to generate draws
out <- data.table(location_id=this.country, year_id=this.year)
out[,c(draw.colnames):=sample(as.matrix(exp[,c(draw.colnames),with=F]),draws.required,prob=as.matrix(exp[,rep("w_pop",draws.required),with=F])) %>% as.list()]

# calculate mean and CI for summary figures
out[, lower := quantile(out[,c(draw.colnames),with=F],0.025)]
out[, mean := rowMeans(out[,c(draw.colnames),with=F])]
out[, median := quantile(out[,c(draw.colnames),with=F],0.5)]
out[, upper := quantile(out[,c(draw.colnames),with=F],0.975)]

exp_draws <- copy(out)

#add year, metric, sex and age group for upload
exp_draws[,measure_id:=19]
exp_draws[,merge:=1]
ages <- data.table(merge=1,age_group_id=c(2:20,30,31,32,235))
sexes <- data.table(merge=1,sex_id=c(1,2))
exp_draws <- merge(exp_draws,ages,by="merge", allow.cartesian=T)
exp_draws <- merge(exp_draws,sexes,by="merge", allow.cartesian=TRUE)
exp_draws[,merge:=NULL]

setnames(exp_draws,c(draw.colnames),c(out.colnames))


#output pop-weighted draws
write.csv(exp_draws, paste0(out.exp.tmp, this.country,"_",this.year, ".csv"), row.names=F)



# also put in summary statistics from all 1000*n draws

out[,true_mean:=weighted.mean(as.matrix(exp[,c(draw.colnames),with=F]),w=as.matrix(exp[,rep("w_pop",draws.required),with=F]))]
out[,true_lower:=wtd.quantile(as.matrix(exp[,c(draw.colnames),with=F]),weights=as.matrix(exp[,rep("w_pop",draws.required),with=F]),probs=0.025)]
out[,true_upper:=wtd.quantile(as.matrix(exp[,c(draw.colnames),with=F]),weights=as.matrix(exp[,rep("w_pop",draws.required),with=F]),probs=0.975)]
out[,true_median:=weightedMedian(as.matrix(exp[,c(draw.colnames),with=F]),w=as.matrix(exp[,rep("w_pop",draws.required),with=F]))]


#also save version with just summary info (mean/ci)
write.csv(out[, -draw.colnames, with=F],
          paste0("FILEPATH", this.country,"_",this.year, ".csv"), row.names=F)


#***********************************************************************************************************************
