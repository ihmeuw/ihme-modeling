############################################################################################################
## Purpose: Take output of employment ratio model and create envelope for HRH estimates
###########################################################################################################

## clear memory
rm(list=ls())

## runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

pacman::p_load(data.table,magrittr,parallel)

## most recent run for occ_emp_ratio
run_id <- 43617 # 41826 # 40034

## in/out
files <- list.files(file.path("FILEPATH"),full.names = T)
out_dir <- file.path(j,"FILEPATH")
outfile <- "prop_emp_oftotal.csv"
outfile_sex <- "prop_ofemp_bysex.csv"

draw.cols <- paste0("draw_",seq(0,999))
id.vars <- c("location_id","year_id","sex_id","age_group_id")

## read in draws and collapse to just the mean
df <- rbindlist(lapply(files,fread))
df[,measure_id := NULL]
df <- melt(df,id.vars = id.vars,value.name = "emp_prop",variable.name = "draw")

## load and merge on  populations
source(file.path(j,"FILEPATH"))
pops <- data.table(get_population(location_set_id = 22, year_id = seq(1980,2017), sex_id = seq(1,2), location_id = -1, age_group_id = seq(8,18)))
pops <- pops[,-c("run_id"),with=F]
poptotal <- data.table(get_population(location_set_id = 22, year_id = seq(1980,2017), sex_id = 3, location_id = -1, age_group_id = 22))
setnames(poptotal,"population","poptotal")
poptotal <- poptotal[,list(location_id,year_id,poptotal)]
pops <- merge(pops,poptotal,by=id.vars[1:2])
df <- merge(df, pops, by=id.vars)

## calculate proportion of employed pop ages 15-69 made up by each sex (used to aggregate
## occ and industry covariates for use in HRH models), and write to file
df[,total_emp := sum(emp_prop*population),by=c(id.vars[1:2],"draw")]
dt <- df[,sum(emp_prop*population/total_emp),by=c(id.vars[1:3],"draw")]
dt <- dcast(dt,location_id + year_id + sex_id ~ draw,value.var = "V1")
dt[,age_group_id := 22]
write.csv(dt,file.path(out_dir,outfile_sex),row.names=F)

## calculate employment proportions of total population, and write to file
dt <- df[,sum(emp_prop*population/poptotal),by=c(id.vars[1:2],"draw")]
dt <- dcast(dt,location_id + year_id ~ draw,value.var = "V1")
dt[,age_group_id := 22]
dt[,sex_id := 3]
write.csv(dt,file.path(out_dir,outfile),row.names=F)
