## Create lifetables for aggregate locations, create summary lifetable file, add onto current LT file
rm(list=ls())
library(data.table); library(foreign); library(reshape2)

## Setup filepaths
if (Sys.info()[1]=="Windows") {
  root <- "filepath" 
  user <- Sys.getenv("USERNAME")

} else {
  root <- "filepath"
  user <- Sys.getenv("USER")
  
  loc_id <- commandArgs()[3]
  type <- commandArgs()[4]
  new_upload_version <- commandArgs()[5]

  
  parent_dir <- paste0("filepath")

}


## Create age map and location map
age_map <- data.table(get_age_map(type="lifetable"))
age_map <- age_map[,list(age_group_id,age_group_name_short)]
setnames(age_map,"age_group_name_short","age")

locations <- data.table(get_locations(level="all"))

years <- c(1970:2016)

## Read in data 
filepaths <- paste0(filepath)
mx_ax_compiled <- rbindlist(lapply(filepaths,load_hdf,by_val = loc_id))

## Format combined file for LT function
format_for_lt <- function(data) {
  data[,qx:=0]
  data[,id:=paste0(location_id,"_",draw)]
  setnames(data,"year_id","year")
  
  return(data)
}

# mx_ax_compiled <- merge(mx_ax_compiled,age_map,by="age_group_id")
mx_ax_compiled <- format_for_lt(mx_ax_compiled)
mx_ax_compiled$age_group_id <- as.integer(mx_ax_compiled$age_group_id)

## Run lifetable function on the data
lt_total <- lifetable(mx_ax_compiled,cap_qx=1)
lt_total[,id:=NULL]
write.csv(lt_total[,list(age_group_id,sex_id,year,draw,mx,ax,qx,n,px,lx,dx,nLx,Tx,ex)],
          paste0(filepath),row.names=F)

## Collapse to 5q0 and 45q15 before saving 5q0 and 45q15
summary_5q0 <- calc_qx(lt_total,age_start=0,age_end=5,id_vars=c("location_id","sex_id","year","draw"))
setnames(summary_5q0,"qx_5q0","mean_5q0")
summary_5q0 <- summary_5q0[,lapply(.SD,mean),.SDcols="mean_5q0", by=c("location_id","sex_id","year")]
setcolorder(summary_5q0,c("sex_id","year","mean_5q0","location_id"))

summary_45q15 <- calc_qx(lt_total,age_start=15,age_end=60,id_vars=c("location_id","sex_id","year","draw"))
setnames(summary_45q15,"qx_45q15","mean_45q15")
summary_45q15 <- summary_45q15[,lapply(.SD,mean),.SDcols="mean_45q15",by=c("location_id","sex_id","year")]
setcolorder(summary_45q15,c("sex_id","year","mean_45q15","location_id"))

write.csv(summary_5q0,paste0(filepath),row.names=F)
write.csv(summary_45q15,paste0(filepath),row.names=F)
