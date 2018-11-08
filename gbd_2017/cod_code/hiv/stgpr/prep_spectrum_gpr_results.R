### Setup
rm(list=ls())
gc()
root <- ifelse(Sys.info()[1]=="Windows", "ADDRESS", "ADDRESS")
library(data.table)

### Paths
gpr.path <- "FILEPATH"

out.path <- "FILEPATH"

### Code
## Add Cambodia to data by scaling Thailand deaths by 0.8
gpr.data <- fread(gpr.path)[, .(location_id, year_id, age_group_id, sex_id, gpr_mean)]
thai.gpr <- gpr.data[location_id==18]
thai.gpr[, gpr_mean:=0.8*gpr_mean]
khm.gpr <- thai.gpr[, location_id:=10]
bound.gpr <- rbind(gpr.data, khm.gpr)
## Fill in 0's for all missing years from 1970 on
fill.data <- data.table(expand.grid(location_id=unique(bound.gpr$location_id), year_id=seq(1970, 2015),
																		age_group_id=unique(bound.gpr$age_group_id), sex_id=unique(bound.gpr$sex_id),
																		gpr_replace=0))
merged.data <- merge(bound.gpr, fill.data, by=c("location_id", "year_id", "age_group_id", "sex_id"), all.y=T)
merged.data[is.na(gpr_mean),gpr_mean:=gpr_replace]
prepped.gpr.data <- merged.data[, gpr_replace:=NULL]

write.csv(prepped.gpr.data, file=out.path, row.names=F)
### End
