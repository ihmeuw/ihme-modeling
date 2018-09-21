rm(list=ls())
gc()
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
hpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH"))
list.of.packages <- c("data.table","ggplot2","parallel","gtools","haven")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
cores <- 20
source(paste0(jpath,"FILEPATH/multi_plot.R"))
source(paste0(hpath, "FILEPATH/get_locations.R"))

loc.table <- get_locations()

c.fbd_version <- commandArgs()[3]
c.args <- fread(paste0(hpath,"FILEPATH/run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]
gbd.aggs.version <- c.args[["gbd_aggs_version"]]
scenarios <- c()
for(scen in c("reference", "better", "worse")) {
  if(c.args[[scen]] == 1) {
    scenarios <- c(scenarios, scen)
  }
}

dah.scalar <- c.args[["dah_scalar"]]
dah_scenario <- F
if(dah.scalar != 0) {
	dah_scenario <- T
	c.ref <- c.args[["reference_dir"]]
}
if(dah_scenario){

	island.locs <- c("GRL", "GUM", "MNP", "PRI", "VIR", "ASM", "BMU")
	agg.locs <- c("ZWE", "CIV", "MOZ", "HTI", "IND_44538", "SWE", "MDA", "ZAF", "KEN", "JPN", "GBR", "USA", "MEX", "BRA", "SAU", "IND", "GBR_4749", "CHN_44533", "CHN", "IDN")

	locs.list <- setdiff(loc.table[spectrum == 1, ihme_loc_id], island.locs)
	inputs <- fread(paste0(jpath,"FILEPATH/forecasted_inputs.csv"))
	dah.list <- intersect(unique(inputs[variable == "HIV DAH" & (year_id == extension.year & pred_mean > 0), ihme_loc_id]),
				locs.list)
	ref.nats <- loc.table[level == 3 & (ihme_loc_id %in% setdiff(locs.list, dah.list) | ihme_loc_id %in% c("SWE", "USA", "SAU", "JPN", "GBR")),ihme_loc_id]
}

locs <- loc.table

for(c.scenario in scenarios) {

	print(c.scenario)
	#load data
	data.files <- list.files(paste0(jpath,"FILEPATH"))
	data.locs <- gsub(".csv", "", data.files)
	data.nats <- intersect(data.locs, loc.table[level == 3, ihme_loc_id])
	if(dah_scenario){
		data.nat.check <- c(data.nats, ref.nats)
	}else{
		data.nat.check <- data.nats
	}
	nat.check <- loc.table[level == 3 & !ihme_loc_id %in% c("GRL", "GUM", "MNP", "PRI", "VIR", "ASM", "BMU"), ihme_loc_id]
	if(length(setdiff(nat.check, data.nat.check)) > 0) {
		stop(print(paste0("Missing locations, ", setdiff(nat.check, data.nat.check))))
	}
	ptm <- proc.time()
	data.list <- mclapply(data.nats, function(loc) {
	  data <- fread(paste0(jpath,"FILEPATH/", loc, ".csv"))
	},mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))
	data <- rbindlist(data.list,fill=T)
	if(dah_scenario){
		ref.list <- mclapply(ref.nats, function(loc) {
		  ref <- fread(paste0(jpath,"FILEPATH/", loc, ".csv"))
		},mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))
		ref <- rbindlist(ref.list, fill = T)
	data <- rbind (data, ref, use.names = T)
}
	
	data <- data[!is.na(value)]
	print(proc.time() - ptm)
	data <- merge(data,locs,by='ihme_loc_id')

	#global
	global.aggs  <- data[,.(value=weighted.mean(x=value,w=pop),pop=sum(pop)),by=.(year_id,run_num,variable)]
	global.aggs[is.na(value), value := 0]
	summary <- global.aggs[,.(pop=mean(pop),mean=mean(value),upper=quantile(value,probs=c(.975)),lower=quantile(value,probs=c(.025))),by=.(year_id,variable)]
	summary[,location_id:=1]

	#super regions 
	sr.aggs  <- data[,.(value=weighted.mean(x=value,w=pop),pop=sum(pop)),by=.(year_id,run_num,variable,super_region_name,super_region_id)]
	sr.aggs[is.na(value), value := 0]
	sr.summary <- sr.aggs[,.(pop=mean(pop),mean=mean(value),upper=quantile(value,probs=c(.975)),lower=quantile(value,probs=c(.025))),by=.(year_id,variable,super_region_id)]
	setnames(sr.summary,"super_region_id","location_id")
	#regions
	r.aggs  <- data[,.(value=weighted.mean(x=value,w=pop),pop=sum(pop)),by=.(year_id,run_num,variable,region_name,region_id)]
	r.aggs[is.na(value), value := 0]
	r.summary <- r.aggs[,.(pop=mean(pop),mean=mean(value),upper=quantile(value,probs=c(.975)),lower=quantile(value,probs=c(.025))),by=.(year_id,variable,region_id)]
	setnames(r.summary,"region_id","location_id")

	aggs <- rbind(summary,sr.summary,r.summary)
	aggs[,location_id:=as.numeric(location_id)]

	aggs <- merge(aggs,locs,by='location_id')
	write.csv(aggs,paste0(jpath,"FILEPATH/aggregations.csv"))


}



