rm(list=ls())
gc()
user <- "USERNAME"
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
hpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
list.of.packages <- c("data.table","ggplot2","haven","parallel","gtools","lme4","haven","plyr")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
code.dir <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
cores <- 20
source(paste0(jpath,"FILEPATH/multi_plot.R"))

### Arguments
if(!is.na(commandArgs()[3])) {
  c.fbd_version <- commandArgs()[3]
  c.iso <- commandArgs()[4]
  c.scenario <- commandArgs()[5]
} else {
  c.fbd_version <- "20170727_dah10"
  c.iso <- "ZMB"
  c.scenario <- "reference"
}

### Functions
source(paste0(hpath, "FILEPATH/get_locations.R"))

### Tables
loc.table <- get_locations()


# Set locations
island.locs <- c("GRL", "GUM", "MNP", "PRI", "VIR", "ASM", "BMU")
locs.list <- setdiff(loc.table[spectrum == 1, ihme_loc_id], island.locs)

c.args <- fread(paste0(code.dir,"FILEPATH/run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]
dah.scalar <- c.args[["dah_scalar"]]
ref.dir <- c.args[["reference_dir"]]

inputs <- fread(paste0(jpath,"FILEPATH/forecasted_inputs.csv"))
dah.locs.list <- intersect(unique(inputs[variable == "HIV DAH" & (year_id == extension.year & pred_mean > 0), ihme_loc_id]),
			locs.list)
inputs <- inputs[variable %in% c("ART Price", "HIV DAH")  & scenario == "reference" & ihme_loc_id ==c.iso,]
dah.treat.prop <- fread(paste0(jpath, "FILEPATH/HIV DAH Ratios.csv"))
dah.treat.prop <- dah.treat.prop[!is.na(threeyrratio),list(ihme_loc_id = ISO3_RC, ratio = threeyrratio)]
#reshape wide on covariate
inputs[,V1:=NULL]
inputs <- dcast.data.table(inputs,ihme_loc_id+year_id+scenario~variable,value.var=c("pred_mean"))
#create doses covariates by rescaling by price
setnames(inputs,old=c("ART Price","HIV DAH", "year_id","ihme_loc_id"),new=c("art_price","hiv_dah","year","iso3"))

hiv_dah <- data.table(read_dta(paste0(jpath, "temp/USER/GHESpc_HIV_DAHpc_20170710.dta")))
hiv_dah <- hiv_dah[iso3 %in% unique(inputs$iso3), .(iso3, year,total_pop)]



merge.dah <- merge(inputs, hiv_dah, by = c("iso3", "year"), all.x = T)

#filling in subnat pops
source(paste0(jpath, "FILEPATH/get_population.R"))
missing_pop <- merge.dah[is.na(total_pop)]
missing_pop[,ihme_loc_id := iso3]
#filling in future pops with 2016 pop
for(cc in unique(missing_pop$iso3)){
	print(cc)
	pop <- fread(paste0(jpath,"FILEPATH/", cc,"_pop.csv"))
	for(yy in c(1995:2040)){
		if(yy < 2016){
			popyear <- pop[year == yy, list(value = sum(value)), by = "year" ]
		} else{
			popyear <- pop[year == 2016, list(value = sum(value)), by = "year" ]
		}
	
	merge.dah[iso3 == cc & year ==yy, total_pop := unique(popyear[,value])]
		}
	}

merge.dah <- merge.dah[!is.na(total_pop),]

merge.dah[, hiv_dah := hiv_dah* total_pop]

setnames(dah.treat.prop, "ihme_loc_id", "iso3")
merge.dah <- merge(merge.dah, dah.treat.prop, by = c("iso3"), all.x = T)
#filling in subnat props
missing_prop <- merge.dah[is.na(ratio),]
for(cc in unique(missing_prop$iso3)){
	dah.treat.prop.cc <- dah.treat.prop[substr(cc, 1, 3) == iso3, ratio]
	merge.dah[iso3 == cc, ratio := dah.treat.prop.cc]
}

pepfar <- fread(paste0(hpath, "FILEPATH/pepfar_expenditures.csv"))
pepfar[,expenditures := as.numeric(expenditures)]
pepfar <- pepfar[!is.na(expenditures),list(country, expenditures, cost_category)]
total <- pepfar[,list(cost_category = "Total", expenditures = sum(as.numeric(expenditures), na.rm = T)), by = c("country")]
pepfar <- rbind(pepfar, total, use.names = T)
pepfar <- pepfar[cost_category %in% c("Total", "Antiretroviral drugs (ARVs)")]
pepfar[cost_category == 'Antiretroviral drugs (ARVs)', cost_category := "ART"]
pepfar <- pepfar[,list(expenditures = sum(as.numeric(expenditures))), by = c("country", "cost_category")]
pepfar <- dcast.data.table(pepfar, country ~ cost_category, value.var = "expenditures")
pepfar[, prop_art_pepfar := ART/Total]
pepfar <- pepfar[!prop_art_pepfar == 0,]
pepfar[,pepfar_scalar := prop_art_pepfar/.11]
setnames(pepfar, "country", "location_name")
pepfar <- merge(pepfar, loc.table[,list(location_name, ihme_loc_id)], by = "location_name")
setnames(pepfar, "ihme_loc_id", "country")
pepfar <- pepfar[pepfar_scalar < 1,]
merge.dah[,country := substr(iso3, 1, 3)]
merge.dah <- merge(merge.dah, pepfar[,list(country, pepfar_scalar)], by = c("country"), all.x = T)
merge.dah[!is.na(pepfar_scalar), dah_art := hiv_dah * ratio * 0.77 * pepfar_scalar ]
merge.dah[is.na(pepfar_scalar), dah_art := (hiv_dah * ratio * 0.77)]
merge.dah[, art_count := dah_art / art_price]
merge.dah[is.na(art_count), art_count := 0]
merge.dah[year > extension.year,art_decline := (1 -(1-dah.scalar) ^ (year - extension.year)) * art_count]

# Subtract decline from reference
mean.draws.list <- list()
i <- 1
for(c.iso in unique(merge.dah$iso3)){
	print(c.iso)
	draws <-  fread(paste0("FILEPATH", c.iso, "_ART_data.csv"))
	collapsed.draws <- draws[, .(art = sum(pop_art)), by = .(year, run_num)]
	mean.draws <- collapsed.draws[, .(art = mean(art)), by = .(year)]
	mean.draws[,iso3 := c.iso]
	mean.draws.list[[i]] <- mean.draws
i <- i+1
}
mean.draws <- rbindlist(mean.draws.list, use.names = T)

merge.draws <- merge(merge.dah, mean.draws, by = c("year", "iso3"))
merge.draws[, rel_diff := (art - art_decline) / art]
merge.draws[rel_diff < 0, rel_diff := 0]
merge.draws[,art_after_dah_decline := art - art_decline]
merge.draws[art_after_dah_decline <0, art_after_dah_decline := 0]
merge.draws[is.na(rel_diff), rel_diff:=1]

# # Scale reference
input.path  <- paste0("FILEPATH/",c.iso,".csv")
draw.dt <- fread(input.path)
forecast.draws <- merge(draw.dt, merge.draws[, .(iso3, year, rel_diff)], by = c("year", "iso3"))
for(i in 0:999) {
  print(i)
  forecast.draws[year > extension.year, (paste0("art_coverage_", i)) := get(paste0("art_coverage_", i)) * rel_diff]
}

#Calculate mean, upper and lower
forecast.summary <-  forecast.draws[,.SD,.SDcols=c("iso3","age","sex","CD4","year")]
forecast.summary[,mean_art_coverage:= rowMeans(na.rm=T,as.matrix(forecast.draws[, paste0("art_coverage_", 0:999), with=F]),dims=1)]
forecast.summary[,lower_art_coverage:= apply(as.matrix(forecast.draws[, paste0("art_coverage_", 0:999), with=F]),1, quantile, probs = c(0.05),na.rm=T )]
forecast.summary[,upper_art_coverage:= apply(as.matrix(forecast.draws[, paste0("art_coverage_", 0:999), with=F]),1, quantile, probs = c(0.95),na.rm=T)]
forecast.summary[order(iso3,age,sex,CD4,year)]

#Save summaries
output.dir  <- "FILEPATH"
write.csv(forecast.summary,file=paste0(output.dir,c.iso,".csv"),row.names = F)

#Draws
output.dir  <- "FILEPATH"
write.csv(forecast.draws,file=paste0(output.dir,c.iso,".csv"),row.names = F)

### End

