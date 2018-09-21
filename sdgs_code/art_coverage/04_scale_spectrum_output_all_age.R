rm(list=ls())
gc()


jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
list.of.packages <- c("data.table","ggplot2","parallel","gtools","haven")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
hpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
code.dir <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
cores <- 5
source(paste0(jpath,"FILEPATH/multi_plot.R"))
source(paste0(jpath, "FILEPATH/get_population.R"))

### Arguments
if(!is.na(commandArgs()[3])) {
	c.fbd_version <- commandArgs()[3]
	c.iso <- commandArgs()[4]
	c.scenario <- commandArgs()[5]
} else {
  c.fbd_version <- "20170721"
  c.iso <- "OMN"
  c.scenario <- "reference"
}
print(commandArgs())
print(c.fbd_version)
print(c.iso)
print(c.scenario)

c.args <- fread(paste0(code.dir,"FILEPATH/run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]
gbd.aggs.version <- c.args[["gbd_aggs_version"]]

gbd_version <- "FILEPATH"
fbd_version <- "FILEPATH"

#load gbd and fbd data  
gbd <- fread(paste0(gbd_version,c.iso,".csv"))[run_num %in% 1:c.draws]
fbd <- fread(paste0(fbd_version,c.iso,"_ART_data.csv"))[run_num %in% 1:c.draws]
loc_id <- unique(gbd[,location_id])

# collapse older gbd
gbd[age_group_id > 20, age_group_id := 21]
gbd <- gbd[, lapply(.SD, sum), by = .(year_id, sex_id, age_group_id, run_num, location_id)]

#convert fbd variables to match gbd  
#sex
fbd[sex=="male",sex_id:=1]
fbd[sex=="female",sex_id:=2]
#age
fbd[,age_group_id:=(age/5)+5]
temp4 <- fbd[age_group_id==5]
temp3 <- fbd[age_group_id==5]
temp2 <- fbd[age_group_id==5]
temp4[,age_group_id:=4]
temp3[,age_group_id:=3]
temp2[,age_group_id:=2]
fbd <- rbind(fbd,temp4,temp3,temp2)
#pops
fbd[,pop:=pop_neg+pop_lt200+pop_200to350+pop_gt350+pop_art]
fbd[,pop_hiv:=pop - pop_neg]
setnames(gbd, c("population", "year_id"), c("pop", "year"))

#make rates
gbd[,g_mort_rate:=hiv_deaths/pop]
gbd[,g_inc_rate:=scaled_new_hiv/pop]
gbd[,g_prev_rate:=pop_hiv/pop]
gbd[,g_art_rate:=ifelse(pop_hiv == 0, 0, pop_art/pop_hiv)]

fbd[,f_mort_rate:=hiv_deaths/pop]
fbd[,f_inc_rate:=new_hiv/pop]
fbd[,f_prev_rate:=pop_hiv/pop]
fbd[,f_art_rate:=ifelse(pop_hiv == 0, 0, pop_art/pop_hiv)]


#fill in missing values
fbd <- fbd[order(age,sex,run_num,year)]
fbd[,temp_f_art_rate:=((shift(f_art_rate,n=1L,type="lag") +shift(f_art_rate,n=1L,type="lead")) / 2) ]
fbd[,temp_f_inc_rate:=((shift(f_inc_rate,n=1L,type="lag") +shift(f_inc_rate,n=1L,type="lead")) / 2) ]
fbd[,temp_f_prev_rate:=((shift(f_prev_rate,n=1L,type="lag") +shift(f_prev_rate,n=1L,type="lead")) / 2) ]
fbd[,temp_f_mort_rate:=((shift(f_mort_rate,n=1L,type="lag") +shift(f_mort_rate,n=1L,type="lead")) / 2) ]
fbd[,temp2_f_art_rate:=((shift(f_art_rate,n=2L,type="lag") +shift(f_art_rate,n=2L,type="lead")) / 2) ]
fbd[,temp2_f_inc_rate:=((shift(f_inc_rate,n=2L,type="lag") +shift(f_inc_rate,n=2L,type="lead")) / 2) ]
fbd[,temp2_f_prev_rate:=((shift(f_prev_rate,n=2L,type="lag") +shift(f_prev_rate,n=2L,type="lead")) / 2) ]
fbd[,temp2_f_mort_rate:=((shift(f_mort_rate,n=2L,type="lag") +shift(f_mort_rate,n=2L,type="lead")) / 2) ]

fbd[is.na(f_inc_rate),f_inc_rate:=temp_f_inc_rate]
fbd[is.na(f_inc_rate),f_inc_rate:=temp2_f_inc_rate]
fbd[is.na(f_mort_rate),f_mort_rate:=temp_f_mort_rate]
fbd[is.na(f_mort_rate),f_mort_rate:=temp2_f_mort_rate]
fbd[is.na(f_prev_rate),f_prev_rate:=temp_f_prev_rate]
fbd[is.na(f_prev_rate),f_prev_rate:=temp2_f_prev_rate]
fbd[is.na(f_art_rate),f_art_rate:=temp_f_art_rate]
fbd[is.na(f_art_rate),f_art_rate:=temp2_f_art_rate]
gbd[g_prev_rate==0,g_art_rate:=0]
fbd[f_prev_rate==0,f_art_rate:=0]

#subset variables
gbd <- gbd[,.SD,.SDcols=c("pop", "year","age_group_id","sex_id","run_num","g_mort_rate","g_inc_rate","g_prev_rate","g_art_rate")]
fbd <- fbd[,.SD,.SDcols=c("year","age_group_id","sex_id","run_num","f_mort_rate","f_inc_rate","f_prev_rate","f_art_rate")]
fbd <- melt.data.table(fbd, id.vars = c("year", "age_group_id", "sex_id", "run_num"))
gbd <- melt.data.table(gbd, id.vars = c("year", "age_group_id", "sex_id", "run_num", "pop"))

pops <- fread(paste0(jpath,'FILEPATH/WPP_Pops_20150101.csv'))
locs  <-data.table(read_dta(paste0(jpath,'FILEPATH/IHME_GDB_2015_LOCS_6.1.15.dta')))
locs <- locs[location_type_id==2,c("location_id","ihme_loc_id"),with=F]
pops <-data.table(merge(pops,locs,by="location_id"))
setnames(fbd,"year","year_id")
fbd[,ihme_loc_id:=c.iso]
fbd <- data.table(merge(fbd,pops,by=c("ihme_loc_id","year_id","age_group_id","sex_id"),all.x=T))

prev <- fbd[variable == "f_prev_rate"]
prev[,prev_count:=pop*value]
prev <- prev[,.SD,.SDcols=c("ihme_loc_id","year_id","age_group_id","sex_id","run_num","prev_count")]
#merge on prev counts as denom for ART
fbd <- data.table(merge(fbd,prev,by=c("ihme_loc_id","year_id","age_group_id","sex_id","run_num"),all.x=T))
fbd[variable=="f_art_rate",pop:=prev_count]

#calculate national all-age both sex agg draws
print("making national aggs")
nat.aggs <- fbd[,.(value=weighted.mean(x=value,w=pop),pop=sum(pop)),by=.(ihme_loc_id,year_id,run_num,variable,location_id)]
nat.aggs[is.na(value), value := 0]

setnames(gbd,"year","year_id")
gbd[,ihme_loc_id:=c.iso]

prev <- gbd[variable == "g_prev_rate"]
prev[,prev_count:=pop*value]
prev <- prev[,.SD,.SDcols=c("ihme_loc_id","year_id","age_group_id","sex_id","run_num","prev_count")]
#merge on prev counts as denom for ART
gbd <- data.table(merge(gbd,prev,by=c("ihme_loc_id","year_id","age_group_id","sex_id","run_num"),all.x=T))
gbd[variable=="g_art_rate",pop:=prev_count]
gbd[,location_id := loc_id]
gbd.nat.aggs <- gbd[,.(value=weighted.mean(x=value,w=pop),pop=sum(pop)),by=.(ihme_loc_id,year_id,run_num,variable, location_id)]
gbd.nat.aggs[is.na(value), value := 0]

final <- rbind(nat.aggs, gbd.nat.aggs, use.names = T)
final[,location_id := NULL]
final <- dcast.data.table(final, ihme_loc_id + year_id + run_num ~ variable, value.var = "value")
final[f_art_rate != 0,f_art_rate := f_art_rate]
final[g_art_rate!= 0,g_art_rate := g_art_rate]

#isolate extension year, calculate scalars
overlap <- final[year_id==extension.year]
overlap[,scale_mort_rate:= ifelse(f_mort_rate == 0, 0, g_mort_rate / f_mort_rate) ]
overlap[,scale_inc_rate:= ifelse(f_inc_rate == 0, 0, g_inc_rate / f_inc_rate) ]
overlap[,scale_prev_rate:= ifelse(f_prev_rate == 0, 0, g_prev_rate / f_prev_rate) ]
overlap[,scale_art_rate:= ifelse(f_art_rate == 0, 0, g_art_rate / f_art_rate) ]
overlap <- overlap[,.SD,.SDcols=c("run_num","scale_mort_rate","scale_inc_rate","scale_prev_rate","scale_art_rate")]

fgbd <- data.table(merge(final,overlap,by=c("run_num"),all=T))
fgbd[,f_mort_rate:=f_mort_rate * scale_mort_rate]
fgbd[,f_inc_rate:=f_inc_rate * scale_inc_rate]
fgbd[,f_prev_rate:=f_prev_rate * scale_prev_rate]
fgbd[f_art_rate != 0,f_art_rate:=f_art_rate * scale_art_rate]
fgbd[f_art_rate >1, f_art_rate := 1]


#take gbd in past, fbd in future
fgbd[year_id<= extension.year,mort_rate:=g_mort_rate]
fgbd[year_id>extension.year,mort_rate:=f_mort_rate]
fgbd[year_id<= extension.year,art_rate:=g_art_rate]
fgbd[year_id>extension.year,art_rate:=f_art_rate]
fgbd[year_id<= extension.year,prev_rate:=g_prev_rate]
fgbd[year_id>extension.year,prev_rate:=f_prev_rate]
fgbd[year_id<= extension.year,inc_rate:=g_inc_rate]
fgbd[year_id>extension.year,inc_rate:=f_inc_rate]

#reshape long on var, wide on draws for disk size
fgbd <- fgbd[,.SD,.SDcols=c("year_id","run_num","mort_rate","inc_rate","prev_rate","art_rate")]

fgbd <- melt.data.table(fgbd,id.vars=c("run_num","year_id"))

#fill tiny number of missing values with 0
missing_percent <- nrow(fgbd[is.na(value)]) / nrow(fgbd)

if (missing_percent < .05) {
  fgbd[is.na(value),value:=1e-10]
}

fgbd[variable=="inc_rate",variable:="Incidence"]
fgbd[variable=="prev_rate",variable:="Prevalence"]
fgbd[variable=="mort_rate",variable:="Mortality"]
fgbd[variable=="art_rate",variable:="ART Coverage"]

fgbd[,ihme_loc_id := c.iso]
fgbd[,location_id := loc_id]
#merging wpp pops back on
fgbd <- merge(fgbd, unique(nat.aggs[variable == "f_mort_rate",list(pop, year_id)]), by = c("year_id"))

###make folders, save
dir.create(paste0(jpath,"FILEPATH/"),showWarnings = F,recursive = T)
write.csv(fgbd,paste0(jpath,"FILEPATH/",c.iso,".csv"))
#calculate national all-age both sex agg sumarries
summary <- fgbd[,.(pop=mean(pop),mean=mean(value),upper=quantile(value,probs=c(.975)),lower=quantile(value,probs=c(.025))),by=.(year_id,location_id,ihme_loc_id,variable)]
dir.create(paste0(jpath,"FILEPATH"),showWarnings = F,recursive = T)
write.csv(summary,paste0(jpath,"FILEPATH/",c.iso,".csv"))


