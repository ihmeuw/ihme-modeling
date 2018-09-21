rm(list=ls())
gc()
root <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
code.dir <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
list.of.packages <- c("data.table","ggplot2","haven","parallel")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
hpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH", "/")
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
cores <- 20
source(paste0(jpath, "FILEPATH/multi_plot.R"))

### Arguments
if(!is.na(commandArgs()[3])) {
  c.fbd_version <- commandArgs()[3]
} else {
  c.fbd_version <- "20170727"
}

c.args <- fread(paste0(code.dir,"FILEPATH/run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]

### Functions
source(paste0(hpath, "FILEPATH/get_locations.R"))

### Tables
loc.table <- get_locations()

#######################################################################################
###Counterfactual Hazard Rate of Change
#######################################################################################
#function to generate rate of change in counterfactual hazard
proc_dat <- function(iso) {
  for(stage in stage.list) {
    path <- paste0("FILEPATH/", iso, "_ART_data.csv")
    if(file.exists(path)) break
  }
  print(iso)
  data <- fread(path)
  data[,population:=pop_neg+pop_lt200+pop_200to350+pop_gt350+pop_art]
  mean_data <- data[,list(population = sum(population), pop_neg = sum(pop_neg)), by = c("year", "run_num")]
  mean_data <- mean_data[,list(population = mean(population), pop_neg = mean(pop_neg)), by = "year"]
  mean_data[,prev := (population - pop_neg)/population]
  max_prev <- max(mean_data$prev)
  
  data <- data[year %in% c(2011,2016)]
  data[,population:=pop_neg+pop_lt200+pop_200to350+pop_gt350+pop_art]
  data[,inc_rate := (new_hiv / population)]
  data[,inc_hazard_rate:=new_hiv/suscept_pop]
  data[,vir_sup:=runif(n=nrow(data),min=.6,max=.8)]
  data[,cov:=pop_art/(pop_lt200+pop_200to350+pop_gt350+pop_art)]
  data[,notrt_inc_hazard_rate := inc_hazard_rate / (1 - (cov* vir_sup))]
  data$iso3 <- iso
  
  data <- data[,.(notrt_inc_hazard_rate=mean(notrt_inc_hazard_rate)),by=.(age,sex,year,iso3)]
  w.dat <- dcast.data.table(data,sex+age+iso3~year,value.var='notrt_inc_hazard_rate')
  setnames(w.dat,"2011","v2011")
  setnames(w.dat,"2016","v2016")
  w.dat[,roc:=(log(v2016/v2011))/5]
  w.dat[,max := max_prev]
  return(w.dat[!is.na(roc),.SD,.SDcols=c("sex","age","iso3","roc","v2016", "max")])
}

loc.list <- loc.table[spectrum == 1, ihme_loc_id]
stage.list <- c("stage_2", "stage_1")
spec.list <- mclapply(loc.list, proc_dat, mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))
rocs <- rbindlist(spec.list)

#splitting by prevalence >.005 
high_prev <- unique(rocs[max >.005,iso3])
rocs_high <- rocs[iso3 %in% high_prev,]
rocs_low <- rocs[!iso3 %in% high_prev,]

#summarize distribution by age and sex and region
summ.rocs.high <- rocs_high[,.(p05=(quantile(roc,probs=.05,na.rm=T)),
                                                 p10=(quantile(roc,probs=.1,na.rm=T)),
                                                 p20=(quantile(roc,probs=.20,na.rm=T)),
                                                 p25=(quantile(roc,probs=.25,na.rm=T)),
                                                 p35=(quantile(roc,probs=.35,na.rm=T)),
                                                 p45=(quantile(roc,probs=.40,na.rm=T)),
                                                 p55=(quantile(roc,probs=.50,na.rm=T)),
                                                 p65=(quantile(roc,probs=.65,na.rm=T)),
                                                 p75=(quantile(roc,probs=.75,na.rm=T)),
                                                 p80=(quantile(roc,probs=.80,na.rm=T)),
                                                 p90=(quantile(roc,probs=.90,na.rm=T)),
                                                 p95=(quantile(roc,probs=.95,na.rm=T))
),by=.(age,sex)]

summ.rocs.low <- rocs_low[,.(p05=(quantile(roc,probs=.05,na.rm=T)),
                                                 p10=(quantile(roc,probs=.1,na.rm=T)),
                                                 p20=(quantile(roc,probs=.20,na.rm=T)),
                                                 p25=(quantile(roc,probs=.25,na.rm=T)),
                                                 p35=(quantile(roc,probs=.35,na.rm=T)),
                                                 p45=(quantile(roc,probs=.40,na.rm=T)),
                                                 p55=(quantile(roc,probs=.50,na.rm=T)),
                                                 p65=(quantile(roc,probs=.65,na.rm=T)),
                                                 p75=(quantile(roc,probs=.75,na.rm=T)),
                                                 p80=(quantile(roc,probs=.80,na.rm=T)),
                                                 p90=(quantile(roc,probs=.90,na.rm=T)),
                                                 p95=(quantile(roc,probs=.95,na.rm=T))
),by=.(age,sex)]

dir.create(paste0(jpath,"FILEPATH/",c.fbd_version), showWarnings = F)
write.csv(summ.rocs.low,paste0(jpath,"FILEPATH/hazard_counterfactual_roc_distribution_low.csv"),row.names=F)
write.csv(summ.rocs.high,paste0(jpath,"FILEPATH/hazard_counterfactual_roc_distribution_high.csv"),row.names=F)
write.csv(high_prev, paste0(jpath,"FILEPATH/high_prev_locs.csv"),row.names=F)
