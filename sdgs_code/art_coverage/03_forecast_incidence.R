rm(list=ls())
gc()
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
list.of.packages <- c("data.table","ggplot2","parallel","gtools","haven")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
code.dir <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH"))
cores <- 5
source(paste0(jpath,"FILEPATH/multi_plot.R"))

### Arguments
c.fbd_version <- commandArgs()[3]
c.iso <- commandArgs()[4]
c.scenario <- commandArgs()[5]
source <- 1
local_test <- 0

secular_trend <- "yes"


if (is.na(commandArgs()[3])) {
  c.fbd_version <- "20170721"
  c.iso <- "AGO"
  c.scenario <- "reference"
}

c.args <- fread(paste0(code.dir,"FILEPATH/run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]


##load data
if (local_test == 1) {
adult.art <- fread(paste0(jpath,"FILEPATH/",c.iso,".csv"))
}else {
adult.art <- fread(paste0("FILEPATH/",c.iso,".csv"))
}

adult.art <- melt.data.table(adult.art[,.SD,.SDcols=c(paste0("art_coverage_",0:999),"iso3","age","sex","year")],
                             id.vars=c("iso3","age","sex","year"))
adult.art[,age:=floor(age/5)*5]
adult.art[,draw:=as.numeric(gsub(as.character(variable),pattern="art_coverage_",replacement=""))]
adult.art <- adult.art[,.(cov=mean(value)),by=.(iso3,age,sex,year,draw)]

#child ART
child.art <- fread(paste0(jpath,"FILEPATH/forecasted_inputs.csv"))
child.art <- child.art[variable == "Child ART" & ihme_loc_id==c.iso & scenario==c.scenario]
setnames(child.art,old=c("pred_mean","ihme_loc_id","year_id"),new=c("child_art","iso3","year"))
child.art <- child.art[,.SD,.SDcols=c("iso3","year","child_art")]

##prep counterfactual hazard rate of change trends
high_prev <- fread(paste0(jpath,"FILEPATH/high_prev_locs.csv" ))
if(c.iso %in% high_prev$x){
  rocs <- fread(paste0(jpath,"FILEPATH/hazard_counterfactual_roc_distribution_high.csv"))
}else{
  rocs <- fread(paste0(jpath,"FILEPATH/hazard_counterfactual_roc_distribution_low.csv"))
}

if (c.scenario == "reference") {
setnames(rocs,"p45","roc_low")  
setnames(rocs,"p55","roc_up")  
}
if (c.scenario == "worse") {
  setnames(rocs,"p80","roc_low")  
  setnames(rocs,"p90","roc_up")  
}
if (c.scenario == "better") {
  setnames(rocs,"p10","roc_low")  
  setnames(rocs,"p20","roc_up")  
}
rocs <- rocs[,.SD,.SDcols=c("age","sex","roc_low","roc_up")]

if (secular_trend=="no"){
rocs[,roc_low:=0]  
rocs[,roc_up:=0]  
}

##load incidence
if (local_test == 1) {
inc <- fread(paste0(jpath,"FILEPATH/",c.iso,"_ART_data.csv"))
}else {
stage.list <- c("stage_2", "stage_1")
  for(stage in stage.list) {
    path <- paste0("FILEPATH/", c.iso, "_ART_data.csv")
    if(file.exists(path)) break
  }
inc <- fread(path)
}

inc[,iso3:=c.iso]
inc[,population:=pop_neg+pop_lt200+pop_200to350+pop_gt350+pop_art]
inc <- inc[,.SD,.SDcols=c("iso3","run_num","year","sex","age","new_hiv","suscept_pop","population")]
inc <- inc[year<extension.year+1]
inc[,inc_rate := (new_hiv / population)]
inc[,inc_hazard_rate:=new_hiv/suscept_pop]
inc <- inc[,.SD,.SDcols=c("iso3","run_num","year","sex","age","inc_rate","inc_hazard_rate")]

#create blank vals for 2016-2040
extension.dt <- inc[year == extension.year]
n = 2040 - extension.year
replicated.dt <- extension.dt[rep(seq_len(nrow(extension.dt)), n), ]
for (i in 1:n) {
  replicated.dt[seq(((i-1)*nrow(extension.dt) + 1), i*nrow(extension.dt)), year := (extension.year + i)]
}
inc <- data.table(rbind(inc[year <= extension.year], replicated.dt))
inc[year > extension.year,inc_rate := NaN]
inc[year > extension.year,inc_hazard_rate := NaN]

#merge inputs
setnames(inc,"run_num","draw")
inc <- merge(inc,adult.art,by=c("iso3","age","sex","year","draw"),all.x=T)

inc <- merge(inc,child.art,by=c("iso3","year"),all.x=T)
inc[is.na(cov),cov:=child_art]

inc <- merge(inc,rocs,by=c("age","sex"),all=T)
inc[is.na(roc_up),roc_up:=0]
inc[is.na(roc_low),roc_low:=0]


#####################################################
### Make forecasts
#######################################################
inc[,vir_sup:=runif(n=nrow(inc),min=.6,max=.8)]
inc[,roc:=runif(n=nrow(inc),min=roc_low,max=roc_up)]
inc[,notrt_inc_hazard_rate := inc_hazard_rate / (1 - (cov* vir_sup))]

inc <- inc[order(iso3,sex,age,draw,year)]
for (c.yr in seq(extension.year+1,2040,1)) {
inc[,growth:=(shift(notrt_inc_hazard_rate)*roc)+shift(notrt_inc_hazard_rate)]    
inc[year==c.yr,notrt_inc_hazard_rate:=growth]  
}
inc[,temp:=((notrt_inc_hazard_rate) * (1 - (cov* vir_sup)))]
inc[year>extension.year,inc_hazard_rate:=temp]
inc[,temp:=NULL]
###cast wide on draw
inc.w <- dcast.data.table(inc[,.SD,.SDcols=c("iso3","draw","year","sex","age","cov","notrt_inc_hazard_rate","inc_hazard_rate")]
,iso3+sex+age+year~draw,value.var = c("cov","notrt_inc_hazard_rate","inc_hazard_rate"))

#fill small # of nans with zeroes
inc[is.na(inc_hazard_rate),inc_hazard_rate:=0]
inc[is.na(notrt_inc_hazard_rate),notrt_inc_hazard_rate:=0]

#cap hazard at max value in past
maxs <- inc[year<2016]
maxs <- maxs[,.(inc_hazard_max=max(inc_hazard_rate),
               notrt_inc_hazard_rate_max=max(notrt_inc_hazard_rate)),by=.(age,sex,iso3,draw)]

inc <- merge(inc,maxs,by=c("age","sex","iso3","draw"))
inc[inc_hazard_rate>inc_hazard_max,inc_hazard_rate:=inc_hazard_max]
###save forecasts
dir.create(showWarnings = F, path=paste0(jpath,"FILEPATH"),recursive = T)
dir.create(showWarnings = F, path=paste0(jpath,"FILEPATH"),recursive = T)

inc.means <- inc[,.(inc_hazard_rate=mean(inc_hazard_rate),inc_hazard_rate_upper=quantile(inc_hazard_rate,probs=.975),inc_hazard_rate_lower=quantile(inc_hazard_rate,probs=.025),
                    notrt_inc_hazard_rate=mean(notrt_inc_hazard_rate),notrt_inc_hazard_rate_upper=quantile(notrt_inc_hazard_rate,probs=.975),notrt_inc_hazard_rate_lower=quantile(notrt_inc_hazard_rate,probs=.025),
                    cov=mean(cov),cov_upper=quantile(cov,probs=.975),cov_lower=quantile(cov,probs=.025),
                    roc=mean(roc),roc_upper=quantile(roc,probs=.975),roc_lower=quantile(roc,probs=.025)),by=.(age,sex,year,iso3)]

write.csv(inc.means,paste0(jpath,"FILEPATH/",c.iso,".csv"))
write.csv(inc.w,paste0(jpath,"FILEPATH/",c.iso,".csv"))

##create spectrum input
pops <- fread(paste0(jpath,"FILEPATH/pops_reference.csv"))
#extend ts for missing places 1950-1979
extender <- pops[iso3 %in% c("AND","DMA","MHL") & year==1980]
for (i in seq(1950,1979)) {
temp <-copy(extender)
temp[,year:=i]
pops <- rbind(pops,temp)  
}
pops <- pops[order(iso3,sex,age,year)]

for (i in 1:10) {
pops[is.na(pop),pop:=shift(pop)]  
}
spec.inc <- inc[age %in% seq(15,45,5)]
spec.inc <- spec.inc[,.SD,.SDcols=c("iso3","age","sex","year","draw","inc_hazard_rate")]
setnames(pops,"iso3","nat_iso3")
spec.inc[,nat_iso3:=substr(iso3,1,3)]
spec.inc <- merge(pops,spec.inc,by=c("age","sex","year","nat_iso3"))
spec.inc <- spec.inc[,.(inc_hazard_rate=weighted.mean(x=inc_hazard_rate,w=pop) * 100),by=.(iso3,year,draw)]
spec.inc <- dcast.data.table(spec.inc,iso3+year~draw)
spec.inc[,iso3:=NULL]

# replace missing draws
missing.draws <- setdiff(paste0(1:1000), names(spec.inc))
for(draw in missing.draws) {
  replace.draw <- sample(setdiff(paste0(1:1000), missing.draws), 1)
  spec.inc[, (draw) := get(replace.draw)]
}

setnames(spec.inc, paste0(1:1000), paste0("draw",1:1000))

dir.create(showWarnings = F, path=paste0(jpath,"FILEPATH/"),recursive = T)
write.csv(spec.inc,paste0(jpath,"FILEPATH/",c.iso,"_SPU_inc_draws.csv"),row.names=F)


