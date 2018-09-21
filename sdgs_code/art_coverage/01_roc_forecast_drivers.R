#######################################################################################
### Setup
#######################################################################################
rm(list=ls())
gc()
root <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
list.of.packages <- c("data.table","ggplot2","haven","parallel")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
code.dir <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
hpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
cores <- 20
source(paste0(jpath,"FILEPATH/multi_plot.R"))

### Arguments
if(!is.na(commandArgs()[3])) {
  c.fbd_version <- commandArgs()[3]
} else {
  c.fbd_version <- "20170721"
}
c.args <- fread(paste0(code.dir,"hiv_forecasting_inputs/run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]
scenarios <- c("reference", "better", "worse")


print(c.fbd_version)
print(c.draws)

stage.list <- c("stage_2", "stage_1")


source(paste0(hpath, "FILEPATH/shared_functions/get_locations.R"))
loc.table <- get_locations()
island.locs <- c("GRL", "GUM", "MNP", "PRI", "VIR", "ASM", "BMU")
loc.list <- setdiff(loc.table[spectrum == 1, ihme_loc_id], island.locs)
### Paths
#######################################################################################
###Prep Data
#######################################################################################
locs <- data.table(read_dta(paste0(root,"FILEPATH/IHME_GDB_2015_LOCS_6.1.15.dta")))

###Education Age-Standardized both sexes
educ <- fread(paste0(jpath,"FILEPATH/agestd_education_upload.csv"))
educ <- merge(educ,locs,by='location_id')
educ <- educ[ ,.(value=mean(mean_value)),by=.(ihme_loc_id,year_id)]
educ[,variable:="Education"]
educ[,roc_year:=1970]
educ[,value:=value/18]

missing.locs <- setdiff(loc.list, unique(educ$ihme_loc_id))
subnats <- grep("_", missing.locs, value = T)
nats <- unique(tstrsplit(subnats, "_")[[1]])
for(nat in nats) {
  print(nat)
  subs<- grep(nat, subnats, value = T)
  nat.educ <- educ[ihme_loc_id == nat]
  subnat.educ <- rbindlist(lapply(subs, function(loc) {
    sub.educ <- copy(nat.educ)[, ihme_loc_id := loc]
  })) 
  educ <- rbind(educ, subnat.educ)
}

### ART Price
art.price <- fread(paste0(jpath,"FILEPATH/STGPR.csv"))
art.price <- art.price[,.SD,.SDcols=c("ihme_loc_id","year_id","gpr_mean")]
setnames(art.price,"gpr_mean","value")
art.price[,variable:="ART Price"]
art.price[,roc_year:=2005]

missing.locs <- setdiff(loc.list, unique(art.price$ihme_loc_id))
subnats <- grep("_", missing.locs, value = T)
nats <- unique(tstrsplit(subnats, "_")[[1]])
for(nat in nats) {
  print(nat)
  subs<- grep(nat, subnats, value = T)
  nat.art.price <- art.price[ihme_loc_id == nat]
  subnat.art.price <- rbindlist(lapply(subs, function(loc) {
    sub.art.price <- copy(nat.art.price)[, ihme_loc_id := loc]
  })) 
  art.price <- rbind(art.price, subnat.art.price)
}


### LDI 
ldi <-  fread("FILEPATH/national_LDIpc_corrd_with_EDU_20170501.csv")
merge.ldi <- merge(ldi, loc.table[, .(location_id, ihme_loc_id)], by = "location_id", all.x = T)
melt.ldi <- melt(merge.ldi, id.vars = c("location_id", "year_id", "ihme_loc_id"))
ldi <- melt.ldi[,.(value = mean(value)),by=c("ihme_loc_id","year_id")]
ldi[,variable:="LDI"]
ldi[,roc_year:=1990]

# HIV DAH and GHES
# Get ZWE from last years data
zwe_dah <- data.table(read_dta(paste0(jpath,"FILEPATH/HIV_DAH_20160920.dta")))[iso3 == "ZWE"]
zwe_dah[,nat_iso3:= iso3]
#pull in pops to make HIV DAH per capita
pops <- fread(paste0(root,'FILEPATH/WPP_Pops_20150101.csv'))
pops <- data.table(merge(pops,locs,by='location_id'))
pops <- pops[age_group_id > 7 & age_group_id < 15]
pops <- pops[,.(pop=sum(pop)),by=.(ihme_loc_id,year_id)]
pops[,nat_iso3:= ihme_loc_id]
pops[,year:= year_id]
zwe_dah <- data.table(merge(zwe_dah,pops,by=c('year','nat_iso3')))
zwe_dah[,hiv_dah:=hiv_dah_all * 1e9 / pop]
setnames(zwe_dah,"iso3","ihme_loc_id")
setnames(zwe_dah,"year","year_id")
zwe_dah <- zwe_dah[,.SD,.SDcols=c("ihme_loc_id","year_id","hiv_dah")]
setnames(zwe_dah,"hiv_dah","value")
zwe_dah[,variable:="HIV DAH"]
zwe_dah[,roc_year:=2010]

hiv_dah_ghes <- data.table(read_dta(paste0(root, "FILEPATH/GHESpc_HIV_DAHpc_20170710.dta")))
hiv_dah <- hiv_dah_ghes[, .(iso3, year, hiv_dah_per_cap)]
setnames(hiv_dah, c("iso3", "year", "hiv_dah_per_cap"), c("ihme_loc_id", "year_id", "value"))
hiv_dah[,variable:="HIV DAH"]
hiv_dah[,roc_year:=2010]

# Fill in missing dah
missing.locs <- setdiff(loc.list, unique(hiv_dah$ihme_loc_id))
subnats <- grep("_", missing.locs, value = T)
zero.locs <- setdiff(missing.locs, subnats)
ex.dah <- hiv_dah[ihme_loc_id == unique(hiv_dah$ihme_loc_id)[1]]
ex.dah[, value := 0]
for(loc in zero.locs) {
  loc.dt <- copy(ex.dah)[, ihme_loc_id := loc]
  hiv_dah <- rbind(hiv_dah, loc.dt)
}

ghes <- hiv_dah_ghes[, .(iso3, year, ghes_per_cap, total_pop)]
ghes[, ghes := ghes_per_cap * total_pop]
ghes[, total_pop := NULL]
setnames(ghes, c("iso3", "ghes_per_cap"), c("nat_iso3", "ghes_pc"))

#pull in gdppc to backcast ghes to 1970
gdppc <- fread(paste0(root,"FILEPATH/ldi_forecasts_20160921.csv"))
gdppc <- data.table(merge(gdppc,locs,by=c('location_id')))
gdppc[,nat_iso3:= ihme_loc_id]
gdppc[,gdppc:= ldi]
gdppc[,year:= year_id]
gdppc <- gdppc[,.SD,.SDcols= c('nat_iso3','gdppc','year')]

ghes <- merge(ghes,gdppc,by=c('nat_iso3','year'),all=T)
ratios <- ghes[year==1995]
ratios[,ratio:= ghes_pc / gdppc]
ratios <- ratios[,.SD,.SDcols= c('nat_iso3','ratio')]
ghes <- data.table(merge(ghes,ratios,by=c('nat_iso3'),all=T))
ghes[year < 1995,ghes_pc:= gdppc * ratio]
ghes$pred_ghes <- predict(lm(ghes_pc~gdppc,data=ghes[year>1995]),newdata=ghes)
ghes[is.na(ghes_pc),ghes_pc:= pred_ghes]

setnames(ghes,"year","year_id")
setnames(ghes,"nat_iso3","ihme_loc_id")
ghes <- ghes[,.SD,.SDcols=c("ihme_loc_id","year_id","ghes_pc")]
setnames(ghes,"ghes_pc","value")
ghes[,variable:="GHES"]
ghes[,roc_year:=1996]

financ.data <- rbind(ldi, hiv_dah, zwe_dah, ghes)
merge.financ <- merge(financ.data, pops, by = c("ihme_loc_id", "year_id"))
merge.financ[variable != "LDI", value := value * pop]
financ.dt <- copy(merge.financ[, .(ihme_loc_id, year_id, value, variable, roc_year, pop)])
setnames(financ.dt, "pop", "total_pop")


# Split financial inputs for subnationals
missing.locs <- setdiff(loc.list, unique(financ.data$ihme_loc_id))
subnats <- grep("_", missing.locs, value = T)
nats <- unique(tstrsplit(subnats, "_")[[1]])
for(nat in nats) {
  print(nat)
  subs<- grep(nat, subnats, value = T)
  nat.financ <- financ.dt[ihme_loc_id == nat]
  subnat.financ <- rbindlist(lapply(subs, function(loc) {
    sub.financ <- copy(nat.financ)[, ihme_loc_id := loc]
  })) 
  subnat.counts <- rbindlist(mclapply(subs, function(loc) {
    for(stage in stage.list) {
      path <- paste0("FILEPATH/", loc, "_ART_data.csv")
      if(file.exists(path)) break
    }
    dt <- fread(path)[year == 2016, .(run_num, pop_neg, pop_lt200, pop_200to350, pop_gt350, pop_art)]
    dt[, pop_hiv := pop_lt200 + pop_200to350 + pop_gt350 + pop_art]
    dt[, pop := pop_lt200 + pop_200to350 + pop_gt350 + pop_art + pop_neg]
    dt[, ihme_loc_id := loc]
    sum.dt <- dt[, lapply(.SD, sum), by = .(ihme_loc_id, run_num), .SDcols = c("pop_hiv", "pop")]
    mean.dt <- sum.dt[, lapply(.SD, mean), by = "ihme_loc_id", .SDcols = c("pop_hiv", "pop")]
    return(mean.dt)
  }, mc.cores = cores))
  subnat.props <- data.table(cbind(subnat.counts[, .(ihme_loc_id)], prop.table(as.matrix(subnat.counts[, .(pop_hiv, pop)]), 2)))
  combined.financ <- merge(subnat.financ, subnat.props, by = "ihme_loc_id", allow.cartesian = T)
  combined.financ[variable == "HIV DAH", total_pop := total_pop * pop]  
  combined.financ[variable == "HIV DAH", value := (value * pop_hiv) / total_pop]
  combined.financ[variable == "GHES", value := value / total_pop]
  financ.data <- rbind(financ.data, combined.financ[, .(ihme_loc_id, year_id, value, variable, roc_year)])
}

# Address remaining missing locations
missing.locs <- setdiff(loc.list, unique(financ.data$ihme_loc_id))


###Child ART
proc_dat <- function(iso) {
  for(stage in stage.list) {
    path <- paste0("FILEPATH/", iso, "_coverage.csv")
    if(file.exists(path)) break
  }
  print(iso)
  data <- fread(path)
  data <- data[,.(coverage=mean(coverage),eligible_pop=mean(eligible_pop)),by=.(age,sex,year,type)]
  data$ihme_loc_id <- iso
  return(data)
}
spec.list <- mclapply(loc.list, proc_dat, mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))
spec <- rbindlist(spec.list)

child.art <- spec[type=="ART" & age =="child" & sex =="female"]
child.art[,value := coverage / eligible_pop]
child.art[eligible_pop==0,value := 0]
child.art[value > 1,value := 1]
setnames(child.art,"year","year_id")
child.art <- child.art[,.SD,.SDcols=c("ihme_loc_id","year_id","value","sex")]
child.art[,variable:="Child ART"]
child.art[,roc_year:=2010]
child.art[,sex:="Both"]

###Cotrim
cotrim <- spec[type=="CTX" & age =="child" & sex=="female"]
cotrim[,value := coverage / eligible_pop]
cotrim[eligible_pop==0,value := 0]
setnames(cotrim,"year","year_id")
cotrim <-cotrim[,.SD,.SDcols=c("ihme_loc_id","year_id","value")]
cotrim[,variable:="Cotrim"]
cotrim[,roc_year:=2010]


###PMTCT
pmtct <- spec[(!(type %in% c("CTX","ART"))) & age =="adult" & sex=="female"]

pmtct<- dcast.data.table(pmtct,ihme_loc_id+year+eligible_pop~type,value.var="coverage")
#Prenatal -- proportional rake all elements if over one
pmtct[,prenatal:=  singleDoseNevir + dualARV + optionA  + optionB  + tripleARTdurPreg  + tripleARTbefPreg]
#rake 
pmtct[,rake_prenatal := prenatal /eligible_pop]
for (c.var in c("dualARV","optionA", "optionB","singleDoseNevir", "tripleARTbefPreg", "tripleARTdurPreg")) {
pmtct[rake_prenatal>1,paste0(c.var):=get(c.var)/rake_prenatal]  
}

#recalculate
pmtct[,prenatal:=  singleDoseNevir + dualARV + optionA  + optionB  + tripleARTdurPreg  + tripleARTbefPreg]
#Postnatal -- keep TripleARTbefPreg same and proportional reduce others
pmtct[,postnatal := optionA_BF  + optionB_BF + tripleARTdurPreg  + tripleARTbefPreg]
#calculate number of excess treatments
pmtct[,postnatal_excess := (postnatal - eligible_pop)]
#calculate sum of non- TripleArtdurPreg
pmtct[,sum_nonTrip := (optionA_BF  + optionB_BF + tripleARTbefPreg)]
#calcualte what this sum should be post raking
pmtct[,new_noTrip := sum_nonTrip - postnatal_excess]
#calcualte raking factor
pmtct[,prop_reduce := sum_nonTrip / new_noTrip]
#rake components
for (c.var in c("optionA_BF","optionB_BF","tripleARTbefPreg")) {
  pmtct[postnatal>eligible_pop,paste0(c.var):=get(c.var)/prop_reduce]  
}


#re-calculate sum
pmtct[,postnatal := optionA_BF  + optionB_BF + tripleARTdurPreg  + tripleARTbefPreg]
#generate coverage 
pmtct.final <- pmtct[,.SD,.SDcols=c("ihme_loc_id","year","prenatal","postnatal","eligible_pop")]
pmtct.final <- melt.data.table(pmtct.final,id.vars=c("ihme_loc_id","year","eligible_pop"),value.name = "num")
                               
pmtct.final[,value:=num/eligible_pop]
pmtct.final[eligible_pop==0,value:=0]

pmtct.final[,variable:=paste0("pmtct ",variable)]
setnames(pmtct.final,"year","year_id")
pmtct.final <-pmtct.final[,.SD,.SDcols=c("ihme_loc_id","year_id","value","variable")]
pmtct.final[,roc_year:=2010]




#######################################################################################
###Fit Rate of Change Models
#######################################################################################
data <- rbind(art.price,financ.data,child.art,cotrim,pmtct.final,educ,fill=T)
data[,sex:=NULL]

data<- data[year_id == extension.year | year_id == roc_year]
data[,n_years:=extension.year-roc_year]
data[year_id == roc_year,year_id:=9999]

data.w <- dcast.data.table(data,ihme_loc_id+variable+n_years~year_id)

setnames(data.w,"9999","roc_year")
setnames(data.w,paste0(extension.year),"extension_year")



data.w[variable %in% c("Child ART","Cotrim","pmtct postnatal","pmtct prenatal","Education"),roc:= (boot::logit(extension_year) - boot::logit(roc_year)) / (n_years)]
data.w[!(variable %in% c("Child ART","Cotrim","pmtct postnatal","pmtct prenatal","Education")),roc:= (log(extension_year) - log(roc_year)) / (n_years)]
data.w <- data.w[!is.na(extension_year) & is.finite(roc) & !is.na(roc)]
  
summ.rocs <- data.w[!is.na(roc) & is.finite(roc),
                    .(p05=(quantile(roc,probs=.05,na.rm=T)),
                     p10=(quantile(roc,probs=.1,na.rm=T)),
                     p20=(quantile(roc,probs=.2,na.rm=T)),
                     p45=(quantile(roc,probs=.45,na.rm=T)),
                     p55=(quantile(roc,probs=.55,na.rm=T)),
                     p60=(quantile(roc,probs=.6,na.rm=T)),
                     p70=(quantile(roc,probs=.7,na.rm=T)),
                     p80=(quantile(roc,probs=.8,na.rm=T)),
                     p90=(quantile(roc,probs=.9,na.rm=T)),
                     p95=(quantile(roc,probs=.95,na.rm=T))
),by=.(variable)]

write.csv(summ.rocs,paste0(jpath,"FILEPATH/forecasted_input_rocs.csv"))


#Load Past Data
data <- rbind(art.price,financ.data,child.art,cotrim,pmtct.final,educ,fill=T)
data[,sex:=NULL]

#make template, merge
data <- data[order(variable,ihme_loc_id,year_id)]
extension.dt <- data[year_id == extension.year]
n = 2040 - extension.year
replicated.dt <- extension.dt[rep(seq_len(nrow(extension.dt)), n), ]
for (i in 1:n) {
  replicated.dt[seq(((i-1)*nrow(extension.dt) + 1), i*nrow(extension.dt)), year_id := (extension.year + i)]
}
replicated.dt <- replicated.dt[,.SD,.SDcols=c("ihme_loc_id","year_id","variable")]
data <- merge(data,replicated.dt,all=T,by=c("ihme_loc_id","year_id","variable"))
data <- data[order(variable,ihme_loc_id,year_id)]

#Make Predictions
make_pred <- function(c.draw,data) {
print(c.draw)  
data[,roc:=runif(n=nrow(data),min=roc_low,max=roc_up)]
data[,pred:=value]  
data <- data[order(variable,ihme_loc_id,year_id)]
for (c.yr in seq(extension.year+1,2040,1)) {
  data[!(variable %in% c("Child ART","Cotrim","pmtct postnatal","pmtct prenatal","Education")),growth:=(shift(pred)*roc)+shift(pred)]   
  data[(variable %in% c("Child ART","Cotrim","pmtct postnatal","pmtct prenatal","Education")),growth:=boot::inv.logit(boot::logit(shift(pred)) + roc)]   
  data[year_id==c.yr,pred:=growth]  
}
data[,draw:=c.draw]
return(data)
}

 
scen.list <- list()
scen.i <- 1
for (c.scenario in scenarios) {
  fit.data <- merge(data,summ.rocs,by=c("variable"),all=T)

  if (c.scenario == "reference") {
    setnames(fit.data,"p45","roc_low")  
    setnames(fit.data,"p55","roc_up") 
  }
  if (c.scenario == "better") {
    setnames(fit.data,"p80","roc_low")  
    setnames(fit.data,"p90","roc_up")
    fit.data[variable=="ART Price",roc_low:=p10]
    fit.data[variable=="ART Price",roc_up:=p20]
  }
  if (c.scenario == "worse") {
    setnames(fit.data,"p10","roc_low")  
    setnames(fit.data,"p20","roc_up")
    fit.data[variable=="ART Price",roc_low:=p80]
    fit.data[variable=="ART Price",roc_up:=p90]
  }  
  n.draws <- c.draws - 1

  pred.list <- mclapply(0:n.draws,make_pred,data=fit.data,mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores), mc.preschedule=F)
  preds <-rbindlist(pred.list)
  preds <-  preds[,.SD,.SDcols=c("ihme_loc_id","year_id","variable","pred","value","draw","roc","roc_year")]
  preds[,scenario:=c.scenario]
  scen.list[[scen.i]] <- preds
  scen.i <- scen.i + 1
}
pred.draws <- rbindlist(scen.list)


pred.summ <- pred.draws[,.(pred_mean=mean(pred,na.rm=T)),
                           by=.(ihme_loc_id,year_id,variable,value,roc_year,scenario)]

#shift forecasts if we have a pre_existing forecast set to respect
shifts <- pred.summ[scenario=='reference' & !is.na(value)]
shifts[,shift:=pred_mean/value]
shifts <- shifts[,.SD,.SDcols=c("ihme_loc_id","year_id","variable","shift")]

pred.summ <- merge(pred.summ,shifts,by=c("ihme_loc_id","year_id","variable"),all=T)
pred.summ[is.na(shift) | !is.finite(shift),shift:=1]
pred.summ[,pred_mean:=pred_mean/shift]
#######################################################################################
###Graph
#######################################################################################
make_plot <- function(c.iso,data=pred.summ,fbd_version=c.fbd_version){

pdf(paste0(jpath,"FILEPATH/",c.iso,".pdf"),width=12,height=8)  

gg <- ggplot(data[ihme_loc_id==c.iso & variable != "Education" ]) + geom_line(aes(x=year_id,y=pred_mean,color=scenario)) +
    facet_wrap(~variable,scales='free',nrow=2) + theme_classic() +
    geom_vline(aes(xintercept=2016),linetype='longdash') + 
    geom_vline(aes(xintercept=roc_year),linetype='longdash') + geom_line(data=data[ihme_loc_id==c.iso & year_id <= extension.year & variable != "Education" ],aes(x=year_id,y=pred_mean)) +
    labs(title=c.iso,y="",x="Year") + scale_color_manual(name="Scenario",values = c("worse" = "red","reference" = "green", "better" = "blue"))
print(gg)
dev.off()  
  
}
dir.create(showWarnings = F, path=paste0(jpath,"FILEPATH"),recursive = T)

mclapply(unique(pred.summ[variable != "Education",ihme_loc_id]),make_plot,mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))
system(command=paste0("qsub -pe multi_slot 4 -P proj_forecasting FILEPATH/pdfappend.sh FILEPATH/ ",
                      "FILEPATH/ "," inputs"))


#######################################################################################
###Save Files
#######################################################################################
pred.summ <- pred.summ[,.SD,.SDcols=c("ihme_loc_id","year_id","variable","scenario","pred_mean")]
dir.create(showWarnings = F, path=paste0(jpath,"FILEPATH/",c.fbd_version),recursive = T)
write.csv(pred.summ,paste0(jpath,"FILEPATH/forecasted_inputs.csv"))

#save child ART and Cotrim 
child <- pred.summ[variable %in% c("Child ART","Cotrim")]
setnames(child,"year_id","year")
#make percent
child[,pred_mean:=pred_mean*100]
child <- dcast.data.table(child,ihme_loc_id+year+scenario~variable)
setnames(child,"Child ART","ART_cov_pct")
setnames(child,"Cotrim","Cotrim_cov_pct")
child[,ART_cov_num:=0]
child[,Cotrim_cov_num:=0]

save_child <- function(c.iso,c.scenario,data) {
print(c.iso)
temp <- data[ihme_loc_id == c.iso & scenario==c.scenario]  
temp[,ihme_loc_id:=NULL]  
temp[,scenario:=NULL]
dir.create(showWarnings = F, path=paste0(jpath,
"FILEPATH",
 c.fbd_version,"_",c.scenario,"/"),recursive = T)

write.csv(temp,paste0(jpath,
"FILEPATH/",c.iso,".csv"),row.names=F)
}
save_child("ZMB",c.scenario="reference",data=child)
for (scen in c("reference","better","worse")) {
mclapply(unique(pred.summ$ihme_loc_id),save_child,mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores),
         data=child,c.scenario=scen)
}

#split PMTCT and save
pmtct[,c("prenatal","rake_prenatal","postnatal","postnatal_excess","sum_nonTrip","new_noTrip","prop_reduce"):=NULL]
pmtct.pieces <- melt.data.table(pmtct,id.vars=c("ihme_loc_id","year","eligible_pop"))
pmtct.pieces[,coverage:=value/eligible_pop]
pmtct.pieces[is.na(coverage),coverage:=0]
pmtct.pieces[coverage>1,coverage:=1]
pmtct.pieces[,value:=NULL]
pmtct.pieces <- dcast.data.table(pmtct.pieces,ihme_loc_id+year+eligible_pop~variable)

pmtct.summs <- dcast.data.table(pred.summ[variable %in% c('pmtct postnatal','pmtct prenatal')],ihme_loc_id+year_id+scenario~variable)
setnames(pmtct.summs,"year_id","year")
setnames(pmtct.summs,"pmtct prenatal","forecast_meanprenatal")
setnames(pmtct.summs,"pmtct postnatal","forecast_meanpostnatal")
pmtct.pieces <- merge(pmtct.pieces,pmtct.summs,all=T,by=c("year","ihme_loc_id"))

#The proportion of prenatal sum going to TripleARTdurPreg (TripleARTdurPreg / prenatal sum) is increased by .1 each year until it hits 1.0
pmtct.pieces[,prop_tripleARTdurPreg := tripleARTdurPreg / forecast_meanprenatal]
pmtct.pieces[is.na(prop_tripleARTdurPreg),prop_tripleARTdurPreg:=0]
pmtct.pieces <- pmtct.pieces[order(ihme_loc_id,scenario,year)]
for (c.yr in extension.year:2040) {
pmtct.pieces[,temp:=shift(prop_tripleARTdurPreg)]
pmtct.pieces[year==c.yr,prop_tripleARTdurPreg:=temp+.1]
}
pmtct.pieces[prop_tripleARTdurPreg>1,prop_tripleARTdurPreg:=1]
pmtct.pieces[year>extension.year,tripleARTdurPreg := forecast_meanprenatal * prop_tripleARTdurPreg]
pmtct.pieces[,prenatal_remainder := forecast_meanprenatal - tripleARTdurPreg]
pmtct.pieces[prop_tripleARTdurPreg == 1,prenatal_remainder := 0]

#loop over prenatal components
for (c.var in c("dualARV", "optionA","optionB","singleDoseNevir", "tripleARTbefPreg")) {
  #calculate the proportion of the remainder held by each other series
  pmtct.pieces[,remainder_temp := get(c.var) / prenatal_remainder]
  pmtct.pieces[is.na(remainder_temp),remainder_temp := 0]
  # hold proportion constant in future
  for (c.yr in extension.year:2040) {
    pmtct.pieces[,temp:=shift(remainder_temp)]
    pmtct.pieces[year==c.yr,paste0(c.var):=temp * prenatal_remainder]
  }  
}

#calculate remainder of postnatal
pmtct.pieces[,postnatal_remainder := forecast_meanpostnatal -( tripleARTdurPreg + tripleARTbefPreg)]
pmtct.pieces[postnatal_remainder < 0 | is.na(postnatal_remainder),postnatal_remainder:=0]
#loop over prenatal components not included in prenatal
for (c.var in c("optionA_BF","optionB_BF"))  {
#calculate the proportion of the remainder held by each other series
pmtct.pieces[,remainder_temp:= get(c.var)/ postnatal_remainder]
pmtct.pieces[remainder_temp < 0 | is.na(remainder_temp),remainder_temp := 0]
pmtct.pieces[remainder_temp>1,remainder_temp:=1]

for (c.yr in extension.year:2040) {
  pmtct.pieces[,temp:=shift(remainder_temp)]
  pmtct.pieces[year==c.yr,paste0(c.var):=temp * postnatal_remainder]
}  
}

#reshape long graph, set to below zero just in case
pmtct.pieces[,c("eligible_pop","prop_tripleARTdurPreg","temp","prenatal_remainder","remainder_temp","postnatal_remainder"):=NULL]
pmtct.pieces <- melt.data.table(pmtct.pieces,id.vars=c("ihme_loc_id","year","scenario"))
pmtct.pieces[value>1,value:=1]
#graph pmtct pieces
make_plot <- function(c.iso,data=pmtct.pieces,fbd_version=c.fbd_version){
  
pdf(paste0(jpath,"FILEPATH/",c.iso,".pdf"),width=12,height=8)  
  
  gg <- ggplot(data[ihme_loc_id==c.iso]) + geom_line(aes(x=year,y=value,color=scenario)) +
    facet_wrap(~variable,scales='free',nrow=2) + theme_classic() +
    geom_vline(aes(xintercept=2016),linetype='longdash') + 
     geom_line(data=data[ihme_loc_id==c.iso & year <= extension.year],aes(x=year,y=value)) +
    labs(title=c.iso,y="",x="Year") + scale_color_manual(name="Scenario",values = c("worse" = "red","reference" = "green", "better" = "blue"))
  print(gg)
  dev.off()  
}
dir.create(showWarnings = F, path=paste0(jpath,"FILEPATH"),recursive = T)

mclapply(unique(pred.summ$ihme_loc_id),make_plot,mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores))
system(command=paste0("qsub -pe multi_slot 4 -P proj_forecasting FILEPATH ",
                      "FILEPATH "," pmtct_pieces"))


#make percent
pmtct.pieces[,value:=value*100]

#reshape wide, save for Spectrum
pmtct.pieces[is.na(value),value:=0]
pmtct.pieces <- dcast.data.table(pmtct.pieces,ihme_loc_id+year+scenario~variable)
pmtct.pieces[,forecast_meanpostnatal:=NULL]
pmtct.pieces[,forecast_meanprenatal:=NULL]

setnames(pmtct.pieces,old=c("optionA","optionA_BF","optionB","optionB_BF"),
         new=c("prenat_optionA","postnat_optionA","prenat_optionB","postnat_optionB"))



for (c.var in names(pmtct.pieces)[4:length(names(pmtct.pieces))]) {
setnames(pmtct.pieces,paste0(c.var),paste0(c.var,"_pct"))  
pmtct.pieces[,paste0(c.var,"_num"):=0]  
}


save_child <- function(c.iso,c.scenario,data) {
  print(c.iso)
  temp <- data[ihme_loc_id == c.iso & scenario==c.scenario]  
  temp[,ihme_loc_id:=NULL]  
  temp[,scenario:=NULL]
  dir.create(showWarnings = F, path=paste0(jpath,
 "FILEPATH",
 c.fbd_version,"_",c.scenario,"/"),recursive = T)
  
  write.csv(temp,paste0(jpath,
  "FILEPATH/",c.iso,".csv"),row.names=F)
}
for (scen in c("reference","better","worse")) {
  mclapply(unique(pred.summ$ihme_loc_id),save_child,mc.cores=ifelse(Sys.info()[1]=="Windows", 1, cores),
           data=pmtct.pieces,c.scenario=scen)
}

