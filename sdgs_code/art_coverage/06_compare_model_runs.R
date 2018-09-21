rm(list=ls())
gc()
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
list.of.packages <- c("data.table","ggplot2","parallel","gtools","haven")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
rm(list=ls())
gc()
user <- "USERNAME"
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
hpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
list.of.packages <- c("data.table","ggplot2","parallel","gtools","haven")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
lapply(list.of.packages, require, character.only = TRUE)
jpath <- ifelse(Sys.info()[1]=="Windows", "FILEPATH", "FILEPATH")
cores <- 5
source(paste0(jpath,"FILEPATH/multi_plot.R"))
locs  <-fread(paste0(jpath,'FILEPATH/GBD_2016_locs_20161114.csv'))

c.fbd_version <- "20170721"
c.args <- fread(paste0(hpath,"FILEPATH/run_versions.csv"))
c.args <- c.args[fbd_version==c.fbd_version]
c.gbd_version <- c.args[["gbd_version"]]
extension.year <- c.args[["extension_year"]]
c.draws <- c.args[["draws"]]
dah.scalar <- c.args[["dah_scalar"]]
if(dah.scalar != 0) {
  dah_scenario <- T
  c.ref <- c.args[["reference_dir"]]
}

### Functions
source(paste0(hpath, "FILEPATH/get_locations.R"))
loc.table <- get_locations()
island.locs <- c("GRL", "GUM", "MNP", "PRI", "VIR", "ASM", "BMU")
loc.table <- loc.table[order(super_region_name, region_name), ]
nat.locs <- setdiff(loc.table[level == 3, ihme_loc_id], island.locs)



#######################################################
###Global, Super-Regional, Regional Plots
#######################################################
i <- 1
scen.list <- list()
for(c.scenario in c("reference","better","worse")) {
  data <- fread(paste0(jpath,"FILEPATH/aggregations.csv")) 
  data[,Scenario:=c.scenario]
  scen.list[[i]] <- data
  i <- i + 1
}
scen.dat <- rbindlist(scen.list)


pdf(paste0(jpath,"FILEPATH/scenario_comparison_aggregations_logit_art.pdf"),width=12,height=8)
for (c.loc in unique(scen.dat$location_id)) {
print(c.loc)
gg <- ggplot(scen.dat[(year_id>2015|Scenario=="reference") & location_id==c.loc]) + 
  geom_line(aes(x=year_id,y=mean,color=Scenario)) +
  geom_ribbon(aes(x=year_id,ymin=lower,ymax=upper,fill=Scenario),alpha=.2) +
  facet_wrap(~variable,scales="free") +
  geom_vline(aes(xintercept=2016),linetype="longdash",color="grey")  +
  pretty + labs(y="",x="",title=locs[location_id==c.loc,location_name])

print(gg)
}
dev.off()




#######################################################
###Country-Specific Plots
#######################################################
dir.create(paste0(jpath,"FILEPATH/",c.fbd_version), showWarnings = F)

pdf(paste0(jpath,"FILEPATH/scenario_comparison_countries_logit_art.pdf"),width=12,height=8)
for (c.iso in nat.locs) {
print(c.iso)
i <- 1
scen.list <- list()
for(c.scenario in c("reference","better", "worse")) {
  if(!file.exists(paste0(jpath,"FILEPATH/",c.iso,".csv"))){
    print("missing scenario")
    next;
  }
data <- fread(paste0(jpath,"FILEPATH/",c.iso,".csv")) 
data[,Scenario:=c.scenario]
scen.list[[i]] <- data
i <- i + 1
}
scen.dat <- rbindlist(scen.list)



scen.dat[,Scenario:=factor(Scenario,levels = rev(unique(scen.dat$Scenario)))]
gg <- ggplot(scen.dat[year_id>2015|Scenario=="reference"]) + 
  geom_line(aes(x=year_id,y=mean,color=Scenario)) +
  geom_ribbon(aes(x=year_id,ymin=lower,ymax=upper,fill=Scenario),alpha=.2) +
  facet_wrap(~variable,scales="free") +
  geom_vline(aes(xintercept=2016),linetype="longdash",color="grey")  +
  pretty + labs(y="",x="",title=paste0(ifelse(c.iso=="Global","Global",unique(locs[ihme_loc_id==c.iso,location_name]))," HIV Scenarios"))


print(gg)
}
dev.off()