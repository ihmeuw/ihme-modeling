
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 10/11/19
# Purpose: Read in all bwga mediated pafs, proportionately split, and resave to match the format of other air pollution PAFs
#          
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())
user <- "USERNAME"

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
} else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
}

arg <- commandArgs(trailingOnly=T)

if (length(arg)==0) {
  #toggle for targeted run on cluster
  arg <- c(37, #location_id
           50) #paf version
}

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

years <- c(1990,1995,2000,2005,2010,2015,2017,2019)
risks <- c("air_pm","air_hap","air_pmhap")
loc <- arg[1]
paf.version <- arg[2]

# in.paf.version <- 47 # use if resaving some PAFs from a previous version

# Directories -------------------------------------------------------------

home.dir <- "FILEPATH"

in.bwga.paf.dir <- "FILEPATH"
in.prop.dir <-  file.path(home.dir,"FILEPATH",in.paf.version)

out.mediation.dir <- file.path(home.dir,"FILEPATH",paf.version)

paf.sum.out <- file.path(home.dir,"FILEPATH",paf.version)

# this table gives us the demographics (age,cause,measure,sex) groups to read in for PAFs later
dem_table <- fread(file.path(home.dir,"FILEPATH.csv"))

  # read in pm bwga pafs
  dt_1 <- fread(paste0(in.bwga.paf.dir,"/",loc,"_1.csv"))
  dt_2 <- fread(paste0(in.bwga.paf.dir,"/",loc,"_2.csv"))
  dt <- rbind(dt_1,dt_2)
  dt <- dt[year_id %in% years]
  
  dt[,draw:=draw+1]
  
  # For negative draws, change to zero according to maternal/neonatal team. 
  # This is appropriate because our risk curve uncertainty does not go below zero.
  # If our risk curve uncertainty were to change, we might need to change this strategy. 
  # Essentially what we are saying here is that 1) PM always makes you be born lighter and earlier and 
  # 2) being born later/heavier is never worse for you. 
  dt[paf<0,paf:=0]
  
  # read in LRI to calculate mediation factor

  # Here the "direct lri paf" is the one that comes straight out of the LRI mrbrt curve.
  # if (direct lri paf)>(bwga lri paf), there is no problem. The mediation factor (percent of attributable burden
  # mediated through bwga) is paf_bwga/paf_direct. The overall LRI paf is equivalent to the direct paf.
  #
  # Otherwise if the bwga mediated paf is greater than the direct paf, there is a problem. We want to pick the MAX
  # of these two numbers, so we replace the LRI paf with the bwga mediated one. The mediation factor here is 1, because
  # ALL of the LRI burden is through bwga.
  
  i <- 1
  list <- list(data.table())
  
  for(year in years){
    for(age in c(2,3)){
      list[[i]] <- fread(paste0(home.dir,"FILEPATH",in.paf.version,"FILEPATH",loc,"_",year,"FILEPATH",age,".csv")) #male
      list[[i+1]] <- fread(paste0(home.dir,"FILEPATH",in.paf.version,"FILEPATH",loc,"_",year,"FILEPATH",age,".csv")) #female
      i <- i+2
    }
  }

  dt_direct <- rbindlist(list)

  # make long by draw
  direct <- melt.data.table(dt_direct,measure.vars=patterns("draw_"),value.name="direct_paf",variable.name="draw",variable.factor=T)
  direct <- direct[,draw:=as.numeric(draw)]

  lri_paf <- merge(dt,direct)
  setnames(lri_paf,"paf","bwga_paf")
  lri_paf[,paf:=max(bwga_paf,direct_paf),by=1:nrow(lri_paf)]
  lri_paf[,mediation_factor:=min(bwga_paf/direct_paf,1), by=1:nrow(lri_paf)]

  # save mediation factor
  write.csv(lri_paf,paste0(out.mediation.dir,"/",loc,".csv"),row.names=F)

  # merge back on
  dt <- rbind(dt[cause_id!=322],lri_paf[,names(dt),with=F])
  
  # read in proportional splits
  splits <- lapply(paste0(in.prop.dir,"/",loc,"_",years,".csv"),fread) %>% rbindlist()
  dt <- merge(dt,splits,by=c("location_id","year_id","draw"))
  
  setnames(dt,"paf","paf_pm")
  dt[,paf_hap:=paf_pm*hap_paf_ratio] #proprotionally split PM paf to Hap and ambient
  dt[,paf_ambient:=paf_pm*ambient_paf_ratio]
  
  #summary file of exposures
  
  lower <- function(x){quantile(x,p=.025)}
  upper <- function(x){quantile(x,p=.975)}
  
  summary <- melt.data.table(dt,id.vars=c("location_id","year_id","draw","age_group_id","sex_id","cause_id","measure_id"),measure.vars = patterns("paf"))
  summary <- summary[,.(mean=mean(value),lower=lower(value),upper=upper(value)),by=c("location_id","year_id","age_group_id","sex_id","cause_id","measure_id","variable")]

  # summary file of PAF
  
  write.csv(summary[variable %in% c("paf_pm","paf_hap","paf_ambient")],
            paste0(paf.sum.out,"FILEPATH",loc,".csv"),row.names=F)
  
  # save draws

  dt <- dt[,.(location_id,year_id,draw,age_group_id,sex_id,cause_id,measure_id,paf_pm,paf_hap,paf_ambient)]

  # duplicate ylls to ylds (assume same except for lbw)

  ylls <- dt[measure_id==4]

  ylds <- copy(ylls)

  # if we do have a separate estimate for ylds (lbw) add it in
  for(cause in dt[measure_id==3,unique(cause_id)]){
    rows <- dt[cause_id==cause & measure_id==3]
    ylds <- rbind(ylds[cause_id!=cause],rows)
  }

  ylds[,measure_id:=3]

  dt <- rbind(ylls,ylds)

  # reshape long by risk
  dt <- melt.data.table(dt, id.vars=c("location_id","year_id","draw","age_group_id","sex_id","cause_id","measure_id"))
  dt[,risk:=tstrsplit(variable,"_",keep=2)]
  dt[,variable:=NULL]

  # reshape wide by draw
  dt[,draw:=paste0("draw_",draw-1)]
  dt <- dcast.data.table(dt, location_id + year_id + age_group_id + sex_id + cause_id + measure_id + risk ~ draw, value.var="value")

  dt[risk=="ambient",risk:="air_pm"]
  dt[risk=="hap",risk:="air_hap"]
  dt[risk=="pm",risk:="air_pmhap"]

  # get all pafs for this country and save into one file for save results.

  read_draws <- function(i,dt,years){
    
    out <- lapply(paste0(home.dir,"/",this.risk,"FILEPATH",in.paf.version,"/",dt[i,measure_id],"_",dt[i,cause_id],"_",loc,"_",years,"_",dt[i,sex_id],dt[i,age_group],".csv"),fread) %>% rbindlist(fill=T)
    return(out)
    
  }
  
  # reformat demographics table to include the "_" for building the filepaths. cataracts is not saved by age, so we have to be a bit creative
  dem_table[!is.na(age_group_id),age_group:=paste0("_",age_group_id)]
  dem_table[is.na(age_group),age_group:=""]

  for(this.risk in risks){
    
    if(this.risk == "air_pm"){
      table <- dem_table[acause != "sense_cataract"]
    }else{
      table <- copy(dem_table)
    }
    
    pafs <- lapply(1:nrow(table),read_draws, dt=table,years=years) %>% rbindlist(fill=T)
    
    # drop neonatal LRI bc we are replacing with newly calculated
    pafs <- pafs[!(cause_id==322 & age_group_id %in% c(2,3))]

    bwga <- copy(dt[risk==this.risk])
    bwga[,risk:=NULL]

    out <- rbind(pafs,bwga,use.names=T,fill=T)

    if(this.risk != "air_pm"){
      N <- 23*2*2 + 15*2*2 + 15*2*3*2 + 15*2*2 + 15*2*2 + 16*2 + 15*2*2 + 11*2*2*2  # number of expected rows
      #    lri      lung      stroke      ihd     COPD  cataracts diabetes  bwga (not LRI)
    }else{
      N <- 23*2*2 + 15*2*2 + 15*2*3*2 + 15*2*2 + 15*2*2 + 15*2*2 + 11*2*2*2  # number of expected rows
      #    lri      lung      stroke      ihd     COPD  diabetes  bwga (not LRI)
    }


    if(nrow(out) != (length(years) * N)){
      warning(paste("Missing rows in location,",loc,"risk,",this.risk))
    }

    write.csv(out,paste0(home.dir,"/", this.risk,"FILEPATH",paf.version,"/",loc,".csv"),row.names=F)
    
    print(paste("Finished with ",this.risk,Sys.time()))
  }