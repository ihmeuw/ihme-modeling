# clear memory
rm(list=ls())
user <- "USERNAME"

# disable scientific notation
options(scipen = 999)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}

arg <- commandArgs(trailingOnly=T)


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

years <- c(1990,1995,2000,2005,2010,2015,2019,2020)
risks <- c("air_pm","air_hap","air_pmhap")
loc <- arg[1]
paf.version <- arg[2]

# Directories -------------------------------------------------------------

home.dir <- "FILEPATH"

in.bwga.paf.dir <- "FILEPATH"
in.prop.dir <-  file.path(home.dir,"FILEPATH",paf.version)

out.mediation.dir <- file.path(home.dir,"FILEPATH",paf.version)
dir.create(out.mediation.dir,recursive = T)

paf.sum.out <- file.path(home.dir,"FILEPATH",paf.version)

dem_table <- fread(file.path(home.dir,"FILEPATH.csv"))  #this table gives us the demographics (age,cause,measure,sex) groups to read in for PAFs later. 


  # read in pm bwga pafs
  dt_1 <- fread(paste0(in.bwga.paf.dir,"/",loc,"_1.csv"))
  dt_2 <- fread(paste0(in.bwga.paf.dir,"/",loc,"_2.csv"))
  dt <- rbind(dt_1,dt_2)
  dt <- dt[year_id %in% years]

  dt[,draw:=draw+1]

  dt[paf<0,paf:=0]

  # read in LRI to calculate mediation factor
  
  i <- 1
  list <- list(data.table())
  
  for(year in years){
    for(age in c(2,3)){  # just 2 and 3 because it is neonatal groups only
      list[[i]] <- fread(paste0(home.dir,"FILEPATH",paf.version,"/4_322_",loc,"_",year,"_1_",age,".csv")) #male
      list[[i+1]] <- fread(paste0(home.dir,"FILEPATH",paf.version,"/4_322_",loc,"_",year,"_2_",age,".csv")) #female
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
  
  # summary file of exposures
  
  lower <- function(x){quantile(x,p=.025)}
  upper <- function(x){quantile(x,p=.975)}

  summary <- melt.data.table(dt,id.vars=c("location_id","year_id","draw","age_group_id","sex_id","cause_id","measure_id"),measure.vars = patterns("paf"))
  summary <- summary[,.(mean=mean(value),lower=lower(value),upper=upper(value)),by=c("location_id","year_id","age_group_id","sex_id","cause_id","measure_id","variable")]

  # summary file of PAF
  
  write.csv(summary[variable %in% c("paf_pm","paf_hap","paf_ambient")],
            paste0(paf.sum.out,"/bwga_",loc,".csv"),row.names=F)
  
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

  ## START OF NON-MEDIATED RESAVE
  
  #reformat demographics table to include the "_" for building the filepaths. cataracts is not saved by age, so we have to be a bit creative
  dem_table[!is.na(age_group_id),age_group:=paste0("_",age_group_id)]
  dem_table[is.na(age_group),age_group:=""]
 
  # find locs that failed 
  missing <- data.table(location_id=numeric(),
                        year_id=numeric(),
                        cause_id=numeric(),
                        measure_id=numeric(),
                        sex_id=numeric(),
                        age_group_id=numeric())
  
  check_missing <- function(i,dt,years){
    
    
    additions <- lapply(years, function(year){
          filename <- paste0(home.dir,"/air_pmhap/paf/draws/",paf.version,"/",dt[i,measure_id],"_",dt[i,cause_id],"_",loc,"_",year,"_",dt[i,sex_id],dt[i,age_group],".csv")
          if(!file.exists(filename) | file.size(filename)==0) {
            new_row <- data.table(location_id = loc,
                            year_id = year,
                            cause_id = dt[i,cause_id],
                            measure_id = dt[i,measure_id],
                            sex_id = dt[i,sex_id],
                            age_group_id = dt[i,age_group])

      return(new_row)
      }
    }) %>% rbindlist
    
    missing <- rbind(missing,additions) %>% unique
    return(missing)
  }

  # note: do not need to check missingness for all risks, because these are saved at the same time!!!
    table <- copy(dem_table)
    
    missing <- lapply(1:nrow(table),check_missing,dt=table,years=years) %>% rbindlist %>% unique
    
    print(paste("Finished with missingness check",Sys.time()))

  if(nrow(missing)>0){
    missing_dir <- paste0(home.dir,"FILEPATH",paf.version)
    dir.create(missing_dir,recursive = T,showWarnings = F)
    write.csv(missing,paste0(missing_dir,"/",loc,".csv"),row.names = F)
    warning("Missing location-year-cause combos!")
    quit(status=1)
    
  }
  
  
  # get all pafs for this country and save into one file for save results.
  read_draws <- function(i,dt,years){
    
    out <- lapply(paste0(home.dir,"/",this.risk,"FILEPATH",paf.version,"/",dt[i,measure_id],"_",dt[i,cause_id],"_",loc,"_",years,"_",dt[i,sex_id],dt[i,age_group],".csv"),fread) %>% rbindlist(fill=T)
    return(out)
    
  }
  
  for(this.risk in risks){
    
    if(this.risk == "air_pm"){
      table <- dem_table[acause != "sense_cataract"]
    }else{
      table <- copy(dem_table)
    }

    pafs <- lapply(1:nrow(table),read_draws, dt=table,years=years) %>% rbindlist(fill=T)
    
    # drop neonatal LRI for neonatal age groups bc we are replacing with newly calculated
    pafs <- pafs[!(cause_id==322 & age_group_id %in% c(2,3))]

    bwga <- copy(dt[risk==this.risk])
    bwga[,risk:=NULL]

    out <- rbind(pafs,bwga,use.names=T,fill=T)

    if(this.risk != "air_pm"){
      N <- 25*2*2 + 15*2*2 + 15*2*3*2 + 15*2*2 + 15*2*2 + 16*2 + 15*2*2 + 11*2*2*2  #number of expected rows
      #    lri      lung      stroke      ihd     COPD  cataracts diabetes  bwga (not LRI)
    }else{
      N <- 25*2*2 + 15*2*2 + 15*2*3*2 + 15*2*2 + 15*2*2 + 15*2*2 + 11*2*2*2  #number of expected rows
      #    lri      lung      stroke      ihd     COPD  diabetes  bwga (not LRI)
    }

    if(nrow(out) != (length(years) * N)){
      warning(paste("Missing rows in location,",loc,"risk,",this.risk))
    }
    
    # copy rows for 2021 and 2022
    temp <- out[year_id==2019]
    temp[,year_id:=2021]
    out <- rbind(out,temp)
    temp[,year_id:=2022]
    out <- rbind(out,temp)
    
    write.csv(out,paste0(home.dir,"/", this.risk,"FILEPATH",paf.version,"/",loc,".csv"),row.names=F)
    
    print(paste("Finished with ",this.risk,Sys.time()))
  }


  

