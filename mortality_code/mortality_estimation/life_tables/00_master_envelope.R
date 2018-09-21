
rm(list=ls()); library(foreign); library(data.table)
if (Sys.info()[1] == 'Windows') {
  username <- "USER"
  root <- "FILEPATH"
  workdir <-  paste("FILEPATH",username,"FILEPATH",sep="")
  source("FILEPATH/get_locations.r")
} else {
  username <- Sys.getenv("USER")
  root <- "FILEPATH"
  workdir <- paste("FILEPATH",username,"FILEPATH",sep="")
  source("FILEPATH/get_locations.r")
}
source(paste0(root, "FILEPATH/get_age_map.r"))
source(paste0(root, "FILEPATH/get_population.R"))


setwd(workdir)

start <- 3         
end <- 3           
test <- F
alt_errout <- F       
no_errout <- T       
run <- 16
intel <- F
update_maps <- F
update_pop <- T

locs <- get_locations()
t <- table(locs$region_id)
locs <- data.frame(count=t)
names(locs) <- c("region","freq")
locs <- locs[order(-locs$freq),]
locs <- as.numeric(as.character(locs$region))


if (!test) {
  
  ## remove step 1
  setwd("FILEPATH")
  if (start<=1 & end >=1) for (ff in dir("FILEPATH")) {
    if(ff!="archive") file.remove(paste("FILEPATH", ff, sep=""))  
  } 
  if (start<=2 & end >=2) for (ff in dir("FILEPATH")) {
    if(ff!="archive" & !grepl("all_entry_sims",ff)) file.remove(paste("FILEPATH", ff, sep=""))  
  } 
  
  setwd("FILEPATH")
  if (start<=3 & end >=3) {
    

    base <- "FILEPATH"
    for (sex in c("male","female")) {
      for (loc in locs) {
        dir.create(paste0(base,sex,"_",loc))
      }
    }
    
    for (sex in c("male","female")) {
      for (loc in locs) {
        step3_dir <- paste0("FILEPATH",sex,"_",loc)
        system(paste0("perl -e 'unlink <", step3_dir, "/*.dta>' "))
        for (ff in dir(paste0("FILEPATH",sex,"_",loc))) {
          if(ff!="archive") file.remove(paste(paste0("FILEPATH",sex,"_",loc,"/"), ff, sep=""))  
        }
      } 
    }  
  }
 }


if(update_maps == T){
  agemap <- data.table(get_age_map(type="lifetable"))[,.(age_group_name, age_group_id)]
  add <- data.table(age_group_name=c("95 plus", "Early Neonatal", "Late Neonatal", "Post Neonatal"), age_group_id=c(235, 2, 3, 4))
  agemap <- rbind(agemap, add)
  write.csv(agemap,"FILEPATH/agemap.csv",row.names=F)
  
  
  age_all <- data.table(get_age_map(type="all"))[,.(age_group_name, age_group_id)]
  write.csv(age_all,"FILEPATH/agemap_all.csv",row.names=F)
  
  locs1 <- get_locations(level="estimate")
  write.csv(locs1,"FILEPATH/estimate_locs.csv",row.names=F)
  
  locs2 <- get_locations(level="all")
  write.csv(locs2,"FILEPATH/locs_all.csv",row.names=F)
  
  
}

if(update_pop ==T){
  populations <- get_population(location_set_id=21, status="recent", location_id=-1, year_id=-1, sex_id=-1, age_group_id=-1)
  write.csv(populations, "FILEPATH/population.csv", row.names=F)
}


setwd(workdir)

qsub <- paste0("qsub -P proj_mortenvelope -o FILEPATH",username,
               "/output -e FILEPATH",username,"/errors -N ")
stata_shell <- paste0(workdir,"stata_shell.sh")
r_shell <- paste0(workdir,"r_shell.sh")
stata_shell_mp <- paste0(workdir,"stata_shell_mp.sh")
stata_shell_mp_disk <- paste0(workdir,"stata_shell_mp_disk.sh")


if (start <= 1 & end >= 1) {
  # options
  slots <- 30
  mem <- paste0(slots*2,"G") 
  hold <- NULL
  jname <- paste0("env_entry_compile")
  
  # qsub and scripts
  sys.sub <- paste(qsub, jname, " -pe multi_slot ",slots," -l mem_free=",mem,sep="")
  script <- paste0(workdir,"01_load_entry_simfiles.do")
  shell <- stata_shell
  
  # submit
  ifelse(test,
         paste(sys.sub, shell, script),
         system(paste(sys.sub, shell, script)))
  
}


## Resave drawfiles
if (start <= 2 & end >= 2) {
  print("submitting resave draws")
  jobids2 <- c()
  for (i in 0:19) {
    # options
    slots <- 9
    mem <- paste0(slots*2,"G") 
    hold <- paste(" -hold_jid \",env_entry_compile,\" ",sep="")
    jname <- paste0("env_entry_resave_",i)
    args <- paste(i)
    
    # qsub and scripts
    sys.sub <- paste(qsub, jname, " -pe multi_slot ",slots," -l mem_free=",mem,ifelse(start < 2,hold,""),sep="")
    script <- paste0(workdir,"02_resave_simfiles.do")
    shell <- stata_shell_mp
    
    # submit
    print(paste(sys.sub, shell, script, "\"", args, "\""))
    if (test == F) system(paste(sys.sub, shell, script, "\"", args, "\""))
    jobids2 <- c(jobids2,jname)
    
  }
}


if (start >2) jobids2 <- "empty"
# ## Resave drawfiles
if (start <= 3 & end >= 3) {
  print("submitting envelope draws")
  jobids3 <- c()
  
  qsub_cntrl <- ifelse(alt_errout,paste0("qsub -P proj_mortenvelope -o FILEPATH",
                                         " -e FILEPATH -N "),qsub)
  qsub_cntrl <- ifelse(no_errout,paste0("qsub -P proj_mortenvelope -N "),qsub)
  
  
    for (loc in locs) {
     for (i in c(0:999)) {
       for (sex in c("male","female")) {    
        if (i == 0) {
          qsub <- paste0("qsub -P proj_mortenvelope -o FILEPATH",username,
                         "/output -e FILEPATH",username,"/errors -N ")
        } else {
          qsub <- qsub_cntrl
        }
        
        
        slots <- 2
        mem <- paste0(slots*2,"G") 
        hold <- paste(" -hold_jid \"",paste(jobids2,collapse=","),"\"",sep="")
        
        jname <- paste0("env_lt_",run,"_",i,sex,loc)
        args <- paste(i," ",sex," ",loc)
        
        # qsub and scripts
        sys.sub <- paste(qsub, jname,ifelse(intel==T,paste0(" -l hosttype=intel"),"") ," -pe multi_slot ",slots," -l mem_free=",mem,ifelse(start < 3,hold,""),sep="")
        script <- paste0(workdir,"03_env_lt_results.do")
        shell <- stata_shell
        
        # submit
        if (test == F) {
          system(paste(sys.sub, shell, script, "\"", args, "\""))
        } 
        jobids3 <- c(jobids3,jname)
        
        if (i %in% c(499,999) & sex == "male") Sys.sleep(20)
      }
     }
  }
}

