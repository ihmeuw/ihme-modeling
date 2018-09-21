
############
## Settings
############

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
print(paste0(workdir))
setwd(workdir)

test <- F        
start <- 5       
fin <- 10

hiv_free <- T
with_hiv <- T

update_maps <- T

locations <- get_locations(level="estimate")
write.csv(locations, "FILEPATH/locations.csv")
save_errout <- paste0("-o FILEPATH",username,"/output -e FILEPATH",username,"/errors")


setwd("FILEPATH")
if (start<=5 & fin >=5) for (ff in dir("FILEPATH")) if(ff!="archive" & grepl("LT_sim_withhiv",ff)) file.remove(paste(ff, sep=""))
if (start<=6 & fin >=6) for (ff in dir("FILEPATH")) if(ff!="archive" & grepl("partial_compile",ff)) file.remove(paste(ff, sep=""))
if (start<=7 & fin >=7) for (ff in dir("FILEPATH")) if(ff!="archive" & grepl("compiled",ff)) file.remove(paste(ff, sep=""))
setwd("FILEPATH")
if (start<=9 & fin >=9) for (ff in dir("FILEPATH")) if(ff!="archive" & grepl("lt_",ff)) file.remove(paste(ff, sep=""))
setwd("FILEPATH")
if (start<=10 & fin >=10) for (ff in dir("FILEPATH")) if(ff!="archive" & ff != "temp" & grepl("lt_",ff)) file.remove(paste(ff, sep=""))


setwd("FILEPATH")
if (start<=5 & fin >=5) for (ff in dir("FILEPATH")) if(ff!="archive" & grepl("LT_sim_nohiv",ff)) file.remove(paste(ff, sep=""))
if (start<=6 & fin >=6) for (ff in dir("FILEPATH")) if(ff!="archive" & grepl("partial_compile",ff)) file.remove(paste(ff, sep=""))
if (start<=7 & fin >=7) for (ff in dir("FILEPATH")) if(ff!="archive" & grepl("compiled",ff)) file.remove(paste(ff, sep=""))
setwd("FILEPATH")
if (start<=9 & fin >=9) for (ff in dir("FILEPATH")) if(ff!="archive" & grepl("lt_",ff)) file.remove(paste(ff, sep=""))
setwd("FILEPATH")
if (start<=10 & fin >=10) for (ff in dir("FILEPATH")) if(ff!="archive" & ff != "temp" & grepl("lt_",ff)) file.remove(paste(ff, sep=""))



setwd(workdir)
if (update_maps == T) {
  library("RMySQL")
  agemap <- data.table(get_age_map(type="lifetable"))[,.(age_group_name, age_group_id)]
  add <- data.table(age_group_name=c("95 plus", "Early Neonatal", "Late Neonatal", "Post Neonatal"), age_group_id=c(235, 2, 3, 4))
  agemap <- rbind(agemap, add)
  write.csv(agemap,"FILEPATH/agemap.csv",row.names=F)
  
  sups <- get_locations(level="super")
  sups <- sups[,c("location_name","ihme_loc_id")]
  names(sups)[names(sups) == "location_name"] <- "super_region_name"
  write.csv(sups,"FILEPATH/super_map.csv",row.names=F)
  
  reg <- get_locations(level="region")
  reg <- reg[,c("location_name","ihme_loc_id","location_id")]
  names(reg)[names(reg) == "location_name"] <- "region_name"
  write.csv(reg,"FILEPATH/region_map.csv",row.names=F)
  
  locs <- get_locations(level="all")
  locs <- locs[,c("ihme_loc_id","location_id","location_name","path_to_top_parent","parent_id")]
  write.csv(locs,"FILEPATH/lowest_map.csv",row.names=F)
  
  base <- get_locations()
  write.csv(base, "FILEPATH/base_map.csv", row.names=F)
}

keep_ages <- c(5:20, 28, 30:32, 235)
pop <- get_population(status = "recent", year_id = -1, sex_id = c(1:2), age_group_id=keep_ages, location_id=-1, location_set_id=21)
write.csv(pop, "FILEPATH/lt_pop.csv", row.names=F)

if (start <= 5 & fin >=5) {
  
  jobids5b <- c()
  
  for (i in c(0:999)) {
    jname <- paste0("agg_lt_",i,"_")
    slots <- 6
    mem <- slots*2
    holds <- "nothing"
    args <- paste(i, sep = " ")
    sys.sub <- paste0("qsub -o FILEPATH",username,"/output -e FILEPATH",username,"/errors -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",holds," ")
    shell <- "r_shell.sh"
    script <- "05b_parallel_lt_aggs.R"
    print(paste(sys.sub, shell, script, "\"", args, "\""))
    system(paste(sys.sub, shell, script, "\"", args, "\""))
    jobids5b <- c(jobids5b, jname)
  }
  
}

  jobids9 <- c()


for (htype in c("hiv_free","with_hiv")) {


  jobids6b <- c()
  jobids7b <- c()
  
  if ( (htype == "hiv_free" & hiv_free == T) | (htype == "with_hiv" & with_hiv == T)) {  
    
    if (start<=6 & fin >=6) {
      if (start > 5) jobids5b <- "05job"
      slots <- 15
      mem <- slots*2
      holds <- paste("-hold_jid \"",paste(jobids5b,collapse=","),"\"",sep="")
      
      for (i in 0:9) {
        jname <- paste0("part_lt_",i,"_",htype)
        sys.sub <- paste0("qsub ",save_errout," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
        in_dir <- ifelse(htype == "hiv_free","FILEPATH","FILEPATH")
        save_dir <- ifelse(htype == "hiv_free","FILEPATH","FILEPATH")
        args <- paste0(i," ",htype," ",in_dir," ",save_dir)
        shell <- "stata_shell_mp.sh"
        script <- "06b_parallel_lt_partial_compile.do"
        print(paste(sys.sub, shell, script, "\"", args, "\""))
        system(paste(sys.sub, shell, script, "\"", args, "\""))
        jobids6b <- c(jobids6b,jname)
      }
    }
    

    if (start<=7 & fin >=7) {
      if (start > 6) jobids6b <- "06job"
      for (param in c("ax","qx","mx")) {
        slots <- 30
        mem <- slots*2
        holds <- paste("-hold_jid \"",paste(jobids6b,collapse=","),"\"",sep="")
        dat_dir <- ifelse(htype == "hiv_free","FILEPATH","FILEPATH")
        
        jname <- paste0("comp_lt_",htype,"_",param)
        sys.sub <- paste0("qsub ",save_errout," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
        args <- paste0(dat_dir," ",htype," ",param)
        shell <- "stata_shell_mp.sh"
        script <- "07b_compile_lt.do"
        print(paste(sys.sub, shell, script, "\"", args, "\""))
        system(paste(sys.sub, shell, script, "\"", args, "\""))
        jobids7b <- c(jobids7b,jname)
      }
    }
    

    if (start <= 9 & fin >= 9) {
      if (start > 7) jobids7b <- "07job"
      slots <- 20
      mem <- slots*2
      holds <- paste("-hold_jid \"",paste(jobids7b,collapse=","),"\"",sep="")
      for (param in c("ax","mx","qx")) {
        div <- 30
        locsfile <- paste0("FILEPATH",ifelse(htype == "with_hiv","temp_lt_agg","temp_lt_hivfree_agg"),"/compiled_lt_",param,".dta")
        outfile <- ifelse(htype == "with_hiv",paste0("FILEPATH/lt_",param,"_"),
                          paste0("FILEPATH/lt_",param,"_"))
        outtype <- "csv"
        
        for (i in 1:(div)) {
          jname <- paste0("lt_ls_",htype,"_",i,"_",param)
          sys.sub <- paste0("qsub ",save_errout," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
          args <- paste0(div," ",i," ",locsfile," ",outfile," ",outtype)
          shell <- "r_shell.sh"
          script <- "09_save_locs.R"
          print(paste(sys.sub, shell, script, "\"", args, "\""))
          system(paste(sys.sub, shell, script, "\"", args, "\""))
          jobids9 <- c(jobids9,jname)
        }
      }
    }
    
    
    
  } 

} 


if (start <= 10 & fin >= 10) {
  jobids10b <- c()
  slots <- 4
  mem <- slots*2
  if (start == 10) {
    jobids9 <- "nothing"
  }

  holds <- paste("-hold_jid \"",paste(jobids9,collapse=","),"\"",sep="")
  
  in_hiv <- paste0("FILEPATH")
  in_nhiv <- paste0("FILEPATH")
  out_hiv <- paste0("FILEPATH")
  out_nhiv <- paste0("FILEPATH")
  
  for (loc in unique(locations$ihme_loc_id)) {
   if(!file.exists(paste0("FILEPATH/lt_", loc, ".csv"))){
      jname <- paste0("lt_",loc)
      sys.sub <- paste0("qsub ",save_errout," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
      args <- paste(loc,in_hiv,in_nhiv,out_hiv,out_nhiv,sep=" ")
      shell <- "r_shell.sh"
      script <- "10_fill_lt_loc.R"
      print(paste(sys.sub, shell, script, "\"", args, "\""))
      system(paste(sys.sub, shell, script, "\"", args, "\""))
      jobids10b <- c(jobids10b,jname)
    }
  }
}









