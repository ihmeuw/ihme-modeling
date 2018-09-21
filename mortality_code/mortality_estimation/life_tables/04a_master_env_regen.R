
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

source(paste0(root, "FILEPATH/get_population.R"))
print(paste0(workdir))
setwd(workdir)
source(paste0(root, "FILEPATH/get_age_map.r"))

test <- F        
start <- 6        
fin <- 9

update_maps <- T

save_errout <- paste0("-o FILEPATH",username,"/output -e FILEPATH",username,"/errors")
run_date <- Sys.Date()
run_date <- gsub("-","",run_date)


setwd(workdir)

if (update_maps == T) {
  agemap <- data.table(get_age_map(type="lifetable"))[,.(age_group_name, age_group_id)]
  add <- data.table(age_group_name=c("95 plus", "Early Neonatal", "Late Neonatal", "Post Neonatal"), age_group_id=c(235, 2, 3, 4))
  agemap <- rbind(agemap, add)
  write.csv(agemap,"FILEPATH/agemap.csv",row.names=F)

  locs <- get_locations()
  write.csv(locs, "FILEPATH/locations.csv")
  
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
}

pop <- get_population(location_id=-1, sex_id=c(1,2), status="recent", age_group_id=-1, year_id=-1, location_set_id=21)
write.csv(pop,"FILEPATH/env_pop.csv",row.names=F)

if (start <= 5 & fin >=5) {
  
jobids5a <- c()
in_dir <- "FILEPATH"
map_dir <- "FILEPATH"
out_dir <- "FILEPATH"

for (i in c(0:999)) {
  jname <- paste0("agg_env_",i)
  slots <- 4
  mem <- slots*2
  holds <- "nothing"
  args <- paste0(i," ",in_dir," ",map_dir," ",out_dir)
  sys.sub <- paste0("qsub -o FILEPATH",username,"/output -e FILEPATH",username,"/errors -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G -hold_jid ",holds," ")
  shell <- "stata_shell.sh"
  script <- "05a_parallel_env_aggs.do"
  system(paste(sys.sub, shell, script, "\"", args, "\""))
  jobids5a <- c(jobids5a, jname)
  }
}


if (start<=6 & fin >=6) {
  if (start > 5) jobids5a <- "nope"
  slots <- 10
  mem <- slots*2
  holds <- paste("-hold_jid \"",paste(jobids5a,collapse=","),"\"",sep="")
  in_dir <- "FILEPATH/temp_env_agg"
  
  jobids6a <- c()
  for (i in c(9)) {
    jname <- paste0("part_env_",i)
    sys.sub <- paste0("qsub ",save_errout," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
    args <- paste0(i," ",in_dir)
    shell <- "stata_shell.sh"
    script <- "06a_parallel_env_partial_compile.do"
    print(paste(sys.sub, shell, script, "\"", args, "\""))
    system(paste(sys.sub, shell, script, "\"", args, "\""))
    jobids6a <- c(jobids6a,jname)
  }
}


if (start<=7 & fin >=7) {
  if (start > 6) jobids6a <- "06job"
  slots <- 35
  mem <- slots*2
  holds <- paste("-hold_jid \"",paste(jobids6a,collapse=","),"\"",sep="")
  
  jname <- paste0("comp_env")
  sys.sub <- paste0("qsub ",save_errout," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
  args <- paste0("nothing")
  shell <- "stata_shell_mp.sh"
  script <- "07a_compile_envelope.do"
  print(paste(sys.sub, shell, script, "\"", args, "\""))
  system(paste(sys.sub, shell, script, "\"", args, "\""))
  jobids7a <- jname
}


if (start <= 9 & fin >= 9) {
  if (start > 7) jobids7a <- "07job"
  slots <- 65
  mem <- slots*2
  holds <- paste("-hold_jid \"",paste(jobids7a,collapse=","),"\"",sep="")
  
  div <- 50
  locsfile <- "FILEPATH/compiled_env.dta"
  outfile <- "FILEPATH/env_"
  outtype <- "dta"
  
  jobids9a <- c()
  for (i in 1:(div)) {
    jname <- paste0("env_locsave_",i)
    sys.sub <- paste0("qsub ",save_errout," -P proj_mortenvelope -cwd -N ",jname," -pe multi_slot ",slots," -l mem_free=",mem,"G ",holds," ")
    args <- paste0(div," ",i," ",locsfile," ",outfile," ",outtype)
    shell <- "r_shell.sh"
    script <- "09_save_locs.R"
    print(paste(sys.sub, shell, script, "\"", args, "\""))
    system(paste(sys.sub, shell, script, "\"", args, "\""))
    jobids9a <- c(jobids9a,jname)
  }
}


