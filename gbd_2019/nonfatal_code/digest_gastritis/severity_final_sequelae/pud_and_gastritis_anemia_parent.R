# 28 Feb 2018
# Parent for parallelizing the anemia split epi model step, which CANNOT be parallelized over location, but can be parallelized for cause and over asymp, mild, mod, adj_acute, adj_complic

#source("FILEPATH/pud_and_gastritis_anemia_parent.R")

# set up environment
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
  scratch <-"FILEPATH_H/gianemia"
} else {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
  shell <- "FILEPATH/r_shell.sh"
  scratch <- "FILEPATH_SCRATCH/gianemia"
}

##create today's directory (loop will create directories for each child)
date <- file.path(scratch, format(Sys.Date(), format = "%Y_%m_%d"))
dir.create((date), showWarnings = F)

# define objects

##create cause list
causes<-list("pud","gastritis")
##turn on and off causes with 0/1
pud <- 1
gastritis <- 1

for (cause in causes){
  if (get(cause)<1){
    causes<-causes[-which(causes==cause)]
  }
}

##create child list
children<-list("asymp", "mild", "mod", "adj_acute", "adj_complic")
##turn on and off children with 0/1
asymp <- 1
mild <- 1
mod <- 1
adj_acute <- 1
adj_complic <- 1

for (child in children) {
  if (get(child)<1){
    children<-children[-which(children==child)]
  }
}

##create directories for draws and qsub jobs on cluster 
for(cause in causes) { 
  for(child in children) {
    
    splits <- file.path(date, paste0(child, "_", cause, "_anemia_splits"))
    dir.create(file.path(splits), showWarnings = F)
    logs <- file.path(date, paste0(child, "_", cause, "_logs"))
    dir.create(file.path(logs), showWarnings = F)  
  
    command <- paste0("qsub  -q all.q  -P proj_PROJECT_NAME -l m_mem_free=60G  -l fthread=30 -l h_rt=24:00:00 -o ",logs," -e ",logs, " -N ", paste0(cause,"_", child, " "), shell, " ", "FILEPATH/pud_and_gastritis_anemia_child.R", " ", cause, " ", child, " ", splits)
    system(command)
  }
}



