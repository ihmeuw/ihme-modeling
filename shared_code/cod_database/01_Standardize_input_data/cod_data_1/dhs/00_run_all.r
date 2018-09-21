## runs the entire maternal mortality process
## NOTE: to run everything, set newflag <- 0
##       to just run it to pick up new DHS's, set newflag <- 1

rm(list=ls())

library(foreign);

root <- ifelse(Sys.info()[1]=="Windows","J:","/home/j")


####################
## set directories
####################

codedir <- paste(root,"/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/code",sep="")
datadir <- paste(root,"/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data",sep="")
logdir <- paste(root,"/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/logs",sep="")

###########################
## define parameters
###########################

test <- F ## if this is F, everything runs and submits. If this is T, nothing submits to the cluster
newflag <- 0 ## If this = 0, it runs all surveys. If this = 1, it will only run new surveys

########################
## create qsub function
########################

qsub <- function(jobname, shell, code=NULL, hold=NULL, pass=NULL, slots=NULL, test=F) {
  
  # set up jobs to hold for
  if (!is.null(hold)) {
    hold.string <- paste(" -hold_jid \"", hold, "\"", sep="")
  }
  
  # set up arguments to pass in
  if (!is.null(pass)) {
    pass.string <- ""
    for (ii in 1:length(pass)) pass.string <- paste(pass.string, " \"", pass[ii], "\"", sep="")
  }
  
  # construct the command
  sub <- paste("/usr/local/bin/SGE/bin/lx24-amd64/qsub -cwd",
               if (!is.null(hold)) hold.string,
               " -N ", jobname,
               if(!is.null(slots)) paste(" -pe multi_slot",slots,sep=" "),
               " ",shell," ",
               if(!is.null(code)) code, " ",
               if (!is.null(pass)) pass.string,
               sep="")
  
  # submit the command to the system
  if (test == F) {
    system(sub) 
    } else {
      cat(paste(sub,"\n")); flush.console()
    }
}

########################
## 0. Delete old analysis codebook; archive and delete old log files
########################

## analysis file
setwd(datadir)
file.remove("DHS_analysis_file.csv")

## log files
setwd(logdir)
logs <- list.files()[list.files() != "archive"]

file.copy(logs,paste("archive/archive",Sys.Date(),logs,sep="_"),overwrite=T)
file.remove(logs)

####################
## 1. call_dhs_mm_cluster.do
##    (finds all WN modules in DHS and AIS, creates codebook of these files, finds new files if applicable)
####################

setwd(codedir)

# set pass string
passarg <- NULL
passarg <- newflag

# submit it
qsub(jobname="dp0",shell="stata_shell.sh",code="call_dhs_mm_cluster.do",pass=passarg,test=test)

## wait until files are created
setwd(datadir)
while(!file.exists("DHS_analysis_file.csv")) Sys.sleep(10)

####################
## 2. dhs_prep_step1_cluster.do - PARALLELIZED
##    (loads data for each survey, formats variables, saves data)
####################

## read in data for looping
setwd(datadir)
dat <- read.csv("DHS_analysis_file.csv", stringsAsFactors=F)

# set hold string
holdids <- NULL
holdids[1] <- "dp0"

## loop through each file
setwd(codedir)
names <- c()
for (i in 1:dim(dat)[1]) {
  
  tmp <- dat[i,]
  
  ## get parameters
  filename <- tmp[,"data_name"]
  country <- tmp[,"country"]
  year <- tmp[,"year"]
  ddir <- tmp[,"survey"]
  
  # set pass string
  passarg <- NULL
  passarg <- paste(newflag,filename,country,year,ddir,sep="-")
  
  ## job name
  name <- paste("dp1",ddir,country,year,sep="_")
  
  # submit it
  qsub(jobname=name,shell="stata_shell.sh",code="dhs_prep_step1_cluster.do",
       pass=passarg,hold=paste(holdids,collapse=","),test=test)
  
  names[i] <- name
}


####################
## 3. dhs_prep_step2_cluster.do
##    (loads all the saved data from dhs_prep_step1.do, does analysis, saves pooled data)
####################

setwd(codedir)

# set hold string
holdids <- names

# submit it
qsub(jobname="dp2",shell="stata_shell.sh",code="dhs_prep_step2_cluster.do",
     hold=paste(holdids,collapse=","),test=test)


####################
## 4. dhs_prep_step3_cluster.do
##    (loads all the saved data from dhs_prep_step1.do, does analysis, saves pooled data)
####################

setwd(codedir)

# set hold string
holdids <- "dp2"

## loop through countries
names <- c()
for (c in unique(dat$country)) {

  # set pass string
  passarg <- NULL
  passarg <- paste(newflag,c,sep="-")
  
  name <- paste("dp3",c,sep="_")
  
  # submit it
  qsub(jobname=name,shell="stata_shell.sh",code="dhs_prep_step3_cluster.do",
       pass=passarg,hold=paste(holdids,collapse=","),slots=2,test=test)
  
  names <- c(names,name)
  
}


####################
## 5. dhs_prep_step4_cluster.do
##    (loads all the saved data from dhs_prep_step1.do, does analysis, saves pooled data)
####################

setwd(codedir)

# set hold string
holdids <- names

# set pass string
passarg <- NULL
passarg <- newflag

# submit it
qsub(jobname="dp4",shell="stata_shell.sh",code="dhs_prep_step4_cluster.do",
     pass=passarg,hold=paste(holdids,collapse=","),slots=5,test=test)

