rm(list=ls())

library(foreign);

root <- ifelse(Sys.info()[1]=="Windows","J:","FILEPATH")
root2 <- ifelse(Sys.info()[1]=="Windows","H:","FILEPATH")


####################
## set directories
####################

codedir <- paste(root2,"FILEPATH",sep="")
datadir <- paste(root,"FILEPATH",sep="")
logdir <- paste(root,"FILEPATH",sep="")

###########################
## define parameters
###########################

test <- F
newflag <- 1

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
  sub <- paste("qsub -cwd",
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

## analysis file
setwd(datadir)
file.remove("FILEPATH")

## log files
setwd(logdir)
logs <- list.files()[list.files() != "archive"]

file.copy(logs,paste("FILEPATH",Sys.Date(),logs,sep="_"),overwrite=T)
file.remove(logs)



setwd(codedir)

# set pass string
passarg <- NULL
passarg <- newflag

# submit it
qsub(jobname="dp0",shell="FILEPATH",code="FILEPATH",pass=passarg,test=test)

## wait until files are created
setwd(datadir)
while(!file.exists("FILEPATH")) Sys.sleep(10)

# read in data for looping
setwd(datadir)
dat <- read.csv("FILEPATH", stringsAsFactors=F)

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
  qsub(jobname=name,shell="FILEPATH",code="FILEPATH",
       pass=passarg,hold=paste(holdids,collapse=","),test=test)
  
  names[i] <- name
}


setwd(codedir)

# set hold string
holdids <- names

# submit it
qsub(jobname="dp2",shell="FILEPATH",code="FILEPATH",
     hold=paste(holdids,collapse=","),test=test)



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
  qsub(jobname=name,shell="FILEPATH",code="FILEPATH",
       pass=passarg,hold=paste(holdids,collapse=","),slots=2,test=test)
  
  names <- c(names,name)
  
}


setwd(codedir)

# set hold string
holdids <- names

# set pass string
passarg <- NULL
passarg <- newflag

# submit it
qsub(jobname="dp4",shell="FILEPATH",code="FILEPATH",
     pass=passarg,hold=paste(holdids,collapse=","),slots=5,test=test)

