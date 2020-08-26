

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "H:/", "/homes/"), user, "/hiv_gbd2019/decomp2019/")

## Packages
library(data.table); library(parallel)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
  loc <- args[1]
  run_name <- args[2]
  n.runs <- args[3]

} else {
  loc <- "IND_4841"
  run_name <- "190630_rhino_ind"
  n.runs <- 1000
}


### Paths
in.dir <- paste0("/ihme/hiv/epp_output/gbd19/",run_name,"/",loc, "/")
prev_dir <- paste0(root,"/WORK/04_epi/01_database/02_data/hiv/04_models/gbd2015/02_inputs/prevalence_draws/",run_name,"/")
inc_dir <- paste0(root,"/WORK/04_epi/01_database/02_data/hiv/04_models/gbd2015/02_inputs/incidence_draws/",run_name,"/")
dir.create(prev_dir, showWarnings = F)
dir.create(inc_dir, showWarnings = F)

prev_path <- paste0(prev_dir, loc, "_SPU_prev_draws.csv")
inc_path <- paste0(inc_dir, loc, "_SPU_inc_draws.csv")

### Code
## Incidence
dt1 <- NULL

missing.runs <- c()
kept.draws <- c()
j = 1


for(i in c(1:n.runs)) {
  file <- paste0('results_incid',i,'.csv')
  if (file.exists(paste0(in.dir, file))) {
    print(paste("Found run",i))
    dt <- fread(paste0(in.dir, file))
    tmp.n.draws <- length(names(dt)[names(dt) != 'year'])
    keep.draw <- sample(1:tmp.n.draws, ifelse(n.runs == 1, 1000, 1))
    k <- 1
    while(any(is.na(dt[,c('year', paste0('draw',keep.draw)), with=F])) & k != 50) {
      keep.draw <- sample(1:tmp.n.draws, ifelse(n.runs == 1000, 1, 1000))
      k <- k + 1
    }
    if(k == 50) {
      print(paste("Missing draw",i))
      missing.runs <- c(missing.runs, i) 
      next     
    }
    kept.draws <- c(kept.draws, keep.draw)
    dt <- dt[,c('year', paste0('draw',keep.draw)), with=F]
    dt <- melt(dt, id.vars = "year", variable.name = "run", value.name = "incid")
    dt <- dt[,draw:=ifelse(n.runs != 1, i, as.integer(sub("draw", "", run))), by = "run"]
    dt1 <- rbindlist(list(dt1,dt))
  }  else {
    print(paste("Missing run",i))
    missing.runs <- c(missing.runs, i)
  }
}

if (length(missing.runs) > 0) {
  replace.with <- sample(dt1[,unique(draw)], length(missing.runs), replace=TRUE)
  for (i in 1:length(missing.runs)) {
    tmp.dt <- dt1[draw==replace.with[i],]
    tmp.dt[,draw := missing.runs[i]]
    dt1 <- rbind(dt1, tmp.dt)
  }
}

dt1[,run:=NULL]
dt1[,incid:=incid*100]
setnames(dt1, c('draw', 'incid'), c('run', 'draw'))
reshaped.dt <- dcast.data.table(dt1,year~run, value.var = "draw")
setnames(reshaped.dt, as.character(1:n.runs), paste0('draw', 1:n.runs))
reshaped.dt <- reshaped.dt[order(year),]

write.csv(reshaped.dt, file = inc_path, row.names=F)


## Prevalence
prev_dt <- NULL

j = 1
for (i in setdiff(c(1:n.runs), missing.runs)) {
  file <- paste0('results_prev',i,'.csv')
  if (file.exists(paste0(in.dir, file))) {
    print(paste('prev',i))
    dt <- fread(paste0(in.dir, file))
    dt <- dt[,c('year', paste0('draw',ifelse(n.runs == 1, kept.draws, kept.draws[j]))), with=F]
    dt <- melt(dt, id.vars = "year", variable.name = "run", value.name = "prev")
    dt <- dt[,draw:=ifelse(n.runs == 1, as.integer(sub("draw", "", run)), i), by = "run"]
    prev_dt <- rbindlist(list(prev_dt,dt))
    j <- j + 1
  }
}

if (length(missing.runs) > 0) {
  for (i in 1:length(missing.runs)) {
    tmp.dt <- prev_dt[draw==replace.with[i],]
    tmp.dt[,draw := missing.runs[i]]
    prev_dt <- rbind(prev_dt, tmp.dt)
  }
}

prev_dt[,run:=NULL]
prev_dt[,prev:=prev*100]
setnames(prev_dt, c('draw', 'prev'), c('run', 'draw'))
out.prev <- dcast.data.table(prev_dt, year~run, value.var = "draw")
setnames(out.prev, as.character(1:n.runs), paste0('draw', 1:n.runs))
out.prev <- out.prev[order(year),]

write.csv(out.prev, file = prev_path, row.names=F)

### End