
### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"","")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "", ""))

## Packages
library(data.table); library(parallel)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
  loc <- args[1]
  run_name <- args[2]
  n.draws <- args[3]

} else {
	loc <- ""
  run_name <- ""
  n.draws <- 1000
}
ncores <- 2

### Paths
in.dir <- paste0("")

prev_dir <- paste0(root,"")
inc_dir <- paste0(root,"")
dir.create(prev_dir, showWarnings = F)
dir.create(inc_dir, showWarnings = F)

prev_path <- paste0(prev_dir, loc, "")
inc_path <- paste0(inc_dir, loc, "")

### Code
## Incidence
dt1 <- NULL

missing.draws <- c()
kept.draws <- c()
j = 1
for(i in c(1:n.draws)) {
  file <- paste0('results_incid',i,'.csv')
  if (file.exists(paste0(in.dir, file))) {
    print(paste("Found draw",i))
    dt <- fread(paste0(in.dir, file))
    tmp.n.draws <- length(names(dt)[names(dt) != 'year'])
    keep.draw <- sample(1:tmp.n.draws, ifelse(n.draws == 1000, 1, 1000))
    k <- 1
    while(any(is.na(dt[,c('year', paste0('draw',keep.draw)), with=F])) & k != 50) {
      keep.draw <- sample(1:tmp.n.draws, ifelse(n.draws == 1000, 1, 1000))
      k <- k + 1
    }
    if(k == 50) {
      print(paste("Missing draw",i))
      missing.draws <- c(missing.draws, i) 
      next     
    }
    kept.draws <- c(kept.draws, keep.draw)
    dt <- dt[,c('year', paste0('draw',keep.draw)), with=F]
    dt <- melt(dt, id.vars = "year", variable.name = "run", value.name = "incid")
    dt <- dt[,draw:=ifelse(n.draws == 1000, i, as.integer(sub("draw", "", run))), by = "run"]
    dt1 <- rbindlist(list(dt1,dt))
  }
  else {
    print(paste("Missing draw",i))
    missing.draws <- c(missing.draws, i)
  }
}

replace.with <- sample(dt1[,unique(draw)], length(missing.draws), replace=TRUE)

if (length(missing.draws) > 0) {
  for (i in 1:length(missing.draws)) {
    tmp.dt <- dt1[draw==replace.with[i],]
    tmp.dt[,draw := missing.draws[i]]
    dt1 <- rbind(dt1, tmp.dt)
  }
}

dt1[,run:=NULL]
dt1[,incid:=incid*100]
setnames(dt1, c('draw', 'incid'), c('run', 'draw'))
reshaped.dt <- dcast.data.table(dt1,year~run, value.var='draw')
setnames(reshaped.dt, as.character(1:1000), paste0('draw', 1:1000))
reshaped.dt <- reshaped.dt[order(year),]

write.csv(reshaped.dt, file = inc_path, row.names=F)

## Prevalence
prev_dt <- NULL

j = 1
for (i in setdiff(c(1:n.draws), missing.draws)) {
  file <- paste0('results_prev',i,'.csv')
  if (file.exists(paste0(in.dir, file))) {
    print(paste('prev',i))
    dt <- fread(paste0(in.dir, file))
    dt <- dt[,c('year', paste0('draw',if(n.draws == 1000) {kept.draws[j]} else{kept.draws})), with=F]
    dt <- melt(dt, id.vars = "year", variable.name = "run", value.name = "prev")
    dt <- dt[,draw:=ifelse(n.draws == 1000, i, as.integer(sub("draw", "", run))), by = "run"]
    prev_dt <- rbindlist(list(prev_dt,dt))
    j <- j + 1
  }
}

if (length(missing.draws) > 0) {
  for (i in 1:length(missing.draws)) {
    tmp.dt <- prev_dt[draw==replace.with[i],]
    tmp.dt[,draw := missing.draws[i]]
    prev_dt <- rbind(prev_dt, tmp.dt)
  }
}

prev_dt[,run:=NULL]
prev_dt[,prev:=prev*100]
setnames(prev_dt, c('draw', 'prev'), c('run', 'draw'))
out.prev <- dcast.data.table(prev_dt,year~run, value.var='draw')
setnames(out.prev, as.character(1:1000), paste0('draw', 1:1000))
out.prev <- out.prev[order(year),]

write.csv(out.prev, file = prev_path, row.names=F)

### End