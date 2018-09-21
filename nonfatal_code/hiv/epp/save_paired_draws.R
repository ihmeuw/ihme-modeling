################################################################################
## Purpose: Compile draws of incidence and prevalence from EPP
## Run instructions: Launch from run_all
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,FILEPATH,FILEPATH)
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, FILEPATH, FILEPATH), user, "/HIV/")

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
	loc <- "GHA"
  run_name <- "161218_all_locations_full_draws"
  n.draws <- 1000
}

### Paths
in.dir <- paste0(FILEPATH,run_name,"/",loc, "/")

prev_dir <- paste0(FILEPATH)
inc_dir <- paste0(FILEPATH)
dir.create(prev_dir, showWarnings = F)
dir.create(inc_dir, showWarnings = F)

prev_path <- paste0(prev_dir, loc, "_SPU_prev_draws.csv")
inc_path <- paste0(inc_dir, loc, "_SPU_inc_draws.csv")

### Code
## Incidence
dt1 <- NULL

missing.draws <- c()
kept.draws <- c()
j = 1
for (i in c(1:n.draws)) {
  file <- paste0('results_incid',i,'.csv')
  if (file.exists(paste0(in.dir, file))) {
    print(paste("Found draw",i))
    dt <- fread(paste0(in.dir, file))
    tmp.n.draws <- length(names(dt)[names(dt) != 'year'])
    keep.draw <- sample(1:tmp.n.draws, 1)
    kept.draws <- c(kept.draws, keep.draw)
    dt <- dt[,c('year', paste0('draw',keep.draw)), with=F]
    dt <- melt(dt, id.vars = "year", variable.name = "run", value.name = "incid")
    dt <- dt[,draw:=i]
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
setnames(reshaped.dt, as.character(1:n.draws), paste0('draw', 1:n.draws))
reshaped.dt <- reshaped.dt[order(year),]

write.csv(reshaped.dt, file = inc_path, row.names=F)

## Prevalence
prev_dt <- NULL

j = 1
for (i in c(1:n.draws)) {
  file <- paste0('results_prev',i,'.csv')
  if (file.exists(paste0(in.dir, file))) {
    print(paste('prev',i))
    dt <- fread(paste0(in.dir, file))
    dt <- dt[,c('year', paste0('draw',kept.draws[j])), with=F]
    dt <- melt(dt, id.vars = "year", variable.name = "run", value.name = "prev")
    dt <- dt[,draw:=i]
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
setnames(out.prev, as.character(1:n.draws), paste0('draw', 1:n.draws))
out.prev <- out.prev[order(year),]

write.csv(out.prev, file = prev_path, row.names=F)

### End