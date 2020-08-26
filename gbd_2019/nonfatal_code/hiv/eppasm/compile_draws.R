### Setup
rm(list=ls())
windows <- Sys.info()[1][["sysname"]]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- "FILEPATH"
## Packages
library(data.table); library(mvtnorm); library(survey);library(assertable)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) > 0) {
  run.name <- args[1]
  loc <- args[2]
  n <- as.integer(args[3])
  draw.fill <- as.logical(args[4])
  paediatric <- as.logical(args[5])
} else {
  run.name <- "190630_rhino2"
  loc <- "MDG"
  n <- 1000
  draw.fill <- TRUE
  paediatric <- TRUE
}

## Functions
fill_draws <- function(fill.dt,type=NULL){
  missing <- setdiff(1:n, unique(fill.dt$run_num))
  print(length(missing))
  if(length(missing) > 0){
    have.draws <- unique(fill.dt$run_num)
    need.draws <- missing
    for(draw in need.draws) {
      print(draw)
      if(length(have.draws) > 1){
        replace.draw <- sample(have.draws, 1)
      }else{replace.draw <- have.draws}
      replace.dt <- fill.dt[run_num == replace.draw]
      replace.dt[, run_num := draw]
      fill.dt <- rbind(fill.dt, replace.dt)
    }
    if(type=="adult"){
    missing_spec <- data.table(ihme_loc_id=rep(loc,length(missing)), missing=need.draws)
    write.csv(missing_spec,paste0("FILEPATH", "/", loc,"/missing_or_neg_draws.csv"),row.names = FALSE)
    }
  }
  return(fill.dt)
}

draw.path <- "FILEPATH"
draw.list <- list.files(draw.path)

draw.list <- draw.list[grepl('.csv', draw.list) & !grepl('theta_', draw.list) & !grepl('under_', draw.list)]
draw.list <- draw.list[gsub('.csv', '', draw.list) %in% 1:n]

dt <- lapply(draw.list, function(draw){
draw.dt <- fread(paste0(draw.path, '/', draw))
})



dt.check <- lapply(dt,function(draw.dt)
  try(assert_values(draw.dt,colnames(draw.dt),test="gte",0))
)

dt.check <- unlist(lapply(dt.check,function(check) !class(check)=="try-error"))
dt <- rbindlist(dt[dt.check])


if(draw.fill){
  dt <- fill_draws(dt,type="adult")
}

compiled.path <- "FILEPATH"
dir.create(compiled.path, recursive = TRUE, showWarnings = FALSE)
write.csv(dt, paste0(compiled.path, loc, '.csv'), row.names = F)


## under 1 splits
if(paediatric){
  split.list <- list.files(draw.path)
  split.list <- split.list[grepl('under_', split.list)]
  split.dt <- lapply(split.list, function(draw){
  draw.dt <- fread(paste0(draw.path, '/', draw))
  })
  
  
  dt.check <- lapply(split.dt,function(draw.dt)
    try(assert_values(draw.dt,colnames(draw.dt),test="gte",0))
  )
  
  dt.check <- unlist(lapply(dt.check,function(check) !class(check)=="try-error"))
  split.dt <- rbindlist(split.dt[dt.check])
  
  if(draw.fill){
    split.dt <- fill_draws(split.dt,type="child")
  }
  
  write.csv(split.dt, paste0(compiled.path, loc, '_under1_splits.csv'), row.names = F)
}

