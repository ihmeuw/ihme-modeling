##Set up --------------------------- 
## Script name: compile_draws.R
## Purpose of script: Average across draws
##
## Author: Maggie Walters
## Date Created: 2018-04-11
## Email: mwalte10@uw.edu
## 
##
## Notes: Created by Tahvi Frank and modified for GBD20 by Maggie Walters
## Some arguments are likely to stay constant across runs, others we're more likely to test different options.
## The arguments that are more likely to vary are pulled from the eppasm run table
##

## Used in basically every script
Sys.umask(mode = "0002")
windows <- Sys.info()[1][["sysname"]]=="Windows"
root <- ifelse(windows,"J:/","/home/j/")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))


args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) == 0){
  array.job = FALSE
  run.name <- "200713_yuka"
  loc <- 'AGO'
  draw.fill <- TRUE
  paediatric <- TRUE
  n = 1000
}else{
  run.name <- args[1]
  array.job <- as.logical(args[2])
  draw.fill <- as.logical(args[4])
  paediatric <- as.logical(args[5])
  
}


gbdyear <- 'gbd20'
stop.year = 2022
gbdeppaiml_dir <- paste0(ifelse(windows, "H:", paste0("/ihme/homes/", user)), "/gbdeppaiml/")
eppasm_dir <- paste0(ifelse(windows, "H:", paste0("/ihme/homes/", user)), "/eppasm/")
hiv_gbd2019_dir <- paste0(ifelse(windows, "H:", paste0("/ihme/homes/", user)), "/hiv_gbd2019/")

setwd(eppasm_dir)
devtools::load_all()
setwd(gbdeppaiml_dir)
devtools::load_all()
if(!array.job & length(args) > 0){
  loc <- args[3]
  n = 1000
}

# Array job ---------------------------------------
if(array.job){
  array.dt <- fread(paste0('FILEPATH/array_table.csv'))
  array.dt <- array.dt[grep('ZAF_490', ihme_loc_id)]
  array.dt <- unique(array.dt[,(loc_scalar)])
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  loc <- array.dt[task_id]
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
      write.csv(missing_spec,paste0("FILEPATH/missing_or_neg_draws.csv"),row.names = FALSE)
      
    }
  }
  return(fill.dt)
}

### Tables
loc.table <- data.table(get_locations(hiv_metadata = T))
print('loc.table loaded')


### Code



draw.path <- paste0("FILEPATH")
draw.list <- list.files(draw.path)

print('draw.list exists')
## subset out additional outputs (theta, under-1 splits)
draw.list <- draw.list[grepl('.csv', draw.list) & !grepl('theta_', draw.list) & !grepl('under_', draw.list)]
draw.list <- draw.list[gsub('.csv', '', draw.list) %in% 1:1000]


dt <- lapply(draw.list, function(draw){
  print(draw)
  draw.dt <- fread(paste0(draw.path, '/', draw))
  draw.dt <- draw.dt[year < 2019]
})
##Sometimes there are negative values, need to replace

dt.check <- lapply(dt,function(draw.dt)
  try(assert_values(draw.dt,colnames(draw.dt),test="gte",0))
)

dt.check <- unlist(lapply(dt.check,function(check) !class(check)=="try-error"))
dt <- rbindlist(dt[dt.check])
print('negative values replaced if necessary')

if(draw.fill){
  dt <- fill_draws(dt,type="adult")
}

print('fill_draws done')
compiled.path <- paste0("FILEPATH/compiled/")
dir.create(compiled.path, recursive = TRUE, showWarnings = FALSE)
if(!is.null(test)){
  write.csv(dt, paste0(compiled.path, loc, '_', test, '.csv'), row.names = F)
  
}else{
  write.csv(dt, paste0(compiled.path, loc, '.csv'), row.names = F)
  
}
print('first file saved')
print(compiled.path)

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
  
  if(!is.null(test)){
    write.csv(split.dt, paste0(compiled.path, loc,'_', test, '_under1_splits.csv'), row.names = F)
    
  }else{
    write.csv(split.dt, paste0(compiled.path, loc, '_under1_splits.csv'), row.names = F)
    
  }
}