## Used in basically every script
rm(list = ls())
Sys.umask(mode = "0002")
windows <- Sys.info()[1][["sysname"]]=="Windows"
root <- "FILEPATH"
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))


args <- commandArgs(trailingOnly = TRUE)
print(args)
if(length(args) == 0){
  array.job = FALSE
  run.name = '240906_quokka_PJNZ_2023'
  loc <- 'KEN_35630'
  draw.fill <- T
  paediatric <- T
  gbdyear <- "gbd23"
  n = 1000
}else{
  run.name <- args[1]
  array.job <- as.logical(args[2])
  loc <- args[3]
  draw.fill <- as.logical(args[4])
  paediatric <- as.logical(args[5])
  gbdyear <- args[6]
}

test = NULL
print(gbdyear)
print(loc)
packages <- c('fastmatch')
for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

library(parallel)
library(data.table)
gbdeppaiml_dir <- "FILEPATH/gbdeppaiml/"
eppasm_dir <- "FILEPATH/eppasm/"
library(mortdb, lib ="FILEPATH")
library(assertable)


if(!array.job & length(args) > 0){
  loc <- args[3]
  n = 1000
}

# Array job ---------------------------------------
if(array.job){
  array.dt <- fread(paste0('FILEPATH/',run.name,'/array_table.csv'))
  task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
  loc <- array.dt[task_id,loc_scalar]
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
    write.csv(missing_spec,paste0('FILEPATH/', gbdyear, '/', run.name, "/", loc,"/missing_or_neg_draws.csv"),row.names = FALSE)

    }
  }
  return(fill.dt)
}

### Tables
loc.table <- data.table(get_locations(hiv_metadata = T))
print('loc.table loaded')


### Code
if(!is.null(test)){
  draw.path <- paste0('FILEPATH/', gbdyear, '/', run.name, "/", loc, '_', test)
}else{
  draw.path <- paste0('FILEPATH/', gbdyear, '/', run.name, "/", loc)
}

print(draw.path)
draw.list <- list.files(draw.path)
print('draw.list exists')

## subset out additional outputs (theta, under-1 splits)
draw.list <- draw.list[gsub('.csv', '', draw.list) %in% 1:1000]    
print(draw.list)
    
dt <- lapply(draw.list, function(draw){
  print(draw)
  draw.dt <- fread(paste0(draw.path, '/', draw))
})

## remove draws with character number
remove.draws <- c()


for (i in c(1:length(draw.list))) {
  print(i)
  if(class(dt[[i]][, c(pop, hiv_deaths, non_hiv_deaths, new_hiv, pop_neg, total_births,
                       hiv_births, birth_prev, pop_art, pop_gt350,pop_200to350, pop_lt200)])=="character"){
    remove.draws <- c(remove.draws, i)
  }
}
dt <- rbindlist(dt)
dt <- dt[!run_num %in% remove.draws]

dt <- melt(dt, id.vars = c('age', 'sex', 'year', 'run_num'))
print(colnames(dt))
dt <- as.data.table(dt)
dt[, value := as.numeric(value)]

##Sometimes there are negative values, need to replace
dt[value <0, value := 0]
print('negative values replaced if necessary')

dt.summary <- dt[, .(mean = mean(value), upper = quantile(value, probs=0.975), cap=quantile(value, probs=0.99)), by= c("age","sex","year", "variable")]
dt <- merge(dt, dt.summary)
drop.draws <- unique(dt[mean>upper&value>cap, run_num])
dt <- dt[!(run_num%in%drop.draws)]
print('problematic draws dropped')
dt[, mean:= NULL][, upper:= NULL][, cap:= NULL]
dt <- dcast(dt, age + sex + year + run_num  ~ variable, value.var = 'value')
dt <- as.data.table(dt)
    
    if(draw.fill){
      dt <- fill_draws(dt,type="adult")
    }
    
    print('fill_draws done')
    compiled.path <- paste0('FILEPATH/', gbdyear, '/', run.name, "/compiled/")
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




