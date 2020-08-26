# ###################################################
# 
# NAME
# 6/2/2017
# Project: ID Distribution Fitting
# 
# Using mean and prev (IQ<70) estimates to calculate
# implied standard deviation. From there, assume a normal 
# distribution and calculate out severity splits for IQ
# 
# ####################################################

rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "ADDRESS"
  h <- "ADDRESS"
} else {
  j <- "ADDRESS"
  user <- Sys.info()[["user"]]
  h <- paste0("ADDRESS", user)
}

## Packages
library(data.table); library(dplyr); library(DBI)

## Other utilities
ubcov_central <- paste0(j, "FILEPATH")
setwd(ubcov_central)
source(paste0(ubcov_central, "FILEPATH.r"))
source(paste0(ubcov_central, "FILEPATH.r"))
source(paste0(ubcov_central, "FILEPATH.r"))


## Me DB
me.db <- fread(paste0(j, 'FILEPATH.csv'))
me_ids <- unique(me.db$modelable_entity_id)

## Paths
draws_root <- paste0(j, 'FILEPATH')
out_root <- paste0(draws_root, "FILEPATH")


##--FUNCTIONS------------------------------------------------------------------

load.draws <- function(path, loc) {
  col <- c("location_id", "year_id", "age_group_id", "sex_id")
  keep <- c(col, paste0("draw_", c(0:999)))
  ## LOAD
  df.m <- paste0(path, "/mean_", loc, ".csv") %>% fread
  df.s <- paste0(path, "/sd_", loc, ".csv") %>% fread
  df.p <- paste0(path, "/prevalence_", loc, ".csv") %>% fread
  df.p <- df.p[measure_id==5]
  ## SUBSET
  df.m <- df.m[, keep, with=F]
  df.s <- df.s[, keep, with=F]
  df.p <- df.p[, keep, with=F]
  ## RESHAPE
  df.m <- melt(df.m, id=col, value.name='imn', variable.name='sim')
  df.s <- melt(df.s, id=col, value.name='isd', variable.name='sim')
  df.p <- melt(df.p, id=col, value.name='iprev', variable.name='sim')
  ## MERGE
  id <- c(col, 'sim')
  setkeyv(df.m, id)
  setkeyv(df.s, id)
  setkeyv(df.p, id)
  df <- df.m[df.s[df.p]]
  return(df)
}


calc.splits <- function(df) {
  ## Calculate implied SD from mean and prev
  df <- df[, sd_est := (70 - imn)/qnorm(iprev)]
  ## Calculating severity splits (proportion between thresholds)
  splits <- list(c(0, 20), c(20, 35), c(35, 50), c(50, 70), c(70, 85))
  for (x in splits) {
    lower <- x[[1]]
    upper <- x[[2]]
    col <- paste0("prop_",lower,"_", upper)
    df <- df[, (col) := pnorm(upper, mean=imn, sd=sd_est) - pnorm(lower, mean=imn, sd=sd_est)]
  }
  ## Calculate profound/(severe + profound)
  df <- df[, prop_cret := prop_0_20/(prop_0_20 + prop_20_35)]
  ## Cols
  cols <- grep("prop_", names(df), value=TRUE)
  ## Split and clean
  df.list <- lapply(cols, function(col) {
    ## Clean and reshape wide
    id <- c("location_id", "year_id", "age_group_id", "sex_id")
    df.w <- dcast(df, paste0(paste0(id, collapse = " + "), " ~ sim"), value.var=col)
    ## Get me_id
    df.w <- df.w[, modelable_entity_id := me.db[split==col]$modelable_entity_id]
    df.w <- df.w[, measure_id := 5]
    return(df.w)    
  })
  return(df.list)    
}

##------------------------------------------------------------------------------



############################################################################################

## SETTINGS
args <- commandArgs(trailingOnly = TRUE)
if (length(args)!=0) {
  ##--LAUNCH-------------------------------------------------------------------------------
  if (args[1]=="launch") {
    # pull relevant locations
    locs <- get_location_hierarchy(153)[level>=3]$location_id
    # refresh folders
    unlink(out_root, recursive=TRUE)
    dir.create(out_root)
    lapply(me_ids, function(x) dir.create(paste0(out_root, "/", x)))
    # submit
    for (loc in locs) {
      # Settings
      job_name <- paste0("cidist_", loc)
      script <- paste0(j, "FILEPATH.r")
      slots <- 2
      memory <- 4
      cluster_project <- "ADDRESS"
      logs <- "FILEPATH"
      # Arguments
      arguments <- c("run", loc, slots)
      qsub(job_name=job_name, script=script, slots=slots, memory=memory, cluster_project=cluster_project, logs=logs, arguments=arguments)
    }
  }  
  ##--RUN-----------------------------------------------------------------------------------
  
  if (args[1]=="run") {
    # settings
    loc <- args[2]
    # run
    df <- load.draws(draws_root, loc)
    df.list <- calc.splits(df)
    # save
    for (i in 1:length(df.list)) {
      me_id <- df.list[[i]][1]$modelable_entity_id
      path <- paste0(out_root, "/", me_id, "/", loc, ".csv")
      write.csv(df.list[[i]], path, row.names=F, na="")
    }
  }
  ##----------------------------------------------------------------------------------------
}