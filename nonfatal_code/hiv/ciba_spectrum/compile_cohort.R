################################################################################
## Purpose: Compile cohort draws from Stage 1 Spectrum for CIBA
## Date modified:
################################################################################

### Setup
rm(list=ls())
windows <- Sys.info()[1]=="Windows"
root <- ifelse(windows,"FILEPATH")
user <- ifelse(windows, Sys.getenv("USERNAME"), Sys.getenv("USER"))
code.dir <- paste0(ifelse(windows, "FILEPATH", paste0(FILEPATH)

## Packages
library(data.table); library(parallel)

## Arguments
args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0) {
	loc <- args[1]
	run.name <- args[2]
	ncores <- args[3]
} else {
	loc <- "AUT"
	run.name <- ""
	ncores <- 1
}

### Paths
in.dir <- paste0("FILEPATH")
out.dir <- paste0("FILEPATH")
dir.create(out.dir, recursive = T, showWarnings = F)
out.path <- paste0(FILEPATH)


### Tables
loc.table <- get_locations()

### Code
file.list <- list.files(in.dir, "_cohort_")
combined.dt <- rbindlist(
	mclapply(file.list, function(file) {
		in.path <- paste0(in.dir, file)
		dt <- fread(in.path)
	}, mc.cores = ncores)
, fill = T)
write.csv(combined.dt, out.path, row.names = F)

### End